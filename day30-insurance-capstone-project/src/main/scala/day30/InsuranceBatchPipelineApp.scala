package day30

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import java.time.format.DateTimeParseException

/**
 * Day 30 — Final Capstone: Insurance (BATCH component)
 *
 * Domain: claims_history.csv (~10,100 historical claims) enriched against
 * policies.csv (280 policies) and customers.csv (150 customers), to produce
 * a per-customer fraud risk band and a set of Spark SQL reports. One
 * customer, CUST0001, is deliberately seeded with ~10% of all claims
 * spread across 7 of their own policies - a "fraud ring" scenario that
 * both this batch app (via claim-frequency scoring) and the companion
 * streaming app, InsuranceClaimsStreamApp, are built to catch.
 *
 * Run from this project's root directory: sbt run, choose this app's
 * number if prompted (two main classes live in this project - this one and
 * the streaming one).
 *
 * Covers (of the capstone's 6 requirements):
 *   - batch half of "batch and streaming components where appropriate"
 *   - broadcast (policies, customers - a two-hop lookup), accumulator
 *     (invalid-row count during RDD parsing), cache/persist (enriched
 *     DataFrame), partition tuning (shuffle.partitions + repartition)
 *   - Spark SQL, joins, aggregations, windows, and the capstone's UDF
 *     (computeRiskBand)
 *   - DAG/lineage/stages/executors/YARN - see explainDagLineageAndStages()
 *     and explainYarnDeployment()
 */
object InsuranceBatchPipelineApp {

  case class Claim(claimId: String, policyId: String, claimAmount: Double, claimType: String, claimDate: String, status: String)
  case class PolicyRef(customerId: String, policyType: String, premium: Double, riskLevel: String)
  case class CustomerRef(name: String, age: Int, city: String)

  val claimsPath = "src/main/resources/claims_history.csv"
  val policiesPath = "src/main/resources/policies.csv"
  val customersPath = "src/main/resources/customers.csv"
  val outputPath = "output/enriched_claims"

  def main(args: Array[String]): Unit = {
    explainOverview()

    val spark = SparkSession.builder().appName("Day30-InsuranceBatchPipelineApp").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Partition tuning #1: see Day 29's identical reasoning - 8 comfortably
    // exceeds local[2]'s 2 cores without the scheduling overhead of the
    // cluster-scale default (200) on a 10K-row practice dataset.
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    println("[CONFIG] spark.sql.shuffle.partitions = 8")

    // =================================================================
    // RAW -> CLEAN (RDD / Pair RDD + accumulator)
    // =================================================================
    val cleanClaimsRDD: RDD[Claim] = rawToCleanRDD(spark)

    println("\n[LINEAGE] cleanClaimsRDD.toDebugString - each indentation level is one RDD in the lineage graph; " +
      "'+-(N)' node is where a shuffle boundary (and therefore a new stage) starts:")
    println(cleanClaimsRDD.toDebugString)

    val claimsDS: Dataset[Claim] = cleanClaimsRDD.toDS()

    // =================================================================
    // ENRICH (two-hop broadcast join + window + UDF)
    // =================================================================
    val (policiesDF, customersDF) = loadReferenceData(spark)
    val enriched = enrichClaims(claimsDS, policiesDF, customersDF)

    println("\n[EXPLAIN] Physical plan for the two-hop enrichment join - look for TWO BroadcastHashJoin nodes, no Exchange on the claims side:")
    enriched.explain("formatted")

    // Persistence: enriched feeds 3 independent aggregations below plus the
    // partitioned write - same reuse-driven persist lesson as every prior
    // capstone-scale day (Day 22, Day 29).
    enriched.persist(StorageLevel.MEMORY_AND_DISK)
    val enrichedCount = enriched.count()
    println(f"[PERSIST] enriched claims cached (MEMORY_AND_DISK) - $enrichedCount%,d rows materialized")
    cleanClaimsRDD.unpersist()

    println("\n[PREVIEW] enriched sample - note claimFrequencyBand (window count) and riskBand (UDF, 3-factor):")
    enriched.select("claimId", "customerId", "name", "policyType", "claimAmount", "claimFrequencyBand", "riskBand")
      .orderBy(col("claimFrequencyBand").desc)
      .show(10, truncate = false)

    // =================================================================
    // AGGREGATE (Spark SQL + window functions)
    // =================================================================
    reportByPolicyType(enriched)
    reportTopCustomersByPayout(enriched)
    reportRunningPayoutPerCustomer(enriched)
    reportRiskBandDistribution(spark, enriched)

    explainDagLineageAndStages()
    explainYarnDeployment()

    writePartitionedOutput(enriched)
    enriched.unpersist()

    println("\n[DONE] Batch pipeline complete.")
    spark.stop()
  }

  def explainOverview(): Unit = {
    println(
      """================================================================
        | Day 30 - Insurance Capstone (BATCH component)
        | raw claims_history.csv
        |   -> RDD parse/validate (accumulator counts invalid rows)
        |   -> Pair RDD reduceByKey dedup by claimId          [shuffle]
        |   -> Dataset[Claim]
        |   -> broadcast join policies, broadcast join customers
        |   -> window: claimFrequency per customer (count over partitionBy)
        |   -> UDF: computeRiskBand(age, policyRiskLevel, claimFrequencyBand)
        |   -> persist(MEMORY_AND_DISK)
        |   -> Spark SQL reports (by policyType, top customers, running
        |      payout, risk band distribution)
        |   -> partitioned Parquet write
        |================================================================
        |""".stripMargin)
  }

  // =================================================================
  // RAW -> CLEAN
  // =================================================================

  def parseLine(line: String): Option[Claim] = {
    val parts = line.split(",", -1)
    if (parts.length != 6) return None
    val Array(claimId, policyId, amountS, claimType, claimDate, status) = parts
    if (claimId.trim.isEmpty || policyId.trim.isEmpty) return None
    val validStatuses = Set("APPROVED", "REJECTED", "PENDING")
    if (!validStatuses.contains(status)) return None
    try {
      val amount = amountS.toDouble
      if (amount <= 0) return None
      LocalDate.parse(claimDate)
      Some(Claim(claimId, policyId, amount, claimType, claimDate, status))
    } catch {
      case _: NumberFormatException => None
      case _: DateTimeParseException => None
    }
  }

  def rawToCleanRDD(spark: SparkSession): RDD[Claim] = {
    val sc = spark.sparkContext

    // Accumulator demonstration (batch flavor): a true running total across
    // this ONE pass over the data, incremented inside a map's side effect
    // and read back on the driver after the triggering action completes -
    // contrast with the streaming app's accumulators, which run for the
    // life of a long-running application instead of one batch job.
    val invalidRowsAcc = sc.longAccumulator("invalidClaimRows")

    val rawLines = sc.textFile(claimsPath)
    val header = rawLines.first()
    val dataLines = rawLines.filter(_ != header)
    dataLines.persist(StorageLevel.MEMORY_ONLY)
    val totalRaw = dataLines.count()

    val parsed: RDD[Option[Claim]] = dataLines.map { line =>
      val result = parseLine(line)
      if (result.isEmpty) invalidRowsAcc.add(1)
      result
    }
    parsed.persist(StorageLevel.MEMORY_ONLY)
    val validOrders = parsed.flatMap(_.toList)

    // Pair RDD dedup by claimId - same shuffle technique as Day 29.
    val keyed: RDD[(String, Claim)] = validOrders.map(c => (c.claimId, c))
    val deduped: RDD[(String, Claim)] = keyed.reduceByKey((first, _) => first)
    val cleanClaimsRDD: RDD[Claim] = deduped.values
    cleanClaimsRDD.persist(StorageLevel.MEMORY_ONLY)
    val totalClean = cleanClaimsRDD.count() // triggers the whole chain, including invalidRowsAcc

    println(
      f"""
        |[CLEAN] raw lines (excl. header): $totalRaw%,d
        |[CLEAN] invalid/malformed (accumulator): ${invalidRowsAcc.value}%,d
        |[CLEAN] unique after Pair RDD dedup by claimId: $totalClean%,d
        |""".stripMargin)

    parsed.unpersist()
    dataLines.unpersist()
    cleanClaimsRDD
  }

  // =================================================================
  // ENRICH
  // =================================================================

  def loadReferenceData(spark: SparkSession): (DataFrame, DataFrame) = {
    val policiesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(policiesPath)
    val customersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(customersPath)
    (policiesDF, customersDF)
  }

  /** Combines three independent risk factors into one label. A genuinely
    * multi-dimensional UDF: age bracket (3 levels) x policy risk level (3
    * levels) x claim-frequency band (4 levels) is up to 36 combinations -
    * a when/otherwise chain covering that cross-product would be an
    * unreadable, untestable wall of conditions duplicating the same point
    * table below. As a plain Scala function it's independently unit-
    * testable and the scoring table is a single, obvious place to tune. */
  def computeRiskBand(age: Int, policyRiskLevel: String, claimFrequencyBand: String): String = {
    val ageScore = if (age < 25 || age > 65) 1 else 0
    val policyScore = policyRiskLevel match {
      case "HIGH" => 2
      case "MEDIUM" => 1
      case _ => 0
    }
    val frequencyScore = claimFrequencyBand match {
      case "CRITICAL" => 3
      case "HIGH" => 2
      case "MEDIUM" => 1
      case _ => 0
    }
    val total = ageScore + policyScore + frequencyScore
    if (total >= 6) "CRITICAL"
    else if (total >= 4) "HIGH"
    else if (total >= 2) "MEDIUM"
    else "LOW"
  }

  def enrichClaims(claimsDS: Dataset[Claim], policiesDF: DataFrame, customersDF: DataFrame): DataFrame = {
    val computeRiskBandUDF = udf(computeRiskBand _)

    val policiesSelected = policiesDF.select(
      col("policyId"), col("customerId"), col("policyType"), col("riskLevel").as("policyRiskLevel")
    )
    val customersSelected = customersDF.select(
      col("customerId").as("custId"), col("name"), col("age"), col("city")
    )

    // Two-hop broadcast join: claims -> policies (to learn customerId and
    // the policy's own risk level) -> customers (to learn age/name/city).
    // Both reference tables are small (280 and 150 rows) - broadcast(...)
    // on each keeps the large claims side shuffle-free through both hops.
    val withPolicy = claimsDS
      .join(broadcast(policiesSelected), Seq("policyId"), "left")
      .withColumn("policyRiskLevel", coalesce(col("policyRiskLevel"), lit("MEDIUM")))

    val withCustomer = withPolicy
      .join(broadcast(customersSelected), col("customerId") === col("custId"), "left")
      .drop("custId")
      .withColumn("name", coalesce(col("name"), lit("Unknown Customer")))
      .withColumn("age", coalesce(col("age"), lit(40)))
      .withColumn("city", coalesce(col("city"), lit("Unknown City")))

    // Window function: how many claims has this customer filed, in total,
    // across ALL their policies - computed per-row (not grouped away) via
    // an unbounded partition window, so every claim row still carries its
    // customer's overall frequency alongside it.
    val byCustomer = Window.partitionBy("customerId")
    val withFrequency = withCustomer
      .withColumn("claimFrequency", count(lit(1)).over(byCustomer))
      .withColumn(
        "claimFrequencyBand",
        when(col("claimFrequency") >= 8, "CRITICAL")
          .when(col("claimFrequency") >= 5, "HIGH")
          .when(col("claimFrequency") >= 2, "MEDIUM")
          .otherwise("LOW")
      )

    withFrequency.withColumn(
      "riskBand",
      computeRiskBandUDF(col("age"), col("policyRiskLevel"), col("claimFrequencyBand"))
    )
  }

  // =================================================================
  // AGGREGATE (Spark SQL + window functions)
  // =================================================================

  def reportByPolicyType(enriched: DataFrame): Unit = {
    val byType = enriched.filter(col("status") === "APPROVED")
      .groupBy("policyType")
      .agg(count(lit(1)).as("claims"), round(sum("claimAmount"), 2).as("totalPayout"))
      .orderBy(col("totalPayout").desc)
    println("\n[REPORT] Approved claims and payout by policy type:")
    byType.show(truncate = false)
  }

  def reportTopCustomersByPayout(enriched: DataFrame): Unit = {
    val byCustomer = enriched.filter(col("status") === "APPROVED")
      .groupBy("customerId", "name")
      .agg(round(sum("claimAmount"), 2).as("totalPayout"))

    val ranked = byCustomer.withColumn("rank", dense_rank().over(Window.orderBy(col("totalPayout").desc)))
      .filter(col("rank") <= 5)
      .orderBy("rank")
    println("\n[REPORT] Top 5 customers by total approved payout (window function: dense_rank):")
    ranked.show(truncate = false)
  }

  def reportRunningPayoutPerCustomer(enriched: DataFrame): Unit = {
    val byCustomerDate = Window.partitionBy("customerId").orderBy("claimDate")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val withRunning = enriched.filter(col("status") === "APPROVED")
      .withColumn("runningPayout", round(sum("claimAmount").over(byCustomerDate), 2))

    println("\n[REPORT] Running approved payout over time - fraud-ring customer CUST0001 vs two regular customers:")
    withRunning.filter(col("customerId").isin("CUST0001", "CUST0002", "CUST0003"))
      .select("customerId", "claimDate", "claimAmount", "runningPayout")
      .orderBy("customerId", "claimDate")
      .show(30, truncate = false)
  }

  def reportRiskBandDistribution(spark: SparkSession, enriched: DataFrame): Unit = {
    enriched.select("customerId", "riskBand").distinct().createOrReplaceTempView("customer_risk")
    println("\n[REPORT] Spark SQL - customer count by risk band:")
    spark.sql(
      """SELECT riskBand, COUNT(*) AS customers
        |FROM customer_risk
        |GROUP BY riskBand
        |ORDER BY CASE riskBand
        |  WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END""".stripMargin
    ).show(truncate = false)
  }

  // =================================================================
  // Write
  // =================================================================

  def writePartitionedOutput(enriched: DataFrame): Unit = {
    println("[WRITE] repartition(col(\"policyType\")) then partitionBy(\"policyType\") - same small-files avoidance as Day 29.")
    enriched.repartition(col("policyType"))
      .write.mode("overwrite")
      .partitionBy("policyType")
      .parquet(outputPath)
    println(s"[WRITE] done - see $outputPath/policyType=.../*.parquet")
  }

  // =================================================================
  // DAG / lineage / stages / executors
  // =================================================================

  def explainDagLineageAndStages(): Unit = {
    println(
      """
        |================================================================
        | DAG, lineage, stages, and executors in this pipeline
        |================================================================
        |
        |DAG (Directed Acyclic Graph): every RDD/DataFrame transformation
        |(map, filter, join, groupBy, withColumn...) is LAZY - calling it
        |just adds a node to a graph of "how to compute this" without
        |running anything. Only an ACTION (count, collect, show, write)
        |triggers the DAG Scheduler to turn that graph into actual work.
        |Nothing in this app ran until the first .count() call.
        |
        |Lineage: the printed cleanClaimsRDD.toDebugString above IS the
        |DAG, in text form, for that one RDD - every line is a parent RDD
        |this one was derived from, all the way back to the raw text file.
        |This is what Day 7's fault tolerance lesson relies on: if a
        |partition's cached data is lost (an executor crashes), Spark
        |doesn't fail the job - it just recomputes that partition FROM this
        |lineage, re-running only the transformations needed to rebuild it.
        |
        |Stages: the DAG Scheduler cuts the graph into stages at every
        |shuffle boundary - a stage is the largest chunk of the DAG that
        |can run as narrow (no-shuffle) transformations back to back on one
        |partition, without needing data from any OTHER partition. This app
        |has shuffle boundaries at: the Pair RDD dedup (reduceByKey), each
        |Spark SQL groupBy/window aggregation, and the repartition before
        |the final write - each one starts a new stage. The two broadcast
        |joins do NOT start new stages, because broadcasting avoids the
        |shuffle that would otherwise force one.
        |
        |Executors: JVM processes that actually run the tasks a stage is
        |split into (one task per partition) - in this local[2] run, that's
        |simulated with 2 threads in the one local JVM; on a real cluster
        |(see explainYarnDeployment() below) these are separate processes
        |on separate machines. An executor also holds this app's cached
        |data (enriched.persist() above) in its own memory/disk blocks, and
        |reports task results and block locations back to the driver.
        |================================================================
        |""".stripMargin)
  }

  def explainYarnDeployment(): Unit = {
    println(
      """Running this BATCH app on YARN instead of local[2]:
        |
        |  spark-submit \
        |    --class day30.InsuranceBatchPipelineApp \
        |    --master yarn \
        |    --deploy-mode cluster \
        |    --num-executors 4 --executor-cores 2 --executor-memory 2g \
        |    --driver-memory 1g \
        |    target/scala-2.12/day30-insurance-capstone-project_2.12-0.1.0.jar
        |
        |  cluster mode is appropriate here too (same reasoning as Day 27):
        |  YARN's ResourceManager runs the driver itself inside an
        |  ApplicationMaster container, so the job survives the submitting
        |  machine disconnecting - though for a one-shot BATCH job like this
        |  (unlike the companion streaming app) that mostly matters for very
        |  long-running jobs, since a short batch job's driver only needs to
        |  live for the job's duration anyway.
        |
        |  This app needs NO checkpoint directory (no updateStateByKey, no
        |  streaming) - that requirement is specific to
        |  InsuranceClaimsStreamApp, which does need one on HDFS once it's
        |  off a single local machine.
        |
        |  The two broadcast joins matter even more at cluster scale: each
        |  of the 4 executors gets ONE copy of policies.csv and
        |  customers.csv (not one per task), so broadcasting avoids both a
        |  shuffle AND redundant network transfer to every task slot.
        |""".stripMargin)
  }
}
