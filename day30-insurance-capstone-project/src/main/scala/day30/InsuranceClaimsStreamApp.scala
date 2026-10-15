package day30

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConverters._

/**
 * Day 30 — Final Capstone: Insurance (STREAMING component)
 *
 * Live claim submissions arrive over a socket (same pattern as Days
 * 23-28). This app resolves each claim's policyId to its owning customer
 * via a broadcast lookup (built from the SAME policies.csv/customers.csv
 * as the batch app), then watches for a customer filing claims in an
 * unusually tight burst - the live-fraud-detection counterpart to the
 * batch app's claim-frequency risk banding. CUST0001 (the batch app's
 * seeded "fraud ring" customer) is also scripted into the feeder to burst
 * here, so the same customer trips both the batch report and this live
 * alert.
 *
 * Needs a feeder, same pattern as Days 23-28. Start it FIRST:
 *
 *   python3 scripts/claims_feed_server.py
 *
 * ...then, in a SECOND terminal:
 *
 *   sbt run
 *
 * (two main classes live in this project - pick this one, or run
 * `sbt "runMain day30.InsuranceClaimsStreamApp"` directly)
 *
 * Covers the streaming half of "batch and streaming where appropriate",
 * plus this component's own broadcast (policy->customer lookup),
 * accumulators (running fraud-alert counters), cache/persist
 * (validClaims), partition tuning (repartition after the single-receiver
 * socket source), Pair RDD + window operations (the burst detector), and
 * a periodic Spark SQL report built from the live running state.
 */
object InsuranceClaimsStreamApp {

  case class Claim(claimId: String, policyId: String, claimAmount: Double, claimType: String, timestamp: String)
  case class PolicyLookup(customerId: String, customerName: String, policyType: String, riskLevel: String)
  case class ClaimRow(claimId: String, policyId: String, policyType: String, riskLevel: String, claimAmount: Double)

  val policiesPath = "src/main/resources/policies.csv"
  val customersPath = "src/main/resources/customers.csv"

  val burstWindow = Seconds(20)
  val burstSlide = Seconds(4)
  val burstClaimThreshold = 4 // 4+ claims from one customer within the window -> fraud burst alert
  val reportEveryNBatches = 5

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day30-InsuranceClaimsStreamApp").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    spark.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint") // required by updateStateByKey

    explainOverview()

    // ---------------------------------------------------------------
    // Broadcast: policyId -> (customerId, customerName, policyType, riskLevel)
    // built once from the same two small reference files the batch app
    // uses, so a claim carrying only policyId can be resolved to its
    // customer without a shuffle.
    // ---------------------------------------------------------------
    val policyLookup = loadPolicyLookup()
    val policyLookupBC = spark.sparkContext.broadcast(policyLookup)

    // ---------------------------------------------------------------
    // Accumulators - true running totals for the app's whole life, unlike
    // any DStream-level count below (which resets every batch/window).
    // ---------------------------------------------------------------
    val totalClaimsAcc = spark.sparkContext.longAccumulator("totalClaimsSeen")
    val highValueClaimsAcc = spark.sparkContext.longAccumulator("highValueClaims")
    val fraudAlertsAcc = spark.sparkContext.longAccumulator("fraudBurstAlerts")
    val flaggedCustomersAcc = spark.sparkContext.collectionAccumulator[String]("flaggedCustomers")

    val lines = ssc.socketTextStream("localhost", 9999)
    val parsedOpt = lines.map(parseLine)
    val validClaims: DStream[Claim] = parsedOpt.flatMap(_.toList)

    // Cache: validClaims feeds THREE independent chains below (per-policy
    // running payout, the customer-level burst detector, and the periodic
    // SQL report) - same reuse-driven cache as every prior streaming day.
    validClaims.cache()

    explainPartitioning(validClaims)
    val repartitioned = validClaims.repartition(4)

    perPolicyRunningPayoutDemo(repartitioned, totalClaimsAcc, highValueClaimsAcc)
    customerBurstDetectionDemo(repartitioned, policyLookupBC, fraudAlertsAcc, flaggedCustomersAcc)
    periodicSqlReportDemo(spark, repartitioned, policyLookupBC, totalClaimsAcc, highValueClaimsAcc, fraudAlertsAcc, flaggedCustomersAcc)

    explainStreamingDagAndYarnNotes()

    ssc.start()
    ssc.awaitTermination()
  }

  def explainOverview(): Unit = {
    println(
      """================================================================
        | Day 30 - Insurance Capstone (STREAMING component)
        | live claim feed (socket)
        |   -> parseLine -> Option[Claim] -> validClaims (cached)
        |   -> repartition(4) [single-receiver partition fix]
        |   -> [per-policy running payout: updateStateByKey]
        |   -> [customer burst detector: broadcast lookup + Pair RDD +
        |        reduceByKeyAndWindow]
        |   -> [periodic Spark SQL report, every 5th batch]
        |================================================================
        |""".stripMargin)
  }

  def parseLine(line: String): Option[Claim] = {
    if (line == null || line.trim.isEmpty) return None
    val parts = line.split(",", -1)
    if (parts.length != 5) return None
    val Array(claimId, policyId, amountS, claimType, timestamp) = parts
    if (claimId.trim.isEmpty || policyId.trim.isEmpty) return None
    try {
      val amount = amountS.toDouble
      if (amount <= 0) None
      else Some(Claim(claimId, policyId, amount, claimType, timestamp))
    } catch {
      case _: NumberFormatException => None
    }
  }

  /** Builds policyId -> customer/policy info with plain Scala I/O, joining
    * policies.csv and customers.csv locally (no Spark needed for 280 and
    * 150 rows) before broadcasting the merged result as one small map. */
  def loadPolicyLookup(): Map[String, PolicyLookup] = {
    val customerNames: Map[String, String] = {
      val src = scala.io.Source.fromFile(customersPath)
      try {
        src.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>
          val parts = line.split(",", -1)
          parts(0) -> parts(1) // customerId -> name
        }.toMap
      } finally src.close()
    }

    val src = scala.io.Source.fromFile(policiesPath)
    try {
      src.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>
        val parts = line.split(",", -1)
        val policyId = parts(0)
        val customerId = parts(1)
        val policyType = parts(2)
        val riskLevel = parts(4)
        val customerName = customerNames.getOrElse(customerId, "Unknown")
        policyId -> PolicyLookup(customerId, customerName, policyType, riskLevel)
      }.toMap
    } finally src.close()
  }

  def explainPartitioning(validClaims: DStream[Claim]): Unit = {
    println(
      """[PARTITIONING] Same issue as every prior streaming day: socketTextStream's
        |one receiver means every micro-batch RDD starts with ONE partition.
        |repartition(4) below spreads the per-policy/per-customer work across more
        |partitions before the heavier stateful/windowed operations run.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------
  // Pair RDD / stateful: running claim count + payout per POLICY
  // ---------------------------------------------------------------
  def perPolicyRunningPayoutDemo(
      validClaims: DStream[Claim],
      totalClaimsAcc: org.apache.spark.util.LongAccumulator,
      highValueClaimsAcc: org.apache.spark.util.LongAccumulator
  ): Unit = {
    val deltas: DStream[(String, (Int, Double))] = validClaims.map { c =>
      totalClaimsAcc.add(1)
      if (c.claimAmount >= 100000.0) highValueClaimsAcc.add(1)
      (c.policyId, (1, c.claimAmount))
    }

    val updateRunning = (newDeltas: Seq[(Int, Double)], state: Option[(Int, Double)]) => {
      val (prevCount, prevSum) = state.getOrElse((0, 0.0))
      Some((prevCount + newDeltas.map(_._1).sum, prevSum + newDeltas.map(_._2).sum))
    }
    val runningByPolicy = deltas.updateStateByKey(updateRunning)

    runningByPolicy.foreachRDD { (rdd, time) =>
      val rows = rdd.collect()
      if (rows.nonEmpty) {
        val top = rows.sortBy { case (_, (_, sum)) => -sum }.take(5)
        println(f"\n[RUNNING PAYOUT] batch @ $time - top policies by running payout so far:")
        top.foreach { case (policyId, (count, sum)) =>
          println(f"  $policyId%-10s claims=$count%-4d runningPayout=₹$sum%.2f")
        }
      }
    }
  }

  // ---------------------------------------------------------------
  // Pair RDD + window: customer-level fraud burst detection
  // ---------------------------------------------------------------
  def customerBurstDetectionDemo(
      validClaims: DStream[Claim],
      policyLookupBC: org.apache.spark.broadcast.Broadcast[Map[String, PolicyLookup]],
      fraudAlertsAcc: org.apache.spark.util.LongAccumulator,
      flaggedCustomersAcc: org.apache.spark.util.CollectionAccumulator[String]
  ): Unit = {
    // Resolve policyId -> customerId via the broadcast lookup BEFORE
    // keying - this is what lets the window count claims per CUSTOMER
    // (the realistic fraud-ring unit) instead of per policy, even though
    // the wire event only ever carries a policyId.
    val claimsByCustomer: DStream[(String, Int)] = validClaims.map { c =>
      val customerId = policyLookupBC.value.get(c.policyId).map(_.customerId).getOrElse("UNKNOWN")
      (customerId, 1)
    }

    val burstCounts = claimsByCustomer.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b, (a: Int, b: Int) => a - b, burstWindow, burstSlide
    )

    burstCounts.foreachRDD { (rdd, time) =>
      val bursts = rdd.filter { case (_, count) => count >= burstClaimThreshold }
        .collect().sortBy { case (_, count) => -count }
      bursts.foreach { case (customerId, count) =>
        fraudAlertsAcc.add(1)
        flaggedCustomersAcc.add(customerId)
        val name = policyLookupBC.value.values.find(_.customerId == customerId).map(_.customerName).getOrElse("Unknown")
        val windowSeconds = burstWindow.milliseconds / 1000
        println(f"\n[FRAUD ALERT] batch @ $time - $customerId%-10s ($name) filed $count%d claims " +
          f"in the last $windowSeconds%d s")
      }
    }
  }

  // ---------------------------------------------------------------
  // Periodic Spark SQL report (same pattern as Day 28)
  // ---------------------------------------------------------------
  def periodicSqlReportDemo(
      spark: SparkSession,
      validClaims: DStream[Claim],
      policyLookupBC: org.apache.spark.broadcast.Broadcast[Map[String, PolicyLookup]],
      totalClaimsAcc: org.apache.spark.util.LongAccumulator,
      highValueClaimsAcc: org.apache.spark.util.LongAccumulator,
      fraudAlertsAcc: org.apache.spark.util.LongAccumulator,
      flaggedCustomersAcc: org.apache.spark.util.CollectionAccumulator[String]
  ): Unit = {
    var batchesSeen = 0

    validClaims.foreachRDD { (rdd, time) =>
      batchesSeen += 1
      if (batchesSeen % reportEveryNBatches == 0) {
        val rows = rdd.collect().map { c =>
          val info = policyLookupBC.value.getOrElse(c.policyId, PolicyLookup("UNKNOWN", "Unknown", "UNKNOWN", "MEDIUM"))
          ClaimRow(c.claimId, c.policyId, info.policyType, info.riskLevel, c.claimAmount)
        }
        if (rows.nonEmpty) {
          import spark.implicits._
          rows.toSeq.toDF().createOrReplaceTempView("live_claims_batch")
          println(f"\n[SQL REPORT] batch @ $time - this batch's claims by policy risk level:")
          spark.sql(
            """SELECT riskLevel, COUNT(*) AS claims, ROUND(SUM(claimAmount), 2) AS amount
              |FROM live_claims_batch GROUP BY riskLevel ORDER BY amount DESC""".stripMargin
          ).show(truncate = false)
        }
        println(f"[ACCUMULATOR SUMMARY] totalClaims=${totalClaimsAcc.value}%,d highValueClaims=${highValueClaimsAcc.value}%,d " +
          f"fraudAlerts=${fraudAlertsAcc.value}%,d flaggedCustomers=${flaggedCustomersAcc.value.asScala.distinct.mkString(",")}")
      }
    }
  }

  def explainStreamingDagAndYarnNotes(): Unit = {
    println(
      """[DAG/STAGES - streaming] Each micro-batch is its own small Spark job with its
        |own DAG, cut into stages the same way as the batch app (shuffle boundaries at
        |updateStateByKey and reduceByKeyAndWindow here) - visible under the Spark
        |UI's Streaming tab (batches) as well as its Jobs/Stages tabs (one job per
        |batch). See InsuranceBatchPipelineApp.explainDagLineageAndStages() for the
        |full mechanics, which apply identically inside each micro-batch here.
        |
        |[YARN - streaming-specific differences from the batch app] This app's driver
        |must run for the ENTIRE streaming job's lifetime (hours/days, not seconds),
        |so on YARN, --deploy-mode cluster matters far more here than for the batch
        |app. ssc.checkpoint("checkpoint") must point at HDFS once this is off a
        |single machine - every executor needs to read/write the SAME checkpoint
        |location. And the single socketTextStream receiver still caps ingestion
        |regardless of --num-executors, same caveat as every prior streaming day.
        |""".stripMargin)
  }
}
