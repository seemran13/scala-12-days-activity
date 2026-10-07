package day22

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.io.File

/**
 * Day 22 — Batch Mini Project
 *
 * An end-to-end batch pipeline, built from everything covered in days 13-21:
 *   1. Read raw transactions                 (extract)
 *   2. Clean invalid records                  (validate)
 *   3. Join customer/product data             (enrich)
 *   4. Aggregate revenue                      (transform)
 *   5. Write partitioned Parquet output       (load)
 *
 * Scenario: an e-commerce daily sales pipeline - exactly the shape of job
 * that runs every night in a real company: pull yesterday's raw transaction
 * feed, throw out what's broken, enrich what's left with reference data,
 * summarize it, and land it somewhere analysts and dashboards can read from.
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 */
object ECommercePipelineApp {

  val outputRoot = "output"

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day22-ECommercePipelineApp")
      .master(masterUrl)
      .getOrCreate()

    deleteRecursively(new File(outputRoot))

    section("STAGE 1 — Read raw transactions")
    val rawTransactions = extractRawTransactions(spark)

    section("STAGE 2 — Clean invalid records")
    val cleanedTransactions = cleanTransactions(spark, rawTransactions)

    section("STAGE 3 — Join customer/product data")
    val enriched = joinReferenceData(spark, cleanedTransactions)

    section("STAGE 4 — Aggregate revenue")
    val dailyRevenue = aggregateRevenue(enriched)

    section("STAGE 5 — Write partitioned Parquet output")
    writePartitionedOutput(dailyRevenue, enriched)

    section("Scenario summary — E-commerce daily sales pipeline")
    pipelineSummary(rawTransactions.count())

    spark.stop()
  }

  // ---------------------------------------------------------------------
  // 1. Read raw transactions
  // ---------------------------------------------------------------------
  def extractRawTransactions(spark: SparkSession): DataFrame = {
    // Everything is read as a String-friendly, loosely-typed frame on
    // purpose: a RAW feed shouldn't be trusted to already match a strict
    // schema (quantity could arrive as "3" or as garbage) - validation in
    // stage 2 is what decides what's usable, not inferSchema.
    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/raw_transactions.csv")

    println(s"Read ${raw.count()} raw transaction rows from the feed.")
    raw.printSchema()
    raw.show(5, truncate = false)
    raw
  }

  // ---------------------------------------------------------------------
  // 2. Clean invalid records
  // ---------------------------------------------------------------------
  def cleanTransactions(spark: SparkSession, raw: DataFrame): DataFrame = {
    // Parse the date defensively - to_date(...) returns NULL for anything
    // that doesn't match the given pattern, instead of throwing. That turns
    // "bad date format" into just another null check, same shape as every
    // other validation rule below.
    val parsed = raw.withColumn("parsedDate", to_date(col("transactionDate"), "yyyy-MM-dd"))

    // Tag every row with WHY it would be rejected, if at all. A row can only
    // have one first-match reason here (checked in priority order) - good
    // enough for a training pipeline; a production one might collect every
    // violated rule instead of just the first.
    val tagged = parsed.withColumn(
      "rejectReason",
      when(col("transactionId").isNull || trim(col("transactionId")) === "", "MISSING_TRANSACTION_ID")
        .when(col("customerId").isNull || trim(col("customerId")) === "", "MISSING_CUSTOMER_ID")
        .when(col("productId").isNull || trim(col("productId")) === "", "MISSING_PRODUCT_ID")
        .when(col("quantity").isNull || col("quantity") <= 0, "INVALID_QUANTITY")
        .when(col("parsedDate").isNull, "UNPARSEABLE_DATE")
        .otherwise(null)
    )

    // Duplicates (same transactionId appearing more than once - an upstream
    // retry resending the same event) are a SEPARATE kind of problem from
    // the per-row checks above: keep the first occurrence, flag the rest.
    val dupeWindow = org.apache.spark.sql.expressions.Window
      .partitionBy("transactionId").orderBy(monotonically_increasing_id())
    val withDupeFlag = tagged.withColumn("occurrence", row_number().over(dupeWindow))
    val fullyTagged = withDupeFlag.withColumn(
      "rejectReason",
      when(col("occurrence") > 1, "DUPLICATE_TRANSACTION_ID").otherwise(col("rejectReason"))
    ).drop("occurrence")

    val rejected = fullyTagged.filter(col("rejectReason").isNotNull)
    val valid = fullyTagged.filter(col("rejectReason").isNull)
      .select(
        col("transactionId"),
        col("parsedDate").as("transactionDate"),
        col("customerId"),
        col("productId"),
        col("quantity")
      )

    // Persist the rejects to disk too - a real pipeline never just silently
    // drops bad data, it quarantines it so someone can investigate the feed.
    rejected.select("transactionId", "transactionDate", "customerId", "productId", "quantity", "rejectReason")
      .write.mode("overwrite").option("header", "true")
      .csv(s"$outputRoot/rejected_transactions")

    println("Data-quality report - rejected row count by reason:")
    rejected.groupBy("rejectReason").count().orderBy(desc("count")).show(truncate = false)

    val rawCount = raw.count()
    val validCount = valid.count()
    val rejectedCount = rejected.count()
    println(f"Raw: $rawCount%,d  ->  Valid: $validCount%,d (${validCount * 100.0 / rawCount}%.1f%%)  " +
      f"Rejected: $rejectedCount%,d (${rejectedCount * 100.0 / rawCount}%.1f%%)")

    println("\nSample of cleaned, valid transactions:")
    valid.show(5, truncate = false)

    valid
  }

  // ---------------------------------------------------------------------
  // 3. Join customer/product data
  // ---------------------------------------------------------------------
  def joinReferenceData(spark: SparkSession, cleanTransactions: DataFrame): DataFrame = {
    val customers = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/customers.csv")
    val products = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/products.csv")

    println(s"Reference data: ${customers.count()} customers, ${products.count()} products " +
      "(both small enough to broadcast).")

    // Both reference tables are tiny (a handful of KB) next to the
    // transaction volume, so broadcast() avoids shuffling the transactions
    // at all - same technique as Day 19, applied here for a real reason
    // rather than as a standalone demo.
    val joined = cleanTransactions
      .join(broadcast(customers), Seq("customerId"), "left")
      .join(broadcast(products), Seq("productId"), "left")

    // A row surviving stage 2's validation can STILL fail to join - its
    // customerId/productId was well-formed but doesn't exist in the
    // reference tables (an orphan reference, same idea as Day 18's
    // nullHandlingDemo and Day 21's LEFT ANTI JOIN check). That's a second,
    // distinct kind of "bad data" a pipeline has to account for.
    val orphanCustomer = joined.filter(col("customerName").isNull)
    val orphanProduct = joined.filter(col("productName").isNull)
    println(s"Transactions referencing an unknown customerId: ${orphanCustomer.count()}")
    println(s"Transactions referencing an unknown productId : ${orphanProduct.count()}")

    orphanCustomer.select("transactionId", "transactionDate", "customerId", "productId")
      .write.mode("overwrite").option("header", "true")
      .csv(s"$outputRoot/orphan_customer_transactions")
    orphanProduct.select("transactionId", "transactionDate", "customerId", "productId")
      .write.mode("overwrite").option("header", "true")
      .csv(s"$outputRoot/orphan_product_transactions")

    // The "gold" dataset for everything downstream: only rows that matched
    // BOTH reference tables, with revenue computed now that unitPrice is
    // available. A null unitPrice (the two unpriced products seeded in the
    // data) would otherwise poison every downstream sum, so it's coalesced
    // to 0 right here, at the point the column first appears.
    val enriched = joined
      .filter(col("customerName").isNotNull && col("productName").isNotNull)
      .withColumn("unitPrice", coalesce(col("unitPrice"), lit(0.0)))
      .withColumn("revenue", round(col("quantity") * col("unitPrice"), 2))
      .withColumn("year", year(col("transactionDate")))
      .withColumn("month", month(col("transactionDate")))
      .withColumn("day", dayofmonth(col("transactionDate")))

    // This DataFrame feeds BOTH stage 4 (aggregation) and stage 5 (the
    // detail-level partitioned write) - cache it once so each of those
    // actions doesn't re-run the two joins and all of stage 2's work from
    // scratch (same lesson as Day 12's cache/persist project, applied here
    // because this pipeline actually has the reused-multiple-times shape).
    enriched.persist(StorageLevel.MEMORY_AND_DISK)
    val enrichedCount = enriched.count() // materialize the cache now, not lazily later
    println(s"\nEnriched, revenue-bearing transactions ready for aggregation: $enrichedCount")
    enriched.select("transactionId", "transactionDate", "customerName", "region",
      "productName", "category", "quantity", "unitPrice", "revenue").show(5, truncate = false)

    enriched
  }

  // ---------------------------------------------------------------------
  // 4. Aggregate revenue
  // ---------------------------------------------------------------------
  def aggregateRevenue(enriched: DataFrame): DataFrame = {
    val dailyRevenue = enriched.groupBy("year", "month", "day")
      .agg(
        round(sum("revenue"), 2).as("totalRevenue"),
        count("*").as("transactionCount"),
        countDistinct("customerId").as("distinctCustomers")
      )
      .orderBy("year", "month", "day")

    println("Daily revenue (first 10 days):")
    dailyRevenue.show(10, truncate = false)

    println("Revenue by category:")
    enriched.groupBy("category")
      .agg(round(sum("revenue"), 2).as("revenue"), count("*").as("transactions"))
      .orderBy(desc("revenue"))
      .show(truncate = false)

    println("Revenue by region:")
    enriched.groupBy("region")
      .agg(round(sum("revenue"), 2).as("revenue"), count("*").as("transactions"))
      .orderBy(desc("revenue"))
      .show(truncate = false)

    println("Top 10 products by revenue:")
    enriched.groupBy("productId", "productName", "category")
      .agg(round(sum("revenue"), 2).as("revenue"), sum("quantity").as("unitsSold"))
      .orderBy(desc("revenue"))
      .show(10, truncate = false)

    dailyRevenue
  }

  // ---------------------------------------------------------------------
  // 5. Write partitioned Parquet output
  // ---------------------------------------------------------------------
  def writePartitionedOutput(dailyRevenue: DataFrame, enriched: DataFrame): Unit = {
    // Detail-level fact table, partitioned by year/month/day - exactly
    // Day 20's pattern: repartition by the partition columns FIRST so each
    // leaf folder gets one clean part-file instead of fragmenting.
    val forWrite = enriched.repartition(col("year"), col("month"), col("day"))
    forWrite.write.mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(s"$outputRoot/sales_fact_partitioned")

    println("Detail fact table written, partitioned by year/month/day:")
    printTree(new File(s"$outputRoot/sales_fact_partitioned"), maxDepth = 3)

    // Daily summary table - much smaller, a handful of rows, a single file
    // is both correct and sufficient (coalesce(1) rather than repartition,
    // since there's no skew left to fix at this point, just a final tidy-up).
    dailyRevenue.coalesce(1).write.mode("overwrite")
      .parquet(s"$outputRoot/daily_revenue_summary")

    println("\nDaily summary table written as a single Parquet file (small result, one file is fine):")
    printTree(new File(s"$outputRoot/daily_revenue_summary"), maxDepth = 1)

    enriched.unpersist()
  }

  // ---------------------------------------------------------------------
  // Scenario summary
  // ---------------------------------------------------------------------
  def pipelineSummary(rawCount: Long): Unit = {
    println(
      f"""This is what a real nightly batch job looks like end to end:
        |
        |  raw_transactions.csv ($rawCount%,d rows)
        |        |
        |        v
        |  [STAGE 2] validate: null checks on id/customerId/productId, quantity > 0,
        |            parseable date, de-duplicate by transactionId
        |            -> output/rejected_transactions/   (quarantined, not deleted)
        |        |
        |        v
        |  [STAGE 3] enrich: broadcast join against customers.csv and products.csv
        |            -> output/orphan_customer_transactions/, orphan_product_transactions/
        |            -> cached in memory (reused by stages 4 AND 5)
        |        |
        |        +------------------------+
        |        |                        |
        |        v                        v
        |  [STAGE 4] aggregate       [STAGE 5] write detail fact table
        |  daily/category/region/    partitioned by year/month/day
        |  product revenue                 |
        |        |                         v
        |        v                  output/sales_fact_partitioned/
        |  output/daily_revenue_summary/
        |
        |Every stage's row count is printed as it happens (see above) - that's the
        |lightweight version of what a production pipeline would send to monitoring:
        |"how many rows went in, how many came out, how many were rejected and why".
        |""".stripMargin
    )
  }

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------
  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
    if (file.exists()) file.delete()
  }

  def printTree(dir: File, maxDepth: Int, depth: Int = 0, prefix: String = ""): Unit = {
    if (!dir.exists() || depth > maxDepth) return
    val entries = Option(dir.listFiles()).getOrElse(Array.empty).sortBy(_.getName)
    val shown = if (depth == maxDepth && entries.length > 4) entries.take(3) else entries
    shown.foreach { f =>
      val marker = if (f.isDirectory) "/" else ""
      println(s"$prefix${f.getName}$marker")
      if (f.isDirectory) printTree(f, maxDepth, depth + 1, prefix + "  ")
    }
    if (depth == maxDepth && entries.length > shown.length) {
      println(s"$prefix... (${entries.length - shown.length} more)")
    }
  }
}
