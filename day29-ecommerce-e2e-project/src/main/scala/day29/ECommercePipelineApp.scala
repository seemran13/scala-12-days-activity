package day29

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import java.time.format.DateTimeParseException

/**
 * Day 29 — End-to-End E-Commerce Project
 *
 * Covers:
 *   1. A raw -> clean -> enrich -> aggregate pipeline, end to end
 *   2. RDD/Pair RDD for one component (the clean stage) and
 *      DataFrame/Dataset for another (everything from enrich onward)
 *   3. A UDF used only where it's genuinely necessary, with built-ins
 *      used everywhere else
 *   4. Joins (broadcast), window functions, partitioning, and persistence
 *   5. An explanation of every shuffle, stage, and optimization choice
 *   6. A production-style README and architecture diagram (see
 *      README.md and docs/architecture.png alongside this file)
 *
 * Capstone combining the RDD techniques from Days 1-12, the DataFrame/SQL
 * techniques from Days 13-22, and the Day 22 raw->clean->enrich->aggregate
 * shape, now done with explicit production-grade justification for every
 * design choice along the way, on a bigger and messier dataset with a
 * deliberately seeded data-skew scenario (one customer responsible for
 * ~12% of all orders).
 *
 * Run from this project's root directory:
 *
 *   sbt run
 *
 * No feeder/socket needed - this is a one-shot batch job, not a streaming
 * app. It reads src/main/resources/{raw_orders,customers,products}.csv,
 * prints every stage's output and explanation to the console, writes
 * output/enriched_orders/ (partitioned Parquet), then pauses for ~10s with
 * the Spark UI URL printed before exiting, in case you want to glance at
 * http://localhost:4040 while it's still up.
 */
object ECommercePipelineApp {

  case class Order(
      orderId: String,
      customerId: String,
      productId: String,
      quantity: Int,
      orderDate: String,
      status: String
  )

  val rawOrdersPath = "src/main/resources/raw_orders.csv"
  val customersPath = "src/main/resources/customers.csv"
  val productsPath = "src/main/resources/products.csv"
  val outputPath = "output/enriched_orders"

  def main(args: Array[String]): Unit = {
    explainPipelineOverview()

    val spark = SparkSession.builder().appName("Day29-ECommercePipelineApp").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // -----------------------------------------------------------------
    // Partitioning choice #1: shuffle partition count
    // -----------------------------------------------------------------
    // Spark's default spark.sql.shuffle.partitions is 200 - tuned for
    // cluster-scale data, not a 12K-row practice dataset. At this size,
    // 200-way shuffles mean up to 200 mostly-empty tasks per shuffle,
    // where scheduling overhead dwarfs actual compute. Spark 3.5's
    // Adaptive Query Execution (on by default: spark.sql.adaptive.enabled)
    // would eventually coalesce those tiny post-shuffle partitions back
    // down on its own, but setting this explicitly up front is clearer and
    // slightly cheaper since Spark never plans the excess partitions at
    // all. 8 is chosen to comfortably exceed local[2]'s 2 cores (so there's
    // still parallel work available) without going absurdly granular.
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    println("[CONFIG] spark.sql.shuffle.partitions set to 8 (default 200 is cluster-scale, not practice-scale)")

    // ===================================================================
    // RAW -> CLEAN  (RDD / Pair RDD component - requirement 2)
    // ===================================================================
    val cleanOrdersRDD: RDD[Order] = rawToCleanRDD(spark)
    val ordersDS: Dataset[Order] = cleanOrdersRDD.toDS()

    // ===================================================================
    // ENRICH  (DataFrame/Dataset component - requirement 2; joins + UDF -
    // requirements 3 and 4)
    // ===================================================================
    val (customersDF, productsDF) = loadReferenceData(spark)
    val enriched = enrichOrders(spark, ordersDS, customersDF, productsDF)

    println(
      """
        |[EXPLAIN] Physical plan for the enrichment join - look for
        |BroadcastHashJoin (NOT SortMergeJoin) and the absence of an
        |Exchange/shuffle on the orders side. Both customers.csv (200 rows)
        |and products.csv (60 rows) are small enough to broadcast whole to
        |every executor, so the large orders side never needs to be
        |shuffled or sorted for either join - only the two small reference
        |tables are sent over the network, once each, up front.
        |""".stripMargin)
    enriched.explain("formatted")

    // Persistence: enriched feeds the preview below PLUS three independent
    // downstream aggregations (monthly revenue, top products per category,
    // customer running totals) plus the final partitioned write - the same
    // "reused multiple times" shape as every earlier day's cache/persist
    // lesson (Day 12 RDD, Day 22 DataFrame, Day 26-28 DStream), now at full
    // pipeline scale. Persisting BEFORE the first action (the preview show
    // below) means the join only actually runs once - every reference
    // after this point reads from the cache. MEMORY_AND_DISK rather than
    // MEMORY_ONLY because a real production version of this dataset could
    // exceed executor memory - spilling the overflow to disk degrades
    // gracefully instead of silently recomputing from scratch (MEMORY_ONLY
    // would just drop the excess and recompute it on next use).
    enriched.persist(StorageLevel.MEMORY_AND_DISK)
    val enrichedCount = enriched.count() // first action - materializes the persist
    println(f"[PERSIST] enriched DataFrame cached (MEMORY_AND_DISK) - $enrichedCount%,d rows materialized")
    cleanOrdersRDD.unpersist() // no longer needed once enriched is cached

    println("\n[PREVIEW] enriched sample - note the normalized city (UDF) and valueTier (built-in when/otherwise) columns:")
    enriched.select("orderId", "customerName", "city", "productName", "category", "revenue", "valueTier", "status")
      .show(10, truncate = false)

    // ===================================================================
    // AGGREGATE  (DataFrame + window functions - requirement 4)
    // ===================================================================
    aggregateMonthlyRevenue(enriched)
    topProductsPerCategory(enriched)
    customerRunningTotal(enriched)

    // ===================================================================
    // Shuffle/stage/optimization explanation - requirement 5
    // ===================================================================
    explainShufflesAndStages()

    writePartitionedOutput(enriched)

    enriched.unpersist()

    println(
      """
        |[DONE] Pipeline complete. Spark UI was available at
        |http://localhost:4040 while this app ran - pausing ~10s before
        |exit in case you want to switch tabs and look at the Jobs/Stages
        |view (you'll see the dedup shuffle, the two broadcast joins with
        |no shuffle, the two aggregation shuffles, and the two window-
        |function shuffles as separate stages).
        |""".stripMargin)
    Thread.sleep(10000)
    spark.stop()
  }

  def explainPipelineOverview(): Unit = {
    println(
      """================================================================
        | Day 29 - End-to-End E-Commerce Pipeline
        | See docs/architecture.png for the full diagram. Stages:
        |
        |   raw_orders.csv (RDD, text lines)
        |        |  parseLine (validate) -> Option[Order]
        |        v
        |   Pair RDD: (orderId -> Order) .reduceByKey(keep first)   [dedup]
        |        |  .toDS()
        |        v
        |   Dataset[Order]
        |        |  broadcast join customers (+ normalizeCity UDF)
        |        |  broadcast join products
        |        |  when/otherwise value-tier column (no UDF needed)
        |        v
        |   enriched DataFrame  --persist(MEMORY_AND_DISK)--
        |        |-> monthly revenue (groupBy + sum)
        |        |-> top-3 products per category (Window + dense_rank)
        |        |-> customer running total (Window + rowsBetween)
        |        |-> partitioned Parquet write (output/enriched_orders)
        |================================================================
        |""".stripMargin)
  }

  // ===================================================================
  // RAW -> CLEAN (RDD / Pair RDD)
  // ===================================================================

  /** Parses one raw CSV line into an Order, or None if it fails validation.
    * Deliberately a broad catch here (NumberFormatException AND
    * DateTimeParseException) since this is a parsing boundary meant to
    * fail safe and drop the line, the same defensive shape as every
    * parseLine in Days 24-28. */
  def parseLine(line: String): Option[Order] = {
    val parts = line.split(",", -1)
    if (parts.length != 6) return None

    val Array(orderId, customerId, productId, quantityS, orderDate, status) = parts
    if (orderId.trim.isEmpty || customerId.trim.isEmpty || productId.trim.isEmpty) return None

    val validStatuses = Set("COMPLETED", "CANCELLED", "RETURNED")
    if (!validStatuses.contains(status)) return None

    try {
      val quantity = quantityS.toInt
      if (quantity <= 0) return None
      LocalDate.parse(orderDate) // throws DateTimeParseException on "2026/13/40" etc.
      Some(Order(orderId, customerId, productId, quantity, orderDate, status))
    } catch {
      case _: NumberFormatException => None
      case _: DateTimeParseException => None
    }
  }

  /** RAW -> CLEAN, built on the RDD API (requirement 2's RDD/Pair RDD
    * component). Parses, validates, and de-duplicates by orderId using an
    * explicit Pair RDD + reduceByKey - a real shuffle, explained in
    * explainShufflesAndStages(). */
  def rawToCleanRDD(spark: SparkSession): RDD[Order] = {
    val sc = spark.sparkContext
    val rawLines = sc.textFile(rawOrdersPath)
    val header = rawLines.first()
    val dataLines = rawLines.filter(_ != header)
    dataLines.persist(StorageLevel.MEMORY_ONLY) // reused by the count below AND by parsing

    val totalRaw = dataLines.count()

    val parsed: RDD[Option[Order]] = dataLines.map(parseLine)
    parsed.persist(StorageLevel.MEMORY_ONLY) // reused by the valid-count AND the flatMap below
    val totalValid = parsed.filter(_.isDefined).count()

    val validOrders: RDD[Order] = parsed.flatMap(_.toList)

    // Pair RDD: key every order by its orderId, then reduceByKey to keep
    // only the first occurrence of each key - this is the Pair RDD
    // component requirement, applied to a genuine problem in the seeded
    // data (raw_orders.csv has ~1% exact-duplicate orderId rows).
    // reduceByKey((a, _) => a) is a real shuffle: every order gets
    // hash-partitioned by orderId so all copies of the same orderId land
    // on the same task, where the reduce function discards every copy
    // after the first.
    val keyed: RDD[(String, Order)] = validOrders.map(o => (o.orderId, o))
    val deduped: RDD[(String, Order)] = keyed.reduceByKey((first, _) => first)
    val cleanOrdersRDD: RDD[Order] = deduped.values
    cleanOrdersRDD.persist(StorageLevel.MEMORY_ONLY)
    val totalClean = cleanOrdersRDD.count()

    println(
      f"""
        |[CLEAN] raw lines (excl. header): $totalRaw%,d
        |[CLEAN] valid after parseLine validation: $totalValid%,d (dropped ${totalRaw - totalValid}%,d malformed/invalid)
        |[CLEAN] unique after Pair RDD dedup by orderId: $totalClean%,d (dropped ${totalValid - totalClean}%,d duplicate orderIds)
        |""".stripMargin)

    parsed.unpersist()
    dataLines.unpersist()
    cleanOrdersRDD
  }

  // ===================================================================
  // ENRICH (DataFrame/Dataset, broadcast joins, UDF where necessary)
  // ===================================================================

  def loadReferenceData(spark: SparkSession): (DataFrame, DataFrame) = {
    val customersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(customersPath)
    val productsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(productsPath)
    (customersDF, productsDF)
  }

  /** Maps a messy, free-text city spelling to its canonical form - a plain
    * Scala function so it's independently unit-testable outside of Spark
    * entirely, then wrapped as a UDF below. */
  def normalizeCity(raw: String): String = {
    if (raw == null) return "Unknown"
    val cleaned = raw.trim.toLowerCase
    val canonical: Map[String, String] = Map(
      "bengaluru" -> "Bengaluru", "bangalore" -> "Bengaluru", "banglore" -> "Bengaluru", "blr" -> "Bengaluru",
      "mumbai" -> "Mumbai", "bombay" -> "Mumbai", "mum" -> "Mumbai",
      "delhi" -> "Delhi", "new delhi" -> "Delhi", "del" -> "Delhi",
      "chennai" -> "Chennai", "madras" -> "Chennai",
      "pune" -> "Pune",
      "hyderabad" -> "Hyderabad", "hyd" -> "Hyderabad",
      "kolkata" -> "Kolkata", "calcutta" -> "Kolkata",
      "jaipur" -> "Jaipur", "jai" -> "Jaipur"
    )
    canonical.getOrElse(cleaned, raw.trim)
  }

  /** Joins orders with customers and products, enriches with a normalized
    * city (UDF - requirement 3) and a value tier (built-in when/otherwise
    * - deliberately NOT a UDF, for contrast). */
  def enrichOrders(spark: SparkSession, ordersDS: Dataset[Order], customersDF: DataFrame, productsDF: DataFrame): DataFrame = {
    // ---------------------------------------------------------------
    // UDF - used here ONLY because no built-in expression can do an
    // arbitrary string->string lookup across 20+ real-world spelling
    // variants ("Bangalore"/"banglore"/"BLR"/"bengaluru " etc.) without
    // a when/otherwise chain that duplicates the same lookup table as a
    // long, unreusable, hard-to-test expression tree. As a plain Scala
    // Map lookup it's trivial to unit test in isolation.
    //
    // The real cost of a UDF: Catalyst's optimizer treats it as an opaque
    // black box - no predicate pushdown, no whole-stage codegen fusion
    // through that column, just a row-by-row JVM call. That cost is
    // negligible here because it only ever runs over customers.csv (200
    // rows, a DIMENSION table) - not over raw_orders (12K+ rows, the FACT
    // table). Applying the same UDF per-row across a large fact table
    // instead of a small dimension table is exactly the anti-pattern to
    // avoid - cardinality of where a UDF runs matters as much as whether
    // one is used at all.
    // ---------------------------------------------------------------
    val normalizeCityUDF = udf(normalizeCity _)

    val customersSelected = customersDF
      .withColumn("cityNormalized", normalizeCityUDF(col("city")))
      .select(col("customerId"), col("name").as("customerName"), col("cityNormalized").as("city"))

    val productsSelected = productsDF
      .select(
        col("productId"),
        col("name").as("productName"),
        col("category"),
        col("price").cast("double").as("price")
      )

    // Both reference tables are small (200 and 60 rows) - broadcast(...)
    // forces a BroadcastHashJoin instead of letting the planner choose a
    // SortMergeJoin, which would shuffle AND sort the large orders side on
    // the join key for no benefit here (see the formatted explain plan
    // printed in main, and explainShufflesAndStages()).
    val enriched = ordersDS
      .join(broadcast(customersSelected), Seq("customerId"), "left")
      .join(broadcast(productsSelected), Seq("productId"), "left")
      .withColumn("customerName", coalesce(col("customerName"), lit("Unknown Customer")))
      .withColumn("city", coalesce(col("city"), lit("Unknown City")))
      .withColumn("productName", coalesce(col("productName"), lit("Unknown Product")))
      .withColumn("category", coalesce(col("category"), lit("Unknown Category")))
      .withColumn("price", coalesce(col("price"), lit(0.0)))
      .withColumn("revenue", round(col("quantity") * col("price"), 2))
      // Built-in when/otherwise, NOT a UDF - a fixed set of numeric
      // thresholds is exactly what Catalyst's expression tree already
      // handles natively (and can still push down/fuse through), so
      // reaching for a UDF here would be the unnecessary kind this
      // requirement warns against.
      .withColumn(
        "valueTier",
        when(col("revenue") >= 15000, "PREMIUM")
          .when(col("revenue") >= 5000, "HIGH")
          .when(col("revenue") >= 1000, "MEDIUM")
          .otherwise("LOW")
      )

    enriched
  }

  // ===================================================================
  // AGGREGATE (DataFrame + window functions)
  // ===================================================================

  def aggregateMonthlyRevenue(enriched: DataFrame): Unit = {
    // groupBy shuffle: Exchange hashpartitioning(orderMonth), with a
    // partial HashAggregate BEFORE the shuffle and a final HashAggregate
    // after - see explainShufflesAndStages().
    val monthly = enriched
      .filter(col("status") === "COMPLETED") // cancelled/returned orders shouldn't count as revenue
      .withColumn("orderMonth", substring(col("orderDate"), 1, 7))
      .groupBy("orderMonth")
      .agg(round(sum("revenue"), 2).as("totalRevenue"), count(lit(1)).as("orders"))
      .orderBy("orderMonth")

    println("\n[EXPLAIN] Physical plan for monthly revenue aggregation - look for two HashAggregate nodes around one Exchange:")
    monthly.explain("formatted")

    println("\n[AGGREGATE] Monthly revenue (COMPLETED orders only):")
    monthly.show(24, truncate = false)
  }

  def topProductsPerCategory(enriched: DataFrame): Unit = {
    val productRevenue = enriched
      .filter(col("status") === "COMPLETED")
      .groupBy("category", "productId", "productName")
      .agg(round(sum("revenue"), 2).as("totalRevenue"))

    // Window function: Exchange hashpartitioning(category) + Sort within
    // each partition by totalRevenue, THEN dense_rank evaluated over each
    // sorted partition - see explainShufflesAndStages() for why this can't
    // get the groupBy's partial/final shuffle-reduction trick.
    val byCategory = Window.partitionBy("category").orderBy(col("totalRevenue").desc)
    val ranked = productRevenue
      .withColumn("rankInCategory", dense_rank().over(byCategory))
      .filter(col("rankInCategory") <= 3)
      .orderBy("category", "rankInCategory")

    println("\n[EXPLAIN] Physical plan for top-N per category - look for Exchange hashpartitioning(category), a Sort, then WindowExec:")
    ranked.explain("formatted")

    println("\n[AGGREGATE] Top 3 products by revenue, per category (window function: dense_rank):")
    ranked.show(100, truncate = false)
  }

  def customerRunningTotal(enriched: DataFrame): Unit = {
    // rowsBetween(unboundedPreceding, currentRow) is what makes this a
    // RUNNING total rather than a per-customer grand total - every prior
    // row (by orderDate, within the same customerId partition) is summed
    // into each row's result.
    val byCustomerDate = Window.partitionBy("customerId").orderBy("orderDate")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val withRunning = enriched
      .filter(col("status") === "COMPLETED")
      .withColumn("runningTotal", round(sum("revenue").over(byCustomerDate), 2))

    println(
      """
        |[EXPLAIN] Physical plan for the running total - same Exchange +
        |Sort + WindowExec shape as above, but partitioned by customerId.
        |CUST0001 was deliberately seeded with ~12% of ALL orders (data
        |skew) - every one of its ~1,400+ rows must land on the SAME task
        |(a window partition can't be split across tasks, since the running
        |total needs every earlier row in order), while most other
        |customers have a few dozen rows at most. That one task becomes a
        |straggler - see explainShufflesAndStages() for why Spark 3.5's
        |adaptive skew-join handling does NOT help here.
        |""".stripMargin)
    withRunning.explain("formatted")

    println("\n[AGGREGATE] Running revenue total per customer over time (sample: skewed CUST0001 vs two regular customers):")
    withRunning.filter(col("customerId").isin("CUST0001", "CUST0002", "CUST0003"))
      .select("customerId", "orderDate", "revenue", "runningTotal")
      .orderBy("customerId", "orderDate")
      .show(40, truncate = false)
  }

  // ===================================================================
  // Write (partitioning for output)
  // ===================================================================

  def writePartitionedOutput(enriched: DataFrame): Unit = {
    println(
      """
        |[WRITE] Writing enriched orders to output/enriched_orders,
        |partitioned by category (Day 20's file-layout technique, reused).
        |
        |repartition(col("category")) BEFORE the partitioned write is
        |deliberate: it hash-partitions the DataFrame by category first, so
        |all of one category's rows land in the same task before writing.
        |Without it, Spark would write up to one file per category PER
        |existing shuffle partition (8, from spark.sql.shuffle.partitions
        |above) - as many as 8 small files in every category= folder
        |instead of close to 1. Same small-files lesson as Day 20, reached
        |here via repartition(col) rather than coalesce, since this also
        |changes HOW rows are grouped, not just how many files come out.
        |""".stripMargin)

    enriched.repartition(col("category"))
      .write.mode("overwrite")
      .partitionBy("category")
      .parquet(outputPath)

    println(s"[WRITE] done - see $outputPath/category=.../*.parquet")
  }

  // ===================================================================
  // Requirement 5: explain every shuffle, stage, and optimization choice
  // ===================================================================

  def explainShufflesAndStages(): Unit = {
    println(
      """
        |================================================================
        | Shuffles, stages, and optimization choices in this pipeline
        |================================================================
        |
        |1. RDD dedup (reduceByKey on orderId) - ONE shuffle.
        |   Exchange hashpartitioning(orderId). Every order is redistributed
        |   across partitions by orderId so duplicates of the same orderId
        |   co-locate on one task, where reduceByKey's combiner keeps only
        |   the first copy. reduceByKey (unlike groupByKey) combines
        |   locally on each partition BEFORE shuffling, so only one Order
        |   per (partition, orderId) pair crosses the network, not every
        |   duplicate.
        |
        |2. Enrichment joins (customers, products) - ZERO shuffles.
        |   broadcast(...) forces a BroadcastHashJoin: the whole small
        |   table (200 rows, then 60 rows) is sent to every executor once,
        |   and each order is matched locally, in-memory, against that copy.
        |   The alternative, a SortMergeJoin, would shuffle AND sort BOTH
        |   sides on the join key - wasteful here since the orders side is
        |   the only side with real volume and doesn't need shuffling at
        |   all if the small side is just broadcast instead.
        |
        |3. Monthly revenue groupBy - ONE shuffle, but with a crucial
        |   optimization: a PARTIAL HashAggregate runs on each partition
        |   BEFORE the Exchange (summing revenue per orderMonth locally
        |   first), then the Exchange hash-partitions by orderMonth, then a
        |   FINAL HashAggregate combines the partial sums. Only one partial
        |   sum per (partition, orderMonth) crosses the network - the same
        |   local-combine-before-shuffle trick as reduceByKey above, just
        |   automatic under the DataFrame API.
        |
        |4. Top-N per category (window function) - ONE shuffle, but NO
        |   partial/final split. Exchange hashpartitioning(category), then
        |   a Sort within each partition by totalRevenue, then WindowExec
        |   evaluates dense_rank() over each fully-materialized, sorted
        |   partition. A window function can't pre-combine before the
        |   shuffle the way an aggregate can, because dense_rank (and sum
        |   with rowsBetween, below) needs to see every row of its
        |   partition, in order, to produce a correct per-row result.
        |
        |5. Customer running total (window function) - same shape as #4,
        |   partitioned by customerId instead of category - and this is
        |   where the seeded data skew bites. CUST0001 has ~12% of all
        |   orders; its entire partition (~1,400+ rows) must land on ONE
        |   task, while most customers have a few dozen rows. That task
        |   runs far longer than its siblings - a straggler that caps the
        |   whole stage's wall-clock time on the slowest task, no matter
        |   how many cores are free elsewhere.
        |
        |   Spark 3.5's Adaptive Query Execution (spark.sql.adaptive.enabled,
        |   on by default) DOES handle a comparable skew problem - but only
        |   for sort-merge JOINS (spark.sql.adaptive.skewJoin.enabled), by
        |   splitting an oversized join partition into several smaller
        |   sub-tasks. It does NOT apply to a window function's partition
        |   shuffle, because a window computation (especially rowsBetween's
        |   order-dependent running total) genuinely needs its whole,
        |   correctly-ordered partition in one place - there's no
        |   equivalent "split and recombine" available. Real mitigations
        |   here would be salting the key (splitting CUST0001's own rows
        |   into sub-buckets, computing partial running totals, then
        |   stitching them back together in order) or simply accepting the
        |   skew as a known, bounded cost at this data size.
        |
        |6. spark.sql.shuffle.partitions = 8 (set explicitly in main, not
        |   left at the default 200) - at this dataset's scale (12K rows),
        |   200-way shuffles would mean up to 200 mostly-empty tasks per
        |   shuffle, where scheduling overhead would dwarf actual compute.
        |   AQE's coalescePartitions would eventually merge small post-
        |   shuffle partitions back down even at the default, but setting
        |   this explicitly avoids Spark planning the excess partitions in
        |   the first place.
        |
        |7. repartition(col("category")) before the partitioned Parquet
        |   write - a SEVENTH shuffle, deliberate, to avoid the small-files
        |   problem (see writePartitionedOutput's own explanation).
        |================================================================
        |""".stripMargin)
  }
}
