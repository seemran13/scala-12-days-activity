package day19

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Day 19 — Broadcast Join
 *
 * Covers:
 *   1. A large fact DataFrame and a small reference DataFrame
 *   2. Using broadcast join
 *   3. When broadcast join is appropriate
 *   4. Comparing with shuffle sort merge join - real timings and plans
 *   5. Scenario: joining millions of transactions with a small branch master
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 */
object BroadcastJoinApp {

  val transactionCount = 5000000L // "millions of transactions"

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day19-BroadcastJoinApp")
      .master(masterUrl)
      .getOrCreate()

    import spark.implicits._

    section("1. A large fact DataFrame and a small reference DataFrame")
    val (transactions, branches) = buildData(spark)

    section("2. Using broadcast join")
    broadcastJoinDemo(transactions, branches)

    section("3. When is a broadcast join appropriate?")
    explainWhenAppropriate(spark)

    section("4. Broadcast join vs shuffle sort merge join - measured")
    compareWithSortMergeJoin(spark, transactions, branches)

    section("5. Scenario: millions of transactions joined with the branch master")
    scenarioReport(transactions, branches)

    spark.stop()
  }

  // ---------------------------------------------------------------------
  // 1. Build the large fact table and the small reference table
  // ---------------------------------------------------------------------
  def buildData(spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._

    // The small reference ("dimension") table: 25 branches, loaded from a
    // tiny CSV. This is the classic shape a broadcast join is built for -
    // a handful of KB, easily held in memory on every executor.
    val branches = spark.read
      .option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/branch_master.csv")

    // The large fact table: millions of transactions, generated with
    // spark.range so we don't need to ship a multi-GB CSV file around -
    // this is itself a realistic, DISTRIBUTED way to produce large
    // synthetic data, computed in parallel across partitions.
    val transactions = spark.range(0, transactionCount)
      .withColumn("txnId", concat(lit("T"), col("id")))
      .withColumn("branchId", (col("id") % 25).cast("int"))
      .withColumn("amount", round(rand(seed = 42) * 9000 + 50, 2))
      .withColumn("dayOfYear", (col("id") % 365).cast("int") + 1)
      .drop("id")

    val branchSizeEstimate = branches.count() * 60 // rough bytes/row guess, just for illustration
    println(s"Fact table (transactions): $transactionCount rows")
    println(s"Reference table (branches): ${branches.count()} rows (~${branchSizeEstimate} bytes - trivially small)")
    transactions.show(5, truncate = false)
    branches.show(5, truncate = false)

    (transactions, branches)
  }

  // ---------------------------------------------------------------------
  // 2. Broadcast join
  // ---------------------------------------------------------------------
  def broadcastJoinDemo(transactions: DataFrame, branches: DataFrame): Unit = {
    // broadcast(...) is a HINT: it tells Spark "send this DataFrame's full
    // contents to every executor and build a hash table there", instead of
    // shuffling the (large) transactions side at all.
    val joined = transactions.join(broadcast(branches), Seq("branchId"), "inner")

    println("Physical plan for the broadcast join - look for BroadcastHashJoin / BroadcastExchange:")
    joined.explain()

    println("\nSample joined rows:")
    joined.select("txnId", "branchId", "branchName", "city", "region", "amount").show(5, truncate = false)

    val t0 = System.nanoTime()
    val total = joined.count()
    val elapsed = (System.nanoTime() - t0) / 1e6
    println(f"\nJoined row count: $total%,d (matches transaction count: every branchId has a match)")
    println(f"count() took $elapsed%.0f ms")
  }

  // ---------------------------------------------------------------------
  // 3. When is a broadcast join appropriate?
  // ---------------------------------------------------------------------
  def explainWhenAppropriate(spark: SparkSession): Unit = {
    val thresholdBytes = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    println(
      s"""Broadcast join is a good fit when:
        | - One side is SMALL relative to executor memory - a dimension/reference
        |   table (branch master, country codes, a product catalog), typically
        |   from a handful of KB up to tens or low hundreds of MB.
        | - That small side is used repeatedly against a much larger fact table -
        |   exactly the classic star-schema fact/dimension shape.
        | - You want to AVOID shuffling the large side entirely - broadcast is
        |   the one join strategy where the big table's data never moves; only
        |   the small table is copied, once, to every executor.
        |
        |Spark auto-picks a broadcast join whenever one side's estimated size is
        |under spark.sql.autoBroadcastJoinThreshold (currently: $thresholdBytes bytes,
        |10MB by default). The broadcast(...) hint FORCES it even if Spark's own
        |size estimate would have said no - useful when you know a table is small
        |but Spark's statistics are stale or unavailable (e.g. after heavy filtering).
        |
        |When it is NOT appropriate:
        | - The "small" side isn't actually small. The ENTIRE table is collected
        |   to the driver first, then sent to every executor - too large and you
        |   risk driver OOM and heavy network cost, wiping out any benefit.
        | - Both sides are large. There's no small side to broadcast; a shuffle of
        |   SOME kind is unavoidable, so sort merge join is the right default.
        | - The join is a right/full outer join where the broadcast side is the
        |   one that can have unmatched rows produced per-partition in a way that
        |   needs care - Spark still supports this, but it's worth knowing
        |   broadcast works most naturally for inner/left joins keyed off the
        |   large side.
        | - You're already memory-constrained on executors - broadcasting adds a
        |   full copy of the small table into every executor's memory, on top of
        |   whatever else is running there.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 4. Broadcast vs shuffle sort merge join - measured
  // ---------------------------------------------------------------------
  def compareWithSortMergeJoin(spark: SparkSession, transactions: DataFrame, branches: DataFrame): Unit = {
    def timeMs[T](body: => T): Double = {
      val t0 = System.nanoTime(); body; (System.nanoTime() - t0) / 1e6
    }

    println("Same join, same 5,000,000-row fact table, two different physical strategies:\n")

    val tBroadcast = timeMs {
      transactions.join(broadcast(branches), Seq("branchId"), "inner").count()
    }

    println("Forcing a shuffle sort merge join (disabling auto-broadcast, no broadcast() hint):")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val tSortMerge = timeMs {
      transactions.join(branches, Seq("branchId"), "inner").count()
    }
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // restore 10MB default

    println(f"\nBroadcast hash join : $tBroadcast%8.0f ms")
    println(f"Shuffle sort merge   : $tSortMerge%8.0f ms")
    println(
      """
        |The sort merge version has to SHUFFLE all 5,000,000 fact rows across
        |the network (by branchId) before it can even start matching, plus sort
        |every partition - real, heavy work the broadcast version skips entirely,
        |since only the tiny 25-row branch table ever moves. On a single local
        |machine with fast local disk/memory the gap can be modest; on a real
        |multi-node cluster, where "shuffle" means actual network transfer
        |between machines, the gap is typically much larger. Either way, the
        |PLAN difference (Exchange + Sort + SortMergeJoin vs BroadcastExchange +
        |BroadcastHashJoin) is the structural, always-true signal.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: millions of transactions joined with the branch master
  // ---------------------------------------------------------------------
  def scenarioReport(transactions: DataFrame, branches: DataFrame): Unit = {
    val enriched = transactions.join(broadcast(branches), Seq("branchId"), "inner")

    println("Report 1 - Revenue by region:")
    enriched.groupBy("region")
      .agg(count("*").as("txnCount"), round(sum("amount"), 2).as("revenue"))
      .orderBy(desc("revenue"))
      .show(truncate = false)

    println("Report 2 - Top 10 branches by revenue:")
    enriched.groupBy("branchId", "branchName", "city")
      .agg(count("*").as("txnCount"), round(sum("amount"), 2).as("revenue"))
      .orderBy(desc("revenue"))
      .show(10, truncate = false)

    println("Report 3 - Average transaction value by city:")
    enriched.groupBy("city")
      .agg(round(avg("amount"), 2).as("avgTxnValue"), count("*").as("txnCount"))
      .orderBy(desc("avgTxnValue"))
      .show(truncate = false)

    val totalRevenue = enriched.agg(round(sum("amount"), 2).as("t")).first().getAs[Double]("t")
    println(f"\nTotal revenue across all $transactionCount%,d transactions: Rs. $totalRevenue%,.2f")
    println("All of this ran as ONE broadcast join against the 25-row branch master -")
    println("the 5,000,000-row fact table never had to be shuffled across the network.")
  }

  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }
}
