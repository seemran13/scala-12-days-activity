package day20

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.File

/**
 * Day 20 — File Formats and Output
 *
 * Covers:
 *   1. Reading/writing CSV, JSON and Parquet
 *   2. Writing partitioned output
 *   3. File layout and number of output files, explained
 *   4. repartition() before writing, practiced
 *   5. Scenario: daily sales stored partitioned by year/month/day
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 *
 * Everything is written under ./output - safe to delete and re-run any time.
 */
object FileFormatsApp {

  val outputRoot = "output"

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day20-FileFormatsApp")
      .master(masterUrl)
      .getOrCreate()

    import spark.implicits._

    // Start clean so every run's file counts/layout are easy to reason about.
    deleteRecursively(new File(outputRoot))

    val salesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/daily_sales.csv")

    println(s"Loaded ${salesDf.count()} daily sales rows, default parallelism = ${spark.sparkContext.defaultParallelism}")
    println(s"Input DataFrame has ${salesDf.rdd.getNumPartitions} partitions right after reading the single CSV file.")
    salesDf.show(5, truncate = false)

    section("1. Reading and writing CSV, JSON and Parquet")
    readWriteFormatsDemo(spark, salesDf)

    section("2. Writing partitioned output")
    partitionedOutputDemo(salesDf)

    section("3. File layout and number of output files, explained")
    explainFileLayout(spark)

    section("4. Practicing repartition() before writing")
    repartitionBeforeWriteDemo(salesDf)

    section("5. Scenario: daily sales partitioned by year/month/day")
    scenarioDailySalesPartitioned(spark, salesDf)

    spark.stop()
  }

  // ---------------------------------------------------------------------
  // 1. CSV / JSON / Parquet round trip
  // ---------------------------------------------------------------------
  def readWriteFormatsDemo(spark: SparkSession, salesDf: DataFrame): Unit = {
    // Trim down to a few columns and a small slice just for this demo, so the
    // three formats below are quick to inspect side by side.
    val sample = salesDf.select("saleId", "year", "month", "day", "region", "product", "amount")
      .filter(col("year") === 2026 && col("month") === 1)

    println(s"Writing ${sample.count()} rows (Jan 2026 only) out as CSV, JSON and Parquet for comparison.")

    sample.write.mode("overwrite")
      .option("header", "true")
      .csv(s"$outputRoot/formats/csv")

    sample.write.mode("overwrite")
      .json(s"$outputRoot/formats/json")

    sample.write.mode("overwrite")
      .parquet(s"$outputRoot/formats/parquet")

    println("\nCSV   - plain text, one header row, human-readable, no embedded schema -")
    println("        every reader must be TOLD the schema (we used inferSchema/header options).")
    val csvBack = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$outputRoot/formats/csv")
    csvBack.printSchema()

    println("JSON  - self-describing (field names on every line) but still text -")
    println("        larger on disk than CSV/Parquet for the same data, no schema needed to read it back.")
    val jsonBack = spark.read.json(s"$outputRoot/formats/json")
    jsonBack.printSchema()

    println("Parquet - binary, COLUMNAR, schema travels WITH the file (no inferSchema needed),")
    println("          compressed by default, and supports predicate/column pushdown - Spark can")
    println("          skip whole row groups or columns it doesn't need. This is why Parquet is the")
    println("          default choice for data that will be read back by Spark (or any columnar engine).")
    val parquetBack = spark.read.parquet(s"$outputRoot/formats/parquet")
    parquetBack.printSchema()

    val csvBytes = dirSizeBytes(new File(s"$outputRoot/formats/csv"))
    val jsonBytes = dirSizeBytes(new File(s"$outputRoot/formats/json"))
    val parquetBytes = dirSizeBytes(new File(s"$outputRoot/formats/parquet"))
    println(f"\nOn-disk size for the SAME ${sample.count()}%,d rows:")
    println(f"  CSV     : $csvBytes%,8d bytes")
    println(f"  JSON    : $jsonBytes%,8d bytes")
    println(f"  Parquet : $parquetBytes%,8d bytes (binary + compressed - usually the smallest)")
  }

  // ---------------------------------------------------------------------
  // 2. Partitioned output
  // ---------------------------------------------------------------------
  def partitionedOutputDemo(salesDf: DataFrame): Unit = {
    // partitionBy(...) does NOT change the DataFrame's in-memory partitions -
    // it changes how Spark lays OUT THE FILES, grouping rows into one
    // subdirectory per distinct value of the partition column(s).
    salesDf.write.mode("overwrite")
      .partitionBy("year")
      .parquet(s"$outputRoot/partitioned_by_year")

    println("Directory layout after .partitionBy(\"year\").parquet(...):")
    printTree(new File(s"$outputRoot/partitioned_by_year"), maxDepth = 2)

    println(
      """Notice: the "year" COLUMN IS GONE from the Parquet files themselves - its value is
        |encoded entirely in the directory name (year=2024, year=2025, year=2026). This is
        |"Hive-style partitioning". When Spark reads this layout back with spark.read.parquet(...),
        |it reconstructs the year column from the folder names automatically.
        |""".stripMargin)

    val readBack = SparkSession.active.read.parquet(s"$outputRoot/partitioned_by_year")
    readBack.printSchema()
    println("Filtering on the partition column lets Spark skip entire folders without reading them -")
    println("this is \"partition pruning\", and .explain() would show a PartitionFilters entry for it:")
    readBack.filter(col("year") === 2025).explain()
  }

  // ---------------------------------------------------------------------
  // 3. File layout and number of output files
  // ---------------------------------------------------------------------
  def explainFileLayout(spark: SparkSession): Unit = {
    println(
      """Spark writes ONE OUTPUT FILE PER IN-MEMORY PARTITION, per task that produces output -
        |not one file per DataFrame, and not one file total. If a DataFrame has 8 partitions
        |when .write is called, you get (up to) 8 part-files, one written by each task:
        |
        |    output/my_table/
        |      part-00000-<uuid>.snappy.parquet
        |      part-00001-<uuid>.snappy.parquet
        |      ... (one per partition)
        |      _SUCCESS                           <- empty marker file, written last,
        |                                             meaning "this write completed fully"
        |
        |With partitionBy(...), that rule applies SEPARATELY inside each partition subfolder:
        |each (year=.../month=.../day=...) directory gets its own set of part-files, one per
        |in-memory partition that happened to contain rows for that key. This is why a
        |partitionBy() write with many distinct keys and many input partitions can explode
        |into thousands of small files - the "small files problem" - which is expensive for
        |downstream readers (every file is a separate open/seek, especially painful on cloud
        |storage like S3).
        |
        |The fix is controlling the number of in-memory partitions BEFORE writing, with
        |repartition() or coalesce() (section 4) - ideally so each partition holds a healthy
        |amount of data (a common rule of thumb: aim for output files in the 128MB-1GB range,
        |not thousands of tiny files).
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 4. repartition() before writing
  // ---------------------------------------------------------------------
  def repartitionBeforeWriteDemo(salesDf: DataFrame): Unit = {
    println(s"salesDf currently has ${salesDf.rdd.getNumPartitions} in-memory partitions " +
      s"(inherited from reading one CSV file, so Spark just used its default parallelism).")

    // Write with NO repartition first - whatever partition count the DataFrame
    // already has is exactly how many part-files come out.
    salesDf.write.mode("overwrite").parquet(s"$outputRoot/no_repartition")
    val filesNoRepartition = countPartFiles(new File(s"$outputRoot/no_repartition"))
    println(s"Wrote with NO repartition call -> $filesNoRepartition part-file(s), " +
      s"matching the ${salesDf.rdd.getNumPartitions} in-memory partitions.")

    // repartition(n) does a FULL SHUFFLE to spread rows evenly across exactly
    // n partitions - more expensive than coalesce, but produces evenly-sized
    // output files even if the current partitioning is skewed.
    val repartitioned = salesDf.repartition(4)
    repartitioned.write.mode("overwrite").parquet(s"$outputRoot/repartitioned_4")
    val filesRepartitioned = countPartFiles(new File(s"$outputRoot/repartitioned_4"))
    println(s"Wrote AFTER .repartition(4) -> $filesRepartitioned part-file(s) (always exactly 4,")
    println("  regardless of how many partitions the DataFrame started with - a full shuffle evens things out).")

    // coalesce(n) merges existing partitions WITHOUT a full shuffle - cheaper,
    // but can only REDUCE the partition count, and can leave partitions uneven
    // because it just groups existing partitions together rather than
    // re-distributing every row.
    val coalesced = salesDf.coalesce(2)
    coalesced.write.mode("overwrite").parquet(s"$outputRoot/coalesced_2")
    val filesCoalesced = countPartFiles(new File(s"$outputRoot/coalesced_2"))
    println(s"Wrote AFTER .coalesce(2) -> $filesCoalesced part-file(s) (cheaper than repartition,")
    println("  no shuffle, but can't increase partition count and may leave sizes uneven).")

    println(
      """Rule of thumb: use repartition(n) when you need an EXACT, EVEN file count (especially
        |after a wide operation like groupBy/join that may have left very uneven partitions), and
        |coalesce(n) when you only need to REDUCE the number of files cheaply and the existing
        |partitions are already reasonably balanced (e.g. right before writing a small final result).
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: daily sales partitioned by year/month/day
  // ---------------------------------------------------------------------
  def scenarioDailySalesPartitioned(spark: SparkSession, salesDf: DataFrame): Unit = {
    println(s"Raw salesDf: ${salesDf.count()} rows across " +
      s"${salesDf.select("year").distinct().count()} years, " +
      s"${salesDf.select("year", "month").distinct().count()} distinct (year, month) combos, " +
      s"${salesDf.select("year", "month", "day").distinct().count()} distinct (year, month, day) combos.")

    // Clean the data a little first - this is realistic: raw source data has
    // a few % missing amount/region (see the Python generator), and we don't
    // want nulls silently breaking downstream sums.
    val cleaned = salesDf
      .withColumn("amount", coalesce(col("amount"), lit(0.0)))
      .na.fill("UNKNOWN", Seq("region"))

    val missingAmount = salesDf.filter(col("amount").isNull).count()
    val missingRegion = salesDf.filter(col("region").isNull || col("region") === "").count()
    println(s"Cleaned $missingAmount row(s) with missing amount (defaulted to 0.0) and " +
      s"$missingRegion row(s) with missing region (defaulted to UNKNOWN) before writing.")

    // Repartition BEFORE the partitioned write: without this, Spark would
    // shuffle however many partitions the CSV read left us with into the
    // partitionBy() buckets, which - with 3 years x ~12 months x ~15 days of
    // keys - can fragment into many small files per leaf folder. Repartitioning
    // by the SAME columns used in partitionBy groups all rows for a given
    // (year, month, day) onto one partition BEFORE the write, so each leaf
    // folder gets exactly one part-file.
    val forWrite = cleaned.repartition(col("year"), col("month"), col("day"))

    forWrite.write.mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(s"$outputRoot/daily_sales_partitioned")

    println("\nDirectory layout for the partitioned daily sales table (first couple of levels):")
    printTree(new File(s"$outputRoot/daily_sales_partitioned"), maxDepth = 3)

    val leafFolders = countLeafPartitionFolders(new File(s"$outputRoot/daily_sales_partitioned"))
    println(s"\n$leafFolders leaf (year/month/day) folders were created, and thanks to repartitioning")
    println("by the same (year, month, day) columns used in partitionBy, almost every leaf folder")
    println("holds exactly ONE part-file instead of being fragmented across several.")

    // Read it back and show that partition pruning + normal queries both work
    // transparently against the partitioned layout.
    val readBack = spark.read.parquet(s"$outputRoot/daily_sales_partitioned")

    println("\nReading a single day back (partition pruning - only that one folder is touched):")
    readBack.filter(col("year") === 2026 && col("month") === 1 && col("day") === 15)
      .select("saleId", "region", "product", "quantity", "amount")
      .show(truncate = false)

    println("Monthly revenue trend for 2026 (reconstructed entirely from folder-name partition columns):")
    readBack.filter(col("year") === 2026)
      .groupBy("year", "month")
      .agg(round(sum("amount"), 2).as("revenue"), count("*").as("txns"))
      .orderBy("month")
      .show(truncate = false)

    println("Top region by total revenue, across the full partitioned dataset:")
    readBack.groupBy("region")
      .agg(round(sum("amount"), 2).as("revenue"), count("*").as("txns"))
      .orderBy(desc("revenue"))
      .show(truncate = false)
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

  def dirSizeBytes(dir: File): Long = {
    if (!dir.exists()) 0L
    else if (dir.isFile) dir.length()
    else dir.listFiles().map(dirSizeBytes).sum
  }

  def countPartFiles(dir: File): Int = {
    if (!dir.exists()) 0
    else dir.listFiles().count(f => f.isFile && f.getName.startsWith("part-"))
  }

  def countLeafPartitionFolders(dir: File): Int = {
    if (!dir.exists()) 0
    else {
      val subdirs = dir.listFiles().filter(_.isDirectory)
      // A "day=" folder is a leaf partition folder; anything above that (year=/month=) recurses.
      if (subdirs.nonEmpty && subdirs.head.getName.startsWith("day=")) subdirs.length
      else subdirs.map(countLeafPartitionFolders).sum
    }
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
