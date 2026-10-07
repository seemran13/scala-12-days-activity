package day21

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Day 21 — Spark Catalog
 *
 * Covers:
 *   1. Listing databases and tables
 *   2. Creating temporary views
 *   3. Registering/querying tables
 *   4. Inspecting table/schema metadata
 *   5. Scenario: a small analytics database for hotel bookings
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 *
 * Spark keeps track of every database, table and view you create in its
 * CATALOG - spark.catalog is the API for asking it "what do you know about?"
 * instead of having to remember table names yourself. This app uses Spark's
 * built-in, file-backed catalog (metastore_db/ + spark-warehouse/, both
 * git-ignored and safe to delete) - no external Hive metastore needed.
 */
object SparkCatalogApp {

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day21-SparkCatalogApp")
      .master(masterUrl)
      // enableHiveSupport() is NOT required for any of this - Spark's default
      // in-memory/embedded catalog already supports databases, managed
      // tables, temp views and full metadata inspection.
      .getOrCreate()

    val hotelsDf = csv(spark, "hotels.csv")
    val roomsDf = csv(spark, "rooms.csv")
    val guestsDf = csv(spark, "guests.csv")
    val bookingsDf = csv(spark, "bookings.csv")

    section("0. Catalog state BEFORE we create anything")
    showCatalogState(spark)

    section("1. Listing databases and tables")
    listDatabasesAndTables(spark)

    section("2. Creating temporary views")
    temporaryViewsDemo(spark, hotelsDf, roomsDf, guestsDf, bookingsDf)

    section("3. Registering/querying tables")
    registerAndQueryTables(spark, hotelsDf, roomsDf, guestsDf, bookingsDf)

    section("4. Inspecting table/schema metadata")
    inspectMetadata(spark)

    section("5. Scenario: a small analytics database for hotel bookings")
    hotelAnalyticsScenario(spark)

    spark.stop()
  }

  def csv(spark: SparkSession, name: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"src/main/resources/$name")

  // ---------------------------------------------------------------------
  // 0. What the catalog looks like before we touch it
  // ---------------------------------------------------------------------
  def showCatalogState(spark: SparkSession): Unit = {
    println("spark.catalog.currentDatabase = " + spark.catalog.currentDatabase)
    println("Databases Spark already knows about:")
    spark.catalog.listDatabases().show(truncate = false)
    println("Tables in the current database (should be empty - nothing registered yet):")
    spark.catalog.listTables().show(truncate = false)
  }

  // ---------------------------------------------------------------------
  // 1. Listing databases and tables
  // ---------------------------------------------------------------------
  def listDatabasesAndTables(spark: SparkSession): Unit = {
    // CREATE DATABASE groups related tables together, exactly like a schema
    // in a traditional RDBMS. Spark's default database is called "default".
    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")
    spark.sql("CREATE DATABASE IF NOT EXISTS staging")

    println("spark.catalog.listDatabases() - every database is itself just a catalog entry:")
    spark.catalog.listDatabases().select("name", "description", "locationUri").show(truncate = false)

    // USE switches which database unqualified table names resolve against -
    // exactly like `USE mydb;` in MySQL/Postgres/Hive.
    spark.sql("USE hotel_analytics")
    println(s"After USE hotel_analytics -> spark.catalog.currentDatabase = ${spark.catalog.currentDatabase}")

    println("listTables() defaults to the CURRENT database (hotel_analytics - empty so far):")
    spark.catalog.listTables().show(truncate = false)

    println("listTables(\"default\") explicitly asks about a DIFFERENT database by name:")
    spark.catalog.listTables("default").show(truncate = false)

    // Switch back to default for the rest of the walkthrough; the scenario
    // section revisits hotel_analytics deliberately.
    spark.sql("USE default")
  }

  // ---------------------------------------------------------------------
  // 2. Temporary views
  // ---------------------------------------------------------------------
  def temporaryViewsDemo(spark: SparkSession, hotelsDf: DataFrame, roomsDf: DataFrame,
                          guestsDf: DataFrame, bookingsDf: DataFrame): Unit = {
    // createOrReplaceTempView: visible only within THIS SparkSession, gone
    // the moment the session ends. It does NOT appear under any database -
    // it lives in a session-local, unnamed scope of its own.
    hotelsDf.createOrReplaceTempView("hotels_view")
    roomsDf.createOrReplaceTempView("rooms_view")
    guestsDf.createOrReplaceTempView("guests_view")
    bookingsDf.createOrReplaceTempView("bookings_view")

    println("Session-scoped temp views now show up in listTables() even though no database holds them:")
    spark.catalog.listTables().show(truncate = false)
    println("Notice the \"isTemporary\" column is true, and \"database\" is null for all four.")

    // createGlobalTempView: visible across ALL SparkSessions sharing this
    // Spark application (handy when code creates a second session), but it
    // still disappears when the application (not just the session) ends.
    // It lives in the special "global_temp" database.
    hotelsDf.createOrReplaceGlobalTempView("hotels_global_view")
    println("\nA global temp view is registered under the special 'global_temp' database:")
    spark.catalog.listTables("global_temp").show(truncate = false)

    println("Querying a plain temp view needs no prefix:")
    spark.sql("SELECT COUNT(*) AS hotelCount FROM hotels_view").show()

    println("Querying a GLOBAL temp view requires the global_temp. prefix:")
    spark.sql("SELECT COUNT(*) AS hotelCount FROM global_temp.hotels_global_view").show()

    println(
      """createOrReplaceTempView vs createOrReplaceGlobalTempView:
        | - Plain temp view : scoped to the SparkSession that created it. Perfect for
        |   "I just want to run SQL against this DataFrame in the current script."
        | - Global temp view : scoped to the whole Spark application, shared across
        |   every SparkSession created from the same SparkContext. Rarely needed in
        |   a simple app like this one, but matters once multiple sessions are in play
        |   (e.g. a long-running service creating a fresh session per request).
        |Neither survives a restart - for that, section 3's managed TABLES are what you want.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 3. Registering/querying tables
  // ---------------------------------------------------------------------
  def registerAndQueryTables(spark: SparkSession, hotelsDf: DataFrame, roomsDf: DataFrame,
                              guestsDf: DataFrame, bookingsDf: DataFrame): Unit = {
    spark.sql("USE hotel_analytics")

    // saveAsTable writes a MANAGED TABLE: Spark stores both the DATA (as
    // Parquet files under spark-warehouse/hotel_analytics.db/<table>/) AND
    // the metadata (schema, location) in its catalog. Unlike a temp view,
    // this survives across spark-shell/application restarts - it's a real,
    // persisted table, not just a pointer to an in-memory DataFrame.
    hotelsDf.write.mode("overwrite").saveAsTable("hotels")
    roomsDf.write.mode("overwrite").saveAsTable("rooms")
    guestsDf.write.mode("overwrite").saveAsTable("guests")
    bookingsDf.write.mode("overwrite").saveAsTable("bookings")

    println("Tables registered in hotel_analytics via saveAsTable (isTemporary = false now):")
    spark.catalog.listTables("hotel_analytics").show(truncate = false)

    println("Query a managed table with plain SQL, no createOrReplaceTempView needed -")
    println("saveAsTable already registered it in the catalog:")
    spark.sql("SELECT hotelId, hotelName, city, starRating FROM hotels ORDER BY starRating DESC").show(truncate = false)

    println("spark.table(\"name\") reads a registered table back as a DataFrame directly:")
    val roomsBack = spark.table("rooms")
    println(s"spark.table('rooms') -> ${roomsBack.count()} rows, ${roomsBack.rdd.getNumPartitions} partition(s)")

    println("\nspark.catalog.tableExists checks the catalog without throwing if missing:")
    println(s"  tableExists('hotels')          = ${spark.catalog.tableExists("hotels")}")
    println(s"  tableExists('does_not_exist')  = ${spark.catalog.tableExists("does_not_exist")}")
    println(s"  tableExists('default', 'hotels') (wrong db) = ${spark.catalog.tableExists("default", "hotels")}")
  }

  // ---------------------------------------------------------------------
  // 4. Inspecting table/schema metadata
  // ---------------------------------------------------------------------
  def inspectMetadata(spark: SparkSession): Unit = {
    spark.sql("USE hotel_analytics")

    println("DESCRIBE shows column names/types - the schema as the catalog recorded it:")
    spark.sql("DESCRIBE TABLE bookings").show(truncate = false)

    println("DESCRIBE EXTENDED additionally shows table-level metadata: provider, location, owner...")
    spark.sql("DESCRIBE EXTENDED bookings").show(60, truncate = false)

    println("spark.catalog.listColumns(...) gives the same column info as a typed DataFrame,")
    println("including nullability and which columns (if any) are partition/bucket columns:")
    spark.catalog.listColumns("bookings").show(truncate = false)

    println("spark.catalog.getTable(...) returns one Table object describing the table itself")
    println("(name, database, description, tableType, isTemporary) - handy for programmatic checks:")
    val t = spark.catalog.getTable("hotel_analytics", "bookings")
    println(s"  name=${t.name} database=${t.database} tableType=${t.tableType} isTemporary=${t.isTemporary}")

    println("\nSHOW TABLES / SHOW DATABASES - the plain-SQL equivalents of the catalog() calls above:")
    spark.sql("SHOW DATABASES").show(truncate = false)
    spark.sql("SHOW TABLES IN hotel_analytics").show(truncate = false)

    println("spark.catalog.refreshTable(...) tells Spark to re-read a table's metadata/files from")
    println("disk - needed if something outside this Spark session changed the underlying data")
    println("(e.g. another job overwrote the files) and cached metadata would otherwise go stale:")
    spark.catalog.refreshTable("hotel_analytics.bookings")
    println("  (refreshed - no output expected unless something was actually stale)")
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: a small analytics database for hotel bookings
  // ---------------------------------------------------------------------
  def hotelAnalyticsScenario(spark: SparkSession): Unit = {
    spark.sql("USE hotel_analytics")

    println("The analytics database now has everything a small BI tool would expect to find by")
    println("just browsing the catalog - no need to already know table names up front:")
    println(s"Databases   : ${spark.catalog.listDatabases().select("name").collect().map(_.getString(0)).mkString(", ")}")
    println(s"Tables here : ${spark.catalog.listTables("hotel_analytics").select("name").collect().map(_.getString(0)).mkString(", ")}")

    // Referential-integrity check straight from the catalog-registered
    // tables - exactly the kind of query a real analytics layer runs before
    // trusting a join. We deliberately seeded a "ghost" room (H99 / the
    // extra room row) with no matching hotel.
    println("\nData-quality check: bookings whose room points at a hotel that doesn't actually exist")
    println("in the hotels table (an orphan reference, seeded on purpose in rooms.csv/bookings.csv):")
    spark.sql(
      """
        |SELECT b.bookingId, b.hotelId, b.roomId, b.status, b.amount
        |FROM bookings b
        |LEFT ANTI JOIN hotels h ON b.hotelId = h.hotelId
        |""".stripMargin
    ).show(truncate = false)

    println("Revenue by hotel, COMPLETED bookings only, amount cleaned of nulls:")
    spark.sql(
      """
        |SELECT h.hotelName, h.city, h.starRating,
        |       COUNT(*) AS completedBookings,
        |       ROUND(SUM(COALESCE(b.amount, 0.0)), 2) AS revenue
        |FROM bookings b
        |JOIN hotels h ON b.hotelId = h.hotelId
        |WHERE b.status = 'COMPLETED'
        |GROUP BY h.hotelName, h.city, h.starRating
        |ORDER BY revenue DESC
        |""".stripMargin
    ).show(truncate = false)

    println("Booking status breakdown, system-wide:")
    spark.sql(
      """
        |SELECT status, COUNT(*) AS bookings, ROUND(SUM(COALESCE(amount, 0.0)), 2) AS totalAmount
        |FROM bookings
        |GROUP BY status
        |ORDER BY bookings DESC
        |""".stripMargin
    ).show(truncate = false)

    println("Top 5 guests by total confirmed+completed spend (guests with a null guestId excluded,")
    println("since there's no one to attribute that spend to):")
    spark.sql(
      """
        |SELECT g.guestName, g.country,
        |       COUNT(*) AS bookings,
        |       ROUND(SUM(COALESCE(b.amount, 0.0)), 2) AS totalSpend
        |FROM bookings b
        |JOIN guests g ON b.guestId = g.guestId
        |WHERE b.status IN ('CONFIRMED', 'COMPLETED')
        |GROUP BY g.guestName, g.country
        |ORDER BY totalSpend DESC
        |LIMIT 5
        |""".stripMargin
    ).show(truncate = false)

    println("Room-type popularity across the whole chain:")
    spark.sql(
      """
        |SELECT r.roomType, COUNT(*) AS bookings, ROUND(AVG(COALESCE(b.amount, 0.0)), 2) AS avgAmount
        |FROM bookings b
        |JOIN rooms r ON b.roomId = r.roomId
        |GROUP BY r.roomType
        |ORDER BY bookings DESC
        |""".stripMargin
    ).show(truncate = false)

    println("\nTeardown, to show the catalog side of DROP: dropTempView removes a session view,")
    println("DROP TABLE removes a managed table's metadata AND its underlying data files.")
    spark.catalog.dropTempView("hotels_view")
    println(s"  dropTempView('hotels_view') -> still exists? ${spark.catalog.tableExists("hotels_view")}")
    println("  (managed tables under hotel_analytics are left in place deliberately, so you can")
    println("   re-run queries against them with a plain spark-shell later if you want to poke around)")
  }

  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }
}
