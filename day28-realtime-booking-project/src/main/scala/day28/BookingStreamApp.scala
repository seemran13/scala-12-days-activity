package day28

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * Day 28 — Real-Time Booking Project
 *
 * Covers:
 *   1. Processing hotel/flight/bus booking events
 *   2. Maintaining booking/cancellation state
 *   3. Calculating occupancy and availability
 *   4. Using Pair RDD and window operations
 *   5. Using broadcast reference data
 *   6. Creating Spark SQL reporting output
 *
 * Scenario: a travel platform's booking/cancellation events for 10 shared
 * inventory resources (4 hotels, 3 flights, 3 buses) stream in from
 * wherever a customer books. The app has to keep a running occupied-vs-
 * available count per resource, flag resources approaching or past
 * capacity, surface which resources are "trending" (lots of bookings in a
 * short window), and periodically publish a Spark SQL report across all of
 * it.
 *
 * Needs a feeder, same pattern as Days 23-27. Start it FIRST, in its own
 * terminal, from this project's root directory:
 *
 *   python3 scripts/booking_feed_server.py
 *
 * ...then, in a SECOND terminal, also from this project's root:
 *
 *   sbt run
 *
 * Resource H2 ("Seaside Resort", only 10 rooms) is deliberately scripted to
 * fill up fast and tip into NEARLY FULL -> SOLD OUT -> OVERBOOKED, then
 * partly recover as cancellations arrive - watch it cross every occupancy
 * tier. Resource F2 ("AI305") fills up normally, then gets hit with a burst
 * of cancellations mid-feed, showing occupancy drop back down. Let it run
 * for at least 2 minutes, then Ctrl+C BOTH terminals (the Scala app first)
 * to stop.
 */
object BookingStreamApp {

  case class BookingEvent(
      eventId: String,
      bookingId: String,
      resourceId: String,
      eventType: String, // BOOK or CANCEL
      units: Int,
      timestamp: String
  )

  case class ResourceInfo(name: String, city: String, serviceType: String, capacity: Int)

  case class BookingStatus(
      resourceId: String,
      name: String,
      serviceType: String,
      city: String,
      booked: Int,
      capacity: Int,
      available: Int,
      occupancyPct: Double
  )

  val nearlyFullThreshold = 0.9 // 90%+ occupied -> NEARLY FULL
  val velocityWindow = Seconds(20)
  val velocitySlide = Seconds(4)
  val trendingBookThreshold = 5 // 5+ BOOK events in the window -> trending
  val reportEveryNBatches = 5

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day28-BookingStreamApp").setMaster("local[2]")
    // Built from the SAME SparkConf/SparkContext that backs the
    // StreamingContext below, so the batch Spark SQL work in requirement 6
    // (creating DataFrames, running spark.sql queries) shares one Spark
    // application with the streaming side instead of starting a second one.
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    spark.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint") // required by updateStateByKey

    explainSchema()

    // ---------------------------------------------------------------------
    // 5. Broadcast reference data
    // ---------------------------------------------------------------------
    // resources.csv (10 rows) is the "master table" of what each resourceId
    // even IS - name, city, service type, total capacity. Loaded with plain
    // Scala I/O (no Spark read needed for 10 rows) and broadcast once, same
    // technique as Day 26's patient directory and Day 27's branch
    // directory. Every booking event only carries a resourceId on the wire
    // (see explainSchema) - this lookup is what turns that bare id into
    // something a report can show a human.
    val resourceDirectory = loadResourceDirectory()
    val resourcesBC = spark.sparkContext.broadcast(resourceDirectory)

    val lines = ssc.socketTextStream("localhost", 9999)
    val parsedOpt = lines.map(parseLine)
    val validEvents: DStream[BookingEvent] = parsedOpt.flatMap(_.toList)

    // validEvents feeds TWO independent downstream chains below (the
    // occupancy/state pipeline and the windowed trending pipeline) - the
    // same "reused multiple times" shape as Days 12/22/26/27, so it's
    // cached for the same reason.
    validEvents.cache()

    // ---------------------------------------------------------------------
    // 2 + 3 + 6. Booking/cancellation state, occupancy/availability,
    //            and the periodic Spark SQL report
    // ---------------------------------------------------------------------
    maintainBookingStateDemo(validEvents, resourcesBC, spark)

    // ---------------------------------------------------------------------
    // 4. Pair RDD and window operations (trending resources)
    // ---------------------------------------------------------------------
    bookingVelocityDemo(validEvents, resourcesBC)

    ssc.start()
    ssc.awaitTermination()
  }

  // ---------------------------------------------------------------------
  // 1. Booking event schema
  // ---------------------------------------------------------------------
  def explainSchema(): Unit = {
    println(
      """Event schema for one booking/cancellation event (one line on the
        |wire = one event):
        |
        |  eventId     String  - unique id for this event, carried through
        |                        for audit/dedup purposes only.
        |  bookingId   String  - which customer booking this event belongs
        |                        to (a BOOK and its later CANCEL share one
        |                        bookingId) - not aggregated on below, but
        |                        what a real system would use to make a
        |                        duplicate CANCEL a no-op instead of
        |                        double-subtracting.
        |  resourceId  String  - which hotel/flight/bus; the KEY every
        |                        aggregation, state update, and window
        |                        operation below groups by.
        |  eventType   String  - BOOK or CANCEL.
        |  units       Int     - how many rooms/seats this event covers
        |                        (a family booking 3 rooms is one event
        |                        with units=3, not three separate events).
        |  timestamp   String  - when the event was recorded (ISO-8601),
        |                        carried through for display only.
        |
        |Deliberately NOT on the wire: serviceType, the resource's name,
        |city, or capacity. Those live only in the broadcast resource
        |directory (requirement 5) and are looked up by resourceId -
        |carrying them on every single event would denormalize data that
        |never changes per-event and can drift out of sync with the real
        |master record. resourceId is validated first/strictest in
        |parseLine for the same reason every downstream operation keys off
        |it.
        |""".stripMargin)
  }

  /** Parses one raw CSV line into a BookingEvent, or None if it fails
    * validation - same stateless, line-at-a-time check used in Days 24-27. */
  def parseLine(line: String): Option[BookingEvent] = {
    if (line == null || line.trim.isEmpty) return None
    val parts = line.split(",", -1)
    if (parts.length != 6) return None

    val Array(eventId, bookingId, resourceId, eventType, unitsS, timestamp) = parts
    if (resourceId.trim.isEmpty) return None
    if (eventType != "BOOK" && eventType != "CANCEL") return None

    try {
      val units = unitsS.toInt
      if (units <= 0) None
      else Some(BookingEvent(eventId, bookingId, resourceId, eventType, units, timestamp))
    } catch {
      case _: NumberFormatException => None
    }
  }

  /** Reads the small resource reference file with plain Scala I/O - no Spark
    * needed for a lookup table this size, same spirit as Days 26-27. */
  def loadResourceDirectory(): Map[String, ResourceInfo] = {
    val src = scala.io.Source.fromFile("src/main/resources/resources.csv")
    try {
      src.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>
        val Array(id, serviceType, name, city, capacity) = line.split(",", -1)
        id -> ResourceInfo(name, city, serviceType, capacity.toInt)
      }.toMap
    } finally {
      src.close()
    }
  }

  // ---------------------------------------------------------------------
  // 2 + 3 + 6. Maintain booking/cancellation state, derive occupancy, report
  // ---------------------------------------------------------------------
  def maintainBookingStateDemo(
      validEvents: DStream[BookingEvent],
      resourcesBC: org.apache.spark.broadcast.Broadcast[Map[String, ResourceInfo]],
      spark: SparkSession
  ): Unit = {
    // Pair RDD: every BOOK adds its units, every CANCEL subtracts them.
    // A Pair RDD/DStream of (resourceId, signedDelta) is the input
    // updateStateByKey needs to carry a running net-booked count per key
    // forward across batches - the same running-state mechanism as Day 24's
    // account totals and Day 26/27's streaks, just with signed deltas.
    val deltas: DStream[(String, Int)] = validEvents.map { e =>
      val signedUnits = if (e.eventType == "BOOK") e.units else -e.units
      (e.resourceId, signedUnits)
    }

    val updateNetBooked = (newDeltas: Seq[Int], state: Option[Int]) => {
      val prevBooked = state.getOrElse(0)
      // Floor at 0 - a resource can't go "negative booked" no matter how
      // many stray/duplicate cancellations arrive; deliberately NOT
      // ceiling-capped at capacity, so a resource CAN read above 100% -
      // that's what the OVERBOOKED tier below is for.
      Some(math.max(0, prevBooked + newDeltas.sum))
    }
    val bookedState: DStream[(String, Int)] = deltas.updateStateByKey(updateNetBooked)

    var batchesSeen = 0

    bookedState.foreachRDD { (rdd, time) =>
      batchesSeen += 1
      val rows = rdd.collect()
      if (rows.nonEmpty) {
        // ---------------------------------------------------------------
        // 3. Calculate occupancy and availability (driver-side, using the
        //    broadcast resource directory from requirement 5)
        // ---------------------------------------------------------------
        val statuses = rows.map { case (resourceId, booked) =>
          val info = resourcesBC.value.getOrElse(resourceId, ResourceInfo("Unknown", "Unknown", "UNKNOWN", 0))
          val available = math.max(0, info.capacity - booked)
          val occupancyPct = if (info.capacity > 0) booked.toDouble / info.capacity else 0.0
          BookingStatus(resourceId, info.name, info.serviceType, info.city, booked, info.capacity, available, occupancyPct)
        }.sortBy(s => -s.occupancyPct)

        println(f"\n[OCCUPANCY] batch @ $time")
        statuses.foreach { s =>
          val tag =
            if (s.booked > s.capacity) " *** OVERBOOKED ***"
            else if (s.occupancyPct >= 1.0) " *** SOLD OUT ***"
            else if (s.occupancyPct >= nearlyFullThreshold) " ** NEARLY FULL **"
            else ""
          println(f"  ${s.resourceId}%-4s ${s.name}%-20s booked=${s.booked}%-4d / cap=${s.capacity}%-4d " +
            f"avail=${s.available}%-4d (${s.occupancyPct * 100}%5.1f%%)$tag")
        }

        // ---------------------------------------------------------------
        // 6. Spark SQL reporting output - every reportEveryNBatches, turn
        //    the current snapshot into a DataFrame and query it with SQL
        // ---------------------------------------------------------------
        if (batchesSeen % reportEveryNBatches == 0) {
          import spark.implicits._
          val statusDF = statuses.toSeq.toDF()
          statusDF.createOrReplaceTempView("booking_status")

          println(f"\n[SQL REPORT] batch @ $time - occupancy by service type")
          spark.sql(
            """SELECT serviceType,
              |       COUNT(*)                                   AS resources,
              |       ROUND(AVG(occupancyPct) * 100, 1)           AS avgOccupancyPct,
              |       SUM(CASE WHEN occupancyPct >= 0.9 THEN 1 ELSE 0 END) AS nearlyFullOrAbove
              |FROM booking_status
              |GROUP BY serviceType
              |ORDER BY avgOccupancyPct DESC""".stripMargin
          ).show(truncate = false)

          println(f"[SQL REPORT] batch @ $time - top 3 most-occupied resources")
          spark.sql(
            """SELECT resourceId, name, city, booked, capacity,
              |       ROUND(occupancyPct * 100, 1) AS occupancyPct
              |FROM booking_status
              |ORDER BY occupancyPct DESC
              |LIMIT 3""".stripMargin
          ).show(truncate = false)
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // 4. Pair RDD and window operations (trending resources)
  // ---------------------------------------------------------------------
  def bookingVelocityDemo(
      validEvents: DStream[BookingEvent],
      resourcesBC: org.apache.spark.broadcast.Broadcast[Map[String, ResourceInfo]]
  ): Unit = {
    // Pair RDD built explicitly with map(... -> (key, value)) then counted
    // with an invertible reduceByKeyAndWindow (Day 25's technique): how
    // many BOOK events has each resource had in the last velocityWindow,
    // re-evaluated every velocitySlide. Only BOOK events count here (not
    // CANCEL) - this is measuring demand/popularity, not net occupancy,
    // which bookedState above already covers.
    val bookEventCounts: DStream[(String, Int)] = validEvents
      .filter(_.eventType == "BOOK")
      .map(e => (e.resourceId, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, (a: Int, b: Int) => a - b, velocityWindow, velocitySlide)

    bookEventCounts.foreachRDD { (rdd, time) =>
      val trending = rdd.filter { case (_, count) => count >= trendingBookThreshold }
        .collect().sortBy { case (_, count) => -count }
      if (trending.nonEmpty) {
        val windowSeconds = velocityWindow.milliseconds / 1000
        println(f"\n[TRENDING] batch @ $time - $trendingBookThreshold%d+ bookings in the last $windowSeconds%d s:")
        trending.foreach { case (resourceId, count) =>
          val info = resourcesBC.value.getOrElse(resourceId, ResourceInfo("Unknown", "Unknown", "UNKNOWN", 0))
          println(f"  $resourceId%-4s ${info.name}%-20s -> $count%d bookings in window")
        }
      }
    }
  }
}
