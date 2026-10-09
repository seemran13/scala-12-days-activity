package day26

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import scala.collection.JavaConverters._

/**
 * Day 26 — Real-Time Healthcare Project
 *
 * Covers:
 *   1. Designing a patient-vital event schema
 *   2. Processing vital streams
 *   3. Creating abnormal-vital alerts
 *   4. Broadcast thresholds and accumulators
 *   5. Stateful/window processing for repeated abnormal readings
 *
 * A capstone combining days 23-25's streaming techniques (sockets, stateless
 * transformations, broadcast joins, accumulators, updateStateByKey,
 * reduceByKeyAndWindow) into one realistic monitoring pipeline, the same way
 * Day 22 combined days 13-21's batch techniques into one pipeline.
 *
 * Needs a feeder, same pattern as Days 23-25. Start it FIRST, in its own
 * terminal, from this project's root directory:
 *
 *   python3 scripts/vital_feed_server.py
 *
 * ...then, in a SECOND terminal, also from this project's root:
 *
 *   sbt run
 *
 * Patient PAT012 deliberately starts "crashing" about a minute into the
 * feed (sustained abnormal heart rate + oxygen saturation for ~40 seconds,
 * then recovers) - watch for it to trigger BOTH the consecutive-streak
 * escalation and the windowed-frequency escalation. Let it run for at least
 * 2 minutes, then Ctrl+C BOTH terminals (the Scala app first) to stop.
 */
object VitalStreamApp {

  case class VitalReading(
      patientId: String,
      timestamp: String,
      heartRate: Double,
      spo2: Double,
      systolicBP: Double,
      diastolicBP: Double,
      temperature: Double
  )

  case class PatientInfo(name: String, age: Int, ward: String)

  // A reading counts as "abnormal" the instant ANY vital is outside its
  // [min, max] range below.
  val consecutiveAbnormalThreshold = 3 // 3+ IN A ROW -> escalate (stateful)
  val windowedAbnormalThreshold = 5 // 5+ within the window -> escalate (windowed)
  val abnormalWindow = Seconds(20)
  val abnormalSlide = Seconds(4)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day26-VitalStreamApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint") // required by updateStateByKey and the invertible reduceByKeyAndWindow

    explainSchema()

    // ---------------------------------------------------------------------
    // 4a. Broadcast thresholds (and a patient directory, broadcast the same way)
    // ---------------------------------------------------------------------
    // The "master table" technique from Day 19/22, applied to a lookup table
    // instead of a DataFrame: both of these are small, read-only, and used
    // by EVERY batch, so each is shipped to the executors ONCE via
    // sc.broadcast(...) rather than being re-sent as part of every task's
    // closure. A broadcast variable doesn't have to come from a DataFrame or
    // RDD read at all - patientDirectory below is built from a plain
    // scala.io.Source read, exactly like Day 2's pure-Scala sample data.
    val thresholds: Map[String, (Double, Double)] = Map(
      "heartRate" -> (60.0, 100.0), // bpm
      "spo2" -> (95.0, 100.0), // % oxygen saturation
      "systolicBP" -> (90.0, 140.0), // mmHg
      "diastolicBP" -> (60.0, 90.0), // mmHg
      "temperature" -> (36.1, 37.8) // Celsius
    )
    val thresholdsBC = sc.broadcast(thresholds)
    val patientDirectory = loadPatientDirectory()
    val patientsBC = sc.broadcast(patientDirectory)

    // ---------------------------------------------------------------------
    // 4b. Accumulators
    // ---------------------------------------------------------------------
    // Unlike every DStream count in Days 23-25 (which resets every batch, or
    // every window), an accumulator is a TRUE running total for the life of
    // the whole streaming application - the same role it played in Day 11,
    // now applied to live data instead of a one-shot RDD job.
    val totalReadingsAcc = sc.longAccumulator("totalReadingsProcessed")
    val invalidReadingsAcc = sc.longAccumulator("invalidReadingsDropped")
    val abnormalReadingsAcc = sc.longAccumulator("abnormalReadingsFlagged")
    val criticalEscalationsAcc = sc.longAccumulator("criticalEscalationsFired")
    val abnormalVitalNamesAcc = sc.collectionAccumulator[String]("abnormalVitalNames")

    // ---------------------------------------------------------------------
    // 2. Process vital streams (parse + validate, same shape as Day 24/25)
    // ---------------------------------------------------------------------
    val lines = ssc.socketTextStream("localhost", 9999)

    // Exactly ONE action evaluates this map per batch (the rdd.foreach right
    // below), so invalidReadingsAcc is incremented exactly once per
    // malformed line - see the big caching comment further down for why
    // that "exactly once" property takes care to preserve once a DStream
    // feeds MULTIPLE separate action chains, which validReadings does.
    val parsedOpt = lines.map(parseLine)
    parsedOpt.foreachRDD { rdd =>
      rdd.foreach { opt => if (opt.isEmpty) invalidReadingsAcc.add(1) }
    }

    // validReadings feeds THREE independent downstream action chains below
    // (the alert pipeline, the stateful streak, and the windowed count) -
    // exactly the "reused multiple times" shape from Day 12/22, so it's
    // cached here for the same reason: without this, parseLine and every
    // upstream step would be recomputed from scratch, three times, every
    // single batch.
    val validReadings: DStream[VitalReading] = parsedOpt.flatMap(_.toList)
    validReadings.cache()

    // ---------------------------------------------------------------------
    // 3. Create abnormal-vital alerts
    // ---------------------------------------------------------------------
    processVitalStreams(validReadings, thresholdsBC, patientsBC, totalReadingsAcc, abnormalReadingsAcc, abnormalVitalNamesAcc)

    // One (patientId, 1-or-0) pair per reading - 1 if ANY vital was out of
    // range, 0 otherwise - the shared input for BOTH techniques in section 5.
    val abnormalFlags: DStream[(String, Int)] =
      validReadings.map(r => (r.patientId, if (isAbnormal(r, thresholdsBC.value)) 1 else 0))

    // ---------------------------------------------------------------------
    // 5. Stateful/window processing for repeated abnormal readings
    // ---------------------------------------------------------------------
    statefulRepeatedAbnormalDemo(abnormalFlags, patientsBC, criticalEscalationsAcc)
    windowedRepeatedAbnormalDemo(abnormalFlags, patientsBC)

    accumulatorSummaryDemo(validReadings, totalReadingsAcc, invalidReadingsAcc, abnormalReadingsAcc, criticalEscalationsAcc, abnormalVitalNamesAcc)

    ssc.start()
    ssc.awaitTermination()
  }

  // ---------------------------------------------------------------------
  // 1. Patient-vital event schema
  // ---------------------------------------------------------------------
  def explainSchema(): Unit = {
    println(
      """Event schema for one vital-sign reading (one line on the wire = one event):
        |
        |  patientId    String  - which patient; the KEY every stateful/windowed
        |                         operation below groups by
        |  timestamp    String  - when the device took the reading (ISO-8601).
        |                         Kept as a String here (not parsed to a Spark
        |                         SQL timestamp) since nothing in this app needs
        |                         date arithmetic on it - just carried through
        |                         for display and audit purposes.
        |  heartRate    Double  - beats per minute
        |  spo2         Double  - blood oxygen saturation, percent
        |  systolicBP   Double  - systolic blood pressure, mmHg
        |  diastolicBP  Double  - diastolic blood pressure, mmHg
        |  temperature  Double  - body temperature, Celsius
        |
        |Doubles rather than Ints throughout - vitals are measured, not counted,
        |and a monitor can legitimately report 36.6 or 98.4. patientId is the
        |one field EVERY downstream operation (broadcast lookup, accumulator
        |tally, updateStateByKey, reduceByKeyAndWindow) keys off of, so it's
        |validated first and most strictly in parseLine.
        |
        |A real hospital feed would likely also carry a deviceId, a schema
        |version, and a unit field per vital (so "98.4" can't be silently
        |misread as Celsius vs Fahrenheit) - left out here to keep the event
        |small enough to read off one printed line, but worth knowing a
        |production schema would widen for exactly those reasons.
        |""".stripMargin)
  }

  /** Parses one raw CSV line into a VitalReading, or None if it fails validation -
    * same stateless, line-at-a-time check used in Days 24-25. */
  def parseLine(line: String): Option[VitalReading] = {
    if (line == null || line.trim.isEmpty) return None
    val parts = line.split(",", -1)
    if (parts.length != 7) return None

    val Array(patientId, timestamp, hrS, spo2S, sysS, diaS, tempS) = parts
    if (patientId.trim.isEmpty) return None

    try {
      val hr = hrS.toDouble
      val spo2 = spo2S.toDouble
      val sys = sysS.toDouble
      val dia = diaS.toDouble
      val temp = tempS.toDouble
      if (hr <= 0 || spo2 <= 0 || sys <= 0 || dia <= 0 || temp <= 0) None
      else Some(VitalReading(patientId, timestamp, hr, spo2, sys, dia, temp))
    } catch {
      case _: NumberFormatException => None
    }
  }

  /** Reads the small patient reference file with plain Scala I/O - no Spark
    * needed for a lookup table this size, same spirit as Day 2's pure-Scala
    * collections before Spark entered the picture at all. */
  def loadPatientDirectory(): Map[String, PatientInfo] = {
    val src = scala.io.Source.fromFile("src/main/resources/patients.csv")
    try {
      src.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>
        val Array(id, name, age, ward) = line.split(",", -1)
        id -> PatientInfo(name, age.toInt, ward)
      }.toMap
    } finally {
      src.close()
    }
  }

  /** Which vitals on this reading are outside their broadcast threshold range,
    * as (vitalName, actualValue) pairs - empty if the reading is fully normal. */
  def checkAbnormal(r: VitalReading, thresholds: Map[String, (Double, Double)]): Seq[(String, Double)] = {
    val values = Seq(
      "heartRate" -> r.heartRate,
      "spo2" -> r.spo2,
      "systolicBP" -> r.systolicBP,
      "diastolicBP" -> r.diastolicBP,
      "temperature" -> r.temperature
    )
    values.filter { case (name, v) =>
      thresholds.get(name).exists { case (lo, hi) => v < lo || v > hi }
    }
  }

  def isAbnormal(r: VitalReading, thresholds: Map[String, (Double, Double)]): Boolean =
    checkAbnormal(r, thresholds).nonEmpty

  // ---------------------------------------------------------------------
  // 3. Create abnormal-vital alerts (+ accumulator bookkeeping)
  // ---------------------------------------------------------------------
  def processVitalStreams(
      validReadings: DStream[VitalReading],
      thresholdsBC: org.apache.spark.broadcast.Broadcast[Map[String, (Double, Double)]],
      patientsBC: org.apache.spark.broadcast.Broadcast[Map[String, PatientInfo]],
      totalAcc: org.apache.spark.util.LongAccumulator,
      abnormalAcc: org.apache.spark.util.LongAccumulator,
      vitalNamesAcc: org.apache.spark.util.CollectionAccumulator[String]
  ): Unit = {
    validReadings.foreachRDD { (rdd, time) =>
      // Accumulator updates inside a transformation (map) only take effect
      // once an ACTION runs it - collect() below is that action, and it's
      // the ONLY action in this whole method, so each accumulator here is
      // touched exactly once per reading per batch (Spark's documented
      // caveat: this guarantee can break under task retries/speculative
      // execution, not a concern in this single-machine local[2] demo).
      val abnormalReadings = rdd.map { r =>
        totalAcc.add(1)
        val abnormalities = checkAbnormal(r, thresholdsBC.value)
        if (abnormalities.nonEmpty) {
          abnormalAcc.add(1)
          abnormalities.foreach { case (name, _) => vitalNamesAcc.add(name) }
        }
        (r, abnormalities)
      }.filter(_._2.nonEmpty).collect()

      if (abnormalReadings.nonEmpty) {
        println(f"\n[ALERT] batch @ $time - ${abnormalReadings.length}%d abnormal reading(s):")
        abnormalReadings.take(10).foreach { case (r, abnormalities) =>
          val info = patientsBC.value.getOrElse(r.patientId, PatientInfo("Unknown", -1, "Unknown"))
          val detail = abnormalities.map { case (name, v) => f"$name=$v%.1f" }.mkString(", ")
          println(f"  ${r.patientId}%-8s (${info.name}%-16s ${info.ward}%-10s) -> $detail")
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // 5a. Stateful: consecutive abnormal readings per patient
  // ---------------------------------------------------------------------
  def statefulRepeatedAbnormalDemo(
      abnormalFlags: DStream[(String, Int)],
      patientsBC: org.apache.spark.broadcast.Broadcast[Map[String, PatientInfo]],
      criticalAcc: org.apache.spark.util.LongAccumulator
  ): Unit = {
    // For each key (patientId), walk this batch's new flags IN ORDER: a 1
    // extends the streak, a 0 resets it to zero - carrying the running
    // streak forward from batch to batch via updateStateByKey, same
    // mechanism as Day 24's running transaction counts, just with "reset on
    // a normal reading" instead of "always add".
    val updateStreak = (newFlags: Seq[Int], state: Option[Int]) => {
      var streak = state.getOrElse(0)
      newFlags.foreach { flag => streak = if (flag == 1) streak + 1 else 0 }
      Some(streak)
    }
    val consecutiveAbnormalStreak = abnormalFlags.updateStateByKey(updateStreak)

    consecutiveAbnormalStreak.foreachRDD { (rdd, time) =>
      // == rather than >= on purpose: fires exactly ONCE the moment a
      // streak CROSSES the threshold, not on every batch while it stays
      // elevated (a patient with 8 abnormal readings in a row shouldn't
      // re-page the clinician every 2 seconds) - and fires again if the
      // streak later resets and climbs back up past the threshold a second
      // time, since that's a genuinely new episode worth re-alerting on.
      val escalations = rdd.filter { case (_, streak) => streak == consecutiveAbnormalThreshold }.collect()
      escalations.foreach { case (patientId, streak) =>
        criticalAcc.add(1)
        val info = patientsBC.value.getOrElse(patientId, PatientInfo("Unknown", -1, "Unknown"))
        println(f"\n*** CRITICAL @ $time *** $patientId%-8s (${info.name}) has had $streak%d consecutive " +
          "abnormal readings - escalate to clinician")
      }
    }
  }

  // ---------------------------------------------------------------------
  // 5b. Windowed: frequency of abnormal readings per patient
  // ---------------------------------------------------------------------
  def windowedRepeatedAbnormalDemo(
      abnormalFlags: DStream[(String, Int)],
      patientsBC: org.apache.spark.broadcast.Broadcast[Map[String, PatientInfo]]
  ): Unit = {
    // The Day 25 technique: an invertible reduceByKeyAndWindow sums how many
    // abnormal flags landed on each patient within the last abnormalWindow,
    // re-evaluated every abnormalSlide. Unlike the stateful streak above,
    // this doesn't care whether the abnormal readings were CONSECUTIVE -
    // five abnormal readings scattered across a 20-second window (with
    // normal readings in between) trips this just as much as five in a row,
    // catching a different pattern: a patient who's unstable but not
    // strictly worsening reading-over-reading.
    val windowedAbnormalCount = abnormalFlags.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      (a: Int, b: Int) => a - b,
      abnormalWindow,
      abnormalSlide
    )

    windowedAbnormalCount.foreachRDD { (rdd, time) =>
      val frequent = rdd.filter { case (_, count) => count >= windowedAbnormalThreshold }
        .collect().sortBy { case (_, count) => -count }
      if (frequent.nonEmpty) {
        val windowSeconds = abnormalWindow.milliseconds / 1000
        println(f"\n[WINDOW ALERT] batch @ $time - $windowedAbnormalThreshold%d+ abnormal readings in the last $windowSeconds%d s:")
        frequent.foreach { case (patientId, count) =>
          val info = patientsBC.value.getOrElse(patientId, PatientInfo("Unknown", -1, "Unknown"))
          println(f"  $patientId%-8s (${info.name}) -> $count%d abnormal reading(s) in window")
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // 4c. Periodic accumulator summary
  // ---------------------------------------------------------------------
  def accumulatorSummaryDemo(
      validReadings: DStream[VitalReading],
      totalAcc: org.apache.spark.util.LongAccumulator,
      invalidAcc: org.apache.spark.util.LongAccumulator,
      abnormalAcc: org.apache.spark.util.LongAccumulator,
      criticalAcc: org.apache.spark.util.LongAccumulator,
      vitalNamesAcc: org.apache.spark.util.CollectionAccumulator[String]
  ): Unit = {
    // A driver-local counter, safe to mutate here because foreachRDD's own
    // function always runs on the driver, once per batch, one at a time -
    // there's no concurrent access to race on, unlike the data itself.
    var batchesSeen = 0

    validReadings.foreachRDD { (_, time) =>
      batchesSeen += 1
      if (batchesSeen % 5 == 0) { // every 5 batches (~10s) - a running total doesn't need reprinting every 2s
        val tally = vitalNamesAcc.value.asScala.groupBy(identity).mapValues(_.size).toSeq.sortBy { case (_, n) => -n }
        println(f"\n[ACCUMULATOR SUMMARY] as of $time")
        println(f"  total readings processed  : ${totalAcc.value}%d")
        println(f"  invalid/malformed dropped : ${invalidAcc.value}%d")
        println(f"  abnormal readings flagged : ${abnormalAcc.value}%d")
        println(f"  critical escalations fired: ${criticalAcc.value}%d")
        if (tally.nonEmpty) {
          println("  abnormal-by-vital-type    : " + tally.map { case (name, n) => s"$name=$n" }.mkString(", "))
        }
      }
    }
  }
}
