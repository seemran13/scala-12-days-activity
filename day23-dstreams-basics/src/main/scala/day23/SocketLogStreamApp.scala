package day23

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Day 23 — DStreams Basics
 *
 * Covers:
 *   1. Creating a StreamingContext and defining a batch interval
 *   2. Reading a socket text stream
 *   3. map / filter / flatMap on a DStream
 *   4. Micro-batch processing, explained
 *   5. Scenario: stream application logs, count ERROR messages every interval
 *
 * This is the SOCKET half of "read a socket/text stream" (the file-based
 * half is FileLogStreamApp, right next to this one). It needs something on
 * the other end of the socket sending text - start the companion Python
 * server FIRST, in its own terminal, from this project's root directory:
 *
 *   python3 scripts/log_server.py
 *
 * ...then, in a SECOND terminal, also from this project's root:
 *
 *   sbt run
 *
 * Let it run for 30-60 seconds to see several batches go by, then Ctrl+C
 * BOTH terminals (the Scala app first, then the Python server) to stop.
 */
object SocketLogStreamApp {

  def main(args: Array[String]): Unit = {
    // local[2] is NOT optional here, unlike every earlier day's local[*]/
    // local[N] choice. A DStream app permanently dedicates ONE core to the
    // RECEIVER (the thread that sits there reading the socket), so with
    // only 1 core available, data would be received but NEVER actually
    // processed - batches would just queue up forever. Streaming always
    // needs at least (number of receivers + 1) cores.
    val conf = new SparkConf().setAppName("Day23-SocketLogStreamApp").setMaster("local[2]")

    // The batch interval is the one genuinely new idea in this whole file:
    // every 5 seconds, Spark closes off whatever arrived on the socket
    // during that window and turns it into one ordinary RDD (one
    // "micro-batch") to run the rest of the DStream's transformations
    // against - see explainMicroBatching() below for the full picture.
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    explainMicroBatching()

    // ---------------------------------------------------------------------
    // 2. Reading a socket text stream
    // ---------------------------------------------------------------------
    // socketTextStream opens a TCP connection to host:port and turns every
    // line of text it receives into one element of the DStream. A fresh RDD
    // of "whatever lines arrived this interval" is produced every 5 seconds,
    // for as long as the connection stays open.
    val lines = ssc.socketTextStream("localhost", 9999)

    // ---------------------------------------------------------------------
    // 3. map / filter / flatMap on a DStream
    // ---------------------------------------------------------------------
    // These are EXACTLY the same transformations as a plain RDD (Day 5) -
    // a DStream is really just "a series of RDDs over time", and calling
    // .map/.filter/.flatMap on it applies that transformation to EVERY
    // micro-batch's RDD automatically, without writing a loop yourself.
    val upperLines = lines.map(_.toUpperCase)
    val nonBlankLines = lines.filter(_.trim.nonEmpty)
    val words = nonBlankLines.flatMap(_.split("\\s+")).filter(_.nonEmpty)

    upperLines.foreachRDD { (rdd, time) =>
      val sample = rdd.take(1)
      if (sample.nonEmpty) println(s"\n[map]     batch @ $time - e.g. upper-cased: ${sample.head}")
    }
    words.foreachRDD { (rdd, time) =>
      val count = rdd.count()
      if (count > 0) println(s"[flatMap] batch @ $time - $count word(s) split out of this batch's lines")
    }

    // ---------------------------------------------------------------------
    // 5. Scenario: count ERROR messages every interval
    // ---------------------------------------------------------------------
    val errorLines = nonBlankLines.filter(_.toUpperCase.contains("ERROR"))

    // .count() on a DStream returns a new DStream of a single Long per
    // batch - the count for JUST that micro-batch, not a running total.
    // That's the key property this scenario is built to show: every batch
    // is independent unless you deliberately carry state across them
    // (stateful DStream operations are a topic of their own, beyond today).
    val errorCountPerBatch = errorLines.count()

    errorCountPerBatch.foreachRDD { (rdd, time) =>
      val count = rdd.collect().headOption.getOrElse(0L)
      println(f"[SCENARIO] batch @ $time -> $count%3d ERROR line(s) in THIS interval alone")
    }

    // Keep the detail behind the number, too - a real log-monitoring job
    // alerts on the count but keeps the actual lines for drill-down.
    errorLines.foreachRDD { (rdd, _) =>
      val sample = rdd.take(2)
      if (sample.nonEmpty) println(s"           sample: ${sample.mkString(" || ")}")
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def explainMicroBatching(): Unit = {
    println(
      """DStreams do NOT process one record at a time the instant it arrives -
        |that's what a true record-at-a-time engine (Flink, or Spark's own
        |newer Structured Streaming in continuous mode) does. Instead, Spark
        |Streaming collects everything that arrives during one BATCH INTERVAL
        |(5 seconds here), packages it into one ordinary RDD, and runs your
        |map/filter/flatMap/etc. pipeline against that RDD exactly like a tiny
        |batch job - hence "micro-batch". A DStream is literally a sequence of
        |these RDDs over time, one per interval; every transformation written
        |on a DStream really means "apply this transformation to each
        |micro-batch's RDD as it shows up".
        |
        |Trade-off: latency is bounded by the batch interval - you'll never
        |see a result sooner than roughly 5 seconds after data arrives here -
        |but in exchange you get to reuse the ENTIRE RDD API (joins,
        |aggregations, custom partitioning, everything from days 4-12) on
        |each micro-batch for free, instead of needing a separate
        |record-at-a-time API with its own, smaller set of operations.
        |""".stripMargin)
  }
}
