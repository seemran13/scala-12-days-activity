package day25

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * Day 25 — Window Operations
 *
 * Covers:
 *   1. Batch interval, window size and sliding interval, explained
 *   2. countByWindow
 *   3. reduceByKeyAndWindow
 *   4. Rolling sales totals
 *   5. Scenario: detect a sudden increase in transactions during a window
 *
 * Builds on Days 23-24's socket-streaming setup. Needs a feeder, started
 * FIRST, in its own terminal, from this project's root directory:
 *
 *   python3 scripts/burst_txn_server.py
 *
 * ...then, in a SECOND terminal, also from this project's root:
 *
 *   sbt run
 *
 * The feeder alternates between a quiet 45-second "NORMAL" phase and a loud
 * 15-second "BURST" phase, on a loop - watch for the *** ALERT *** lines
 * when a burst phase kicks in. Let it run for at least 2-3 minutes to see a
 * full cycle, then Ctrl+C BOTH terminals (the Scala app first) to stop.
 */
object TransactionWindowApp {

  case class Transaction(txnId: String, timestamp: String, accountId: String, txnType: String, amount: Double)

  // -------------------------------------------------------------------
  // DEMO durations - scaled down so this is watchable in a couple of
  // minutes instead of a real 10 minutes. See explainWindowConcepts()
  // for exactly how these map onto the scenario's actual "10-minute
  // window" requirement, and the PRODUCTION values you'd really use.
  // -------------------------------------------------------------------
  val batchInterval: Duration = Seconds(2)

  val salesWindow: Duration = Seconds(10) // rolling sales total window
  val salesSlide: Duration = Seconds(4)

  val recentWindow: Duration = Seconds(10) // stand-in for a 10-MINUTE window in production
  val recentSlide: Duration = Seconds(2)

  val baselineWindow: Duration = Seconds(30) // stand-in for a much longer historical baseline (e.g. 1 hour)
  val baselineSlide: Duration = Seconds(2)

  val spikeRatioThreshold = 2.0 // recent rate must be at least this many times the baseline rate
  val spikeMinAbsoluteCount = 5L // ...AND at least this many transactions, so a quiet period's noise never "spikes"

  def main(args: Array[String]): Unit = {
    // Same reasoning as Days 23-24: one core is permanently tied up
    // receiving from the socket, so local[2] is the floor for anything to
    // actually get PROCESSED, not just received.
    val conf = new SparkConf().setAppName("Day25-TransactionWindowApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, batchInterval)
    ssc.sparkContext.setLogLevel("ERROR")

    // reduceByKeyAndWindow's INVERTIBLE form (section 3/4) needs a
    // checkpoint directory, same requirement as Day 24's updateStateByKey -
    // it has to durably remember enough to subtract an expired batch later.
    ssc.checkpoint("checkpoint")

    explainWindowConcepts()

    val lines = ssc.socketTextStream("localhost", 9999)
    val validTxns = lines.map(parseLine).flatMap(_.toList)

    countByWindowDemo(validTxns)
    rollingSalesTotalsDemo(validTxns)
    detectSuddenIncreaseScenario(validTxns)

    ssc.start()
    ssc.awaitTermination()
  }

  // ---------------------------------------------------------------------
  // 1. Batch interval, window size and sliding interval
  // ---------------------------------------------------------------------
  def explainWindowConcepts(): Unit = {
    println(
      f"""Three durations, three different jobs:
        |
        |  BATCH INTERVAL (${batchInterval.milliseconds / 1000}%d s here) - how often Spark cuts off the
        |    incoming data and turns it into one micro-batch RDD. Set once, for the
        |    whole StreamingContext (Day 23). Every other duration below must be
        |    an exact multiple of this one.
        |
        |  WINDOW SIZE / WINDOW DURATION (e.g. ${recentWindow.milliseconds / 1000}%d s for the scenario's "recent"
        |    window) - how far BACK in time a windowed operation looks, spanning
        |    MULTIPLE batches. A 10-second window with a 2-second batch interval
        |    covers the last 5 micro-batches' worth of data, combined into one RDD.
        |
        |  SLIDING INTERVAL (e.g. ${recentSlide.milliseconds / 1000}%d s for that same window) - how often a NEW
        |    windowed result is produced. A window doesn't have to slide forward by
        |    a full window's width each time - sliding interval = window duration
        |    gives NON-overlapping ("tumbling") windows, while sliding interval <
        |    window duration (the usual case, and what's used throughout this app)
        |    gives OVERLAPPING windows, where most data is counted in more than one
        |    consecutive window result.
        |
        |Picture it as a tape measure sliding along the stream of batches:
        |
        |    batch:     [B1][B2][B3][B4][B5][B6][B7][B8]...
        |    window @t1: [--------window-------]
        |    window @t2:      [--------window-------]      <- slid forward by one slide interval
        |    window @t3:           [--------window-------]
        |
        |IMPORTANT - about the scenario's "10-minute window": watching a real
        |10-minute window live would mean waiting 10 real minutes between each
        |printed result, which makes for a terrible demo. This app uses a
        |${recentWindow.milliseconds / 1000}%d-second window/slide instead so you can actually SEE it react in
        |real time - the mechanics are identical either way. To run this for real
        |against a 10-minute window with a 1-minute baseline refresh, you'd change
        |ONLY the constants at the top of this file:
        |    recentWindow   = Minutes(10),  recentSlide   = Minutes(1)
        |    baselineWindow = Hours(1),     baselineSlide  = Minutes(1)
        |    batchInterval  = Seconds(30)   (a bigger batch interval for a slower-moving stream)
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 2. countByWindow
  // ---------------------------------------------------------------------
  def countByWindowDemo(validTxns: org.apache.spark.streaming.dstream.DStream[Transaction]): Unit = {
    val windowSeconds = recentWindow.milliseconds / 1000

    // countByWindow is the simplest windowed operation there is: it just
    // counts every element that falls inside the current window, re-emitting
    // a fresh total every slide interval. No key needed - it's a count over
    // the whole DStream, exactly like DStream.count() (Day 23) except summed
    // across MULTIPLE batches instead of just the current one.
    val windowedCount = validTxns.countByWindow(recentWindow, recentSlide)

    windowedCount.foreachRDD { (rdd, time) =>
      val n = rdd.collect().headOption.getOrElse(0L)
      println(f"\n[countByWindow] batch @ $time -> $n%3d transaction(s) in the last $windowSeconds%d s")
    }
  }

  // ---------------------------------------------------------------------
  // 3 & 4. reduceByKeyAndWindow / rolling sales totals
  // ---------------------------------------------------------------------
  def rollingSalesTotalsDemo(validTxns: org.apache.spark.streaming.dstream.DStream[Transaction]): Unit = {
    val salesWindowSeconds = salesWindow.milliseconds / 1000
    val txnAmounts = validTxns.map(t => (t.accountId, t.amount))

    // The INVERTIBLE form of reduceByKeyAndWindow: instead of recomputing
    // the sum from scratch over every RDD currently inside the window (what
    // the plain two-argument form below would do, every single slide), it
    // takes the PREVIOUS window's result and just ADDS the newly-entered
    // batch's sum while SUBTRACTING the batch that just aged out - far
    // cheaper for a long-running job with a wide window, at the cost of
    // needing an inverse function (here, simple subtraction) and a
    // checkpoint directory to stay recoverable.
    val rollingSalesByAccount = txnAmounts.reduceByKeyAndWindow(
      (a: Double, b: Double) => a + b, // reduce: combine a new value into the window
      (a: Double, b: Double) => a - b, // inverse: remove a value that just left the window
      salesWindow,
      salesSlide
    )

    rollingSalesByAccount.foreachRDD { (rdd, time) =>
      // Floating-point add-then-subtract over many slides can leave tiny
      // drift (e.g. -0.0000000002 instead of exactly 0) - round and clamp
      // so the output stays readable. A production job handling real money
      // would use integer cents instead of Double for exactly this reason.
      val rows = rdd.collect()
        .map { case (acc, total) => (acc, math.max(0.0, math.round(total * 100) / 100.0)) }
        .sortBy(-_._2)
      if (rows.nonEmpty) {
        println(f"\n[rolling sales] batch @ $time - rolling ${salesWindowSeconds}%d s total per account (top 5):")
        rows.take(5).foreach { case (acc, total) => println(f"  $acc%-10s Rs. $total%12.2f") }
      }
    }

    // reduceByWindow (no "ByKey") is the un-keyed sibling - a single rolling
    // total across EVERY account combined, same invertible technique.
    val overallRollingSales = validTxns.map(_.amount).reduceByWindow(
      (a: Double, b: Double) => a + b,
      (a: Double, b: Double) => a - b,
      salesWindow,
      salesSlide
    )
    overallRollingSales.foreachRDD { (rdd, _) =>
      val total = math.max(0.0, rdd.collect().headOption.getOrElse(0.0))
      if (total > 0) println(f"                 total rolling sales, all accounts combined: Rs. $total%14.2f")
    }
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: detect a sudden increase in transactions during a window
  // ---------------------------------------------------------------------
  def detectSuddenIncreaseScenario(validTxns: org.apache.spark.streaming.dstream.DStream[Transaction]): Unit = {
    val recentWindowSeconds = recentWindow.milliseconds / 1000.0
    val baselineWindowSeconds = baselineWindow.milliseconds / 1000.0

    // Two countByWindow calls over the SAME stream, at two different window
    // widths: a short "recent" window (the scenario's 10-minute window,
    // scaled down for the demo) and a much longer "baseline" window to
    // represent normal, everyday traffic. Tagging both with the same dummy
    // key ("x") lets them be compared side by side with an ordinary DStream
    // join - both emit one RDD per slide interval (both configured with the
    // same slide here), so the join lines them up batch by batch.
    val recentCounts = validTxns.countByWindow(recentWindow, recentSlide).map(c => ("x", c))
    val baselineCounts = validTxns.countByWindow(baselineWindow, baselineSlide).map(c => ("x", c))
    val joined = recentCounts.join(baselineCounts)

    joined.foreachRDD { (rdd, time) =>
      rdd.collect().headOption.foreach { case (_, (recentCount, baselineCount)) =>
        val recentRate = recentCount / recentWindowSeconds
        val baselineRate = baselineCount / baselineWindowSeconds
        val ratio =
          if (baselineRate > 0) recentRate / baselineRate
          else if (recentCount > 0) Double.PositiveInfinity
          else 0.0
        val isSpike = recentCount >= spikeMinAbsoluteCount && ratio >= spikeRatioThreshold

        if (isSpike) {
          println(f"\n*** ALERT @ $time *** sudden increase in transactions detected!")
          println(f"    recent rate  = $recentRate%6.2f txn/s (last ${recentWindowSeconds}%.0f s, $recentCount%d txns)")
          println(f"    baseline rate= $baselineRate%6.2f txn/s (last ${baselineWindowSeconds}%.0f s, $baselineCount%d txns)")
          println(f"    that's ${ratio}%.1fx normal - worth paging someone about, in a real system")
        } else {
          println(f"[SCENARIO] batch @ $time - recent $recentRate%5.2f txn/s vs baseline $baselineRate%5.2f txn/s " +
            f"(${ratio}%.1fx) - normal")
        }
      }
    }
  }

  /** Parses one raw CSV line into a Transaction, or None if it fails validation -
    * same stateless, line-at-a-time check used in Day 24. */
  def parseLine(line: String): Option[Transaction] = {
    if (line == null || line.trim.isEmpty) return None
    val parts = line.split(",", -1)
    if (parts.length != 5) return None

    val Array(txnId, timestamp, accountId, txnType, amountStr) = parts
    if (accountId.trim.isEmpty) return None
    if (txnType != "DEBIT" && txnType != "CREDIT") return None

    try {
      val amount = amountStr.toDouble
      if (amount <= 0) None
      else Some(Transaction(txnId, timestamp, accountId, txnType, amount))
    } catch {
      case _: NumberFormatException => None
    }
  }
}
