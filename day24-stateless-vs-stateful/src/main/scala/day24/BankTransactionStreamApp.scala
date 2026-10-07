package day24

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Day 24 — Stateless vs Stateful Streaming
 *
 * Covers:
 *   1. Implementing stateless transformations
 *   2. Stateful processing, explained conceptually
 *   3. Tracking running counts by key
 *   4. Comparing current-batch results with accumulated state
 *   5. Scenario: maintain running transaction counts per bank account
 *
 * Needs a feeder, same as Day 23. Start the Python server FIRST, in its own
 * terminal, from this project's root directory:
 *
 *   python3 scripts/txn_server.py
 *
 * ...then, in a SECOND terminal, also from this project's root:
 *
 *   sbt run
 *
 * Let it run for 30-60 seconds to see several batches (and the running
 * totals climb), then Ctrl+C BOTH terminals (the Scala app first) to stop.
 */
object BankTransactionStreamApp {

  case class Transaction(txnId: String, timestamp: String, accountId: String, txnType: String, amount: Double)

  def main(args: Array[String]): Unit = {
    // Same reasoning as Day 23: one core is permanently tied up receiving
    // from the socket, so at least 2 are needed for anything to actually
    // get PROCESSED.
    val conf = new SparkConf().setAppName("Day24-BankTransactionStreamApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Stateful operations (updateStateByKey, section 3+) need somewhere
    // DURABLE to store each batch's updated state, in case a worker dies
    // mid-stream and the state has to be recovered rather than lost. A
    // local checkpoint directory stands in here for what would be HDFS/S3
    // in a real deployment.
    ssc.checkpoint("checkpoint")

    explainStatelessVsStateful()

    val lines = ssc.socketTextStream("localhost", 9999)

    // ---------------------------------------------------------------------
    // 1. Stateless transformations
    // ---------------------------------------------------------------------
    // Parsing and validating a line only ever needs THAT line - nothing
    // about any earlier or later batch. That's the definition of
    // "stateless": map/filter/flatMap (every transformation from Day 23)
    // only ever look at the current micro-batch's RDD in isolation.
    val parsed = lines.map(parseLine)
    // DStream has no RDD-style .collect(PartialFunction) - flatMap with
    // Option's own iterator (via .toList) is the standard way to drop the
    // Nones and unwrap the Somes in one step.
    val validTxns = parsed.flatMap(_.toList)
    val invalidCount = parsed.filter(_.isEmpty).count()

    invalidCount.foreachRDD { (rdd, time) =>
      val n = rdd.collect().headOption.getOrElse(0L)
      if (n > 0) println(s"\n[stateless] batch @ $time -> $n malformed line(s) dropped (still just this batch)")
    }

    // Still entirely stateless: a per-account transaction count, but ONLY
    // for whatever arrived in THIS batch. Call this twice in a row with an
    // empty batch in between and the second result has no memory of the
    // first - reduceByKey on a DStream resets every interval, exactly like
    // every other DStream transformation.
    val currentBatchCounts = validTxns.map(t => (t.accountId, 1L)).reduceByKey(_ + _)

    // ---------------------------------------------------------------------
    // 2. Stateful processing, explained + 3. Running counts by key
    // ---------------------------------------------------------------------
    // updateStateByKey is the classic DStream stateful operation: for every
    // KEY that appears in ANY batch (not just this one), Spark hands your
    // update function (a) the new values that arrived THIS batch for that
    // key, and (b) the state left over from LAST batch (None the very first
    // time a key is seen) - and whatever you return becomes next batch's
    // "previous state" for that key. Spark keeps this running per-key state
    // alive across batches (backed by the checkpoint directory), which is
    // exactly what a stateless transformation can never do on its own.
    val updateRunningCount = (newCounts: Seq[Long], runningState: Option[Long]) => {
      val previousTotal = runningState.getOrElse(0L)
      val newTotal = previousTotal + newCounts.sum
      Some(newTotal) // Some(...) keeps the key alive; None would drop it from state entirely
    }
    val runningCountByAccount = validTxns.map(t => (t.accountId, 1L)).updateStateByKey(updateRunningCount)

    // A second piece of state, keyed differently, to show "by key" doesn't
    // have to mean just the account - here it's (account, txnType) instead,
    // breaking the running count down by DEBIT vs CREDIT per account.
    val updateByTypeCount = (newCounts: Seq[Long], runningState: Option[Long]) => {
      Some(runningState.getOrElse(0L) + newCounts.sum)
    }
    val runningCountByAccountAndType = validTxns
      .map(t => ((t.accountId, t.txnType), 1L))
      .updateStateByKey(updateByTypeCount)

    // ---------------------------------------------------------------------
    // 4. Compare current-batch results with accumulated state
    // ---------------------------------------------------------------------
    // .join on two pair DStreams performs an INNER JOIN on their keys,
    // PER BATCH - so this line prints, side by side for every account that
    // had activity this interval, "how many transactions just now" next to
    // "how many ever, for this account, since the app started".
    val comparison = currentBatchCounts.join(runningCountByAccount)

    comparison.foreachRDD { (rdd, time) =>
      val rows = rdd.collect().sortBy(_._1)
      if (rows.nonEmpty) {
        println(s"\n[STATELESS vs STATEFUL] batch @ $time")
        println(f"  ${"account"}%-10s ${"this batch"}%12s ${"running total"}%15s")
        rows.foreach { case (account, (thisBatch, running)) =>
          println(f"  $account%-10s $thisBatch%12d $running%15d")
        }
      }
    }

    // ---------------------------------------------------------------------
    // 5. Scenario: running transaction counts per bank account
    // ---------------------------------------------------------------------
    // runningCountByAccount (above) already IS the scenario - it's what a
    // real account-activity dashboard or a fraud-velocity check would be
    // built on: "how many transactions has THIS account made, total, so
    // far today" updated continuously as new transactions stream in,
    // without ever re-reading everything from the beginning.
    runningCountByAccount.foreachRDD { (rdd, time) =>
      val top = rdd.collect().sortBy(-_._2).take(3)
      if (top.nonEmpty) {
        println(s"[SCENARIO] top accounts by RUNNING total transaction count as of $time:")
        top.foreach { case (account, total) => println(f"  $account%-10s $total%5d transactions all-time") }
      }
    }
    runningCountByAccountAndType.foreachRDD { (rdd, time) =>
      val sample = rdd.collect().sortBy(r => (r._1._1, r._1._2)).take(4)
      if (sample.nonEmpty) {
        println(s"           running DEBIT/CREDIT breakdown, sample: " +
          sample.map { case ((acc, typ), n) => s"$acc/$typ=$n" }.mkString(", "))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /** Parses one raw CSV line into a Transaction, or None if it fails validation -
    * a stateless, line-at-a-time check with no memory of any other line. */
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

  def explainStatelessVsStateful(): Unit = {
    println(
      """STATELESS transformation: the output for the current batch depends ONLY on
        |the current batch's data. map/filter/flatMap/reduceByKey on a DStream are all
        |stateless - call reduceByKey on an empty batch and you get an empty result,
        |with zero memory of how much data arrived in any earlier batch. This is
        |exactly how every Day 23 transformation behaved.
        |
        |STATEFUL processing: the output depends on the current batch's data AND
        |everything that came before it, combined via a running STATE that Spark
        |keeps per key across batches (updateStateByKey below). An empty batch for
        |a key that already has state simply LEAVES that key's state unchanged -
        |the running total doesn't reset to zero just because nothing new arrived.
        |
        |The trade-off: stateful operations need a checkpoint directory (somewhere
        |durable to persist state between batches, in case of a failure) and carry
        |more overhead than a stateless one, which is why Spark makes you opt into
        |state explicitly with updateStateByKey rather than making it the default.
        |""".stripMargin)
  }
}
