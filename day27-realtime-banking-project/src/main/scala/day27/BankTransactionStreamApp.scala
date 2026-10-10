package day27

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * Day 27 — Real-Time Banking Project
 *
 * Covers:
 *   1. Designing a transaction event schema
 *   2. Aggregating transactions by account
 *   3. Detecting suspicious bursts using windows
 *   4. Joining transactions with small branch/risk reference data
 *   5. Using cache/persist and partitioning
 *   6. Explaining how the application would run on YARN
 *
 * Scenario: a bank's transaction switch streams every card swipe, transfer,
 * deposit and withdrawal across its branches. The app has to keep a running
 * total per account, flag an account that suddenly fires off a burst of
 * transactions in a short window (classic card-fraud signal), and tag any
 * of that activity that touches a HIGH-risk branch.
 *
 * Needs a feeder, same pattern as Days 23-26. Start it FIRST, in its own
 * terminal, from this project's root directory:
 *
 *   python3 scripts/txn_feed_server.py
 *
 * ...then, in a SECOND terminal, also from this project's root:
 *
 *   sbt run
 *
 * Accounts ACC2011 and ACC2012 are deliberately scripted to fire a burst of
 * transactions roughly once a minute, with ACC2012's burst routed through a
 * HIGH-risk branch - watch for both the plain [BURST ALERT] and the
 * upgraded "(HIGH RISK BRANCH ACTIVITY)" tag. Let it run for at least
 * 2 minutes, then Ctrl+C BOTH terminals (the Scala app first) to stop.
 */
object BankTransactionStreamApp {

  case class Transaction(
      txnId: String,
      accountId: String,
      branchId: String,
      amount: Double,
      txnType: String,
      timestamp: String
  )

  case class BranchInfo(branchName: String, city: String, riskLevel: String)

  val burstWindow = Seconds(20)
  val burstSlide = Seconds(4)
  val burstCountThreshold = 6 // 6+ transactions from one account within the window -> flag a burst

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day27-BankTransactionStreamApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint") // required by updateStateByKey and the invertible reduceByKeyAndWindow

    explainSchema()

    // ---------------------------------------------------------------------
    // 4. Join transactions with small branch/risk reference data
    // ---------------------------------------------------------------------
    // branches.csv has 8 rows - far too small to justify a shuffle join.
    // Broadcasting it once and looking it up with a plain Map.get inside a
    // map() is a BROADCAST HASH JOIN in everything but name: every executor
    // gets its own full copy up front, so enriching a transaction with its
    // branch's name/city/risk level costs a hash-map lookup, never a
    // network shuffle - the same "small reference table, broadcast it"
    // technique as Day 19's DataFrame broadcast join and Day 26's patient
    // directory, just applied here to streaming data.
    val branchDirectory = loadBranchDirectory()
    val branchesBC = sc.broadcast(branchDirectory)

    val lines = ssc.socketTextStream("localhost", 9999)
    val parsedOpt = lines.map(parseLine)
    val validTxns: DStream[Transaction] = parsedOpt.flatMap(_.toList)

    // ---------------------------------------------------------------------
    // 5. Cache/persist and partitioning
    // ---------------------------------------------------------------------
    // validTxns feeds THREE independent downstream action chains below
    // (per-account aggregation, burst detection, and the enrichment/alert
    // pipeline) - the same "reused multiple times" shape as Day 12 (RDD),
    // Day 22 (DataFrame) and Day 26 (DStream), so it's cached here for the
    // same reason: without this, parseLine and every upstream step would be
    // recomputed from scratch three times, every single batch.
    validTxns.cache()

    explainPartitioning(validTxns)
    val repartitioned = validTxns.repartition(4)

    // ---------------------------------------------------------------------
    // 2. Aggregate transactions by account
    // ---------------------------------------------------------------------
    aggregateByAccountDemo(repartitioned)

    // ---------------------------------------------------------------------
    // 3. Detect suspicious bursts using windows
    //    (+ tag bursts that also touch a HIGH-risk branch, closing the loop
    //    with requirement 4's broadcast join)
    // ---------------------------------------------------------------------
    detectSuspiciousBurstsDemo(repartitioned, branchesBC)

    explainYarnDeployment()

    ssc.start()
    ssc.awaitTermination()
  }

  // ---------------------------------------------------------------------
  // 1. Transaction event schema
  // ---------------------------------------------------------------------
  def explainSchema(): Unit = {
    println(
      """Event schema for one bank transaction (one line on the wire = one event):
        |
        |  txnId       String  - unique id for this transaction, carried
        |                        through for audit/dedup purposes only -
        |                        nothing here groups or aggregates by it.
        |  accountId   String  - which account; the KEY every aggregation,
        |                        windowed count, and burst check below
        |                        groups by.
        |  branchId    String  - which physical branch processed it; the KEY
        |                        the broadcast join resolves against the
        |                        branch/risk reference table.
        |  amount      Double  - transaction amount, in rupees. A Double, not
        |                        an Int or a fixed-point type - acceptable
        |                        for this practice pipeline, though a real
        |                        ledger would use a decimal/money type to
        |                        avoid floating-point rounding on sums.
        |  txnType     String  - DEPOSIT / WITHDRAWAL / TRANSFER / PAYMENT.
        |  timestamp   String  - when the switch recorded it (ISO-8601).
        |                        Kept as a String (not parsed to a Spark SQL
        |                        timestamp) since nothing here needs date
        |                        arithmetic on it, only display/audit.
        |
        |accountId and branchId are validated first and strictest in
        |parseLine, since every operation below keys off one or the other.
        |A real core-banking feed would also carry a deviceId/channel
        |(ATM, POS, mobile, net-banking), a currency code, and a
        |running ledger sequence number per account - left out here to keep
        |one line of CSV readable at a glance.
        |""".stripMargin)
  }

  /** Parses one raw CSV line into a Transaction, or None if it fails
    * validation - same stateless, line-at-a-time check used in Days 24-26. */
  def parseLine(line: String): Option[Transaction] = {
    if (line == null || line.trim.isEmpty) return None
    val parts = line.split(",", -1)
    if (parts.length != 6) return None

    val Array(txnId, accountId, branchId, amountS, txnType, timestamp) = parts
    if (accountId.trim.isEmpty || branchId.trim.isEmpty) return None

    val validTypes = Set("DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT")
    if (!validTypes.contains(txnType)) return None

    try {
      val amount = amountS.toDouble
      if (amount <= 0) None
      else Some(Transaction(txnId, accountId, branchId, amount, txnType, timestamp))
    } catch {
      case _: NumberFormatException => None
    }
  }

  /** Reads the small branch reference file with plain Scala I/O - no Spark
    * needed for a lookup table this size, same spirit as Day 26's patient
    * directory. */
  def loadBranchDirectory(): Map[String, BranchInfo] = {
    val src = scala.io.Source.fromFile("src/main/resources/branches.csv")
    try {
      src.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>
        val Array(id, name, city, risk) = line.split(",", -1)
        id -> BranchInfo(name, city, risk)
      }.toMap
    } finally {
      src.close()
    }
  }

  // ---------------------------------------------------------------------
  // 5. Cache/persist and partitioning (explanation + a live before/after)
  // ---------------------------------------------------------------------
  def explainPartitioning(validTxns: DStream[Transaction]): Unit = {
    println(
      """Partitioning note:
        |
        |  socketTextStream opens exactly ONE receiver, so every micro-batch
        |  RDD it produces starts life with just ONE partition - all of that
        |  batch's parsing and filtering runs on a single core, no matter how
        |  many cores or executors the cluster actually has. That's fine at
        |  a few dozen transactions per batch, but would bottleneck hard on
        |  a real feed.
        |
        |  The fix demonstrated below: validTxns.repartition(4) reshuffles
        |  each batch's RDD across 4 partitions before the expensive work
        |  (parsing was already done, but the aggregation/window/join logic
        |  downstream now runs in parallel). The per-batch partition counts
        |  print below so you can see the 1 -> 4 jump yourself.
        |
        |  Separately, reduceByKey/reduceByKeyAndWindow/updateStateByKey all
        |  partition their OUTPUT by key (accountId) using Spark's default
        |  HashPartitioner - every transaction for a given account lands on
        |  the same partition, which is what makes a running per-account
        |  state/total correct without any extra shuffling beyond that one
        |  grouping step.
        |""".stripMargin)

    validTxns.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty()) {
        println(f"[PARTITIONS] batch @ $time - validTxns (pre-repartition) has ${rdd.getNumPartitions}%d partition(s)")
      }
    }
  }

  // ---------------------------------------------------------------------
  // 2. Aggregate transactions by account (stateless this-batch vs stateful running)
  // ---------------------------------------------------------------------
  def aggregateByAccountDemo(validTxns: DStream[Transaction]): Unit = {
    // Stateless: resets every batch, same semantics as Day 24's per-batch counts.
    val perBatchTotals: DStream[(String, Double)] =
      validTxns.map(t => (t.accountId, t.amount)).reduceByKey(_ + _)

    // Stateful: a running (transactionCount, totalAmount) pair per account,
    // carried forward across batches via updateStateByKey - the Day 24
    // "running total" technique, now tracking two numbers instead of one.
    val perAccountDelta: DStream[(String, (Int, Double))] =
      validTxns.map(t => (t.accountId, (1, t.amount)))

    val updateRunning = (newDeltas: Seq[(Int, Double)], state: Option[(Int, Double)]) => {
      val (prevCount, prevSum) = state.getOrElse((0, 0.0))
      val addedCount = newDeltas.map(_._1).sum
      val addedSum = newDeltas.map(_._2).sum
      Some((prevCount + addedCount, prevSum + addedSum))
    }
    val runningTotals = perAccountDelta.updateStateByKey(updateRunning)

    // Join this batch's stateless totals against the stateful running
    // totals so both are visible side by side - exactly Day 24's
    // "this batch vs running total" comparison.
    val combined = perBatchTotals.join(runningTotals)

    combined.foreachRDD { (rdd, time) =>
      val rows = rdd.collect().sortBy { case (accountId, _) => accountId }
      if (rows.nonEmpty) {
        println(f"\n[ACCOUNT TOTALS] batch @ $time")
        println(f"  ${"account"}%-10s ${"this batch"}%-16s running (count, sum)")
        rows.foreach { case (accountId, (batchSum, (runCount, runSum))) =>
          println(f"  $accountId%-10s ₹$batchSum%-15.2f count=$runCount%-4d sum=₹$runSum%.2f")
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // 3 + 4. Detect suspicious bursts using windows, tagged with branch risk
  // ---------------------------------------------------------------------
  def detectSuspiciousBurstsDemo(
      validTxns: DStream[Transaction],
      branchesBC: org.apache.spark.broadcast.Broadcast[Map[String, BranchInfo]]
  ): Unit = {
    // Invertible reduceByKeyAndWindow (Day 25's technique): how many
    // transactions has each account made in the last burstWindow,
    // re-evaluated every burstSlide - independent of whether those
    // transactions were consecutive, catching "many transactions in a
    // short time" regardless of what else the account did in between.
    val txnCountByAccount: DStream[(String, Int)] = validTxns
      .map(t => (t.accountId, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, (a: Int, b: Int) => a - b, burstWindow, burstSlide)

    // Which accounts touched a HIGH-risk branch during the SAME burstWindow -
    // windowed with the exact same (burstWindow, burstSlide) as
    // txnCountByAccount above, since DStream.leftOuterJoin requires both
    // sides to share one slide duration.
    val highRiskInWindow: DStream[(String, Boolean)] = validTxns
      .map { t =>
        val info = branchesBC.value.getOrElse(t.branchId, BranchInfo("Unknown", "Unknown", "UNKNOWN"))
        (t.accountId, info.riskLevel == "HIGH")
      }
      .reduceByKeyAndWindow((a: Boolean, b: Boolean) => a || b, burstWindow, burstSlide)

    val withRiskFlag = txnCountByAccount.leftOuterJoin(highRiskInWindow)

    withRiskFlag.foreachRDD { (rdd, time) =>
      val bursts = rdd.filter { case (_, (count, _)) => count >= burstCountThreshold }
        .collect().sortBy { case (_, (count, _)) => -count }
      bursts.foreach { case (accountId, (count, touchedHighRisk)) =>
        val tag = if (touchedHighRisk.getOrElse(false)) " (HIGH RISK BRANCH ACTIVITY)" else ""
        val windowSeconds = burstWindow.milliseconds / 1000
        println(f"\n[BURST ALERT] batch @ $time - $accountId%-10s had $count%d transactions " +
          f"in the last $windowSeconds%d s$tag")
      }
    }
  }

  // ---------------------------------------------------------------------
  // 6. How this would run on YARN
  // ---------------------------------------------------------------------
  def explainYarnDeployment(): Unit = {
    println(
      """Running this on a YARN cluster instead of local[2]:
        |
        |  spark-submit \
        |    --class day27.BankTransactionStreamApp \
        |    --master yarn \
        |    --deploy-mode cluster \
        |    --num-executors 4 \
        |    --executor-cores 2 \
        |    --executor-memory 2g \
        |    --driver-memory 1g \
        |    --conf spark.yarn.maxAppAttempts=2 \
        |    target/scala-2.12/day27-realtime-banking-project_2.12-0.1.0.jar
        |
        |  deploy-mode cluster vs client:
        |    - cluster: YARN's ResourceManager launches the DRIVER itself
        |      inside an ApplicationMaster container on a cluster node. The
        |      app survives the submitting machine disconnecting - the right
        |      choice for a long-running streaming job like this one.
        |    - client: the driver stays on the machine that ran spark-submit
        |      (useful for interactive debugging, but if that machine or its
        |      network drops, the whole streaming app dies with it).
        |
        |  Executors: --num-executors 4 --executor-cores 2 requests 4
        |  containers of 2 cores each from YARN's ResourceManager, running on
        |  whichever NodeManagers have room - this is what actually gives the
        |  repartition(4) above somewhere real to parallelize across, instead
        |  of 4 partitions competing for local[2]'s 2 cores.
        |
        |  Checkpointing: ssc.checkpoint("checkpoint") must point at HDFS (or
        |  another cluster-visible filesystem, e.g. "hdfs:///checkpoints/day27")
        |  rather than a local path once this runs on YARN - every executor
        |  needs to read/write the SAME checkpoint location, and a local path
        |  would only exist on whichever single machine happens to run it.
        |
        |  The socket receiver itself is the one piece that does NOT scale
        |  out on YARN: ssc.socketTextStream uses exactly one receiver
        |  (occupying one executor core permanently just to listen), so no
        |  matter how many executors are requested, ingestion stays capped at
        |  what one TCP connection can deliver - a real deployment would
        |  replace it with a partitioned source (Kafka, say) to actually use
        |  more than one ingestion worker.
        |
        |  Dynamic allocation (spark.dynamicAllocation.enabled) is normally
        |  turned OFF for streaming apps like this - the workload is a steady
        |  drip rather than bursty batch jobs, so there's rarely idle
        |  executors worth giving back, and scaling executors up/down mid-
        |  stream risks disrupting a receiver or a stateful operator's
        |  checkpointed partitions.
        |""".stripMargin)
  }
}
