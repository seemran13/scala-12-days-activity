# Day 27 — Real-Time Banking Project

**Scenario:** a bank's transaction switch streams every deposit, withdrawal,
transfer and payment across its branches. The app keeps a running total per
account, flags an account that suddenly fires a burst of transactions in a
short window (a classic card-fraud signal), and tags any burst that also
touches a HIGH-risk branch.

## The 6 requirements, and where they live

### 1. Design a transaction event schema

`explainSchema()` in `BankTransactionStreamApp.scala` prints the full
reasoning, and the `Transaction` case class is the schema itself:

```scala
case class Transaction(
    txnId: String, accountId: String, branchId: String,
    amount: Double, txnType: String, timestamp: String
)
```

`accountId` and `branchId` are validated first/strictest in `parseLine`,
because every aggregation, window and join below keys off one or the other.
`amount` is a `Double` (acceptable for this practice pipeline; a real ledger
would use a decimal/money type to avoid floating-point rounding on sums).

### 2. Aggregate transactions by account

`aggregateByAccountDemo` computes two views side by side, same comparison
as Day 24:

- **Stateless** `perBatchTotals` — `reduceByKey(_ + _)` on `(accountId, amount)`,
  resets every batch.
- **Stateful** `runningTotals` — `updateStateByKey` tracking a running
  `(transactionCount, totalAmount)` pair per account across the whole
  application's life.

```
[ACCOUNT TOTALS] batch @ ...
  account    this batch       running (count, sum)
  ACC2003    ₹4821.50         count=14   sum=₹58210.75
```

### 3. Detect suspicious bursts using windows

`detectSuspiciousBurstsDemo` uses an invertible `reduceByKeyAndWindow`
(Day 25's technique) to count each account's transactions over the last
20 seconds, re-evaluated every 4 seconds. An account hitting
`burstCountThreshold` (6) or more transactions in that window fires a
`[BURST ALERT]` — independent of whether those transactions were
consecutive, catching "many transactions in a short time" even with other
activity mixed in.

### 4. Join transactions with small branch/risk reference data

`branches.csv` (8 rows) is loaded with plain `scala.io.Source` (no Spark
read) and broadcast once via `sc.broadcast(...)`. Looking up a branch's
name/city/risk level with `branchesBC.value.getOrElse(...)` inside a `map`
is a **broadcast hash join** in everything but name — the same "small
reference table, broadcast it" technique as Day 19's DataFrame join and
Day 26's patient directory, here applied to a live stream. Every burst
alert is tagged `(HIGH RISK BRANCH ACTIVITY)` if any of that account's
transactions in the current batch went through a HIGH-risk branch.

### 5. Use cache/persist and partitioning

- **Cache:** `validTxns.cache()` — this DStream feeds three independent
  downstream chains (account aggregation, burst detection, the branch join),
  so without caching, `parseLine` and everything upstream would be
  recomputed from scratch three times per batch. Same lesson as Day 12
  (RDD), Day 22 (DataFrame), and Day 26 (DStream).
- **Partitioning:** `explainPartitioning` prints the mechanics and then
  prints each batch's real partition count. `socketTextStream` opens exactly
  one receiver, so every micro-batch RDD starts with a single partition —
  `validTxns.repartition(4)` spreads the downstream work across more
  partitions. Separately, `reduceByKey`/`reduceByKeyAndWindow`/
  `updateStateByKey` all partition their *output* by key (`accountId`) via
  Spark's default `HashPartitioner`, which is what makes a running
  per-account total correct without extra shuffling.

### 6. Explain how the application would run on YARN

`explainYarnDeployment()` prints a full walkthrough: the `spark-submit`
command with `--master yarn --deploy-mode cluster`, why cluster mode (not
client) suits a long-running streaming job, what `--num-executors`/
`--executor-cores` actually request from YARN's ResourceManager, why
`ssc.checkpoint(...)` has to point at HDFS once this isn't running on one
local machine, why the single socket receiver is the one piece that does
*not* scale out on YARN (replaced by Kafka in a real deployment), and why
dynamic allocation is normally left off for a steady-drip streaming workload
like this one.

## The feeder: `scripts/txn_feed_server.py`

A TCP server standing in for the bank's transaction switch. Every ~2 seconds
it sends one transaction from a random "normal" account through an ordinary
branch (plus an occasional malformed line to exercise `parseLine`'s
validation path):

| Accounts | Behavior |
|---|---|
| `ACC2001`–`ACC2010` | "normal" — one transaction every couple of seconds through a LOW/MEDIUM-risk branch. Never enough in a 20s window to trip the burst threshold (6). |
| `ACC2011` | "bursty" — every ~50s, fires 8-10 transactions 1.5s apart through an ordinary branch. Tuned to trip `[BURST ALERT]` without the risk tag. |
| `ACC2012` | "bursty + risky" — same burst pattern, but every transaction routes through `BR07`/`BR08` (the two HIGH-risk branches). Tuned to trip `[BURST ALERT] (HIGH RISK BRANCH ACTIVITY)`. |

## How to run

**Terminal 1** (start first, from the project root):

```bash
python3 scripts/txn_feed_server.py
```

Wait for `listening on localhost:9999 - waiting for the Spark app to connect...`.

**Terminal 2** (from the project root, after Terminal 1 is listening):

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH
sbt run
```

Let it run for **at least 2 minutes** — long enough to see both bursty
accounts fire at least once (every ~50s), and to see `[PARTITIONS]` and
`[ACCOUNT TOTALS]` print every batch.

Stop with Ctrl+C in **Terminal 2 first** (the Scala app), then Terminal 1.

## Things to try

- Watch `ACC2011` trip a plain `[BURST ALERT]` and `ACC2012` trip the same
  alert with `(HIGH RISK BRANCH ACTIVITY)` appended — same burst size, the
  join is what tells them apart.
- Compare the `[ACCOUNT TOTALS]` running sum against the per-batch sum —
  the running one only ever grows, the per-batch one resets every 2s.
- Lower `burstCountThreshold` in `BankTransactionStreamApp.scala` and
  re-run to make normal accounts occasionally trip the alert too.
- Try changing `.repartition(4)` to `.repartition(1)` and watch the
  `[PARTITIONS]` line — the rest of the pipeline still works, just with
  less parallelism available downstream.
