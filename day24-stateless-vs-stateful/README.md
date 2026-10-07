# Day 24 — Stateless vs Stateful Streaming

Covers:
1. Implementing stateless transformations
2. Stateful processing, explained conceptually
3. Tracking running counts by key
4. Comparing current-batch results with accumulated state
5. Scenario: maintain running transaction counts per bank account

Builds directly on Day 23's DStreams basics — same socket-streaming setup,
same batch-interval idea — but asks a new question about each batch: does
this transformation's output depend on ONLY the current batch, or on
everything that's streamed in so far?

## How to run

Same two-terminal pattern as Day 23.

**Terminal 1** — start the transaction feed server first:
```bash
cd ~/scala-spark-practice/day24-stateless-vs-stateful
python3 scripts/txn_server.py
```

**Terminal 2** — then start the Spark app (check `java -version` says 17
first, if this is a fresh terminal):
```bash
cd ~/scala-spark-practice/day24-stateless-vs-stateful
sbt run
```

Watch Terminal 2 for 30-60+ seconds — the longer it runs, the more
interesting the "running total" numbers get, since they keep climbing batch
after batch while the "this batch" numbers stay small and bounce around.
When you've seen enough, **Ctrl+C in Terminal 2 first**, then **Ctrl+C in
Terminal 1**.

## Project layout

```
day24-stateless-vs-stateful/
  build.sbt
  project/build.properties
  src/main/resources/
    log4j2.properties
    sample_transactions.txt     <- 500 lines: txnId,timestamp,accountId,type,amount
                                    for 10 accounts (ACC1001-ACC1010), with ~3%
                                    missing accountId, ~2% non-numeric amount,
                                    ~1.5% invalid type, and a few blank lines
  src/main/scala/day24/
    BankTransactionStreamApp.scala
  scripts/
    txn_server.py                <- feeds the app over TCP, port 9999
```

`checkpoint/` appears next to the project once you run it — that's Spark's
durable store for the running state (git-ignored, safe to delete between
runs if you want a completely fresh running count from zero).

## Concepts, mapped to the code

### 1. Stateless transformations
```scala
val parsed = lines.map(parseLine)
val validTxns = parsed.flatMap(_.toList)
val currentBatchCounts = validTxns.map(t => (t.accountId, 1L)).reduceByKey(_ + _)
```
`parseLine` validates one line using only that line's own content (field
count, non-empty `accountId`, a recognized `DEBIT`/`CREDIT` type, a
parseable positive `amount`) — exactly the kind of check that has no
business knowing about any other line, past or future. `reduceByKey` on the
resulting pairs is stateless too: it only ever sees the current micro-batch's
RDD, so the per-account count it produces is reset to nothing at the start
of every single interval.

### 2. Stateful processing, explained conceptually
Printed in full by `explainStatelessVsStateful()` at startup. Short version:
a **stateless** transformation's output depends only on the current batch.
A **stateful** one's output depends on the current batch *plus* a running
state Spark keeps per key across every batch since the app started. An empty
batch leaves a stateful key's value unchanged; it does **not** reset it to
zero the way a stateless `reduceByKey` effectively would.

### 3. Tracking running counts by key — `updateStateByKey`
```scala
val updateRunningCount = (newCounts: Seq[Long], runningState: Option[Long]) => {
  val previousTotal = runningState.getOrElse(0L)
  Some(previousTotal + newCounts.sum)
}
val runningCountByAccount = validTxns.map(t => (t.accountId, 1L)).updateStateByKey(updateRunningCount)
```
For every key that has *ever* appeared, Spark calls your update function
with `(the new values that arrived this batch, the state left over from last
batch)` and keeps whatever you return as the new state. `ssc.checkpoint("checkpoint")`
(set once near the top of `main`) is what makes this durable — `updateStateByKey`
refuses to run without a checkpoint directory configured, because losing
that running state on a worker failure with no way to recover it would make
"running total" meaningless. A second state, keyed by `(accountId, txnType)`
instead of just `accountId`, shows that "by key" isn't limited to a single
column — it's whatever key you group by.

### 4. Comparing current-batch results with accumulated state
```scala
val comparison = currentBatchCounts.join(runningCountByAccount)
```
`.join` on two pair DStreams is an inner join **per batch**: for every
account active in a given interval, it prints "this batch" next to "running
total" side by side — the clearest possible contrast between a stateless
result (small, resets every interval) and a stateful one (only ever grows,
or stays flat, never resets).

### 5. Scenario: running transaction counts per bank account
`runningCountByAccount` *is* the scenario — this is exactly what a real
account-activity dashboard, or a simple fraud-velocity check ("has this
account made an unusual number of transactions today?"), would be built on:
a continuously updated per-account count that never needs to re-scan
everything from the beginning to answer "how many so far?". Every batch also
prints the top 3 accounts by running total and a sample of the
`(account, type)` breakdown, so you can watch specific accounts climb over
successive batches.

## Things to try

- Watch the SAME account across several `[STATELESS vs STATEFUL]` batch
  printouts — "this batch" jumps around (0, 1, 3...) while "running total"
  only ever goes up.
- Kill `txn_server.py` mid-run without stopping the Spark app, wait ~15
  seconds, then restart it — the running totals pick up exactly where they
  left off (state survived the gap), while "this batch" simply shows nothing
  until new data arrives again.
- Delete the `checkpoint/` directory and re-run from scratch — the running
  totals start back at zero, since that directory is the only place the
  accumulated state lives.
- Open `sample_transactions.txt` and find a line with a missing `accountId`,
  one with `N/A` for amount, and one with the bogus `XFER` type — all three
  get silently dropped by `parseLine`, which is exactly why the
  `[stateless] batch @ ... -> N malformed line(s) dropped` line exists: to
  make that otherwise-invisible filtering visible.
