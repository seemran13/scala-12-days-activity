# Day 25 — Window Operations

Covers:
1. Batch interval, window size and sliding interval, explained
2. `countByWindow`
3. `reduceByKeyAndWindow`
4. Rolling sales totals
5. Scenario: detect a sudden increase in transactions during a window

Builds directly on Days 23-24's socket-streaming setup. Where Day 24 asked
"does this depend on just this batch, or on everything so far", Day 25 asks
a narrower question: "does this depend on just this batch, or on the last
**N** batches" — a window being a bounded slice of recent history, not the
unbounded running state from Day 24's `updateStateByKey`.

## About the scenario's "10-minute window"

Watching a real 10-minute window live would mean waiting 10 real minutes
between every printed result — a bad demo. This app uses a 10-**second**
window/slide instead (with a 30-second baseline window standing in for a
much longer historical baseline, like "the last hour") so you can actually
watch it react within a couple of minutes. The mechanics are identical
either way — `TransactionWindowApp.scala` has the exact constants you'd
change to run this for real (`recentWindow = Minutes(10)`, etc.), explained
in both the code comments and the app's own printed output.

## How to run

Same two-terminal pattern as Days 23-24.

**Terminal 1** — start the transaction feed server first:
```bash
cd ~/scala-spark-practice/day25-window-operations
python3 scripts/burst_txn_server.py
```

**Terminal 2** — then start the Spark app (check `java -version` says 17
first, if this is a fresh terminal):
```bash
cd ~/scala-spark-practice/day25-window-operations
sbt run
```

The feeder alternates forever between a quiet **45-second NORMAL phase**
(~1.5-3 transactions/second) and a loud **15-second BURST phase**
(~15-30 transactions/second). Let it run for at least a minute or two to see
a full cycle — watch for the `*** ALERT ***` lines when a burst phase kicks
in, and watch them stop once the rate settles back down. When you've seen
enough, **Ctrl+C in Terminal 2 first**, then **Ctrl+C in Terminal 1**.

## Project layout

```
day25-window-operations/
  build.sbt
  project/build.properties
  src/main/resources/
    log4j2.properties
  src/main/scala/day25/
    TransactionWindowApp.scala
  scripts/
    burst_txn_server.py          <- generates transactions LIVE (no sample
                                     file this time - see note below)
```

No static `sample_*.csv`/`.txt` this time: the feeder generates transactions
programmatically so it can run indefinitely and deliberately control its own
rate (normal vs. burst), which a fixed-size file replayed on a loop couldn't
do as cleanly. It still injects the same realistic failure modes as Day 24
(~2% missing `accountId`, ~1% non-numeric amount) so the parse/validate step
has genuine work to do.

`checkpoint/` appears next to the project once you run it (git-ignored) —
required by `reduceByKeyAndWindow`'s invertible form, same idea as Day 24's
`updateStateByKey` checkpoint.

## Concepts, mapped to the code

### 1. Batch interval, window size and sliding interval
Printed in full by `explainWindowConcepts()` at startup, with an ASCII
diagram of overlapping windows sliding across a sequence of batches. Short
version: **batch interval** (`Seconds(2)` here) is how often data gets cut
into a micro-batch at all — set once, for the whole `StreamingContext`.
**Window size** is how far back a windowed operation looks, spanning
multiple batches (`Seconds(10)` = the last 5 micro-batches, with a 2-second
batch interval). **Sliding interval** is how often a new windowed result
comes out — smaller than the window size gives *overlapping* windows (most
data counted in more than one consecutive result), which is what every
operation in this app uses. Both window size and sliding interval must be
exact multiples of the batch interval — Spark enforces this.

### 2. `countByWindow`
```scala
val windowedCount = validTxns.countByWindow(recentWindow, recentSlide)
```
The simplest windowed operation: no key needed, just a running count of
every element currently inside the window, re-emitted every slide interval.
It's `DStream.count()` (Day 23) generalized from "just this batch" to "the
last N batches".

### 3 & 4. `reduceByKeyAndWindow` / rolling sales totals
```scala
val rollingSalesByAccount = txnAmounts.reduceByKeyAndWindow(
  (a: Double, b: Double) => a + b,   // reduce: fold a new value into the window
  (a: Double, b: Double) => a - b,   // inverse: remove a value that just aged out
  salesWindow, salesSlide
)
```
This is the **invertible** form: rather than recomputing the sum from
scratch over everything currently in the window on every single slide (what
the plain two-argument `reduceByKeyAndWindow(reduceFunc, window, slide)`
would do), it takes the *previous* window's result, adds the newly-arrived
batch's contribution, and subtracts the batch that just fell out the other
end — much cheaper for a long-running job with a wide window. The trade-off
is needing a genuine inverse function (straightforward for `+`/`-`, not
always possible — e.g. `max` has no inverse) and a checkpoint directory. A
real floating-point gotcha is handled explicitly: repeated add-then-subtract
can leave tiny drift (like `-0.0000000002` instead of exactly `0`), so the
output is rounded and clamped — a comment notes that production code
handling real money would use integer cents instead of `Double` for exactly
this reason. `reduceByWindow` (no "ByKey") is shown alongside it as the
un-keyed sibling — one rolling total across every account combined.

### 5. Scenario: detect a sudden increase in transactions
```scala
val recentCounts = validTxns.countByWindow(recentWindow, recentSlide).map(c => ("x", c))
val baselineCounts = validTxns.countByWindow(baselineWindow, baselineSlide).map(c => ("x", c))
val joined = recentCounts.join(baselineCounts)
```
Two `countByWindow` calls over the *same* stream at two different widths — a
short "recent" window (the scenario's 10-minute window) and a much longer
"baseline" window standing in for normal, everyday traffic. Tagging both
with the same dummy key (`"x"`) lets a plain DStream `.join` line them up
batch by batch, since both are configured with the same slide interval.
Both counts are converted to a **rate** (transactions/second) so windows of
different widths are actually comparable, and an alert fires only when
*both* the ratio (recent rate ≥ 2× baseline rate) *and* an absolute floor
(≥ 5 transactions) are met — the floor exists specifically so a quiet
period's ordinary noise (e.g. 1 transaction in an otherwise-empty window vs.
0 baseline) never falsely reads as a "spike."

## Things to try

- Watch one full cycle end to end: `*** ALERT ***` lines should start
  appearing a few seconds into each BURST phase (once the recent window has
  filled with burst-rate data) and stop a few seconds after it ends (once
  the recent window has "forgotten" the burst again).
- Change `spikeRatioThreshold` from `2.0` to `5.0` and re-run — the alert
  should become harder to trigger, since the burst phase's rate has to climb
  even further above baseline before it counts as a spike.
- Change `salesSlide` from `Seconds(4)` to `Seconds(2)` (matching
  `batchInterval`) and see the rolling sales totals update twice as often.
- Try swapping the invertible `reduceByKeyAndWindow` call for the plain
  three-argument form (`reduceByKeyAndWindow(reduceFunc, salesWindow, salesSlide)`,
  no inverse function) — the printed totals should be identical, since it's
  purely a performance difference, not a correctness one.
