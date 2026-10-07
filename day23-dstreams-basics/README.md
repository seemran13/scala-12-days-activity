# Day 23 — DStreams Basics

Covers:
1. Creating a `StreamingContext` and defining a batch interval
2. Reading a socket/text stream
3. `map` / `filter` / `flatMap` on a DStream
4. Micro-batch processing, explained
5. Scenario: stream application logs, count ERROR messages every interval

This is Spark's original streaming API (`org.apache.spark.streaming`,
"DStreams" — Discretized Streams), distinct from the newer Structured
Streaming API. It's still fully supported in Spark 3.5.1 and is what "batch
interval" / "micro-batch" questions are usually about at this stage of a
curriculum.

## Why this day needs two terminals (and sometimes three)

Every previous day's app was self-contained: run `sbt run`, it reads a file
that's already sitting on disk, and it finishes. **Streaming apps don't
finish** — they sit there waiting for new data forever, so something else
has to be *producing* that data while the Spark app runs. This project ships
two small Python scripts for exactly that job (nothing to install — Python 3
is already on your WSL machine).

## Project layout

```
day23-dstreams-basics/
  build.sbt
  project/build.properties
  src/main/resources/
    log4j2.properties
    sample_app_logs.txt         <- 335 lines: ~60% INFO, ~20% WARN, ~15% ERROR,
                                    plus lowercase/mixed-case levels, a line
                                    whose message just happens to contain
                                    "Error" as a component name, and a few
                                    blank lines
  src/main/scala/day23/
    SocketLogStreamApp.scala    <- socketTextStream + the full scenario
    FileLogStreamApp.scala      <- textFileStream (the file half of "socket/text")
  scripts/
    log_server.py                <- feeds SocketLogStreamApp over TCP
    file_stream_feeder.py        <- feeds FileLogStreamApp by dropping files
```

## How to run: the socket scenario (main demo)

**Terminal 1** — start the log server first (it waits for Spark to connect):
```bash
cd ~/scala-spark-practice/day23-dstreams-basics
python3 scripts/log_server.py
```

**Terminal 2** — then start the Spark app:
```bash
cd ~/scala-spark-practice/day23-dstreams-basics
sbt run
```
(No `local[2]`/`local[4]` argument this time — see "Why `local[2]` is
hardcoded" below.)

Watch Terminal 2 for about 30-60 seconds. Every ~5 seconds you'll see a new
batch's `map`/`flatMap` demo output, then the scenario's ERROR count for
*that interval only*, plus a couple of sample ERROR lines. When you've seen
enough, **Ctrl+C in Terminal 2 first**, then **Ctrl+C in Terminal 1**.

## How to run: the file-stream half

```bash
cd ~/scala-spark-practice/day23-dstreams-basics
sbt "runMain day23.FileLogStreamApp"
```
(`runMain` is needed because this project now has two `main` methods — plain
`sbt run` would ask you to pick one interactively.)

Then, in a second terminal, either drop one file in by hand:
```bash
cd ~/scala-spark-practice/day23-dstreams-basics
cp src/main/resources/sample_app_logs.txt streaming_input/batch1.txt
```
or let the feeder script keep dropping new ones automatically:
```bash
python3 scripts/file_stream_feeder.py
```

## Concepts, mapped to the code

### 1. StreamingContext and batch interval
```scala
val conf = new SparkConf().setAppName("...").setMaster("local[2]")
val ssc = new StreamingContext(conf, Seconds(5))
```
`Seconds(5)` is the **batch interval**: every 5 seconds, Spark closes off
whatever arrived since the last interval and turns it into one RDD. Every
DStream transformation you write really means "run this against each
interval's RDD as it's produced."

### Why `local[2]` is hardcoded (not `local[*]`/args like earlier days)
A DStream app permanently dedicates **one core to the receiver** — the
thread that sits there reading the socket. With only 1 core total, data
would be *received* but never actually *processed*; batches would just queue
up forever and nothing would print. Streaming always needs at least
`(number of receivers + 1)` cores, so both apps hardcode `local[2]` instead
of taking a master URL as an argument like every earlier day's app did.

### 2. Reading a socket/text stream
- **Socket** (`SocketLogStreamApp`): `ssc.socketTextStream("localhost", 9999)`
  opens a TCP connection and turns every line received into one DStream
  element. `log_server.py` is the thing sending those lines — it replays
  `sample_app_logs.txt` on a loop with small random delays, so the feed
  never runs dry.
- **Text/file** (`FileLogStreamApp`): `ssc.textFileStream("streaming_input")`
  monitors a directory and turns every *new, complete* file dropped into it
  into part of the next batch — no socket needed, but Spark only notices
  files that appear **after** the stream starts, and expects each file to
  show up atomically (hence the write-to-`.tmp`-then-rename trick in
  `file_stream_feeder.py`).

### 3. map / filter / flatMap on a DStream
```scala
val upperLines = lines.map(_.toUpperCase)
val nonBlankLines = lines.filter(_.trim.nonEmpty)
val words = nonBlankLines.flatMap(_.split("\\s+")).filter(_.nonEmpty)
```
Exactly the same transformations as a plain RDD (Day 5) — because a DStream
*is* just a sequence of RDDs over time. Calling `.map`/`.filter`/`.flatMap`
on a DStream applies that transformation to every micro-batch's RDD
automatically, without writing your own loop over batches.

### 4. Micro-batch processing, explained
Printed in full by `explainMicroBatching()` at startup, in short: DStreams
don't process one record the instant it arrives (that's a true
record-at-a-time engine, like Flink). Spark Streaming buffers everything
that shows up during one batch interval, bundles it into one RDD, and runs
your whole pipeline against that RDD like a tiny batch job — hence
"micro-batch." The trade-off is latency bounded by the batch interval (never
faster than ~5 seconds here) in exchange for reusing the entire RDD API on
every batch for free.

### 5. Scenario: count ERROR messages every interval
```scala
val errorLines = nonBlankLines.filter(_.toUpperCase.contains("ERROR"))
val errorCountPerBatch = errorLines.count()   // DStream[Long] - ONE count per batch
errorCountPerBatch.foreachRDD { (rdd, time) =>
  val count = rdd.collect().headOption.getOrElse(0L)
  println(f"[SCENARIO] batch @ $time -> $count%3d ERROR line(s) in THIS interval alone")
}
```
`.count()` on a DStream returns a *new* DStream of one `Long` per batch — the
count for **just that interval**, not a running total since the app started.
That's deliberate: it's the cleanest way to see that each micro-batch is
independent unless you reach for a *stateful* DStream operation
(`updateStateByKey`/`mapWithState`, a topic of its own, beyond today). The
sample ERROR lines are printed alongside the count, same as a real
log-monitoring job would keep detail behind an alert number for drill-down.

## Things to try

- Let `log_server.py` run for a couple of minutes and watch the ERROR count
  bounce around batch to batch — it's never cumulative, by design.
- Stop `log_server.py` mid-run (Ctrl+C) without stopping the Spark app —
  you'll see batches with 0 new lines (and no scenario line printed, since
  the `if count > 0` guards skip empty batches) until you reconnect it.
- Open `sample_app_logs.txt` and find the lowercase `error` and mixed-case
  `Error` lines, plus the `ErrorHandlerRegistry initialized...` line — all
  three legitimately match `.contains("ERROR")` case-insensitively, which is
  exactly the kind of false-positive/true-positive mix a real substring
  filter has to live with.
- Try changing `Seconds(5)` to `Seconds(2)` or `Seconds(10)` in
  `SocketLogStreamApp` and see how choppier or smoother the batch counts look.
