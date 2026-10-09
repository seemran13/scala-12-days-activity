# Day 26 — Real-Time Healthcare Project

A capstone that combines everything from Days 23-25 (sockets, stateless
transformations, broadcast joins, accumulators, `updateStateByKey`,
`reduceByKeyAndWindow`) into one realistic patient-monitoring pipeline —
the streaming counterpart to Day 22, which combined the batch techniques
from Days 13-21 into one pipeline.

**Scenario:** a hospital ward of 12 patients, each with a bedside monitor
sending one vital-sign reading every ~2 seconds over a TCP socket. The
Spark app has to flag abnormal readings in real time, and separately catch
two different *patterns* of trouble: a patient whose vitals are abnormal
several times in a row (worsening), and a patient whose vitals are abnormal
often but scattered (unstable).

## The 5 requirements, and where they live

### 1. Design a patient-vital event schema

`explainSchema()` in `VitalStreamApp.scala` prints the full reasoning, and
the `VitalReading` case class is the schema itself:

```scala
case class VitalReading(
    patientId: String, timestamp: String,
    heartRate: Double, spo2: Double,
    systolicBP: Double, diastolicBP: Double, temperature: Double
)
```

- `patientId` is a `String` and is validated first/strictest in `parseLine`,
  because it's the key every downstream broadcast lookup, accumulator
  tally, `updateStateByKey`, and `reduceByKeyAndWindow` operation groups by.
- Vitals are `Double`, not `Int` — they're measured, not counted (98.4°F is
  a legitimate reading).
- `timestamp` is kept as a plain `String` (not parsed into a Spark SQL
  timestamp) since nothing here needs date arithmetic on it, only display.
- A real hospital feed would likely also carry a `deviceId`, a schema
  version, and a unit field per vital — left out here to keep one line of
  CSV readable at a glance, but called out in the comment as what a
  production schema would add.

### 2. Process vital streams

Same stateless shape as Days 24-25: `ssc.socketTextStream("localhost", 9999)`
→ `lines.map(parseLine)` → `Option[VitalReading]` → `flatMap(_.toList)` to
drop the `None`s into a clean `DStream[VitalReading]` called `validReadings`.

`validReadings.cache()` is called deliberately — this DStream feeds **three**
independent downstream action chains (the alert pipeline, the stateful
streak detector, and the windowed frequency detector). Without caching,
`parseLine` and everything upstream of it would be recomputed from scratch
three times per batch — the same lesson as Day 12 (RDD `persist`) and
Day 22 (DataFrame `persist`), now applied to a live stream.

### 3. Create abnormal-vital alerts

`checkAbnormal` / `isAbnormal` are pure functions comparing each of the 5
vitals against its broadcast `[min, max]` range. `processVitalStreams`
prints an `[ALERT]` block every batch that has one or more abnormal
readings, e.g.:

```
[ALERT] batch @ ... - 2 abnormal reading(s):
  PAT012   (Deepa Nambiar    General   ) -> heartRate=142.0, spo2=87.0
```

### 4. Use broadcast thresholds and accumulators

**Broadcast:**
- `thresholdsBC` — the `Map[String, (Double, Double)]` of healthy vital
  ranges, shipped to every executor once via `sc.broadcast(...)` instead of
  being re-serialized into every task's closure.
- `patientsBC` — a `Map[String, PatientInfo]` loaded from
  `src/main/resources/patients.csv` with plain `scala.io.Source` (no Spark
  read at all), broadcast the same way. This deliberately shows that a
  broadcast variable doesn't have to come from a DataFrame/RDD — any small,
  read-only, frequently-used Scala value qualifies.

**Accumulators** (`totalReadingsAcc`, `invalidReadingsAcc`,
`abnormalReadingsAcc`, `criticalEscalationsAcc`, plus a
`CollectionAccumulator[String]` tallying *which* vitals go abnormal most
often) are **true running totals for the life of the whole application** —
unlike every DStream count in Days 23-25, which resets every batch or
window. `accumulatorSummaryDemo` prints a running summary every 5 batches
(~10s):

```
[ACCUMULATOR SUMMARY] as of ...
  total readings processed  : 540
  invalid/malformed dropped : 9
  abnormal readings flagged : 61
  critical escalations fired: 2
  abnormal-by-vital-type    : spo2=24, heartRate=22, systolicBP=15
```

Each accumulator is only ever incremented inside the single `.map(...)`
whose result is immediately `.collect()`-ed in `processVitalStreams` — the
*only* action evaluating that map per batch, so each reading is counted
exactly once (Spark's documented caveat: this "exactly once" guarantee can
break under task retries/speculative execution, not a concern on a single
local machine).

### 5. Stateful/window processing for repeated abnormal readings

Both detectors share the same input, `abnormalFlags: DStream[(String, Int)]`
— one `(patientId, 1 or 0)` pair per reading — but catch two different
patterns:

- **Stateful (`statefulRepeatedAbnormalDemo`)** — `updateStateByKey` walks
  each batch's new flags in order per patient, extending a running streak on
  a `1` and resetting it to `0` on a `0` (same running-state mechanism as
  Day 24's running transaction counts). Fires `*** CRITICAL ***` the moment
  the streak **equals** `consecutiveAbnormalThreshold` (3) — `==` rather
  than `>=` on purpose, so it escalates once per crossing instead of
  re-paging a clinician every 2 seconds while a patient stays critical, but
  will fire again if the streak resets and climbs back up a second time.

- **Windowed (`windowedRepeatedAbnormalDemo`)** — an invertible
  `reduceByKeyAndWindow` (Day 25's technique) sums abnormal flags per
  patient over the last `abnormalWindow` (20s), re-evaluated every
  `abnormalSlide` (4s). Fires `[WINDOW ALERT]` at `windowedAbnormalThreshold`
  (5) or more. Unlike the stateful streak, this doesn't care whether the
  abnormal readings were *consecutive* — 5 scattered abnormal readings in
  20 seconds trips it just as much as 5 in a row, catching patients who are
  unstable but not strictly worsening reading-over-reading.

## The feeder: `scripts/vital_feed_server.py`

A TCP server standing in for the ward's bedside monitors. Every ~2 seconds
it emits one CSV line per patient (shuffled, plus an occasional malformed
line to exercise `parseLine`'s validation path):

| Patients | Behavior |
|---|---|
| `PAT001`–`PAT009` | "normal" — vitals sampled from healthy ranges almost always; a rare (~3%) one-off blip, never enough to chain into either alert. |
| `PAT010`, `PAT011` | "unstable" — abnormal ~35% of readings, scattered rather than consecutive. Tuned to reliably trip the **windowed** detector without reliably tripping the **stateful** streak. |
| `PAT012` | "crashing" — normal for the first ~60s, then **every** vital goes and stays abnormal for ~40s straight (tachycardia, desaturation, hypertension, fever), then recovers. Tuned to reliably trip **both** detectors, since it's a sustained run of abnormal readings. |

## How to run

**Terminal 1** (start first, from the project root):

```bash
python3 scripts/vital_feed_server.py
```

Wait for `listening on localhost:9999 - waiting for the Spark app to connect...`.

**Terminal 2** (from the project root, after Terminal 1 is listening):

```bash
sbt run
```

If JDK reverts to 11 in a fresh terminal (the recurring WSL2 quirk from
earlier days), set it explicitly first:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH
```

Let it run for **at least 2 minutes** — long enough to see PAT012's crash
window (starts ~60s in, lasts ~40s) trigger both a `*** CRITICAL ***` and a
`[WINDOW ALERT]`, and to see `[ACCUMULATOR SUMMARY]` print a few times.

Stop with Ctrl+C in **Terminal 2 first** (the Scala app), then Terminal 1.
The feeder only accepts one connection per run of the Spark app in a given
session, but it loops and accepts a new connection if you restart
`sbt run` without restarting the Python server.

## Things to try

- Watch `PAT010`/`PAT011` trip `[WINDOW ALERT]` repeatedly over a long run
  without ever reaching `*** CRITICAL ***` — that's the "unstable but
  scattered" pattern by design.
- Watch `PAT012` go quiet (no alerts) for the first ~60s, then fire both
  alert types back-to-back, then go quiet again as it recovers.
- Lower `consecutiveAbnormalThreshold` or `windowedAbnormalThreshold` in
  `VitalStreamApp.scala` and re-run to see the alerts fire more/less often.
- Compare the `[ACCUMULATOR SUMMARY]`'s running totals against the alert
  log — the accumulator counts never reset, while the windowed count does.
