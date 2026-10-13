# Day 28 — Real-Time Booking Project

**Scenario:** a travel platform's booking/cancellation events for 10 shared
inventory resources (4 hotels, 3 flights, 3 buses) stream in from wherever a
customer books. The app keeps a running occupied-vs-available count per
resource, flags resources approaching or past capacity, surfaces which
resources are "trending", and periodically publishes a Spark SQL report
across all of it.

## The 6 requirements, and where they live

### 1. Process hotel/flight/bus booking events

`explainSchema()` and the `BookingEvent` case class:

```scala
case class BookingEvent(
    eventId: String, bookingId: String, resourceId: String,
    eventType: String, units: Int, timestamp: String
)
```

Deliberately **not** on the wire: `serviceType`, the resource's name, city,
or capacity — those live only in the broadcast resource directory
(requirement 5) and are looked up by `resourceId`, avoiding a denormalized
field that could drift out of sync with the real master record.

### 2. Maintain booking/cancellation state

`maintainBookingStateDemo` builds a Pair RDD/DStream of
`(resourceId, signedDelta)` — `+units` for a `BOOK`, `-units` for a
`CANCEL` — and feeds it into `updateStateByKey` to carry a running
net-booked count per resource forward across batches (the Day 24/26/27
running-state technique). The update function floors at 0 (no "negative
booked") but deliberately does **not** ceiling-cap at capacity, so a
resource's booked count can read above 100% — that's what the OVERBOOKED
tier below is for.

### 3. Calculate occupancy and availability

Every batch, the current `(resourceId, booked)` state is collected and
joined (on the driver, via the broadcast directory) against each resource's
capacity to compute `available` and `occupancyPct`, printed as:

```
[OCCUPANCY] batch @ ...
  H2   Seaside Resort       booked=11   / cap=10   avail=0    (110.0%) *** OVERBOOKED ***
  H1   Grand Palace         booked=22   / cap=40   avail=18   ( 55.0%)
```

Four tiers: normal, `** NEARLY FULL **` (≥90%), `*** SOLD OUT ***` (100%),
`*** OVERBOOKED ***` (>100%).

### 4. Use Pair RDD and window operations

`bookingVelocityDemo` builds an explicit Pair RDD (`map(e => (e.resourceId, 1))`
on `BOOK` events only) and counts it with an invertible `reduceByKeyAndWindow`
(Day 25's technique) over a 20-second window, re-evaluated every 4 seconds —
independent of the running occupancy state, this measures *demand* (how many
bookings a resource is getting right now) rather than net occupancy, and
prints `[TRENDING]` for any resource at 5+ bookings in the window.

### 5. Use broadcast reference data

`resources.csv` (10 rows: `resourceId,serviceType,name,city,capacity`) is
loaded with plain `scala.io.Source` and broadcast once via
`sc.broadcast(...)` — the same "small reference table, broadcast it"
technique as Days 19/26/27, used here by both the occupancy calculation
and the trending report to turn a bare `resourceId` into a name/city/type.

### 6. Create Spark SQL reporting output

Every 5th batch, inside `maintainBookingStateDemo`, the current occupancy
snapshot (already collected to the driver for requirement 3) is turned into
a `Seq[BookingStatus]`, converted to a DataFrame with `spark.implicits._`,
registered as a temp view, and queried with `spark.sql(...)`:

```
[SQL REPORT] batch @ ... - occupancy by service type
+-----------+---------+---------------+------------------+
|serviceType|resources|avgOccupancyPct|nearlyFullOrAbove |
+-----------+---------+---------------+------------------+
|HOTEL      |4        |42.3           |1                 |
|FLIGHT     |3        |18.1           |0                 |
|BUS        |3        |9.7            |0                 |
+-----------+---------+---------------+------------------+

[SQL REPORT] batch @ ... - top 3 most-occupied resources
...
```

The `SparkSession` is built from the **same** `SparkConf`/`SparkContext`
that backs the `StreamingContext`, so this batch SQL work and the streaming
pipeline share one Spark application rather than starting a second one.

## The feeder: `scripts/booking_feed_server.py`

A TCP server standing in for the platform's booking switch. Every ~2 seconds
it sends one or two ordinary events from the 8 "normal" resources (plus an
occasional malformed line to exercise `parseLine`'s validation path):

| Resource | Behavior |
|---|---|
| `H1`, `H3`, `H4`, `F1`, `F3`, `B1`, `B2`, `B3` | "normal" — steady, mostly-`BOOK` trickle, never close to capacity. |
| `H2` ("Seaside Resort", cap=10) | books hard for the first ~30s (crosses NEARLY FULL → SOLD OUT → OVERBOOKED, and trips `[TRENDING]`), then settles into mostly cancellations to partly recover. |
| `F2` ("AI305", cap=150) | fills steadily for the first ~40s, then fires a 6-event cancellation burst around the ~50s mark, then returns to a normal trickle. |

## How to run

**Terminal 1** (start first, from the project root):

```bash
python3 scripts/booking_feed_server.py
```

Wait for `listening on localhost:9999 - waiting for the Spark app to connect...`.

**Terminal 2** (from the project root, after Terminal 1 is listening):

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH
sbt run
```

Let it run for **at least 2 minutes** — long enough to watch `H2` climb
through all four occupancy tiers, see `F2`'s cancellation burst drop its
occupancy in one batch, see `[TRENDING]` fire for `H2`, and see a couple of
`[SQL REPORT]` blocks print (every 5th batch, ~10s apart).

Stop with Ctrl+C in **Terminal 2 first** (the Scala app), then Terminal 1.

## Things to try

- Watch `H2` cross NEARLY FULL → SOLD OUT → OVERBOOKED in the `[OCCUPANCY]`
  output, then partially recover as cancellations roll in later.
- Compare `F2`'s occupancy right before and right after its cancellation
  burst — one batch should show a sharp drop.
- Lower `trendingBookThreshold` or widen `velocityWindow` in
  `BookingStreamApp.scala` to make normal resources trip `[TRENDING]` too.
- Change `reportEveryNBatches` to see the SQL report print more or less
  often, or add your own `spark.sql(...)` query against `booking_status`.
