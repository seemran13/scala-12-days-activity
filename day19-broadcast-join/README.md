# Day 19 — Broadcast Join

A 5,000,000-row fact table joined against a 25-row branch master, using a
broadcast join — with real `.explain()` plans and timings comparing it
against a forced shuffle sort merge join on the identical data.

## Run

```bash
sbt "run local[4]"
```

The fact table is generated at runtime with `spark.range(...)` rather than
shipped as a file, so the project stays small while still exercising a
genuinely large, distributed dataset. First run may take a little longer
while 5,000,000 rows are generated and joined a few times.

## Data

- `branch_master.csv` — 25 branches (id, name, city, region): the small
  reference/dimension table
- The fact table (`transactions`) — 5,000,000 synthetic transactions, each
  tagged with a `branchId` in `[0, 25)`, generated in-memory

## What it covers

1. **Large fact + small reference**: sizes and samples of both printed up
   front.
2. **Broadcast join**: `transactions.join(broadcast(branches),
   Seq("branchId"), "inner")` — `broadcast(...)` is a hint telling Spark to
   ship the *entire* small side to every executor and build a hash table
   there, instead of shuffling the large side at all.
3. **When it's appropriate**: covers the default 10MB
   `spark.sql.autoBroadcastJoinThreshold`, why the broadcast hint exists
   (to force it when Spark's own size estimate is wrong or stale), and the
   cases where broadcasting backfires — a "small" side that isn't actually
   small (driver OOM risk, since the whole table is collected to the
   driver first), both sides being large, or executors already under
   memory pressure.
4. **Broadcast vs shuffle sort merge, measured**: the identical join run
   twice over all 5,000,000 rows — once as a broadcast hash join, once
   forced into a sort merge join by disabling auto-broadcast — with wall
   clock times for both and an explanation of why the sort-merge version
   does strictly more work (shuffle + sort on the large side) that the
   broadcast version skips entirely.
5. **Scenario**: revenue by region, top 10 branches by revenue, and
   average transaction value by city — all computed from one broadcast
   join where the 5-million-row fact table never gets shuffled.

## Push to GitHub

```bash
git init && git add . && git commit -m "Day 19: Broadcast join"
git branch -M main
git remote add origin https://github.com/seemran13/day19-broadcast-join.git
git push -u origin main
```
