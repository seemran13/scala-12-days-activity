# Day 30 — 20 Interview Questions From This Project

Every answer below is grounded in a real decision made in
`InsuranceBatchPipelineApp.scala` or `InsuranceClaimsStreamApp.scala` — not
generic Spark trivia. Use these to practice explaining *why* you built it
this way, not just *what* it does.

---

**1. Walk me through your project's architecture in two minutes.**

A claims-processing pipeline for an insurer, with a batch half and a
streaming half sharing the same two small reference tables (policies,
customers). The batch app (`InsuranceBatchPipelineApp`) reads ~10,100
historical claims, cleans them with an RDD/Pair RDD stage, enriches them
with a two-hop broadcast join, computes a per-customer fraud risk band with
a UDF, and produces four Spark SQL reports. The streaming app
(`InsuranceClaimsStreamApp`) watches a live claim feed, resolves each
claim's policy to its customer via the same broadcast data, and flags a
customer filing an unusual burst of claims in real time. One seeded
scenario — a "fraud ring" customer with ~10% of all claims across 7
policies — is designed to be caught by both halves, in two different ways.

**2. Why did you split this into a batch app and a streaming app instead of
one combined app?**

They answer different questions on different time horizons. The batch app
computes a *historical* risk profile per customer (how many claims have
they EVER filed, across however long the data covers) — that needs the
whole dataset at once and tolerates minutes of latency. The streaming app
answers "is something suspicious happening RIGHT NOW" — a burst of claims
in the last 20 seconds — which only makes sense computed continuously as
data arrives. Different latency requirements, different right tool: a
one-shot Spark SQL job vs. a long-running Spark Streaming job.

**3. Why a Pair RDD for deduplication instead of DataFrame's `dropDuplicates`?**

Both would work; I used the Pair RDD specifically because this capstone
asked for an RDD/Pair RDD component distinct from the DataFrame work, and
deduplication is a genuinely natural fit for it:
`claims.map(c => (c.claimId, c)).reduceByKey((first, _) => first)` keeps
the first copy of each claimId. `reduceByKey` combines locally on each
partition *before* shuffling, so only one record per (partition, claimId)
pair crosses the network — `dropDuplicates` on a DataFrame does something
functionally similar under the hood via Catalyst, but doing it explicitly
at the RDD level is what makes the shuffle mechanics visible and
teachable.

**4. Why broadcast policies.csv and customers.csv instead of a regular join?**

Both are small — 280 and 150 rows — easily small enough to fit in every
executor's memory. `broadcast(...)` forces a `BroadcastHashJoin`: the whole
small table is sent to every executor once, and the large claims side is
matched locally with no shuffle at all. The alternative, letting Spark pick
a `SortMergeJoin`, would shuffle AND sort the large claims side on the join
key for no benefit, since the small side could just be sent whole instead.

**5. Your enrichment does TWO broadcast joins in a row (claims→policies→
customers). Why not join customers directly onto claims?**

Because claims don't carry a customerId — only a policyId. A customer owns
possibly many policies, so `customerId` lives on the policy record, not the
claim. The first join (claims→policies) is what *resolves* customerId in
the first place; only then can the second join (→customers) bring in
name/age/city. This mirrors the streaming app's lookup too:
`policyLookupBC` is built by pre-joining policies and customers locally
with plain Scala, specifically so the streaming side only needs ONE
broadcast lookup instead of two separate ones per claim.

**6. Explain the UDF you used and why it's a UDF instead of `when/otherwise`.**

`computeRiskBand(age, policyRiskLevel, claimFrequencyBand)` combines three
independent factors — age bracket, the policy's inherent risk level, and
how often this customer has claimed — into one label. That's up to
3 × 3 × 4 = 36 combinations; a `when/otherwise` chain covering that
cross-product would be a long, unreadable, untestable wall of conditions
duplicating the same scoring table. As a plain Scala function it's
independently unit-testable outside Spark entirely, and the scoring table
is one obvious place to tune. I deliberately did NOT use a UDF for
`claimFrequencyBand` itself (that's a plain `when` chain on one number) —
only the genuinely multi-dimensional combination got the UDF.

**7. What's the real cost of using a UDF, and why is it acceptable here?**

Catalyst's optimizer treats a UDF as an opaque black box — no predicate
pushdown, no whole-stage codegen fusion through that column, just a
row-by-row JVM call. That cost scales with how many rows it runs over.
Here it's applied to the enriched claims DataFrame, so it does run over
every claim row (~10K) — a real cost, but it's a single pass with no
join/aggregation downstream of it that Catalyst could've optimized through
anyway, so the opacity costs little in practice at this scale. At much
larger scale, I'd consider pre-computing the risk band as a batch step
written back to a lookup table, so the live/streaming side could read a
cached value instead of recomputing it per claim.

**8. Walk me through what `enriched.persist(StorageLevel.MEMORY_AND_DISK)`
actually does, and when.**

`persist()` itself does nothing immediately — it just marks the DataFrame
to be cached the NEXT time an action runs it. The very next line,
`enriched.count()`, is that action: it executes the full join/window/UDF
chain once, and as each partition's result is computed, it's also stored
in the executor's memory (spilling to disk if it doesn't fit, because I
chose MEMORY_AND_DISK over MEMORY_ONLY). Every reference to `enriched`
after that point reads from the cached blocks instead of recomputing the
joins — which matters because it feeds 4 independent downstream
aggregations plus the final write.

**9. Why MEMORY_AND_DISK instead of MEMORY_ONLY?**

MEMORY_ONLY silently drops any partition that doesn't fit in memory and
recomputes it from scratch on next use. MEMORY_AND_DISK spills the
overflow to disk instead — slower than memory, but it still avoids
recomputation. At this practice dataset's size it barely matters, but it's
the safer default for anything that might not fit, which is the realistic
production case.

**10. Explain `spark.sql.shuffle.partitions = 8`. Why not the default 200?**

200 is tuned for cluster-scale data. At ~10K rows, a 200-way shuffle means
up to 200 mostly-empty tasks per shuffle, where scheduling overhead would
dwarf the actual compute. 8 comfortably exceeds `local[2]`'s 2 cores
(leaving real parallel work available) without that overhead. Spark 3.5's
Adaptive Query Execution would eventually coalesce small post-shuffle
partitions back down even at the default, but setting it explicitly avoids
planning the excess partitions in the first place.

**11. What's `repartition(col("policyType"))` doing right before the final
write, and why not just `coalesce`?**

It hash-partitions the DataFrame by `policyType` BEFORE the partitioned
write, so all of one policy type's rows land in the same task first.
Without it, Spark would write up to one file per `policyType` PER existing
shuffle partition (8) — up to 8 small files in every `policyType=` folder
instead of close to 1. `coalesce` only reduces partition COUNT without
changing which rows land where, so it can't fix that on its own here —
`repartition(col)` is doing the grouping AND the shuffle in one step.

**12. What's the difference between the accumulator in the batch app and
the ones in the streaming app?**

The batch accumulator (`invalidClaimRows`) is a true running total across
ONE pass over the data — it's read once, right after the triggering action
(`cleanClaimsRDD.count()`) completes, then the app moves on. The streaming
accumulators (`totalClaimsSeen`, `fraudBurstAlerts`, etc.) are running
totals for the ENTIRE life of a long-running application — they keep
accumulating across every micro-batch until the app is stopped, unlike any
DStream-level count (which resets every batch or window).

**13. Why does your customer-level fraud burst detector resolve
`policyId → customerId` BEFORE the window, not after?**

Because the wire event only ever carries `policyId` — there's no
`customerId` on a live claim at all. The window function
(`reduceByKeyAndWindow`) groups by whatever key you give it, so to count
claims *per customer* (the realistic fraud-ring unit — one person filing
through several of their own policies) rather than *per policy*, the key
has to already be resolved to customerId by the time it reaches the
window. That resolution happens via the broadcast `policyLookupBC` map
inside a plain `.map(...)`, before the `reduceByKeyAndWindow` call.

**14. Your batch app uses a window function with NO `partitionBy`
(`Window.orderBy(col("totalPayout").desc)` for top-5 customers). What does
that mean for performance?**

It means the whole dataset is treated as ONE partition for ranking
purposes — Spark will warn ("No Partition Defined for Window operation")
and move all the grouped customer-payout rows to a single task to compute
a truly global rank. That's correct and necessary for a genuinely global
top-5, but it doesn't parallelize — acceptable here because it runs on the
already-aggregated per-customer summary (150 rows at most), not the raw
10K-row claims table.

**15. What's the difference between the stateful running payout
(`updateStateByKey`, per policy) and the windowed burst detector
(`reduceByKeyAndWindow`, per customer) in the streaming app?**

`updateStateByKey` keeps state FOREVER (until the app stops or the key is
explicitly cleared) — the running payout per policy only ever grows.
`reduceByKeyAndWindow` keeps a bounded, sliding view — only claims from the
last `burstWindow` (20s) count, and old ones fall out of the count as the
window slides forward every `burstSlide` (4s). One answers "what's the
total so far", the other answers "what's happening right now".

**16. Explain the DAG and why it's "lazy".**

Every transformation (map, filter, join, groupBy...) just adds a node to a
graph describing HOW to compute something — nothing runs yet. Only an
action (count, collect, show, write) triggers the DAG Scheduler to turn
that graph into real work. In this project, nothing executed until the
first `.count()` call in `rawToCleanRDD` — all the parsing/filtering logic
above it was just being described, not run.

**17. What does `cleanClaimsRDD.toDebugString` show, and how does it relate
to fault tolerance?**

It prints the RDD's lineage as text — every line is a parent RDD this one
was derived from, back to the raw text file read. That lineage is exactly
what lets Spark recover from a lost partition (an executor crash) WITHOUT
failing the whole job: instead of needing a backup copy of the data, Spark
just recomputes the missing partition by re-running the transformations
recorded in that lineage, starting from a still-available ancestor.

**18. What's a stage, and where are the stage boundaries in your batch
pipeline?**

A stage is the largest chunk of the DAG that can run as shuffle-free
(narrow) transformations back-to-back on one partition, without needing
data from any other partition. The DAG Scheduler cuts a new stage at every
shuffle boundary. In this pipeline, that's: the Pair RDD dedup
(`reduceByKey`), each Spark SQL groupBy/window aggregation, and the
`repartition` before the final write. The two broadcast joins do NOT
create new stages — broadcasting is specifically what avoids the shuffle
that would otherwise force one.

**19. How would you deploy the streaming app on YARN, and what changes
versus the batch app?**

`spark-submit --master yarn --deploy-mode cluster` for both, but cluster
mode matters far more for the streaming app: its driver has to stay alive
for the job's entire run (hours/days), so having YARN's ResourceManager
host it inside an ApplicationMaster container (surviving the submitting
machine disconnecting) is much more valuable than for a batch job that
finishes in under a minute. The streaming app also needs
`ssc.checkpoint(...)` pointed at HDFS (not a local path) once it's off one
machine, since every executor must read/write the same checkpoint
location — the batch app needs no checkpoint at all. Both apps' broadcast
joins matter even more at cluster scale: each of N executors gets ONE copy
of the small tables, not one per task.

**20. What's the biggest limitation of this design, and how would you fix
it at real production scale?**

The single `socketTextStream` receiver: no matter how many executors YARN
hands the streaming app, ingestion is capped at what one TCP connection can
deliver, because there's only ever one receiver running on one executor
core. At real scale I'd replace the socket source with Kafka, which
partitions a topic across brokers and lets Spark run multiple parallel
consumers — turning the single ingestion bottleneck into something that
actually scales with the cluster.
