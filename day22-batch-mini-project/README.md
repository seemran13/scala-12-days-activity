# Day 22 — Batch Mini Project

Covers:
1. Build an end-to-end batch pipeline
2. Read raw transactions
3. Clean invalid records
4. Join customer/product data
5. Aggregate revenue and write partitioned Parquet output
6. Scenario: an e-commerce daily sales pipeline

This project stitches together nearly everything from days 13-21 (DataFrames,
joins, broadcast joins, aggregations, cache/persist, partitioned Parquet
output, repartition-before-write) into one realistic nightly batch job,
instead of introducing it as an isolated demo.

## Project layout

```
day22-batch-mini-project/
  build.sbt
  project/build.properties
  src/main/resources/
    log4j2.properties
    raw_transactions.csv   <- 8,000 rows, 30-day window, deliberately messy
    customers.csv          <- 150 customers (a few missing email/region)
    products.csv            <- 50 products across 5 categories (2 missing price)
  src/main/scala/day22/
    ECommercePipelineApp.scala
```

`raw_transactions.csv` is seeded with every failure mode a real upstream feed
produces: ~2% missing `customerId`, ~1.5% missing `productId`, ~1.5% orphan
`customerId` (well-formed but not in `customers.csv`), ~1.5% orphan
`productId`, ~1.5% non-positive `quantity` (0 or negative — a mislogged
return), ~1% malformed `transactionDate` (wrong format), and ~1% duplicate
`transactionId` (an upstream retry resending the same event).

## How to run

```bash
sbt "run local[2]"
sbt "run local[4]"
```

Everything written goes under `./output/` (git-ignored, deleted and rebuilt
at the start of every run — safe to re-run any time).

## Pipeline stages, mapped to the code

```
raw_transactions.csv (8,000 rows)
      |
      v
[STAGE 2] validate: null checks on id/customerId/productId, quantity > 0,
          parseable date, de-duplicate by transactionId
          -> output/rejected_transactions/   (quarantined, not deleted)
      |
      v
[STAGE 3] enrich: broadcast join against customers.csv and products.csv
          -> output/orphan_customer_transactions/, orphan_product_transactions/
          -> cached in memory (reused by stages 4 AND 5)
      |
      +------------------------+
      |                        |
      v                        v
[STAGE 4] aggregate       [STAGE 5] write detail fact table
daily/category/region/    partitioned by year/month/day
product revenue                 |
      |                         v
      v                  output/sales_fact_partitioned/
output/daily_revenue_summary/
```

### 1 & 2. Read raw transactions / clean invalid records — `extractRawTransactions`, `cleanTransactions`
The raw CSV is read with `inferSchema` but treated as untrusted: a
`rejectReason` column is computed with a chained `when(...).when(...)...otherwise(null)`,
checking (in priority order) missing `transactionId`, missing `customerId`,
missing `productId`, non-positive `quantity`, and an unparseable date
(`to_date(..., "yyyy-MM-dd")` returns `null` instead of throwing on a bad
format — same defensive pattern used throughout). A separate `row_number()`
window over `transactionId` catches duplicates, which can override any
single-row reason since a repeated event is a distinct problem from a
malformed one. Rejected rows are **quarantined**, not dropped —
written to `output/rejected_transactions/` with their reason attached, and a
data-quality report (`rejectReason` counts) is printed, the same shape of
report a real pipeline would push to monitoring.

### 3. Join customer/product data — `joinReferenceData`
`customers.csv` and `products.csv` are both tiny, so both joins use
`broadcast(...)` (Day 19's technique, applied here because it's actually the
right call, not just a demo). A row can pass stage 2's per-row validation and
*still* fail here if its `customerId`/`productId` doesn't exist in the
reference tables — those orphan rows are written to their own quarantine
folders, separate from stage 2's rejects, because "well-formed but doesn't
match anything" is a different failure mode than "malformed." Only rows that
matched both tables become the enriched "gold" dataset, with `revenue`
computed (`quantity * unitPrice`, with `unitPrice` coalesced to 0 first,
since two seeded products have no price). This DataFrame is `persist()`-ed
because it feeds *both* stage 4 and stage 5 — the Day 12 lesson applied for a
real reason instead of as a standalone demo.

### 4. Aggregate revenue — `aggregateRevenue`
Daily revenue (grouped by `year, month, day`), revenue by category, revenue
by region, and top-10 products by revenue — four different cuts of the same
cached `enriched` DataFrame.

### 5. Write partitioned Parquet output — `writePartitionedOutput`
The detail-level fact table is repartitioned by `(year, month, day)` *before*
`partitionBy("year", "month", "day").parquet(...)` — Day 20's lesson, so each
leaf folder gets one clean part-file instead of fragmenting. The much
smaller daily summary table is written with `coalesce(1)` instead — a single
small table doesn't need repartitioning, just a final tidy-up into one file.

## Things to try

- Open `output/rejected_transactions/` and check the `rejectReason` column —
  every row there is traceable back to exactly which rule it failed.
- Compare `output/orphan_customer_transactions/` row count to the
  `MISSING_CUSTOMER_ID` count from stage 2 — they're deliberately different
  failure modes (malformed vs. well-formed-but-unknown).
- Delete `./output/` and re-run with `local[2]` vs `local[4]` — the content
  is identical, but in-memory partition counts (and therefore some
  intermediate file counts) will differ.
- Try removing the `.persist(...)` call in `joinReferenceData` and watch
  stage 4 and stage 5 both re-run the extract/clean/join chain from scratch —
  visible as extra time, same lesson as Day 12.
