# Day 20 — File Formats and Output

Covers:
1. Reading/writing CSV, JSON and Parquet
2. Writing partitioned output
3. File layout and number of output files, explained
4. Practicing `repartition()` before writing
5. Scenario: storing daily sales partitioned by year/month/day

## Project layout

```
day20-file-formats-and-output/
  build.sbt
  project/build.properties
  src/main/resources/
    log4j2.properties
    daily_sales.csv          <- 2,772 sales rows, spanning 2024-01 through 2026-09
  src/main/scala/day20/
    FileFormatsApp.scala
```

`daily_sales.csv` columns: `saleId, year, month, day, region, product, quantity, unitPrice, amount`.
It's generated with realistic edge cases on purpose:
- Not every day in every month has sales (gaps), and some days are much heavier
  than others (occasional 20-transaction days vs. typical 1-5) — enough skew to
  make `repartition` vs `coalesce` differences visible.
- ~3% of rows have a missing `amount` and ~2% have a missing `region`, so the
  scenario section has to clean nulls before aggregating, same as real data.

## How to run

```bash
sbt "run local[2]"
sbt "run local[4]"
```

Everything the app writes goes under `./output/` (git-ignored). The app deletes
and recreates `./output/` at the start of every run, so it's always safe to
re-run — it's simply overwriting its own scratch output, not touching your
source data.

## Concepts, mapped to the code

### 1. Reading/writing CSV, JSON and Parquet — `readWriteFormatsDemo`
The same slice of data is written out in all three formats and read back, so
you can compare them directly:
- **CSV**: plain text, human-readable, but carries no schema — a reader must
  supply one (`inferSchema`/`header` options), and type information (is this
  column an Int or a String?) has to be *guessed* by scanning the file.
- **JSON**: self-describing (every line repeats its field names), so no schema
  options are needed to read it back, but that repetition makes it the
  largest of the three on disk for the same data.
- **Parquet**: binary and **columnar**, with the schema stored inside the
  file itself, compressed by default, and able to skip columns/row-groups it
  doesn't need (column and predicate pushdown). This is why Parquet is
  generally the default choice for data Spark itself will read back later.

The code prints the actual on-disk byte sizes for all three so the difference
isn't just theoretical.

### 2. Writing partitioned output — `partitionedOutputDemo`
`df.write.partitionBy("year").parquet(...)` does **not** change how many
in-memory partitions the DataFrame has — it changes the **file layout** on
disk, writing one subdirectory per distinct value of the partitioning
column(s), Hive-style:

```
output/partitioned_by_year/
  year=2024/
  year=2025/
  year=2026/
```

The `year` column disappears from inside the Parquet files themselves — its
value lives entirely in the folder name, and Spark reconstructs it on read.
Filtering on `year` after reading it back lets Spark skip whole folders
without opening them at all ("partition pruning"), which `.explain()` shows
as a `PartitionFilters` entry.

### 3. File layout and number of output files — `explainFileLayout`
The core rule: **Spark writes one output file per in-memory partition**, one
per task. An 8-partition DataFrame writing without `partitionBy` produces (up
to) 8 `part-*` files plus a `_SUCCESS` marker. With `partitionBy(...)`, that
same rule applies *inside every partition subfolder* — so a write with many
distinct partition-column values and many input partitions can explode into
thousands of tiny files (the classic "small files problem"), which is
expensive for any downstream reader. The fix is controlling the partition
count *before* writing — which is exactly section 4.

### 4. `repartition()` before writing — `repartitionBeforeWriteDemo`
Three writes of the same DataFrame, side by side, each printing the resulting
`part-*` file count:
- **No repartition**: file count == whatever partitions the DataFrame already
  had (inherited from reading one CSV file).
- **`.repartition(4)`**: always exactly 4 files — a full shuffle redistributes
  every row evenly, more expensive but guarantees even file sizes.
- **`.coalesce(2)`**: cheaper (no full shuffle), but can only *reduce* the
  partition count by merging existing partitions, which can leave files
  uneven if the input was already skewed.

Rule of thumb used in the comments: `repartition(n)` when you need an exact,
even file count (especially after a wide op like `groupBy`/`join`);
`coalesce(n)` when you just need fewer files cheaply from already-balanced
partitions.

### 5. Scenario: daily sales partitioned by year/month/day — `scenarioDailySalesPartitioned`
Puts all four previous sections together on the full `daily_sales.csv`
dataset:
1. **Clean first**: `coalesce(col, default)` / `na.fill(...)` replace the
   missing `amount`/`region` values generated into the source data, so sums
   and group-bys downstream aren't silently corrupted by nulls.
2. **Repartition by the partition columns themselves**:
   `cleaned.repartition(col("year"), col("month"), col("day"))` before the
   write. This groups every row for a given `(year, month, day)` onto a
   single in-memory partition *before* `partitionBy` splits it into folders,
   so (almost) every leaf folder ends up with exactly one `part-*` file
   instead of being fragmented — directly applying the lesson from
   sections 3 and 4 to a realistic multi-column partition key.
3. **Write partitioned**: `.partitionBy("year", "month", "day").parquet(...)`,
   producing a layout like:
   ```
   output/daily_sales_partitioned/
     year=2024/month=1/day=1/part-....parquet
     year=2024/month=1/day=3/part-....parquet
     ...
   ```
4. **Read back and query**: a single-day lookup (partition pruning), a
   month-by-month 2026 revenue trend, and a by-region revenue ranking — all
   computed from a DataFrame whose `year`/`month`/`day` columns are
   reconstructed purely from folder names.

## Notes / things to try

- Delete `./output/` and re-run with `local[2]` vs `local[4]` — the *content*
  of the output is identical, but the "no repartition" file count will differ
  because it tracks `defaultParallelism`.
- Try commenting out the `.repartition(col("year"), col("month"), col("day"))`
  call in the scenario and compare `countLeafPartitionFolders` /
  `countPartFiles` behavior — you should see more fragmented, uneven files
  per leaf folder without it.
- Open any `part-*.parquet` file's folder name directly (e.g.
  `output/daily_sales_partitioned/year=2025/month=6/day=10/`) to see the
  Hive-style partition encoding firsthand.
