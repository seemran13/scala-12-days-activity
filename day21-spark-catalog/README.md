# Day 21 — Spark Catalog

Covers:
1. Listing databases and tables
2. Creating temporary views
3. Registering/querying tables
4. Inspecting table/schema metadata
5. Scenario: a small analytics database for hotel bookings

## Project layout

```
day21-spark-catalog/
  build.sbt
  project/build.properties
  src/main/resources/
    log4j2.properties
    hotels.csv      <- 8 hotels
    rooms.csv       <- 43 real rooms + 1 orphan room pointing at a fake hotel (H99)
    guests.csv      <- 60 guests
    bookings.csv    <- 505 bookings (incl. ~4% null amount, ~2% null guestId,
                        and 5 bookings against the orphan room)
  src/main/scala/day21/
    SparkCatalogApp.scala
```

## How to run

```bash
sbt "run local[2]"
sbt "run local[4]"
```

This app creates real on-disk artifacts the first time it runs: `metastore_db/`
(Spark's embedded Derby metastore, where table *metadata* lives) and
`spark-warehouse/` (where managed tables' actual Parquet *data* lives). Both
are already in `.gitignore`. Because `saveAsTable(...)` is used with
`.mode("overwrite")`, it's safe to run this repeatedly — each run replaces the
previous tables rather than erroring or duplicating data.

## Concepts, mapped to the code

### 0. Catalog state before anything exists — `showCatalogState`
`spark.catalog` is the API for asking Spark what it already knows about —
`currentDatabase`, `listDatabases()`, `listTables()`. Run first, before
creating anything, so you can see the baseline: just the built-in `default`
database and zero tables.

### 1. Listing databases and tables — `listDatabasesAndTables`
`CREATE DATABASE hotel_analytics` behaves exactly like `CREATE DATABASE` in
MySQL/Postgres — it's a namespace to group related tables, visible afterward
via `spark.catalog.listDatabases()`. `USE hotel_analytics` changes which
database *unqualified* table names resolve against; `listTables()` always
means "tables in the current database" unless you pass a database name
explicitly (`listTables("default")`).

### 2. Creating temporary views — `temporaryViewsDemo`
- **`createOrReplaceTempView`**: registers a DataFrame as a queryable name
  for the lifetime of the current `SparkSession` only. It shows up in
  `listTables()` with `isTemporary = true` and `database = null` — it
  doesn't belong to any database, which is exactly why it disappears when
  the session ends.
- **`createOrReplaceGlobalTempView`**: like a temp view, but visible to
  *every* `SparkSession` sharing the same Spark application. It lives in a
  special reserved database called `global_temp`, and must be queried with
  that prefix (`SELECT * FROM global_temp.hotels_global_view`). Still gone
  when the application itself stops — neither kind of temp view is a
  persisted table.

### 3. Registering/querying tables — `registerAndQueryTables`
`df.write.saveAsTable("hotels")` is the real, persistent counterpart to a
temp view: Spark writes the **data** to `spark-warehouse/` (as Parquet by
default) and records the **metadata** (schema, storage location, format) in
its catalog. The result is a **managed table** — after this call, `hotels`
is queryable with plain SQL from any session that opens this same warehouse,
not just the one that created it, and it survives past this run of the app
(next run's `.mode("overwrite")` replaces it cleanly). `spark.table("rooms")`
reads a registered table straight back as a DataFrame, and
`spark.catalog.tableExists(...)` checks the catalog safely without throwing
if the name isn't there.

### 4. Inspecting table/schema metadata — `inspectMetadata`
Several ways to ask the catalog about one table, from least to most detail:
- `DESCRIBE TABLE bookings` — just column names and types.
- `DESCRIBE EXTENDED bookings` — adds table-level metadata: provider
  (parquet), location on disk, owner, creation time.
- `spark.catalog.listColumns("bookings")` — the same column info
  programmatically, including nullability.
- `spark.catalog.getTable("hotel_analytics", "bookings")` — one `Table`
  object (name/database/tableType/isTemporary) for quick programmatic
  checks instead of parsing `DESCRIBE` output.
- `SHOW DATABASES` / `SHOW TABLES IN hotel_analytics` — the plain-SQL
  equivalents of the `spark.catalog` calls, if you prefer writing SQL.
- `spark.catalog.refreshTable(...)` — tells Spark to re-read a table's
  files/metadata from disk, needed if something *outside* this Spark session
  changed the underlying data and cached metadata would otherwise go stale.

### 5. Scenario: a small analytics database for hotel bookings — `hotelAnalyticsScenario`
Puts it all together: with `hotels`, `rooms`, `guests` and `bookings` all
registered as managed tables under `hotel_analytics`, the "analytics
database" is browsable purely through the catalog — a BI tool (or a new
teammate) doesn't need to be told table names up front, just the database.
From there it runs:
1. **A referential-integrity check straight from the catalog**: a
   `LEFT ANTI JOIN` finds bookings whose `hotelId` doesn't exist in `hotels`
   at all — exactly the 5 bookings deliberately seeded against the orphan
   `H99`/extra-room row in the sample data.
2. **Revenue by hotel** (COMPLETED bookings only, nulled amounts coalesced
   to 0 so they don't silently vanish from the sum).
3. **Booking status breakdown** system-wide.
4. **Top 5 guests by spend** (rows with a null `guestId` are naturally
   excluded by the inner join — there's no guest to credit that spend to).
5. **Room-type popularity** across the whole hotel chain.
6. **Teardown contrast**: `spark.catalog.dropTempView(...)` removes a session
   view's registration only; it's shown here purely to contrast with
   `DROP TABLE`, which would remove both a managed table's catalog entry
   *and* delete its underlying Parquet files — the managed tables themselves
   are deliberately left in place so you can poke at them afterward.

## Things to try

- After running once, start a plain `spark-shell` (or a second small app) in
  this same project directory and run
  `spark.sql("SHOW TABLES IN hotel_analytics").show()` — the managed tables
  are still there, because they're real files plus real metastore entries,
  not something tied to the process that created them.
- Try querying `hotels_view` (a plain temp view) from a *second* `SparkSession`
  created via `spark.newSession()` — it won't be found, proving temp views
  really are session-scoped, unlike the global temp view or the managed
  tables.
- Delete `metastore_db/` and `spark-warehouse/` and re-run — everything
  rebuilds from scratch, since the CSVs in `src/main/resources/` are the
  only real source of truth here.
