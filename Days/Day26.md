# Day 26 — Loading to Iceberg on MinIO

> Workshop: Data Ingestion

Days 22–25 landed data into Postgres with dlt. Today we get it into the **Iceberg lakehouse** —
open table format (Parquet + metadata) on MinIO, catalogued by Lakekeeper, queryable by Trino
(and later DuckDB and Spark). **This day depends on the lakehouse from Module 3** — how Iceberg,
Lakekeeper, and Trino were stood up is documented in
[examples/module3-lakehouse/README.md](../examples/module3-lakehouse/README.md) (the "Iceberg
stand-up"). Read that first if you skipped ahead.

## An honest detour: can dlt write straight to the catalog?

The tidy story would be "point dlt's destination at the Iceberg REST catalog and go." We tried
it, and it's worth showing what we found, because **catalog integration maturity is a real
selection criterion** when you build a platform:

- **Core dlt has no `iceberg` destination** (as of 1.28) — only Postgres, filesystem, DuckDB, etc.
- **The community `dlt-iceberg` package** targets REST catalogs, but currently errors against
  current dlt with an API-drift bug (`update_stored_schema() got an unexpected keyword 'force'`).
- **dlt's `filesystem` + `table_format="iceberg"`** (dlt 1.30) *does* write a valid Iceberg table
  to MinIO — but in dlt's own **serverless** layout. Even with REST-catalog config it created the
  namespace in Lakekeeper yet **did not register the table**, so Trino couldn't see it.

Conclusion: dlt is excellent at the **extract-load into a warehouse** (Postgres — Days 22–25); its
**direct-to-Iceberg-catalog** path isn't production-ready on our stack yet. So we split the job
the way real platforms often do: **dlt lands raw/curated data; the query engine materialises it
into the lakehouse.**

## The pattern: land with dlt, promote with Trino

Trino already talks to both worlds — a `postgres` catalog (the CNPG `appdb` where dlt landed
`cricket.*`) and the `lakehouse` catalog (Iceberg via Lakekeeper). So promotion is a single SQL
idiom, `CREATE TABLE ... AS SELECT` (CTAS):

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.cricket;
CREATE TABLE lakehouse.cricket.batting AS SELECT * FROM postgres.cricket.batting;
```

Trino reads the Postgres rows and **writes a real Iceberg table** — Parquet data + Iceberg
metadata in MinIO, registered in Lakekeeper — in one statement. This is *SQL federation*: one
engine reading one source and writing another, no bespoke code.

## Generic example

Any Trino catalog can be a source. Materialise a TPC-H table into Iceberg to see the mechanism
with zero setup:

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;
CREATE TABLE lakehouse.demo.nation AS SELECT * FROM tpch.tiny.nation;   -- now an Iceberg table
SELECT count(*) FROM lakehouse.demo.nation;                            -- 25
```

## Applied example (🏏)

Promote all three cricket sources — the full script is
[`examples/module3-lakehouse/promote-cricket-to-iceberg.sql`](../examples/module3-lakehouse/promote-cricket-to-iceberg.sql):

```
lakehouse.cricket.batting  : 20 rows (Iceberg)
lakehouse.cricket.bowling  : 19 rows (Iceberg)
lakehouse.cricket.fielding : 23 rows (Iceberg)

SELECT player, total_runs FROM lakehouse.cricket.batting ORDER BY total_runs DESC LIMIT 3;
 Jonathan Dalley | 754
 Nathan Botes    | 275
 Harry Carter    | 269
```

Your batting/bowling/fielding now live as **open Iceberg tables** on MinIO — the same bytes
Trino, DuckDB (Day 38), and Spark (Module 6) can all read, and the raw material for the dbt
models in Module 4. The `cricket` schema in Postgres remains the landing zone (dlt's domain);
the lakehouse is the analytics store.

## Why this split is good, not a compromise

Keeping **extract-load (dlt → Postgres)** separate from **materialise (Trino → Iceberg)** is the
modern ELT shape: raw data is always landed and replayable before anyone reshapes it, and each
tool does what it's best at. When dlt's native Iceberg catalog support matures, it slots straight
into the "land" step — the lakehouse and everything downstream is unchanged.

## Summary

We loaded real data into the Iceberg lakehouse. The direct dlt→catalog path isn't ready yet
(a genuine, useful finding), so we used the robust stack-native pattern: **dlt lands into
Postgres, Trino promotes into Iceberg with CTAS** — SQL federation, no glue code. Cricket
batting/bowling/fielding are now open Iceberg tables on MinIO, catalogued by Lakekeeper. Next:
**Day 27 — running dlt inside Airflow** (the ingestion on a schedule), then **Module 4 (dbt)**
turns these lakehouse tables into tested models.
