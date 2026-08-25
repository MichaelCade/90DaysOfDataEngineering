# Day 38 — DuckDB with Iceberg: Local Development Workflow

> Module 3: Data Lakehouse

Trino is the cluster's heavyweight engine. But a lakehouse's promise is **open tables many engines
can read** — so you shouldn't need a cluster to poke at the data on your laptop. **DuckDB** is the
counterpart: a single-file, in-process SQL engine (think "SQLite for analytics") with an Iceberg
extension. Same open format, zero infrastructure.

## Why DuckDB for local dev

- **No server** — `pip install duckdb` and you have a columnar MPP-ish engine in your Python/CLI.
- **Reads the open formats directly** — Parquet, and Iceberg via its `iceberg` extension.
- **Talks to your warehouse too** — a `postgres` extension reads Postgres directly.
- **Fast iteration** — prototype a transformation locally, then run the identical SQL on Trino at
  scale. (This is also why dbt-with-DuckDB is a great local target — a Module 4 thread.)

## Live: DuckDB reads the cluster's Postgres warehouse

The Postgres LoadBalancer is reachable from the laptop, so DuckDB queries the real warehouse with no
export step:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("ATTACH 'host=192.168.169.191 port=5432 dbname=appdb user=app password=…' "
            "AS pg (TYPE postgres, READ_ONLY);")
con.execute("SELECT player,total_runs FROM pg.cricket.batting ORDER BY total_runs DESC LIMIT 3").fetchall()
# [('Jonathan Dalley', 754), ('Nathan Botes', 275), ('Harry Carter', 269)]   ✅
```

## Live: DuckDB reads an Iceberg table

DuckDB's `iceberg_scan` reads the **same Iceberg format** the cluster uses. Verified on a local
Iceberg table (created with pyiceberg, populated from the cricket data):

```python
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute(f"SELECT player,total_runs FROM iceberg_scan('{metadata_json}') "
            "ORDER BY total_runs DESC LIMIT 3").fetchall()
# [('Jonathan Dalley', 754), ('Nathan Botes', 275), ('Harry Carter', 269)]   ✅  (20 rows)
```

Point `iceberg_scan` at a table's `metadata.json` and DuckDB walks the exact manifest→Parquet tree
from Day 29 — no catalog server required to read a table you can reach.

## The honest caveat: reaching *this* cluster's lakehouse

Reading the cluster's `lakehouse.cricket.*` from the laptop needs **two** endpoints reachable:

1. the **Lakekeeper REST catalog** (to resolve the table → current metadata), and
2. the **MinIO S3 endpoint** the catalog vends in file locations.

On this cluster both are **in-cluster only**. Verified from the Mac: the Trino and Postgres
LoadBalancers answer, but `minio.minio.svc:9000` doesn't resolve and there's no external MinIO LB —
so DuckDB here can't fetch the Parquet even if it reached the catalog. Two clean fixes:

- **Expose MinIO + Lakekeeper externally** (nip.io + TLS like the other UIs) and use Lakekeeper
  **credential vending** (Day 32) so DuckDB gets scoped keys and an endpoint it can reach; or
- **Run DuckDB inside the cluster** (a pod / notebook), where the in-cluster DNS just works.

This isn't a DuckDB limitation — it's the lakehouse networking boundary, and naming it is the point:
"open format, any engine" still requires the engine to have a **network path** to catalog + storage.

```mermaid
flowchart LR
    DD["DuckDB (laptop)"] -->|"✅ reachable"| PG[("Postgres LB")]
    DD -->|"✅ local files"| LOC[("local Iceberg / Parquet")]
    DD -.->|"❌ in-cluster only"| CAT["Lakekeeper"] & MIN[("MinIO"]
    DDC["DuckDB (in-cluster pod)"] -->|"✅ in-cluster DNS"| CAT
    DDC --> MIN
```

## Applied example (🏏)

The local-dev loop for the cricket analytics: pull `cricket.batting` from Postgres into DuckDB (or a
local Iceberg/Parquet copy), prototype a metric — say a new "boundary %" — interactively on the
laptop in milliseconds, get the SQL right, then lift it into a dbt-trino model (Module 4) that runs
against the full lakehouse. Same open data, two engines, sized to the task: DuckDB for the tight
feedback loop, Trino for the shared, scaled result.

## Summary

DuckDB is the zero-infra local counterpart to Trino: one file, reads Parquet and **Iceberg**
directly (verified `iceberg_scan` on a local Iceberg table), and reads the cluster's **Postgres**
live over its LB. Reaching the cluster's *Iceberg* tables needs Lakekeeper **and** MinIO reachable —
in-cluster-only here (verified), fixable by exposing them with credential vending or running DuckDB
in-cluster. Local prototype → Trino at scale, on the same open format. Next: **Day 39 — Hands-On:
build the whole lakehouse on Kubernetes** (the module, assembled end to end).
