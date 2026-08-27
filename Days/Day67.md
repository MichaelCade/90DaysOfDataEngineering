# Day 67 — Spark + Iceberg: Compaction & Table Maintenance Jobs

> Module 6: Batch Processing

Day 33 covered *why* Iceberg tables need housekeeping — small files pile up, snapshots grow forever —
and ran the fixes as Trino `ALTER TABLE ... EXECUTE`. But maintenance is **heavy, scheduled batch
work**, which is exactly Spark's job. Today we run the same Iceberg procedures from Spark, verified on
the cluster: compaction and snapshot expiry as a SparkApplication.

## Iceberg procedures, called from Spark

Iceberg ships maintenance as **stored procedures** the Spark SQL extension exposes via `CALL`:

| Procedure | Does | Trino equivalent (Day 33) |
|---|---|---|
| `rewrite_data_files` | bin-pack small files into fewer big ones (compaction) | `EXECUTE optimize` |
| `expire_snapshots` | drop old snapshots + delete unreferenced files | `EXECUTE expire_snapshots` |
| `remove_orphan_files` | delete files no metadata references | `EXECUTE remove_orphan_files` |
| `rewrite_manifests` | compact the manifest layer itself | — |

## Verified: compaction + expiry as a Spark job

The job ([`jobs/cricket_maintenance.py`](../examples/module6-spark/jobs/cricket_maintenance.py))
made a table with five small files (five appends), compacted it, then expired snapshots. Real output
from the SparkApplication:

```
BEFORE compaction: files= 5 snapshots= 5
rewrite_data_files: {'rewritten_data_files_count': 5, 'added_data_files_count': 1,
                     'rewritten_bytes_count': 3154, 'failed_data_files_count': 0}
AFTER compaction:  files= 1 snapshots= 6      <- 5 files -> 1; +1 snapshot (the rewrite commit)
AFTER expire:      files= 1 snapshots= 1 rows= 5   <- old snapshots + their files reclaimed, data intact
```

Note the `CALL` returns a **result row** you can log/act on — `rewritten_data_files_count: 5,
added_data_files_count: 1` is the compaction receipt. And, as on Day 33, compaction *adds* a snapshot
(it's a normal atomic commit); the old small files only disappear once you **expire**. Expiry bounds
time travel (snapshots → 1) — set retention to your real recovery need.

```python
spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'cricket_spark.maint_demo')")
spark.sql("""CALL lakehouse.system.expire_snapshots(
             table => 'cricket_spark.maint_demo', retain_last => 1,
             older_than => TIMESTAMP '2999-01-01 00:00:00')""")
```

## Why Spark for maintenance (vs Trino)

Both engines can run these — but at scale, compacting a multi-terabyte table is a big distributed
write, and that's Spark's wheelhouse: many executors rewriting file groups in parallel. Trino's
`EXECUTE optimize` is perfect for interactive/smaller jobs; a nightly Spark maintenance
SparkApplication (or **ScheduledSparkApplication**, Day 66) is the pattern for large tables. Same
Iceberg tables, same procedures, the engine chosen to fit the size.

`rewrite_data_files` also takes options — `where` to compact only some partitions, and sort/z-order
strategies to co-locate related rows for better pruning:

```sql
CALL lakehouse.system.rewrite_data_files(
  table => 'big.events', where => 'day = DATE ''2026-08-26''',
  strategy => 'sort', sort_order => 'user_id')
```

```mermaid
flowchart LR
    W["frequent writes<br/>(dlt/Spark/Trino)"] --> SF["small files + snapshot sprawl"]
    SF --> RW["Spark: rewrite_data_files<br/>(compact, parallel)"]
    RW --> EX["Spark: expire_snapshots<br/>(reclaim)"]
    EX --> H["healthy table ✅"]
```

## Applied example (🏏)

The cricket tables get merged/re-promoted weekly and now also written by Spark — the exact churn that
breeds small files. A scheduled Spark maintenance job would `rewrite_data_files` on
`lakehouse.cricket_spark.*` (and the promoted `cricket.*`) then `expire_snapshots(retain_last => …)`
to keep a sensible window of time travel. Verified here in miniature (5→1 files, snapshots→1, rows
intact); the identical job scales to huge tables by adding executors.

## Summary

Iceberg maintenance runs from Spark as `CALL lakehouse.system.<procedure>`: **rewrite_data_files**
(compaction — verified 5 files → 1), **expire_snapshots** (reclaim + bound time travel — verified
snapshots → 1, data intact), plus remove_orphan_files/rewrite_manifests. Same procedures as Trino
(Day 33), but Spark is the engine for **large, scheduled** maintenance. Next: **Day 68 — Pandera
validating data inside the Spark job itself.**
