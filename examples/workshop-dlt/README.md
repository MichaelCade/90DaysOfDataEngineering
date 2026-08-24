# Data Ingestion with dlt

[dlt](https://dlthub.com) (data load tool) is a pip-installable Python library that handles
schema inference, nested-JSON normalization, typing, incremental loading, and load tracking —
so ingestion is a few lines of Python, not a pile of boilerplate.

## Where does dlt run?

**dlt is a library — it runs wherever the Python process that calls it runs. There is no dlt
server or cluster component.**

- **Local dev (this workshop):** on your machine. The pipeline pulls the API over your
  internet connection and writes *out* to the cluster's Postgres via the LoadBalancer
  (`192.168.169.191:5432`). Fastest way to iterate.
- **Production (Day 27):** the *same code* runs **inside the cluster** — as an Airflow task
  (KubernetesExecutor pod), a CronJob, or a container — close to the data, using MinIO's
  internal endpoint (`minio.minio.svc:9000`). Nothing about the pipeline changes; only where
  the Python runs.

## Setup (local)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install "dlt[postgres]"
```

## Run — [`jsonplaceholder_pipeline.py`](jsonplaceholder_pipeline.py)

dlt reads the destination credentials from config/env. Point it at the cluster's Postgres:

```bash
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<app-password>@192.168.169.191:5432/appdb"
python jsonplaceholder_pipeline.py
# -> 1 load package(s) were loaded to destination postgres and into dataset dlt_workshop
```

## What dlt created (no schema written by hand)

In the `dlt_workshop` schema of `appdb`:

```
users                 -- the data, with nested JSON flattened:
                         company__name, address__city, address__geo__lat, ...
                         + inferred types (id : bigint, ...)
                         + lineage columns: _dlt_id, _dlt_load_id
_dlt_loads            -- one row per load (id, status, timestamp)
_dlt_pipeline_state   -- pipeline state (used for incremental loads later)
_dlt_version          -- schema version history
```

Inspect it:

```bash
kubectl -n postgres exec deploy/pg-... -- psql -d appdb -c "\dt dlt_workshop.*"
kubectl -n postgres exec deploy/pg-... -- psql -d appdb -c "SELECT id, name, address__city, company__name FROM dlt_workshop.users;"
```

## Notes

- `@dlt.resource` = one table; `write_disposition="replace"` fully refreshes it each run
  (later we use `append` and `merge` for incremental — Day 24).
- The `_dlt_*` tables are how dlt does idempotent, incremental, resumable loads — we didn't
  have to build any of that (compare the manual DELETE-then-insert in the Day 14 DAG).

## Cleanup

```bash
kubectl -n postgres exec deploy/pg-... -- psql -d appdb -c "DROP SCHEMA dlt_workshop CASCADE;"
```
