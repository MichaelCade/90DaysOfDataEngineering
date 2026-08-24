# Day 22 — dlt: Introduction & Core Concepts

> Workshop: Data Ingestion

## What dlt is (and isn't)

[dlt](https://dlthub.com) is a **Python library** — `pip install dlt` — that does the tedious
parts of loading data: it infers a schema from your source, normalizes nested JSON, coerces
types, creates and evolves the destination tables, loads incrementally, and tracks every
load. You write the *what* (where the data comes from); dlt handles the *how* (getting it
into the destination cleanly).

Crucially, **dlt is not a server or a platform**. It runs wherever your Python runs:

- **Locally** while you develop (what we do today) — pulling the API on your laptop and
  writing out to the cluster's Postgres.
- **In the cluster** for production (Day 27) — the exact same code, run as an Airflow task
  or a container, close to the data.

That portability is the point: no infrastructure to stand up, and dev/prod run identical code.

## Core concepts

- **Resource** — a function that yields rows, decorated with `@dlt.resource`. One resource =
  one destination table. It's the unit of extraction.
- **Source** — a collection of related resources (e.g. an API with several endpoints).
- **Pipeline** — ties a source/resource to a **destination** and a **dataset**, and runs the
  load: `dlt.pipeline(pipeline_name=..., destination="postgres", dataset_name="dlt_workshop")`.
- **Destination** — where data lands: `postgres`, `duckdb`, `filesystem`/S3, `bigquery`,
  and (Day 26) Iceberg. Swapping destinations is a one-line change.
- **Dataset** — the logical namespace in the destination (a **schema** in Postgres).
- **Write disposition** — how each run writes: `replace` (full refresh), `append`
  (add rows), or `merge` (upsert on a key — the basis of incremental, Day 24).
- **Schema inference & normalization** — dlt reads the data and builds the schema: nested
  objects are flattened into columns (`address__city`), nested *lists* become child tables,
  and types are inferred.
- **Lineage & state** — dlt adds `_dlt_id` / `_dlt_load_id` columns to every row, and keeps
  `_dlt_loads`, `_dlt_version`, and `_dlt_pipeline_state` tables so loads are traceable,
  resumable, and incremental — none of which you have to build.

## Hands-on

The whole pipeline: [`examples/workshop-dlt/jsonplaceholder_pipeline.py`](../examples/workshop-dlt/jsonplaceholder_pipeline.py).

```python
@dlt.resource(name="users", write_disposition="replace")
def users():
    resp = requests.get("https://jsonplaceholder.typicode.com/users")
    resp.raise_for_status()
    yield resp.json()

pipeline = dlt.pipeline(pipeline_name="jsonplaceholder",
                        destination="postgres", dataset_name="dlt_workshop")
pipeline.run(users())
```

Run it against the cluster's Postgres:

```bash
pip install "dlt[postgres]"
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
python jsonplaceholder_pipeline.py
```

**What you get, with no schema written by hand** — in the `dlt_workshop` schema of `appdb`:

```
users                 nested JSON flattened -> company__name, address__city,
                      address__geo__lat, ...  + inferred types (id : bigint)
                      + lineage columns _dlt_id, _dlt_load_id
_dlt_loads            one row per load (id, status)
_dlt_pipeline_state   state for incremental loads
_dlt_version          schema version history
```

Compare to the **Day 14 DAG**, where we hand-wrote the `CREATE TABLE`, the column list, and
the idempotent insert. dlt inferred all of it and gave us load tracking for free.

## Why this matters

The Day 14 approach doesn't scale: every new source means more hand-written schema and
insert code, and you own all the edge cases (new fields, type changes, incremental state).
dlt turns "ingest a source" into "write a resource function" and handles the rest — which is
exactly what you want when a real platform has *dozens* of sources.

## Summary

dlt is a Python library that makes ingestion declarative: define a **resource**, point a
**pipeline** at a **destination**, and dlt infers the schema, normalizes nested data, adds
lineage/state, and tracks the load. We loaded a REST API into Postgres with a handful of
lines. Next: **Day 23** goes deeper on REST APIs (pagination, the `rest_api` source) and
**Day 24** on incremental loading — the state-tracking that makes re-runs cheap and correct.
