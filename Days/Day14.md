# Day 14 — Writing Your First (Real) DAG

> Module 2: Workflow Orchestration

A "hello world" DAG proves the plumbing. Today we write one that does actual data
engineering — and, importantly, one that drives the infrastructure we've already built.

## The concept: a mini ELT pipeline

The DAG follows the canonical shape you'll repeat for the rest of this series:

```
extract (public API)  →  land raw in MinIO (the "lake")  →  load into Postgres
(the "warehouse")  →  validate (a quality gate)
```

Two design choices worth calling out, because they're *data engineering*, not just Airflow:

- **Land the raw response in object storage first.** Before touching the warehouse, we
  write the untouched API payload to MinIO under `raw/weather/<date>/<city>.json`. That
  raw landing zone means you can **re-process history** without re-hitting the source, and
  it decouples ingestion from loading. This is the "lake" half of the lakehouse pattern
  we build properly in Module 3.
- **Make the load idempotent.** The load task does `DELETE WHERE reading_date = <ds>` and
  then inserts. Re-run the same day's task — a retry, a backfill, a manual trigger — and you
  get the same result, not duplicated rows. Idempotency (Day 12) stops being theory the
  moment you write a load.

It also uses two patterns you'll lean on constantly:

- **Connections + Hooks** — the DAG never hard-codes credentials. It references two Airflow
  *connections* (`minio_s3`, `postgres_appdb`) and uses `S3Hook` / `PostgresHook`. Secrets
  live in Airflow, not in your code or Git.
- **The KubernetesExecutor** — each task (`extract_and_land`, `load_to_postgres`,
  `validate`) runs in its own pod.

The DAG: [`examples/module2-airflow/dags/weather_elt.py`](../examples/module2-airflow/dags/weather_elt.py).

## Hands-on

### 1. One-time setup (bucket + connections)

The lake bucket:

```bash
# via the MinIO console, or mc:
mc mb local/datalake
```

The two Airflow connections (run once — they're stored in Airflow's metadata DB and are then
available to every task pod):

```bash
# MinIO (S3-compatible): note the endpoint_url in --conn-extra
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow connections add minio_s3 --conn-type aws \
  --conn-login minio-admin --conn-password '<minio-password>' \
  --conn-extra '{"endpoint_url":"http://minio.minio.svc.cluster.local:9000","region_name":"us-east-1"}'

# Postgres (the appdb from Module 1)
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow connections add postgres_appdb --conn-type postgres \
  --conn-host pg-rw.postgres.svc.cluster.local --conn-schema appdb \
  --conn-login app --conn-password '<app-password>' --conn-port 5432
```

> In a "real" setup you'd define these as env-var connections from a Kubernetes Secret
> (`AIRFLOW_CONN_MINIO_S3` / `AIRFLOW_CONN_POSTGRES_APPDB`) so they're declarative and never
> typed by hand — a good hardening exercise once the DAG works.

### 2. Ship the DAG

git-sync pulls from the pushed repo, so commit and push:

```bash
git add examples/module2-airflow/dags/weather_elt.py && git commit -m "Day 14: weather ELT DAG" && git push
```

Within ~30s it appears in the UI as `weather_elt`. **Unpause** it (toggle on the left).

### 3. Run and watch

Trigger it (▶) and watch each task run as its own pod:

```bash
kubectl -n airflow get pods -w        # weather-elt-<task>-... pods appear, run, complete
```

### 4. Verify the data landed

```bash
# raw files in the lake:
mc ls -r local/datalake/raw/weather/

# rows in the warehouse:
kubectl -n postgres exec deploy/pg-rw --help >/dev/null 2>&1  # (use the primary pod)
psql "host=192.168.169.191 port=5432 dbname=appdb user=app sslmode=require" \
  -c "SELECT * FROM weather_readings ORDER BY reading_date DESC, city;"
```

## Gotchas

- **Connections must exist before the DAG runs**, or tasks fail with "connection not found".
- **git-sync reads the pushed repo** — a local-only DAG won't appear.
- **Idempotency is on you.** `insert_rows` with no dedup would duplicate on every re-run;
  the `DELETE`-then-insert (or an upsert) is what makes re-runs safe.

## Further reading

- TaskFlow API: <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html>
- Connections & Hooks: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html>

## Summary

We wrote a real ELT DAG: it extracts from a public API, **lands the raw data in MinIO**,
**loads it idempotently into Postgres**, and **validates** the result — using Airflow
connections and hooks so no credentials live in code, with each task running as its own pod
via the KubernetesExecutor. That's the whole Module 1 + 2 stack working together, and the
template every later pipeline in this series follows.
