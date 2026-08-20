# Day 13 — Deploying Airflow on K8s with Helm

> Module 2: Workflow Orchestration

## The concept

On Day 11–12 we covered *what* orchestration is and Airflow's core pieces. Today we run it
— properly, on Kubernetes. Airflow has four moving parts:

- **Scheduler** — decides what runs when, and hands tasks to the executor.
- **Executor** — actually runs the tasks. This is the big choice on Kubernetes.
- **Webserver** — the UI and API.
- **Metadata database** — the source of truth for DAG runs, task state, connections.

### Why KubernetesExecutor

Airflow's executor is where "on Kubernetes" really pays off:

- **LocalExecutor** runs tasks as subprocesses of the scheduler — one node, no isolation.
- **CeleryExecutor** runs a pool of persistent worker pods fed by a Redis broker — more
  throughput, but you're paying for idle workers and running Redis.
- **KubernetesExecutor** launches **one pod per task**, on demand. No Redis, no idle
  workers, natural isolation and resource limits per task, and it scales to zero between
  runs. On a cluster you already have, it's the obvious fit — and it's what we use.

### Don't run Postgres twice

The chart will happily deploy its own bundled Postgres. Don't let it — we already stood up
a highly-available, backed-up PostgreSQL in Module 1. We point Airflow at that instead
(`postgresql.enabled: false` + a metadata connection secret). One database to operate and
protect, not two.

### DAGs as GitOps

Rather than baking DAGs into an image, a **git-sync** sidecar continuously pulls them from a
Git repo. Push a DAG, it appears — no rebuilds. That's the workflow this course uses:
DAGs live in the repo under `examples/module2-airflow/dags`.

## Hands-on

Full commands and manifests: [`/examples/module2-airflow`](../examples/module2-airflow).

The shape of it:

1. **A dedicated database + role** for Airflow on the CNPG cluster (not the app database),
   and a secret with the connection string the chart reads:
   ```
   postgresql://airflow:***@pg-rw.postgres.svc.cluster.local:5432/airflow
   ```
2. **Install the chart**, pinned to Airflow 2.x, with KubernetesExecutor, external Postgres,
   RWX logs, and git-sync:
   ```bash
   helm upgrade --install airflow apache-airflow/airflow \
     --version 1.16.0 --namespace airflow -f values.yaml
   ```
3. **Expose the webserver** through Traefik over HTTPS (same nip.io pattern as Module 1).
4. **Verify:** the schema appears in the external DB (~48 tables), and the UI redirects to
   `/login` over HTTPS.

## Gotchas

- **The chart now defaults to Airflow 3.** If you want Airflow 2.x (latest stable, best
  compatibility with the dbt/dlt/Spark/Soda providers coming later), **pin chart `1.16.0`**
  (→ Airflow 2.10.5). Newer charts install Airflow 3, which has a different architecture.
- **HTTPS + secure cookies.** Airflow behind a TLS-terminating proxy needs
  `config.webserver.base_url` set to its external `https://` URL and
  `enable_proxy_fix: 'True'` — otherwise logins and redirects break (the same trap as the
  K10 dashboard in Module 1).
- **KubernetesExecutor logs are ephemeral.** Task pods disappear when done, taking their
  logs with them — so put logs on shared **RWX** storage (`ceph-filesystem`) or configure
  remote logging to S3/MinIO, or the UI can't show completed-task logs.
- **git-sync reads the *pushed* repo.** A DAG in your local working copy won't appear until
  you `git push` it.

## Verify + first DAG

Push [`hello_data_engineering.py`](../examples/module2-airflow/dags/hello_data_engineering.py),
wait ~30s for git-sync, trigger it, and watch the KubernetesExecutor work:

```bash
kubectl -n airflow get pods -w    # a pod appears per task, runs, and completes
```

That per-task pod is the whole point — orchestration that's genuinely native to the cluster.

## Further reading

- Airflow Helm chart: <https://airflow.apache.org/docs/helm-chart/stable/index.html>
- KubernetesExecutor: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html>

## Summary

We deployed Airflow on Kubernetes with the **KubernetesExecutor** (a pod per task), backed
by the **CloudNativePG** database from Module 1, with **git-sync** delivering DAGs from the
repo and the UI served over HTTPS through Traefik. The orchestration spine is in place —
next we write DAGs against it, and later wire in ingestion, transformation, and quality.
