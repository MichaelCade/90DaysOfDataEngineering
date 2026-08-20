# Apache Airflow on Kubernetes

Apache Airflow deployed with the official Helm chart, using **KubernetesExecutor** (each
task runs as its own pod), the **CloudNativePG Postgres from Module 1** as its metadata
database, and **git-sync** to deliver DAGs from this repo.

## Prerequisites

- The Module 1 **CloudNativePG** cluster (`pg` in namespace `postgres`).
- An **RWX** StorageClass for shared task logs — here `ceph-filesystem` (Rook-Ceph).
- **Traefik + MetalLB** and the nip.io DNS pattern (see Module 1) for the web UI ingress.
- `helm` and `kubectl`.

## 1. Create a dedicated database + role in Postgres

Don't reuse the app database — give Airflow its own. On the CNPG primary:

```bash
PRIMARY=$(kubectl -n postgres get clusters.postgresql.cnpg.io pg -o jsonpath='{.status.currentPrimary}')
PW=$(openssl rand -hex 16)
kubectl -n postgres exec "$PRIMARY" -- psql \
  -c "CREATE ROLE airflow WITH LOGIN PASSWORD '$PW';" \
  -c "CREATE DATABASE airflow OWNER airflow;"
```

(The `airflow` role is *not* a CNPG-managed role, so it persists and survives failover.)

## 2. Create the metadata connection secret

The chart reads the SQLAlchemy connection string from a secret's `connection` key:

```bash
kubectl create namespace airflow
kubectl -n airflow create secret generic airflow-metadata-db \
  --from-literal=connection="postgresql://airflow:$PW@pg-rw.postgres.svc.cluster.local:5432/airflow"
```

No `sslmode` needed — libpq defaults to `prefer`, so it uses TLS to CNPG automatically.

## 3. Install Airflow

> **Pin the chart version.** The latest chart defaults to **Airflow 3.x**. For Airflow 2.x
> (latest stable, widest tooling compatibility) use chart **1.16.0** → Airflow **2.10.5**.

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.16.0 --namespace airflow -f values.yaml
```

Key [`values.yaml`](values.yaml) choices: `executor: KubernetesExecutor`,
`postgresql.enabled: false` + `data.metadataSecretName`, `logs.persistence` on
`ceph-filesystem`, `dags.gitSync` from this repo, and `config.webserver.base_url` /
`enable_proxy_fix` so it works behind Traefik's TLS.

## 4. Expose the web UI

```bash
kubectl apply -f ingress.yaml     # airflow.192.168.169.190.nip.io, Traefik websecure + TLS
```

## 5. Verify

```bash
kubectl -n airflow get pods        # scheduler / webserver / triggerer / statsd Running
# schema created in the external DB:
kubectl -n postgres exec "$PRIMARY" -- psql -d airflow -tAc \
  "select count(*) from information_schema.tables where table_schema='public';"   # ~48
# UI (redirects to /login over HTTPS):
curl -skI -L https://airflow.192.168.169.190.nip.io/ | head -1                     # HTTP 200
```

Web UI: **https://airflow.192.168.169.190.nip.io/** — default login **admin / admin**
(change it under Security → List Users before anyone untrusted can reach it).

> **If `admin/admin` is rejected**, the chart's create-user job didn't leave a user behind
> (check with `airflow users list` — "No data found"). Create one manually:
> ```bash
> kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
>   airflow users create --username admin --password admin \
>   --firstname Admin --lastname User --role Admin --email admin@example.com
> ```

## DAGs (git-sync)

DAGs live in [`dags/`](dags/) and are pulled by a git-sync sidecar from the **pushed**
repo — so:

```bash
git add examples/module2-airflow/dags/ && git commit -m "add first DAG" && git push
```

Within ~30s the DAG appears in the UI. Trigger `hello_data_engineering` and watch each task
spin up as its own pod:

```bash
kubectl -n airflow get pods -w     # <dag>-<task>-<...> pods appear and complete
```

## Cleanup

```bash
helm -n airflow uninstall airflow
kubectl delete ns airflow
# in Postgres: DROP DATABASE airflow; DROP ROLE airflow;
```
