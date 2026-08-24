# Day 7 — PostgreSQL on K8s: Metadata & Operational Store

> Module 1: Infrastructure Foundation

## The concept

Almost every tool in the modern data stack needs a **relational database to store its
own state** — not the analytical data, but the *metadata*: Airflow keeps its DAG runs,
task states, and connections in Postgres; dbt can persist run artifacts; a lakehouse
catalog needs somewhere to record table pointers. PostgreSQL is the default choice
because it's rock-solid, open source, and ubiquitous.

So before we deploy anything that *orchestrates* or *transforms*, we stand up the
database those tools lean on. On Kubernetes there are two ways to do that:

1. **A Helm chart** (e.g. Bitnami) — installs Postgres as a StatefulSet. Simple, but
   *you* own day-2 operations: failover, backups, minor-version upgrades, reconfiguration.
2. **An operator** — a controller that encodes an expert's operational knowledge as
   code. You declare *what* you want (`3 instances, 5Gi, this database`) and the
   operator continuously reconciles reality to match: it bootstraps replicas, promotes
   a new primary on failure, manages backups, and handles rolling upgrades.

For a course about doing data engineering *properly*, the operator pattern is the one
worth learning — it's how production Postgres runs on Kubernetes. We use
**[CloudNativePG](https://cloudnative-pg.io)** (CNPG): a modern, well-maintained,
Kubernetes-native Postgres operator.

### What "highly available" means here

Our `Cluster` asks for **3 instances**. CNPG turns that into:

- **1 primary** — accepts reads and writes.
- **2 replicas** — stream the write-ahead log (WAL) from the primary and stay in sync.

If the primary's node dies, the operator **promotes** a healthy replica to primary
automatically, and reroutes the `pg-rw` service to it — no manual intervention. That's
the payoff of the operator model.

### Storage on Kubernetes

Each instance gets its own `PersistentVolumeClaim`. On this platform that's backed by
**Rook-Ceph** (`ceph-block`, RWO) — so the data itself is *already* replicated 3× across
Ceph OSDs underneath, independent of Postgres's own streaming replication. Two layers of
redundancy: Postgres replication for fast failover, Ceph replication for durability.

## Hands-on

Full manifests and commands: [`/examples/module1-infra/postgres`](../examples/module1-infra/postgres).

**1. Install the CNPG operator** (controller + CRDs only):

```bash
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo update
helm upgrade --install cnpg --namespace cnpg-system --create-namespace cnpg/cloudnative-pg
kubectl -n cnpg-system rollout status deploy/cnpg-cloudnative-pg
```

**2. Declare a cluster** — the whole database is described by one manifest:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: { name: pg, namespace: postgres }
spec:
  instances: 3
  storage: { size: 5Gi, storageClass: ceph-block }
  bootstrap:
    initdb: { database: appdb, owner: app }
```

```bash
kubectl create namespace postgres
kubectl apply -f examples/module1-infra/postgres/cluster.yaml
```

**3. Watch it bootstrap.** The operator runs an `initdb` job for the primary, then a
`join` job for each replica (provisioning a Ceph PVC for each):

```bash
kubectl -n postgres get clusters.postgresql.cnpg.io pg -w
# ... Cluster in healthy state, 3/3 ready, primary pg-1
```

**4. Prove the HA is real** — the primary should report two streaming standbys:

```bash
kubectl -n postgres exec pg-1 -- \
  psql -tAc "select application_name, state, sync_state from pg_stat_replication;"
# pg-2|streaming|async
# pg-3|streaming|async
```

**5. Connect.** The operator generated three Services and a credentials secret:

- `pg-rw` → primary (read/write), `pg-ro` → replicas (read-only), `pg-r` → any.
- Secret `pg-app` holds the `app` user's password and a ready-made `uri`.

Applications inside the cluster use:
`host=pg-rw.postgres.svc.cluster.local port=5432 dbname=appdb user=app`.

## Gotchas

- **`Cluster` name collision.** If you also run Kasten K10 (or anything with a `Cluster`
  CRD), `kubectl get cluster` is ambiguous. Use the fully-qualified
  `clusters.postgresql.cnpg.io` for CNPG.
- **Set `storageClass` explicitly** if your cluster's default isn't the block class you want.
- **Resources:** we set modest CPU/memory requests so the scheduler places pods
  predictably. Bump them up for anything resembling real load.

## Further reading

- CloudNativePG docs: <https://cloudnative-pg.io/docs/>
- CNPG `Cluster` API reference: <https://cloudnative-pg.io/docs/cloudnative-pg.v1/>
- Why an operator? — the [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)

## Summary

We deployed a **highly-available PostgreSQL cluster** with the CloudNativePG operator:
one primary and two streaming replicas, persistent storage on Rook-Ceph, and an
application database — all from a single declarative manifest, with automatic failover
handled for us. This is the metadata backbone the orchestration and transformation
layers will plug into next. **Next up (Day 8):** storage formats — Parquet, Avro, ORC.
