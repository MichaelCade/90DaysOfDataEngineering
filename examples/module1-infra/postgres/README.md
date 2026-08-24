# PostgreSQL on Kubernetes — CloudNativePG

A highly-available PostgreSQL cluster for the data platform, deployed with the
[CloudNativePG](https://cloudnative-pg.io) operator. This is the operational /
metadata store that later modules (Airflow, dbt, etc.) depend on.

**What you get:** 1 primary + 2 streaming replicas with automatic failover,
persistent storage on Rook-Ceph (`ceph-block`), and an application database
(`appdb`) with its own role — all declared in [`cluster.yaml`](cluster.yaml).

## Prerequisites

- A working Kubernetes cluster with a default (or named) block `StorageClass`.
  This example uses `ceph-block` (Rook-Ceph). Change `spec.storage.storageClass`
  in `cluster.yaml` if yours differs.
- `helm` and `kubectl`.

## 1. Install the operator

```bash
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo update
helm upgrade --install cnpg \
  --namespace cnpg-system --create-namespace \
  cnpg/cloudnative-pg

kubectl -n cnpg-system rollout status deploy/cnpg-cloudnative-pg
```

The chart installs only the operator (controller, webhooks, RBAC, CRDs). Clusters
are then declared as `Cluster` resources.

## 2. Deploy the PostgreSQL cluster

```bash
kubectl create namespace postgres
kubectl apply -f cluster.yaml
```

## 3. Verify

```bash
# Cluster status (see note below about the resource name):
kubectl -n postgres get clusters.postgresql.cnpg.io pg
# NAME  AGE  INSTANCES  READY  STATUS                    PRIMARY
# pg    ...  3          3      Cluster in healthy state  pg-1

# Pods: one per instance
kubectl -n postgres get pods

# Replication: primary should show two streaming standbys
kubectl -n postgres exec pg-1 -- \
  psql -tAc "select application_name, state, sync_state from pg_stat_replication;"
```

> **Note — `Cluster` name collision.** If another CRD named `Cluster` is installed
> (e.g. Kasten K10's `clusters.dist.kio.kasten.io`), the short name `kubectl get cluster`
> is ambiguous and may resolve to the wrong type. Use the fully-qualified
> `clusters.postgresql.cnpg.io` (or `cnpg` shortname) for CloudNativePG.

## 4. Connect

The operator creates three Services and an application-user secret:

| Service | Use |
|---|---|
| `pg-rw` | read/write — always points at the **primary** |
| `pg-ro` | read-only — load-balances across **replicas** |
| `pg-r`  | any instance |

```bash
# App connection details live in the pg-app secret:
kubectl -n postgres get secret pg-app -o jsonpath='{.data.uri}' | base64 -d; echo

# Apps inside the cluster connect to:
#   host=pg-rw.postgres.svc.cluster.local port=5432 dbname=appdb user=app
```

### From outside the cluster

PostgreSQL is a raw TCP protocol, so it is exposed with a **MetalLB `LoadBalancer`
Service** (declared in `cluster.yaml` under `spec.managed.services`), **not** an HTTP
Ingress — an Ingress can't carry a Postgres connection. The primary is reachable at a
pinned address:

```bash
kubectl -n postgres get svc pg-rw-lb
# NAME       TYPE           EXTERNAL-IP       PORT(S)
# pg-rw-lb   LoadBalancer   192.168.169.191   5432:.../TCP

# From a workstation with psql (macOS: brew install libpq) or a GUI (DBeaver/TablePlus):
PGPASSWORD=$(kubectl -n postgres get secret pg-app -o jsonpath='{.data.password}' | base64 -d) \
  psql "host=192.168.169.191 port=5432 dbname=appdb user=app sslmode=require"
```

The operator sets the service selector to `instanceRole=primary`, so it automatically
follows the primary after a failover.

> **Security:** this is reachable by any host on the LAN (private IP — **not**
> internet-exposed). Keep the `app` password strong and connect with `sslmode=require`
> (CloudNativePG serves TLS by default).

## Backups & disaster recovery

Postgres is protected two complementary ways:

1. **CNPG Barman (physical) → MinIO** — declared in [`cluster.yaml`](cluster.yaml) under
   `spec.backup`. CNPG continuously archives WAL and takes base backups to the `cnpg-barman`
   bucket on MinIO (`s3://cnpg-barman`; creds in the `barman` secret). A **physical** backup
   captures the **entire instance — every database** — and WAL archiving enables
   **point-in-time recovery (PITR)**.
2. **Kasten K10 (orchestration)** — the `cnpg-bp` Blueprint + `cnpg-bp-binding`
   ([`kasten/`](kasten/)) make K10 trigger a CNPG `Backup` on every policy run and delete it
   when the restore point expires. K10 also snapshots the namespace's Kubernetes resources.
   Pattern: <https://github.com/michaelcourcy/kasten-cnpg>.

### On-demand backup

```bash
kubectl apply -f - <<'EOF'
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: { name: backup-initial, namespace: postgres }
spec:
  method: barmanObjectStore
  cluster: { name: pg }
EOF
kubectl -n postgres get backups.postgresql.cnpg.io    # PHASE -> completed
```
Backups land under `s3://cnpg-barman/pg/base/<id>/` (+ WAL under `pg/wals/`).

### Kasten policies (create in the K10 UI)

The BlueprintBinding auto-applies to any CNPG cluster, so you only add policies:
1. **App policy** — back up namespace `postgres`, and **set a location profile**
   (e.g. `kasten-backups`) — required so the blueprint's Kanister action runs. Each run
   triggers a CNPG barman backup automatically.
2. **Backup-object policy** — a second, high-frequency policy capturing only the
   `backups.postgresql.cnpg.io` objects, so restore points contain the Backup specs (with the
   barman `backupId`/paths) needed to recover after a full-namespace loss.

### Restore (manual — recreate the cluster)

The blueprint does backup + expiry only; restore is a deliberate recreate that brings back
**all databases**:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: { name: pg-restored, namespace: postgres }
spec:
  instances: 3
  storage: { size: 5Gi, storageClass: ceph-block }
  bootstrap:
    recovery:
      source: pg
      # recoveryTarget: { backupID: "20260820T142353" }   # or targetTime for PITR
  externalClusters:
    - name: pg
      barmanObjectStore:
        destinationPath: s3://cnpg-barman
        endpointURL: http://minio.minio.svc.cluster.local:9000
        wal: { compression: gzip }
        s3Credentials:
          accessKeyId: { name: barman, key: aws_access_key_id }
          secretAccessKey: { name: barman, key: aws_secret_access_key }
```
For a full-namespace disaster (namespace deleted), first restore the `barman` and `pg-app`
secrets from a K10 restore point, then apply the recovery cluster above.

### Keeping backups restorable (learned the hard way)

A backup is only restorable if its **`begin_wal` segment is in the object store**. On an idle
database the current WAL segment isn't archived until it fills or is switched — so a
just-taken backup can be temporarily **un-restorable** (`WAL ... not found` at recovery time).
This cluster sets `postgresql.parameters.archive_timeout: "5min"` so WAL is archived on a
schedule regardless of write activity. To sanity-check a backup:

```bash
BEGIN=$(kubectl -n postgres get backup <name> -o jsonpath='{.status.beginWal}')
# confirm s3://cnpg-barman/pg/wals/…/$BEGIN.gz exists before relying on that backup
```

**Verified DR drill:** wrote rows → `Backup` (barman) → recovered into a fresh `pg-restored`
cluster (`bootstrap.recovery`, recover-to-latest) → both rows present. Full cycle confirmed.

> **⚠️ Version note:** `spec.backup.barmanObjectStore` is **deprecated** and will be removed in
> CNPG **1.31.0**. Before upgrading past 1.30, migrate to the **Barman Cloud Plugin** (CNPG-I) —
> same backup approach, config moves to a plugin.

## Cleanup

```bash
kubectl delete -f cluster.yaml           # removes the cluster (PVCs may remain per reclaim policy)
kubectl -n postgres delete pvc --all     # remove data volumes
helm -n cnpg-system uninstall cnpg       # remove the operator (optional)
```
