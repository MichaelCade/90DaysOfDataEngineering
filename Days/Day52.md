# Day 52 — Soda Core & the SodaCL Check Language

> Module 5: Data Quality & Observability

dbt tests live *inside* the dbt project and run at build time. But you often want to check data that
dbt doesn't own — a landing table, a source, another team's warehouse — on its own schedule.
**Soda Core** is an open-source tool for exactly that: point it at a SQL data source, write checks in
a readable YAML language (**SodaCL**), and run a **scan**. No models, no build — just "is this table
OK right now?"

## Two files: a data source and some checks

**`configuration.yml`** — how to connect:

```yaml
data_source cricket_pg:
  type: postgres
  host: 192.168.169.191
  username: app
  password: ${POSTGRES_APP_PASSWORD}    # env var — no secret in the file
  database: appdb
  schema: cricket
```

**`checks/cricket.yml`** — what to assert, in SodaCL:

```yaml
checks for batting:
  - row_count > 0                       # the load produced rows
  - duplicate_count(player) = 0         # no duplicate players
  - missing_count(player) = 0           # every row has a key
  - failed rows:
      name: total_runs must be non-negative
      fail query: SELECT player, total_runs FROM cricket.batting WHERE total_runs < 0
```

Run it:

```bash
soda scan -d cricket_pg -c configuration.yml checks/cricket.yml
```

## SodaCL — the common check types

| Check | Asserts | Dimension |
|---|---|---|
| `row_count > 0` | table isn't empty | completeness |
| `missing_count(col) = 0` | no nulls | completeness |
| `duplicate_count(col) = 0` | no dupes | uniqueness |
| `invalid_count(col) = 0` + `valid min/max` | values in range | validity |
| `failed rows: fail query:` | a custom SQL rule (returns bad rows) | consistency |
| `freshness(ts) < 1d` | data is recent | timeliness |
| `schema:` | expected columns/types present | contract-ish |

`failed rows` is the escape hatch — any invariant you can express as "SELECT the bad rows" becomes a
check (the same idea as a dbt singular test, but on any table without a dbt project).

## Verified live

Against the cricket landing zone, the committed checks pass:

```
$ soda scan -d cricket_pg -c configuration.yml checks/cricket.yml
11/11 checks PASSED:
  batting: row_count>0, total_runs non-negative, duplicate(player)=0, missing(player)=0, missing(total_runs)=0
  bowling: row_count>0, duplicate(player)=0, invalid_count(economy_rate)=0
  fielding: row_count>0, total_victims reconcile, duplicate(player)=0
All is good. No failures.
```

And a deliberately-too-strict check shows a failure clearly — and Soda **exits non-zero** (2), which
is what lets a scheduler treat it as a gate (Day 53):

```
$ soda scan ... (row_count > 1000)
1/1 check FAILED:  batting ... [FAILED]  check_value: 20
Oops! 1 failures.        # exit code 2
```

## Where Soda fits

Soda scans data **at rest, over SQL** — the natural "between stages" checkpoint. In our pipeline
that's the **Postgres landing zone**: validate what dlt loaded *before* Trino promotes it to Iceberg.

> **Honest caveat:** Soda's Trino connector requires HTTPS, so it can't scan our internal
> plain-HTTP Trino (the lakehouse) as-is — you'd enable Trino TLS (Day 32) first. Gating on the
> Postgres landing zone is the right place anyway: catch a bad load before it propagates, rather
> than after it's already Iceberg.

The committed config lives in
[`examples/module5-quality/soda/`](../examples/module5-quality/soda/).

## Applied example (🏏)

The cricket checks encode the landing-zone contract: the weekly dlt merge must leave non-empty
tables, one row per player (no merge misfire doubling someone), a human-plausible economy rate, and
`total_victims` never fewer than a fielder's catches. If Saturday's export arrives malformed, the
scan goes red and — wired into Airflow tomorrow — stops the pipeline before the bad data becomes
Iceberg and then marts.

## Summary

**Soda Core** validates SQL data sources with readable **SodaCL** checks (`row_count`,
`missing_count`, `duplicate_count`, `invalid_count`, `failed rows`, `freshness`, `schema`) run as a
**scan** — verified 11/11 passing on the cricket landing zone, exiting non-zero on failure. It sits at
the between-stages boundary (Postgres landing), complementing dbt's post-transform tests. Next:
**Day 53 — wiring a Soda scan into Airflow** as a blocking quality stage.
