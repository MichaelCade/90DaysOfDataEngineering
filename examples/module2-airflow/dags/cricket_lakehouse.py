"""Day 27 (applied 🏏) — the cricket pipeline, orchestrated end-to-end in Airflow.

The whole platform in one scheduled DAG, running in-cluster:

    fetch snapshots ──dlt(merge)──►  Postgres cricket.*  ──Trino CTAS──►  Iceberg lakehouse.cricket.*

- `load_to_postgres`  — dlt reads the batting/bowling/fielding snapshots (from the repo's raw
  URLs — the landed captures) and MERGEs them into Postgres `cricket.*`. Re-runs upsert, so
  running this weekly after each match keeps the tables current (Day 24's incremental, scheduled).
- `promote_to_lakehouse` — Trino materialises those Postgres tables as Iceberg tables in the
  lakehouse (Day 26's CTAS), so Trino/DuckDB/Spark all see the refreshed data.

Secrets come from the Airflow connection `postgres_appdb` (not hard-coded). Trino needs no auth.
Both tasks run as KubernetesExecutor pods that reach `pg-rw.postgres.svc`,
`trino.lakehouse.svc` and the internet directly.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

RAW = ("https://raw.githubusercontent.com/MichaelCade/90DaysOfDataEngineering/"
       "main/examples/cricket/data/snapshots")
SEASON = 2026


@dag(
    dag_id="cricket_lakehouse",
    schedule="0 7 * * 1",                 # 07:00 every Monday (post weekend fixtures)
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    tags=["cricket", "dlt", "lakehouse", "day27"],
)
def cricket_lakehouse():

    @task.virtualenv(requirements=["dlt[postgres]==1.28.0", "openpyxl", "requests"],
                     system_site_packages=True)
    def load_to_postgres():
        import io
        import os
        import json
        import dlt
        import requests
        import openpyxl
        from airflow.hooks.base import BaseHook

        c = BaseHook.get_connection("postgres_appdb")
        os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = (
            f"postgresql://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}"
        )

        RAW = ("https://raw.githubusercontent.com/MichaelCade/90DaysOfDataEngineering/"
               "main/examples/cricket/data/snapshots")

        @dlt.resource(name="batting", write_disposition="merge", primary_key=("player", "season"))
        def batting():
            squad = requests.get(f"{RAW}/batting_2026_v2.json", timeout=30).json()
            for p in squad:
                if p.get("Total Runs", 0):
                    yield {"season": SEASON, **p}

        def _xlsx_rows(filename):
            wb = openpyxl.load_workbook(io.BytesIO(requests.get(f"{RAW}/{filename}", timeout=30).content),
                                        data_only=True)
            rows = list(wb[wb.sheetnames[0]].iter_rows(values_only=True))
            header = rows[0]
            for values in rows[1:]:
                if values and values[1] not in (None, "", "Player"):
                    yield {"season": SEASON, **dict(zip(header, values))}

        @dlt.resource(name="bowling", write_disposition="merge", primary_key=("player", "season"))
        def bowling():
            yield from _xlsx_rows("bowling_2026_v2.xlsx")

        @dlt.resource(name="fielding", write_disposition="merge", primary_key=("player", "season"))
        def fielding():
            yield from _xlsx_rows("fielding_2026_v1.xlsx")

        info = dlt.pipeline(pipeline_name="cricket", destination="postgres",
                            dataset_name="cricket").run([batting(), bowling(), fielding()])
        print(info)

    @task.virtualenv(requirements=["trino"], system_site_packages=True)
    def promote_to_lakehouse():
        from trino.dbapi import connect
        cur = connect(host="trino.lakehouse.svc.cluster.local", port=8080, user="airflow").cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.cricket"); cur.fetchall()
        for t in ("batting", "bowling", "fielding"):
            cur.execute(f"DROP TABLE IF EXISTS lakehouse.cricket.{t}"); cur.fetchall()
            cur.execute(f"CREATE TABLE lakehouse.cricket.{t} AS SELECT * FROM postgres.cricket.{t}")
            cur.fetchall()
            cur.execute(f"SELECT count(*) FROM lakehouse.cricket.{t}")
            print(f"lakehouse.cricket.{t}:", cur.fetchall()[0][0], "rows")

    load_to_postgres() >> promote_to_lakehouse()


cricket_lakehouse()
