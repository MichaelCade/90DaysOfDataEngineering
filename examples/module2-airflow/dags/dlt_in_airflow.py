"""Day 27 (generic) — running a dlt pipeline as a scheduled Airflow task, in-cluster.

The dlt workshop ran pipelines from a laptop. Production ingestion runs *on a schedule, next to
the data*. With Airflow on KubernetesExecutor, each task becomes its own pod in the cluster —
so a task pod reaches `pg-rw.postgres.svc` directly (no LoadBalancer needed).

dlt isn't in the Airflow image, so we use `@task.virtualenv`: Airflow builds a fresh virtualenv
inside the task pod, pip-installs dlt, and runs the function there. `system_site_packages=True`
lets that venv also see Airflow itself, so we can pull DB credentials from the **Airflow
connection** `postgres_appdb` instead of hard-coding secrets in this (public) repo.

Same dlt code as Day 22 — only *where* it runs changed.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="dlt_in_airflow",
    schedule="0 6 * * *",                 # 06:00 daily
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    tags=["workshop", "dlt", "day27"],
)
def dlt_in_airflow():
    @task.virtualenv(
        requirements=["dlt[postgres]==1.28.0"],
        system_site_packages=True,        # so we can import airflow's BaseHook for the connection
    )
    def load_users():
        import os
        import dlt
        from dlt.sources.helpers import requests
        from airflow.hooks.base import BaseHook

        # creds from the Airflow connection — never written into the repo
        c = BaseHook.get_connection("postgres_appdb")
        os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = (
            f"postgresql://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}"
        )

        @dlt.resource(name="users", write_disposition="replace")
        def users():
            resp = requests.get("https://jsonplaceholder.typicode.com/users")
            resp.raise_for_status()
            yield resp.json()

        info = dlt.pipeline(
            pipeline_name="airflow_jsonplaceholder",
            destination="postgres",
            dataset_name="dlt_workshop",
        ).run(users())
        print(info)

    load_users()


dlt_in_airflow()
