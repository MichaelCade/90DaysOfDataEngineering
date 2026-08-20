"""A first DAG for #90DaysOfDataEngineering — Module 2.

Proves the scheduler + KubernetesExecutor work end to end: each task below runs in its own
pod on the cluster. Delivered to Airflow via git-sync from this repo
(examples/module2-airflow/dags), so pushing a change here makes it appear in the UI.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _say_hello() -> None:
    print("Hello from Airflow on Kubernetes — Module 2 is live!")


with DAG(
    dag_id="hello_data_engineering",
    description="First DAG: verifies Airflow + KubernetesExecutor on the cluster.",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["90days", "module2"],
) as dag:
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'This task ran as its own pod via the KubernetesExecutor.'",
    )

    say_hello >> done
