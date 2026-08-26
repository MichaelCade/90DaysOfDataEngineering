"""Day 66 (applied 🏏) — orchestrate a Spark job from Airflow.

The counterpart to running a SparkApplication by hand (`kubectl apply`): here Airflow submits and
monitors it via SparkKubernetesOperator. The operator applies cricket_batch_sparkapp.yaml into the
`spark` namespace, streams the driver logs, and fails the task if the Spark job fails — so a Spark
batch step slots into a DAG exactly like the dbt step (Day 48) did.

Prereqs (applied once, out of band):
  * Spark Operator installed (Module 6, Day 65).
  * RBAC: airflow-worker (ns airflow) may manage SparkApplications in ns spark
    (examples/module6-spark/airflow/airflow-spark-rbac.yaml).
  * Secret `minio-creds` in ns spark (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="spark_cricket_batch",
    schedule=None,                        # trigger manually / wire downstream of cricket_lakehouse
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    tags=["cricket", "spark", "lakehouse", "day66"],
) as dag:
    SparkKubernetesOperator(
        task_id="cricket_batch_spark",
        namespace="spark",
        application_file="cricket_batch_sparkapp.yaml",   # resolved from the dags folder
        kubernetes_conn_id="kubernetes_default",          # in-cluster config (airflow-worker SA)
        get_logs=True,
        delete_on_termination=True,
    )
