"""Day 14 — a real ELT DAG for #90DaysOfDataEngineering (Module 2).

The canonical data-engineering shape, end to end on the cluster:

    extract (public API)  ->  land raw in MinIO (the "lake")  ->  load into Postgres
    (the "warehouse")  ->  validate (a quality gate)

It exercises the whole Module 1 + 2 stack: Airflow orchestration on the KubernetesExecutor,
MinIO (S3) as the raw landing zone, and the CloudNativePG Postgres as the warehouse.

Prerequisites (see Days/Day14.md):
  - MinIO bucket `datalake`
  - Airflow connections `minio_s3` (S3/MinIO) and `postgres_appdb` (Postgres appdb)
"""
from __future__ import annotations

import json
from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# A few cities (name -> lat/lon). Open-Meteo is free and needs no API key.
CITIES = {
    "London": (51.51, -0.13),
    "New York": (40.71, -74.01),
    "Tokyo": (35.68, 139.69),
}
S3_CONN = "minio_s3"
BUCKET = "datalake"
PG_CONN = "postgres_appdb"


with DAG(
    dag_id="weather_elt",
    description="Extract weather from an API, land raw in MinIO, load to Postgres, validate.",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["90days", "module2", "elt"],
) as dag:

    @task
    def extract_and_land() -> list:
        """Pull current weather per city, land each raw JSON response in MinIO."""
        ds = get_current_context()["ds"]
        s3 = S3Hook(aws_conn_id=S3_CONN)
        rows = []
        for city, (lat, lon) in CITIES.items():
            resp = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            # Land the raw response in the lake, partitioned by date then city.
            key = f"raw/weather/{ds}/{city.replace(' ', '_')}.json"
            s3.load_string(json.dumps(payload), key=key, bucket_name=BUCKET, replace=True)
            cur = payload["current"]
            rows.append([ds, city, cur["temperature_2m"], cur["relative_humidity_2m"], cur["wind_speed_10m"]])
        print(f"landed {len(rows)} raw files under s3://{BUCKET}/raw/weather/{ds}/")
        return rows

    @task
    def load_to_postgres(rows: list) -> None:
        """Load the parsed rows into the warehouse table — idempotently."""
        ds = get_current_context()["ds"]
        pg = PostgresHook(postgres_conn_id=PG_CONN)
        pg.run(
            """
            CREATE TABLE IF NOT EXISTS weather_readings (
                reading_date   date,
                city           text,
                temperature_c  double precision,
                humidity_pct   double precision,
                wind_speed     double precision,
                PRIMARY KEY (reading_date, city)
            );
            """
        )
        # Idempotent: clear this date's partition first, so re-runs don't duplicate.
        pg.run("DELETE FROM weather_readings WHERE reading_date = %s;", parameters=(ds,))
        pg.insert_rows(
            table="weather_readings",
            rows=rows,
            target_fields=["reading_date", "city", "temperature_c", "humidity_pct", "wind_speed"],
        )
        print(f"loaded {len(rows)} rows into weather_readings for {ds}")

    @task
    def validate() -> None:
        """A simple data-quality gate — fail the run if the data looks wrong."""
        ds = get_current_context()["ds"]
        pg = PostgresHook(postgres_conn_id=PG_CONN)
        (count,) = pg.get_first(
            "SELECT count(*) FROM weather_readings WHERE reading_date = %s;", parameters=(ds,)
        )
        (null_temps,) = pg.get_first(
            "SELECT count(*) FROM weather_readings WHERE reading_date = %s AND temperature_c IS NULL;",
            parameters=(ds,),
        )
        if count != len(CITIES):
            raise ValueError(f"expected {len(CITIES)} rows for {ds}, found {count}")
        if null_temps:
            raise ValueError(f"found {null_temps} null temperature(s) for {ds}")
        print(f"validated: {count} rows for {ds}, no null temperatures")

    landed = extract_and_land()
    load_to_postgres(landed) >> validate()
