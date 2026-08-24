"""Workshop / Day 22 — a first dlt pipeline for #90DaysOfDataEngineering.

Ingests a REST API into Postgres with **zero** hand-written schema: dlt infers the columns
and types from the JSON, flattens nested objects, creates the tables, and tracks the load.

Compare this to the manual extract/create-table/insert we wrote in the Day 14 DAG — dlt does
the "L" (and schema management) for you.

Run it (locally, against the cluster's Postgres LoadBalancer):

    export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
    python jsonplaceholder_pipeline.py

Data lands in the `dlt_workshop` schema of the `appdb` database.
"""
import dlt
from dlt.sources.helpers import requests


@dlt.resource(name="users", write_disposition="replace")
def users():
    """One dlt 'resource' = one table. Yield rows (dicts); dlt figures out the rest."""
    resp = requests.get("https://jsonplaceholder.typicode.com/users")
    resp.raise_for_status()
    yield resp.json()


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="jsonplaceholder",
        destination="postgres",
        dataset_name="dlt_workshop",   # becomes a Postgres schema
    )
    load_info = pipeline.run(users())
    print(load_info)


if __name__ == "__main__":
    main()
