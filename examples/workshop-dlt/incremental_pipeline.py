"""Workshop / Day 24 — incremental loading with dlt.

Day 23 pulled whole endpoints every run (a *full* load). That doesn't scale: re-pulling
everything is wasteful and, past a certain size, impossible. **Incremental loading** pulls only
what's new or changed since last time — which means dlt has to remember "where did I get to?".

Two independent choices:
  - write_disposition: `append` (add new rows) vs `merge` (upsert on a key) vs `replace`.
  - `dlt.sources.incremental("<cursor>")` — a field that only moves forward (an id, a
    timestamp). dlt stores its **last value** in pipeline state and, on the next run, skips
    everything at or below it. That state lives in the `_dlt_pipeline_state` table.

Here we page jsonplaceholder comments with a cursor on `id` and `merge` on `id`. Run it twice:
the first run loads every comment; the second loads **0** — because the cursor is already at the
max id and dlt filters the rest out. That "0 new on re-run" is the whole point: state persisted.

    export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
    python incremental_pipeline.py        # run 1: loads all
    python incremental_pipeline.py        # run 2: loads 0 new (cursor remembered)

Data lands in the `dlt_workshop` schema of `appdb` (table: comments).
"""
import dlt
from dlt.sources.helpers import requests


@dlt.resource(name="comments", write_disposition="merge", primary_key="id")
def comments(updated=dlt.sources.incremental("id", initial_value=0)):
    """Yield comments; dlt's `incremental` on `id` filters out anything <= the last id seen.

    `updated.last_value` is the high-water mark dlt persisted from the previous run. We pass it
    to the API as a hint (`id_gte`) so we don't even fetch old rows where the source supports
    it — and dlt still enforces the cursor client-side regardless.
    """
    resp = requests.get(
        "https://jsonplaceholder.typicode.com/comments",
        params={"id_gte": updated.last_value or 0},
    )
    resp.raise_for_status()
    yield resp.json()


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="jsonplaceholder_incr",
        destination="postgres",
        dataset_name="dlt_workshop",
    )
    load_info = pipeline.run(comments())
    print(load_info)

    # How many rows did THIS run actually load? (0 on a no-op re-run.)
    print("  rows loaded this run:", pipeline.last_trace.last_normalize_info.row_counts.get("comments", 0))
    print("  total rows in table :", len(pipeline.dataset()["comments"].fetchall()))


if __name__ == "__main__":
    main()
