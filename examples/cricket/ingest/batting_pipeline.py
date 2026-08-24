"""Cricket applied example — ingest Uffington 1st XI batting stats with dlt (incremental merge).

The *applied* companion to the dlt workshop. The generic Day 24 lesson uses a clean public API
(USGS earthquakes); here we use real, relatable data — obtained the way it realistically has to
be, since Play-Cricket's statistics are not in its API:

  The *extract* is a **browser scrape run in your own logged-in session**
  (see ../scrape/batting_run_ranges.js) that lands a JSON snapshot. dlt takes over at the file.

Incremental in the real world: each scrape is a *snapshot* of the season-to-date, and a later
scrape often lists only the players who featured in the newer games (with their cumulative
totals bumped up). `write_disposition="merge"` on (player, season) is exactly right for that —
it **upserts** the players present in each snapshot and leaves everyone else untouched. Replay
the snapshots in order and the table converges on the current state, with no duplicates.

Snapshots live in ../data/snapshots/. Pick one by CLI arg or env; default is the first capture:

    export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
    python batting_pipeline.py                         # loads batting_2026_v1.json
    python batting_pipeline.py batting_2026_v2.json    # later capture -> merges on top

Data lands in the `cricket` schema of `appdb` (table: batting).
"""
import json
import os
import sys
import dlt

HERE = os.path.dirname(os.path.abspath(__file__))
SNAP_DIR = os.environ.get("CRICKET_SNAPSHOT_DIR", os.path.join(HERE, "..", "data", "snapshots"))
SNAPSHOT = (sys.argv[1] if len(sys.argv) > 1
            else os.environ.get("CRICKET_SNAPSHOT", "batting_2026_v1.json"))
SEASON = int(os.environ.get("CRICKET_SEASON", "2026"))


@dlt.resource(
    name="batting",
    write_disposition="merge",          # upsert, not append — the Day 24 topic
    primary_key=("player", "season"),   # one row per player per season; re-runs update in place
)
def batting(snapshot: str):
    """Read one scraped snapshot and yield a row per player.

    We add `season` (the scrape doesn't carry it) so the table can hold many seasons and the
    merge key is stable. dlt infers every other column + type and normalizes the messy source
    keys ("Total Runs", "0-9", "Not Out") to safe names (total_runs, _0_9, not_out).
    """
    with open(os.path.join(SNAP_DIR, snapshot)) as f:
        squad = json.load(f)
    for player in squad:
        if player.get("Total Runs", 0) == 0:      # skip non-batters (mirrors the personal script)
            continue
        yield {"season": SEASON, **player}


def main() -> None:
    pipeline = dlt.pipeline(pipeline_name="cricket", destination="postgres", dataset_name="cricket")
    load_info = pipeline.run(batting(SNAPSHOT))
    print(f"loaded snapshot: {SNAPSHOT}")
    print(load_info)
    print(f"  batting: {len(pipeline.dataset()['batting'].fetchall())} rows total in cricket.batting")


if __name__ == "__main__":
    main()
