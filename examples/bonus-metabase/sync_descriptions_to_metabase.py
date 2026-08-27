"""Push plain-English column descriptions into Metabase so hovering a field explains itself.

Seeded from the cricket data dictionary (examples/module4-dbt/DATA_DICTIONARY.md), which is the
richer superset of the dbt model descriptions. Matches Metabase tables/fields by name and PUTs a
description onto each — table-level and column-level.

YOU run this (it needs Metabase admin access); it never ships credentials in the repo. Auth via an
API key (Admin ▸ Settings ▸ API Keys — recommended) or an admin email/password session.

    export METABASE_URL="https://metabase.192.168.169.190.nip.io"
    export METABASE_API_KEY="mb_xxx"                 # or METABASE_USER + METABASE_PASSWORD
    export METABASE_DB="Cricket Lakehouse"           # the data-source display name you created
    python sync_descriptions_to_metabase.py            # add --dry-run to preview without writing
"""
import os
import sys
import urllib3
import requests

urllib3.disable_warnings()
DRY = "--dry-run" in sys.argv
URL = os.environ.get("METABASE_URL", "https://metabase.192.168.169.190.nip.io").rstrip("/")
DB_NAME = os.environ.get("METABASE_DB", "Cricket Lakehouse")

# table description + {column: description}, keyed by table name (schema cricket_dbt / cricket_spark)
DICT = {
    "batting_summary": {
        "_table": "Batting metrics per player per season (Conversion %, Early Exit %, runs/innings).",
        "innings": "Innings batted = sum of the 12 run-range buckets (each innings lands in one band).",
        "fifties": "Scores of 50-99 in the season.",
        "hundreds": "Scores of 100+ in the season.",
        "scores_50_plus": "Total 50+ scores (fifties + hundreds).",
        "conversion_pct": "Of your 50+ scores, the % turned into hundreds (killer instinct). NULL if no 50+ scores yet.",
        "early_exit_pct": "% of innings out cheaply (0-9) - soft-dismissal risk.",
        "runs_per_innings": "Runs / innings. NOT a true average (source lacks a not-out count).",
        "total_runs": "Total runs scored in the season (raw).",
        "high_score": "Best single innings in the season (raw).",
    },
    "bowling_summary": {
        "_table": "Bowling metrics per player per season - economy vs strike rate, five-fers, best figures.",
        "economy_rate": "Runs conceded per over - how well you contain (lower is better).",
        "strike_rate": "Balls per wicket - how fast you strike (lower is better). NULL if wicketless.",
        "average": "Runs conceded per wicket. NULL if wicketless.",
        "five_wicket_hauls": "Times the bowler took 5+ wickets in an innings.",
        "best_wickets": "Best-figures wickets, parsed from best_bowling (e.g. 4 from '4/15').",
        "best_runs": "Best-figures runs conceded, parsed from best_bowling (e.g. 15 from '4/15').",
        "is_wicketless": "True if the bowler has no wickets yet (explains NULL strike_rate/average).",
        "bowling_style": "Label for how the bowler is effective, from economy & strike rate.",
    },
    "fielding_summary": {
        "_table": "Fielding per player per season - role, victims, catch %.",
        "role": "'keeper' (any keeping catch/stumping) or 'fielder'.",
        "total_victims": "All dismissals credited to the player.",
        "total_catches": "Catches (keeping + out-fielding).",
        "catch_pct": "% of the player's victims that were catches (vs run-outs/stumpings).",
    },
    "player_season_summary": {
        "_table": "One Big Table: one wide row per player per season across batting, bowling, fielding.",
        "bowling_strike_rate": "Balls per wicket (bowling_summary.strike_rate, renamed to avoid a clash).",
        "fielding_role": "keeper/fielder (from fielding_summary).",
        "fielding_victims": "Dismissals credited (from fielding_summary).",
        "disciplines_contributed": "How many of the 3 disciplines (bat/bowl/field) the player featured in this season (0-3). 3 = all-rounder.",
    },
}


def auth_headers():
    key = os.environ.get("METABASE_API_KEY")
    if key:
        return {"x-api-key": key}
    user, pw = os.environ.get("METABASE_USER"), os.environ.get("METABASE_PASSWORD")
    if not (user and pw):
        sys.exit("Set METABASE_API_KEY, or METABASE_USER + METABASE_PASSWORD.")
    tok = requests.post(f"{URL}/api/session", json={"username": user, "password": pw},
                        verify=False, timeout=15).json()["id"]
    return {"X-Metabase-Session": tok}


def main():
    h = auth_headers()
    dbs = requests.get(f"{URL}/api/database", headers=h, verify=False, timeout=15).json()
    dbs = dbs.get("data", dbs)
    db = next((d for d in dbs if d["name"] == DB_NAME), None)
    if not db:
        sys.exit(f"Database '{DB_NAME}' not found. Names: {[d['name'] for d in dbs]}")
    meta = requests.get(f"{URL}/api/database/{db['id']}/metadata", headers=h, verify=False, timeout=30).json()

    updated_t = updated_f = 0
    for t in meta.get("tables", []):
        spec = DICT.get(t["name"])
        if not spec:
            continue
        if spec.get("_table"):
            print(f"[table] {t['schema']}.{t['name']}: {spec['_table']}")
            if not DRY:
                requests.put(f"{URL}/api/table/{t['id']}", headers=h, verify=False, timeout=15,
                             json={"description": spec["_table"]})
            updated_t += 1
        for fld in t.get("fields", []):
            desc = spec.get(fld["name"])
            if not desc:
                continue
            print(f"  [field] {t['name']}.{fld['name']}: {desc}")
            if not DRY:
                requests.put(f"{URL}/api/field/{fld['id']}", headers=h, verify=False, timeout=15,
                             json={"description": desc})
            updated_f += 1
    print(f"\n{'DRY-RUN, would update' if DRY else 'Updated'}: {updated_t} tables, {updated_f} fields.")


if __name__ == "__main__":
    main()
