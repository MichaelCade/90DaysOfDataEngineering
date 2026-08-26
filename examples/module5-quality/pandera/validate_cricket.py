"""Validate the cricket landing data with Pandera (Day 54).

Pulls cricket.batting from Postgres into a DataFrame and validates it against batting_schema.
Uses lazy=True so *all* failures are reported at once, not just the first.

    export POSTGRES_APP_PASSWORD='...'
    python validate_cricket.py
"""
from __future__ import annotations

import os
import sys

import duckdb
import pandera.pandas as pa

from schemas import batting_schema


def load_batting():
    pw = os.environ["POSTGRES_APP_PASSWORD"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        f"ATTACH 'host=192.168.169.191 port=5432 dbname=appdb user=app password={pw}' "
        "AS pg (TYPE postgres, READ_ONLY);"
    )
    return con.execute(
        "SELECT player, season, total_runs, high_score FROM pg.cricket.batting"
    ).df()


def main() -> int:
    df = load_batting()
    try:
        batting_schema.validate(df, lazy=True)
        print(f"✅ batting: {len(df)} rows valid")
        return 0
    except pa.errors.SchemaErrors as err:
        print(f"❌ batting: {len(err.failure_cases)} failing case(s)")
        print(err.failure_cases[["check", "column", "failure_case"]].to_string(index=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
