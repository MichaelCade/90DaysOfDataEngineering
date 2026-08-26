"""Pandera schemas for the cricket data — validation at the *DataFrame* level (in-flight).

Where Soda checks tables at rest (SQL) and dbt checks model outputs, Pandera checks a pandas
DataFrame *in Python* — the ideal place to validate data the moment it's extracted, before it's
loaded anywhere. Import these and call `.validate(df)` (or `validate(df, lazy=True)` to collect
every failure at once).
"""
from __future__ import annotations

import pandera.pandas as pa
from pandera import Check, Column

# One row per player per season. Column checks assert type + value rules; the frame-level check
# asserts a cross-column invariant (a high score can't exceed the player's total runs).
batting_schema = pa.DataFrameSchema(
    {
        "player": Column(str, Check.str_length(min_value=1), nullable=False),
        "season": Column(int, Check.ge(2000)),
        "total_runs": Column(int, Check.ge(0)),
        "high_score": Column(int, Check.in_range(0, 400)),
    },
    checks=Check(
        lambda d: d["high_score"] <= d["total_runs"],
        name="high_score_le_total_runs",
        error="high_score must not exceed total_runs",
    ),
    strict=False,   # allow extra columns (dlt adds _dlt_id etc.); set True to enforce exact shape
    coerce=True,
)

bowling_schema = pa.DataFrameSchema(
    {
        "player": Column(str, Check.str_length(min_value=1), nullable=False),
        "season": Column(int, Check.ge(2000)),
        "wickets": Column(int, Check.ge(0)),
        # economy rate is nullable in raw data but, when present, must be human-plausible
        "economy_rate": Column(float, Check.in_range(0, 15), nullable=True),
    },
    strict=False,
    coerce=True,
)
