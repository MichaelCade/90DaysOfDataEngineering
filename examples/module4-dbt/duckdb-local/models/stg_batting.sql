-- Same staging logic as the Trino project, reading the seeded CSV instead of an Iceberg source.
-- (DuckDB and Trino share enough SQL that the derivations port unchanged.)
with source as (
    select * from {{ ref('cricket_batting') }}
)
select
    season,
    player,
    total_runs,
    high_score,
    ( _0_9 + _10_19 + _20_29 + _30_39 + _40_49
    + _50_59 + _60_69 + _70_79 + _80_89 + _90_99 + _100_149 + _150x ) as innings,
    ( _50_59 + _60_69 + _70_79 + _80_89 + _90_99 ) as fifties,
    ( _100_149 + _150x ) as hundreds,
    _0_9 as single_digit_scores
from source
