-- Staging: batting. Rename nothing (dlt already gave us clean names) but *derive* the two
-- facts the marts need and the source doesn't state outright:
--   * innings  = Σ of the run-range buckets. Each innings falls in exactly one bucket, so the
--     buckets sum to innings played. This is the anchor for every batting rate downstream.
--   * fifties / hundreds = collapse the buckets into the milestones a coach actually talks about.
with source as (
    select * from {{ source('cricket', 'batting') }}
)
select
    season,
    player,
    total_runs,
    high_score,
    not_out,

    -- innings played = sum of the mutually-exclusive run-range buckets
    ( _0_9 + _10_19 + _20_29 + _30_39 + _40_49
    + _50_59 + _60_69 + _70_79 + _80_89 + _90_99
    + _100_149 + _150x ) as innings,

    ( _50_59 + _60_69 + _70_79 + _80_89 + _90_99 ) as fifties,   -- 50–99
    ( _100_149 + _150x )                            as hundreds,   -- 100+
    _0_9                                            as single_digit_scores,  -- 0–9 ("early exit")

    -- keep the raw buckets so tests can re-check the innings arithmetic
    _0_9, _10_19, _20_29, _30_39, _40_49,
    _50_59, _60_69, _70_79, _80_89, _90_99, _100_149, _150x
from source
