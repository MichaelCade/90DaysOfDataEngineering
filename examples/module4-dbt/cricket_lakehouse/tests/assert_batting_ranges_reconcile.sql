-- Cross-check the derived innings against the raw buckets, and sanity-check the components:
-- the single-digit (0–9) count and the sum of milestone buckets can never exceed innings.
-- Returns offending rows; zero rows = pass.
select
    player,
    season,
    innings,
    single_digit_scores,
    (fifties + hundreds) as milestone_scores
from {{ ref('stg_cricket__batting') }}
where single_digit_scores > innings
   or (fifties + hundreds) > innings
   or innings < 0
