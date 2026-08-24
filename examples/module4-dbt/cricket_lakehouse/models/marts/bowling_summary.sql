-- Mart: bowling metrics, framed as the classic trade-off between the two ways to be good.
--   * economy_rate  — runs per over: how well you *contain*.
--   * strike_rate   — balls per wicket: how fast you *strike* (NULL if wicketless).
-- A great spell can win on either axis; putting them side by side is the point. We surface the
-- wicketless bowlers explicitly (is_wicketless) rather than letting their NULLs read as "missing".
with b as (
    select * from {{ ref('stg_cricket__bowling') }}
)
select
    player,
    season,
    overs,
    maidens,
    runs,
    wickets,
    economy_rate,        -- containment
    strike_rate,         -- penetration (balls per wicket); NULL for wicketless bowlers
    average,             -- runs per wicket; NULL for wicketless bowlers
    five_wicket_hauls,
    best_wickets,
    best_runs,
    is_wicketless,

    -- a compact label for how a bowler earns their keep
    case
        when is_wicketless then 'no wickets yet'
        when economy_rate < 4.0 and strike_rate < 20 then 'both — economical strike bowler'
        when economy_rate < 4.0                       then 'containing (tight economy)'
        when strike_rate < 20                         then 'attacking (quick strikes)'
        else 'wicket-taker'
    end as bowling_style
from b
