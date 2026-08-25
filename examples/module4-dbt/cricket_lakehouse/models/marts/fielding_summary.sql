-- Mart: fielding. "Catch efficiency" here = the share of a player's dismissals that came from
-- catches (vs run-outs/stumpings) — the mix of *how* they take victims, split by role so a
-- keeper isn't compared against a slip on raw catch counts.
with f as (
    select * from {{ ref('stg_cricket__fielding') }}
)
select
    player,
    season,
    case when is_keeper then 'keeper' else 'fielder' end as role,
    total_victims,
    total_catches,
    run_outs,
    stumpings,

    {{ pct('total_catches', 'total_victims') }} as catch_pct
from f
