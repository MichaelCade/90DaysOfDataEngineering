-- Same batting metrics as the Trino mart. DuckDB's round()/case are standard SQL, so the only
-- change from the lakehouse version is the source (a seed) — the metric logic is identical.
with b as (
    select * from {{ ref('stg_batting') }}
)
select
    player,
    season,
    innings,
    total_runs,
    (fifties + hundreds) as scores_50_plus,
    case when (fifties + hundreds) > 0
         then round(100.0 * hundreds / (fifties + hundreds), 1) end as conversion_pct,
    case when innings > 0
         then round(100.0 * single_digit_scores / innings, 1) end as early_exit_pct
from b
