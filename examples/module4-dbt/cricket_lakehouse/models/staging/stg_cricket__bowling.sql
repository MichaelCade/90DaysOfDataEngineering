-- Staging: bowling. This is where we *clean the '-' strike rates* (the Module 4 promise).
--
-- Wicketless bowlers can't have a strike rate or average (you divide by wickets). The source
-- writes '-'; dlt refused to coerce that into a number and instead kept the numeric columns
-- typed (NULL here) and parked the literal '-' in `strike_rate__v_text` / `average__v_text`.
-- So the cleaning in dbt is *interpretation*, not string surgery: give the NULL a meaning
-- (is_wicketless) and drop the variant text columns from the clean model.
--
-- We also split the source's "best_bowling" text ("4/15") into two typed columns.
with source as (
    select * from {{ source('cricket', 'bowling') }}
)
select
    season,
    rank,
    player,
    overs,
    maidens,
    runs,
    wickets,
    economy_rate,
    strike_rate,                      -- NULL for wicketless bowlers (was '-')
    average,                          -- NULL for wicketless bowlers (was '-')
    _5_wicket_haul as five_wicket_hauls,

    (wickets = 0) as is_wicketless,   -- explains *why* strike_rate/average are NULL

    -- "4/15" -> best_wickets=4, best_runs=15  (NULL when no best recorded)
    try_cast(split_part(best_bowling, '/', 1) as integer) as best_wickets,
    try_cast(split_part(best_bowling, '/', 2) as integer) as best_runs,
    best_bowling
from source
