-- Mart: one big table (OBT), one row per player per season, stitching batting + bowling +
-- fielding into the single wide row a dashboard or a coaching briefing reads from. Not everyone
-- bats *and* bowls *and* fields, so it's a FULL OUTER JOIN on (player, season) and we coalesce
-- the keys. This is the OBT pattern — denormalised for read convenience — sitting on top of the
-- focused per-discipline marts, so the DAG is source -> staging -> per-discipline mart -> OBT.
with bat as (
    select * from {{ ref('batting_summary') }}
),
bowl as (
    select * from {{ ref('bowling_summary') }}
),
field as (
    select * from {{ ref('fielding_summary') }}
),
keys as (
    select player, season from bat
    union select player, season from bowl
    union select player, season from field
)
select
    k.player,
    k.season,

    -- batting
    bat.innings,
    bat.total_runs,
    bat.high_score,
    bat.conversion_pct,
    bat.early_exit_pct,
    bat.runs_per_innings,

    -- bowling
    bowl.wickets,
    bowl.economy_rate,
    bowl.strike_rate    as bowling_strike_rate,
    bowl.bowling_style,

    -- fielding
    field.role          as fielding_role,
    field.total_victims as fielding_victims,
    field.catch_pct,

    -- a quick "how many disciplines did this player contribute in" count (0–3)
    ( if(bat.total_runs   is not null, 1, 0)
    + if(bowl.wickets     is not null, 1, 0)
    + if(field.total_victims is not null, 1, 0) ) as disciplines_contributed
from keys k
left join bat   on bat.player   = k.player and bat.season   = k.season
left join bowl  on bowl.player  = k.player and bowl.season  = k.season
left join field on field.player = k.player and field.season = k.season
