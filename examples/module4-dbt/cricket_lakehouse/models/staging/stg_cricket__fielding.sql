-- Staging: fielding. Split the squad into the two roles that make the numbers comparable —
-- a wicket-keeper's catches and a slip fielder's catches are different jobs — and expose the
-- dismissal components the mart turns into a "how did this player take their victims" mix.
with source as (
    select * from {{ source('cricket', 'fielding') }}
)
select
    season,
    rank,
    player,

    wicket_keeping_catches,
    stumpings,
    fielding_catches,
    run_outs,
    total_catches,                                        -- keeping + out-fielding catches
    total_victims,                                        -- all dismissals credited to the player

    -- a keeper is anyone credited with a stumping or a keeping catch
    (wicket_keeping_catches > 0 or stumpings > 0) as is_keeper
from source
