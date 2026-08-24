-- The '-' cleaning invariant, asserted: strike_rate is NULL for exactly the wicketless bowlers
-- and never for anyone who took a wicket. A singular test returns the *offending* rows; zero
-- rows = pass. If dlt's variant handling ever changed, or a numeric strike_rate crept in for a
-- wicketless bowler, this catches it.
select player, season, wickets, strike_rate, is_wicketless
from {{ ref('stg_cricket__bowling') }}
where (is_wicketless and strike_rate is not null)   -- wicketless but somehow has an SR
   or (not is_wicketless and strike_rate is null)   -- took wickets but SR is missing
