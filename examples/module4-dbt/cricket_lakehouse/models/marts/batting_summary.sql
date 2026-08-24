-- Mart: batting metrics a captain would actually pin on the wall.
--   * Conversion %  — of your 50+ scores, how many you turned into hundreds (killer instinct).
--   * Early Exit %  — share of innings that ended in single figures (0–9); soft-dismissal risk.
--   * Runs/innings  — a rough scoring rate. NOT a true average: the source only tells us whether
--                     a player is *currently* not out (a boolean), not a count of not-outs, so a
--                     real (runs / dismissals) average isn't recoverable. We're honest about that.
-- Guards: every rate is wrapped so we never divide by zero — a debutant with no 50s gets a NULL
-- conversion, not a crash, and the tests assert that NULL is the *only* place it appears.
with b as (
    select * from {{ ref('stg_cricket__batting') }}
)
select
    player,
    season,
    innings,
    total_runs,
    high_score,
    fifties,
    hundreds,
    (fifties + hundreds) as scores_50_plus,

    case when (fifties + hundreds) > 0
         then round(100e0 * hundreds / (fifties + hundreds), 1)
    end as conversion_pct,

    case when innings > 0
         then round(100e0 * single_digit_scores / innings, 1)
    end as early_exit_pct,

    case when innings > 0
         then round(total_runs * 1e0 / innings, 1)
    end as runs_per_innings
from b
