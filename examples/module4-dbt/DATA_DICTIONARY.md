# Cricket Lakehouse — Data Dictionary 🏏

Plain-English guide to every column in the analytics tables, and where each number comes from. The
**raw** stats went in; the pipeline derived the rest. Read this alongside the Metabase tables.

## The raw inputs (what the club actually recorded)

These land from the scrape/CSV exports → dlt → Postgres → Iceberg (`lakehouse.cricket.*`). Everything
else is *computed* from them.

| Raw column | Meaning |
|---|---|
| `player`, `season` | who, and which year (the key: one row per player per season) |
| `total_runs`, `high_score` | runs scored in the season; best single innings |
| `_0_9`, `_10_19`, … `_100_149`, `_150x` | **run-range buckets** — how many innings ended with a score in that band. Each innings falls in exactly one bucket. |
| `overs`, `maidens`, `runs`, `wickets` | bowling: overs bowled, maiden overs, runs conceded, wickets taken |
| `best_bowling` | best figures as text, e.g. `4/15` (4 wickets for 15 runs) |
| `economy_rate`, `strike_rate`, `average` | bowling rates (see below); `'-'` in the source for wicketless bowlers |
| `wicket_keeping_catches`, `stumpings`, `fielding_catches`, `run_outs`, `total_victims` | fielding dismissals |

---

## `batting_summary` — one row per batter per season

| Column | Plain English | How it's computed |
|---|---|---|
| `player`, `season` | the batter, the year | raw |
| `total_runs`, `high_score` | season runs; best innings | raw |
| `innings` | **innings batted** | **sum of the 12 run-range buckets** (`_0_9 + … + _150x`) |
| `fifties` | scores of 50–99 | `_50_59 + _60_69 + _70_79 + _80_89 + _90_99` |
| `hundreds` | scores of 100+ | `_100_149 + _150x` |
| `scores_50_plus` | total "big scores" (50+) | `fifties + hundreds` |
| **`conversion_pct`** | of your 50+ scores, **% turned into hundreds** — killer instinct | `100 × hundreds ÷ (fifties + hundreds)`; NULL if no 50+ scores yet |
| **`early_exit_pct`** | **% of innings out cheaply** (0–9) — soft-dismissal risk | `100 × _0_9 ÷ innings` |
| `runs_per_innings` | rough scoring rate | `total_runs ÷ innings` (⚠️ *not* a true average — the source doesn't give a not-out count) |

---

## `bowling_summary` — one row per bowler per season

| Column | Plain English | How it's computed |
|---|---|---|
| `player`, `season` | the bowler, the year | raw |
| `overs`, `maidens`, `runs`, `wickets` | workload + output | raw |
| `economy_rate` | **runs conceded per over** — how well you *contain* (lower = better) | raw |
| `strike_rate` | **balls per wicket** — how fast you *strike* (lower = better) | raw; **NULL if wicketless** (can't divide by zero wickets) |
| `average` | runs conceded per wicket | raw; NULL if wicketless |
| `five_wicket_hauls` | times you took 5+ in an innings | raw (`_5_wicket_haul`) |
| `best_wickets`, `best_runs` | best figures, split into numbers | parsed from `best_bowling` `"4/15"` → 4 and 15 |
| `is_wicketless` | true if the bowler has no wickets yet | `wickets = 0` (explains the NULL rates) |
| **`bowling_style`** | a label for *how* they're effective | rules on economy & strike rate: `both — economical strike bowler` / `containing` / `attacking` / `wicket-taker` / `no wickets yet` |

> **Economy vs strike rate**: two ways to be a good bowler — keep it tight (economy) or take wickets
> fast (strike rate). `bowling_style` reads both at once. Great as a scatter chart in Metabase.

---

## `fielding_summary` — one row per fielder per season

| Column | Plain English | How it's computed |
|---|---|---|
| `player`, `season` | the fielder, the year | raw |
| `role` | **`keeper` or `fielder`** | `keeper` if they have any keeping catches or stumpings, else `fielder` |
| `total_victims` | all dismissals credited to them | raw |
| `total_catches` | catches (keeping + out-fielding) | raw |
| `run_outs`, `stumpings` | other dismissal types | raw |
| **`catch_pct`** | **% of their victims that were catches** (vs run-outs/stumpings) | `100 × total_catches ÷ total_victims` |

---

## `player_season_summary` — the "one big table" (OBT): one wide row per player per season

Stitches batting + bowling + fielding together so a dashboard reads everything about a player from one
row (a `FULL JOIN` — players who only bat *or* bowl *or* field still appear).

| Column | From | Meaning |
|---|---|---|
| `player`, `season` | key | the player-year |
| `innings`, `total_runs`, `high_score`, `conversion_pct`, `early_exit_pct`, `runs_per_innings` | batting_summary | batting side |
| `wickets`, `economy_rate`, `bowling_strike_rate`, `bowling_style` | bowling_summary | bowling side (`strike_rate` renamed `bowling_strike_rate` to avoid clashing) |
| `fielding_role`, `fielding_victims`, `catch_pct` | fielding_summary | fielding side |
| **`disciplines_contributed`** | derived | **how many of the 3 disciplines** (bat/bowl/field) the player featured in this season — `0–3`. `3` = a genuine all-rounder |

---

## `cricket_spark.batting_summary` — the Spark-written version (Module 6)

Same idea as the dbt `batting_summary`, produced by a **Spark** job instead of dbt (to prove Spark and
Trino share the same Iceberg tables). Columns: `player`, `season`, `total_runs`, `innings`,
`fifty_plus` (= 50+ scores). It's a demo of the engine, not a replacement for the dbt mart.

---

## Two honest caveats

- **`runs_per_innings` is not a batting average.** A real average divides runs by *dismissals*
  (innings minus not-outs). The source only tells us whether a player is *currently* not out (a
  boolean), not a count — so we compute runs ÷ innings and name it honestly.
- **NULLs are meaningful, not missing.** A NULL `conversion_pct` = the batter has no 50+ scores yet; a
  NULL `strike_rate` = a wicketless bowler (`is_wicketless = true`). The pipeline's tests assert these
  NULLs appear *only* where they should.
