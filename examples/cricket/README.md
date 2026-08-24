# Applied Example: Uffington 1st XI Cricket Stats 🏏

A real, relatable dataset threaded through the course as the **applied companion** to each
module's neutral textbook example. Same concepts, real data — Uffington Cricket Club 1st XI
**batting, bowling and fielding**, refreshable after every game.

## Where the data comes from (the honest bit)

Play-Cricket's **statistics** are not available through its public API (the API only exposes
match scorecards, and only to club admins). So the data is obtained directly from the site —
two different ways, because the stats pages differ:

- **Batting run-ranges → browser scrape.** The run-range breakdown (0–9, 10–19, … 150+) has no
  export, so [`scrape/batting_run_ranges.js`](scrape/batting_run_ranges.js) is pasted into the
  browser console **while you're logged in**; it walks each player's page and downloads a JSON
  snapshot. You run it in your own session — it's the *extract* step and it stays with you.
- **Bowling & fielding → the site's "Export to CSV".** Those stats tables have a built-in export,
  cleaner and more robust than scraping — just export and drop the `.xlsx` in.

Either way, **dlt takes over at the landed file.** "The extract is something I run; the pipeline
starts at the file" is a common real-world pattern — not every source is a clean API.

> Snapshots are point-in-time. Re-capture after each game and re-run — `merge` updates the
> tables in place (that's the Day 24 demo below).

## Layout

```
examples/cricket/
├── scrape/batting_run_ranges.js         # batting extract (browser console, your session)
├── data/snapshots/
│   ├── batting_2026_v1.json             # first batting capture (27 players)
│   ├── batting_2026_v2.json             # later capture (29 players, updated totals)
│   ├── bowling_2026_v1.xlsx             # earlier CSV export (Harry Carter 28 wkts)
│   ├── bowling_2026_v2.xlsx             # later CSV export  (Harry Carter 37 wkts)
│   └── fielding_2026_v1.xlsx            # CSV export (keeping + out-fielding)
└── ingest/
    ├── batting_pipeline.py              # dlt: JSON snapshot -> cricket.batting  (merge)
    ├── bowling_pipeline.py              # dlt: xlsx  export  -> cricket.bowling  (merge)
    └── fielding_pipeline.py             # dlt: xlsx  export  -> cricket.fielding (merge)
```

## How it maps onto the modules

| Stage | This example | Module |
|---|---|---|
| Ingest landed files → warehouse, typed, incremental `merge` | `ingest/*.py` | 2 — dlt ✅ |
| Land raw per season in MinIO / Iceberg, partition by season, time-travel across years | _(to come)_ | 3 — Lakehouse |
| Ranges/dismissals/fielding → metrics (Conversion %, Early Exit %, econ-vs-SR, catch efficiency) as tested models; **clean the `-` strike rates here** | _(to come)_ | 4 — dbt |
| Assert invariants (innings = Σ ranges; % in 0–100; dismissals ≤ innings) | _(to come)_ | 5 — Quality |
| Schedule: ingest → transform → test → regenerate coaching briefings | _(to come)_ | 2/6 — Airflow |

## Run the ingest (Module 2)

```bash
pip install openpyxl        # for the .xlsx exports
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"

# batting — load first capture, then merge the later one on top
python ingest/batting_pipeline.py                       # batting_2026_v1.json
python ingest/batting_pipeline.py batting_2026_v2.json  # -> merges (totals update in place)

# bowling & fielding — same pattern with the CSV exports
python ingest/bowling_pipeline.py                       # bowling_2026_v1.xlsx
python ingest/bowling_pipeline.py bowling_2026_v2.xlsx  # -> merges (Harry Carter 28 -> 37)
python ingest/fielding_pipeline.py                      # fielding_2026_v1.xlsx
```

Everything lands in the `cricket` schema of `appdb` (`batting`, `bowling`, `fielding`), keyed
`(player, season)` with `write_disposition="merge"` — re-run any time and rows upsert, never
duplicate. Things dlt did for free that the personal pandas scripts do by hand:

- **normalized messy keys** — `"Not Out"` → `not_out` (boolean), `"Total Runs"` → `total_runs`,
  `"150+"` → `_150x`, `"ECONOMY RATE"` → `economy_rate`, `"WICKET KEEPING CATCHES"` →
  `wicket_keeping_catches`.
- **handled a mixed-type column** — wicketless bowlers have `'-'` for strike rate; dlt kept the
  numbers in a typed `strike_rate` (double) and parked the `'-'` in a `strike_rate__v_text`
  **variant** column, instead of erroring (compare `bowling-stats.py`'s manual `str.replace('-','')`).
- **lineage + load tracking** — `_dlt_id`, `_dlt_load_id`, `_dlt_loads`.
