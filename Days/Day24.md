# Day 24 — dlt: Incremental Loading

> Workshop: Data Ingestion

Days 22–23 did **full loads** — pull the whole endpoint every run. That's fine for 10 users or
150 pokémon, but it doesn't scale: re-reading everything is wasteful, and past a certain size
it's impossible. **Incremental loading** pulls only what's new or changed since last time — and
the hard part it solves for you is *state*: "where did I get to last run?"

## Two independent choices

Incremental is really two orthogonal decisions:

**1. How each run writes (`write_disposition`)**
- `replace` — wipe and reload (full refresh). Days 22–23.
- `append` — add new rows only. Good for immutable events (logs, readings).
- `merge` — **upsert** on a primary key: update rows that exist, insert those that don't. The
  workhorse for records that *change* (an order's status, a player's running total).

**2. What "new" means (`dlt.sources.incremental`)**
A **cursor** — a field that only moves forward: an `id`, a `created_at`, an `updated_at`. dlt
stores the cursor's **last value** in pipeline state (the `_dlt_pipeline_state` table in the
destination) and, next run, filters out everything at or below it. You never track offsets by
hand; dlt remembers.

## Generic example — the cursor, and state that persists

[`examples/workshop-dlt/incremental_pipeline.py`](../examples/workshop-dlt/incremental_pipeline.py)
pulls jsonplaceholder comments with a cursor on `id` and `merge` on `id`:

```python
@dlt.resource(name="comments", write_disposition="merge", primary_key="id")
def comments(updated=dlt.sources.incremental("id", initial_value=0)):
    resp = requests.get("https://.../comments", params={"id_gte": updated.last_value or 0})
    yield resp.json()          # dlt drops anything with id <= the last id it saw
```

Run it twice:

```
RUN 1 (full load)                 rows loaded this run: 500   total: 500
RUN 2 (cursor remembered)         rows loaded this run:   0   total: 500
```

**That "0 new on re-run" is the whole lesson.** dlt persisted the high-water mark (`id = 500`)
and, on the second run, filtered everything out — no duplicates, no re-work. Delete the pipeline
state and it would reload from scratch; that state *is* the incremental.

## Applied example — merge with data that actually changes (🏏)

The generic case shows a *no-op* re-run. The cricket data shows the *interesting* case: totals
that grow. [`examples/cricket/ingest/batting_pipeline.py`](../examples/cricket/ingest/batting_pipeline.py)
loads a scraped season snapshot, `merge` on `(player, season)`. We captured the season twice —
`batting_2026_v1.json`, then `batting_2026_v2.json` after more games — and replayed them in order:

| Player | after v1 | after v2 (merge) |
|---|---|---|
| Jonathan Dalley | 549 | **754** |
| Nathan Botes | 179 | **275** |
| Michael Cade | 129 | **145** |
| Seth Roberts | — | **133** (new) |

Row count **18 → 20** — every returning player's total updated *in place*, two new players
inserted, nobody duplicated or lost. That's exactly how you'd run it for real: re-scrape after
each match, load again, and `merge` converges the table on the latest state. Each scrape is a
partial-or-full snapshot; merge doesn't care, it upserts what's present.

## Full vs incremental — the trade

Full loads are simple and self-correcting (every run is the truth), but wasteful. Incremental is
efficient but you're trusting a cursor and a merge key — pick them wrong (a timestamp that goes
backwards, a non-unique key) and you drop or double data. Rule of thumb: **`append` for immutable
events, `merge` for mutable records**, and choose a cursor that genuinely only moves forward.

## Summary

Incremental loading = load only what changed, with dlt holding the **state** (the cursor's last
value) so re-runs are cheap and correct. `append` adds, `merge` upserts, and
`dlt.sources.incremental` tracks the high-water mark for you. We saw both: a generic re-run that
loaded **0** because the cursor was remembered, and cricket totals that **updated in place** via
merge across two real captures. Next: **Day 25 — schema evolution**, where the *source* changes
shape (a new field appears) and dlt adapts the destination table automatically.
