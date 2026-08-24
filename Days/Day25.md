# Day 25 — dlt: Schema Evolution & Data Contracts

> Workshop: Data Ingestion

Sources change shape. An API adds a field, a vendor renames one, a **scrape** suddenly includes
a new stat because the site added a column. With hand-written `CREATE TABLE` that's a migration
and a 2am broken load. dlt takes a different default: it **evolves the destination schema to
match the data** — and, when you'd rather *not* accept surprises, lets you put a **contract** in
the way.

## Default: automatic evolution

A new field in the data becomes a new column in the table — typed from the values, NULL for the
rows that predate it. No DDL, no migration. That's why every example so far "just worked" the
first time: dlt created the tables and columns from what it saw.

## When automatic isn't what you want: data contracts

Auto-evolution is great in dev and dangerous in production — a source that silently changes can
quietly corrupt a table. A **data contract** sets the policy, at three levels (`tables`,
`columns`, `data_type`), each one of:

- `evolve` — accept the change (the default).
- `freeze` — **reject** it: the load raises, nothing changes. Use in production to fail loud.
- `discard_row` / `discard_value` — drop the offending row or just the unexpected value.

## Generic example — evolve, then freeze

[`examples/workshop-dlt/schema_evolution_pipeline.py`](../examples/workshop-dlt/schema_evolution_pipeline.py)
loads the same table three times, changing the source shape each time:

```
phase 1 (id, name, city)       -> ['id', 'name', 'city']
phase 2 (+ country, evolve)    -> ['id', 'name', 'city', 'country']      # dlt added the column
phase 3 (+ age, freeze)        -> rejected by contract (PipelineStepFailed)
   columns unchanged            -> ['id', 'name', 'city', 'country']      # nothing slipped in
```

Phase 2 is dlt adapting for you; phase 3 is you drawing a line:

```python
pipeline.run(resource, schema_contract={"columns": "freeze"})   # new columns -> raise
```

## Applied angle — why this matters *most* for a scrape (🏏)

The cricket batting data comes from a **DOM scrape** (Day 24), and scrapes are the single most
fragile kind of source — Play-Cricket tweaks a stats page, a column shifts, and your parse
quietly changes shape. Two dlt behaviours map straight onto that:

- **Evolve (dev):** if a future scrape starts capturing a new stat — say the site adds a
  "Stumped" dismissal or "Balls Faced" — `cricket.batting` gains the column automatically; your
  existing rows just carry NULL for it. Zero code change to keep ingesting.
- **Freeze (production):** once the coaching reports depend on a fixed set of columns, load the
  cricket pipeline under `schema_contract={"columns": "freeze"}`. Now if the scrape drifts and
  starts emitting an unexpected field, the load **fails loudly** instead of silently reshaping
  the table the dbt models and briefings are built on. For a scraped source, that guard rail is
  worth a lot.

Rule of thumb: **`evolve` while you explore, `freeze` once something downstream depends on the
shape.**

## Summary

dlt evolves the destination schema to match incoming data by default — new fields become new
columns, no migration. **Data contracts** (`evolve` / `freeze` / `discard`) at the table, column,
and type levels let you take back control, which matters most for fragile sources like scrapes:
evolve while developing, freeze once dbt and reports depend on the shape. That wraps the dlt
core (Days 22–25). Next in the applied thread: ingest the **bowling** data too, then the
workshop's finale — **Day 26 (dlt → Iceberg)** and **Day 27 (dlt inside Airflow)** — once the
Module 3 lakehouse is deployed.
