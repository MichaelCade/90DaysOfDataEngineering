# Day 21 — Data Ingestion Patterns: Batch, Micro-batch & Streaming

> Workshop: Data Ingestion

Getting data *into* your platform is the first practical problem in any data pipeline.
Before we pick a tool, it's worth knowing the three shapes ingestion comes in — because the
shape drives the tooling.

## The three patterns

- **Batch** — collect data and load it on a schedule (hourly, nightly). Simple, cheap,
  easy to reason about and re-run. The right default for the vast majority of analytics.
  *"Load yesterday's orders every morning at 6am."*
- **Micro-batch** — the same idea, but small and frequent (every few minutes). Lower latency
  than nightly batch without the complexity of true streaming. *"Load new records every 5
  minutes."*
- **Streaming** — process each event as it arrives, continuously, with sub-second latency.
  Powerful but operationally heavier — you're running always-on infrastructure (Kafka, Flink)
  and dealing with ordering, late data, and exactly-once semantics. Reserve it for use cases
  that genuinely need real-time. *(We get to streaming properly in Module 7 with Kafka.)*

A useful rule of thumb: **start with batch.** Move to micro-batch when latency matters, and
to streaming only when the business truly can't wait minutes. Most "we need real-time"
requirements are satisfied by a 5-minute micro-batch.

## Full vs incremental

Orthogonal to the pattern is *how much* you load each time:

- **Full load** — re-read the entire source every run. Simple and self-correcting, but
  wasteful (and often impossible) as data grows.
- **Incremental load** — only pull what's changed since last time (by a timestamp, an
  incrementing id, or a change feed). Efficient, but you have to track state — "where did I
  get to last time?" — and handle updates, not just inserts.

Incremental is where ingestion gets fiddly, and it's exactly the kind of state-tracking a
good ingestion tool should handle for you (Day 24).

## Where dlt fits

For **batch and micro-batch** ingestion — which is most of what a data platform needs —
**dlt** is a lightweight, Python-native fit. It's a library, not a platform: you write a
short pipeline, and it handles schema inference, normalization, typing, incremental state,
and load tracking. You run it on a schedule (in our case, eventually as an Airflow task).

It's the **E and L** of ELT: extract from the source, load into the destination, and let the
transformation happen *after* landing (that's dbt's job, in Module 4). Keeping extract-load
separate from transform is a deliberate modern-stack choice — it means your raw data is
always landed and replayable before anyone reshapes it.

## Summary

Ingestion comes in **batch**, **micro-batch**, and **streaming** shapes, crossed with
**full** vs **incremental** loading. Batch + incremental covers most real needs; streaming
is for genuine real-time. For batch/micro-batch we'll use **dlt** — starting tomorrow
(Day 22) with its core concepts and a first pipeline.
