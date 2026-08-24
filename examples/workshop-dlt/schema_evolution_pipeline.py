"""Workshop / Day 25 — schema evolution & data contracts with dlt.

Sources change shape. An API adds a field; a scrape suddenly includes a new stat. With
hand-written DDL that means a migration and a broken load. dlt, by default, **evolves the
destination schema automatically** — a new field in the data becomes a new column in the table,
typed and backfilled as NULL for old rows. When you'd rather *not* accept surprise changes, a
**data contract** lets you freeze the schema and reject (or discard) anything unexpected.

This one script tells the whole story in three phases against one table:

    export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
    python schema_evolution_pipeline.py

Table: `dlt_workshop.players_evo`.
"""
import dlt


def make_players(cols):
    """Yield two rows, optionally including extra columns — to simulate a source changing shape."""
    base = [{"id": 1, "name": "Ava"}, {"id": 2, "name": "Ben"}]
    for r in base:
        row = dict(r)
        if "city" in cols:
            row["city"] = "Oxford" if r["id"] == 1 else "Bath"
        if "country" in cols:
            row["country"] = "UK"
        if "age" in cols:
            row["age"] = 30 + r["id"]
        yield row


def columns_of(pipeline, table):
    with pipeline.sql_client() as c:
        rows = c.execute_sql(
            "select column_name from information_schema.columns "
            "where table_schema=%s and table_name=%s order by ordinal_position",
            pipeline.dataset_name, table,
        )
    # hide dlt's bookkeeping columns for clarity
    return [r[0] for r in rows if not r[0].startswith("_dlt")]


def res(cols):
    return dlt.resource(make_players(cols), name="players_evo", write_disposition="replace")


def main() -> None:
    p = dlt.pipeline(pipeline_name="schema_evo", destination="postgres", dataset_name="dlt_workshop")

    # Phase 1 — initial shape: id, name, city
    p.run(res({"city"}))
    print("phase 1 (id, name, city)       ->", columns_of(p, "players_evo"))

    # Phase 2 — source gains `country`. Default behaviour = EVOLVE: dlt adds the column.
    p.run(res({"city", "country"}))
    print("phase 2 (+ country, evolve)    ->", columns_of(p, "players_evo"))

    # Phase 3 — source gains `age`, but we FREEZE the contract. dlt rejects the new column.
    try:
        p.run(res({"city", "country", "age"}), schema_contract={"columns": "freeze"})
        print("phase 3 (+ age, freeze)        -> UNEXPECTED: load succeeded")
    except Exception as e:
        print(f"phase 3 (+ age, freeze)        -> rejected by contract ({type(e).__name__})")
        print("   columns unchanged            ->", columns_of(p, "players_evo"))


if __name__ == "__main__":
    main()
