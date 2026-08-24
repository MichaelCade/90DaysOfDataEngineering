"""Workshop / Day 23 — dlt's declarative `rest_api` source for #90DaysOfDataEngineering.

Day 22 called an API by hand with `requests.get`. Real APIs paginate — the data spans many
pages and you have to follow "next" links. Writing that loop yourself (and the retries, and
the rate-limit handling) for every endpoint doesn't scale.

dlt's **`rest_api` source** turns an API into *configuration*: base URL, which resources
(endpoints) to pull, how they paginate, and where the rows live in the response. dlt does the
paging, retries, and schema/loading for you.

We hit the free, no-auth **PokeAPI**, whose list endpoints paginate with a `next` URL in the
body:  {"count": 1302, "next": ".../pokemon?offset=50&limit=50", "results": [ ... ]}

Run it (locally, against the cluster's Postgres LoadBalancer):

    export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
    python rest_api_pipeline.py

Data lands in the `dlt_workshop` schema of `appdb` (tables: pokemon, berry).
"""
import dlt
from dlt.sources.rest_api import rest_api_source


def pokeapi_source():
    """One `rest_api` source describing TWO endpoints. Each becomes one destination table.

    Note what we DON'T write: no request loop, no `while next_url`, no page counter — just a
    declaration that these endpoints use a body `next` link (`json_link`) and that the rows
    live under `results` (`data_selector`).
    """
    return rest_api_source(
        {
            "client": {"base_url": "https://pokeapi.co/api/v2/"},
            # Defaults applied to every resource unless overridden.
            "resource_defaults": {
                "write_disposition": "replace",
                "endpoint": {
                    "params": {"limit": 50},                         # page size
                    "data_selector": "results",                      # rows live here
                    "paginator": {                                   # how to page
                        "type": "json_link",                         # follow a URL in the body
                        "next_url_path": "next",                     # ...found at $.next
                    },
                },
            },
            "resources": [
                {"name": "pokemon", "endpoint": {"path": "pokemon"}},
                {"name": "berry", "endpoint": {"path": "berry"}},
            ],
        }
    )


def main() -> None:
    source = pokeapi_source()

    # Cap pages so the demo is fast: 3 pages x 50 = 150 rows per resource.
    # `.add_limit(n)` stops after n pages — remove it to pull every page (all ~1300 pokemon).
    source.resources["pokemon"].add_limit(3)
    source.resources["berry"].add_limit(3)

    pipeline = dlt.pipeline(
        pipeline_name="pokeapi",
        destination="postgres",
        dataset_name="dlt_workshop",   # same schema as Day 22 — tables accumulate
    )
    load_info = pipeline.run(source)
    print(load_info)

    # Show what landed, straight from the pipeline's dataset (no separate DB client needed).
    ds = pipeline.dataset()
    for table in ("pokemon", "berry"):
        n = len(ds[table].fetchall())
        print(f"  {table}: {n} rows")


if __name__ == "__main__":
    main()
