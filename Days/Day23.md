# Day 23 — dlt: Ingesting from REST APIs

> Workshop: Data Ingestion

Day 22 called an API the manual way — `requests.get(...)`, one call, one page. Real APIs
don't hand you everything at once: they **paginate**. And every API paginates *differently*.
Writing that loop by hand, per endpoint, with retries and rate-limit handling, is exactly the
boilerplate dlt exists to remove.

## The pagination problem

Ask an API for a list and you get back one page plus a way to get the next one. The "way"
varies by API:

- **Body `next` link** — the response carries the next page's URL: `{"next": ".../?offset=50", "results": [...]}` (PokeAPI, DRF-style APIs).
- **`Link` header** — the next URL is in an HTTP `Link: <...>; rel="next"` header (GitHub).
- **Offset / limit** — you increment `?offset=` yourself until you get an empty page.
- **Page number** — `?page=1`, `?page=2`, … until empty.
- **Cursor** — an opaque token returned each page that you pass to the next request.

Hand-coding the right one for every source is where ingestion rots. dlt makes it a one-line
declaration.

## The `rest_api` source: an API as configuration

dlt's **`rest_api` source** turns "pull this API" into a config object — no request loop:

```python
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {"base_url": "https://pokeapi.co/api/v2/"},
    "resource_defaults": {
        "write_disposition": "replace",
        "endpoint": {
            "params": {"limit": 50},                    # page size
            "data_selector": "results",                 # where the rows live in the response
            "paginator": {"type": "json_link",          # follow a URL in the body...
                          "next_url_path": "next"},      # ...found at $.next
        },
    },
    "resources": [
        {"name": "pokemon", "endpoint": {"path": "pokemon"}},
        {"name": "berry",   "endpoint": {"path": "berry"}},
    ],
})
```

Three ideas do all the work:

- **`paginator`** — *how* to page. `json_link`, `header_link`, `offset`, `page_number`,
  `cursor`, `single_page`. dlt follows the pages until there are no more. (If you omit it,
  dlt tries to **auto-detect** — handy, but being explicit is clearer and safer.)
- **`data_selector`** — *where* the rows are in the JSON (here, under `results`).
- **`resource_defaults`** — settings shared by every resource, so you write them once. A
  **source** is a collection of **resources**; each resource → one destination table.

The full script: [`examples/workshop-dlt/rest_api_pipeline.py`](../examples/workshop-dlt/rest_api_pipeline.py).

## Running it

```bash
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://app:<pw>@192.168.169.191:5432/appdb"
python rest_api_pipeline.py
```

To keep the demo quick, the script caps pages with `.add_limit(3)` (3 pages × 50 = 150 rows);
drop it to pull every page. Result — **two endpoints, two tables, zero paging code**:

```
pokemon: 150 rows     <- hit our 3-page cap
berry:    68 rows     <- pagination ended on its own (last page reached before the cap)
```

That contrast is the lesson in miniature: dlt followed the `next` link across pages for both,
stopped `pokemon` at our limit, and stopped `berry` when the API said there were no more pages.
In Postgres (`dlt_workshop` schema) you get `pokemon` and `berry` tables — inferred columns,
plus the same `_dlt_id` / `_dlt_load_id` lineage and `_dlt_loads` tracking as Day 22.

## Real APIs need auth — and usually the `Link` header

Most useful APIs require a token and paginate via the `Link` header. dlt handles both
declaratively — and keeps the secret out of your code (it reads from env/`secrets.toml`,
e.g. `SOURCES__REST_API__GITHUB_TOKEN`):

```python
rest_api_source({
    "client": {
        "base_url": "https://api.github.com/",
        "auth": {"type": "bearer", "token": dlt.secrets["github_token"]},
        "paginator": {"type": "header_link"},          # GitHub uses the Link header
    },
    "resources": [
        {"name": "issues",
         "endpoint": {"path": "repos/dlt-hub/dlt/issues", "params": {"per_page": 100}}},
    ],
})
```

Same shape as before — only the paginator and auth changed. That's the payoff of declaring the
API instead of coding it: swapping sources is editing config, not rewriting loops.

## Why this matters

A data platform pulls from *dozens* of APIs. If each one means a bespoke pagination loop,
retry policy, and schema, ingestion becomes the most fragile part of the stack. `rest_api`
collapses "integrate an API" down to declaring its base URL, endpoints, pagination, and auth —
and dlt still gives you schema inference, typing, lineage, and load tracking on top.

## Summary

Real APIs paginate, and every one does it differently. dlt's **`rest_api`** source makes the
API a configuration: a **paginator** (how to page), a **`data_selector`** (where the rows are),
and a list of **resources** (each → a table), with **auth** and shared **defaults** declared
once. We paged PokeAPI into Postgres with no request loop. Next: **Day 24 — incremental
loading**, where dlt tracks *what it already loaded* so re-runs pull only new data.
