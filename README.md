# EVD Warehouse ETL

Data warehouse ETL for EVD: MinIO → Postgres (`bronze` → `silver` → `gold`).

## Scope

- **In scope**: MinIO → Postgres → dbt (silver/gold marts).
- **Bronze is schema-inferred, not hand-modeled.** JSON records from each
  sending system are flattened and staged into `bronze.{source}_raw` tables
  whose columns are discovered from the data itself and evolve **additively**
  (new fields → new nullable columns; existing columns/data are never altered).
- **No Data Vault modeling.** `silver`/`gold` are plain medallion-style dbt
  transforms — typing, dedup, joins — not hubs/links/satellites.

See [`docs/architecture.md`](docs/architecture.md) for the full design and
[`CLAUDE.md`](CLAUDE.md) for the conventions this repo follows.

## Stack

| Layer         | Tool         | Notes                                                      |
| ------------- | ------------ | ---------------------------------------------------------- |
| Raw lake      | MinIO        | S3-compatible, bucket `evd`, one prefix per sending system |
| Lake reader   | DuckDB       | `httpfs`, reads JSON (incl. gzip) directly from MinIO      |
| Orchestration | Dagster      | Bronze ingestion assets + `@dbt_assets` wrapping dbt       |
| Warehouse     | Postgres     | Schemas: `bronze`, `silver`, `gold`                        |
| Transform     | dbt-postgres | Silver (typed/deduped) → gold (marts)                      |
| Python deps   | uv           | Single `pyproject.toml` at root                            |

## Repository layout

```
EVD-Warehouse-ETL/
├── infra/postgres/          # init.sql — CREATE SCHEMA bronze/silver/gold
├── orchestration/
│   └── evd_orchestration/
│       ├── resources/       # minio, duckdb_io, postgres
│       ├── assets/
│       │   ├── bronze/      # schema inference + generic ingest asset factory
│       │   └── transform/   # @dbt_assets wrapping transform/evd_transform
│       ├── jobs.py, schedules.py, sensors.py
│       └── tools/migrate.py # runs infra/postgres/init.sql
├── transform/
│   └── evd_transform/       # dbt project: models/{silver,gold}
├── scripts/infer_schema.py  # dry-run: sample MinIO, print inferred schema
├── tests/                   # pytest — pure schema-inference logic
└── docker-compose.yml, Makefile, pyproject.toml
```

## Local development

Prereqs: Docker, [uv](https://docs.astral.sh/uv/), GNU make.

```bash
cp .env.example .env       # fill in MinIO + Postgres credentials
make migrate                # create bronze/silver/gold schemas
make explore SOURCE=lims    # sample real MinIO files, print inferred schema (no writes)
make ingest                 # stage bronze.lims_raw from MinIO
make dbt-deps && make dbt   # build silver + gold
```

Or run the Dagster UI locally:

```bash
make dagster-dev            # http://localhost:3000
```

Run tests:

```bash
make test
```

## Adding a new sending system

Bronze ingestion is a factory, not per-source boilerplate. To add `[future]`:

1. `orchestration/evd_orchestration/assets/bronze/<future>.py`:
   ```python
   from .ingest import build_bronze_asset
   bronze_<future>_raw = build_bronze_asset("<future>")
   ```
2. Register it in `assets/bronze/__init__.py`, `assets/__init__.py`, and
   `jobs.py`'s `ingest_job` selection.
3. Add `bronze.<future>_raw` as a dbt source in
   `transform/evd_transform/models/silver/_sources.yml`.

No changes to the ingestion/schema-inference logic itself are needed — MinIO
layout (`evd/<future>_raw/records/` → `evd/_processed_<future>_raw/records/`)
and Postgres table (`bronze.<future>_raw`) follow the same convention as
`lims`.
