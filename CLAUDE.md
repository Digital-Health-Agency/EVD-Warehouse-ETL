# EVD Warehouse ETL

MinIO → Postgres (`bronze` → `silver` → `marts` → `gold`). Full design:
[`docs/architecture.md`](docs/architecture.md).

## Scope

- **In scope**: MinIO → Postgres → dbt (silver/gold).
- **No Data Vault modeling.** `silver`/`gold` are plain medallion transforms —
  typing, dedup, joins — not hubs/links/satellites.
- **Bronze's table shape is inferred, not hand-authored.** There is no
  migration tool/ORM for bronze — only `infra/postgres/init.sql` creating the
  three schemas. Bronze tables and columns are created by the Dagster asset
  itself (`orchestration/evd_orchestration/assets/bronze/ingest.py`).

## Repository layout

```
EVD-Warehouse-ETL/
├── infra/postgres/init.sql          # CREATE SCHEMA bronze/silver/gold
├── orchestration/evd_orchestration/
│   ├── resources/                   # minio.py, duckdb_io.py, postgres.py
│   ├── assets/
│   │   ├── bronze/
│   │   │   ├── schema_inference.py  # pure functions — flatten/sanitize/infer/hash
│   │   │   ├── ingest.py            # build_bronze_asset(source, folder) factory
│   │   │   └── lims.py              # bronze_lims_raw = build_bronze_asset("lims")
│   │   └── transform/                # @dbt_assets wrapping transform/evd_transform
│   ├── jobs.py, schedules.py, sensors.py
│   └── tools/migrate.py             # runs infra/postgres/init.sql
├── transform/evd_transform/         # dbt project: models/{silver,marts,gold}
├── scripts/infer_schema.py          # dry-run: sample MinIO, print inferred schema
└── tests/                           # pytest, pure schema-inference logic only
```

## The additive-schema rule

This is the core contract of the whole repo — read this before touching
`assets/bronze/`:

- New JSON field → new nullable column, added via
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- **Never** `ALTER COLUMN TYPE`, never drop a column, never rewrite existing
  rows. Bronze is append-only.
- Type inference is intentionally only two-valued: `BOOLEAN` for real
  booleans, `TEXT` for everything else (`infer_pg_type` in
  `schema_inference.py`). This is what makes "additive only" actually
  achievable — there is no type-conflict case that requires altering a
  column, because non-boolean values always fit TEXT.
- If a value doesn't match its column's already-established type (rare: a
  field flips from boolean to non-boolean across batches or vice versa),
  `_coerce` in `ingest.py` converts the value to fit — it never fails the
  insert and it never changes the schema.
- Real typing (numeric, date, enums) happens in `silver` dbt models, once a
  human has looked at actual bronze data — never in bronze itself.

## Flattening contract

- `flatten_record` (in `schema_inference.py`) is fully recursive: nested
  objects join with `__`, arrays are index-flattened
  (`results__0__value`), depth is capped at 6 (deeper nesting collapses to a
  JSON-text leaf).
- `sanitize_column_name` always strips leading/trailing underscores — this is
  what reserves `_`-prefixed names for envelope columns
  (`_ingested_at`, `_source`, `_batch_id`, `_source_file`, `_raw_hash`,
  `_processed`). Don't add a data column that could start with `_`.
- `canonical_hash` hashes the **original nested record**, not the flattened
  one, so `_raw_hash` dedup stays stable even if flattening rules change.

## Reading MinIO

Always via DuckDB's `httpfs` (`read_source_records` in `ingest.py`), never
raw `boto3.get_object` + manual gzip/JSON parsing — `format='auto'` handles
single-object/array/NDJSON uniformly and `compression='auto'` sniffs gzip
regardless of extension. `boto3` (`MinIOResource`) is only for listing keys
and the copy+delete move-to-processed step, which DuckDB can't do.

## Adding a new sending system

`build_bronze_asset(source, folder)` reads `{source}_raw/{folder}/` in MinIO
and writes `bronze.{source}_raw`. `folder` is **not** always `"records"` —
each sending system's actual data sub-folder is whatever the source drops
files into (e.g. `adam_cases_raw/cases/`, `cbs_raw/reports/`); it's declared
explicitly per system, not guessed. One sending system can own more than one
raw prefix/table (ADAM has `adam_cases_raw` and `adam_travellers_raw` — two
independent bronze tables, no shared code beyond the naming).

If the folder name isn't known yet (or the source hasn't started sending),
pass `folder=None`: the asset discovers the sub-folder at run time, ignoring
any `_dlt*` bookkeeping folders, and skips the run if none or more than one
non-`_dlt*` candidate exists rather than guessing (see
`bronze_krcs_evd_screening_raw`).

1. `assets/bronze/<system>.py`:
   `bronze_<system>_raw = build_bronze_asset("<system>", folder="<entity>")`.
2. Register in `assets/bronze/__init__.py`, `assets/__init__.py`,
   `jobs.py`'s `ingest_job` selection, **and** `evd_orchestration/__init__.py`
   — both the import list and the `Definitions(assets=[...])` list. This last
   one is the step that's easy to miss: the `assets/bronze/__init__.py` /
   `assets/__init__.py` re-exports and the `jobs.py` selection string do
   *not* register the asset with Dagster — only appearing in
   `Definitions(assets=[...])` does. Skipping it produces a Dagster load
   error even though every other file looks correctly wired up.
3. Add `bronze.<system>_raw` as a source in
   `transform/evd_transform/models/silver/_sources.yml`.

No changes to `schema_inference.py` or `ingest.py` — the factory is the whole
point.

## dbt layering

- `silver` sources `bronze` directly (`source()`); `marts`
  (`models/marts/{dimensions,facts}`) refs `silver`; `gold` refs `marts`
  only — no layer-skipping anywhere in the chain.
- One dbt model per meaningful entity per source
  (`silver_<source>__<entity>.sql`), not one giant passthrough — the current
  `silver_lims__raw.sql` is an explicit placeholder to be replaced once real
  LIMS columns are known (`make explore SOURCE=lims`).
- `profiles.yml` lives in-repo at `transform/profiles.yml`, env-var driven —
  not `~/.dbt/`.

## Conventions

- Python: ruff, line length 100.
- SQL: lowercase keywords, one column per line in `SELECT`.
- Tests (`tests/`) exercise the pure `schema_inference.py` functions only —
  no MinIO/Postgres I/O in unit tests. `tests/conftest.py` sets dummy env vars
  so importing `evd_orchestration` (which builds `Definitions()` at import
  time) doesn't require real credentials.

## Things to avoid

- ❌ Any business/domain logic in `assets/bronze/` or bronze tables — bronze
  mirrors source shape, nothing more.
- ❌ `ALTER COLUMN TYPE` on a bronze table, ever.
- ❌ Updating/deleting bronze rows in place — corrections are new batches.
- ❌ dbt models in `gold` referencing `silver` or `bronze` directly (must go
  through `marts`), `marts` referencing `bronze` directly (must go through
  `silver`), or `silver` referencing anything but `bronze` sources.
- ❌ Hardcoding MinIO prefixes/table names outside `build_bronze_asset` — the
  `{source}_raw/{folder}/` ↔ `bronze.{source}_raw` ↔
  `_processed_{source}_raw/{folder}/` mapping should only ever be derived from
  the `source`/`folder` values passed into the factory.
