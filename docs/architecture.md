# Architecture

## Overview

```
MinIO (evd bucket)                Postgres                          dbt
─────────────────────            ──────────                        ─────
{source}_raw/{folder}/  ──────►  bronze.{source}_raw   ──────►  silver.*  ──────►  marts.*  ──────►  gold.*
        │                        (schema inferred,                (typed,           (dims/facts,      (reports)
        │ move on success        additive evolution,                deduped)         star schema)
        ▼                        append-only)
_processed_{source}_raw/
      {folder}/
```

`{folder}` is the sending system's actual data sub-folder — not always
literally `records/` (e.g. `adam_cases_raw/cases/`, `cbs_raw/reports/`). It's
declared per system when the asset is registered, or, if not yet known,
discovered at run time (see "MinIO conventions" below).

- **Bronze**: one Postgres table per sending system (`bronze.lims_raw`,
  future `bronze.[system]_raw`), whose columns are *inferred from the JSON
  itself* by a Dagster asset and *evolve additively* — new fields become new
  nullable columns; nothing is ever altered or dropped, and data is
  append-only.
- **Silver**: dbt models that cast bronze's TEXT-only columns to real types,
  dedupe, and drop irrelevant envelope columns — one model per meaningful
  entity per source.
- **Marts** (schema `marts`): dbt dimension and fact models
  (`models/marts/{dimensions,facts}`) that conform silver into a star
  schema — dimensions are shared across facts (date, epiweek, location,
  facility list, lab test), facts union/dedupe silver records per domain
  (cases, screening, lab results).
- **Gold** (schema `gold`): dbt report models that join marts facts and
  dimensions into business-ready, dashboard/API-consumption datasets. Built
  only on `marts` — never directly on `silver` or `bronze`.

## Why bronze isn't hand-modeled

Sending systems (starting with LIMS) send JSON whose shape isn't fully known
upfront and changes over time as fields are added upstream. Hand-authoring a
migration (Prisma, Alembic, ...) for bronze would mean a person updates the
schema every time the source adds a field — brittle and slow. Instead, bronze
inspects the JSON at ingest time and evolves its own schema:

- New JSON keys → new nullable Postgres columns, added via
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- Existing columns are **never** altered or dropped — only appended to.
- Type inference is deliberately simple: **`BOOLEAN` for real booleans,
  `TEXT` for everything else** (numbers, codes, free text). This means a
  column's type, once created, never needs to change — there is no
  `ALTER COLUMN TYPE`, and therefore no scenario where evolving the schema
  can fail or corrupt existing rows. Real typing (numeric, date, structured
  codes) happens downstream in `silver`, where it's a modeling decision made
  by someone who has looked at the actual data, not a runtime guess.
- If a single field's value type still disagrees across records once the
  column already exists (e.g. a flag sent as JSON `true` in one batch and the
  string `"true"` in another), the value is coerced to match the column's
  already-established type at insert time rather than the schema changing to
  match it (see `_coerce` in `assets/bronze/ingest.py`) — bronze must never
  reject a batch over a type surprise.

## Flattening nested JSON

LIMS (and future) records are nested JSON. Bronze flattens them fully:

- Nested objects join with `__`: `patient.address.city` →
  `patient__address__city`.
- Arrays are index-flattened: `results[0].value` → `results__0__value`.
- This is a deliberate tradeoff: it maximizes queryability of bronze directly,
  at the cost of being sensitive to array length changing across batches
  (record N's `results__2__*` columns exist only if some batch actually had a
  3rd result). Accepted because bronze is meant to be explored and cast from
  in `silver`, not queried directly by end users.
- Depth is capped (`max_depth=6` in `flatten_record`) — anything nested deeper
  collapses to a single JSON-text column rather than continuing to explode
  columns, as a safety valve against pathological nesting.
- Column names are sanitized (lowercased, non-`[a-z0-9_]` replaced,
  truncated to Postgres's 63-byte identifier limit with a hash suffix on
  collision) and always have leading/trailing underscores stripped — this is
  what reserves the `_`-prefixed names (`_ingested_at`, `_raw_hash`, ...) for
  envelope columns without any possibility of a data column colliding with
  one.

## MinIO conventions

- Bucket: `evd`.
- Per sending system: `evd/{source}_raw/{folder}/` — incoming JSON,
  optionally gzip-compressed (any extension; compression is sniffed, not
  assumed from the filename). `{folder}` varies per system (`records` for
  LIMS, `cases`/`travellers` for ADAM, `reports` for CBS, `signals` for
  mDharura) and is passed explicitly to `build_bronze_asset`. One sending
  system can own multiple independent raw prefixes/tables (ADAM's `cases`
  and `travellers` are two separate bronze tables).
- If a system's folder name isn't known yet, `build_bronze_asset(source,
  folder=None)` discovers it at run time: it lists the immediate
  sub-folders under `{source}_raw/`, ignores any `_dlt*` bookkeeping
  folders (and any flat files sitting directly under the prefix, like an
  `init` marker, which never show up as sub-folders), and uses the result
  only if exactly one real candidate exists — otherwise it skips the run
  rather than guessing (`krcs_evd_screening_raw`, folder unknown at
  integration time).
- After a file is durably committed to `bronze.{source}_raw` (insert
  transaction succeeds), it is moved — copy then delete, not just copied — to
  `evd/_processed_{source}_raw/{folder}/`.
- Processing is per-file, not per-batch: if a run fails partway through, files
  already committed are already moved; the next run only sees files that
  truly haven't been staged yet. This makes `records/` itself the worklist —
  no separate cursor/watermark table is needed.
- Row-level dedup backstop: `_raw_hash` (MD5 of the canonical nested JSON) is
  UNIQUE with `ON CONFLICT DO NOTHING`, so re-running against an already-moved
  file (or a file that reappears) is a no-op rather than a duplicate insert.

## Reading MinIO: DuckDB, not manual parsing

Bronze reads MinIO via DuckDB's `httpfs` extension
(`read_json_auto('s3://...', format='auto', compression='auto')`) rather than
raw `boto3.get_object` + manual gzip/JSON parsing:

- `format='auto'` handles a single JSON object, a JSON array, or NDJSON in one
  call.
- `compression='auto'` sniffs gzip regardless of file extension.
- DuckDB's Python client returns nested STRUCT/LIST values as native Python
  `dict`/`list` via `.fetchall()` — no pandas dependency needed before
  `flatten_record` runs.

`boto3` (via `MinIOResource`) is still used for what DuckDB can't do: listing
keys under a prefix, and the copy+delete "move to processed" step.

## Envelope columns (every bronze table)

| Column | Purpose |
|---|---|
| `_ingested_at` | When the row landed in Postgres |
| `_source` | Sending system name |
| `_batch_id` | UUID, one per source file processed |
| `_source_file` | The MinIO key the row came from |
| `_raw_hash` | MD5 of the canonical nested JSON — dedup key |
| `_processed` | Reserved for future silver-consumption tracking; not yet flipped by any process |

## dbt layering

- `silver` sources `bronze` directly (`{{ source('bronze', 'lims_raw') }}`);
  `marts` (`dimensions`/`facts`) refs only `silver` models; `gold` refs only
  `marts` models — no layer-skipping anywhere in the chain, same rule as the
  sibling `eth` project.
- Adding a new bronze source means adding one line to
  `models/silver/_sources.yml`, not restructuring anything.
- Silver models are where real typing/casting/dedup logic belongs — never in
  bronze.

## Deferred / open

- **Gold mart SQL** — built: 5 dimension models (`dim_date`, `dim_epiweek`,
  `dim_location`, `dim_facilitylist`, `dim_labtest`), 4 fact models
  (`fct_cases`, `fct_screening`, `fct_lab_result`, `fct_contact`), and 6 gold
  report models (`models/gold/`). One known gap: `fct_contact.sql` currently
  duplicates `fct_cases.sql`'s union/dedup logic rather than implementing
  real contact-tracing joins — a placeholder until contact-tracing source
  data exists, not yet a distinct model.
- **Roles/grants** (`bronze_rw`, `superset_ro`, ...) — not created yet; no
  BI/Superset wiring has been requested for this repo.
- **`_processed` flag** — column exists (matching the sibling `eth`
  convention) but nothing sets it yet; would need an explicit post-hook once
  silver becomes incremental.
- **MinIO event-driven sensors** — currently a daily cron schedule
  (`schedules.py`); a MinIO bucket-notification sensor could replace polling
  later (`sensors.py` has the placeholder).
