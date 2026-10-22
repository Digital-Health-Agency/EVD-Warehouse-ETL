# Fixes for building the warehouse in a fresh environment

Two problems stop a brand-new environment from installing or building this
project, even though production keeps working. Both were found while
setting up the full pipeline locally on 22 Oct 2026 (MinIO → bronze → dbt)
against freshly fetched source data. This branch fixes both. A third note
covers the same class of problem in the NPHIIS models on `feat/nphiis`.

## 1. `uv sync` hangs while installing dbt

**Symptom.** `uv sync` (or the first `uv run`) never finishes. It is stuck
building `dbt-core-experimental-parser`.

**Cause.** `uv.lock` is gitignored, so every fresh install resolves the
newest versions allowed by `pyproject.toml`. `dbt-postgres>=1.8,<2.0`
accepts `dbt-core` 1.12, which depends on `dbt-core-experimental-parser`.
That package is a small source distribution whose build step downloads a
prebuilt Rust binary from GitHub at install time. The download is slow or
blocked on some networks and the install hangs. The parser is an alpha
component of dbt v2; the team runs dbt 1.11.

**Fix.** `pyproject.toml` adds `dbt-core<1.12` to
`[tool.uv] constraint-dependencies`, next to the existing `greenlet` pin.
Fresh installs resolve dbt-core 1.11.x, which needs no download step.

**When to remove it.** When the team deliberately moves to dbt 1.12+ (or
v2), or starts committing `uv.lock`.

## 2. `silver.slv_mdharura` fails: column "community_unit" does not exist

**Symptom.** `dbt build` fails on `slv_mdharura`, and everything downstream
(`dim_location`, `fct_community_signal`, `report_community_signals`) is
skipped:

```
Database Error in model slv_mdharura
  column "community_unit" does not exist
```

**Cause.** On 8 Jul the m-Dharura extractor
(`EVD-Surveillance-scripts-v1`, commit `eaa08ce`) renamed the field
`community_unit` to `unit_name` / `unit_type`. Bronze is schema-inferred
and additive, so a bronze table only has a `community_unit` column if it
ingested records from **before** the rename. Production does; any
environment that loads m-Dharura data from after July does not, and the
model's hard-coded `community_unit` reference fails.

**Fix.** `slv_mdharura.sql` asks the database which columns bronze
actually has (`adapter.get_columns_in_relation`) and selects
`community_unit` only when it exists; otherwise it emits
`null::text as community_unit`.

- Production (column present): compiled SQL is identical to before, so
  results do not change.
- Fresh environments (column absent): the model builds and
  `community_unit` is null. The same value is available in `unit_name`
  for every record since the rename.
- Downstream models keep the `community_unit` column, so nothing else
  changes.

## 3. The same problem in the NPHIIS silver models (`feat/nphiis`, not fixed here)

Bronze only creates a column once some record has a non-null value for
that field. Any silver model that lists bronze columns explicitly fails on
a table that has never seen a value for one of them. On a fresh load of
the current NPHIIS data (11 cases, 128 contacts) these were missing:

| Model | Columns missing from bronze |
|---|---|
| `slv_nphiis_cases` | `preliminary_result`, `contact_id`, `last_followup` |
| `slv_nphiis_contacts` | `epid`, `contact_id`, `county`, `sub_county`, `ward`, `facility`, `occupation`, `birth_date`, `pregnant`, `travelled`, `vaccinated` |

Locally they were added as empty `text` columns (exactly what bronze
ingestion does when a field first appears), so no model code was changed
on `feat/nphiis`. Production will meet the same failure whenever one of
these fields has never had a value.

A small macro would make every silver model safe:

```sql
{# macros/column_or_null.sql
   Select a bronze column if the table has it, else a typed null with the
   same name. Bronze only creates a column after it has seen a value. #}
{% macro column_or_null(relation, column, type='text') %}
  {%- set names = adapter.get_columns_in_relation(relation)
      | map(attribute='name') | map('lower') | list -%}
  {%- if column | lower in names -%}{{ column }}
  {%- else -%}null::{{ type }} as {{ column }}{%- endif -%}
{% endmacro %}
```

Usage inside a silver model:

```sql
{% set src = source('bronze', 'nphiis_contacts_raw') %}
select
    {{ column_or_null(src, 'epid') }},
    {{ column_or_null(src, 'county') }},
    ...
from {{ src }}
```

## How this was verified

- Fresh `uv sync` on Python 3.12 completes with the pin (dbt-core 1.11.16,
  dagster 1.13.26).
- `dbt build` on `feat/nphiis` plus these two fixes: 30 of 30 models pass
  on freshly fetched data from LIMS, UHAI, ADaM (uploaded files),
  m-Dharura, CBS, KRCS, Taifa Care and NPHIIS.
- `slv_mdharura` builds every m-Dharura signal (833 on first load, 840
  after the 22 Oct refresh); `report_community_signals` matches it.
- On this branch (based on `main`), `dbt parse` succeeds and
  `slv_mdharura` builds against the same bronze data.
