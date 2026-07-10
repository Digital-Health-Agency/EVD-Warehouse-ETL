# EVD Analytics Guide for API Consumers

This guide describes the analytics data model exposed to API consumers,
following the Medallion architecture (Bronze → Silver → Marts → Gold). It
reflects the current state of the repo — see
[`architecture.md`](architecture.md) for the underlying ETL design.

## 1. Architecture Overview

- **Bronze**: raw ingestion from source systems, no business
  transformations.
- **Silver**: cleaned, standardized, deduplicated datasets with harmonized
  fields.
- **Marts**: dimensional warehouse — conformed dimensions and fact tables.
- **Gold**: business-ready reporting models, optimized for dashboards and
  APIs.

## 2. Bronze Layer

Purpose: preserve source fidelity. Consumers: data engineers only.

Eight sending systems land in bronze today:

| Table | Sending system |
|---|---|
| `bronze.adam_cases_raw` | ADAM (cases) |
| `bronze.adam_travellers_raw` | ADAM (travellers) |
| `bronze.uhai_raw` | UHAI (traveler/case screenings) |
| `bronze.lims_raw` | LIMS (lab results) |
| `bronze.cbs_raw` | CBS (reports) |
| `bronze.mdharura_raw` | mDharura (signals) |
| `bronze.echis_raw` | eCHIS (signals) |
| `bronze.krcs_evd_screening_raw` | KRCS EVD screening |

## 3. Silver Layer

Purpose: cleansed operational datasets. Only 4 of the 8 bronze sources have
a silver model today — `cbs`, `mdharura`, `echis`, and `krcs_evd_screening`
are ingested into bronze but have no silver transform yet.

| Model | Source |
|---|---|
| `slv_adam_cases` | `bronze.adam_cases_raw` |
| `slv_adam_travellers` | `bronze.adam_travellers_raw` |
| `slv_uhai_cases` | `bronze.uhai_raw` |
| `slv_lims_results` | `bronze.lims_raw` |

Business rules applied: standardized dates, normalized Yes/No values,
trimmed text, data type enforcement, duplicate removal.

(`silver_lims__raw.sql` also exists as a passthrough `select *` over
`bronze.lims_raw` — a legacy placeholder from before `slv_lims_results` was
written; not a second source of truth.)

## 4. Marts Layer

Dimensions:

- `dim_date` — calendar dimension (2020-2035), `date_key` surrogate key.
- `dim_epiweek` — epi-week dimension derived from `dim_date`.
- `dim_location` — deduped county/subcounty/ward/point-of-entry
  combinations across ADAM and UHAI, `location_key` surrogate key.
- `dim_facilitylist` — facility master list from the MFL seed,
  `facility_key` surrogate key, `mfl_code`/`facility_name` for joins.
- `dim_labtest` — deduped lab test/specimen/LOINC combinations from
  `slv_lims_results`.

Facts:

- `fct_cases` — unions `slv_adam_cases` + `slv_uhai_cases`, deduped by
  `source_system`/`source_record_id`, joined to date/location/facility
  dims; carries `case_count` plus suspected/probable/confirmed/tested/
  died/recovered flags and counts.
- `fct_screening` — unions ADAM traveller + UHAI screening records with the
  same dimension joins and count pattern.
- `fct_lab_result` — deduped LIMS results joined to facility and
  collection/result date dims, with a categorized result
  (Positive/Negative/Inconclusive/Unknown/Other).
- `fct_contact` (future) — **known limitation**: the file currently
  duplicates `fct_cases`'s union/dedup logic rather than implementing real
  contact-tracing joins. Not yet reliable for contact-tracing analytics.

Dimensions are for filtering and drill-downs; facts provide measures.

## 5. Gold Reporting Layer

Business-ready datasets that actually exist today:

- `report_case_summary` — weekly case summary by location/facility/source
  with suspected/probable/confirmation/testing/sample-collection/recovery/
  fatality rates.
- `report_case_trend` — weekly case aggregation plus cumulative totals,
  4-week moving average, week-over-week change and % change.
- `report_case_distribution` — combined daily/epiweek case, screening, and
  lab activity by geography/facility with cross-domain rates.
- `report_screening_summary` — daily/weekly screening summary by
  location/facility/classification.
- `report_laboratory_summary` — weekly/monthly lab result summary by
  facility/test/specimen with positivity/negative/inconclusive/unknown/
  completion rates.
- `report_geographic_summary` — geography-oriented rollup combining cases,
  screening, and lab activity.

Not yet built (do not rely on these as API endpoints): `report_case_
demographics`, `report_case_outcomes`, `report_contact_summary`.

## 6. API Consumption Guidelines

- Use Gold models for reporting.
- Use Marts facts only for advanced analytics.
- Avoid querying Bronze directly.
- Filter using date, epidemiological week, county, subcounty, facility, and
  `source_system` where available.

## 7. Common Dimensions

- **Time**: `full_date` (`dim_date`), epi_year/epi_week (`dim_epiweek`).
- **Geography**: county, subcounty, ward, point_of_entry (`dim_location`,
  `location_key`).
- **Facility**: `facility_key`, `mfl_code`, `facility_name`
  (`dim_facilitylist`).
- **Source**: `source_system` values are `adam`, `uhai`, `lims`, `cbs`,
  `mdharura`, `echis`, `krcs_evd_screening` — though only `adam`, `uhai`,
  and `lims` currently flow through to marts/gold (see §3).

## 8. Typical Analytics

Executive KPIs, weekly case trends, screening performance, laboratory
positivity, facility performance, geographic summaries, operational
monitoring.

## 9. Best Practices

- Always query Gold for dashboards.
- Join on surrogate keys (`case_key`, `location_key`, `facility_key`,
  `date_key`).
- Use `dim_epiweek` for weekly reporting.
- Do not expose internal batch IDs (`batch_id`, `source_file`) through
  public APIs.
- Version API responses as models evolve.

## 10. Transform Guide (Engineering)

For engineers extending the dbt build (`transform/evd_transform/`), not
just consuming its output.

### Project layout

```
transform/evd_transform/
├── models/
│   ├── silver/                  # schema: silver
│   │   └── _sources.yml         # declares bronze.* as dbt sources
│   ├── marts/                   # schema: marts
│   │   ├── dimensions/
│   │   └── facts/
│   └── gold/                    # schema: gold
└── dbt_project.yml              # +materialized: table per layer
```

`transform/profiles.yml` lives in-repo, env-var driven — not `~/.dbt/`.

### Enforced ref chain

- `silver` models use `{{ source('bronze', '<table>') }}` — never a raw
  table name.
- `marts` models use `{{ ref(...) }}` only on `silver` models.
- `gold` models use `{{ ref(...) }}` only on `marts` models (dimensions or
  facts) — never on `silver` or `bronze` directly.

This is enforced by convention, not a dbt constraint — verified today via
`grep ref(` over `models/gold/`: every gold model's `ref()` calls resolve to
`fct_*`/`dim_*` marts models, none to a `slv_*` silver model.

### Adding a new bronze source through to gold

1. Bronze wiring: see `CLAUDE.md`'s "Adding a new sending system" steps
   (`build_bronze_asset`, `Definitions(assets=[...])` registration, dbt
   source in `_sources.yml`).
2. One silver model per meaningful entity
   (`silver_<source>__<entity>.sql`), typing/casting/deduping the bronze
   TEXT columns — use `make explore SOURCE=<name>` first to see the
   inferred bronze shape before writing the cast list.
3. Wire the new silver model into the relevant marts fact (union +
   dedupe pattern, see `fct_cases.sql`) and/or dimension.
4. Add or extend a gold report model if a new business view is needed.

### Key commands

- `dbt run` / `dbt run --select silver` / `dbt run --select marts+` —
  build a layer and everything downstream.
- `dbt test` — runs schema tests declared in model `.yml` files.
- `make explore SOURCE=<name>` — dry-run: samples MinIO, prints the
  inferred bronze schema without writing anything.
- In production, `dbt run` isn't invoked manually — the Dagster
  `evd_dbt_assets` (`assets/transform/`) wraps the dbt project and runs it
  as part of the orchestrated pipeline.

### Known gaps for maintainers

- `silver_lims__raw.sql` is a legacy `select *` placeholder superseded by
  `slv_lims_results.sql` — candidate for removal.
- Schema/data tests are sparse: only `silver_lims__raw.yml` has any
  (`unique`/`not_null` on `_raw_hash`) — most silver/marts/gold models have
  no `.yml` tests yet.
- `cbs`, `mdharura`, `echis`, `krcs_evd_screening` have bronze tables and
  declared dbt sources but no silver model — first step for onboarding them
  into marts/gold.
- `fct_contact.sql` needs real contact-tracing source data and join logic;
  currently a copy of `fct_cases.sql`.
