# Sex and age group on screening records

`gold.report_screening` carries the traveller's or patient's sex and a
reporting age group, using the same column names and age bands as the
contact report (`report_contact_registration`), so dashboards can show and
filter screenings by sex and age the same way they do contacts.

## New columns

| Layer | Column | Meaning |
|---|---|---|
| `silver.slv_uhai_cases` | `age_years` | Age in whole years as UHAI reports it (UHAI has no date of birth). Non-numeric values are null. |
| `marts.fct_screening` | `source_person_sex` | Sex, conformed by the `normalise_sex` macro: trimmed and upper-cased, `M` → `MALE`, `F` → `FEMALE`, blank → null. Other values (ADaM `INTERSEX`, UHAI `OTHER`) are kept as reported. |
| `marts.fct_screening` | `source_person_date_of_birth` | Date of birth (ADaM, Taifa Care). Null for UHAI. |
| `marts.fct_screening` | `source_person_age_years` | Reported age (UHAI). Null for ADaM and Taifa Care. |
| `gold.report_screening` | `age_at_screening` | Whole years at the screening date: from the date of birth where there is one, else the reported age. Values outside 0–110 are null. |
| `gold.report_screening` | `reporting_age_group` | `<1`, `1-4`, `5-9`, `10-14`, `15-19`, `20-24`, `25-34`, `35-44`, `45-54`, `55-64`, `65+`, or `UNKNOWN` (same bands as `report_contact_registration`). |

The three screening branches in `fct_screening` are combined with
`union all`, which matches columns **by position**, so the new person
columns sit at the same place in every branch (after the coordinates).

## Where the values come from

| Source | Sex | Age basis |
|---|---|---|
| ADaM travellers | `sex` | `date_of_birth` |
| UHAI | `sex` | `age` (bronze), read only if the bronze column exists |
| Taifa Care / KenyaEMR | `sex` | `date_of_birth` |

## Coverage on the 22 Oct 2026 data

| Source | Records | Sex filled | Age group known |
|---|---|---|---|
| UHAI | 268,912 | 99.7% | 98.7% |
| ADaM | 147,756 | 70.6% | 68.7% |
| Taifa Care | 206 | 100% | 100% |

Overall: MALE 219,586, FEMALE 152,007, OTHER 602, INTERSEX 390, blank
44,289 (mostly ADaM). Age group UNKNOWN: 49,737, mostly ADaM records with no
date of birth.

## Data-quality note for UHAI

Of the UHAI age values, 274,068 are plausible (0–105). The rest are banded
`UNKNOWN`:

- 1,958 between 1935 and 2026, which look like **birth years** entered in
  the age field.
- 129 that are simply invalid (for example 128, 2,900 and 30,581).

The birth-year values are not converted to ages, to avoid guessing. If UHAI
confirms the pattern, they could be turned into ages as
`screening year − value`.

## Verification

- `dbt build --select slv_uhai_cases+ fct_screening+` passes, and the
  screening row count is unchanged (416,874), so no record was added or
  dropped.
- The dashboard backend's screening linelist reads both columns
  (EVD-Dashboard-Backend branch `feature/screening-sex-age-group`). Deploy
  this ETL change **before** that backend branch: until these columns exist
  in production, a backend that selects them would fail on screening
  linelists.
