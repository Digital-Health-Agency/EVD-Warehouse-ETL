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

# Contact, travel, clinical and exposure fields (UHAI)

On 27 Oct 2026 UHAI's surveillance API (`/api/v1/surveillance/ebola-cases`)
added 20 fields. With the scripts-repo loader change that keeps every API
field (EVD-Surveillance-scripts-v1, branch `feat/uhai-keep-all-api-fields`),
they reach `bronze.uhai_raw` and are carried through to
`gold.report_screening`. ADaM and Taifa Care rows have them as null.

| `gold.report_screening` column | From UHAI field | Coverage (UHAI) |
|---|---|---|
| `person_phone_number` | `phone_number`: the WhatsApp number the screening was submitted from | 100% |
| `dependent_phone_number` | `dependent_phone_number`: number given for a dependant | 3.8% (dependants only) |
| `is_dependent` | `is_dependent` (Yes/No) | 100% |
| `country_of_departure` | `country_of_departure` | 99.7% |
| `entry_country` | `entry_country` | 84.9% |
| `arrival_date` | `arrival_date` (date) | 84.9% (from September) |
| `mode_of_transport` | `mode_of_transport` (upper-cased, e.g. AIR) | 99.7% |
| `transport_number` | `transport_number` (flight / vehicle) | 99.9% |
| `seat_number` | `seat_number` | 76.2% |
| `temperature` | `temperature` (numeric °C) | 50.8% |
| `symptoms` | `symptoms` | 1.2% (only when reported) |
| `risk_factors` | `risk_factors` | 7.3% |
| `visited_affected_countries`, `contact_with_confirmed_case`, `healthcare_lab_work`, `funeral_body_prep`, `contact_animals`, `bushmeat_handling`, `forest_cave_exposure` | same names (Yes/No) | 100% each |
| `screening_channel` | `source` (bronze column `source_field`) | 100% (agent, app_self, self, whatsapp) |

Coverage measured on 299,630 UHAI screenings, 3 Jul – 28 Oct 2026.

**Personal data.** `person_phone_number` and `dependent_phone_number`
identify people. The dashboard backend classifies both as personal data:
only users with the surveillance role can see or search them, and other
callers' queries never name the columns.

**Why `column_or_null`.** A bronze column only exists once some record has
had a value for it. `slv_uhai_cases` reads every new field through the
`column_or_null` macro, so the model still builds against a bronze table
that has not received a field yet (a fresh environment, or before the first
load after UHAI's change). The catch: a misspelt column name also silently
becomes null, so check coverage after changes. This is how `source` landing
as `source_field` was found.

**Getting history.** The API returns the new fields for past screenings too,
so one re-pull of UHAI from 3 Jul fills them for every record. Pull in
weekly windows: a single 3 Jul – today run timed out on the larger payload.
