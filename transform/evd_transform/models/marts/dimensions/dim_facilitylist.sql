select

    cast("Code" as text) as facility_key,
    cast("Code" as text) as mfl_code,

    trim("Facility Name") as facility_name,
    trim("Province") as province,
    trim("County") as county,
    trim("Sub County") as subcounty,
    trim("Ward") as ward

from {{ ref('facility_list_dump_20260528') }}

where "Code" is not null