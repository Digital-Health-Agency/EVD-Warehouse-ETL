

with cases as (

    select *
    from {{ ref('fct_cases') }}

),

case_dates as (

    select *
    from {{ ref('dim_date') }}

),

created_dates as (

    select *
    from {{ ref('dim_date') }}

),

locations as (

    select *
    from {{ ref('dim_location') }}

),

facilities as (

    select *
    from {{ ref('dim_facilitylist') }}

),

final as (

    select
        /* Case identifiers */
        c.case_key,
        c.source_system,
        c.source_record_id,
        c.system_id,
        c.identifier_number,
        c.specimen_id,

        /* Case date */
        c.case_date_key,
        cd.full_date as case_date,

        /* Record creation date */
        c.created_date_key,
        crd.full_date as created_date,
        c.created_at,

        /* Location */
        c.location_key,
        l.county,
        l.subcounty,
        l.ward,
        l.point_of_entry,

        /* Facility */
        c.facility_key,
        f.mfl_code,
        f.facility_name,

        /* Case details */
        c.record_type,
        c.case_classification,
        c.laboratory_result,
        c.outcome,
        c.samples_collected,

        /* Boolean indicators */
        c.suspected_flag,
        c.probable_flag,
        c.confirmed_flag,
        c.tested_flag,
        c.died_flag,
        c.recovered_flag,

        /* Numeric indicators */
        c.case_count,
        c.suspected_case_count,
        c.probable_case_count,
        c.confirmed_case_count,
        c.tested_case_count,
        c.sample_collected_count,
        c.recovered_case_count,
        c.death_count,

        /* Lineage */
        c.batch_id,
        c.source_file

    from cases c

    left join case_dates cd
        on c.case_date_key = cd.date_key

    left join created_dates crd
        on c.created_date_key = crd.date_key

    left join locations l
        on c.location_key = l.location_key

    left join facilities f
        on c.facility_key = f.facility_key

)

select *
from final