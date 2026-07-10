
with adam_cases as (

    select
        'adam'::text as source_system,
        _raw_hash::text as source_record_id,

        system_id,
        identifier_number,
        specimen_id,

        reporting_county,
        reporting_subcounty,
        null::text as ward,
        null::text as point_of_entry,
        null::text as community_health_unit,

        health_facility as facility_name,
        null::text as facility_mfl,

        investigation_date as case_date,
        created_at,

        record_type,
        case_classification,
        final_laboratory_results as laboratory_result,
        outcome,

        samples_collected,

        case
            when lower(trim(coalesce(case_classification, '')))
                like '%suspect%'
            then true
            else false
        end as suspected_flag,

        case
            when lower(trim(coalesce(case_classification, '')))
                like '%probable%'
            then true
            else false
        end as probable_flag,

        case
            when lower(trim(coalesce(case_classification, '')))
                like '%confirm%'
            then true

            when lower(trim(coalesce(final_laboratory_results, ''))) in (
                'positive',
                'detected',
                'reactive'
            )
            then true

            else false
        end as confirmed_flag,

        case
            when samples_collected is true then true
            when nullif(trim(final_laboratory_results), '') is not null then true
            else false
        end as tested_flag,

        case
            when lower(trim(coalesce(outcome, ''))) in (
                'died',
                'dead',
                'death',
                'deceased'
            )
            then true
            else false
        end as died_flag,

        case
            when lower(trim(coalesce(outcome, ''))) in (
                'recovered',
                'recovery',
                'alive and recovered',
                'discharged'
            )
            then true
            else false
        end as recovered_flag,

        _batch_id::text as batch_id,
        _source_file::text as source_file

    from {{ ref('slv_adam_cases') }}

),

uhai_cases as (

    select
        'uhai'::text as source_system,
        coalesce(dlt_id, system_id)::text as source_record_id,

        system_id,
        identifier_number,
        null::text as specimen_id,

        reporting_county,
        reporting_subcounty,
        ward,
        point_of_entry,
        community_health_unit,

        null::text as facility_name,
        nullif(trim(facility_id::text), '') as facility_mfl,

        reporting_date as case_date,
        created_at,

        'case'::text as record_type,

        case
            when lower(trim(coalesce(confirmed, ''))) in (
                'yes', 'true', '1'
            )
            then 'Confirmed'

            when lower(trim(coalesce(suspected, ''))) in (
                'yes', 'true', '1'
            )
            then 'Suspected'

            else null
        end as case_classification,

        result as laboratory_result,

        case
            when lower(trim(coalesce(died, ''))) in (
                'yes', 'true', '1'
            )
            then 'Died'

            when lower(trim(coalesce(recovered, ''))) in (
                'yes', 'true', '1'
            )
            then 'Recovered'

            else null
        end as outcome,

        null::boolean as samples_collected,

        case
            when lower(trim(coalesce(suspected, ''))) in (
                'yes', 'true', '1'
            )
            then true

            when lower(trim(coalesce(suspected, ''))) in (
                'no', 'false', '0'
            )
            then false

            else null
        end as suspected_flag,

        false as probable_flag,

        case
            when lower(trim(coalesce(confirmed, ''))) in (
                'yes', 'true', '1'
            )
            then true

            when lower(trim(coalesce(confirmed, ''))) in (
                'no', 'false', '0'
            )
            then false

            else null
        end as confirmed_flag,

        case
            when lower(trim(coalesce(tested, ''))) in (
                'yes', 'true', '1'
            )
            then true

            when lower(trim(coalesce(tested, ''))) in (
                'no', 'false', '0'
            )
            then false

            else null
        end as tested_flag,

        case
            when lower(trim(coalesce(died, ''))) in (
                'yes', 'true', '1'
            )
            then true

            when lower(trim(coalesce(died, ''))) in (
                'no', 'false', '0'
            )
            then false

            else null
        end as died_flag,

        case
            when lower(trim(coalesce(recovered, ''))) in (
                'yes', 'true', '1'
            )
            then true

            when lower(trim(coalesce(recovered, ''))) in (
                'no', 'false', '0'
            )
            then false

            else null
        end as recovered_flag,

        dlt_load_id::text as batch_id,
        null::text as source_file

    from {{ ref('slv_uhai_cases') }}

),

cases as (

    select * from adam_cases

    union all

    select * from uhai_cases

),

facility_by_mfl as (

    select
        nullif(trim(mfl_code::text), '') as mfl_code,
        min(facility_key) as facility_key

    from {{ ref('dim_facilitylist') }}

    where nullif(trim(mfl_code::text), '') is not null

    group by
        nullif(trim(mfl_code::text), '')

),

facility_by_name as (

    select
        lower(trim(facility_name)) as normalized_facility_name,
        min(facility_key) as facility_key

    from {{ ref('dim_facilitylist') }}

    where nullif(trim(facility_name), '') is not null

    group by
        lower(trim(facility_name))

),

final as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                'c.source_system',
                'c.source_record_id'
            ])
        }} as case_key,

        l.location_key,

        coalesce(
            facility_by_mfl.facility_key,
            facility_by_name.facility_key
        ) as facility_key,

        case_date.date_key as case_date_key,
        created_date.date_key as created_date_key,

        c.source_system,
        c.source_record_id,

        c.system_id,
        c.identifier_number,
        c.specimen_id,

        c.record_type,
        c.case_classification,
        c.laboratory_result,
        c.outcome,

        c.samples_collected,
        c.suspected_flag,
        c.probable_flag,
        c.confirmed_flag,
        c.tested_flag,
        c.died_flag,
        c.recovered_flag,

        c.created_at,

        c.batch_id,
        c.source_file

    from cases c

    left join {{ ref('dim_location') }} l
        on lower(trim(coalesce(c.reporting_county, ''))) =
           lower(trim(coalesce(l.county, '')))

       and lower(trim(coalesce(c.reporting_subcounty, ''))) =
           lower(trim(coalesce(l.subcounty, '')))

       and lower(trim(coalesce(c.ward, ''))) =
           lower(trim(coalesce(l.ward, '')))

       and lower(trim(coalesce(c.point_of_entry, ''))) =
           lower(trim(coalesce(l.point_of_entry, '')))

    left join facility_by_mfl
        on c.facility_mfl = facility_by_mfl.mfl_code

    left join facility_by_name
        on lower(trim(c.facility_name)) =
           facility_by_name.normalized_facility_name

    left join {{ ref('dim_date') }} case_date
        on c.case_date = case_date.full_date

    left join {{ ref('dim_date') }} created_date
        on c.created_at::date = created_date.full_date

)

select
    case_key,

    location_key,
    facility_key,

    case_date_key,
    created_date_key,

    source_system,
    source_record_id,

    system_id,
    identifier_number,
    specimen_id,

    record_type,
    case_classification,
    laboratory_result,
    outcome,

    samples_collected,
    suspected_flag,
    probable_flag,
    confirmed_flag,
    tested_flag,
    died_flag,
    recovered_flag,

    created_at,

    1 as case_count,

    case
        when suspected_flag is true then 1
        else 0
    end as suspected_case_count,

    case
        when probable_flag is true then 1
        else 0
    end as probable_case_count,

    case
        when confirmed_flag is true then 1
        else 0
    end as confirmed_case_count,

    case
        when tested_flag is true then 1
        else 0
    end as tested_case_count,

    case
        when samples_collected is true then 1
        else 0
    end as sample_collected_count,

    case
        when recovered_flag is true then 1
        else 0
    end as recovered_case_count,

    case
        when died_flag is true then 1
        else 0
    end as death_count,

    batch_id,
    source_file

from final