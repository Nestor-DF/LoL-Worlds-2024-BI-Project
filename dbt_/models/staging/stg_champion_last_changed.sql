{{ config(materialized='view') }}

with source as (
    -- Normally we would select from the table here, but we are using seeds here
    select * from {{ ref('champion_last_changed') }}
),

renamed as (
    select
        "Name" as champion,
        "Last Changed" as last_changed_raw,

        -- extrae número: V14.14 -> 14.14
        cast(
            regexp_replace("Last Changed", '[^0-9\.]', '', 'g')
            as {{ dbt.type_numeric() }}
        ) as last_changed_version
    from source
)

select * from renamed
