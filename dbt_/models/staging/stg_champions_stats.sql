{{ config(materialized='view') }}

with source as (
  select * from {{ source('raw', 'champions_stats') }}
),

renamed as (
  select
    champion,

    cast(picks  as {{ dbt.type_numeric() }}) as picks,
    cast(bans   as {{ dbt.type_numeric() }}) as bans,
    cast(wins   as {{ dbt.type_numeric() }}) as wins,
    cast(losses as {{ dbt.type_numeric() }}) as losses,

    cast(dpm as {{ dbt.type_numeric() }}) as dmg_per_min,
    cast(gpm as {{ dbt.type_numeric() }}) as gold_per_min,

    cast(gd_15  as {{ dbt.type_numeric() }}) as gold_diff_15,
    cast(csd_15 as {{ dbt.type_numeric() }}) as cs_diff_15,
    cast(xpd_15 as {{ dbt.type_numeric() }}) as xp_diff_15
  from source
)

select * from renamed
