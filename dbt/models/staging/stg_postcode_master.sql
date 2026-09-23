with ons as (
    select * from {{ ref('stg_ons_postcode') }}
),

fallback as (
    select * from {{ ref('stg_postcode_io') }}
),

region_names as (
    select * from {{ ref('stg_region_lookup') }}
),

combined as (
    select
        coalesce(ons.postcode, fallback.postcode) as postcode,
        coalesce(ons.latitude, fallback.latitude) as latitude,
        coalesce(ons.longitude, fallback.longitude) as longitude,
        coalesce(rl.region_name, fallback.region, ons.region) as region,
        coalesce(ons.admin_district, fallback.admin_district) as admin_district,
        case when ons.postcode is not null then 'ons' else 'postcodes_io' end as source
    from ons
    full outer join fallback
        on ons.postcode = fallback.postcode
    left join region_names as rl
        on ons.region = rl.region_code
)

select * from combined