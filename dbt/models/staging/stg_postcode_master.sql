-- Silver: reconciled postcode lookup, preferring ONS (authoritative) and
-- falling back to postcodes.io where a postcode is missing from ONS.
with ons as (
    select * from {{ ref('stg_ons_postcode') }}
),

fallback as (
    select * from {{ ref('stg_postcode_io') }}
),

combined as (
    select
        coalesce(ons.postcode, fallback.postcode) as postcode,
        coalesce(ons.latitude, fallback.latitude) as latitude,
        coalesce(ons.longitude, fallback.longitude) as longitude,
        coalesce(ons.region, fallback.region) as region,
        coalesce(ons.admin_district, fallback.admin_district) as admin_district,
        case when ons.postcode is not null then 'ons' else 'postcodes_io' end as source
    from ons
    full outer join fallback
        on ons.postcode = fallback.postcode
)

select * from combined
