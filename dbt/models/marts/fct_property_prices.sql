-- Gold: property transactions enriched with region, ready for Metabase
select
    lr.transaction_id,
    lr.price,
    lr.transaction_date,
    date_trunc('month', lr.transaction_date) as transaction_month,
    lr.property_type,
    lr.postcode,
    pc.region,
    pc.admin_district,
    pc.latitude,
    pc.longitude,
    pc.source as postcode_source
from {{ ref('stg_land_registry') }} as lr
left join {{ ref('stg_postcode_master') }} as pc
    on lr.postcode = pc.postcode
