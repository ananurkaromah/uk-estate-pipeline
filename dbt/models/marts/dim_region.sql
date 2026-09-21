-- Gold: one row per region for dashboard filters/joins
select distinct
    region,
    admin_district
from {{ ref('stg_postcode_master') }}
where region is not null
