-- Silver: postcode -> lat/long/region from the ONS Postcode Directory (NSPL)
select
    upper(trim(postcode)) as postcode,
    latitude,
    longitude,
    region_name as region,
    lad_name as admin_district,
    lsoa_code,
    msoa_code
from {{ source('bronze', 'ons_postcode') }}
where status is distinct from '0'  -- exclude terminated postcodes
  and latitude is not null
  and longitude is not null
