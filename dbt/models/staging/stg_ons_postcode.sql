select
    upper(trim(postcode)) as postcode,
    latitude,
    longitude,
    region_code as region,
    lad_code as admin_district,
    lsoa_code,
    msoa_code
from {{ source('bronze', 'ons_postcode') }}
where status is null  -- `status` holds NSPL's termination date (YYYYMM); null means still live
  and postcode is not null
  and latitude is not null
  and longitude is not null