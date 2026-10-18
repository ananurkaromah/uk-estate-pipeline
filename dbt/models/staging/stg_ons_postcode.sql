-- Silver: postcode -> lat/long/region from the ONS Postcode Directory (NSPL).
-- Terminated postcodes are kept: Price Paid Data records the postcode used at the
-- time of sale, so historical transactions often sit on postcodes that have since
-- been terminated. `terminated_yyyymm` holds the termination date, null if live.
select
    upper(trim(postcode)) as postcode,
    latitude,
    longitude,
    region_code as region,
    lad_code as admin_district,
    lsoa_code,
    msoa_code,
    status as terminated_yyyymm
from {{ source('bronze', 'ons_postcode') }}
where postcode is not null
  and latitude is not null
  and longitude is not null