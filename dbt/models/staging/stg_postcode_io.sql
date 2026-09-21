-- Silver: postcode -> lat/long/region from postcodes.io
select
    upper(trim(postcode)) as postcode,
    latitude,
    longitude,
    region,
    admin_district
from {{ source('bronze', 'postcode_io') }}
where latitude is not null
  and longitude is not null
