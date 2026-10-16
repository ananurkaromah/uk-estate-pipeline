-- Silver: postcode -> lat/long/region from postcodes.io
SELECT DISTINCT
    upper(trim(postcode)) as postcode,
    latitude,
    longitude,
    region,
    admin_district
FROM {{ source('bronze', 'postcode_io') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
