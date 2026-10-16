SELECT
    upper(trim(postcode)) AS postcode,
    latitude,
    longitude,
    region_code AS region,
    lad_code AS admin_district,
    lsoa_code,
    msoa_code
FROM {{ source('bronze', 'ons_postcode') }}
WHERE status IS NULL  -- `status` holds NSPL's termination date (YYYYMM); null means still live
  AND postcode IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL