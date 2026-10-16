WITH ons AS (
    SELECT * FROM {{ ref('stg_ons_postcode') }}
),

fallback AS (
    SELECT * FROM {{ ref('stg_postcode_io') }}
),

region_names AS (
    SELECT * FROM {{ ref('stg_region_lookup') }}
),

combined AS (
    SELECT
        coalesce(ons.postcode, fallback.postcode) AS postcode,
        coalesce(ons.latitude, fallback.latitude) AS latitude,
        coalesce(ons.longitude, fallback.longitude) AS longitude,
        coalesce(rl.region_name, fallback.region, CASE ons.region WHEN 'W99999999' THEN 'Wales' ELSE ons.region END) AS region,
        coalesce(ons.admin_district, fallback.admin_district) AS admin_district,
        CASE WHEN ons.postcode IS NOT NULL THEN 'ons' ELSE 'postcodes_io' END AS source
    FROM ons
    FULL OUTER JOIN fallback
        ON ons.postcode = fallback.postcode
    LEFT JOIN region_names AS rl
        ON ons.region = rl.region_code
)

SELECT * FROM combined