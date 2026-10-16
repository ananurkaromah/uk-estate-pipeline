-- Gold: one row per region for dashboard filters/joins
SELECT DISTINCT
    region,
    admin_district
FROM {{ ref('stg_postcode_master') }}
WHERE region IS NOT NULL
