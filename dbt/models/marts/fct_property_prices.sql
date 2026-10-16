{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

-- Gold: property transactions enriched with region, ready for Metabase.
-- Incremental + unique_key='transaction_id' means dbt merges on
-- transaction_id across runs -- new transactions get inserted, existing
-- ones get updated in place -- instead of a full table rebuild every run.
SELECT
    lr.transaction_id,
    lr.price,
    lr.price > 10000000 AS is_price_outlier,
    lr.transaction_date,
    date_trunc('month', lr.transaction_date) AS transaction_month,
    lr.property_type,
    lr.ppd_category_type,
    lr.postcode,
    pc.region,
    pc.admin_district,
    pc.latitude,
    pc.longitude,
    pc.source AS postcode_source
FROM {{ ref('stg_land_registry') }} AS lr
LEFT JOIN {{ ref('stg_postcode_master') }} AS pc
    ON lr.postcode = pc.postcode