{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    post_hook="delete from {{ this }} as g where not exists (select 1 from {{ ref('stg_land_registry') }} s where s.transaction_id = g.transaction_id)"
) }}

-- Gold: property transactions enriched with region, ready for Metabase.
-- Incremental + unique_key='transaction_id' means dbt merges on
-- transaction_id across runs -- new transactions get inserted, existing
-- ones get updated in place -- instead of a full table rebuild every run.
-- an incremental model only inserts and updates. When a transaction is later cancelled (D), 
-- silver drops it but gold keeps the old row. The hook deletes gold rows that no longer exist in silver, 
-- so gold always equals silver.
-- Risk: if silver were ever empty because of an upstream failure, 
-- the hook would empty gold. In that situation dbt_run fails first, so the hook does not run.

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