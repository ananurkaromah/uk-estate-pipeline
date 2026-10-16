-- Silver: cleaned, typed, deduplicated Land Registry transactions.
-- record_status = 'D' (Delete) rows are kept in bronze for audit
-- traceability, but filtered out here -- they represent transactions
-- that have been formally voided/withdrawn by HM Land Registry.
WITH source AS (
    SELECT * FROM {{ source('bronze', 'land_registry_pp') }}
),

cleaned AS (
    SELECT
        transaction_id,
        cast(price AS NUMERIC(10, 2)) AS price,
        cast(date_of_transfer AS date) AS transaction_date,
        upper(trim(postcode)) AS postcode,
        property_type,
        old_new,
        duration,
        town_city,
        district,
        county,
        ppd_category_type
    FROM source
    WHERE price IS NOT NULL
       AND postcode IS NOT NULL
       AND record_status != 'D'
)

SELECT DISTINCT * FROM cleaned