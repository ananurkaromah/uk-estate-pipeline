-- Silver: ONS region code -> human-readable name lookup
SELECT
    region_code,
    region_name
FROM {{ source('bronze', 'region_lookup') }}