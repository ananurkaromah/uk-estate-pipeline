-- Fails if any region still looks like a raw ONS code (e.g. E12000007),
-- which happens when the region code-to-name lookup is missing or broken.
SELECT DISTINCT region
FROM {{ ref('fct_property_prices') }}
WHERE region ~ '^[A-Z][0-9]{8}$'