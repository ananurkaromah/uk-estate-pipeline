-- Fails (returns a row) if fewer than 98% of transactions resolve to a region.
-- Normal value is ~99.9%. This is the check that would have caught the NSPL
-- ingestion bug where only one of 150 regional files was loaded.
SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE region IS NOT NULL) / COUNT(*), 1) AS match_rate_pct
FROM {{ ref('fct_property_prices') }}
HAVING 100.0 * COUNT(*) FILTER (WHERE region IS NOT NULL) / COUNT(*) < 98