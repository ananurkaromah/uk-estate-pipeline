-- Fails if the gold table has fewer than 5,000,000 (about 95% of the current 5.27M). 
--The table only grows through UPSERT apart from cancellations, so a drop below this means data loss.

SELECT COUNT(*) AS row_count
FROM {{ ref('fct_property_prices') }}
HAVING COUNT(*) < 5000000