-- Fails if the gold table has fewer than 50,000 rows. The table is ~90,000 rows
-- and should only grow with the incremental load, so a drop below this floor
-- means something went badly wrong. Raise the floor as the data grows.
SELECT COUNT(*) AS row_count
FROM {{ ref('fct_property_prices') }}
HAVING COUNT(*) < 50000