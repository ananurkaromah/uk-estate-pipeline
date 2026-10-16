{{ config(severity='warn') }}

-- Warns if the newest transaction is more than 75 days old. Land Registry
-- publishes each month's data around the 20th working day, so a gap of up to
-- ~60 days is normal.
SELECT MAX(transaction_date) AS latest_transaction_date
FROM {{ ref('fct_property_prices') }}
HAVING MAX(transaction_date) < CURRENT_DATE - INTERVAL '75 days'