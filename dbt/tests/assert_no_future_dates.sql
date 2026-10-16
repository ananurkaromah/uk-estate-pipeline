{{ config(severity='warn') }}

-- Warns on transactions dated in the future, which would indicate a
-- data-entry error in the source.
SELECT transaction_id, transaction_date
FROM {{ ref('fct_property_prices') }}
WHERE transaction_date > CURRENT_DATE