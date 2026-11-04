-- Silver: cleaned and typed Land Registry transactions.
-- Dropped here (kept in bronze for audit):
--   * record_status = 'D' (transactions deleted at source)
--   * rows with no postcode (cannot be mapped to a region)
--   * rows with no price
with source as (
    select * from {{ source('bronze', 'land_registry_pp') }}
),

cleaned as (
    select
        transaction_id,
        cast(price as numeric) as price,
        cast(date_of_transfer as date) as transaction_date,
        upper(trim(postcode)) as postcode,
        property_type,
        old_new,
        duration,
        town_city,
        district,
        county,
        ppd_category_type
    from source
    where price is not null
      and nullif(trim(postcode), '') is not null
      and coalesce(record_status, 'A') != 'D'
)

select * from cleaned