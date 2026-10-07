-- Silver: cleaned, typed, deduplicated Land Registry transactions.
-- record_status = 'D' (Delete) rows are kept in bronze for audit
-- traceability, but filtered out here -- they represent transactions
-- that have been formally voided/withdrawn by HM Land Registry.
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
      and postcode is not null
      and record_status != 'D'
)

select distinct * from cleaned