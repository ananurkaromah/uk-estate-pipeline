-- Silver: cleaned, typed, deduplicated Land Registry transactions
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
        county
    from source
    where price is not null
      and postcode is not null
)

select distinct * from cleaned
