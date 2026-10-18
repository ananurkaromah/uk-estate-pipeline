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
      and coalesce(record_status, 'A') != 'D'
)

select * from cleaned