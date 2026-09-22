-- Silver: ONS region code -> human-readable name lookup
select
    region_code,
    region_name
from {{ source('bronze', 'region_lookup') }}