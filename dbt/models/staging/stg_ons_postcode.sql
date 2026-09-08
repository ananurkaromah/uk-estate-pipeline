-- Silver: postcode -> lat/long/region from the ONS Postcode Directory (NSPL)
--
-- NOTE: NSPL only provides region/local authority as CODES (e.g. "E12000007"),
-- never human-readable names -- there is no name column in any NSPL release.
-- This means when stg_postcode_master.sql coalesces this with
-- stg_postcode_io (which returns readable names like "London"), the
-- resulting `region`/`admin_district` values will be a code for
-- ONS-resolved rows and a name for postcodes.io-fallback rows -- a real,
-- known inconsistency worth calling out (see README "Future Work"):
-- fixing it properly would mean joining ONS's separate code-to-name
-- lookup ("Register") tables, which this project doesn't currently do.
select
    upper(trim(postcode)) as postcode,
    latitude,
    longitude,
    region_code as region,
    lad_code as admin_district,
    lsoa_code,
    msoa_code
from {{ source('bronze', 'ons_postcode') }}
where status is null  -- `status` holds NSPL's termination date (YYYYMM); null means still live
  and latitude is not null
  and longitude is not null