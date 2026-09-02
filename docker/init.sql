-- Create schemas for the medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant all to estate user
GRANT ALL ON SCHEMA bronze TO estate;
GRANT ALL ON SCHEMA silver TO estate;
GRANT ALL ON SCHEMA gold TO estate;

-- Bronze: Land Registry Price Paid
CREATE TABLE IF NOT EXISTS bronze.land_registry_pp (
    transaction_id    TEXT,
    price             INTEGER,
    date_of_transfer  DATE,
    postcode          TEXT,
    property_type     CHAR(1),      -- D=Detached, S=Semi-Detached, T=Terraced, F=Flats/Maisonettes, O=Other
    old_new           CHAR(1),      -- Y=New, N=Established
    duration          CHAR(1),      -- F=Freehold, L=Leasehold
    paon              TEXT,         -- Primary Addressable Object Name (house number/name)
    saon              TEXT,         -- Secondary Addressable Object Name (flat/unit)
    street            TEXT,
    locality          TEXT,
    town_city         TEXT,
    district          TEXT,
    county            TEXT,
    ppd_category_type CHAR(1),      -- A=Standard Price Paid Entry, B=Additional Price Paid Entry
    record_status     CHAR(1),      -- A=Addition, C=Change, D=Delete
    ingested_at       TIMESTAMP DEFAULT NOW()
);

-- Bronze: ONS Postcode
CREATE TABLE IF NOT EXISTS bronze.ons_postcode (
    postcode           TEXT         PRIMARY KEY,
    status             TEXT,        -- 1=Live, 0=Terminated
    user_type          INTEGER,
    easting            INTEGER,
    northing           INTEGER,
    positional_quality INTEGER,
    country            TEXT,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    postcode_area      TEXT,
    postcode_district  TEXT,
    postcode_sector    TEXT,
    lsoa_code          TEXT,        -- Lower Layer Super Output Area
    msoa_code          TEXT,        -- Middle Layer Super Output Area
    lad_code           TEXT,        -- Local Authority District
    lad_name           TEXT,
    region_code        TEXT,
    region_name        TEXT,
    ingested_at        TIMESTAMP DEFAULT NOW()
);

-- Bronze: postcodes.io
CREATE TABLE IF NOT EXISTS bronze.postcode_io (
    postcode           TEXT         PRIMARY KEY,
    quality            INTEGER,
    eastings           INTEGER,
    northings          INTEGER,
    country            TEXT,
    nhs_ha             TEXT,
    longitude          DOUBLE PRECISION,
    latitude           DOUBLE PRECISION,
    european_electoral_region TEXT,
    primary_care_trust TEXT,
    region             TEXT,
    lsoa               TEXT,        -- Lower Layer Super Output Area
    msoa               TEXT,        -- Middle Layer Super Output Area
    incode             TEXT,        -- Inward Code
    outcode            TEXT,        -- Outward Code
    parliamentary_constituency TEXT,
    admin_district     TEXT,        -- Local Authority District
    parish             TEXT,
    admin_county       TEXT,
    admin_ward         TEXT,
    ced                TEXT,        -- County Electoral Division
    ccg                TEXT,        -- Clinical Commissioning Group
    nuts               TEXT,        -- Nomenclature of Units for Territorial Statistics
    ingested_at        TIMESTAMP DEFAULT NOW()
);

-- Indexes for join performance
CREATE INDEX IF NOT EXISTS idx_lr_pp_transaction ON bronze.land_registry_pp (transaction_id);
CREATE INDEX IF NOT EXISTS idx_lr_pp_postcode     ON bronze.land_registry_pp (postcode);
CREATE INDEX IF NOT EXISTS idx_lr_pp_date         ON bronze.land_registry_pp (date_of_transfer);
CREATE INDEX IF NOT EXISTS idx_lr_pp_county       ON bronze.land_registry_pp (county);
CREATE INDEX IF NOT EXISTS idx_lr_pp_property     ON bronze.land_registry_pp (property_type);
