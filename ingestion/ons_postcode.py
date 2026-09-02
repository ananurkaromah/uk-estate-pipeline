"""Ingest the ONS Postcode Directory (NSPL) into bronze.ons_postcode.

The NSPL is published by ONS Geography via the Open Geography Portal as a
bulk CSV/zip export. The exact download URL changes between releases, so
set NSPL_SOURCE_URL to the current direct CSV link before running this
(e.g. from https://geoportal.statistics.gov.uk -> search "NSPL").

This is used alongside postcodes.io (see postcodes.py) as a second,
authoritative source for postcode -> region/LSOA/MSOA lookups; the dbt
layer reconciles the two (see models/staging/stg_postcode_master.sql).
"""
import logging
import os
from io import StringIO

import pandas as pd
import requests

from db import load_dataframe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NSPL_SOURCE_URL = os.environ.get("NSPL_SOURCE_URL", "")

# Mapping from the raw NSPL column names to bronze.ons_postcode columns.
# NSPL exports commonly use these short codes; adjust if ONS changes them.
COLUMN_MAP = {
    "pcds": "postcode",
    "doterm": "status",
    "usertype": "user_type",
    "oseast1m": "easting",
    "osnrth1m": "northing",
    "osgrdind": "positional_quality",
    "ctry": "country",
    "lat": "latitude",
    "long": "longitude",
    "pcon": "postcode_area",
    "oslaua": "postcode_district",
    "osward": "postcode_sector",
    "lsoa11": "lsoa_code",
    "msoa11": "msoa_code",
    "laua": "lad_code",
    "ladnm": "lad_name",
    "rgn": "region_code",
    "rgn_name": "region_name",
}

BRONZE_COLUMNS = list(COLUMN_MAP.values())


def run():
    if not NSPL_SOURCE_URL:
        raise ValueError(
            "NSPL_SOURCE_URL is not set. Get the current NSPL CSV download "
            "link from https://geoportal.statistics.gov.uk and set it as an "
            "environment variable before running this task."
        )

    logger.info("Downloading ONS Postcode Directory from %s", NSPL_SOURCE_URL)
    resp = requests.get(NSPL_SOURCE_URL, timeout=300)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    df = df.rename(columns=COLUMN_MAP)

    missing = [c for c in BRONZE_COLUMNS if c not in df.columns]
    if missing:
        logger.warning("NSPL export missing expected columns: %s", missing)
    df = df[[c for c in BRONZE_COLUMNS if c in df.columns]]

    before = len(df)
    df = df.dropna(subset=["postcode"])
    logger.info("Dropped %d rows missing postcode", before - len(df))

    load_dataframe(df, table_name="ons_postcode", schema="bronze")


if __name__ == "__main__":
    run()
