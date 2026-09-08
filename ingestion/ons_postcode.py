"""Ingest the ONS Postcode Directory (NSPL) into bronze.ons_postcode.

The NSPL is published by ONS Geography via the Open Geography Portal as a
**zip archive** containing one or more CSV files (not a raw CSV response).
The exact download URL changes between releases and isn't a fixed link --
set NSPL_SOURCE_URL to the current download link before running this. The
Open Geography Portal (https://geoportal.statistics.gov.uk) doesn't expose
a permanent URL: open the current NSPL dataset page, click "Download", and
copy the resulting file link.

Downloaded to a temp file and loaded in chunks -- the full NSPL is large
enough that holding the whole zip in memory (BytesIO) plus a full parsed
DataFrame at once is wasteful, especially in a memory-constrained
environment (e.g. WSL2 with a capped .wslconfig memory setting).

This is used alongside postcodes.io (see postcodes.py) as a second,
authoritative source for postcode -> region/LSOA/MSOA lookups; the dbt
layer reconciles the two (see models/staging/stg_postcode_master.sql).
"""
import logging
import os
import tempfile
import zipfile

import pandas as pd
import requests

from db import ensure_schema, get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NSPL_SOURCE_URL = os.environ.get("NSPL_SOURCE_URL", "")

# Confirmed against the actual downloaded file's header (August 2026 NSPL
# release) -- NSPL column suffixes change with each release (e.g. "lad26cd"
# vs older "laua"), and critically NSPL only provides CODES, never
# human-readable names, for region/local authority -- there is no
# "region_name"/"lad_name" source column in any NSPL release.
COLUMN_MAP = {
    "pcds": "postcode",
    "doterm": "status",           # termination date (YYYYMM), blank if still live
    "usrtypind": "user_type",
    "east1m": "easting",
    "north1m": "northing",
    "gridind": "positional_quality",
    "ctry26cd": "country",
    "lat": "latitude",
    "long": "longitude",
    "rgn26cd": "region_code",
    "lad26cd": "lad_code",
    "lsoa21cd": "lsoa_code",
    "msoa21cd": "msoa_code",
}

BRONZE_COLUMNS = list(COLUMN_MAP.values())
SCHEMA = "bronze"
TABLE_NAME = "ons_postcode"
CHUNK_SIZE = 50_000


def run():
    if not NSPL_SOURCE_URL:
        raise ValueError(
            "NSPL_SOURCE_URL is not set. Get the current NSPL download link "
            "from https://geoportal.statistics.gov.uk and set it as an "
            "environment variable before running this task."
        )

    engine = get_engine()
    ensure_schema(engine, SCHEMA)
    warned_missing = False

    # DROP ... CASCADE once up front rather than pandas' to_sql
    # if_exists="replace", which fails once a dbt view depends on this
    # table. dbt_run always runs again right after ingestion in the DAG,
    # so any dropped downstream view gets recreated.
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_NAME} CASCADE")

    logger.info("Downloading ONS Postcode Directory from %s", NSPL_SOURCE_URL)
    total_rows = 0

    with requests.get(NSPL_SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        # Stream the zip to disk instead of holding it in memory (BytesIO).
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            with zipfile.ZipFile(tmp.name) as archive:
                # NSPL zips nest the data CSV under a "Data/" folder alongside
                # documentation/user guide files -- find it rather than assume a name.
                csv_names = [
                    n for n in archive.namelist()
                    if n.lower().endswith(".csv") and "/data/" in n.lower()
                ]
                if not csv_names:
                    csv_names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise ValueError(f"No CSV found in NSPL zip. Contents: {archive.namelist()}")

                csv_name = csv_names[0]
                logger.info("Streaming %s from NSPL zip in chunks", csv_name)

                with archive.open(csv_name) as f:
                    for chunk in pd.read_csv(f, low_memory=False, chunksize=CHUNK_SIZE):
                        chunk = chunk.rename(columns=COLUMN_MAP)

                        missing = [c for c in BRONZE_COLUMNS if c not in chunk.columns]
                        if missing and not warned_missing:
                            logger.warning("NSPL export missing expected columns: %s", missing)
                            warned_missing = True
                        chunk = chunk[[c for c in BRONZE_COLUMNS if c in chunk.columns]]

                        before = len(chunk)
                        chunk = chunk.dropna(subset=["postcode"])
                        dropped = before - len(chunk)
                        if dropped:
                            logger.info("Dropped %d rows missing postcode in this chunk", dropped)

                        chunk.to_sql(
                            TABLE_NAME,
                            engine,
                            schema=SCHEMA,
                            if_exists="append",
                            index=False,
                        )
                        total_rows += len(chunk)
                        logger.info("Loaded chunk (%d rows so far)", total_rows)

    logger.info("Finished loading %d rows into %s.%s", total_rows, SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()