'''
Ingest the ONS Postcode Directory (NSPL) into bronze.ons_postcode.
Downloaded to a temp file and loaded in chunks.
'''

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

    logger.info("Downloading ONS Postcode Directory from %s", NSPL_SOURCE_URL)
    total_rows = 0
    first_chunk = True

    with requests.get(NSPL_SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            with zipfile.ZipFile(tmp.name) as archive:
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
                        if missing and first_chunk:
                            logger.warning("NSPL export missing expected columns: %s", missing)
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
                            if_exists="replace" if first_chunk else "append",
                            index=False,
                        )
                        first_chunk = False
                        total_rows += len(chunk)
                        logger.info("Loaded chunk (%d rows so far)", total_rows)

    logger.info("Finished loading %d rows into %s.%s", total_rows, SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()