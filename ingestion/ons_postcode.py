import logging
import os
import tempfile
import zipfile

import pandas as pd
import requests
from sqlalchemy import inspect

from db import ensure_schema, get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NSPL_SOURCE_URL = os.environ.get("NSPL_SOURCE_URL", "")

COLUMN_MAP = {
    "pcds": "postcode",
    "doterm": "status",
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
LOOKUP_TABLE_NAME = "region_lookup"
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
    inspector = inspect(engine)

    if inspector.has_table(TABLE_NAME, schema=SCHEMA):
        with engine.begin() as conn:
            conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA}.{TABLE_NAME}")
        logger.info("Truncated existing %s.%s", SCHEMA, TABLE_NAME)

    logger.info("Downloading ONS Postcode Directory from %s", NSPL_SOURCE_URL)
    total_rows = 0

    with requests.get(NSPL_SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            with zipfile.ZipFile(tmp.name) as archive:
                # Combined UK file only -- not multi_csv/ (different header
                # format) and not Documents/ (unrelated lookup tables).
                csv_names = [
                    n for n in archive.namelist()
                    if n.lower().endswith(".csv")
                    and n.lower().startswith("data/")
                    and "multi_csv" not in n.lower()
                ]
                if not csv_names:
                    raise ValueError(f"No combined NSPL CSV found in zip. Contents: {archive.namelist()}")

                csv_name = csv_names[0]
                logger.info("Streaming %s from NSPL zip in chunks", csv_name)

                with archive.open(csv_name) as f:
                    for chunk in pd.read_csv(f, low_memory=False, chunksize=CHUNK_SIZE):
                        chunk = chunk.rename(columns=COLUMN_MAP)

                        missing = [c for c in BRONZE_COLUMNS if c not in chunk.columns]
                        if missing:
                            logger.warning("NSPL export missing expected columns: %s", missing)
                        chunk = chunk[[c for c in BRONZE_COLUMNS if c in chunk.columns]]

                        chunk.to_sql(
                            TABLE_NAME,
                            engine,
                            schema=SCHEMA,
                            if_exists="append",
                            index=False,
                        )
                        total_rows += len(chunk)
                        logger.info("Loaded chunk (%d rows so far)", total_rows)

                # Region code -> name lookup, bundled in same zip.
                lookup_candidates = [
                    n for n in archive.namelist()
                    if "rgn region names and codes" in n.lower() and n.lower().endswith(".csv")
                ]
                if lookup_candidates:
                    lookup_name = lookup_candidates[0]
                    logger.info("Loading region code->name lookup from %s", lookup_name)
                    with archive.open(lookup_name) as f:
                        lookup_df = pd.read_csv(f, encoding="utf-8-sig")

                    code_col = next((c for c in lookup_df.columns if c.upper().endswith("CD")), None)
                    name_col = next((c for c in lookup_df.columns if c.upper().endswith("NM")), None)

                    if code_col and name_col:
                        lookup_df = lookup_df[[code_col, name_col]].rename(
                            columns={code_col: "region_code", name_col: "region_name"}
                        )
                        if inspector.has_table(LOOKUP_TABLE_NAME, schema=SCHEMA):
                            with engine.begin() as conn:
                                conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA}.{LOOKUP_TABLE_NAME}")
                        lookup_df.to_sql(LOOKUP_TABLE_NAME, engine, schema=SCHEMA, if_exists="append", index=False)
                        logger.info("Loaded %d region code->name mappings into %s.%s", len(lookup_df), SCHEMA, LOOKUP_TABLE_NAME)
                    else:
                        logger.warning("Could not detect code/name columns in region lookup file: %s", lookup_df.columns.tolist())
                else:
                    logger.warning("Region lookup file not found in NSPL zip; region will stay as raw codes")

    logger.info("Finished loading %d rows into %s.%s", total_rows, SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()