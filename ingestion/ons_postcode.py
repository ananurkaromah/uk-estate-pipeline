"""Ingest the ONS Postcode Directory (NSPL) into bronze.ons_postcode.

The NSPL is published as a zip archive. The download is validated (expected
columns, region lookup file, a row-count floor) and the reference tables are
replaced inside a single transaction, so a failed or truncated download never
leaves them empty or half-loaded.
"""
import logging
import os
import tempfile
import zipfile

import pandas as pd
import requests
from db import ensure_schema, get_engine
from sqlalchemy import inspect

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
MIN_EXPECTED_ROWS = 1_000_000  # the full UK file has ~2.7M rows


def _find_data_csv(archive: zipfile.ZipFile) -> str:
    """Combined UK file only: not multi_csv/ (different header format) and
    not Documents/ (unrelated lookup tables)."""
    csv_names = [
        n for n in archive.namelist()
        if n.lower().endswith(".csv")
        and n.lower().startswith("data/")
        and "multi_csv" not in n.lower()
    ]
    if not csv_names:
        raise ValueError(f"No combined NSPL CSV found in zip. Contents: {archive.namelist()}")
    return csv_names[0]


def _read_region_lookup(archive: zipfile.ZipFile) -> pd.DataFrame:
    candidates = [
        n for n in archive.namelist()
        if "rgn region names and codes" in n.lower() and n.lower().endswith(".csv")
    ]
    if not candidates:
        raise ValueError(
            "Region lookup file not found in the NSPL zip; regions would fall back to raw ONS codes."
        )
    with archive.open(candidates[0]) as f:
        lookup_df = pd.read_csv(f, encoding="utf-8-sig")

    code_col = next((c for c in lookup_df.columns if c.upper().endswith("CD")), None)
    name_col = next((c for c in lookup_df.columns if c.upper().endswith("NM")), None)
    if not (code_col and name_col):
        raise ValueError(
            f"Could not detect code/name columns in the region lookup file: {lookup_df.columns.tolist()}"
        )
    return lookup_df[[code_col, name_col]].rename(
        columns={code_col: "region_code", name_col: "region_name"}
    )


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

    logger.info("Downloading ONS Postcode Directory from %s", NSPL_SOURCE_URL)
    with requests.get(NSPL_SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            with zipfile.ZipFile(tmp.name) as archive:
                csv_name = _find_data_csv(archive)
                logger.info("Validating %s", csv_name)

                # Validate everything BEFORE touching the existing tables.
                with archive.open(csv_name) as f:
                    header = set(pd.read_csv(f, nrows=0).columns)
                missing = [c for c in COLUMN_MAP if c not in header]
                if missing:
                    raise ValueError(
                        f"NSPL file is missing expected columns: {missing}. "
                        "The release format may have changed."
                    )
                lookup_df = _read_region_lookup(archive)

                total_rows = 0
                # Single transaction: TRUNCATE + load are rolled back together
                # if anything below fails, leaving the previous data intact.
                with engine.begin() as conn:
                    if inspector.has_table(TABLE_NAME, schema=SCHEMA):
                        conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA}.{TABLE_NAME}")

                    with archive.open(csv_name) as f:
                        for chunk in pd.read_csv(f, low_memory=False, chunksize=CHUNK_SIZE):
                            chunk = chunk.rename(columns=COLUMN_MAP)[BRONZE_COLUMNS]
                            chunk.to_sql(
                                TABLE_NAME, conn, schema=SCHEMA, if_exists="append", index=False
                            )
                            total_rows += len(chunk)
                            logger.info("Loaded chunk (%d rows so far)", total_rows)

                    if total_rows < MIN_EXPECTED_ROWS:
                        raise ValueError(
                            f"Only {total_rows} NSPL rows loaded (expected at least "
                            f"{MIN_EXPECTED_ROWS}). Wrong file or truncated download; rolling back."
                        )

                    if inspector.has_table(LOOKUP_TABLE_NAME, schema=SCHEMA):
                        conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA}.{LOOKUP_TABLE_NAME}")
                    lookup_df.to_sql(
                        LOOKUP_TABLE_NAME, conn, schema=SCHEMA, if_exists="append", index=False
                    )
                    logger.info(
                        "Loaded %d region code->name mappings into %s.%s",
                        len(lookup_df), SCHEMA, LOOKUP_TABLE_NAME,
                    )

    logger.info("Finished loading %d rows into %s.%s", total_rows, SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()