"""Ingest UK HM Land Registry Price Paid Data (monthly CSV, no auth) into bronze.

Downloaded to a temp file and loaded in chunks rather than all at once --
the monthly Price Paid Data file is large enough that loading the full
response body and a full DataFrame into memory at once is wasteful,
especially in a memory-constrained environment (e.g. WSL2 with a capped
.wslconfig memory setting).
"""
import logging
import tempfile

import pandas as pd
import requests

from db import ensure_schema, get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    "/pp-monthly-update-new-version.csv"
)

COLUMNS = [
    "transaction_id", "price", "date_of_transfer", "postcode", "property_type",
    "old_new", "duration", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type", "record_status",
]

SCHEMA = "bronze"
TABLE_NAME = "land_registry_pp"
CHUNK_SIZE = 50_000


def run():
    engine = get_engine()
    ensure_schema(engine, SCHEMA)

    # Use DROP ... CASCADE once up front rather than pandas' to_sql
    # if_exists="replace" (which fails once a dbt view depends on this
    # table -- Postgres won't DROP TABLE if anything references it,
    # without CASCADE). dbt_run always runs again right after ingestion
    # in the DAG, so any dropped downstream view gets recreated.
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_NAME} CASCADE")

    logger.info("Downloading Land Registry Price Paid Data from %s", SOURCE_URL)
    total_rows = 0

    with requests.get(SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        # Stream to a temp file on disk instead of holding the whole
        # response body (resp.text) in memory.
        with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            for chunk in pd.read_csv(
                tmp.name, header=None, names=COLUMNS, chunksize=CHUNK_SIZE
            ):
                before = len(chunk)
                chunk = chunk.dropna(subset=["postcode", "price"])
                dropped = before - len(chunk)
                if dropped:
                    logger.info("Dropped %d rows missing postcode/price in this chunk", dropped)

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