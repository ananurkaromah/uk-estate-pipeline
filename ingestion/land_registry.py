import logging
import tempfile

import pandas as pd
import requests
from sqlalchemy import inspect

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

    inspector = inspect(engine)
    if inspector.has_table(TABLE_NAME, schema=SCHEMA):
        with engine.begin() as conn:
            conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA}.{TABLE_NAME}")
        logger.info("Truncated existing %s.%s", SCHEMA, TABLE_NAME)

    logger.info("Downloading Land Registry Price Paid Data from %s", SOURCE_URL)
    total_rows = 0

    with requests.get(SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            for chunk in pd.read_csv(
                tmp.name, header=None, names=COLUMNS, chunksize=CHUNK_SIZE
            ):
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