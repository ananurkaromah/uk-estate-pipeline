"""Ingest UK HM Land Registry Price Paid Data (monthly rolling CSV) into
bronze, using UPSERT (not truncate) so transaction history accumulates
across runs instead of being replaced every time.

transaction_id is the natural unique key. record_status is preserved
as-is ('A'=Addition, 'C'=Change, 'D'=Delete) -- 'D' rows are kept (soft
delete, for audit traceability) and filtered out downstream in
stg_land_registry.sql, not deleted here.
"""
import logging
import tempfile

import pandas as pd
import requests
from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

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


def ensure_table(engine):
    """Create the table with an explicit transaction_id PRIMARY KEY, once.

    This is the one deliberate exception to the "bronze is schema-less,
    pandas infers it" design: a stable unique key is required for UPSERT
    to be possible at all.
    """
    inspector = inspect(engine)
    if inspector.has_table(TABLE_NAME, schema=SCHEMA):
        return
    with engine.begin() as conn:
        conn.exec_driver_sql(f"""
            CREATE TABLE {SCHEMA}.{TABLE_NAME} (
                transaction_id     TEXT PRIMARY KEY,
                price              NUMERIC,
                date_of_transfer   DATE,
                postcode           TEXT,
                property_type      TEXT,
                old_new            TEXT,
                duration           TEXT,
                paon               TEXT,
                saon               TEXT,
                street             TEXT,
                locality           TEXT,
                town_city          TEXT,
                district           TEXT,
                county             TEXT,
                ppd_category_type  TEXT,
                record_status      TEXT
            )
        """)
    logger.info("Created %s.%s with transaction_id as PRIMARY KEY", SCHEMA, TABLE_NAME)


def upsert_chunk(engine, chunk: pd.DataFrame):
    """INSERT ... ON CONFLICT (transaction_id) DO UPDATE -- new transactions
    get inserted, existing ones (amendments, 'C'/'D' status changes) get
    their values overwritten in place."""
    chunk = chunk.where(pd.notnull(chunk), None)  # NaN -> None for SQL NULL
    records = chunk.to_dict(orient="records")
    if not records:
        return

    metadata = MetaData()
    table = Table(TABLE_NAME, metadata, autoload_with=engine, schema=SCHEMA)
    stmt = pg_insert(table).values(records)
    update_cols = {
        c.name: stmt.excluded[c.name] for c in table.columns if c.name != "transaction_id"
    }
    stmt = stmt.on_conflict_do_update(index_elements=["transaction_id"], set_=update_cols)

    with engine.begin() as conn:
        conn.execute(stmt)


def run():
    engine = get_engine()
    ensure_schema(engine, SCHEMA)
    ensure_table(engine)

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
                upsert_chunk(engine, chunk)
                total_rows += len(chunk)
                logger.info("Upserted chunk (%d rows processed so far)", total_rows)

    logger.info("Finished upserting %d rows into %s.%s (existing table grows/updates, not replaced)", total_rows, SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()
