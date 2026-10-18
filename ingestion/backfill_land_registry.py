"""One-off backfill of historical Price Paid Data from HM Land Registry's yearly files.

Usage (inside the Airflow container):
    python /opt/airflow/ingestion/backfill_land_registry.py 2025 2024

Insert-only: rows whose transaction_id already exists in bronze.land_registry_pp
are left untouched, so the monthly UPSERT stays the source of truth for amendments.
Yearly files have no record_status column (it exists in the monthly file only), so
backfilled rows are marked 'A'. Safe to re-run: existing rows are skipped.
"""
import argparse
import io
import logging
import re
import tempfile
import time

import pandas as pd
import requests
from db import ensure_schema, get_engine
from land_registry import SCHEMA, TABLE_NAME, ensure_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://price-paid-data.publicdata.landregistry.gov.uk"

# 15 columns: the monthly file's 16th column, record_status, is monthly-only.
COLUMNS = [
    "transaction_id", "price", "date_of_transfer", "postcode", "property_type",
    "old_new", "duration", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type",
]
CHUNK_SIZE = 100_000


def insert_chunk(engine, chunk: pd.DataFrame) -> int:
    """COPY a chunk into a temp table, then INSERT ... ON CONFLICT DO NOTHING."""
    buf = io.StringIO()
    chunk.to_csv(buf, header=False, index=False)
    buf.seek(0)
    cols = ", ".join(COLUMNS)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(
                f"CREATE TEMP TABLE land_registry_stage "
                f"(LIKE {SCHEMA}.{TABLE_NAME} INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            cur.copy_expert(f"COPY land_registry_stage ({cols}) FROM STDIN WITH (FORMAT csv)", buf)
            cur.execute(
                f"INSERT INTO {SCHEMA}.{TABLE_NAME} ({cols}, record_status) "
                f"SELECT {cols}, 'A' FROM land_registry_stage "
                "ON CONFLICT (transaction_id) DO NOTHING"
            )
            inserted = cur.rowcount
        raw.commit()
        return inserted
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def backfill_file(engine, name: str) -> None:
    url = f"{BASE_URL}/pp-{name}.csv"
    logger.info("Downloading %s", url)
    started = time.monotonic()
    read_total = inserted_total = 0

    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(block)
            tmp.flush()

            for chunk in pd.read_csv(
                tmp.name, header=None, dtype=str, keep_default_na=False, chunksize=CHUNK_SIZE
            ):
                if chunk.shape[1] != len(COLUMNS):
                    raise ValueError(f"{url}: expected {len(COLUMNS)} columns, found {chunk.shape[1]}")
                chunk.columns = COLUMNS
                inserted_total += insert_chunk(engine, chunk)
                read_total += len(chunk)
                logger.info("%s: %d rows read, %d inserted so far", name, read_total, inserted_total)

    logger.info(
        "%s finished in %.0fs: %d rows read, %d new, %d already present",
        name, time.monotonic() - started, read_total, inserted_total, read_total - inserted_total,
    )


def main():
    parser = argparse.ArgumentParser(description="Backfill Price Paid Data from the yearly files")
    parser.add_argument("files", nargs="+", help="e.g. 2025 2024, or 2023-part1 2023-part2")
    args = parser.parse_args()
    for name in args.files:
        if not re.fullmatch(r"\d{4}(-part[12])?", name):
            parser.error(f"unexpected file name '{name}'")

    engine = get_engine()
    ensure_schema(engine, SCHEMA)
    ensure_table(engine)
    for name in args.files:
        backfill_file(engine, name)


if __name__ == "__main__":
    main()