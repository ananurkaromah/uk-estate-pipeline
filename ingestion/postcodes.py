"""Look up postcodes.io for postcodes that the ONS NSPL does not know.

ONS is the primary geography source and postcodes.io is only a fallback, so only
postcodes missing from bronze.ons_postcode (live or terminated) are looked up.
Results are collected in memory, then the table is replaced in one transaction.
"""
import logging

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

from db import ensure_schema, get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BULK_LOOKUP_URL = "https://api.postcodes.io/postcodes"
BATCH_SIZE = 100
REQUEST_TIMEOUT = (10, 60)
LOG_EVERY = 20
SCHEMA = "bronze"
TABLE_NAME = "postcode_io"
MIN_ONS_ROWS = 1_000_000  # the full UK file has ~2.7M rows

DDL = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
        postcode text,
        latitude double precision,
        longitude double precision,
        region text,
        admin_district text
    )
"""

MISSING_FROM_ONS_SQL = """
    SELECT DISTINCT upper(trim(l.postcode)) AS postcode
    FROM bronze.land_registry_pp l
    LEFT JOIN bronze.ons_postcode o
           ON upper(trim(o.postcode)) = upper(trim(l.postcode))
    WHERE l.postcode IS NOT NULL
      AND o.postcode IS NULL
"""


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def run():
    engine = get_engine()
    ensure_schema(engine, SCHEMA)

    with engine.begin() as conn:
        conn.exec_driver_sql(DDL)
        ons_rows = conn.exec_driver_sql("SELECT count(*) FROM bronze.ons_postcode").scalar()
    if ons_rows < MIN_ONS_ROWS:
        raise ValueError(
            f"bronze.ons_postcode has only {ons_rows} rows; it must be loaded before enrichment, "
            "otherwise nearly every postcode would be looked up."
        )

    postcodes = pd.read_sql(MISSING_FROM_ONS_SQL, engine)["postcode"].tolist()
    total_batches = (len(postcodes) + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info("%d postcodes are missing from ONS; looking them up in %d batches", len(postcodes), total_batches)

    session = _build_session()
    all_rows = []

    for batch_num, batch in enumerate(_chunks(postcodes, BATCH_SIZE), start=1):
        resp = session.post(BULK_LOOKUP_URL, json={"postcodes": batch}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        for item in resp.json().get("result", []):
            result = item.get("result")
            if result:
                all_rows.append({
                    "postcode": result["postcode"],
                    "latitude": result["latitude"],
                    "longitude": result["longitude"],
                    "region": result.get("region"),
                    "admin_district": result.get("admin_district"),
                })

        if batch_num % LOG_EVERY == 0 or batch_num == total_batches:
            logger.info("Processed batch %d/%d (%d resolved so far)", batch_num, total_batches, len(all_rows))

    if postcodes and not all_rows:
        logger.warning("None of the %d looked-up postcodes were resolved by postcodes.io", len(postcodes))

    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA}.{TABLE_NAME}")
        if all_rows:
            pd.DataFrame(all_rows).to_sql(
                TABLE_NAME, conn, schema=SCHEMA, if_exists="append", index=False, chunksize=10_000
            )

    logger.info("Finished: resolved %d/%d postcodes into %s.%s", len(all_rows), len(postcodes), SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()