"""Enrich distinct postcodes from bronze.land_registry_pp using postcodes.io
(free bulk lookup, no key) into bronze.postcode_io.

The full monthly Land Registry file can have tens of thousands of distinct
postcodes, and postcodes.io's bulk endpoint accepts at most 100 per
request -- so a full run makes hundreds of sequential HTTP calls. A single
transient timeout on any one of those calls used to fail the whole task
with no retry and no visibility into how far it had gotten. Fixed here
with: an HTTP session configured to retry transient failures, a longer
per-request timeout, progress logging every N batches, and incremental
per-batch writes to Postgres (instead of holding everything in memory and
writing once at the end) so a failure partway through doesn't discard
already-resolved postcodes from this run.
"""
import logging

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

from db import ensure_schema, get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BULK_LOOKUP_URL = "https://api.postcodes.io/postcodes"
BATCH_SIZE = 100  # postcodes.io bulk lookup limit per request
REQUEST_TIMEOUT = (10, 60)  # (connect timeout, read timeout) seconds
LOG_EVERY = 20  # log progress every N batches
SCHEMA = "bronze"
TABLE_NAME = "postcode_io"


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,  # 2s, 4s, 8s, 16s, 32s between retries
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

    postcodes = pd.read_sql(
        "SELECT DISTINCT postcode FROM bronze.land_registry_pp WHERE postcode IS NOT NULL",
        engine,
    )["postcode"].tolist()

    total_batches = (len(postcodes) + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info("Looking up %d distinct postcodes across %d batches", len(postcodes), total_batches)

    # Start this run's table fresh; each batch below appends to it.
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_NAME}")

    session = _build_session()
    total_resolved = 0

    for batch_num, batch in enumerate(_chunks(postcodes, BATCH_SIZE), start=1):
        resp = session.post(BULK_LOOKUP_URL, json={"postcodes": batch}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        rows = []
        for item in resp.json().get("result", []):
            result = item.get("result")
            if result:
                rows.append({
                    "postcode": result["postcode"],
                    "latitude": result["latitude"],
                    "longitude": result["longitude"],
                    "region": result.get("region"),
                    "admin_district": result.get("admin_district"),
                })

        if rows:
            pd.DataFrame(rows).to_sql(
                TABLE_NAME, engine, schema=SCHEMA, if_exists="append", index=False
            )
            total_resolved += len(rows)

        if batch_num % LOG_EVERY == 0 or batch_num == total_batches:
            logger.info(
                "Processed batch %d/%d (%d postcodes resolved so far)",
                batch_num, total_batches, total_resolved,
            )

    logger.info("Finished: resolved %d/%d postcodes into %s.%s", total_resolved, len(postcodes), SCHEMA, TABLE_NAME)


if __name__ == "__main__":
    run()