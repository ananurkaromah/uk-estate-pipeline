"""Failure alerts for Airflow tasks, sent to a Discord webhook.

ALERT_WEBHOOK_URL is read from the environment. If it is not set, alerts are
skipped and the task failure itself is unaffected.
"""
import logging
import os

import requests

logger = logging.getLogger(__name__)


def notify_failure(context):
    """Airflow on_failure_callback: post a short message to Discord."""
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        logger.warning("ALERT_WEBHOOK_URL is not set; skipping failure alert")
        return

    ti = context["task_instance"]
    exception = str(context.get("exception") or "no exception message")[:500]
    message = (
        f"**Task failed:** `{ti.dag_id}.{ti.task_id}`\n"
        f"Run: `{context.get('run_id')}` (attempt {ti.try_number})\n"
        f"Error: `{exception}`\n"
        f"Log: {ti.log_url}"
    )
    try:
        resp = requests.post(url, json={"content": message[:1900]}, timeout=10)
        resp.raise_for_status()
    except requests.RequestException:
        # Never let a broken alert channel hide the real task failure.
        logger.exception("Could not send the failure alert")