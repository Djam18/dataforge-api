"""Execution failure alerts — Slack webhook and email notifications.

Learning point: alerting should be fire-and-forget. The alert itself
failing must never crash the pipeline worker or retry loop.

Pattern: wrap every alert call in try/except, log the failure, move on.
The pipeline status is the source of truth — alerts are best-effort.

Slack incoming webhooks are the easiest integration:
  1. Create a Slack app → Add incoming webhook → Copy URL
  2. Set SLACK_WEBHOOK_URL env var
  3. Done. No OAuth, no tokens to rotate.
"""

import os
import json
import logging
import smtplib
import urllib.request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL_FROM  = os.environ.get("ALERT_EMAIL_FROM", "")
ALERT_EMAIL_TO    = os.environ.get("ALERT_EMAIL_TO", "")
SMTP_HOST         = os.environ.get("SMTP_HOST", "localhost")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASS         = os.environ.get("SMTP_PASS", "")


def _post_slack(payload: dict) -> None:
    """POST a JSON payload to the configured Slack incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        return
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
        resp.read()


def _send_email(subject: str, body: str) -> None:
    """Send a plain-text alert email via SMTP."""
    if not all([ALERT_EMAIL_FROM, ALERT_EMAIL_TO, SMTP_HOST]):
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_FROM
    msg["To"] = ALERT_EMAIL_TO
    msg.attach(MIMEText(body, "plain"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=5) as server:
        if SMTP_USER and SMTP_PASS:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(ALERT_EMAIL_FROM, [ALERT_EMAIL_TO], msg.as_string())


def alert_execution_failed(
    pipeline_id: int,
    pipeline_name: str,
    execution_id: int,
    error: str,
) -> None:
    """Fire alerts when a pipeline execution fails.

    Called from the Celery task after an unrecoverable failure.
    Both Slack and email are attempted independently — one failing
    does not prevent the other.
    """
    short_error = error[:300] if len(error) > 300 else error

    # Slack
    try:
        _post_slack({
            "text": f":red_circle: *Pipeline failed* — `{pipeline_name}` (id={pipeline_id})",
            "attachments": [
                {
                    "color": "danger",
                    "fields": [
                        {"title": "Execution ID", "value": str(execution_id), "short": True},
                        {"title": "Error", "value": short_error, "short": False},
                    ],
                }
            ],
        })
    except Exception as exc:
        logger.warning("[alerts] Slack alert failed: %s", exc)

    # Email
    try:
        _send_email(
            subject=f"[DataForge] Pipeline failed: {pipeline_name}",
            body=(
                f"Pipeline '{pipeline_name}' (id={pipeline_id}) failed.\n\n"
                f"Execution ID: {execution_id}\n\n"
                f"Error:\n{error}"
            ),
        )
    except Exception as exc:
        logger.warning("[alerts] Email alert failed: %s", exc)


def alert_execution_recovered(
    pipeline_id: int,
    pipeline_name: str,
    execution_id: int,
) -> None:
    """Fire a recovery alert when a previously-failing pipeline succeeds."""
    try:
        _post_slack({
            "text": (
                f":large_green_circle: *Pipeline recovered* — "
                f"`{pipeline_name}` (id={pipeline_id}, exec={execution_id})"
            ),
        })
    except Exception as exc:
        logger.warning("[alerts] Slack recovery alert failed: %s", exc)
