"""
nexusflow/adapters/sendgrid.py
SendGrid MCP Adapter — L1 Collector Agent tool.
Pure httpx, no SendGrid SDK dependency.
Handles offset/limit pagination, 429 rate limiting, and 401 auth failure.
API reference: https://docs.sendgrid.com/api-reference
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import SendGridBounce, SendGridStat
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

SENDGRID_API = "https://api.sendgrid.com/v3"
_PAGE_SIZE = 500


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class SendGridAdapter:
    """
    Fetches bounce records and delivery statistics from the SendGrid REST API v3,
    and dispatches plain-text notification emails via the Mail Send endpoint.
    Returns typed ``SendGridBounce`` and ``SendGridStat`` objects —
    no raw dicts escape this adapter.

    Authentication uses the ``Authorization: Bearer <API_KEY>`` header.
    The key is read from ``settings.sendgrid_api_key``
    (env var ``SENDGRID_API_KEY``) unless overridden via the constructor.

    The default sender address falls back to ``settings.sendgrid_from_email``
    (env var ``SENDGRID_FROM_EMAIL``) when ``from_email`` is not supplied
    per-call.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.sendgrid_api_key
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise ``CollectorError`` immediately if the API key is absent."""
        if not self._api_key:
            raise CollectorError(
                "SENDGRID_API_KEY not configured", retry_after=0
            )

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into ``CollectorError`` with useful messages.
        Must be called *before* ``resp.raise_for_status()``.
        """
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("X-RateLimit-Reset", 60))
            raise CollectorError(
                f"SendGrid rate limit hit ({context})",
                retry_after=retry_after,
            )
        if resp.status_code == 401:
            raise CollectorError(
                "SendGrid authentication failed — check SENDGRID_API_KEY",
                retry_after=0,
            )
        resp.raise_for_status()

    @staticmethod
    def _unix_to_dt(ts: int | float) -> datetime:
        """Convert a Unix timestamp (seconds) to a UTC-aware ``datetime``."""
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    @staticmethod
    def _build_bounce(raw: dict[str, Any]) -> SendGridBounce:
        """Coerce a single raw bounce dict into a typed ``SendGridBounce``."""
        return SendGridBounce(
            email=raw["email"],
            created_at=datetime.fromtimestamp(raw["created"], tz=timezone.utc),
            reason=raw.get("reason", ""),
            status=raw.get("status", ""),
            source_ip=raw.get("ip", ""),
        )

    @staticmethod
    def _build_stat(raw: dict[str, Any]) -> SendGridStat:
        """
        Coerce a single date-bucket from the stats response into a typed
        ``SendGridStat``.  The API returns one top-level object per date,
        with metrics nested inside ``stats[0].metrics``.
        """
        metrics: dict[str, Any] = {}
        stats_list: list[dict[str, Any]] = raw.get("stats", [])
        if stats_list:
            metrics = stats_list[0].get("metrics", {})

        return SendGridStat(
            date=raw["date"],
            requests=metrics.get("requests", 0),
            delivered=metrics.get("delivered", 0),
            opens=metrics.get("opens", 0),
            clicks=metrics.get("clicks", 0),
            bounces=metrics.get("bounces", 0),
            spam_reports=metrics.get("spam_reports", 0),
            unsubscribes=metrics.get("unsubscribes", 0),
        )

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_bounces(
        self,
        lookback_hours: int = 72,
    ) -> list[SendGridBounce]:
        """
        Fetch email bounce records created within the last ``lookback_hours``.

        Uses ``GET /suppression/bounces`` with ``offset``/``limit`` pagination
        (500 records per page).  All pages are consumed until an empty page is
        returned.

        Parameters
        ----------
        lookback_hours:
            How far back (in hours) to include bounces by their creation time.
            Defaults to 72 hours.

        Returns
        -------
        list[SendGridBounce]
            Typed bounce objects whose ``created_at`` falls within the lookback
            window, ordered as returned by the API (most recent first).

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        start_time = int(
            (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp()
        )
        end_time = int(datetime.now(timezone.utc).timestamp())

        bounces: list[SendGridBounce] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {
                    "start_time": start_time,
                    "end_time": end_time,
                    "limit": _PAGE_SIZE,
                    "offset": offset,
                }

                resp = await client.get(
                    f"{SENDGRID_API}/suppression/bounces",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, "fetch_bounces")

                page: list[dict[str, Any]] = resp.json()

                for raw in page:
                    bounces.append(self._build_bounce(raw))

                # If fewer results than the page size were returned, we are done
                if len(page) < _PAGE_SIZE:
                    break

                offset += _PAGE_SIZE

        logger.info(
            "SendGridAdapter: fetched %d bounces (last %dh)",
            len(bounces),
            lookback_hours,
        )
        return bounces

    async def fetch_stats(
        self,
        lookback_days: int = 7,
        aggregated_by: str = "day",
    ) -> list[SendGridStat]:
        """
        Fetch global email delivery statistics for the last ``lookback_days``.

        Uses ``GET /stats`` with ``start_date``, ``end_date``, and
        ``aggregated_by`` query parameters.  The endpoint returns one object
        per time bucket (day / week / month) so no pagination is required.

        Parameters
        ----------
        lookback_days:
            Number of calendar days to include, counting back from today.
            Defaults to 7.
        aggregated_by:
            Bucket granularity — ``"day"``, ``"week"``, or ``"month"``.
            Defaults to ``"day"``.

        Returns
        -------
        list[SendGridStat]
            Typed stat objects, one per time bucket, in chronological order
            as returned by the API.

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        today = datetime.now(timezone.utc).date()
        start_date = (today - timedelta(days=lookback_days)).isoformat()
        end_date = today.isoformat()

        params: dict[str, Any] = {
            "start_date": start_date,
            "end_date": end_date,
            "aggregated_by": aggregated_by,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{SENDGRID_API}/stats",
                headers=self._headers,
                params=params,
            )
            self._raise_for_status(resp, "fetch_stats")

            data: list[dict[str, Any]] = resp.json()

        stats = [self._build_stat(raw) for raw in data]

        logger.info(
            "SendGridAdapter: fetched %d stat buckets (last %dd, aggregated_by=%s)",
            len(stats),
            lookback_days,
            aggregated_by,
        )
        return stats

    async def send_notification(
        self,
        to_email: str,
        subject: str,
        body: str,
        from_email: str | None = None,
    ) -> bool:
        """
        Send a plain-text notification email via ``POST /mail/send``.

        Used by the Executor Agent to dispatch decision notifications after a
        human approval is recorded.

        Parameters
        ----------
        to_email:
            Recipient email address.
        subject:
            Email subject line.
        body:
            Plain-text email body.
        from_email:
            Sender address.  Falls back to ``settings.sendgrid_from_email``
            (env var ``SENDGRID_FROM_EMAIL``) when ``None``.

        Returns
        -------
        bool
            ``True`` when SendGrid accepts the message (HTTP 202), ``False``
            on any non-fatal failure (e.g. 400 bad request).  Fatal errors
            (401, 429) still raise ``CollectorError`` so the caller can
            distinguish authentication/throttle failures from soft send errors.

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        resolved_from = from_email or settings.sendgrid_from_email
        if not resolved_from:
            raise CollectorError(
                "SENDGRID_FROM_EMAIL not configured", retry_after=0
            )

        payload: dict[str, Any] = {
            "personalizations": [
                {"to": [{"email": to_email}]},
            ],
            "from": {"email": resolved_from},
            "subject": subject,
            "content": [
                {"type": "text/plain", "value": body},
            ],
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{SENDGRID_API}/mail/send",
                headers=self._headers,
                json=payload,
            )

        # 429 / 401 are always fatal — delegate to the shared helper
        if resp.status_code in (401, 429):
            self._raise_for_status(resp, "send_notification")

        if resp.status_code == 202:
            logger.info(
                "SendGridAdapter: notification sent to %s (subject=%r)",
                to_email,
                subject,
            )
            return True

        logger.warning(
            "SendGridAdapter: send_notification failed — HTTP %d body=%s",
            resp.status_code,
            resp.text[:200],
        )
        return False
