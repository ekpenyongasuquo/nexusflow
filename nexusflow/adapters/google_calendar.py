"""
nexusflow/adapters/google_calendar.py
Google Calendar MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Google SDK dependency.
Handles pageToken pagination, 429 rate limiting, and 401 auth failure.
API reference: https://developers.google.com/calendar/api/v3/reference/events/list
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import CalendarEvent
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

GCAL_API = "https://www.googleapis.com/calendar/v3"


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class GoogleCalendarAdapter:
    """
    Fetches events from the Google Calendar REST API v3.
    Returns typed ``CalendarEvent`` objects —
    no raw dicts escape this adapter.

    Authentication uses the ``Authorization: Bearer <ACCESS_TOKEN>`` header.
    The token is read from ``settings.google_calendar_access_token``
    (env var ``GOOGLE_CALENDAR_ACCESS_TOKEN``) unless overridden via the
    constructor.

    The default ``calendar_id`` falls back to
    ``settings.google_calendar_id`` (env var ``GOOGLE_CALENDAR_ID``) when
    not supplied per-call; ``"primary"`` is used when that is also unset.
    """

    def __init__(self, access_token: str | None = None) -> None:
        self._token = access_token or settings.google_calendar_access_token
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if the access token is absent."""
        if not self._token:
            raise CollectorError(
                "GOOGLE_CALENDAR_ACCESS_TOKEN not configured", retry_after=0
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """
        Parse a Google Calendar datetime string into a UTC-aware datetime.

        Google returns either a full RFC 3339 timestamp (``2024-01-15T10:00:00Z``)
        or a date-only string (``2024-01-15``) for all-day events.
        Date-only values are converted to midnight UTC.
        """
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        # All-day event — date only, no time component
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into CollectorError with useful messages.
        Must be called *before* resp.raise_for_status().
        """
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"Google Calendar rate limit hit ({context})",
                retry_after=retry_after,
            )
        if resp.status_code == 401:
            raise CollectorError(
                "Google Calendar authentication failed — check GOOGLE_CALENDAR_ACCESS_TOKEN",
                retry_after=0,
            )
        resp.raise_for_status()

    @staticmethod
    def _extract_attendees(raw_attendees: list[dict[str, Any]]) -> list[str]:
        """
        Return a flat list of attendee email addresses from the raw
        ``attendees`` array returned by the API.
        """
        return [a["email"] for a in raw_attendees if "email" in a]

    @staticmethod
    def _extract_organizer(raw_organizer: dict[str, Any]) -> str:
        """
        Return the organizer as ``"Display Name <email>"`` when both fields
        are present, falling back to just the email, then to an empty string.
        """
        email = raw_organizer.get("email", "")
        display = raw_organizer.get("displayName", "")
        if display and email:
            return f"{display} <{email}>"
        return email or display

    def _build_event(self, raw: dict[str, Any]) -> CalendarEvent:
        """Coerce a single raw API event dict into a typed ``CalendarEvent``."""
        start_raw: dict[str, Any] = raw.get("start") or {}
        end_raw: dict[str, Any] = raw.get("end") or {}

        # Google uses "dateTime" for timed events, "date" for all-day events
        start_str = start_raw.get("dateTime") or start_raw.get("date", "")
        end_str = end_raw.get("dateTime") or end_raw.get("date", "")

        return CalendarEvent(
            id=raw["id"],
            title=raw.get("summary", "(no title)"),
            description=raw.get("description") or None,
            status=raw.get("status", "confirmed"),
            start_time=self._parse_dt(start_str) if start_str else datetime.now(timezone.utc),
            end_time=self._parse_dt(end_str) if end_str else datetime.now(timezone.utc),
            organizer=self._extract_organizer(raw.get("organizer") or {}),
            attendees=self._extract_attendees(raw.get("attendees") or []),
            location=raw.get("location") or None,
            html_link=raw.get("htmlLink", ""),
        )

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_events(
        self,
        calendar_id: str = "primary",
        lookback_hours: int = 72,
        max_results: int = 50,
    ) -> list[CalendarEvent]:
        """
        Fetch calendar events that started within the last ``lookback_hours``.

        Uses ``GET /calendars/{calendarId}/events`` with ``pageToken``
        pagination.  All pages are consumed until fewer results than
        ``maxResults`` are returned or no ``nextPageToken`` is present.

        Parameters
        ----------
        calendar_id:
            The Google Calendar ID to query.  Defaults to ``"primary"``
            (the user's main calendar), or ``settings.google_calendar_id``
            when that env var is set.
        lookback_hours:
            How far back (in hours) to include events by their start time.
            Defaults to 72 hours.
        max_results:
            Maximum number of events per API page (1–2500).  Defaults to 50.

        Returns
        -------
        list[CalendarEvent]
            Typed event objects whose ``start_time`` falls within the lookback
            window, in chronological order as returned by the API.

        Raises
        ------
        CollectorError
            On missing token, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        resolved_calendar_id = (
            calendar_id
            if calendar_id != "primary"
            else (settings.google_calendar_id or "primary")
        )

        time_min = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).isoformat()
        time_max = datetime.now(timezone.utc).isoformat()

        params: dict[str, Any] = {
            "timeMin": time_min,
            "timeMax": time_max,
            "maxResults": max_results,
            "singleEvents": "true",
            "orderBy": "startTime",
        }

        events: list[CalendarEvent] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                resp = await client.get(
                    f"{GCAL_API}/calendars/{resolved_calendar_id}/events",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, "fetch_events")

                data: dict[str, Any] = resp.json()

                for raw in data.get("items", []):
                    events.append(self._build_event(raw))

                # Pagination — follow nextPageToken when present
                next_page_token = data.get("nextPageToken")
                if next_page_token:
                    params["pageToken"] = next_page_token
                else:
                    break

        logger.info(
            "GoogleCalendarAdapter: fetched %d events (calendar=%s, last %dh)",
            len(events),
            resolved_calendar_id,
            lookback_hours,
        )
        return events

    async def fetch_upcoming(
        self,
        calendar_id: str = "primary",
        hours_ahead: int = 48,
        max_results: int = 20,
    ) -> list[CalendarEvent]:
        """
        Fetch future calendar events starting from now through the next
        ``hours_ahead`` hours.

        Parameters
        ----------
        calendar_id:
            The Google Calendar ID to query.  Defaults to ``"primary"``.
        hours_ahead:
            How far into the future (in hours) to look for events.
            Defaults to 48 hours.
        max_results:
            Maximum number of events per API page (1–2500).  Defaults to 20.

        Returns
        -------
        list[CalendarEvent]
            Typed event objects for future events only, ordered
            chronologically as returned by the API.

        Raises
        ------
        CollectorError
            On missing token, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        resolved_calendar_id = (
            calendar_id
            if calendar_id != "primary"
            else (settings.google_calendar_id or "primary")
        )

        now = datetime.now(timezone.utc)
        time_min = now.isoformat()
        time_max = (now + timedelta(hours=hours_ahead)).isoformat()

        params: dict[str, Any] = {
            "timeMin": time_min,
            "timeMax": time_max,
            "maxResults": max_results,
            "singleEvents": "true",
            "orderBy": "startTime",
        }

        events: list[CalendarEvent] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                resp = await client.get(
                    f"{GCAL_API}/calendars/{resolved_calendar_id}/events",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, "fetch_upcoming")

                data: dict[str, Any] = resp.json()

                for raw in data.get("items", []):
                    events.append(self._build_event(raw))

                # Pagination — follow nextPageToken when present
                next_page_token = data.get("nextPageToken")
                if next_page_token:
                    params["pageToken"] = next_page_token
                else:
                    break

        logger.info(
            "GoogleCalendarAdapter: fetched %d upcoming events (calendar=%s, next %dh)",
            len(events),
            resolved_calendar_id,
            hours_ahead,
        )
        return events
