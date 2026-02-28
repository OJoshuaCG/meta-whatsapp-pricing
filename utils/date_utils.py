"""Date parsing and validation utilities."""

from __future__ import annotations

from datetime import date, timedelta


def parse_date(value: str) -> date:
    """Parse an ISO-8601 date string (YYYY-MM-DD).

    Raises:
        ValueError: If the string is not a valid YYYY-MM-DD date.
    """
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD."
        ) from exc


def day_before(d: date) -> date:
    """Return the calendar day immediately before *d*."""
    return d - timedelta(days=1)
