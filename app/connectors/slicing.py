"""Date-window slicing and incremental cursor arithmetic (§12).

Slicing is the load-bearing idea borrowed from Airbyte's datetime cursor. It buys
three things at once:

  bounded responses   a year of GA4 `pagePath` data in one request is enormous
                      and is exactly how the "connector hangs on large results"
                      failure mode happens. A 7-day window cannot.
  resumability        a slice that commits advances the cursor, so a backfill
                      interrupted at month nine resumes at month nine.
  fair progress       "slice 34 of 52" is a real progress bar; "syncing…" is not.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from app.connectors.base import StreamSlice


def date_slices(start: date, end: date, step_days: int) -> list[StreamSlice]:
    """Split [start, end] inclusive into consecutive windows of at most step_days.

    Oldest first, so a partial backfill leaves a contiguous history behind it
    rather than a hole in the middle.
    """
    if start > end:
        return []
    step = max(1, step_days)
    slices: list[StreamSlice] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=step - 1), end)
        slices.append(StreamSlice(start_date=cursor, end_date=window_end))
        cursor = window_end + timedelta(days=1)
    return slices


@dataclass(slots=True)
class SyncWindow:
    start: date
    end: date
    is_backfill: bool
    reason: str

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


def parse_cursor(value: str | None) -> date | None:
    """Cursors are stored as ISO dates; tolerate the compact form providers use."""
    if not value:
        return None
    text = value.strip()
    try:
        if len(text) == 8 and text.isdigit():  # GA4 returns 20260830
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def resolve_sync_window(
    *,
    cursor_value: str | None,
    today: date,
    backfill_start: date | None = None,
    lookback_days: int = 3,
    default_backfill_days: int = 90,
    max_history_days: int | None = None,
    provider_lag_days: int = 0,
) -> SyncWindow | None:
    """Decide which dates this run should fetch. None means "nothing due".

    First run  → the configured historical range.
    Later runs → from (cursor - lookback) to now, because analytics providers
                 restate recent days. Re-fetching a few days and upserting is the
                 only way those corrections ever land; an append-only cursor
                 would freeze the first (wrong) numbers forever.
    """
    end = today - timedelta(days=max(0, provider_lag_days))

    earliest_allowed = today - timedelta(days=max_history_days) if max_history_days is not None else None

    cursor = parse_cursor(cursor_value)
    if cursor is None:
        start = backfill_start or (today - timedelta(days=default_backfill_days))
        is_backfill = True
        reason = "initial backfill"
    else:
        start = cursor - timedelta(days=max(0, lookback_days))
        is_backfill = False
        reason = f"incremental from cursor {cursor.isoformat()} with {lookback_days}d lookback"

    # Never ask for data the user excluded, or that the provider has discarded.
    if backfill_start and start < backfill_start:
        start = backfill_start
        reason += f"; clamped to configured start {backfill_start.isoformat()}"
    if earliest_allowed and start < earliest_allowed:
        start = earliest_allowed
        reason += f"; clamped to provider retention window ({max_history_days}d)"

    if start > end:
        return None
    return SyncWindow(start=start, end=end, is_backfill=is_backfill, reason=reason)


# --- second-resolution timestamp cursors -----------------------------------------
#
# Record-grain sources (LeadSquared) checkpoint on a UTC timestamp, not a date: a
# date cursor re-fetches up to a day on every run and cannot express "everything
# up to 10:35:12 is persisted". Stored as ISO-8601 with a trailing Z.

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def format_ts(value: datetime) -> str:
    """Naive-or-aware datetime -> canonical UTC cursor string."""
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(microsecond=0).strftime(_TS_FORMAT)


def parse_ts(value: str | None) -> datetime | None:
    """Canonical cursor string -> naive-UTC datetime. None if absent/unparseable.

    A bare date (the legacy date-cursor format) is read as that day's midnight,
    so an old cursor can never look *newer* than it really was.
    """
    if not value:
        return None
    text = value.strip()
    try:
        if len(text) == 10:
            return datetime.fromisoformat(text)
        return datetime.strptime(text[:19].replace(" ", "T"), "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def advance_ts(current: str | None, candidate: datetime | str | None) -> str | None:
    """Monotonic timestamp advance — never moves backwards, same rule as dates."""
    cand = candidate if isinstance(candidate, datetime) else parse_ts(candidate)
    if cand is None:
        return current
    if cand.tzinfo is not None:
        cand = cand.astimezone(UTC).replace(tzinfo=None)
    cur = parse_ts(current)
    if cur is None or cand > cur:
        return format_ts(cand)
    return current


def advance_cursor(current: str | None, candidate: date | str | None) -> str | None:
    """Monotonic cursor advance. Never moves backwards.

    Guards the case where a lookback re-fetch of older days would otherwise
    rewind the cursor and cause the same range to be re-synced forever.
    """
    candidate_date = candidate if isinstance(candidate, date) else parse_cursor(candidate)
    if candidate_date is None:
        return current
    current_date = parse_cursor(current)
    if current_date is None or candidate_date > current_date:
        return candidate_date.isoformat()
    return current
