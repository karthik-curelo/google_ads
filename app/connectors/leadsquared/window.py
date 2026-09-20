"""Deterministic, verified windowed retrieval for LeadSquared (P0: no skipped rows).

What the live API does (all verified against api-in21, 2026-09-19):

* Both retrieval endpoints filter on a *modification* timestamp — activities on
  `ModifiedOn`, leads on `LeadLastModifiedOn` — return a `RecordCount` that is the
  true total for the window regardless of page size, and page with plain
  offset/limit. There is no page token, cursor or continuation field.
* Ordering by `CreatedOn` (what the connector used to send) is not deterministic:
  identical requests returned different rows on different calls whenever many rows
  share a timestamp, so multi-page pulls silently dropped rows (4 `booking_created`
  rows were lost on a 2,858-row burst day).
* Ordering by the record's unique id *is* deterministic (3/3 identical pulls) — but
  even that is not enough on its own. Offset paging over a set that is being
  modified underneath it shifts positions: a row that leaves the window
  mid-pull (because it was just modified, moving its timestamp out of range) makes
  every later row slide up one slot, and one is skipped. `RecordCount` still
  matches afterwards, so a count check cannot see it.

So the strategy is not "paginate better" — it is to avoid multi-page offset
paging over live data at all:

1. A window is only accepted when its whole result fits in ONE page. One request is
   one snapshot: there is no offset to shift, and ties cannot reorder anything.
2. A window whose `RecordCount` exceeds a page is split in time (proportionally to
   the count) until every leaf fits. Emptier stretches cost one call.
3. Every leaf is verified against its own response: rows delivered == the
   `RecordCount` in the same response, ids distinct. A mismatch re-fetches the
   window; it never yields.
4. Only a *one-second interval* that holds more than a page (a bulk job stamping many
   rows within the same second) cannot be split further. That one case pages in
   unique-id order and is accepted only if the count is identical before and after
   the pull and every row is accounted for; otherwise it is retried, then fails.

Windows are CLOSED intervals that SHARE their boundary second (next.start == prev.end).
This is not a style choice. The API takes whole seconds but stores finer timestamps
(it prints `.000`), and `ToDate=12:12:04` means "up to 12:12:04.000". So two adjacent
windows `[00..04]` + `[05..09]` leave the crack (04.000, 05.000) covered by neither:
on the live account the 10-second slice `[00..09]` held 10 leads while the adjacent
halves held 5 + 4 — one lead in ten lost at every boundary in a bulk hour. Sharing the
boundary (`[00..05]` + `[05..09]` = 6 + 4 = 10) closes the crack; a row stamped exactly on
a boundary is returned by both windows and absorbed by the idempotent upsert. (An
earlier check found "0 rows lost at day boundaries" — on quiet days, where one second of
data is usually empty. It proved nothing about a burst.)

The fetcher yields windows chronologically and gap-free, so the caller can persist each
one and advance its checkpoint to that window's end.
"""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from app.connectors import errors as E
from app.core.logging import get_logger

logger = get_logger(__name__)

WINDOW_FMT = "%Y-%m-%d %H:%M:%S"
_SECOND = timedelta(seconds=1)


@dataclass(slots=True)
class Page:
    """One response: the source's total for the window plus the rows on this page."""

    record_count: int
    rows: list[dict[str, Any]]


class PageSource(Protocol):
    """The one thing the fetcher needs from an endpoint."""

    page_size: int

    async def fetch(self, start: datetime, end: datetime, page_index: int, page_size: int) -> Page:
        """Rows with the filter timestamp in [start, end] (inclusive seconds),
        ordered by the record's UNIQUE id, 1-based `page_index`."""

    def row_id(self, row: dict[str, Any]) -> str | None:
        """The record's unique id, or None if the row carries none."""


@dataclass(slots=True)
class Window:
    """A verified-complete window."""

    start: datetime
    end: datetime
    rows: list[dict[str, Any]]
    source_count: int
    fetched_rows: int
    unmappable: int = 0
    api_calls: int = 0
    split: bool = False
    paged_fallback: bool = False
    distinct: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self.distinct = len(self.rows)


class WindowFetcher:
    def __init__(
        self,
        source: PageSource,
        *,
        fill: float = 0.6,
        max_fanout: int = 8,
        integrity_retries: int = 2,
        max_span: timedelta = timedelta(days=365),
    ) -> None:
        self.source = source
        self.page_size = source.page_size
        self.fill = fill
        self.max_fanout = max_fanout
        self.integrity_retries = integrity_retries
        self.max_span = max_span
        self.calls = 0  # every request issued, for the caller's api_calls accounting
        self._calls_reported = 0

    # --- public ---------------------------------------------------------------
    async def windows(
        self, start: datetime, end: datetime, *, initial_span: timedelta | None = None
    ) -> AsyncIterator[Window]:
        """Yield verified windows that cover [start, end] with no gap.

        Consecutive windows share their boundary second (see the module docstring), so
        the union is the whole closed interval, including the sub-second cracks.
        `initial_span` is the first window's length. Incremental runs pass the
        whole range (one request for a quiet stream); a backfill starts small and
        the length then follows the observed density.
        """
        if start > end:
            return
        cursor = start
        span = initial_span or max(end - start, _SECOND)
        while True:
            w_end = min(cursor + max(span, _SECOND), end)
            window_len = max(w_end - cursor, _SECOND)
            total = 0
            last_end = cursor
            async for leaf in self._resolve(cursor, w_end):
                total += leaf.source_count
                last_end = leaf.end
                yield leaf
            if last_end >= end:
                return
            cursor = last_end  # SHARED boundary: the next window starts where this one ended
            span = self._next_span(window_len, total)

    # --- internals ------------------------------------------------------------
    def _next_span(self, window_len: timedelta, rows: int) -> timedelta:
        """Size the next top-level window from the density just observed, so a
        long backfill costs roughly one request per page of data instead of
        re-splitting from scratch every time."""
        if rows == 0:
            return min(window_len * 4, self.max_span)  # empty stretch: stride out fast
        density = rows / max(1.0, window_len.total_seconds())
        target = self.fill * self.page_size / density
        return min(max(timedelta(seconds=target), _SECOND), self.max_span)

    def _take_calls(self) -> int:
        n = self.calls - self._calls_reported
        self._calls_reported = self.calls
        return n

    async def _fetch(self, a: datetime, b: datetime, page_index: int, page_size: int) -> Page:
        self.calls += 1
        return await self.source.fetch(a, b, page_index, page_size)

    def _check_single_page(self, page: Page) -> str | None:
        """Why this response cannot be trusted as the whole window, or None."""
        n = page.record_count
        if len(page.rows) != n:
            return f"source reported {n} rows but the same response carried {len(page.rows)}"
        ids = [self.source.row_id(r) for r in page.rows]
        mapped = [i for i in ids if i is not None]
        if len(set(mapped)) != len(mapped):
            return f"{len(mapped) - len(set(mapped))} duplicate id(s) within one response"
        return None

    def _leaf(self, a: datetime, b: datetime, page: Page, *, split: bool) -> Window:
        rows, unmappable, seen = [], 0, set()
        for r in page.rows:
            rid = self.source.row_id(r)
            if rid is None:
                unmappable += 1
            elif rid not in seen:
                seen.add(rid)
                rows.append(r)
        return Window(
            start=a,
            end=b,
            rows=rows,
            source_count=page.record_count,
            fetched_rows=len(page.rows),
            unmappable=unmappable,
            api_calls=self._take_calls(),
            split=split,
        )

    async def _resolve(self, a: datetime, b: datetime, *, split: bool = False) -> AsyncIterator[Window]:
        problem = None
        for attempt in range(1 + self.integrity_retries):
            page = await self._fetch(a, b, 1, self.page_size)
            n = page.record_count
            if n > self.page_size:
                break  # too big for one page: split below
            problem = self._check_single_page(page)
            if problem is None:
                yield self._leaf(a, b, page, split=split)
                return
            logger.warning("window %s..%s attempt %d not trustworthy: %s", a, b, attempt + 1, problem)
        else:
            raise E.reconciliation_error(
                f"LeadSquared window {a:%Y-%m-%d %H:%M:%S}..{b:%Y-%m-%d %H:%M:%S} could not be verified "
                f"after {1 + self.integrity_retries} attempts: {problem}",
                technical_details={"window": [a.isoformat(), b.isoformat()], "problem": problem},
            )

        secs = int((b - a).total_seconds())
        if secs <= 1:  # a one-second (or zero-width) interval cannot be split any further
            yield await self._paged_fallback(a, b, n)
            return

        parts = max(2, min(self.max_fanout, math.ceil(n / (self.fill * self.page_size)), secs))
        step = math.ceil(secs / parts)
        cur = a
        while cur < b:
            nxt = min(cur + timedelta(seconds=step), b)  # children share their boundary second
            async for leaf in self._resolve(cur, nxt, split=True):
                yield leaf
            cur = nxt

    async def _paged_fallback(self, a: datetime, b: datetime, expected: int) -> Window:
        """One one-second interval holds more rows than a page. Page in unique-id order and
        accept only a pull that is provably whole (see module docstring, point 4)."""
        problem = "no attempt made"
        for attempt in range(1 + self.integrity_retries):
            by_id: dict[str, dict[str, Any]] = {}
            fetched = unmappable = 0
            first_count: int | None = None
            last_count = expected
            index = 1
            while True:
                page = await self._fetch(a, b, index, self.page_size)
                if first_count is None:
                    first_count = page.record_count
                # Past the last page LeadSquared answers RecordCount=0 with no rows
                # (live-verified); that is the end marker, not a count change.
                if page.rows:
                    last_count = page.record_count
                fetched += len(page.rows)
                for r in page.rows:
                    rid = self.source.row_id(r)
                    if rid is None:
                        unmappable += 1
                    else:
                        by_id.setdefault(rid, r)
                # +2: tolerate a source that briefly reports a shorter last page
                if len(page.rows) < self.page_size or index > math.ceil(first_count / self.page_size) + 2:
                    break
                index += 1

            if last_count != first_count:
                problem = f"count changed during the pull ({first_count} -> {last_count})"
            elif fetched != first_count or len(by_id) + unmappable != first_count:
                problem = (
                    f"expected {first_count} rows, got {fetched} delivered / {len(by_id)} distinct "
                    f"(+{unmappable} without an id)"
                )
            else:
                return Window(
                    start=a,
                    end=b,
                    rows=list(by_id.values()),
                    source_count=first_count,
                    fetched_rows=fetched,
                    unmappable=unmappable,
                    api_calls=self._take_calls(),
                    split=True,
                    paged_fallback=True,
                )
            logger.warning("one-second window %s attempt %d rejected: %s", a, attempt + 1, problem)

        raise E.reconciliation_error(
            f"LeadSquared one-second window {a:%Y-%m-%d %H:%M:%S} holds more than one page and could "
            f"not be paged consistently after {1 + self.integrity_retries} attempts: {problem}",
            technical_details={"window": [a.isoformat(), b.isoformat()], "problem": problem},
        )
