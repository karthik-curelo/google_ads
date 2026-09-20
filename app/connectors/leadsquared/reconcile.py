"""Source vs warehouse reconciliation and the deletion sweep.

LeadSquared exposes no "deleted since" feed: a deleted activity or lead simply stops
appearing, and the incremental sync — which only reads what exists — can never see
the absence. So the warehouse is upsert *history*. This module finds deletions the
only reliable way available and records them WITHOUT destroying history:

    1. Compare, per time window, the source's own count with the warehouse's count of
       live rows whose `source_modified_on` falls in the same window. Once the
       incremental sync has caught up, a window where the warehouse holds MORE than
       the source has lost rows at the source (a re-modified row would have moved to
       a newer window in both places).
    2. Drill into such windows (halving) until the id lists are small, then diff the
       source's ids against the warehouse's.
    3. Confirm every candidate with an independent by-id lookup (`record_exists`);
       only a definite "not found" tombstones the row (`deleted_at = now`). The row
       is kept; a row that reappears at the source clears its own tombstone on the
       next upsert.

Windows where the warehouse holds FEWER rows than the source are reported as
`missing` (a sync gap to repair by re-running the sync), never guessed at.

Preconditions are enforced, not assumed: the stream must have a timestamp checkpoint
past the swept range plus the lookback (otherwise "warehouse has more" could just
mean "the sync has not caught up"), and no row in the range may lack
`source_modified_on` (rows written before that column existed until re-fetched).
A safety cap refuses to tombstone an implausibly large share of the range.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update

from app.connectors import errors as E
from app.connectors.base import StreamDefinition
from app.connectors.leadsquared.activity_catalog import event_for_stream_name
from app.connectors.leadsquared.connector import LEADS_STREAM, LeadSquaredCRMConnector
from app.connectors.slicing import parse_ts
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import LeadsquaredActivity, LeadsquaredLead
from app.sync.state import is_ts_state, load_state

logger = get_logger(__name__)

_SECOND = timedelta(seconds=1)


@dataclass
class SweepReport:
    stream: str
    start: datetime
    end: datetime
    windows_checked: int = 0
    windows_equal: int = 0
    missing_windows: list[dict[str, Any]] = field(default_factory=list)  # warehouse < source
    deleted_ids: list[str] = field(default_factory=list)  # confirmed gone at the source
    still_exist_ids: list[str] = field(
        default_factory=list
    )  # in warehouse window, not source window, but exist
    tombstoned: int = 0
    applied: bool = False
    api_calls: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "range": [self.start.isoformat(), self.end.isoformat()],
            "windows_checked": self.windows_checked,
            "windows_equal": self.windows_equal,
            "missing_windows": self.missing_windows,
            "deleted_ids": self.deleted_ids,
            "still_exist_ids": len(self.still_exist_ids),
            "tombstoned": self.tombstoned,
            "applied": self.applied,
            "api_calls": self.api_calls,
        }


class _Warehouse:
    """The destination side of one (connection, stream)."""

    def __init__(self, connection_id: int, stream: StreamDefinition) -> None:
        self.connection_id = connection_id
        self.stream = stream
        if stream.name == LEADS_STREAM:
            self.model, self.id_col = LeadsquaredLead, LeadsquaredLead.prospect_id
        else:
            self.model, self.id_col = LeadsquaredActivity, LeadsquaredActivity.prospect_activity_id

    def _scope(self):
        m = self.model
        conds = [m.connection_id == self.connection_id]
        if self.model is LeadsquaredActivity:
            conds.append(m.stream == self.stream.name)
        return conds

    async def count_live(self, a: datetime, b: datetime) -> int:
        m = self.model
        async with SessionLocal() as s:
            return int(
                (
                    await s.execute(
                        select(func.count()).where(
                            *self._scope(),
                            m.deleted_at.is_(None),
                            m.source_modified_on >= a,
                            m.source_modified_on < b,
                        )
                    )
                ).scalar_one()
            )

    async def live_ids(self, a: datetime, b: datetime) -> set[str]:
        m = self.model
        async with SessionLocal() as s:
            rows = await s.execute(
                select(self.id_col).where(
                    *self._scope(),
                    m.deleted_at.is_(None),
                    m.source_modified_on >= a,
                    m.source_modified_on < b,
                )
            )
            return set(rows.scalars())

    async def count_missing_timestamp(self, a: datetime, b: datetime) -> int:
        m = self.model
        async with SessionLocal() as s:
            return int(
                (
                    await s.execute(
                        select(func.count()).where(
                            *self._scope(), m.deleted_at.is_(None), m.source_modified_on.is_(None)
                        )
                    )
                ).scalar_one()
            )

    async def tombstone(self, ids: list[str], when: datetime) -> int:
        if not ids:
            return 0
        m = self.model
        async with SessionLocal() as s:
            result = await s.execute(
                update(m)
                .where(*self._scope(), self.id_col.in_(ids), m.deleted_at.is_(None))
                .values(deleted_at=when)
            )
            await s.commit()
            return int(result.rowcount or 0)


async def sweep_deletions(
    connector: LeadSquaredCRMConnector,
    stream: StreamDefinition,
    connection_id: int,
    start: datetime,
    end: datetime,
    *,
    apply: bool = False,
    lookback: timedelta = timedelta(hours=24),
    max_tombstone_fraction: float = 0.02,
    always_allowed: int = 25,
    leaf_rows: int = 500,
) -> SweepReport:
    """Find (and with `apply=True`, tombstone) rows deleted at the source in [start, end]."""
    report = SweepReport(stream=stream.name, start=start, end=end)
    wh = _Warehouse(connection_id, stream)

    state = await load_state(connection_id, stream.name)
    if not is_ts_state(state) or parse_ts(state.cursor_value) < end + lookback:
        raise E.invalid_configuration(
            f"{stream.name}: the sync checkpoint ({state.cursor_value}) has not passed the swept range plus "
            f"its {lookback} lookback, so 'warehouse has more than the source' could just mean 'not caught up'. "
            "Run the sync to completion first.",
            provider="leadsquared",
        )
    untimestamped = await wh.count_missing_timestamp(start, end)
    if untimestamped:
        raise E.invalid_configuration(
            f"{stream.name}: {untimestamped} warehouse rows have no source_modified_on (written before the column "
            "existed and not yet re-fetched). Re-run the backfill for this stream before sweeping.",
            provider="leadsquared",
        )

    async def check(a: datetime, b: datetime) -> None:
        source_n = await connector.count_window(stream, a, b)
        report.api_calls += 1
        wh_n = await wh.count_live(a, b)
        report.windows_checked += 1
        if source_n == wh_n:
            report.windows_equal += 1
            return
        if wh_n < source_n:
            report.missing_windows.append(
                {"start": a.isoformat(), "end": b.isoformat(), "source": source_n, "warehouse": wh_n}
            )
            return
        secs = int((b - a).total_seconds())
        if wh_n > leaf_rows and secs > 1:  # too big to diff cheaply: halve and recurse
            mid = a + timedelta(seconds=math.ceil(secs / 2))
            await check(a, mid)  # halves SHARE the boundary second: closed at the source ...
            await check(mid, b)  # ... half-open [a, b) at the warehouse, so nothing is counted twice or lost
            return
        source_ids = await connector.window_ids(stream, a, b)
        report.api_calls += 1
        for rid in sorted(await wh.live_ids(a, b) - source_ids):
            gone = not await connector.record_exists(stream, rid)
            report.api_calls += 1
            (report.deleted_ids if gone else report.still_exist_ids).append(rid)

    # month-sized top-level windows keep the first comparisons meaningful and cheap
    cursor = start
    while cursor < end:
        nxt = min(cursor + timedelta(days=31), end)
        await check(cursor, nxt)
        cursor = nxt

    if apply and report.deleted_ids:
        total_live = await wh.count_live(start, end) or 1
        # A handful of deletions is normal on any range; a large share of the range is
        # far more likely a broken/incomplete sync than real deletions.
        allowed = max(always_allowed, int(total_live * max_tombstone_fraction))
        if len(report.deleted_ids) > allowed:
            raise E.invalid_configuration(
                f"{stream.name}: refusing to tombstone {len(report.deleted_ids)} of {total_live} rows "
                f"(more than the {allowed} allowed: {always_allowed} or {max_tombstone_fraction:.0%} of the "
                "range). That is far more than deletions normally account for — "
                "investigate (an incomplete sync?) before forcing this.",
                provider="leadsquared",
            )
        report.tombstoned = await wh.tombstone(report.deleted_ids, datetime.now(UTC))
        report.applied = True
    return report


@dataclass
class CountRow:
    stream: str
    source_total: int
    warehouse_total: int
    warehouse_live: int
    tombstoned: int
    checkpoint: str | None

    @property
    def difference(self) -> int:
        return self.source_total - self.warehouse_live


async def source_vs_warehouse(
    connector: LeadSquaredCRMConnector,
    streams: list[StreamDefinition],
    connection_id: int,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[CountRow]:
    """Source total vs warehouse total for each stream over the SAME range.

    The source count is the API's own `RecordCount` for [since, until]. The warehouse
    count is rows for the connection/stream whose `source_modified_on` lies in that same
    range (live = not tombstoned) — or, with no range given, every row. Comparing like
    with like matters: a warehouse that only covers part of history must not be measured
    against the source's all-time total. Any remaining difference is expected to be sync
    lag (rows changed since the last checkpoint) or source deletions not yet swept — the
    report gives the numbers so it can be judged.
    """
    ranged = since is not None or until is not None
    since = since or datetime(2000, 1, 1)
    until = until or datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    out: list[CountRow] = []
    for stream in streams:
        wh = _Warehouse(connection_id, stream)
        m = wh.model
        scope = wh._scope()
        if ranged:
            scope = [*scope, m.source_modified_on >= since, m.source_modified_on <= until]
        async with SessionLocal() as s:
            total = int((await s.execute(select(func.count()).where(*scope))).scalar_one())
            live = int(
                (await s.execute(select(func.count()).where(*scope, m.deleted_at.is_(None)))).scalar_one()
            )
        state = await load_state(connection_id, stream.name)
        out.append(
            CountRow(
                stream=stream.name,
                source_total=await connector.count_window(stream, since, until),
                warehouse_total=total,
                warehouse_live=live,
                tombstoned=total - live,
                checkpoint=state.cursor_value if is_ts_state(state) else None,
            )
        )
    return out


def is_activity_stream(stream: StreamDefinition) -> bool:
    return event_for_stream_name(stream.name) is not None
