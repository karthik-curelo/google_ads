"""Per-stream cursor: load before a stream runs, commit only after its rows land.

The Airbyte state rule (§12) that makes a resumed sync *correct* rather than
merely convenient: the cursor is advanced only once the destination has
committed the records the new value covers. A crash between write and commit
re-fetches a window; it never skips one.

State is an opaque JSON blob to everything except the connector that wrote it.
This module only owns the one field the platform genuinely shares — the date
cursor — and the monotonic rule that stops a lookback re-fetch from rewinding it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.connectors.slicing import advance_cursor, parse_cursor
from app.core.database import SessionLocal
from app.models import SyncState


@dataclass(slots=True)
class StreamState:
    connection_id: int
    stream: str
    cursor_field: str | None = None
    cursor_value: str | None = None
    blob: dict[str, Any] = field(default_factory=dict)
    records_synced: int = 0

    @property
    def cursor_date(self) -> date | None:
        return parse_cursor(self.cursor_value)


async def load_state(connection_id: int, stream: str) -> StreamState:
    async with SessionLocal() as session:
        row = (
            await session.execute(
                select(SyncState).where(SyncState.connection_id == connection_id, SyncState.stream == stream)
            )
        ).scalar_one_or_none()
        if row is None:
            return StreamState(connection_id=connection_id, stream=stream)
        return StreamState(
            connection_id=connection_id,
            stream=stream,
            cursor_field=row.cursor_field,
            cursor_value=row.cursor_value,
            blob=dict(row.state or {}),
            records_synced=row.records_synced,
        )


async def commit_state(
    state: StreamState,
    *,
    reached: date | str | None,
    added_records: int = 0,
    blob: dict[str, Any] | None = None,
) -> StreamState:
    """Advance the cursor to `reached` (monotonic) and persist. Call after write.

    Two code paths can commit state for the same (connection, stream) at once — a
    manual "Sync now" landing while a scheduled run is mid-flight. Both would
    SELECT-then-INSERT and the second hits the UNIQUE constraint, so the loser of
    that race re-reads and updates the row the winner created. The cursor is
    advanced against whatever is in the DB *now*, not just this run's stale
    in-memory value, so a concurrent further-ahead write is never rewound.
    """
    new_cursor = advance_cursor(state.cursor_value, reached)

    for attempt in (1, 2):
        async with SessionLocal() as session:
            row = (
                await session.execute(
                    select(SyncState).where(
                        SyncState.connection_id == state.connection_id,
                        SyncState.stream == state.stream,
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                row = SyncState(
                    connection_id=state.connection_id,
                    stream=state.stream,
                    cursor_field=state.cursor_field,
                )
                session.add(row)
            row.cursor_field = state.cursor_field or row.cursor_field
            row.cursor_value = advance_cursor(row.cursor_value, new_cursor)
            if blob is not None:
                row.state = blob
            row.records_synced = (row.records_synced or 0) + max(0, added_records)
            try:
                await session.commit()
                persisted = row.cursor_value
                break
            except IntegrityError:
                await session.rollback()
                if attempt == 2:
                    raise
    else:  # pragma: no cover - the loop always breaks or raises
        persisted = new_cursor

    state.cursor_value = persisted
    if blob is not None:
        state.blob = blob
    state.records_synced += max(0, added_records)
    return state


async def reset_state(connection_id: int, stream: str | None = None) -> int:
    """Drop cursor(s) so the next run does a full backfill. Returns rows removed."""
    from sqlalchemy import delete

    async with SessionLocal() as session:
        stmt = delete(SyncState).where(SyncState.connection_id == connection_id)
        if stream is not None:
            stmt = stmt.where(SyncState.stream == stream)
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount or 0


__all__ = ["StreamState", "commit_state", "load_state", "reset_state"]
