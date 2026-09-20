"""Connection leases: atomic claim, heartbeat renewal, fenced release.

A connection is claimed by writing `locked_by`/`locked_at` on its row. That row is
a *lease*: the holder renews `locked_at` on a heartbeat, and anyone may take a lease
whose last renewal is older than `lease_seconds`. Consequences:

* Claiming is one conditional statement, never "SELECT a job, then UPDATE it". The
  due-connection claim is a single `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE
  SKIP LOCKED) RETURNING id`, so two schedulers (two processes, two hosts) can poll
  at the same instant and each gets a disjoint set; neither can double-claim.
* A crashed or killed worker stops heartbeating, so its lease expires after
  `lease_seconds` (minutes) and another worker takes over. The old design's only
  recovery was the run-timeout window (~3 hours).
* A worker that *loses* its lease (paused too long, DB partition) is fenced: the
  heartbeat notices its renewal matched no row and cancels the run, so two workers
  never write for one connection.
* Different connections are independent rows, so different connectors/connections
  still run concurrently — only one connection is exclusive.

`worker_id` must be unique per process. The previous constant ("scheduler-1") made
two instances indistinguishable, so each treated the other's lock as its own.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select, update

from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import CONN_PAUSED, Connection

logger = get_logger(__name__)


def new_worker_id(prefix: str = "worker") -> str:
    """Unique per process incarnation: host, pid and a random suffix."""
    return f"{prefix}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


def lease_cutoff(now: datetime, lease_seconds: float) -> datetime:
    """Legacy leases (no recorded expiry) last renewed before this instant are expired."""
    return now - timedelta(seconds=lease_seconds)


def lease_values(now: datetime, worker_id: str, lease_seconds: float) -> dict:
    """The columns a claim/renewal writes: who holds it, when, and - crucially - until
    when. The HOLDER states its own expiry, so a process with a different lease
    configuration than its observers (a laptop CLI next to the production scheduler)
    is judged by what it promised, not by what a stranger's settings assume."""
    return {
        "locked_at": now,
        "locked_by": worker_id,
        "lease_expires_at": now + timedelta(seconds=lease_seconds),
    }


def expired(now: datetime, lease_seconds: float):
    """SQL: this connection's lease has lapsed (its holder stopped renewing).

    A row carrying `lease_expires_at` lapses when that instant passes. A row locked by
    code that predates the column has none, so it falls back to `locked_at` plus the
    observer's lease length - the old rule, kept only for that transition.
    """
    return or_(
        and_(Connection.lease_expires_at.isnot(None), Connection.lease_expires_at < now),
        and_(Connection.lease_expires_at.is_(None), Connection.locked_at < lease_cutoff(now, lease_seconds)),
    )


def free(now: datetime, lease_seconds: float):
    """SQL: nobody currently holds a live lease on this connection."""
    return or_(Connection.locked_at.is_(None), expired(now, lease_seconds))


async def claim_connection(
    connection_id: int, worker_id: str, *, lease_seconds: float, now: datetime | None = None
) -> bool:
    """Atomically claim one connection. True iff this call now holds the lease.

    Succeeds when the row is unlocked, its lease has expired, or it is already ours
    (re-entrancy for a caller that pre-claimed with the same id).
    """
    now = now or datetime.now(UTC)
    async with SessionLocal() as session:
        result = await session.execute(
            update(Connection)
            .where(
                Connection.id == connection_id,
                or_(free(now, lease_seconds), Connection.locked_by == worker_id),
            )
            .values(**lease_values(now, worker_id, lease_seconds))
        )
        await session.commit()
        return result.rowcount == 1


async def claim_due(
    worker_id: str, *, limit: int, lease_seconds: float, now: datetime | None = None
) -> list[int]:
    """Claim up to `limit` due connections in ONE atomic statement.

    On PostgreSQL the inner SELECT locks its rows with `FOR UPDATE SKIP LOCKED`, so a
    concurrent claimer skips (rather than waits on, or duplicates) rows already being
    claimed. The due/free predicate is repeated on the UPDATE, so a row that was
    finished and rescheduled between the two steps cannot be claimed on stale data.
    SQLite (tests, local dev) serialises writers, which gives the same guarantee.
    """
    if limit <= 0:
        return []
    now = now or datetime.now(UTC)
    is_free = free(now, lease_seconds)
    due = (
        select(Connection.id)
        .where(
            Connection.enabled.is_(True),
            Connection.status != CONN_PAUSED,
            Connection.next_run_at.isnot(None),
            Connection.next_run_at <= now,
            is_free,
        )
        .order_by(Connection.next_run_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    async with SessionLocal() as session:
        result = await session.execute(
            update(Connection)
            .where(Connection.id.in_(due), is_free)
            .values(**lease_values(now, worker_id, lease_seconds))
            .returning(Connection.id)
        )
        ids = list(result.scalars())
        await session.commit()
    return ids


async def renew_lease(
    connection_id: int, worker_id: str, *, lease_seconds: float, now: datetime | None = None
) -> bool:
    """Heartbeat: push the expiry out again. False means the lease is no longer ours
    (fencing signal)."""
    async with SessionLocal() as session:
        result = await session.execute(
            update(Connection)
            .where(Connection.id == connection_id, Connection.locked_by == worker_id)
            .values(**lease_values(now or datetime.now(UTC), worker_id, lease_seconds))
        )
        await session.commit()
        return result.rowcount == 1


async def release_lease(connection_id: int, worker_id: str) -> None:
    """Release, but only a lease that is still ours — a run that overran and had its
    lease taken over must never clear its successor's lock on the way out."""
    with contextlib.suppress(Exception):
        async with SessionLocal() as session:
            await session.execute(
                update(Connection)
                .where(Connection.id == connection_id, Connection.locked_by == worker_id)
                .values(locked_at=None, locked_by=None, lease_expires_at=None)
            )
            await session.commit()


@contextlib.asynccontextmanager
async def heartbeat(
    connection_id: int, worker_id: str, *, interval: float, lease_seconds: float
) -> AsyncIterator[asyncio.Event]:
    """Renew the lease every `interval` seconds while the body runs.

    Yields an event that is set if the lease was lost. On loss the enclosing task is
    cancelled — the caller distinguishes that from an outside cancellation by
    checking the event.
    """
    owner = asyncio.current_task()
    lost = asyncio.Event()

    async def _beat() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                still_ours = await renew_lease(connection_id, worker_id, lease_seconds=lease_seconds)
            except Exception:  # noqa: BLE001 - a DB blip must not drop a healthy lease
                logger.warning(
                    "lease renewal for connection %s failed; will retry", connection_id, exc_info=True
                )
                continue
            if not still_ours:
                logger.error(
                    "lease on connection %s lost by %s — cancelling the run", connection_id, worker_id
                )
                lost.set()
                if owner is not None:
                    owner.cancel()
                return

    task = asyncio.create_task(_beat(), name=f"lease-heartbeat-{connection_id}")
    try:
        yield lost
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


__all__ = [
    "claim_connection",
    "claim_due",
    "expired",
    "free",
    "lease_values",
    "heartbeat",
    "lease_cutoff",
    "new_worker_id",
    "release_lease",
    "renew_lease",
]
