"""In-process sync scheduler (§13).

A single asyncio task polls for due connections, claims each with a compare-and-
swap on its lock columns, and runs it — no broker, because the repo has none and
§33 rules out adding one. The CAS claim is what makes "never run one connection
twice at once" true even if a manual "Sync now" lands mid-poll.

ponytail: single process. The lock is a DB row, so a second *process* would be
safe against double-runs too, but nothing here elects a leader — run exactly one
scheduler. Move the claim to SELECT ... FOR UPDATE SKIP LOCKED and run N workers
if throughput ever needs it.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select, update

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import CONN_PAUSED, Connection

logger = get_logger(__name__)


class SyncScheduler:
    def __init__(self, *, worker_id: str = "scheduler-1") -> None:
        settings = get_settings()
        self.worker_id = worker_id
        self.poll_seconds = settings.scheduler_poll_seconds
        self.max_concurrent = max(1, settings.max_concurrent_syncs)
        self._stale_after = settings.sync_run_timeout_seconds + 300
        self._sem = asyncio.Semaphore(self.max_concurrent)
        self._running: dict[int, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # --- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        if self._loop_task is not None:
            return
        self._stop.clear()
        self._loop_task = asyncio.create_task(self._loop(), name="sync-scheduler")
        logger.info(
            "Sync scheduler started (poll=%ss, max_concurrent=%s)",
            self.poll_seconds,
            self.max_concurrent,
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._loop_task is not None:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task
            self._loop_task = None
        for task in list(self._running.values()):
            task.cancel()
        if self._running:
            await asyncio.gather(*self._running.values(), return_exceptions=True)
        self._running.clear()
        logger.info("Sync scheduler stopped")

    # --- polling -------------------------------------------------------
    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a bad tick must not kill the loop
                logger.exception("Scheduler tick failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_seconds)

    async def _tick(self) -> None:
        free = self.max_concurrent - len(self._running)
        if free <= 0:
            return
        for connection_id in await self._claim_due(limit=free):
            self._spawn(connection_id, trigger="schedule")

    async def _claim_due(self, *, limit: int) -> list[int]:
        now = datetime.now(UTC)
        stale_cutoff = now - timedelta(seconds=self._stale_after)
        async with SessionLocal() as session:
            rows = (
                (
                    await session.execute(
                        select(Connection.id)
                        .where(
                            Connection.enabled.is_(True),
                            Connection.status != CONN_PAUSED,
                            Connection.next_run_at.isnot(None),
                            Connection.next_run_at <= now,
                            or_(
                                Connection.locked_at.is_(None),
                                Connection.locked_at < stale_cutoff,
                            ),
                        )
                        .order_by(Connection.next_run_at)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )

            claimed: list[int] = []
            for cid in rows:
                result = await session.execute(
                    update(Connection)
                    .where(
                        Connection.id == cid,
                        or_(
                            Connection.locked_at.is_(None),
                            Connection.locked_at < stale_cutoff,
                        ),
                    )
                    .values(locked_at=now, locked_by=self.worker_id)
                )
                if result.rowcount == 1:
                    claimed.append(cid)
            await session.commit()
            return claimed

    # --- run tracking -------------------------------------------------------
    def _spawn(self, connection_id: int, *, trigger: str, sync_mode: str | None = None) -> asyncio.Task:
        if connection_id in self._running:
            return self._running[connection_id]
        task = asyncio.create_task(
            self._guarded_run(connection_id, trigger=trigger, sync_mode=sync_mode),
            name=f"sync-conn-{connection_id}",
        )
        self._running[connection_id] = task
        return task

    async def _guarded_run(self, connection_id: int, *, trigger: str, sync_mode: str | None) -> None:
        from app.sync.runner import run_connection

        async with self._sem:
            try:
                await run_connection(
                    connection_id, trigger=trigger, sync_mode=sync_mode, worker_id=self.worker_id
                )
            except asyncio.CancelledError:
                logger.info("Sync for connection %s cancelled", connection_id)
                raise
            except Exception:  # noqa: BLE001 - runner already recorded it
                logger.exception("Sync for connection %s raised", connection_id)
            finally:
                self._running.pop(connection_id, None)
                await self._clear_lock(connection_id)

    async def _clear_lock(self, connection_id: int) -> None:
        with contextlib.suppress(Exception):
            async with SessionLocal() as session:
                await session.execute(
                    update(Connection)
                    .where(Connection.id == connection_id)
                    .values(locked_at=None, locked_by=None)
                )
                await session.commit()

    # --- manual trigger -------------------------------------------------------
    async def trigger(self, connection_id: int, *, sync_mode: str | None = None) -> bool:
        """Claim and run now. False if already running or claim lost."""
        if connection_id in self._running:
            return False
        now = datetime.now(UTC)
        stale_cutoff = now - timedelta(seconds=self._stale_after)
        async with SessionLocal() as session:
            result = await session.execute(
                update(Connection)
                .where(
                    Connection.id == connection_id,
                    or_(Connection.locked_at.is_(None), Connection.locked_at < stale_cutoff),
                )
                .values(locked_at=now, locked_by=self.worker_id)
            )
            await session.commit()
            if result.rowcount != 1:
                return False
        self._spawn(connection_id, trigger="manual", sync_mode=sync_mode)
        return True

    @property
    def active(self) -> list[int]:
        return list(self._running)

    async def cancel(self, connection_id: int) -> bool:
        task = self._running.get(connection_id)
        if task is None:
            return False
        task.cancel()
        return True


async def trigger_sync_detached(connection_id: int, *, sync_mode: str | None = None) -> None:
    """Fallback used when no scheduler is running (tests, scheduler disabled).

    Claims the lock and runs inline in a background task so the API can return
    immediately, same contract as SyncScheduler.trigger.
    """
    from app.sync.runner import run_connection

    now = datetime.now(UTC)
    async with SessionLocal() as session:
        result = await session.execute(
            update(Connection)
            .where(Connection.id == connection_id, Connection.locked_at.is_(None))
            .values(locked_at=now, locked_by="detached")
        )
        await session.commit()
        if result.rowcount != 1:
            return
    with contextlib.suppress(Exception):
        await run_connection(connection_id, trigger="manual", sync_mode=sync_mode)


__all__ = ["SyncScheduler", "trigger_sync_detached"]
