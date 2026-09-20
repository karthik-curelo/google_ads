"""In-process sync scheduler (§13).

A single asyncio task polls for due connections, claims them with one atomic
statement (app/sync/leases.py) and runs each — no broker, because the repo has none
and §33 rules out adding one.

Safety properties, and what enforces each:

  one run per connection      the connection row is a lease; claiming is a single
                              conditional UPDATE, and run_connection() re-claims
                              atomically on entry, so a scheduler tick, "Sync now",
                              a second scheduler process and a bare call can all
                              race and exactly one proceeds;
  crash recovery              the holder heartbeats; a lease not renewed for
                              `sync_lease_seconds` is expired and reclaimed — by the
                              next tick of *any* scheduler, not only after a restart;
  multiple instances          each process has a unique worker_id, the claim uses
                              FOR UPDATE SKIP LOCKED, and the reaper only touches
                              expired leases (or our own previous incarnation's,
                              when a stable WORKER_ID is configured) — never a lock
                              another live instance is heartbeating;
  bounded concurrency         at most `max_concurrent_syncs` runs at once. Different
                              connections run concurrently; provider API budgets are
                              shared through the process-wide rate limiters, so
                              concurrency cannot multiply the request rate.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime

from sqlalchemy import select, update

from app.connectors import errors as E
from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import CONN_ERROR, CONN_HEALTHY, RUN_CANCELLED, RUN_RETRYING, RUN_RUNNING, Connection, SyncRun
from app.sync import leases

logger = get_logger(__name__)


class SyncScheduler:
    def __init__(self, *, worker_id: str | None = None) -> None:
        settings = get_settings()
        self.worker_id = worker_id or settings.worker_id or leases.new_worker_id("scheduler")
        self.poll_seconds = settings.scheduler_poll_seconds
        self.max_concurrent = max(1, settings.max_concurrent_syncs)
        self._lease_seconds = settings.sync_lease_seconds
        self._sem = asyncio.Semaphore(self.max_concurrent)
        self._running: dict[int, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # --- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        if self._loop_task is not None:
            return
        await self._reap_orphaned_locks()
        self._stop.clear()
        self._loop_task = asyncio.create_task(self._loop(), name="sync-scheduler")
        logger.info(
            "Sync scheduler %s started (poll=%ss, max_concurrent=%s, lease=%ss)",
            self.worker_id,
            self.poll_seconds,
            self.max_concurrent,
            self._lease_seconds,
        )

    async def _reap_orphaned_locks(self) -> int:
        """Recover connections whose worker died mid-sync. Returns how many.

        Only reaps a lock that is *expired* (its heartbeat stopped: the holder is
        dead or partitioned) or that belongs to this very worker_id but is not being
        run by this process (a previous incarnation, when a stable WORKER_ID is
        configured). A fresh lease held by another instance is left alone — the old
        "every lock at startup is an orphan" rule was only true for exactly one
        process, and would have killed a live sibling's run.

        Each reap is compare-and-set on the (locked_by, locked_at) it observed, so it
        loses cleanly to a heartbeat that renews in the same instant. It is treated
        like a graceful cancellation (runner._finalize): the orphaned run is closed
        as cancelled, nothing counts as a failure, the connection is due again.
        """
        now = datetime.now(UTC)
        reaped = 0
        async with SessionLocal() as session:
            candidates = (
                (
                    await session.execute(
                        select(Connection).where(
                            Connection.locked_at.isnot(None),
                            leases.expired(now, self._lease_seconds)
                            | (Connection.locked_by == self.worker_id),
                        )
                    )
                )
                .scalars()
                .all()
            )
            for conn in candidates:
                if conn.id in self._running:
                    continue  # ours and genuinely in flight
                held_by, held_at = conn.locked_by, conn.locked_at
                cas = await session.execute(
                    update(Connection)
                    .where(
                        Connection.id == conn.id,
                        Connection.locked_by == held_by,
                        Connection.locked_at == held_at,
                    )
                    .values(locked_at=None, locked_by=None, lease_expires_at=None)
                )
                if cas.rowcount != 1:
                    continue  # renewed or re-claimed in the meantime
                runs = (
                    (
                        await session.execute(
                            select(SyncRun).where(
                                SyncRun.connection_id == conn.id,
                                SyncRun.status.in_((RUN_RUNNING, RUN_RETRYING)),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                for run in runs:
                    run.status = RUN_CANCELLED
                    run.finished_at = now
                    run.phase = "failed"
                    if run.error_code is None:
                        run.error_code = E.ErrorCode.CANCELLED
                        run.error_message = (
                            "Orphaned by a process restart — recovered after its lease expired."
                        )
                await session.refresh(conn)
                if conn.status == "syncing":
                    conn.status = CONN_ERROR if conn.consecutive_failures else CONN_HEALTHY
                conn.next_run_at = now  # due again on the very next tick
                reaped += 1
                logger.warning(
                    "Reaped expired lease on connection %s (held by %s since %s) — worker died mid-sync",
                    conn.id,
                    held_by,
                    held_at,
                )
            await session.commit()
        return reaped

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
        logger.info("Sync scheduler %s stopped", self.worker_id)

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
        # Recover dead workers' connections first — any live scheduler does this,
        # so a crash is repaired within one lease + one poll, not at next restart.
        await self._reap_orphaned_locks()
        free = self.max_concurrent - len(self._running)
        if free <= 0:
            return
        for connection_id in await self._claim_due(limit=free):
            self._spawn(connection_id, trigger="schedule")

    async def _claim_due(self, *, limit: int) -> list[int]:
        return await leases.claim_due(self.worker_id, limit=limit, lease_seconds=self._lease_seconds)

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
                await leases.release_lease(connection_id, self.worker_id)

    # --- manual trigger -------------------------------------------------------
    async def trigger(self, connection_id: int, *, sync_mode: str | None = None) -> bool:
        """Claim and run now. False if already running or the claim was lost."""
        if connection_id in self._running:
            return False
        # A manual claim must not "re-enter" a lock that merely carries this
        # scheduler's own id but belongs to a run we are not executing.
        claimed = await leases.claim_connection(
            connection_id, self.worker_id, lease_seconds=self._lease_seconds
        )
        if not claimed:
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

    Claims the lease and runs inline in a background task so the API can return
    immediately, same contract as SyncScheduler.trigger. The worker id is unique
    per call — the old constant "detached" made two API processes look like one.
    """
    from app.sync.runner import run_connection

    worker_id = leases.new_worker_id("detached")
    settings = get_settings()
    if not await leases.claim_connection(connection_id, worker_id, lease_seconds=settings.sync_lease_seconds):
        return
    try:
        # worker_id must match the identity claimed just above — run_connection()
        # does its own atomic claim on entry, and a mismatched id would see this
        # connection as validly locked by someone else and decline to do any work.
        with contextlib.suppress(Exception):
            await run_connection(connection_id, trigger="manual", sync_mode=sync_mode, worker_id=worker_id)
    finally:
        await leases.release_lease(connection_id, worker_id)


__all__ = ["SyncScheduler", "trigger_sync_detached"]
