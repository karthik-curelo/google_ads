"""End-to-end: scheduler/runner → connector → destination → state (§25, §28, §37)."""

from datetime import UTC, datetime, timedelta

import pytest

from app.connectors import errors as E
from app.connectors.registry import RegistryEntry, registry
from app.models import (
    Connection,
    GoogleAnalyticsPerformance,
    OAuthIdentity,
    SyncError,
    SyncRun,
    SyncState,
)
from app.sync.runner import run_connection
from app.sync.scheduler import SyncScheduler
from tests._fakes import StubConnector


@pytest.fixture(autouse=True)
def _register_stub():
    if "stub" not in registry:
        registry.register(RegistryEntry(connector_class=StubConnector))
    StubConnector.fail_with = None
    StubConnector.rows_per_day = 2
    StubConnector.entered_read_slice = None
    yield
    StubConnector.fail_with = None
    StubConnector.entered_read_slice = None


async def _make_connection(session, org, *, interval: int | None = 3600) -> Connection:
    ident = OAuthIdentity(
        organization_id=org.id,
        provider="stub",
        external_account_id="stub-acc",
        scopes=[],
    )
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="stub",
        name="stub-conn",
        resource_id="r1",
        config={},
        streams=[{"stream": "daily", "sync_mode": "incremental", "enabled": True}],
        backfill_start_date=datetime.now(UTC).date() - timedelta(days=4),
        lookback_days=2,
        schedule_interval_seconds=interval,
        enabled=True,
        next_run_at=datetime.now(UTC),
    )
    session.add(conn)
    await session.commit()
    return conn


async def test_full_sync_writes_rows_and_advances_cursor(session, org):
    conn = await _make_connection(session, org)

    outcome = await run_connection(conn.id, trigger="manual")

    assert outcome.status == "succeeded"
    assert outcome.records_inserted == 5 * 2  # 5 days (today-4..today) × 2 rows

    rows = (await session.execute(GoogleAnalyticsPerformance.__table__.select())).all()
    assert len(rows) == 10
    assert {r.stream for r in rows} == {"daily"}
    assert all(r.sessions in (10, 11) for r in rows)

    st = (await session.execute(SyncState.__table__.select())).one()
    # UTC, not local date — matches what the runner itself uses
    # (datetime.now(UTC).date()) to resolve "today". Using local date.today()
    # here made this test fail for part of every day in a UTC+ timezone: local
    # midnight rolls the calendar over before UTC's does, so the two would
    # briefly disagree about which day "today" is.
    assert st.cursor_value == datetime.now(UTC).date().isoformat()

    await session.refresh(conn)
    assert conn.status == "healthy"
    assert conn.last_success_at is not None
    assert conn.next_run_at > datetime.now(UTC)
    assert conn.total_records_synced == 10
    assert conn.locked_at is None


async def test_incremental_second_run_only_fetches_lookback(session, org):
    conn = await _make_connection(session, org)
    await run_connection(conn.id, trigger="manual")

    StubConnector.rows_per_day = 3  # provider "restates" — upsert must correct
    out2 = await run_connection(conn.id, trigger="schedule")

    assert out2.status == "succeeded"
    # second run covers only cursor-2d .. today (3 days) not the full backfill
    assert out2.records_fetched == 3 * 3
    rows = (await session.execute(GoogleAnalyticsPerformance.__table__.select())).all()
    # 5 original days + 1 new channel per lookback day; no duplicates
    assert len({(r.date, r.dimensions["channel"]) for r in rows}) == len(rows)


async def test_retryable_failure_marks_run_failed_and_records_error(session, org):
    conn = await _make_connection(session, org)
    StubConnector.fail_with = E.rate_limit_error("throttled by provider")

    outcome = await run_connection(conn.id, trigger="schedule")

    assert outcome.status == "failed"
    assert outcome.will_retry is True
    errs = (await session.execute(SyncError.__table__.select())).all()
    assert errs and errs[0].code == E.ErrorCode.RATE_LIMIT_ERROR

    await session.refresh(conn)
    assert conn.consecutive_failures == 1
    assert conn.status == "error"
    assert conn.next_run_at > datetime.now(UTC)  # backoff scheduled

    run = (await session.execute(SyncRun.__table__.select())).one()
    assert run.status == "failed" and run.will_retry is True


async def test_cancellation_does_not_count_as_a_failure(session, org):
    """A cancelled run (process shutdown/restart mid-sync, e.g. a deploy) must
    not be indistinguishable from a real provider failure: no
    consecutive_failures bump, connection status stays/returns to healthy,
    and the next run is scheduled normally — not backed off. Previously this
    left connections stuck showing a false "error" after every routine
    restart until their next scheduled run happened to succeed."""
    import asyncio

    conn = await _make_connection(session, org)
    StubConnector.entered_read_slice = asyncio.Event()

    task = asyncio.create_task(run_connection(conn.id, trigger="schedule"))
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    run = (await session.execute(SyncRun.__table__.select())).one()
    assert run.status == "cancelled"

    await session.refresh(conn)
    assert conn.consecutive_failures == 0  # not counted as a failure
    assert conn.status == "healthy"  # not left stuck showing "error"
    assert conn.locked_at is None  # lock released, not left stale
    assert conn.next_run_at is not None
    # scheduled on the normal cadence, not backed off (backoff would push
    # this well past a single interval for a fresh connection)
    assert conn.next_run_at <= datetime.now(UTC) + timedelta(seconds=conn.schedule_interval_seconds + 5)


async def test_cancellation_after_a_real_failure_preserves_error_state(session, org):
    """The reverse case: if a connection already had a genuine standing
    failure, an unrelated cancellation (e.g. a restart during the next
    attempt) must not silently paper over that — it shouldn't clear
    consecutive_failures or the error status, since the real problem hasn't
    actually been resolved."""
    import asyncio

    conn = await _make_connection(session, org)
    StubConnector.fail_with = E.rate_limit_error("throttled by provider")
    await run_connection(conn.id, trigger="schedule")
    await session.refresh(conn)
    assert conn.consecutive_failures == 1
    assert conn.status == "error"

    StubConnector.fail_with = None
    StubConnector.entered_read_slice = asyncio.Event()
    task = asyncio.create_task(run_connection(conn.id, trigger="schedule"))
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await session.refresh(conn)
    assert conn.consecutive_failures == 1  # untouched, not reset by the cancellation
    assert conn.status == "error"  # the real standing failure is preserved


async def test_scheduler_start_reaps_orphaned_lock_from_a_hard_kill(session, org):
    """A hard process kill (no graceful shutdown, e.g. OOM-killer, a forced
    VM stop) leaves a connection locked and status='syncing' with no
    run_connection() call ever getting to run its own cleanup — previously
    only recoverable after the ~3h stale-lock window in _claim_due. A fresh
    scheduler start must reclaim it within seconds instead: clear the lock,
    close out the orphaned "running" sync_runs row as cancelled (not
    failed), and make it immediately due again — all without counting as a
    failure, matching the graceful-cancellation behavior in _finalize."""
    conn = await _make_connection(session, org)
    # Simulate exactly what a hard-killed run_connection() leaves behind:
    # locked, "syncing", and an orphaned SyncRun stuck at status="running".
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=5)
    conn.locked_by = "scheduler-dead"
    conn.status = "syncing"
    conn.next_run_at = datetime.now(UTC) + timedelta(hours=2)  # far in the future
    orphaned_run = SyncRun(
        connection_id=conn.id,
        organization_id=org.id,
        trigger="schedule",
        sync_mode="incremental",
        status="running",
        phase="fetching",
    )
    session.add(orphaned_run)
    await session.commit()

    # Call the reap step directly (not the full start()/stop() lifecycle) so
    # this test is deterministic — it doesn't race against whether the poll
    # loop's first _tick() also manages to spawn a real (successful) sync in
    # the gap before stop(), which would legitimately push next_run_at back
    # into the future and mask the very thing this test checks.
    # test_scheduler_start_spawns_the_reap_step below proves start() wires it.
    sched = SyncScheduler(worker_id="scheduler-2")
    await sched._reap_orphaned_locks()

    await session.refresh(conn)
    assert conn.locked_at is None
    assert conn.locked_by is None
    assert conn.status == "healthy"  # not left stuck showing "syncing" or "error"
    assert conn.consecutive_failures == 0  # not counted as a failure
    assert conn.next_run_at <= datetime.now(UTC)  # immediately due, not 2h away

    await session.refresh(orphaned_run)
    assert orphaned_run.status == "cancelled"
    assert orphaned_run.finished_at is not None
    assert orphaned_run.error_code is not None
    assert "restart" in (orphaned_run.error_message or "").lower()


async def test_scheduler_start_reap_preserves_genuine_error_status(session, org):
    """The same preserved-error-state invariant as the graceful-cancellation
    case: a connection with a real standing failure streak must not have
    that silently cleared just because it also happened to be mid-sync when
    the process was killed."""
    conn = await _make_connection(session, org)
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=5)
    conn.locked_by = "scheduler-dead"
    conn.status = "syncing"
    conn.consecutive_failures = 3
    await session.commit()

    sched = SyncScheduler(worker_id="scheduler-2")
    await sched._reap_orphaned_locks()

    await session.refresh(conn)
    assert conn.locked_at is None
    assert conn.consecutive_failures == 3  # untouched
    assert conn.status == "error"  # the real standing failure is preserved


async def test_scheduler_start_calls_the_reap_step(session, org):
    """Proves start() actually wires up the reap step (the two tests above
    exercise its logic directly, for determinism — this proves it's not
    dead code)."""
    from unittest.mock import AsyncMock

    sched = SyncScheduler(worker_id="scheduler-3")
    sched._reap_orphaned_locks = AsyncMock(wraps=sched._reap_orphaned_locks)
    await sched.start()
    await sched.stop()
    sched._reap_orphaned_locks.assert_awaited_once()


async def _drain(sched, timeout=5.0):
    import asyncio

    async with asyncio.timeout(timeout):
        while sched.active:
            await asyncio.sleep(0.02)


async def test_scheduler_trigger_prevents_concurrent_runs(session, org):
    conn = await _make_connection(session, org)
    sched = SyncScheduler(worker_id="test")

    first = await sched.trigger(conn.id)
    second = await sched.trigger(conn.id)
    assert first is True and second is False

    await _drain(sched)
    await sched.stop()
    await session.refresh(conn)
    assert conn.locked_at is None  # released after the run finished


async def test_scheduler_claims_due_connection(session, org):
    conn = await _make_connection(session, org)
    sched = SyncScheduler(worker_id="test")
    await sched._tick()  # one poll cycle claims + spawns
    assert conn.id in sched.active
    await _drain(sched)
    await sched.stop()

    runs = (await session.execute(SyncRun.__table__.select())).all()
    assert len(runs) == 1 and runs[0].status == "succeeded"
