"""run_connection()'s own atomic claim/release — the fix for a real production
race found while enabling the LeadSquared connection: a live scheduler
process and a directly-invoked run_connection() call both saw the connection
as unclaimed (only SyncScheduler.trigger()/_claim_due() set the lock; a bare
call never did), so both proceeded, and any exception in the narrow window
before the connector was constructed skipped the runner's own finally
cleanup entirely, leaving the connection locked and the SyncRun row stuck at
'running' forever.

This file exercises run_connection()'s claim directly, independent of the
scheduler, since that's now the single place the guarantee actually lives.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select

from app.connectors import errors as E
from app.connectors.registry import RegistryEntry, registry
from app.models import Connection, OAuthIdentity, SyncRun
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


async def _make_connection(session, org, *, connector_id: str = "stub") -> Connection:
    ident = OAuthIdentity(organization_id=org.id, provider="stub", external_account_id="stub-acc", scopes=[])
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id=connector_id,
        name="stub-conn",
        resource_id="r1",
        config={},
        streams=[{"stream": "daily", "sync_mode": "incremental", "enabled": True}],
        backfill_start_date=date.today() - timedelta(days=2),
        lookback_days=1,
        schedule_interval_seconds=None,
        enabled=True,
        next_run_at=datetime.now(UTC),
    )
    session.add(conn)
    await session.commit()
    return conn


async def _n_runs(session, connection_id: int) -> int:
    rows = (
        (await session.execute(select(SyncRun).where(SyncRun.connection_id == connection_id))).scalars().all()
    )
    return len(rows)


async def test_two_concurrent_direct_calls_only_one_actually_runs(session, org):
    conn = await _make_connection(session, org)
    StubConnector.entered_read_slice = asyncio.Event()

    task1 = asyncio.create_task(run_connection(conn.id, trigger="manual"))
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2.0)

    # A second, independent direct call for the same connection while the
    # first is still mid-flight — must return None immediately, not block,
    # not create a second SyncRun row, not touch the connector at all.
    result2 = await run_connection(conn.id, trigger="manual")
    assert result2 is None

    task1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task1

    assert await _n_runs(session, conn.id) == 1  # only the first call ever created a row

    await session.refresh(conn)
    assert conn.locked_at is None and conn.locked_by is None  # released on the way out


async def test_scheduler_claim_and_direct_call_race_only_one_wins(session, org):
    conn = await _make_connection(session, org)
    StubConnector.entered_read_slice = asyncio.Event()

    sched = SyncScheduler(worker_id="scheduler-test")
    claimed = await sched.trigger(conn.id)
    assert claimed is True
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2.0)

    # A direct call with a *different* identity than the scheduler's own —
    # exactly the shape of the production incident (a live scheduler process
    # and a manually-invoked run_connection() disagreeing about ownership).
    result = await run_connection(conn.id, trigger="manual")
    assert result is None

    cancelled = await sched.cancel(conn.id)
    assert cancelled is True
    await asyncio.sleep(0.05)  # let _guarded_run's finally clear the lock
    await sched.stop()

    assert await _n_runs(session, conn.id) == 1  # the scheduler's run, and only that one

    await session.refresh(conn)
    assert conn.locked_at is None and conn.locked_by is None


async def test_lock_released_after_successful_sync(session, org):
    conn = await _make_connection(session, org)
    outcome = await run_connection(conn.id, trigger="manual")
    assert outcome is not None and outcome.status == "succeeded"

    await session.refresh(conn)
    assert conn.locked_at is None and conn.locked_by is None


async def test_lock_released_after_exception_before_connector_is_even_constructed(session, org):
    """The exact gap the production incident fell into: an exception raised
    between the claim succeeding and the connector being constructed (here,
    an unknown connector_id — load_connectors().get() raises before anything
    else runs) used to leave the connection locked forever with no SyncRun
    row ever reaching a terminal state. The lock must come back down anyway;
    the exception itself still propagates, unchanged from before this fix."""
    conn = await _make_connection(session, org, connector_id="does-not-exist")

    with pytest.raises(E.ConnectorError):
        await run_connection(conn.id, trigger="manual")

    await session.refresh(conn)
    assert conn.locked_at is None and conn.locked_by is None  # released despite the raise

    # A SyncRun row *was* created (the claim + row-creation happen together,
    # before the point of failure) — this fix targets the lock, not that
    # row's own terminal status, which is a separate, smaller residual gap.
    assert await _n_runs(session, conn.id) == 1


async def test_already_locked_connection_creates_no_new_syncrun(session, org):
    conn = await _make_connection(session, org)
    conn.locked_at = datetime.now(UTC)
    conn.locked_by = "someone-else"
    await session.commit()

    result = await run_connection(conn.id, trigger="manual")
    assert result is None
    assert await _n_runs(session, conn.id) == 0  # no work started at all

    await session.refresh(conn)
    assert conn.locked_by == "someone-else"  # untouched — we never owned it, never clear it


async def test_a_stale_lock_can_still_be_reclaimed(session, org):
    """Mirrors SyncScheduler's own stale-lock takeover — a lock older than
    sync_run_timeout_seconds + 300s is treated as abandoned, not live."""
    from app.core.config import get_settings

    conn = await _make_connection(session, org)
    settings = get_settings()
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=settings.sync_run_timeout_seconds + 301)
    conn.locked_by = "long-dead-worker"
    await session.commit()

    outcome = await run_connection(conn.id, trigger="manual")
    assert outcome is not None and outcome.status == "succeeded"


async def test_worker_id_reentrancy_lets_a_pre_claiming_caller_proceed(session, org):
    """A caller that pre-claims with its own worker_id (exactly what
    SyncScheduler._claim_due/trigger do before spawning run_connection())
    must not be locked out by its own prior claim."""
    conn = await _make_connection(session, org)
    now = datetime.now(UTC)
    conn.locked_at = now
    conn.locked_by = "same-caller"
    await session.commit()

    outcome = await run_connection(conn.id, trigger="schedule", worker_id="same-caller")
    assert outcome is not None and outcome.status == "succeeded"


async def test_local_scheduler_does_not_runaway_poll_after_a_success(session, org):
    """Local-only reproduction of the production incident's *symptom* (a
    connection re-claimed and re-spawned on every poll tick, forever) —
    proven absent under real timing pressure by running an actual
    SyncScheduler with a fast poll interval (not GCP, not the real 30s
    default) for several real ticks and confirming exactly one SyncRun gets
    created, not one per tick."""
    conn = await _make_connection(session, org)
    conn.schedule_interval_seconds = 3600  # long — must not look "due" again right after success
    await session.commit()

    sched = SyncScheduler(worker_id="local-fast-poll-test")
    sched.poll_seconds = 0.15  # far faster than production's 30s, so several ticks fit in a short test
    await sched.start()
    await asyncio.sleep(1.2)  # ~8 poll ticks at this rate
    await sched.stop()

    runs = (await session.execute(select(SyncRun).where(SyncRun.connection_id == conn.id))).scalars().all()
    assert len(runs) == 1, f"expected exactly 1 run across ~8 poll ticks, got {len(runs)}"
    assert runs[0].status == "succeeded"

    await session.refresh(conn)
    assert conn.locked_at is None and conn.locked_by is None
    assert conn.next_run_at > datetime.now(UTC)  # pushed a full interval out, not left "due"
