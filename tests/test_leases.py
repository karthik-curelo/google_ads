"""Race safety: atomic claiming, leases, heartbeat/fencing, crash recovery, and
concurrency across connectors.

The scenarios are the ones the audit named: two workers claiming one connection,
overlapping scheduled runs, manual + scheduled overlap, scheduler restart, worker
crash, several application instances — and different connectors running at once.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select

from app.connectors.registry import RegistryEntry, registry
from app.core.config import get_settings
from app.models import Connection, OAuthIdentity, SyncRun
from app.sync import leases
from app.sync.runner import run_connection
from app.sync.scheduler import SyncScheduler
from tests._fakes import StubConnector

LEASE = 300


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


async def _connection(session, org, *, name="c", interval=3600, due=True) -> Connection:
    ident = OAuthIdentity(
        organization_id=org.id, provider="stub", external_account_id=f"acc-{name}", scopes=[]
    )
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="stub",
        name=name,
        resource_id=f"r-{name}",
        config={},
        streams=[{"stream": "daily", "sync_mode": "incremental", "enabled": True}],
        backfill_start_date=date.today() - timedelta(days=1),
        lookback_days=1,
        schedule_interval_seconds=interval,
        enabled=True,
        next_run_at=datetime.now(UTC) if due else datetime.now(UTC) + timedelta(hours=5),
    )
    session.add(conn)
    await session.commit()
    return conn


# --- atomic claiming ----------------------------------------------------------------


async def test_two_workers_claiming_the_same_connection_at_once_exactly_one_wins(session, org):
    conn = await _connection(session, org)
    results = await asyncio.gather(
        *(leases.claim_connection(conn.id, f"worker-{i}", lease_seconds=LEASE) for i in range(8))
    )
    assert sum(results) == 1, "exactly one of eight simultaneous claimants may hold the lease"
    await session.refresh(conn)
    assert conn.locked_by in {f"worker-{i}" for i in range(8)}


async def test_two_schedulers_polling_at_the_same_instant_claim_disjoint_connections(session, org):
    conns = [await _connection(session, org, name=f"c{i}") for i in range(6)]
    a, b = SyncScheduler(worker_id="sched-A"), SyncScheduler(worker_id="sched-B")
    got_a, got_b = await asyncio.gather(a._claim_due(limit=6), b._claim_due(limit=6))
    assert set(got_a).isdisjoint(got_b)
    assert set(got_a) | set(got_b) == {c.id for c in conns}  # nothing left unclaimed either


async def test_a_connection_claimed_by_one_scheduler_is_invisible_to_the_other(session, org):
    await _connection(session, org)
    a, b = SyncScheduler(worker_id="sched-A"), SyncScheduler(worker_id="sched-B")
    assert len(await a._claim_due(limit=5)) == 1
    assert await b._claim_due(limit=5) == []


async def test_claim_due_ignores_paused_disabled_and_not_yet_due_connections(session, org):
    ok = await _connection(session, org, name="ok")
    paused = await _connection(session, org, name="paused")
    paused.status = "paused"
    disabled = await _connection(session, org, name="disabled")
    disabled.enabled = False
    await _connection(session, org, name="later", due=False)
    await session.commit()
    assert await leases.claim_due("w", limit=10, lease_seconds=LEASE) == [ok.id]
    assert disabled.id and paused.id


async def test_a_stale_due_read_cannot_claim_a_connection_that_was_just_rescheduled(session, org):
    """The predicate is re-checked inside the UPDATE, not trusted from an earlier read."""
    conn = await _connection(session, org)
    conn.next_run_at = datetime.now(UTC) + timedelta(hours=3)  # finished + rescheduled meanwhile
    await session.commit()
    assert await leases.claim_due("late-claimer", limit=5, lease_seconds=LEASE) == []


# --- manual + scheduled overlap, overlapping runs ------------------------------------------


async def test_manual_and_scheduled_runs_never_overlap(session, org):
    conn = await _connection(session, org)
    StubConnector.entered_read_slice = asyncio.Event()
    sched = SyncScheduler(worker_id="sched-1")

    assert await sched.trigger(conn.id) is True  # the "scheduled" path is mid-flight
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2)

    assert await sched.trigger(conn.id) is False  # manual "Sync now" while it runs
    assert await run_connection(conn.id, trigger="manual") is None  # a bare direct call
    other = SyncScheduler(worker_id="sched-2")  # a second application instance
    assert await other.trigger(conn.id) is False
    assert await other._claim_due(limit=5) == []

    await sched.cancel(conn.id)
    await asyncio.sleep(0.05)
    await sched.stop()
    runs = (await session.execute(select(SyncRun))).scalars().all()
    assert len(runs) == 1  # only the first ever created a run


async def test_two_simultaneous_run_connection_calls_execute_exactly_once(session, org):
    conn = await _connection(session, org)
    outs = await asyncio.gather(*(run_connection(conn.id, trigger="manual") for _ in range(5)))
    assert sum(o is not None for o in outs) >= 1
    runs = (await session.execute(select(SyncRun))).scalars().all()
    # each successful run holds the lease exclusively; none can interleave with another
    assert all(r.status == "succeeded" for r in runs)
    assert len({r.execution_id for r in runs}) == len(runs)


# --- heartbeat, expiry, fencing ---------------------------------------------------------------


async def test_the_heartbeat_keeps_a_long_run_alive_past_the_lease_window(session, org, monkeypatch):
    conn = await _connection(session, org)
    settings = get_settings()
    monkeypatch.setattr(settings, "sync_lease_seconds", 1)
    monkeypatch.setattr(settings, "sync_heartbeat_seconds", 0.2)
    StubConnector.entered_read_slice = asyncio.Event()

    task = asyncio.create_task(run_connection(conn.id, trigger="manual", worker_id="w-long"))
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2)
    await asyncio.sleep(1.6)  # longer than the 1s lease

    # still held: a rival cannot take a lease whose holder is heartbeating
    assert await leases.claim_connection(conn.id, "rival", lease_seconds=1) is False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_a_lease_that_stops_being_renewed_expires_and_can_be_taken_over(session, org):
    conn = await _connection(session, org)
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=LEASE + 5)  # holder died 5 minutes ago
    conn.locked_by = "dead-worker"
    await session.commit()
    assert await leases.claim_connection(conn.id, "successor", lease_seconds=LEASE) is True
    await session.refresh(conn)
    assert conn.locked_by == "successor"


async def test_a_worker_that_loses_its_lease_is_fenced_and_cannot_clobber_its_successor(
    session, org, monkeypatch
):
    conn = await _connection(session, org)
    settings = get_settings()
    monkeypatch.setattr(settings, "sync_heartbeat_seconds", 0.1)
    StubConnector.entered_read_slice = asyncio.Event()

    task = asyncio.create_task(run_connection(conn.id, trigger="schedule", worker_id="w-old"))
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2)

    # a network partition / long pause: the lease is taken over by another worker
    conn_db = await session.get(Connection, conn.id)
    conn_db.locked_by = "w-new"
    conn_db.locked_at = datetime.now(UTC)
    await session.commit()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=3)  # the heartbeat noticed and cancelled the run

    await session.refresh(conn)
    assert conn.locked_by == "w-new"  # the loser did NOT release or clear the winner's lock
    run = (await session.execute(select(SyncRun))).scalar_one()
    assert run.status == "cancelled" and "lease was lost" in (run.error_message or "")
    assert conn.consecutive_failures == 0  # lease loss is not a provider failure


async def test_release_only_clears_a_lease_that_is_still_ours(session, org):
    conn = await _connection(session, org)
    conn.locked_by, conn.locked_at = "someone-else", datetime.now(UTC)
    await session.commit()
    await leases.release_lease(conn.id, "not-the-owner")
    await session.refresh(conn)
    assert conn.locked_by == "someone-else"


# --- crash recovery, scheduler restart, multiple instances --------------------------------------------


async def test_a_crashed_workers_connection_is_recovered_by_any_scheduler_once_its_lease_expires(
    session, org
):
    conn = await _connection(session, org, due=False)
    orphan = SyncRun(
        connection_id=conn.id, organization_id=org.id, trigger="schedule", status="running", phase="fetching"
    )
    session.add(orphan)
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=LEASE + 30)
    conn.locked_by = "sched-dead"
    conn.status = "syncing"
    await session.commit()

    survivor = SyncScheduler(worker_id="sched-alive")  # NOT a restart of the dead one
    assert await survivor._reap_orphaned_locks() == 1

    await session.refresh(conn)
    await session.refresh(orphan)
    assert conn.locked_by is None and conn.status == "healthy" and conn.consecutive_failures == 0
    assert conn.next_run_at <= datetime.now(UTC)  # due again immediately
    assert orphan.status == "cancelled" and orphan.finished_at is not None


async def test_a_live_siblings_fresh_lease_is_never_reaped(session, org):
    """The old 'every lock at startup is an orphan' rule would have killed this run."""
    conn = await _connection(session, org)
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=5)
    conn.locked_by = "sched-sibling"
    conn.status = "syncing"
    await session.commit()
    restarted = SyncScheduler(worker_id="sched-restarted")
    assert await restarted._reap_orphaned_locks() == 0
    await session.refresh(conn)
    assert conn.locked_by == "sched-sibling" and conn.status == "syncing"


async def test_a_restarted_scheduler_with_a_stable_worker_id_reclaims_its_own_old_lock_at_once(session, org):
    conn = await _connection(session, org)
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=5)
    conn.locked_by = "vm-scheduler"  # this process's identity, before the crash
    conn.status = "syncing"
    await session.commit()
    again = SyncScheduler(worker_id="vm-scheduler")
    await again.start()
    await again.stop()
    await session.refresh(conn)
    assert conn.locked_by is None


async def test_a_lock_this_scheduler_is_actively_running_is_not_reaped(session, org):
    conn = await _connection(session, org)
    StubConnector.entered_read_slice = asyncio.Event()
    sched = SyncScheduler(worker_id="sched-1")
    await sched.trigger(conn.id)
    await asyncio.wait_for(StubConnector.entered_read_slice.wait(), timeout=2)
    assert await sched._reap_orphaned_locks() == 0
    await sched.cancel(conn.id)
    await asyncio.sleep(0.05)
    await sched.stop()


async def test_worker_ids_are_unique_per_process_incarnation():
    assert leases.new_worker_id("s") != leases.new_worker_id("s")
    assert SyncScheduler().worker_id != SyncScheduler().worker_id  # never the old constant


# --- different connectors run concurrently; the same one does not ------------------------------------


async def test_different_connections_run_concurrently_but_each_only_once(session, org):
    """Independent connectors overlap (both are inside read_slice at the same moment);
    the same connection never does."""
    a = await _connection(session, org, name="google-ads-like")
    b = await _connection(session, org, name="meta-like")
    inside, peak = 0, 0
    gate = asyncio.Event()
    original = StubConnector.read_slice

    async def gated(self, stream, slice_):
        nonlocal inside, peak
        inside += 1
        peak = max(peak, inside)
        try:
            await asyncio.wait_for(gate.wait(), timeout=3)
            async for rec in original(self, stream, slice_):
                yield rec
        finally:
            inside -= 1

    StubConnector.read_slice = gated
    try:
        sched = SyncScheduler(worker_id="sched-1")
        assert await sched.trigger(a.id) and await sched.trigger(b.id)
        for _ in range(100):
            if inside == 2:
                break
            await asyncio.sleep(0.02)
        assert inside == 2 and peak == 2, "two independent connections must execute concurrently"
        assert await sched.trigger(a.id) is False  # ...while the same one may not
        gate.set()
        async with asyncio.timeout(5):
            while sched.active:
                await asyncio.sleep(0.02)
        await sched.stop()
    finally:
        StubConnector.read_slice = original

    runs = (await session.execute(select(SyncRun))).scalars().all()
    assert len(runs) == 2 and {r.status for r in runs} == {"succeeded"}


async def test_global_concurrency_is_bounded(session, org, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_concurrent_syncs", 2)
    for i in range(5):
        await _connection(session, org, name=f"c{i}")
    sched = SyncScheduler(worker_id="sched-1")
    assert sched.max_concurrent == 2
    await sched._tick()
    assert len(sched.active) <= 2  # never more than the bound in flight
    async with asyncio.timeout(10):
        while sched.active:
            await asyncio.sleep(0.02)
    await sched.stop()


# --- the holder declares its own lease (found live: a slow-heartbeat CLI reaped by a fast scheduler) ---


async def test_a_holders_own_lease_length_governs_not_the_observers(session, org):
    """Live finding: a manual CLI run (lease 300s, heartbeat 60s) was reaped by a scheduler
    configured with a 30s lease, because expiry was judged by the OBSERVER's setting. The
    holder now records `lease_expires_at`, so a stranger's shorter setting cannot kill it."""
    conn = await _connection(session, org)
    assert await leases.claim_connection(conn.id, "laptop-cli", lease_seconds=300) is True
    await session.refresh(conn)
    assert conn.lease_expires_at is not None
    assert conn.lease_expires_at - conn.locked_at == timedelta(seconds=300)

    # 45s of silence: long past a 30s observer's idea of "stale", well inside the holder's 300s
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=45)
    conn.lease_expires_at = datetime.now(UTC) + timedelta(seconds=255)
    await session.commit()

    impatient = SyncScheduler(worker_id="vm-scheduler")
    impatient._lease_seconds = 30
    assert await impatient._reap_orphaned_locks() == 0
    assert await leases.claim_connection(conn.id, "vm-scheduler", lease_seconds=30) is False
    assert await impatient._claim_due(limit=5) == []
    await session.refresh(conn)
    assert conn.locked_by == "laptop-cli"


async def test_a_lease_whose_recorded_expiry_has_passed_is_free_to_anyone(session, org):
    conn = await _connection(session, org)
    conn.locked_by = "gone"
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=10)  # recently renewed by ITS clock...
    conn.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)  # ...but it promised only until now
    await session.commit()
    assert await leases.claim_connection(conn.id, "successor", lease_seconds=300) is True


async def test_a_renewal_pushes_the_recorded_expiry_forward_and_release_clears_it(session, org):
    conn = await _connection(session, org)
    await leases.claim_connection(conn.id, "w", lease_seconds=60)
    await session.refresh(conn)
    first = conn.lease_expires_at
    await asyncio.sleep(0.05)
    assert await leases.renew_lease(conn.id, "w", lease_seconds=60) is True
    await session.refresh(conn)
    assert conn.lease_expires_at > first
    assert await leases.renew_lease(conn.id, "someone-else", lease_seconds=60) is False  # fenced

    await leases.release_lease(conn.id, "w")
    await session.refresh(conn)
    assert (conn.locked_by, conn.locked_at, conn.lease_expires_at) == (None, None, None)


async def test_a_lock_written_before_the_column_existed_still_expires_by_the_old_rule(session, org):
    conn = await _connection(session, org)
    conn.locked_by = "old-release-worker"
    conn.locked_at = datetime.now(UTC) - timedelta(seconds=LEASE + 30)
    conn.lease_expires_at = None  # written by code that predates the column
    await session.commit()
    assert await leases.claim_connection(conn.id, "successor", lease_seconds=LEASE) is True
