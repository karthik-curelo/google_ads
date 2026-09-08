"""End-to-end: scheduler/runner → connector → destination → state (§25, §28, §37)."""

from datetime import UTC, date, datetime, timedelta

import pytest

from app.connectors import errors as E
from app.connectors.registry import RegistryEntry, registry
from app.models import (
    Connection,
    OAuthIdentity,
    ReportRow,
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
    yield
    StubConnector.fail_with = None


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
        backfill_start_date=date.today() - timedelta(days=4),
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

    rows = (await session.execute(ReportRow.__table__.select())).all()
    assert len(rows) == 10
    assert {r.stream for r in rows} == {"daily"}
    assert all(r.sessions in (10, 11) for r in rows)

    st = (await session.execute(SyncState.__table__.select())).one()
    assert st.cursor_value == date.today().isoformat()

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
    rows = (await session.execute(ReportRow.__table__.select())).all()
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
