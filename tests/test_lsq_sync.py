"""End-to-end LeadSquared sync: real connector -> window fetcher -> transactional
upsert -> reconciliation -> checkpoint, against the faithful in-memory API.

The properties under test are the ones the audit demanded: complete, lossless,
idempotent, and safe to crash or replay at any point.
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, date, datetime, time, timedelta

import httpx
import pytest
import respx
from sqlalchemy import func, select

from app.connectors import errors as E
from app.connectors.leadsquared.activity_catalog import ACTIVITY_TYPES
from app.core.config import get_settings
from app.models import (
    Connection,
    LeadsquaredActivity,
    LeadsquaredLead,
    OAuthIdentity,
    SyncError,
    SyncRun,
    SyncState,
    SyncStreamStat,
)
from app.sync import runner as runner_mod
from app.sync.runner import RunProgress, run_connection
from app.sync.scheduler import SyncScheduler
from app.sync.state import TS_STATE_VERSION
from app.sync.writer import DestinationWriter
from tests._lsq_fake import FakeLeadSquared, install

NOW = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
# A burst pinned to a whole past day. Data placed "N hours ago" makes the number of API calls a
# backfill needs depend on where the clock happens to sit against UTC midnight (the first window
# is a whole day), so any test that asserts on call counts anchors its data here instead.
PINNED_DAY = datetime.combine((NOW - timedelta(days=3)).date(), time.min)


def _t(hours_ago: float) -> datetime:
    return NOW - timedelta(hours=hours_ago)


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    monkeypatch.setattr("app.connectors.http.random.uniform", lambda _a, _b: 0.0)


@pytest.fixture
def fake():
    with respx.mock(assert_all_called=False) as router:
        f = FakeLeadSquared()
        install(router, f)
        yield f


async def _connection(
    session,
    org,
    *,
    streams: list[str] | None = None,
    config=None,
    interval=10800,
    backfill_start: date | None = None,
) -> Connection:
    ident = OAuthIdentity(
        organization_id=org.id, provider="leadsquared", external_account_id="static", scopes=[]
    )
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="leadsquared",
        name="LeadSquared",
        resource_id="account",
        config=config or {},
        streams=[{"stream": s, "sync_mode": "incremental", "enabled": True} for s in (streams or [])],
        backfill_start_date=backfill_start or (NOW - timedelta(days=4)).date(),
        lookback_days=3,
        schedule_interval_seconds=interval,
        enabled=True,
        next_run_at=datetime.now(UTC),
    )
    session.add(conn)
    await session.commit()
    return conn


async def _count(session, model) -> int:
    return (await session.execute(select(func.count()).select_from(model))).scalar_one()


async def _state(session, stream: str) -> SyncState | None:
    return (await session.execute(select(SyncState).where(SyncState.stream == stream))).scalar_one_or_none()


def _cp(state: SyncState | None) -> datetime | None:
    return datetime.strptime(state.cursor_value, "%Y-%m-%dT%H:%M:%SZ") if state else None


def _burst_activities(
    fake: FakeLeadSquared,
    code: int,
    n: int,
    *,
    ties: int = 8,
    prefix: str = "a",
    on_day: datetime | None = None,
) -> None:
    """`n` activities over ~10h, but only `ties` distinct seconds per hour — many ties.

    By default the last ~10h; `on_day` pins them to hours 2-12 of that day instead."""
    rng = random.Random(11)
    for i in range(n):
        hour = rng.randrange(2, 12)
        when = on_day + timedelta(hours=hour) if on_day else _t(hour)
        fake.add_activity(code, f"{prefix}{i:05d}", when + timedelta(seconds=i % ties))


# --- complete backfill --------------------------------------------------------------


async def test_backfill_is_complete_reconciled_and_checkpointed(session, org, fake):
    _burst_activities(fake, 206, 3200)  # > 3 pages, heavy timestamp ties
    for i in range(1500):
        fake.add_lead(f"lead{i:05d}", _t(3 + (i % 20)), Source="google_lp", ProspectStage="Prospect")
    conn = await _connection(session, org, streams=["booking_created", "leads"])

    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "succeeded", out.error_message
    assert await _count(session, LeadsquaredActivity) == 3200
    assert await _count(session, LeadsquaredLead) == 1500
    ids = (await session.execute(select(LeadsquaredActivity.prospect_activity_id))).scalars().all()
    assert len(set(ids)) == 3200  # no duplicate logical records

    stat = (
        await session.execute(select(SyncStreamStat).where(SyncStreamStat.stream == "booking_created"))
    ).scalar_one()
    r = stat.reconciliation
    assert r["source_count"] == r["fetched"] == r["distinct"] == r["persisted"] >= 3200
    assert r["mismatches"] == 0 and r["skipped"] == 0
    # nothing was ever read past page 1 except a genuine single-second overflow
    assert all(req["body"]["Paging"]["PageIndex"] == 1 for req in fake.calls_to("RetrieveByActivityEvent"))

    st = await _state(session, "booking_created")
    assert st.state["v"] == TS_STATE_VERSION and st.cursor_value.endswith("Z")
    assert st.cursor_value >= (NOW - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")


async def test_complete_lead_payload_is_preserved_in_the_warehouse_row(session, org, fake):
    attrs = {f"mx_F{i}": f"v{i}" for i in range(150)}
    attrs.update(
        ProspectStage="Opportunity",
        FirstName="Asha",
        mx_Disposition="Callback",
        OwnerId="o-1",
        OwnerIdName="Priya",
        LeadConversionDate="2026-09-01 07:00:00.000",
        mx_Patient_Tags="vip",
        mx_Outbound_Call_COunter="4",
        ProspectActivityDate_Max="2026-09-01 08:00:00.000",
    )
    fake.add_lead("p1", _t(2), **attrs)
    conn = await _connection(session, org, streams=["leads"])
    assert (await run_connection(conn.id, trigger="manual")).status == "succeeded"

    row = (await session.execute(select(LeadsquaredLead))).scalar_one()
    for k, v in attrs.items():
        assert row.raw[k] == v, f"{k} lost on its way to Postgres"
    assert row.raw["LeadLastModifiedOn"]
    assert row.prospect_stage == "Opportunity" and row.owner_id == "o-1"
    assert row.source_modified_on is not None and row.deleted_at is None


async def test_every_activity_type_lands_in_the_raw_table_with_its_discriminator(session, org, fake):
    for code in ACTIVITY_TYPES:
        fake.add_activity(code, f"act-{code}", _t(2), mx_Custom_1=f"payload-{code}")
    fake.add_lead("p1", _t(2))
    conn = await _connection(session, org)  # streams=[] -> every declared stream

    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "succeeded", out.error_message
    rows = (await session.execute(select(LeadsquaredActivity))).scalars().all()
    assert len(rows) == 84
    assert {r.activity_event for r in rows} == set(ACTIVITY_TYPES)
    assert all(r.raw["mx_Custom_1"] == f"payload-{r.activity_event}" for r in rows)
    assert all(r.activity_event_name == ACTIVITY_TYPES[r.activity_event] for r in rows)
    assert len(out.streams_ok) == 85 and not out.streams_failed


# --- idempotency, incremental overlap, updates --------------------------------------


async def test_repeating_the_same_sync_changes_nothing(session, org, fake):
    _burst_activities(fake, 206, 1200)
    conn = await _connection(session, org, streams=["booking_created"])
    first = await run_connection(conn.id, trigger="manual")
    before = (
        await session.execute(select(LeadsquaredActivity.prospect_activity_id, LeadsquaredActivity.raw))
    ).all()

    second = await run_connection(conn.id, trigger="schedule")  # re-reads the lookback overlap

    assert first.records_inserted == 1200 and second.records_inserted == 0
    # "updated" means REWRITTEN. Everything in the overlap was already current, so the
    # guarded upsert left it alone: zero updates, all 1200 counted as unchanged.
    assert second.records_updated == 0
    stat = (
        await session.execute(select(SyncStreamStat).where(SyncStreamStat.sync_run_id == second.run_id))
    ).scalar_one()
    assert stat.reconciliation["unchanged"] == 1200 and stat.reconciliation["mismatches"] == 0
    after = (
        await session.execute(select(LeadsquaredActivity.prospect_activity_id, LeadsquaredActivity.raw))
    ).all()
    assert sorted((i, str(r)) for i, r in before) == sorted((i, str(r)) for i, r in after)
    assert await _count(session, LeadsquaredActivity) == 1200


async def test_incremental_run_picks_up_updates_and_new_rows_without_duplicating(session, org, fake):
    fake.add_activity(206, "old", _t(20), Status="Active", mx_Custom_6="100")
    fake.add_activity(206, "edited-later", _t(20), mx_Custom_6="100")
    conn = await _connection(session, org, streams=["booking_created"])
    await run_connection(conn.id, trigger="manual")

    # the source edits one row (ModifiedOn moves forward) and creates another
    fake.add_activity(206, "edited-later", _t(1), created=_t(20), mx_Custom_6="999", Status="Updated")
    fake.add_activity(206, "brand-new", _t(0.5), mx_Custom_6="5")
    out = await run_connection(conn.id, trigger="schedule")

    assert out.status == "succeeded"
    rows = {r.prospect_activity_id: r for r in (await session.execute(select(LeadsquaredActivity))).scalars()}
    assert set(rows) == {"old", "edited-later", "brand-new"}
    assert (
        rows["edited-later"].raw["mx_Custom_6"] == "999" and rows["edited-later"].raw["Status"] == "Updated"
    )
    assert rows["old"].raw["mx_Custom_6"] == "100"
    assert out.records_inserted == 1  # only brand-new
    assert out.records_updated == 1  # only the row that really changed; "old" was re-read but left alone


async def test_a_stale_fetch_can_never_overwrite_a_newer_row(session, org, fake):
    """The upsert guard runs inside the INSERT ... ON CONFLICT statement, so a replayed
    or out-of-order write loses no matter which worker commits last."""
    conn = await _connection(session, org, streams=["booking_created"])
    fake.add_activity(206, "a1", _t(1), mx_Custom_6="NEW")
    await run_connection(conn.id, trigger="manual")

    from app.connectors.leadsquared.connector import LeadSquaredCRMConnector

    c = LeadSquaredCRMConnector.__new__(LeadSquaredCRMConnector)
    stream = next(s for s in LeadSquaredCRMConnector.declared_streams() if s.name == "booking_created")
    stale = c._to_activity_record(
        stream,
        {
            "ProspectActivityId": "a1",
            "ActivityEvent": "206",
            "CreatedOn": _t(3).strftime("%Y-%m-%d %H:%M:%S"),
            "ModifiedOn": _t(3).strftime("%Y-%m-%d %H:%M:%S"),
            "mx_Custom_6": "STALE",
        },
        {},
    )
    writer = DestinationWriter(
        organization_id=org.id,
        connection_id=conn.id,
        connector_id="leadsquared",
        provider="leadsquared",
        resource_id="account",
        sync_run_id=None,
    )
    await writer.write_records(stream, [stale])

    row = (await session.execute(select(LeadsquaredActivity))).scalar_one()
    assert row.raw["mx_Custom_6"] == "NEW"


async def test_a_legacy_date_cursor_is_ignored_and_the_stream_is_reswept(session, org, fake):
    """Cursors written by the old date-based sync must not be trusted: they are the very
    thing that left history before the backfill start uncovered."""
    fake.add_activity(206, "a1", _t(30))  # older than a day: only a full sweep finds it
    conn = await _connection(session, org, streams=["booking_created"])
    session.add(
        SyncState(
            connection_id=conn.id,
            stream="booking_created",
            cursor_field="CreatedOn",
            cursor_value=NOW.strftime("%Y-%m-%d"),
            state={},
        )
    )
    await session.commit()

    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "succeeded" and await _count(session, LeadsquaredActivity) == 1
    st = await _state(session, "booking_created")
    assert st.state["v"] == TS_STATE_VERSION and "T" in st.cursor_value


# --- checkpoint safety and crash recovery ----------------------------------------------


async def test_checkpoint_holds_at_the_last_good_window_when_a_later_window_fails(session, org, fake):
    _burst_activities(fake, 206, 3000)
    conn = await _connection(session, org, streams=["booking_created"])

    def fail_once_two_windows_have_been_served(c):
        if "RetrieveByActivityEvent" in c["path"] and fake.leaf_responses >= 2:
            return httpx.Response(
                500,
                json={
                    "Status": "Error",
                    "ExceptionType": "MXInvalidInputException",
                    "ExceptionMessage": "boom",
                },
            )
        return None

    fake.inject.append(fail_once_two_windows_have_been_served)
    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "failed" and out.streams_failed == ["booking_created"]
    st = await _state(session, "booking_created")
    persisted = await _count(session, LeadsquaredActivity)
    assert 0 < persisted < 3000
    stat = (await session.execute(select(SyncStreamStat))).scalar_one()
    assert stat.checkpoint_after == st.cursor_value != None  # noqa: E711
    # every row at or before the checkpoint is present: the checkpoint never outran the data
    cp = datetime.strptime(st.cursor_value, "%Y-%m-%dT%H:%M:%SZ")
    expected_before_cp = sum(
        1
        for r in fake.activities[206].values()
        if datetime.strptime(r["ModifiedOn"], "%Y-%m-%d %H:%M:%S") <= cp
    )
    assert persisted >= expected_before_cp

    # heal the source: the next run resumes and finishes with nothing lost or duplicated
    fake.inject.clear()
    out2 = await run_connection(conn.id, trigger="schedule")
    assert out2.status == "succeeded"
    ids = (await session.execute(select(LeadsquaredActivity.prospect_activity_id))).scalars().all()
    assert len(ids) == len(set(ids)) == 3000


async def test_crash_after_the_write_but_before_the_checkpoint_loses_nothing(session, org, fake, monkeypatch):
    _burst_activities(fake, 206, 2500)
    conn = await _connection(session, org, streams=["booking_created"])
    real = runner_mod.commit_state_ts
    calls = {"n": 0}

    async def crash_on_second_checkpoint(*a, **kw):
        # Empty windows also checkpoint; the crash must land right after a window that
        # actually wrote rows — the dangerous spot (rows committed, checkpoint not).
        if kw.get("added_records", 0) > 0:
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("process killed between commit and checkpoint")
        return await real(*a, **kw)

    monkeypatch.setattr(runner_mod, "commit_state_ts", crash_on_second_checkpoint)
    out = await run_connection(conn.id, trigger="manual")
    assert out.status == "failed"
    written_when_crashed = await _count(session, LeadsquaredActivity)
    st = await _state(session, "booking_created")
    assert written_when_crashed > 0

    monkeypatch.setattr(runner_mod, "commit_state_ts", real)
    out2 = await run_connection(conn.id, trigger="schedule")
    assert out2.status == "succeeded"
    ids = (await session.execute(select(LeadsquaredActivity.prospect_activity_id))).scalars().all()
    assert len(ids) == len(set(ids)) == 2500  # the re-read window was an idempotent no-op
    assert st is not None


async def test_a_write_that_rolls_back_leaves_no_partial_window_and_no_checkpoint(
    session, org, fake, monkeypatch
):
    fake.add_activity(206, "a1", _t(2))
    fake.add_activity(206, "a2", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    real = DestinationWriter.write_records

    async def boom(self, stream, records, **kw):
        raise RuntimeError("deadlock detected")

    monkeypatch.setattr(DestinationWriter, "write_records", boom)
    out = await run_connection(conn.id, trigger="manual")
    assert out.status == "failed"
    assert await _count(session, LeadsquaredActivity) == 0
    # Verified-empty windows before the data may checkpoint, but never past the data.
    st = await _state(session, "booking_created")
    assert st is None or _cp(st) < _t(2)

    monkeypatch.setattr(DestinationWriter, "write_records", real)
    assert (await run_connection(conn.id, trigger="manual")).status == "succeeded"
    assert await _count(session, LeadsquaredActivity) == 2


# --- reconciliation ---------------------------------------------------------------------


async def test_a_window_the_destination_did_not_fully_persist_fails_and_holds_the_checkpoint(
    session, org, fake, monkeypatch
):
    for i in range(20):
        fake.add_activity(206, f"a{i}", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    real = DestinationWriter.write_records

    async def silently_drops_one(self, stream, records, **kw):
        return await real(self, stream, records[:-1], **kw)  # a write that quietly loses a row

    monkeypatch.setattr(DestinationWriter, "write_records", silently_drops_one)
    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "failed" and out.error_code == E.ErrorCode.RECONCILIATION_FAILED
    st = await _state(session, "booking_created")
    assert st is None or _cp(st) < _t(2)  # the checkpoint never passed the window that did not reconcile
    err = (await session.execute(select(SyncError))).scalars().first()
    assert err.code == E.ErrorCode.RECONCILIATION_FAILED and err.retryable
    stat = (await session.execute(select(SyncStreamStat))).scalar_one()
    assert stat.status == "failed" and stat.reconciliation["mismatches"] == 1 and stat.records_failed >= 1


async def test_one_bad_stream_makes_the_run_partial_not_silently_successful(session, org, fake):
    fake.add_activity(206, "a1", _t(2))
    fake.add_activity(223, "c1", _t(2))
    conn = await _connection(session, org, streams=["booking_created", "booking_cancelled"])

    def malformed_for_223(c):
        if "RetrieveByActivityEvent" in c["path"] and c["body"]["Parameter"]["ActivityEvent"] == 223:
            return httpx.Response(200, json={"RecordCount": 4})  # rows promised, no list
        return None

    fake.inject.append(malformed_for_223)
    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "partial_success"
    assert out.streams_ok == ["booking_created"] and out.streams_failed == ["booking_cancelled"]
    st223 = await _state(session, "booking_cancelled")
    assert st223 is None or _cp(st223) < _t(2)  # a malformed page never advanced it past the data
    assert await _count(session, LeadsquaredActivity) == 1  # the healthy stream still landed


async def test_a_logical_500_fails_the_stream_immediately_without_retries(session, org, fake):
    fake.add_activity(206, "a1", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    sent = {"n": 0}

    def logical_error(c):
        if "Retrieve" in c["path"]:
            sent["n"] += 1
            return httpx.Response(
                500,
                json={
                    "Status": "Error",
                    "ExceptionType": "MXInvalidInputException",
                    "ExceptionMessage": "bad",
                },
            )
        return None

    fake.inject.append(logical_error)
    out = await run_connection(conn.id, trigger="manual")
    assert out.status == "failed" and out.will_retry is False and out.retry_count == 0
    assert sent["n"] == 1  # one request, not five


# --- observability -----------------------------------------------------------------------


async def test_every_run_and_stream_exposes_the_required_fields(session, org, fake):
    _burst_activities(fake, 206, 900)
    conn = await _connection(session, org, streams=["booking_created"])
    out = await run_connection(conn.id, trigger="schedule", worker_id="worker-A")

    run = (await session.execute(select(SyncRun))).scalar_one()
    assert run.execution_id and len(run.execution_id) == 32
    assert run.worker_id == "worker-A" and run.trigger == "schedule"
    assert run.status == "succeeded" and run.started_at and run.finished_at and run.duration_ms is not None
    assert run.records_fetched == out.records_fetched == 900
    assert run.records_inserted == 900 and run.records_updated == 0 and run.records_skipped == 0
    assert run.records_failed == 0 and run.retry_count == 0 and run.rate_limit_events == 0
    assert run.api_calls >= 1
    assert run.state_before == {} and "booking_created" in run.state_after

    stat = (await session.execute(select(SyncStreamStat))).scalar_one()
    assert stat.stream == "booking_created" and stat.status == "succeeded"
    assert stat.started_at and stat.finished_at and stat.duration_ms is not None
    assert stat.api_calls >= 1 and stat.checkpoint_before is None and stat.checkpoint_after
    assert stat.reconciliation["windows"] >= 1


async def test_retry_and_rate_limit_counts_are_recorded(session, org, fake):
    fake.add_activity(206, "a1", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    state = {"n": 0}

    def throttled_twice(c):
        if "Retrieve" in c["path"]:
            state["n"] += 1
            if state["n"] <= 2:
                return httpx.Response(
                    429,
                    json={
                        "Status": "Error",
                        "ExceptionType": "MXThrottleException",
                        "ExceptionMessage": "Too many calls",
                    },
                )
        return None

    fake.inject.append(throttled_twice)
    out = await run_connection(conn.id, trigger="manual")
    assert out.status == "succeeded"
    run = (await session.execute(select(SyncRun))).scalar_one()
    assert run.retry_count == 2 and run.rate_limit_events == 2
    stat = (await session.execute(select(SyncStreamStat))).scalar_one()
    assert stat.retry_count == 2 and stat.rate_limit_events == 2


async def test_status_is_retrying_while_the_client_backs_off(session, org):
    conn = await _connection(session, org)
    run = SyncRun(
        connection_id=conn.id, organization_id=org.id, trigger="manual", status="running", phase="fetching"
    )
    session.add(run)
    await session.commit()

    progress = RunProgress(run.id, min_interval=0)
    progress.on_retry("RATE_LIMIT_ERROR", 1, 4.0)
    await asyncio.sleep(0.1)
    await session.refresh(run)
    assert run.status == "retrying" and "RATE_LIMIT_ERROR" in (run.phase_detail or "")

    progress.note("resumed")
    await asyncio.sleep(0.1)
    await session.refresh(run)
    assert run.status == "running"


# --- scheduled execution with real configuration -------------------------------------------


async def test_a_scheduled_run_with_real_configuration_succeeds(session, org, fake):
    fake.add_activity(206, "a1", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    sched = SyncScheduler(worker_id="sched-1")

    await sched._tick()  # the scheduler process claims and runs it: trigger == "schedule"
    async with asyncio.timeout(10):
        while sched.active:
            await asyncio.sleep(0.02)
    await sched.stop()

    run = (await session.execute(select(SyncRun))).scalar_one()
    assert run.trigger == "schedule" and run.status == "succeeded" and run.worker_id == "sched-1"
    await session.refresh(conn)
    assert conn.status == "healthy" and conn.last_scheduled_success_at is not None
    assert conn.consecutive_scheduled_failures == 0


async def test_a_manual_success_must_not_hide_a_broken_schedule(session, org, fake, monkeypatch):
    """The production incident: the scheduler's process lacked LEADSQUARED_* while the
    operator's manual runs (with the env present) kept succeeding and kept flipping the
    connection back to 'healthy'."""
    fake.add_activity(206, "a1", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    settings = get_settings()
    good_key = settings.leadsquared_access_key

    monkeypatch.setattr(settings, "leadsquared_access_key", "")  # the scheduler process's environment
    bad = await run_connection(conn.id, trigger="schedule")
    assert bad.status == "failed" and bad.error_code == E.ErrorCode.INVALID_CONFIGURATION
    await session.refresh(conn)
    assert conn.status == "invalid_configuration" and conn.consecutive_scheduled_failures == 1

    monkeypatch.setattr(settings, "leadsquared_access_key", good_key)  # operator's environment
    manual = await run_connection(conn.id, trigger="manual")
    assert manual.status == "succeeded"  # data did land...
    await session.refresh(conn)
    assert conn.status != "healthy"  # ...but the connection is NOT declared healthy
    assert conn.consecutive_scheduled_failures == 1
    assert "scheduled" in (conn.status_detail or "").lower()

    ok = await run_connection(conn.id, trigger="schedule")  # only a scheduled success clears it
    assert ok.status == "succeeded"
    await session.refresh(conn)
    assert conn.status == "healthy" and conn.consecutive_scheduled_failures == 0
    assert conn.last_scheduled_success_at is not None


async def test_response_envelope_attributes_are_not_stored_and_an_unchanged_lead_is_never_rewritten(
    session, org, fake
):
    """Live finding: LeadSquared appends `Total` (the QUERY's total, not a lead field) to every
    lead in a response — 153 on one read, 108 on the next. Stored in `raw`, it made every
    re-read look like a change and rewrote every unchanged lead on every overlap."""
    for i in range(50):
        fake.add_lead(f"lead{i:03d}", _t(3 + i / 10), Source="google_lp", ProspectStage="Prospect")
    conn = await _connection(session, org, streams=["leads"])
    first = await run_connection(conn.id, trigger="manual")
    assert first.records_inserted == 50

    row = (await session.execute(select(LeadsquaredLead))).scalars().first()
    assert "Total" not in row.raw  # envelope noise is not a lead attribute
    assert row.raw["ProspectStage"] == "Prospect"  # real attributes are all still there

    # a different window shape changes the echoed `Total`; the leads themselves did not change
    fake.add_lead("extra", _t(0.5), Source="direct")
    second = await run_connection(conn.id, trigger="schedule")
    assert second.records_inserted == 1
    assert second.records_updated == 0, (
        "unchanged leads were rewritten because a volatile attribute leaked into raw"
    )


# --- daily API budget: a backfill must never be able to spend the account's whole quota ---------


async def test_a_stream_that_reaches_its_budget_share_defers_cleanly_and_resumes_to_completion(
    session, org, fake
):
    _burst_activities(fake, 206, 3000, on_day=PINNED_DAY)
    conn = await _connection(
        session,
        org,
        streams=["booking_created"],
        config={"daily_api_budget": 6},  # the whole burst needs 12 calls
        backfill_start=PINNED_DAY.date(),
    )

    first = await run_connection(conn.id, trigger="manual")
    assert first.status == "succeeded" and first.warnings >= 1  # deferred, not failed
    stat = (await session.execute(select(SyncStreamStat))).scalars().first()
    assert stat.reconciliation["budget_deferred"] is True and stat.reconciliation["mismatches"] == 0
    have = await _count(session, LeadsquaredActivity)
    assert 0 < have < 3000
    assert first.api_calls <= 14 + 3  # the cap is honoured (a window in flight may finish)
    cp = _cp(await _state(session, "booking_created"))

    # the checkpoint sits exactly where the data stops: everything at/before it is persisted
    expected = sum(
        1
        for r in fake.activities[206].values()
        if datetime.strptime(r["ModifiedOn"], "%Y-%m-%d %H:%M:%S") <= cp
    )
    assert have >= expected

    # a later day (the 24h rolling window has moved on) resumes and finishes; nothing lost/duplicated
    conn_row = await session.get(Connection, conn.id)
    conn_row.config = {"daily_api_budget": 0}  # unlimited
    await session.commit()
    second = await run_connection(conn.id, trigger="schedule")
    assert second.status == "succeeded"
    ids = (await session.execute(select(LeadsquaredActivity.prospect_activity_id))).scalars().all()
    assert len(ids) == len(set(ids)) == 3000


async def test_budget_already_spent_by_earlier_runs_counts_against_todays_allowance(session, org, fake):
    fake.add_activity(206, "a1", _t(2))
    conn = await _connection(session, org, streams=["booking_created"], config={"daily_api_budget": 10})
    session.add(
        SyncRun(
            connection_id=conn.id,
            organization_id=org.id,
            trigger="schedule",
            status="succeeded",
            api_calls=9,  # nine of today's ten calls were spent by earlier runs
        )
    )
    await session.commit()

    out = await run_connection(conn.id, trigger="schedule")
    assert out.status == "succeeded" and out.api_calls <= 3  # only the remaining allowance is spendable


async def test_the_budget_is_shared_so_one_huge_backlog_cannot_starve_the_streams_behind_it(
    session, org, fake
):
    _burst_activities(fake, 22, 3000, prefix="huge", on_day=PINNED_DAY)  # a giant backlog FIRST...
    for code in (206, 223):
        for i in range(5):
            # ...and two small streams behind it
            fake.add_activity(code, f"s{code}-{i}", PINNED_DAY + timedelta(hours=2))
    conn = await _connection(
        session,
        org,
        streams=["activity_22", "booking_created", "booking_cancelled"],
        config={"daily_api_budget": 14},  # the giant alone would need 12 of them
        backfill_start=PINNED_DAY.date(),
    )
    out = await run_connection(conn.id, trigger="manual")
    assert out.status == "succeeded"
    rows = (await session.execute(select(LeadsquaredActivity.activity_event))).scalars().all()
    assert rows.count(206) == 5 and rows.count(223) == 5, "the small streams still completed"
    assert 0 < rows.count(22) < 3000, "the giant stream took only its share and deferred the rest"


async def test_a_connector_without_a_budget_is_never_capped(session, org):
    """Other connectors' behaviour is unchanged: the default budget is LeadSquared-only."""
    from app.core.config import get_settings
    from app.sync.runner import _default_daily_budget

    s = get_settings()
    assert _default_daily_budget(s, "google") == 0 and _default_daily_budget(s, "meta") == 0
    assert _default_daily_budget(s, "leadsquared") == s.leadsquared_daily_api_budget > 0


# --- observability is reachable through the API, and provider limiters are shared ---------------------


async def test_run_and_stream_observability_is_exposed_by_the_api(session, org, fake, auth_headers):
    from httpx import ASGITransport, AsyncClient

    from app.main import create_app

    fake.add_activity(206, "a1", _t(2))
    fake.add_activity(206, "a2", _t(2))
    conn = await _connection(session, org, streams=["booking_created"])
    out = await run_connection(conn.id, trigger="schedule", worker_id="worker-A")

    async with AsyncClient(transport=ASGITransport(app=create_app()), base_url="http://t") as client:
        run = (await client.get(f"/api/v1/sync-runs/{out.run_id}", headers=auth_headers)).json()
        conn_view = (await client.get(f"/api/v1/connections/{conn.id}", headers=auth_headers)).json()

    for key in (
        "connector_id", "connection_id", "execution_id", "worker_id", "status", "started_at", "finished_at",
        "duration_ms", "records_fetched", "records_inserted", "records_updated", "records_skipped",
        "records_failed", "api_calls", "retry_count", "rate_limit_events", "checkpoint_before",
        "checkpoint_after", "error_code", "error_message",
    ):  # fmt: skip
        assert key in run, f"run is missing {key}"
    assert (
        run["connector_id"] == "leadsquared"
        and run["worker_id"] == "worker-A"
        and len(run["execution_id"]) == 32
    )
    assert run["status"] == "succeeded" and run["records_inserted"] == 2

    (stream,) = run["streams"]
    for key in (
        "stream", "status", "started_at", "finished_at", "duration_ms", "records_failed", "api_calls",
        "retry_count", "rate_limit_events", "checkpoint_before", "checkpoint_after", "reconciliation",
    ):  # fmt: skip
        assert key in stream, f"stream stats are missing {key}"
    assert stream["reconciliation"]["source_count"] == stream["reconciliation"]["persisted"] == 2
    assert stream["checkpoint_after"] and stream["checkpoint_before"] is None

    assert conn_view["consecutive_scheduled_failures"] == 0 and "last_scheduled_success_at" in conn_view


def test_google_and_meta_clients_share_one_limiter_per_provider_api():
    from app.connectors.google.analytics import GoogleAnalyticsConnector
    from app.connectors.google.search_console import GoogleSearchConsoleConnector
    from app.connectors.meta.ads import MetaAdsConnector
    from app.connectors.meta.instagram import InstagramInsightsConnector
    from tests._fakes import make_ctx

    ga_a, ga_b = GoogleAnalyticsConnector(make_ctx()), GoogleAnalyticsConnector(make_ctx())
    gsc = GoogleSearchConsoleConnector(make_ctx())
    assert ga_a.http.limiter is ga_b.http.limiter  # two GA4 connections share GA4's bucket...
    assert ga_a.http.limiter is not gsc.http.limiter  # ...but not Search Console's separate quota

    ctx = make_ctx(provider_settings={"meta_app_id": "app-1", "http_timeout_seconds": 30.0})
    ads, ig = MetaAdsConnector(ctx), InstagramInsightsConnector(ctx)
    assert ads.http.limiter is ig.http.limiter  # Graph usage is metered per app across products


def test_a_big_backlog_is_not_capped_at_an_equal_share_when_the_streams_behind_it_are_small():
    """Live finding: 85 streams, 80 of them empty. An equal split gave the 250k-row lead backlog ~70
    calls; it should get almost everything except a reserve for each stream still to run."""
    from app.sync.runner import ApiBudget

    class _NoHttp:  # a connector with no HTTP client yet: zero calls made
        pass

    b = ApiBudget(6000, 0, _NoHttp(), 85)
    assert b.stream_cap() == 6000 - 5 * 84  # 5,580, not 70
    b.streams_left = 1
    assert b.stream_cap() == 6000  # the last stream may use whatever is left

    tiny = ApiBudget(100, 0, _NoHttp(), 85)
    assert tiny.stream_cap() >= 1  # never zero: every stream can always make progress
    assert ApiBudget(0, 0, _NoHttp(), 85).stream_cap() is None  # 0 = unlimited


async def test_a_bulk_burst_of_subsecond_timestamps_lands_completely_in_the_warehouse(session, org, fake):
    """End to end: the 12:10-13:09 bulk-hour shape (about 40 leads/second, millisecond timestamps).
    Live, adjacent windows lost 6-10% of these to the cracks between them; the warehouse must hold
    every lead the source holds."""
    rng = random.Random(21)
    for i in range(6000):
        fake.add_lead(
            f"bulk{i:05d}", _t(3) + timedelta(seconds=rng.randrange(150)), ProspectStage="Reports Preparing"
        )
    conn = await _connection(session, org, streams=["leads"])

    out = await run_connection(conn.id, trigger="manual")

    assert out.status == "succeeded"
    assert await _count(session, LeadsquaredLead) == 6000, "leads were lost between windows"
    stat = (await session.execute(select(SyncStreamStat))).scalar_one()
    assert stat.reconciliation["windows"] > 3 and stat.reconciliation["mismatches"] == 0
