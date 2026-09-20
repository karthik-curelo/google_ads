"""Deletion handling and source-vs-warehouse reconciliation.

LeadSquared has no deleted-since feed, so deletions are found by comparing windows
and confirmed by an independent by-id lookup before a row is tombstoned (never
deleted). These tests pin: nothing is tombstoned without both signals, history is kept,
a reappearing row un-deletes itself, and implausible mass-deletes are refused.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest
import respx
from sqlalchemy import func, select

from app.connectors import errors as E
from app.connectors.base import ConnectorContext, StaticTokenProvider
from app.connectors.leadsquared.connector import LeadSquaredCRMConnector
from app.connectors.leadsquared.reconcile import source_vs_warehouse, sweep_deletions
from app.models import Connection, LeadsquaredActivity, LeadsquaredLead, OAuthIdentity
from app.sync.runner import run_connection
from tests._lsq_fake import HOST, FakeLeadSquared, install

NOW = datetime.now(UTC).replace(tzinfo=None, microsecond=0)


def _t(hours_ago: float) -> datetime:
    return NOW - timedelta(hours=hours_ago)


# a range comfortably older than the sync's 24h lookback, and inside the 4-day backfill
RANGE = (_t(90), _t(40))


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    monkeypatch.setattr("app.connectors.http.random.uniform", lambda _a, _b: 0.0)


@pytest.fixture
def fake():
    with respx.mock(assert_all_called=False) as router:
        f = FakeLeadSquared()
        install(router, f)
        yield f


def _connector() -> LeadSquaredCRMConnector:
    return LeadSquaredCRMConnector(
        ConnectorContext(
            token_provider=StaticTokenProvider(),
            provider_settings={
                "leadsquared_access_key": "ak",
                "leadsquared_secret_key": "sk",
                "leadsquared_host": HOST,
                "leadsquared_rate_per_second": 1000,
                "leadsquared_burst": 1000,
            },
        )
    )


async def _synced(session, org, fake, streams) -> Connection:
    ident = OAuthIdentity(
        organization_id=org.id, provider="leadsquared", external_account_id="static", scopes=[]
    )
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="leadsquared",
        name="LSQ",
        resource_id="account",
        config={},
        streams=[{"stream": s, "sync_mode": "incremental", "enabled": True} for s in streams],
        backfill_start_date=(NOW - timedelta(days=5)).date(),
        lookback_days=3,
        schedule_interval_seconds=10800,
        enabled=True,
        next_run_at=datetime.now(UTC),
    )
    session.add(conn)
    await session.commit()
    out = await run_connection(conn.id, trigger="manual")
    assert out.status == "succeeded", out.error_message
    return conn


def _seed_activities(fake: FakeLeadSquared, n: int = 40) -> list[str]:
    rng = random.Random(4)
    ids = []
    for i in range(n):
        aid = f"a{i:03d}"
        fake.add_activity(206, aid, _t(rng.uniform(45, 85)))
        ids.append(aid)
    return ids


async def _live(session) -> int:
    return (
        await session.execute(
            select(func.count())
            .select_from(LeadsquaredActivity)
            .where(LeadsquaredActivity.deleted_at.is_(None))
        )
    ).scalar_one()


async def test_the_sweep_finds_source_deletions_dry_run_changes_nothing_apply_tombstones_only_those(
    session, org, fake
):
    ids = _seed_activities(fake)
    conn = await _synced(session, org, fake, ["booking_created"])
    assert await _live(session) == 40

    gone = {"a003", "a017", "a031"}
    for a in gone:
        fake.delete_activity(206, a)

    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "booking_created")
    dry = await sweep_deletions(c, stream, conn.id, *RANGE, apply=False)
    assert set(dry.deleted_ids) == gone and dry.tombstoned == 0 and not dry.applied
    assert await _live(session) == 40  # a dry run touches nothing

    done = await sweep_deletions(c, stream, conn.id, *RANGE, apply=True)
    await c.aclose()
    assert set(done.deleted_ids) == gone and done.tombstoned == 3 and done.applied

    assert await _live(session) == 37  # three tombstoned...
    total = (await session.execute(select(func.count()).select_from(LeadsquaredActivity))).scalar_one()
    assert total == 40  # ...and NO row was deleted: history is preserved
    rows = {r.prospect_activity_id: r for r in (await session.execute(select(LeadsquaredActivity))).scalars()}
    assert all(rows[a].deleted_at is not None for a in gone)
    assert all(rows[a].deleted_at is None for a in ids if a not in gone)


async def test_a_tombstoned_row_that_reappears_at_the_source_un_deletes_itself(session, org, fake):
    _seed_activities(fake)
    conn = await _synced(session, org, fake, ["booking_created"])
    fake.delete_activity(206, "a003")
    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "booking_created")
    await sweep_deletions(c, stream, conn.id, *RANGE, apply=True)
    await c.aclose()
    assert await _live(session) == 39

    fake.add_activity(206, "a003", _t(1), mx_Custom_6="restored")  # it comes back, freshly modified
    assert (await run_connection(conn.id, trigger="schedule")).status == "succeeded"

    row = (
        await session.execute(
            select(LeadsquaredActivity).where(LeadsquaredActivity.prospect_activity_id == "a003")
        )
    ).scalar_one()
    await session.refresh(row)
    assert row.deleted_at is None and row.raw["mx_Custom_6"] == "restored"
    assert await _live(session) == 40


async def test_a_row_merely_edited_later_is_not_mistaken_for_a_deletion(session, org, fake):
    """In the warehouse's old window but no longer in the source's (its ModifiedOn moved) — the
    by-id lookup says it still exists, so it must never be tombstoned."""
    _seed_activities(fake)
    conn = await _synced(session, org, fake, ["booking_created"])
    fake.add_activity(206, "a005", _t(0.2), created=_t(60), mx_Custom_6="edited after our last sync")

    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "booking_created")
    rep = await sweep_deletions(c, stream, conn.id, *RANGE, apply=True)
    await c.aclose()
    assert rep.deleted_ids == [] and "a005" in rep.still_exist_ids and rep.tombstoned == 0
    assert await _live(session) == 40


async def test_a_sweep_is_refused_until_the_sync_has_caught_up(session, org, fake):
    _seed_activities(fake)
    conn = await _synced(session, org, fake, ["booking_created"])
    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "booking_created")
    with pytest.raises(
        E.ConnectorError
    ) as exc:  # range runs up to "now": checkpoint hasn't passed it + lookback
        await sweep_deletions(c, stream, conn.id, _t(50), _t(0))
    await c.aclose()
    assert exc.value.code == E.ErrorCode.INVALID_CONFIGURATION and "caught up" in exc.value.message.lower()


async def test_an_implausible_mass_deletion_is_refused_not_applied(session, org, fake):
    ids = _seed_activities(fake, 120)
    conn = await _synced(session, org, fake, ["booking_created"])
    for a in ids[:60]:  # half of everything "vanishes": far more likely a broken sync than deletions
        fake.delete_activity(206, a)
    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "booking_created")
    with pytest.raises(E.ConnectorError) as exc:
        await sweep_deletions(c, stream, conn.id, *RANGE, apply=True)
    await c.aclose()
    assert "refusing to tombstone" in exc.value.message
    assert await _live(session) == 120  # nothing applied


async def test_rows_lacking_a_source_timestamp_block_the_sweep(session, org, fake):
    _seed_activities(fake, 5)
    conn = await _synced(session, org, fake, ["booking_created"])
    row = (await session.execute(select(LeadsquaredActivity))).scalars().first()
    row.source_modified_on = None  # a legacy row that has not been re-fetched yet
    await session.commit()
    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "booking_created")
    with pytest.raises(E.ConnectorError) as exc:
        await sweep_deletions(c, stream, conn.id, *RANGE)
    await c.aclose()
    assert "source_modified_on" in exc.value.message


async def test_deleted_leads_are_found_and_confirmed_by_the_empty_by_id_answer(session, org, fake):
    for i in range(30):
        fake.add_lead(f"lead{i:03d}", _t(45 + i))
    conn = await _synced(session, org, fake, ["leads"])
    for gone in ("lead004", "lead022"):
        del fake.leads[gone]

    c = _connector()
    stream = next(s for s in c.get_streams() if s.name == "leads")
    rep = await sweep_deletions(c, stream, conn.id, _t(90), _t(40), apply=True)
    await c.aclose()
    assert set(rep.deleted_ids) == {"lead004", "lead022"} and rep.tombstoned == 2
    live = (
        await session.execute(
            select(func.count()).select_from(LeadsquaredLead).where(LeadsquaredLead.deleted_at.is_(None))
        )
    ).scalar_one()
    assert live == 28


async def test_source_vs_warehouse_counts_agree_after_a_sync_and_expose_deletions(session, org, fake):
    _seed_activities(fake)
    for i in range(12):
        fake.add_lead(f"lead{i:03d}", _t(3 + i))
    conn = await _synced(session, org, fake, ["booking_created", "leads"])

    c = _connector()
    streams = [s for s in c.get_streams() if s.name in ("booking_created", "leads")]
    rows = {r.stream: r for r in await source_vs_warehouse(c, streams, conn.id)}
    assert (rows["booking_created"].source_total, rows["booking_created"].warehouse_live) == (40, 40)
    assert (rows["leads"].source_total, rows["leads"].warehouse_live) == (12, 12)
    assert rows["leads"].difference == 0 and rows["leads"].checkpoint

    fake.delete_activity(206, "a001")  # deleted at the source, not yet swept
    after = {r.stream: r for r in await source_vs_warehouse(c, streams, conn.id)}
    await c.aclose()
    assert after["booking_created"].difference == -1  # warehouse holds one the source no longer does
