from datetime import date

import pytest

from app.connectors.base import EntityRecord, Record, StreamDefinition
from app.connectors.validation import build_json_schema
from app.models import AdEntity, Connection, OAuthIdentity, ReportRow, SkippedRecord
from app.sync.state import commit_state, load_state, reset_state
from app.sync.writer import DestinationWriter

pytestmark = pytest.mark.asyncio


async def _connection(session, org) -> Connection:
    ident = OAuthIdentity(
        organization_id=org.id,
        provider="google",
        external_account_id="acc-1",
        email="u@example.com",
        scopes=["s"],
    )
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="google_analytics",
        name="c",
        resource_id="123",
        config={},
        streams=[],
    )
    session.add(conn)
    await session.commit()
    return conn


FACT_STREAM = StreamDefinition(
    name="daily_overview",
    description="",
    json_schema=build_json_schema(["date"], ["sessions"]),
    primary_key=["date"],
)


async def test_state_load_commit_monotonic(session, org):
    conn = await _connection(session, org)
    st = await load_state(conn.id, "daily_overview")
    assert st.cursor_value is None

    st = await commit_state(st, reached=date(2026, 8, 10), added_records=5)
    assert st.cursor_value == "2026-08-10"

    st2 = await load_state(conn.id, "daily_overview")
    assert st2.cursor_value == "2026-08-10" and st2.records_synced == 5

    st2 = await commit_state(st2, reached=date(2026, 8, 5))  # older — must not rewind
    assert st2.cursor_value == "2026-08-10"

    removed = await reset_state(conn.id)
    assert removed == 1
    assert (await load_state(conn.id, "daily_overview")).cursor_value is None


async def test_writer_inserts_then_upserts_and_counts(session, org):
    conn = await _connection(session, org)
    writer = DestinationWriter(
        organization_id=org.id,
        connection_id=conn.id,
        connector_id="google_analytics",
        provider="google",
        resource_id="123",
        sync_run_id=None,
    )
    recs = [
        Record(
            stream="daily_overview",
            key_values={"date": "2026-08-01"},
            date=date(2026, 8, 1),
            dimensions={"date": "2026-08-01"},
            metrics={"sessions": 10},
            measures={"sessions": 10, "users": 7},
        ),
        Record(
            stream="daily_overview",
            key_values={"date": "2026-08-02"},
            date=date(2026, 8, 2),
            dimensions={"date": "2026-08-02"},
            metrics={"sessions": 20},
            measures={"sessions": 20},
        ),
    ]
    r1 = await writer.write_records(FACT_STREAM, recs)
    assert (r1.inserted, r1.updated, r1.skipped) == (2, 0, 0)

    recs[0].measures["sessions"] = 99
    r2 = await writer.write_records(FACT_STREAM, recs)
    assert (r2.inserted, r2.updated) == (0, 2)

    rows = (await session.execute(ReportRow.__table__.select())).all()
    assert len(rows) == 2
    aug1 = next(row for row in rows if str(row.date) == "2026-08-01")
    assert aug1.sessions == 99 and aug1.users == 7


async def test_writer_logs_skipped_records(session, org):
    conn = await _connection(session, org)
    writer = DestinationWriter(
        organization_id=org.id,
        connection_id=conn.id,
        connector_id="google_analytics",
        provider="google",
        resource_id="123",
        sync_run_id=None,
    )
    bad = [
        Record(stream="daily_overview", key_values={"date": None}, date=None),
        Record(stream="daily_overview", key_values={"date": "2026-08-01"}, date=date(2026, 8, 1)),
    ]
    result = await writer.write_records(FACT_STREAM, bad)
    assert result.inserted == 1 and result.skipped == 1
    skips = (await session.execute(SkippedRecord.__table__.select())).all()
    assert len(skips) == 1 and "primary key" in skips[0].reason


async def test_writer_upserts_entities(session, org):
    conn = await _connection(session, org)
    writer = DestinationWriter(
        organization_id=org.id,
        connection_id=conn.id,
        connector_id="google_ads",
        provider="google",
        resource_id="123",
        sync_run_id=None,
    )
    ents = [
        EntityRecord(stream="campaigns", level="campaign", external_id="c1", name="Camp 1", status="ENABLED"),
        EntityRecord(
            stream="campaigns", level="campaign", external_id="c1", name="Camp 1", status="ENABLED"
        ),  # within-batch dup
    ]
    r = await writer.write_entities(ents)
    assert r.inserted == 1 and r.skipped == 1
    r2 = await writer.write_entities(
        [
            EntityRecord(
                stream="campaigns", level="campaign", external_id="c1", name="Renamed", status="PAUSED"
            )
        ]
    )
    assert r2.updated == 1
    row = (await session.execute(AdEntity.__table__.select())).one()
    assert row.name == "Renamed" and row.status == "PAUSED"
