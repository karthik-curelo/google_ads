"""DestinationWriter routing for LeadSquared — the first connector with more
than one destination table, so STREAM_MODEL_OVERRIDES actually has to route
`leads` to leadsquared_leads and every activity stream to the single shared
leadsquared_activities table (§Phase A/D of the LSQ implementation)."""

from datetime import date

import pytest
from sqlalchemy import select

from app.connectors.base import Record
from app.connectors.leadsquared.connector import _streams
from app.models import Connection, LeadsquaredActivity, LeadsquaredLead, OAuthIdentity
from app.sync.writer import DestinationWriter

pytestmark = pytest.mark.asyncio

_STREAMS = {s.name: s for s in _streams()}


async def _connection(session, org) -> Connection:
    ident = OAuthIdentity(
        organization_id=org.id,
        provider="leadsquared",
        external_account_id="static",
        scopes=[],
    )
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="leadsquared",
        name="LeadSquared",
        resource_id="account",
        config={},
        streams=[],
    )
    session.add(conn)
    await session.commit()
    return conn


def _writer(conn) -> DestinationWriter:
    return DestinationWriter(
        organization_id=conn.organization_id,
        connection_id=conn.id,
        connector_id="leadsquared",
        provider="leadsquared",
        resource_id=conn.resource_id,
        sync_run_id=None,
    )


def _lead_record(prospect_id: str, source: str = "google_lp") -> Record:
    return Record(
        stream="leads",
        key_values={"prospect_id": prospect_id},
        date=date(2026, 9, 11),
        dimensions={
            "prospect_id": prospect_id,
            "source": source,
            "mx_source_campaign_id": "23226177337",
            "phone": "+91-9999999999",
        },
        raw={"ProspectID": prospect_id, "Source": source},
        cursor_value="2026-09-11 06:00:00",
    )


def _activity_record(stream: str, activity_id: str, booking_id: str | None, related: str = "p1") -> Record:
    return Record(
        stream=stream,
        key_values={"prospect_activity_id": activity_id},
        date=date(2026, 9, 11),
        dimensions={
            "prospect_activity_id": activity_id,
            "related_prospect_id": related,
            "booking_id": booking_id,
        },
        raw={"ProspectActivityId": activity_id},
        cursor_value="2026-09-11 06:00:00",
    )


async def test_leads_land_in_leadsquared_leads_with_promoted_columns(session, org):
    conn = await _connection(session, org)
    writer = _writer(conn)

    result = await writer.write_records(_STREAMS["leads"], [_lead_record("p1")])
    assert result.inserted == 1 and result.updated == 0

    row = (
        await session.execute(select(LeadsquaredLead).where(LeadsquaredLead.connection_id == conn.id))
    ).scalar_one()
    assert row.prospect_id == "p1"
    assert row.source == "google_lp"
    assert row.dimensions["mx_source_campaign_id"] == "23226177337"
    assert row.raw["ProspectID"] == "p1"
    assert row.date == date(2026, 9, 11)


async def test_lead_upsert_by_prospect_id_updates_not_duplicates(session, org):
    conn = await _connection(session, org)
    writer = _writer(conn)

    await writer.write_records(_STREAMS["leads"], [_lead_record("p1", source="google_lp")])
    result2 = await writer.write_records(_STREAMS["leads"], [_lead_record("p1", source="Meta_Form")])
    assert result2.inserted == 0 and result2.updated == 1

    rows = (
        (await session.execute(select(LeadsquaredLead).where(LeadsquaredLead.connection_id == conn.id)))
        .scalars()
        .all()
    )
    assert len(rows) == 1
    assert rows[0].source == "Meta_Form"  # the later write won


async def test_every_activity_stream_lands_in_the_one_shared_activities_table(session, org):
    conn = await _connection(session, org)
    writer = _writer(conn)

    await writer.write_records(
        _STREAMS["booking_created"], [_activity_record("booking_created", "a1", "BK-1")]
    )
    await writer.write_records(
        _STREAMS["post_booking_order_status"], [_activity_record("post_booking_order_status", "a2", "BK-1")]
    )
    await writer.write_records(
        _STREAMS["booking_cancelled"], [_activity_record("booking_cancelled", "a3", "BK-1")]
    )

    rows = (
        (
            await session.execute(
                select(LeadsquaredActivity).where(LeadsquaredActivity.connection_id == conn.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 3  # one physical table, three event types
    assert {r.stream for r in rows} == {"booking_created", "post_booking_order_status", "booking_cancelled"}
    # the lifecycle join key: every row for the same booking carries the same
    # normalised booking_id regardless of which event type it came from.
    assert {r.booking_id for r in rows} == {"BK-1"}
    assert {r.related_prospect_id for r in rows} == {"p1"}
    assert {r.prospect_activity_id for r in rows} == {"a1", "a2", "a3"}


async def test_activity_upsert_key_is_prospect_activity_id_not_booking_id(session, org):
    """Multiple 208 rows for the *same* booking are distinct activities (one
    booking can have N status-transition rows, per LSQ_VERIFICATION_2026-09-11.md
    §D) — they must not collide on booking_id."""
    conn = await _connection(session, org)
    writer = _writer(conn)

    await writer.write_records(
        _STREAMS["post_booking_order_status"],
        [_activity_record("post_booking_order_status", "a1", "BK-1")],
    )
    result2 = await writer.write_records(
        _STREAMS["post_booking_order_status"],
        [_activity_record("post_booking_order_status", "a2", "BK-1")],
    )
    assert result2.inserted == 1  # a2 is a new row, not an update of a1

    rows = (
        (
            await session.execute(
                select(LeadsquaredActivity).where(LeadsquaredActivity.connection_id == conn.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2
    assert {r.prospect_activity_id for r in rows} == {"a1", "a2"}
    assert {r.booking_id for r in rows} == {"BK-1"}


async def test_dimension_promotion_is_a_noop_for_connectors_without_extra_columns(session, org):
    """The broadened `_report_row` promotion (any dimension key matching a
    real column) must not change behaviour for a connector whose table has no
    extra columns beyond the shared PerformanceRowMixin ones — regression
    guard for the writer change made to support LeadSquared."""
    from app.connectors.base import StreamDefinition
    from app.connectors.validation import build_json_schema
    from app.models import GoogleAnalyticsPerformance
    from app.models import OAuthIdentity as _Ident

    ident = _Ident(organization_id=org.id, provider="google", external_account_id="ga-acc", scopes=[])
    session.add(ident)
    await session.flush()
    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="google_analytics",
        name="GA4",
        resource_id="123",
        config={},
        streams=[],
    )
    session.add(conn)
    await session.commit()

    writer = DestinationWriter(
        organization_id=org.id,
        connection_id=conn.id,
        connector_id="google_analytics",
        provider="google",
        resource_id="123",
        sync_run_id=None,
    )
    stream = StreamDefinition(
        name="daily_overview",
        description="",
        json_schema=build_json_schema(["date"], ["sessions"]),
        primary_key=["date"],
    )
    # A stray dimension key that happens to share a name with a LeadSquared
    # column (source) must NOT be promoted onto GoogleAnalyticsPerformance,
    # which has no such column — it should simply be dropped by the existing
    # model_cols filter, exactly as before this change.
    record = Record(
        stream="daily_overview",
        key_values={"date": "2026-09-11"},
        date=date(2026, 9, 11),
        dimensions={"date": "2026-09-11", "source": "should-not-appear"},
        metrics={"sessions": 10},
        measures={"sessions": 10},
    )
    await writer.write_records(stream, [record])
    row = (
        await session.execute(
            select(GoogleAnalyticsPerformance).where(GoogleAnalyticsPerformance.connection_id == conn.id)
        )
    ).scalar_one()
    assert row.sessions == 10
    assert not hasattr(row, "source")
