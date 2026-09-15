"""Local, fixture-based lifecycle walkthrough: Lead -> 206 Booking Created ->
multiple 208 status updates -> optional 223 cancellation. Uses the isolated
SQLite test database (tests/conftest.py) — no production data, no live API
calls. Complements test_leadsquared_writer.py's narrower upsert-key tests
with the full scenario end to end."""

from datetime import date, timedelta

import pytest
from sqlalchemy import select

from app.connectors.base import Record
from app.connectors.leadsquared.connector import _streams
from app.models import Connection, LeadsquaredActivity, LeadsquaredLead, OAuthIdentity
from app.sync.writer import DestinationWriter

pytestmark = pytest.mark.asyncio

_STREAMS = {s.name: s for s in _streams()}
_D0 = date(2026, 9, 11)


async def _connection(session, org) -> Connection:
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


async def test_full_lead_to_booking_lifecycle_with_multiple_208_and_a_cancellation(session, org):
    conn = await _connection(session, org)
    writer = _writer(conn)
    PROSPECT_ID = "p-lifecycle-1"
    BOOKING_ID = "BK-777"

    # 1. the lead itself
    lead = Record(
        stream="leads",
        key_values={"prospect_id": PROSPECT_ID},
        date=_D0,
        dimensions={
            "prospect_id": PROSPECT_ID,
            "source": "google_lp",
            "mx_source_campaign_id": "23226177337",
        },
        raw={"ProspectID": PROSPECT_ID, "Source": "google_lp"},
    )
    await writer.write_records(_STREAMS["leads"], [lead])

    # 2. Booking Created (206) — booking_id from mx_Custom_2
    created = Record(
        stream="booking_created",
        key_values={"prospect_activity_id": "a-206"},
        date=_D0,
        dimensions={
            "prospect_activity_id": "a-206",
            "related_prospect_id": PROSPECT_ID,
            "booking_id": BOOKING_ID,
            "total_paid_amount": "999",
            "payment_status": "Success",
        },
        raw={"ProspectActivityId": "a-206", "mx_Custom_2": BOOKING_ID, "ActivityEvent": "206"},
    )
    await writer.write_records(_STREAMS["booking_created"], [created])

    # 3. Three 208 status updates for the SAME booking, in order, over three days
    statuses = ["pending", "confirmed", "completed"]
    for i, status in enumerate(statuses):
        row = Record(
            stream="post_booking_order_status",
            key_values={"prospect_activity_id": f"a-208-{i}"},
            date=_D0 + timedelta(days=i),
            dimensions={
                "prospect_activity_id": f"a-208-{i}",
                "related_prospect_id": PROSPECT_ID,
                "booking_id": BOOKING_ID,
                "booking_status": status,
                "actual_amount": "999",
                "booking_amount": "999",
            },
            raw={"ProspectActivityId": f"a-208-{i}", "mx_Custom_4": BOOKING_ID, "mx_Custom_6": status},
        )
        await writer.write_records(_STREAMS["post_booking_order_status"], [row])

    # --- assertions on the lead ---
    lead_row = (
        await session.execute(select(LeadsquaredLead).where(LeadsquaredLead.connection_id == conn.id))
    ).scalar_one()
    assert lead_row.prospect_id == PROSPECT_ID
    assert lead_row.raw["ProspectID"] == PROSPECT_ID  # raw preserved

    # --- assertions on the activity trail ---
    activities = (
        (
            await session.execute(
                select(LeadsquaredActivity).where(LeadsquaredActivity.connection_id == conn.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(activities) == 4  # 1x 206 + 3x 208, no collisions
    assert {a.booking_id for a in activities} == {BOOKING_ID}  # booking_id extracted correctly on every row
    assert {a.related_prospect_id for a in activities} == {PROSPECT_ID}

    # multiple 208 rows for one booking must not collide/dedupe into one row
    status_rows = [a for a in activities if a.stream == "post_booking_order_status"]
    assert len(status_rows) == 3
    ordered = sorted(status_rows, key=lambda a: a.date)
    assert [r.dimensions["booking_status"] for r in ordered] == statuses  # status ordering preserved
    assert all(r.raw.get("mx_Custom_4") == BOOKING_ID for r in status_rows)  # raw retained per row

    created_row = next(a for a in activities if a.stream == "booking_created")
    assert created_row.dimensions["total_paid_amount"] == "999"
    assert created_row.dimensions["payment_status"] == "Success"

    # --- §15: no 223 for this booking is a valid state, not an error ---
    cancellations = [a for a in activities if a.stream == "booking_cancelled"]
    assert cancellations == []


async def test_lifecycle_with_a_cancellation_is_identifiable(session, org):
    conn = await _connection(session, org)
    writer = _writer(conn)
    PROSPECT_ID = "p-lifecycle-2"
    BOOKING_ID = "BK-888"

    created = Record(
        stream="booking_created",
        key_values={"prospect_activity_id": "b-206"},
        date=_D0,
        dimensions={
            "prospect_activity_id": "b-206",
            "related_prospect_id": PROSPECT_ID,
            "booking_id": BOOKING_ID,
        },
        raw={"ProspectActivityId": "b-206", "mx_Custom_2": BOOKING_ID},
    )
    await writer.write_records(_STREAMS["booking_created"], [created])

    cancelled = Record(
        stream="booking_cancelled",
        key_values={"prospect_activity_id": "b-223"},
        date=_D0 + timedelta(days=1),
        dimensions={
            "prospect_activity_id": "b-223",
            "related_prospect_id": PROSPECT_ID,
            "booking_id": BOOKING_ID,
            "cancelled_amount": "500",
        },
        raw={
            "ProspectActivityId": "b-223",
            "mx_Custom_3": BOOKING_ID,
            "mx_Custom_6": "Customer changed mind",
        },
    )
    await writer.write_records(_STREAMS["booking_cancelled"], [cancelled])

    activities = (
        (
            await session.execute(
                select(LeadsquaredActivity).where(LeadsquaredActivity.connection_id == conn.id)
            )
        )
        .scalars()
        .all()
    )
    cancellation_rows = [a for a in activities if a.stream == "booking_cancelled"]
    assert len(cancellation_rows) == 1
    assert cancellation_rows[0].booking_id == BOOKING_ID  # same normalised booking_id as the 206 row
    assert cancellation_rows[0].dimensions["cancelled_amount"] == "500"
    # no 208 history at all for this booking — still valid (§15), no assumption it must exist
    assert not [a for a in activities if a.stream == "post_booking_order_status"]
