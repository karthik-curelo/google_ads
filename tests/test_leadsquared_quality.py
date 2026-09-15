"""Data-quality checks (implementation instruction §17) — each check is
exercised with both a clean fixture (0 findings) and a seeded bad row."""

from datetime import date

import pytest

from app.connectors.leadsquared import quality as dq
from app.models import AdEntity, Connection, LeadsquaredActivity, LeadsquaredLead, OAuthIdentity

pytestmark = pytest.mark.asyncio


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


def _lead(conn, prospect_id: str, **dims) -> LeadsquaredLead:
    return LeadsquaredLead(
        organization_id=conn.organization_id,
        connection_id=conn.id,
        connector_id="leadsquared",
        provider="leadsquared",
        stream="leads",
        resource_id="account",
        record_key=f"key-{prospect_id}",
        prospect_id=prospect_id,
        source=dims.pop("source", None),
        date=date(2026, 9, 11),
        dimensions=dims,
        metrics={},
    )


def _activity(
    conn, stream: str, activity_id: str, booking_id: str | None, related: str = "p1"
) -> LeadsquaredActivity:
    return LeadsquaredActivity(
        organization_id=conn.organization_id,
        connection_id=conn.id,
        connector_id="leadsquared",
        provider="leadsquared",
        stream=stream,
        resource_id="account",
        record_key=f"key-{activity_id}",
        prospect_activity_id=activity_id,
        related_prospect_id=related,
        booking_id=booking_id,
        date=date(2026, 9, 11),
        dimensions={},
        metrics={},
    )


async def test_unknown_sources_flags_a_value_not_in_the_known_set(session, org):
    conn = await _connection(session, org)
    session.add_all(
        [
            _lead(conn, "p1", source="Meta_Form"),
            _lead(conn, "p2", source="A Brand New Source Nobody Has Seen"),
        ]
    )
    await session.commit()

    finding = await dq.unknown_sources(session, conn.id)
    assert finding.count == 1
    assert finding.sample[0]["prospect_id"] == "p2"


async def test_unknown_sources_clean_when_all_known(session, org):
    conn = await _connection(session, org)
    session.add_all([_lead(conn, "p1", source="Meta_Form"), _lead(conn, "p2", source=None)])
    await session.commit()
    finding = await dq.unknown_sources(session, conn.id)
    assert finding.clean


async def test_id_format_mismatches_flags_neither_google_nor_meta_shaped(session, org):
    conn = await _connection(session, org)
    session.add_all(
        [
            _lead(conn, "p1", mx_source_campaign_id="23226177337"),  # Google-shaped (11 digits)
            _lead(conn, "p2", mx_source_campaign_id="23853121772240798"),  # Meta-shaped (17 digits)
            _lead(conn, "p3", mx_source_campaign_id="not-a-real-id-123"),  # neither
        ]
    )
    await session.commit()
    finding = await dq.id_format_mismatches(session, conn.id)
    assert finding.count == 1
    assert finding.sample[0]["prospect_id"] == "p3"


async def test_attribution_orphans_flags_a_campaign_id_with_no_matching_entity(session, org):
    conn = await _connection(session, org)
    session.add(
        AdEntity(
            organization_id=org.id,
            connection_id=conn.id,
            connector_id="google_ads",
            provider="google",
            resource_id="9999",
            level="campaign",
            external_id="23226177337",
            name="Search_FBC",
        )
    )
    session.add_all(
        [
            _lead(conn, "p1", mx_source_campaign_id="23226177337"),  # matches ad_entities
            _lead(conn, "p2", mx_source_campaign_id="99999999999"),  # no matching entity anywhere
        ]
    )
    await session.commit()
    finding = await dq.attribution_orphans(session, conn.id)
    assert finding.count == 1
    assert finding.sample[0]["prospect_id"] == "p2"


async def test_missing_campaign_or_ad_identifiers_flags_gclid_without_any_id(session, org):
    conn = await _connection(session, org)
    session.add_all(
        [
            _lead(conn, "p1", mx_gclid="CjwK...", mx_source_campaign_id="23226177337"),
            _lead(conn, "p2", mx_gclid="CjwK...other"),  # gclid present, no campaign/adset id at all
        ]
    )
    await session.commit()
    finding = await dq.missing_campaign_or_ad_identifiers(session, conn.id)
    assert finding.count == 1
    assert finding.sample[0]["prospect_id"] == "p2"


async def test_duplicate_prospect_activity_ids_clean_in_normal_operation(session, org):
    conn = await _connection(session, org)
    session.add_all(
        [
            _activity(conn, "booking_created", "a1", "BK-1"),
            _activity(conn, "post_booking_order_status", "a2", "BK-1"),
        ]
    )
    await session.commit()
    finding = await dq.duplicate_prospect_activity_ids(session, conn.id)
    assert finding.clean


async def test_booking_id_inconsistencies_flags_downstream_activity_with_no_206(session, org):
    conn = await _connection(session, org)
    session.add_all(
        [
            _activity(conn, "booking_created", "a1", "BK-1"),
            _activity(conn, "post_booking_order_status", "a2", "BK-1"),  # fine, BK-1 has a 206
            _activity(conn, "post_booking_order_status", "a3", "BK-2"),  # BK-2 has no 206 anywhere
        ]
    )
    await session.commit()
    finding = await dq.booking_id_inconsistencies(session, conn.id)
    assert finding.count == 1
    assert finding.sample[0]["booking_id"] == "BK-2"


async def test_booking_id_inconsistencies_missing_208_alone_is_not_flagged(session, org):
    """§15: a booking with no post_booking_order_status row at all is a
    normal, expected state — this check only flags the reverse (a downstream
    row with no 206), never the mere absence of 208."""
    conn = await _connection(session, org)
    session.add(_activity(conn, "booking_created", "a1", "BK-1"))
    await session.commit()
    finding = await dq.booking_id_inconsistencies(session, conn.id)
    assert finding.clean


async def test_run_all_returns_one_finding_per_check(session, org):
    conn = await _connection(session, org)
    findings = await dq.run_all(session, conn.id)
    assert len(findings) == len(dq.CHECKS)
    assert all(f.clean for f in findings)  # empty connection — nothing to flag
