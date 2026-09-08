"""Warehouse views: one per stream, typed columns, catalog populated."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from app.connectors.registry import load_connectors
from app.core.database import SessionLocal, engine
from app.models import Connection, OAuthIdentity, Organization, ReportRow
from app.warehouse.views import _view_name, rebuild_views

pytestmark = pytest.mark.asyncio


async def _seed_connections(s) -> tuple[int, int]:
    """Minimal org + identity + two connections so report_rows FKs resolve."""
    org = Organization(name="T", slug="t")
    s.add(org)
    await s.flush()
    ident = OAuthIdentity(organization_id=org.id, provider="google", external_account_id="acct")
    s.add(ident)
    await s.flush()
    ga = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="google_analytics",
        name="GA4",
        resource_id="P1",
    )
    gsc = Connection(
        organization_id=org.id,
        oauth_identity_id=ident.id,
        connector_id="google_search_console",
        name="GSC",
        resource_id="sc-domain:example.com",
    )
    s.add_all([ga, gsc])
    await s.flush()
    return org.id, ga.id, gsc.id


async def test_rebuild_creates_a_view_per_fact_stream():
    names = await rebuild_views(engine)
    reg = load_connectors()
    for entry in reg.all():
        cls = entry.connector_class
        for stream in cls.declared_streams():
            if stream.grain != "entity":
                assert _view_name(cls.connector_id, stream.name) in names
    # the streams added for "overall analysis" and the GSC set
    assert "v_ga4_landing_pages" in names
    assert "v_ga4_hourly_overview" in names
    assert "v_gsc_search_analytics_by_query" in names


async def test_view_projects_typed_and_json_columns():
    await rebuild_views(engine)
    async with SessionLocal() as s:
        _org, ga_id, gsc_id = await _seed_connections(s)
        s.add(
            ReportRow(
                organization_id=_org,
                connection_id=ga_id,
                connector_id="google_analytics",
                provider="google",
                stream="session_quality",
                resource_id="P1",
                record_key="k1",
                date=date(2026, 8, 20),
                dimensions={"sessionDefaultChannelGroup": "Organic Search", "deviceCategory": "mobile"},
                metrics={"bounceRate": 0.25, "engagementRate": 0.75, "averageSessionDuration": 123.4},
                sessions=10,
                bounce_rate=0.25,
                engagement_rate=0.75,
                avg_session_duration=123.4,
            )
        )
        s.add(
            ReportRow(
                organization_id=_org,
                connection_id=gsc_id,
                connector_id="google_search_console",
                provider="google",
                stream="search_analytics_by_query",
                resource_id="sc-domain:example.com",
                record_key="k2",
                date=date(2026, 8, 20),
                dimensions={"query": "blood test near me"},
                metrics={"clicks": 5, "impressions": 100, "ctr": 0.05, "position": 3.2},
                clicks=5,
                impressions=100,
                average_position=3.2,
            )
        )
        await s.commit()

        ga = (
            (
                await s.execute(
                    text(
                        "SELECT property_id, channel_group, device_category, sessions, "
                        "bounce_rate, average_session_duration FROM v_ga4_session_quality"
                    )
                )
            )
            .mappings()
            .one()
        )
        assert ga["property_id"] == "P1"
        assert ga["channel_group"] == "Organic Search"
        assert ga["device_category"] == "mobile"
        assert float(ga["bounce_rate"]) == pytest.approx(0.25)
        assert float(ga["average_session_duration"]) == pytest.approx(123.4)

        gsc = (
            (
                await s.execute(
                    text(
                        "SELECT property_id, query, clicks, impressions, ctr, position "
                        "FROM v_gsc_search_analytics_by_query"
                    )
                )
            )
            .mappings()
            .one()
        )
        assert gsc["property_id"] == "sc-domain:example.com"
        assert gsc["query"] == "blood test near me"
        assert int(gsc["clicks"]) == 5
        # ctr is not a promoted column -> read from the metrics JSON and cast
        assert float(gsc["ctr"]) == pytest.approx(0.05)


async def test_catalog_lists_every_view():
    names = await rebuild_views(engine)
    async with SessionLocal() as s:
        listed = set(
            (await s.execute(text("SELECT DISTINCT view_name FROM warehouse_catalog"))).scalars().all()
        )
        roles = set((await s.execute(text("SELECT DISTINCT role FROM warehouse_catalog"))).scalars().all())
    assert set(names) <= listed
    assert {"key", "dimension", "metric", "measure", "meta"} <= roles
