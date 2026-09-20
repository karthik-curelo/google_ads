"""Startup preflight — the fix for scheduled runs failing quietly because the scheduler
process's environment lacked a connector's settings (LeadSquared, 2026-09-18)."""

from __future__ import annotations

from datetime import UTC, datetime

from app.core.config import get_settings
from app.models import Connection, OAuthIdentity
from app.sync.preflight import misconfigured_connections, run_preflight


async def _lsq_connection(session, org, *, enabled=True) -> Connection:
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
        streams=[],
        enabled=enabled,
        schedule_interval_seconds=10800,
        next_run_at=datetime.now(UTC),
    )
    session.add(conn)
    await session.commit()
    return conn


async def test_a_process_with_the_settings_reports_nothing(session, org):
    await _lsq_connection(session, org)
    assert await misconfigured_connections() == []


async def test_a_process_missing_the_settings_names_the_connection_and_the_variable(
    session, org, monkeypatch
):
    conn = await _lsq_connection(session, org)
    monkeypatch.setattr(get_settings(), "leadsquared_access_key", "")
    monkeypatch.setattr(get_settings(), "leadsquared_secret_key", "")

    (problem,) = await misconfigured_connections()
    assert problem.connection_id == conn.id and problem.connector_id == "leadsquared"
    assert "LEADSQUARED_ACCESS_KEY" in problem.reason and "LEADSQUARED_SECRET_KEY" in problem.reason


async def test_disabled_connections_are_not_reported(session, org, monkeypatch):
    await _lsq_connection(session, org, enabled=False)
    monkeypatch.setattr(get_settings(), "leadsquared_access_key", "")
    assert await misconfigured_connections() == []


async def test_run_preflight_flags_the_connection_so_it_is_visible_before_the_first_failed_run(
    session, org, monkeypatch, caplog
):
    conn = await _lsq_connection(session, org)
    monkeypatch.setattr(get_settings(), "leadsquared_host", "")

    with caplog.at_level("ERROR"):
        problems = await run_preflight()

    assert len(problems) == 1
    assert any("PREFLIGHT" in r.message and "cannot run in this process" in r.message for r in caplog.records)
    await session.refresh(conn)
    assert conn.status == "invalid_configuration"
    assert "Misconfigured in the scheduler process" in (conn.status_detail or "")


async def test_an_unknown_connector_is_reported_too(session, org):
    ident = OAuthIdentity(organization_id=org.id, provider="x", external_account_id="e", scopes=[])
    session.add(ident)
    await session.flush()
    session.add(
        Connection(
            organization_id=org.id,
            oauth_identity_id=ident.id,
            connector_id="no-such-connector",
            name="ghost",
            resource_id="r",
            config={},
            streams=[],
            enabled=True,
        )
    )
    await session.commit()
    (problem,) = await misconfigured_connections()
    assert problem.reason == "unknown connector"


async def test_healthz_reports_degraded_with_the_reason_when_a_scheduled_connection_cannot_run(
    session, org, monkeypatch
):
    from httpx import ASGITransport, AsyncClient

    from app.main import create_app

    conn = await _lsq_connection(session, org)
    app = create_app()
    app.state.scheduler = object()  # this process runs schedules (lifespan is not exercised by ASGITransport)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        ok = (await client.get("/healthz")).json()
        assert ok["status"] == "ok" and "misconfigured" not in ok

        monkeypatch.setattr(get_settings(), "leadsquared_access_key", "")
        bad = (await client.get("/healthz")).json()

    assert bad["status"] == "degraded"
    (entry,) = bad["misconfigured"]
    assert entry["connection_id"] == conn.id and "LEADSQUARED_ACCESS_KEY" in entry["reason"]


async def test_healthz_stays_ok_in_a_process_that_does_not_schedule(session, org, monkeypatch):
    """An API-only process legitimately lacks nothing it needs: only a scheduling process is judged."""
    from httpx import ASGITransport, AsyncClient

    from app.main import create_app

    await _lsq_connection(session, org)
    monkeypatch.setattr(get_settings(), "leadsquared_access_key", "")
    app = create_app()
    app.state.scheduler = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        assert (await client.get("/healthz")).json()["status"] == "ok"
