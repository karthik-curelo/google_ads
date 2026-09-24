"""Startup preflight — the fix for scheduled runs failing quietly because the scheduler
process's environment lacked a connector's settings (LeadSquared, 2026-09-18)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from app.core.config import get_settings
from app.models import Connection, OAuthIdentity
from app.sync import preflight
from app.sync.preflight import misconfigured_connections, run_preflight, schema_drift


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


async def test_schema_drift_is_none_on_a_fresh_database(session):
    """The normal test/dev state: schema built by `create_all`, no `alembic_version` table
    at all. That is "can't tell", never treated as drift."""
    assert await schema_drift() is None


@pytest.fixture
async def alembic_version_table(session):
    """A raw `alembic_version` table, isolated from other tests: DDL + the seeded row are
    committed outside the per-test transaction (SQLite auto-commits DDL), so it is dropped
    explicitly on teardown rather than relying on rollback."""

    async def seed(revision: str) -> None:
        await session.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
        await session.execute(text("INSERT INTO alembic_version (version_num) VALUES (:v)"), {"v": revision})
        await session.commit()

    yield seed
    await session.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await session.commit()


async def test_schema_drift_reports_when_the_database_is_behind_the_code(
    session, monkeypatch, caplog, alembic_version_table
):
    """The 2026-09-23 incident: code (and its migration file) shipped, the migration was
    never run — every scheduled write of a new column then fails, quietly, forever."""
    monkeypatch.setattr(preflight, "_code_head_revisions", lambda: {"e2a9c5b17f03"})
    await alembic_version_table("b7c3d91e4a52")

    drift = await schema_drift()
    assert drift is not None
    assert "b7c3d91e4a52" in drift.reason and "e2a9c5b17f03" in drift.reason

    with caplog.at_level("ERROR"):
        await run_preflight()
    assert any("PREFLIGHT" in r.message and "e2a9c5b17f03" in r.message for r in caplog.records)


async def test_schema_drift_is_none_when_the_database_matches_the_code(
    session, monkeypatch, alembic_version_table
):
    monkeypatch.setattr(preflight, "_code_head_revisions", lambda: {"e2a9c5b17f03"})
    await alembic_version_table("e2a9c5b17f03")
    assert await schema_drift() is None


async def test_healthz_reports_schema_drift_even_in_a_process_that_does_not_schedule(
    session, monkeypatch, alembic_version_table
):
    """Schema drift breaks any process that touches the affected rows, not only scheduled
    syncs, so it is reported unconditionally rather than gated on `app.state.scheduler`."""
    from httpx import ASGITransport, AsyncClient

    from app.main import create_app

    monkeypatch.setattr(preflight, "_code_head_revisions", lambda: {"e2a9c5b17f03"})
    await alembic_version_table("b7c3d91e4a52")
    app = create_app()
    app.state.scheduler = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        body = (await client.get("/healthz")).json()

    assert body["status"] == "degraded"
    assert "e2a9c5b17f03" in body["schema_drift"]


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
