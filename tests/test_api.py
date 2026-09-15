"""API surface: auth, integrations, OAuth start, connection lifecycle, observability."""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.connectors.registry import RegistryEntry, registry
from app.main import create_app
from app.models import Connection, OAuthIdentity
from app.sync.runner import run_connection
from tests._fakes import StubConnector


@pytest.fixture(autouse=True)
def _stub():
    if "stub" not in registry:
        registry.register(RegistryEntry(connector_class=StubConnector))
    StubConnector.fail_with = None


@pytest_asyncio.fixture
async def client(org):
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


H = {"Authorization": "Bearer test-token"}


async def test_auth_required(client):
    assert (await client.get("/api/v1/integrations")).status_code == 401
    assert (await client.get("/api/v1/integrations", headers=H)).status_code == 200


async def test_integrations_list_and_detail(client):
    body = (await client.get("/api/v1/integrations", headers=H)).json()
    ids = {i["connector_id"] for i in body["integrations"]}
    assert {"google_analytics", "google_ads", "meta_ads", "instagram_insights"} <= ids

    ga = (await client.get("/api/v1/integrations/google_analytics", headers=H)).json()
    assert ga["available"] is True  # test env sets GOOGLE_CLIENT_ID/SECRET
    assert any(s["name"] == "daily_overview" for s in ga["streams"])


async def test_oauth_connect_returns_google_url(client):
    r = await client.post(
        "/api/v1/integrations/google_analytics/connect",
        headers=H,
        json={"redirect_after": "http://testserver/"},
    )
    assert r.status_code == 200
    url = r.json()["authorization_url"]
    assert url.startswith("https://accounts.google.com/o/oauth2/v2/auth?")
    assert "analytics.readonly" in url and "access_type=offline" in url


async def test_oauth_callback_rejects_unknown_state(client):
    r = await client.get("/api/v1/oauth/google/callback", params={"code": "x", "state": "nope"})
    assert r.status_code == 400
    assert "not recognised" in r.text or "Start the connection flow again" in r.text


async def test_connection_lifecycle_end_to_end(client, session, org):
    ident = OAuthIdentity(
        organization_id=org.id,
        provider="stub",
        external_account_id="s1",
        scopes=[],
    )
    session.add(ident)
    await session.commit()

    # discover (StubConnector needs no network)
    r = await client.post(
        "/api/v1/connections/discover",
        headers=H,
        json={"connector_id": "stub", "identity_id": ident.id},
    )
    assert r.status_code == 200 and r.json()["resources"][0]["resource_id"] == "r1"

    # create
    r = await client.post(
        "/api/v1/connections",
        headers=H,
        json={
            "connector_id": "stub",
            "identity_id": ident.id,
            "resource_id": "r1",
            "backfill_days": 3,
            "schedule_interval_seconds": 3600,
        },
    )
    assert r.status_code == 201
    conn_id = r.json()["id"]
    assert r.json()["streams"][0]["stream"] == "daily"

    # duplicate rejected
    dup = await client.post(
        "/api/v1/connections",
        headers=H,
        json={"connector_id": "stub", "identity_id": ident.id, "resource_id": "r1"},
    )
    assert dup.status_code == 409

    # sync endpoint accepts the request
    assert (await client.post(f"/api/v1/connections/{conn_id}/sync", headers=H)).status_code == 202

    # run it deterministically and check observability. The fire-and-forget
    # "sync now" background task above and this direct call now race for the
    # same connection-level lock (run_connection() claims atomically on
    # entry) — whichever loses gets None back immediately rather than
    # racing to sync twice, so tolerate either outcome and, if we lost the
    # race, wait for the winner's own run to actually finish before asserting.
    outcome = await run_connection(conn_id, trigger="manual")
    if outcome is None:
        import asyncio

        for _ in range(50):
            detail = (await client.get(f"/api/v1/connections/{conn_id}", headers=H)).json()
            if detail["status"] != "syncing":
                break
            await asyncio.sleep(0.05)
        assert detail["latest_run"]["status"] == "succeeded"
    else:
        assert outcome.status == "succeeded"
        assert outcome.records_fetched == 8  # 4 days (today-3..today) × 2 rows

    detail = (await client.get(f"/api/v1/connections/{conn_id}", headers=H)).json()
    assert detail["status"] == "healthy"
    assert detail["latest_run"]["status"] == "succeeded"

    runs = (await client.get(f"/api/v1/connections/{conn_id}/runs", headers=H)).json()["runs"]
    assert len(runs) >= 1
    run_detail = (await client.get(f"/api/v1/sync-runs/{runs[0]['id']}", headers=H)).json()
    assert run_detail["streams"][0]["stream"] == "daily"

    data = (await client.get(f"/api/v1/connections/{conn_id}/data?limit=100", headers=H)).json()
    assert len(data["rows"]) == 8
    assert data["rows"][0]["sessions"] in (10, 11)

    # pause / resume
    assert (await client.post(f"/api/v1/connections/{conn_id}/pause", headers=H)).json()["enabled"] is False
    assert (await client.post(f"/api/v1/connections/{conn_id}/resume", headers=H)).json()["enabled"] is True

    # tenant isolation: a second org's token cannot see this connection
    import hashlib

    from app.models import ApiToken, Organization

    other = Organization(name="Other", slug="other")
    session.add(other)
    await session.flush()
    session.add(ApiToken(organization_id=other.id, token_hash=hashlib.sha256(b"other-token").hexdigest()))
    await session.commit()
    r = await client.get(f"/api/v1/connections/{conn_id}", headers={"Authorization": "Bearer other-token"})
    assert r.status_code == 404

    # delete
    assert (await client.delete(f"/api/v1/connections/{conn_id}", headers=H)).status_code == 204
    assert (await session.get(Connection, conn_id)) is None
