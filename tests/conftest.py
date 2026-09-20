"""Test configuration.

Environment is pinned BEFORE any `app.*` import so the module-level engine,
settings cache and crypto key all bind to the test database and a throwaway key.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

# --- must run before app imports --------------------------------------------
# One database file PER test process: two pytest runs at once (a CI shard, or a developer
# starting a second run) must not drop each other's tables.
_TMP = Path(tempfile.gettempdir()) / f"mc_test_{os.getpid()}.db"
os.environ.update(
    ENVIRONMENT="development",
    TESTING="true",
    SCHEDULER_ENABLED="false",
    # SQLite by default (zero setup). Point TEST_DATABASE_URL at a scratch PostgreSQL
    # database to run the same suite against the production dialect — the atomic
    # claim (FOR UPDATE SKIP LOCKED) and the guarded upsert are Postgres behaviour.
    DATABASE_URL=os.environ.get("TEST_DATABASE_URL") or f"sqlite+aiosqlite:///{_TMP.as_posix()}",
    ENCRYPTION_KEY="_9MHMv2vFm7isYQk4wp5p9ZmlJTZvkK4rnsKrCYm2ac=",
    API_TOKEN="test-token",
    GOOGLE_CLIENT_ID="test-google-client",
    GOOGLE_CLIENT_SECRET="test-google-secret",
    GOOGLE_REDIRECT_URI="http://testserver/api/v1/oauth/google/callback",
    META_APP_ID="test-meta-app",
    META_APP_SECRET="test-meta-secret",
    META_REDIRECT_URI="http://testserver/api/v1/oauth/meta/callback",
    # LeadSquared: real-shaped configuration (so "scheduled execution with real
    # configuration" is exercised through the runner) with a limiter fast enough
    # that the suite is not throttled by its own rate limiter.
    LEADSQUARED_ACCESS_KEY="test-ak",
    LEADSQUARED_SECRET_KEY="test-sk",
    LEADSQUARED_HOST="https://lsq.test",
    LEADSQUARED_RATE_PER_SECOND="1000",
    LEADSQUARED_BURST="1000",
)

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

from app.core.database import SessionLocal, engine  # noqa: E402
from app.models import ApiToken, Base, Organization  # noqa: E402


@pytest_asyncio.fixture(autouse=True)
async def _fresh_db():
    from app.connectors.http import reset_shared_rate_limiters

    reset_shared_rate_limiters()  # process-wide limiters must not leak between tests
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def session():
    async with SessionLocal() as s:
        yield s


@pytest_asyncio.fixture
async def org(session):
    import hashlib

    o = Organization(name="Test", slug="test")
    session.add(o)
    await session.flush()
    session.add(
        ApiToken(
            organization_id=o.id,
            token_hash=hashlib.sha256(b"test-token").hexdigest(),
            label="test",
        )
    )
    await session.commit()
    return o


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer test-token"}
