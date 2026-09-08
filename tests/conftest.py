"""Test configuration.

Environment is pinned BEFORE any `app.*` import so the module-level engine,
settings cache and crypto key all bind to the test database and a throwaway key.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

# --- must run before app imports --------------------------------------------
_TMP = Path(tempfile.gettempdir()) / "mc_test.db"
os.environ.update(
    ENVIRONMENT="development",
    TESTING="true",
    SCHEDULER_ENABLED="false",
    DATABASE_URL=f"sqlite+aiosqlite:///{_TMP.as_posix()}",
    ENCRYPTION_KEY="_9MHMv2vFm7isYQk4wp5p9ZmlJTZvkK4rnsKrCYm2ac=",
    API_TOKEN="test-token",
    GOOGLE_CLIENT_ID="test-google-client",
    GOOGLE_CLIENT_SECRET="test-google-secret",
    GOOGLE_REDIRECT_URI="http://testserver/api/v1/oauth/google/callback",
    META_APP_ID="test-meta-app",
    META_APP_SECRET="test-meta-secret",
    META_REDIRECT_URI="http://testserver/api/v1/oauth/meta/callback",
)

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

from app.core.database import SessionLocal, engine  # noqa: E402
from app.models import ApiToken, Base, Organization  # noqa: E402


@pytest_asyncio.fixture(autouse=True)
async def _fresh_db():
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
