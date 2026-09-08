"""Shared FastAPI dependencies: DB session, tenant auth, connector construction."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from typing import Annotated, Any

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.base import ConnectorContext
from app.connectors.registry import load_connectors
from app.core.config import Settings, get_settings
from app.core.database import SessionLocal
from app.models import ApiToken, Organization
from app.oauth.service import DatabaseTokenProvider


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


SessionDep = Annotated[AsyncSession, Depends(get_session)]


async def require_org(
    session: SessionDep,
    authorization: Annotated[str | None, Header()] = None,
) -> Organization:
    settings = get_settings()
    token = ""
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()

    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing bearer token")

    token_hash = hashlib.sha256(token.encode()).hexdigest()
    row = (
        await session.execute(select(ApiToken).where(ApiToken.token_hash == token_hash))
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid bearer token")

    org = await session.get(Organization, row.organization_id)
    if org is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token is not bound to an organization")
    _ = settings
    return org


OrgDep = Annotated[Organization, Depends(require_org)]


def get_scheduler(request: Request):
    """The app's SyncScheduler, or None when it is disabled (tests / CLI)."""
    return getattr(request.app.state, "scheduler", None)


SchedulerDep = Annotated[Any, Depends(get_scheduler)]


def build_connector(
    connector_id: str,
    identity_id: int,
    *,
    resource_id: str | None = None,
    resource_metadata: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    settings: Settings | None = None,
):
    """Construct a ready-to-use connector bound to an OAuth identity."""
    settings = settings or get_settings()
    entry = load_connectors().get(connector_id)
    provider = entry.connector_class.provider
    provider_settings: dict[str, Any] = {"http_timeout_seconds": settings.http_timeout_seconds}
    if provider == "google":
        provider_settings.update(
            google_ads_developer_token=settings.google_ads_developer_token,
            google_ads_api_version=settings.google_ads_api_version,
            google_ads_login_customer_id=settings.google_ads_login_customer_id,
        )
    elif provider == "meta":
        provider_settings.update(
            meta_api_version=settings.meta_api_version,
            meta_app_id=settings.meta_app_id,
            meta_app_secret=settings.meta_app_secret,
        )
    ctx = ConnectorContext(
        token_provider=DatabaseTokenProvider(identity_id, settings=settings),
        config=config or {},
        resource_id=resource_id,
        resource_metadata=resource_metadata or {},
        provider_settings=provider_settings,
    )
    return entry.connector_class(ctx)
