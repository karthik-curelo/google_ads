"""FastAPI application: routers, the sync scheduler, and a thin driver UI.

Startup wires four things and nothing more (§33 — modular monolith, no extra
infrastructure):

  1. structured logging with secret masking
  2. schema — Alembic in production; `create_all` elsewhere so a clean checkout
     runs with no migration step
  3. a bootstrap tenant + API token for local use (printed once)
  4. the in-process SyncScheduler (unless disabled or under tests)
"""

from __future__ import annotations

import contextlib
import hashlib
import secrets
from collections.abc import AsyncIterator
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select

from app.api.routers import connections, integrations, oauth, runs
from app.connectors import errors as E
from app.connectors.registry import load_connectors
from app.core.config import get_settings
from app.core.database import engine
from app.core.logging import get_logger, setup_logging
from app.models import ApiToken, Base, Organization
from app.sync.scheduler import SyncScheduler

logger = get_logger(__name__)
_WEB_DIR = Path(__file__).parent / "web"


async def _create_schema() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _bootstrap_tenant() -> None:
    """Ensure one org + a usable API token exist for local development."""
    from app.core.database import SessionLocal

    settings = get_settings()
    async with SessionLocal() as session:
        org = (
            await session.execute(select(Organization).where(Organization.slug == "default"))
        ).scalar_one_or_none()
        if org is None:
            org = Organization(name="Default", slug="default")
            session.add(org)
            await session.flush()

        configured = settings.api_token.strip()
        if configured:
            token_hash = hashlib.sha256(configured.encode()).hexdigest()
            exists = (
                await session.execute(select(ApiToken).where(ApiToken.token_hash == token_hash))
            ).scalar_one_or_none()
            if exists is None:
                session.add(ApiToken(organization_id=org.id, token_hash=token_hash, label="configured"))
                logger.info("Bound configured API_TOKEN to organization %s", org.slug)
        else:
            has_any = (
                await session.execute(select(ApiToken).where(ApiToken.organization_id == org.id))
            ).first()
            if not has_any and settings.is_development:
                raw = secrets.token_urlsafe(32)
                session.add(
                    ApiToken(
                        organization_id=org.id,
                        token_hash=hashlib.sha256(raw.encode()).hexdigest(),
                        label="dev-autoissued",
                    )
                )
                logger.warning(
                    "No API_TOKEN set — issued a development token (shown once):\n\n    %s\n\n"
                    "Use it as: Authorization: Bearer <token>. Set API_TOKEN in .env to pin one.",
                    raw,
                )
        await session.commit()


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    setup_logging()
    load_connectors()

    if settings.environment != "production":
        await _create_schema()
    await _bootstrap_tenant()



    scheduler: SyncScheduler | None = None
    if settings.scheduler_enabled and not settings.testing:
        scheduler = SyncScheduler()
        await scheduler.start()
    app.state.scheduler = scheduler

    logger.info(
        "%s ready — %d connectors, scheduler %s",
        settings.app_name,
        len(load_connectors()),
        "on" if scheduler else "off",
    )
    try:
        yield
    finally:
        if scheduler is not None:
            await scheduler.stop()
        await engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        docs_url="/docs" if settings.debug else None,
        redoc_url=None,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=sorted({settings.frontend_base_url, settings.public_base_url, "http://localhost:8000"}),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(E.ConnectorError)
    async def _connector_error_handler(_request, exc: E.ConnectorError):  # pragma: no cover - glue
        status = exc.http_status or (400 if exc.recoverable else 502)
        return JSONResponse(status_code=status, content={"error": exc.as_user_dict()})

    prefix = settings.api_prefix.rstrip("/")
    for module in (integrations, oauth, connections, runs):
        app.include_router(module.router, prefix=prefix)

    @app.get("/healthz", tags=["meta"])
    async def healthz() -> dict:
        return {"status": "ok", "connectors": len(load_connectors())}

    if _WEB_DIR.is_dir():
        app.mount("/app", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")

        @app.get("/", include_in_schema=False)
        async def _root() -> FileResponse:
            return FileResponse(_WEB_DIR / "index.html")

    return app


app = create_app()
