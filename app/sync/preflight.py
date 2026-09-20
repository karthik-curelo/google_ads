"""Startup preflight: does THIS process have what its scheduled connections need?

The incident this exists for: LeadSquared's credentials were added to the developer's
.env and to code, but never to the production service's environment. Every scheduled
run failed with "LeadSquared is not configured" — quietly, every three hours — while
manual runs from a machine that did have the credentials kept succeeding.

A scheduler process must therefore check, at startup and again in /healthz, that every
enabled connection's connector has the settings it declares in `requires_settings`,
and say so loudly if not. (It only applies to processes that actually schedule runs.)
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select

from app.connectors.registry import availability, load_connectors
from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import CONN_INVALID_CONFIG, Connection

logger = get_logger(__name__)


@dataclass(frozen=True)
class Misconfigured:
    connection_id: int
    connection_name: str
    connector_id: str
    reason: str


async def misconfigured_connections() -> list[Misconfigured]:
    """Enabled connections whose connector cannot run in this process's environment."""
    settings = get_settings()
    registry = load_connectors()
    out: list[Misconfigured] = []
    async with SessionLocal() as session:
        rows = (await session.execute(select(Connection).where(Connection.enabled.is_(True)))).scalars().all()
    for conn in rows:
        if conn.connector_id not in registry:
            out.append(Misconfigured(conn.id, conn.name, conn.connector_id, "unknown connector"))
            continue
        ok, reason = availability(registry.get(conn.connector_id), settings)
        if not ok:
            out.append(Misconfigured(conn.id, conn.name, conn.connector_id, reason or "not available"))
    return out


async def run_preflight(*, mark_connections: bool = True) -> list[Misconfigured]:
    """Log every misconfiguration at ERROR and, optionally, flag the connections so the
    problem is visible in the API/UI immediately rather than after the first failed run."""
    problems = await misconfigured_connections()
    for p in problems:
        logger.error(
            "PREFLIGHT: connection %s (%s, connector %s) cannot run in this process — %s "
            "Scheduled runs of it WILL fail until the environment is fixed.",
            p.connection_id,
            p.connection_name,
            p.connector_id,
            p.reason,
        )
    if problems and mark_connections:
        async with SessionLocal() as session:
            for p in problems:
                conn = await session.get(Connection, p.connection_id)
                if conn is not None and conn.status not in ("syncing",):
                    conn.status = CONN_INVALID_CONFIG
                    conn.status_detail = f"Misconfigured in the scheduler process: {p.reason}"[:2000]
            await session.commit()
    return problems


__all__ = ["Misconfigured", "misconfigured_connections", "run_preflight"]
