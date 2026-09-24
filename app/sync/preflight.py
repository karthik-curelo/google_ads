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
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from app.connectors.registry import availability, load_connectors
from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import CONN_INVALID_CONFIG, Connection

logger = get_logger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[2]


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
    drift = await schema_drift()
    if drift is not None:
        logger.error("PREFLIGHT: %s", drift.reason)
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


@dataclass(frozen=True)
class SchemaDrift:
    reason: str


def _code_head_revisions() -> set[str]:
    """The migration revision(s) this running code's model layer expects the schema to
    already be at — read from the migration files on disk, not from any live connection."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    return set(ScriptDirectory.from_config(cfg).get_heads())


async def _current_db_revisions() -> set[str] | None:
    """None means "can't tell" (no `alembic_version` table yet, e.g. a brand-new dev
    database) — never treated as drift."""
    async with SessionLocal() as session:
        try:
            result = await session.execute(text("SELECT version_num FROM alembic_version"))
        except DBAPIError:
            return None
        return {row[0] for row in result.all()}


async def schema_drift() -> SchemaDrift | None:
    """Is this process's code ahead of the database it is talking to?

    The incident this exists for (2026-09-23): the LSQ dietician/disposition columns
    (migration e2a9c5b17f03) shipped in the same deploy as the code that writes them, and
    the migration was never run. Every scheduled sync of the `leads` stream then failed —
    quietly, every three hours — on an "column does not exist" error buried in a stack
    trace, while the connection's OTHER streams (and every other connector) kept
    succeeding, so the connection still showed healthy. Nothing said "the schema is behind
    the code" anywhere a human would see it before this.
    """
    current = await _current_db_revisions()
    if current is None:
        return None
    heads = _code_head_revisions()
    if current == heads:
        return None
    return SchemaDrift(
        f"database schema is at {sorted(current)}, this code expects {sorted(heads)} — "
        "run `alembic upgrade head` (or the specific revision being deployed) before "
        "scheduled syncs can fully succeed here"
    )


__all__ = ["Misconfigured", "SchemaDrift", "misconfigured_connections", "run_preflight", "schema_drift"]
