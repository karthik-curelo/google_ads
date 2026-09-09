"""Async database engine, session factory, and dialect-aware bulk upsert.

Postgres is the production target; SQLite is the zero-setup default so that
migrations, the test suite, and `uvicorn app.main:app` all run on a clean
checkout with no container runtime. The only place the two dialects genuinely
diverge for our workload is INSERT ... ON CONFLICT, which is isolated in
`bulk_upsert` below.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from functools import lru_cache
from typing import Any

from sqlalchemy import Table, event, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings

settings = get_settings()

_is_sqlite = settings.database_url.startswith("sqlite")

_engine_kwargs: dict[str, Any] = {"echo": settings.db_echo, "future": True}
if not _is_sqlite:
    # Connection pooling matters once several syncs write concurrently.
    _engine_kwargs.update(pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=1800)

engine: AsyncEngine = create_async_engine(settings.database_url, **_engine_kwargs)

if _is_sqlite:

    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_pragmas(dbapi_connection, _record):  # pragma: no cover - driver hook
        cursor = dbapi_connection.cursor()
        # WAL lets the scheduler read while a sync writes; without it SQLite
        # serialises everything and the poller trips over its own writes.
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=10000")
        cursor.close()


SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped session."""
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def dialect_name() -> str:
    return engine.dialect.name


@lru_cache
def destination_info() -> dict[str, Any]:
    """Where synced rows land — for the UI. Credentials are never included."""
    from app.sync.writer import CONNECTOR_MODEL_MAP

    url = make_url(settings.database_url)
    backend = url.get_backend_name()  # 'postgresql' | 'sqlite'
    return {
        "engine": backend,
        "database": url.database,
        "host": None if backend == "sqlite" else url.host,
        # One performance table per source (fact grain); ad_entities holds the
        # Google/Meta campaign-tree attributes for every source that has them.
        "fact_tables": {cid: m.__tablename__ for cid, m in CONNECTOR_MODEL_MAP.items()},
        "entity_table": "ad_entities",
    }


async def bulk_upsert(
    session: AsyncSession,
    table: Table,
    rows: Sequence[dict[str, Any]],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
    chunk_size: int = 500,
) -> int:
    """Batch INSERT ... ON CONFLICT DO UPDATE. Returns rows submitted.

    This is the `append_dedup` destination mode: analytics providers restate
    recent days, so re-ingesting an overlapping window must correct rows rather
    than duplicate them. Chunked because both drivers bind one parameter per
    column per row and SQLite caps at 32k bind parameters.

    ponytail: supports exactly the two dialects we ship (postgresql, sqlite).
    Add a branch here if a third destination is ever needed.
    """
    if not rows:
        return 0

    if update_columns is None:
        update_columns = [c for c in rows[0] if c not in conflict_columns]

    dialect = dialect_name()
    if dialect == "postgresql":
        stmt_factory = pg_insert
    elif dialect == "sqlite":
        stmt_factory = sqlite_insert
    else:  # pragma: no cover - guarded by config
        raise NotImplementedError(
            f"bulk_upsert has no ON CONFLICT support for dialect {dialect!r}; use postgresql or sqlite."
        )

    total = 0
    # Keep every row's column set identical — executemany degrades badly (and on
    # SQLite errors outright) if the dicts disagree on keys.
    columns = list(rows[0].keys())
    for start in range(0, len(rows), chunk_size):
        chunk = [{c: row.get(c) for c in columns} for row in rows[start : start + chunk_size]]
        stmt = stmt_factory(table).values(chunk)
        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=list(conflict_columns),
                set_={c: stmt.excluded[c] for c in update_columns},
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=list(conflict_columns))
        await session.execute(stmt)
        total += len(chunk)
    return total


async def bulk_insert(
    session: AsyncSession,
    table: Table,
    rows: Sequence[dict[str, Any]],
    chunk_size: int = 500,
) -> int:
    if not rows:
        return 0
    columns = list(rows[0].keys())
    for start in range(0, len(rows), chunk_size):
        chunk = [{c: row.get(c) for c in columns} for row in rows[start : start + chunk_size]]
        await session.execute(insert(table), chunk)
    return len(rows)
