"""Shared SQLAlchemy declarative base and dialect-portable column types."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import BigInteger, DateTime, Integer, MetaData, TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import JSON

# Explicit naming convention so Alembic can autogenerate reversible constraint
# drops — without it, unnamed constraints are un-droppable on Postgres.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def utcnow() -> datetime:
    return datetime.now(UTC)


# JSONB on Postgres (indexable, binary), plain JSON on SQLite.
JSONType = JSON().with_variant(JSONB(), "postgresql")

# SQLite has no BIGSERIAL; plain INTEGER PRIMARY KEY is its rowid alias and the
# only form it will autoincrement.
PKType = BigInteger().with_variant(Integer(), "sqlite")


class UTCDateTime(TypeDecorator):
    """Always store and return timezone-aware UTC.

    SQLite (the dev/test database) has no native tz and hands back naive
    datetimes, which then blow up when compared against `datetime.now(UTC)` in
    the scheduler's due-query and elsewhere. Normalising in one type decorator
    fixes it for both dialects instead of scattering `.replace(tzinfo=...)`.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, _dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value: datetime | None, _dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


TimestampType = UTCDateTime()
