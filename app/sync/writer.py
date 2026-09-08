"""Destination writer (§11, §18) — records in, committed warehouse rows out.

Two grains, two tables, one code path each:

  fact rows    -> dedicated performance tables, upserted on (connection, stream, record_key) so a
                 lookback re-fetch corrects yesterday's restated numbers instead
                 of duplicating them (§12 append_dedup).
  entity rows  -> ad_entities, upserted on (connection, level, external_id).

Everything a provider returned survives: the typed cross-provider measures are
promoted to columns for one-query spend comparison, and the untouched
dimensions / metrics / raw payload ride alongside as JSON (§11 "do not destroy
provider-native information").

Writes run in their own short transactions, batched, never holding a lock open
across an HTTP call (§32).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from app.connectors.base import EntityRecord, Record, StreamDefinition
from app.connectors.validation import validate_records
from app.core.database import SessionLocal, bulk_insert, bulk_upsert
from app.core.logging import get_logger
from app.models import (
    AdEntity,
    GoogleAdsPerformance,
    GoogleAnalyticsPerformance,
    GoogleSearchConsolePerformance,
    MetaAdsPerformance,
    SkippedRecord,
    make_record_key,
)

logger = get_logger(__name__)

MEASURE_COLUMNS: frozenset[str] = frozenset(
    {
        "impressions",
        "clicks",
        "cost",
        "conversions",
        "conversion_value",
        "revenue",
        "sessions",
        "users",
        "new_users",
        "page_views",
        "engaged_sessions",
        "reach",
        "average_position",
        "event_count",
        "engagement_rate",
        "bounce_rate",
        "avg_session_duration",
        "screen_page_views_per_session",
        "user_engagement_duration",
    }
)

CONNECTOR_MODEL_MAP = {
    "google_ads": GoogleAdsPerformance,
    "google_analytics": GoogleAnalyticsPerformance,
    "google_search_console": GoogleSearchConsolePerformance,
    "meta_ads": MetaAdsPerformance,
}

_ENTITY_CONFLICT = ("connection_id", "level", "external_id")
_ENTITY_UPDATE = (
    "name",
    "status",
    "parent_external_id",
    "channel",
    "objective",
    "daily_budget",
    "lifetime_budget",
    "currency",
    "start_date",
    "end_date",
    "raw",
    "sync_run_id",
    "updated_at",
)


class WriteResult:
    __slots__ = ("fetched", "inserted", "updated", "skipped")

    def __init__(self) -> None:
        self.fetched = 0
        self.inserted = 0
        self.updated = 0
        self.skipped = 0

    def add(self, other: WriteResult) -> None:
        self.fetched += other.fetched
        self.inserted += other.inserted
        self.updated += other.updated
        self.skipped += other.skipped

    def as_dict(self) -> dict[str, int]:
        return {
            "records_fetched": self.fetched,
            "records_inserted": self.inserted,
            "records_updated": self.updated,
            "records_skipped": self.skipped,
        }


class DestinationWriter:
    """Owns the warehouse write for one connection's run."""

    def __init__(
        self,
        *,
        organization_id: int,
        connection_id: int,
        connector_id: str,
        provider: str,
        resource_id: str,
        sync_run_id: int | None,
    ) -> None:
        self.organization_id = organization_id
        self.connection_id = connection_id
        self.connector_id = connector_id
        self.provider = provider
        self.resource_id = resource_id
        self.sync_run_id = sync_run_id
        self.model = CONNECTOR_MODEL_MAP.get(self.connector_id)

    # --- fact grain ------------------------------------------------------------
    async def write_records(
        self,
        stream: StreamDefinition,
        records: Sequence[Record],
        *,
        schema_version: int = 1,
    ) -> WriteResult:
        result = WriteResult()
        result.fetched = len(records)
        if not records:
            return result

        if not self.model:
            raise ValueError(f"No performance model mapped for connector {self.connector_id}")

        require_date = stream.date_partitioned and stream.grain == "fact"
        validated = validate_records(list(records), stream.primary_key, require_date=require_date)

        rows: list[dict[str, Any]] = []
        for record in validated.valid:
            rows.append(self._report_row(stream, record, schema_version))

        async with SessionLocal() as session:
            if rows:
                existing = await self._count_existing(session, stream.name, [r["record_key"] for r in rows])
                
                # Dynamically determine the update columns based on the mapped model
                update_cols = [
                    c.name for c in self.model.__table__.columns
                    if c.name not in (
                        "id", 
                        "organization_id", 
                        "connection_id", 
                        "connector_id", 
                        "provider", 
                        "stream", 
                        "resource_id", 
                        "record_key"
                    )
                ]
                
                await bulk_upsert(
                    session,
                    self.model.__table__,
                    rows,
                    conflict_columns=("connection_id", "stream", "record_key"),
                    update_columns=tuple(update_cols),
                )
                result.updated = existing
                result.inserted = len(rows) - existing
            if validated.skipped:
                await self._record_skips(session, stream.name, validated.skipped)
                result.skipped = len(validated.skipped)
            await session.commit()

        return result

    def _report_row(self, stream: StreamDefinition, record: Record, schema_version: int) -> dict[str, Any]:
        row: dict[str, Any] = {
            "organization_id": self.organization_id,
            "connection_id": self.connection_id,
            "connector_id": self.connector_id,
            "provider": self.provider,
            "stream": stream.name,
            "resource_id": self.resource_id,
            "record_key": make_record_key(stream.name, record.key_values),
            "date": record.date,
            "dimensions": record.dimensions or {},
            "metrics": record.metrics or {},
            "raw": record.raw,
            "currency": record.currency or (record.measures or {}).get("currency"),
            "schema_version": schema_version,
            "sync_run_id": self.sync_run_id,
            "source_updated_at": None,
        }
        measures = record.measures or {}
        # Only populate measure columns that actually exist on this specific table
        if self.model:
            model_cols = {c.name for c in self.model.__table__.columns}
            for col in MEASURE_COLUMNS:
                if col in model_cols:
                    row[col] = measures.get(col)
        return row

    async def _count_existing(self, session, stream: str, keys: list[str]) -> int:
        from sqlalchemy import func, select

        if not keys or not self.model:
            return 0
        stmt = (
            select(func.count())
            .select_from(self.model.__table__)
            .where(
                self.model.connection_id == self.connection_id,
                self.model.stream == stream,
                self.model.record_key.in_(keys),
            )
        )
        return int((await session.execute(stmt)).scalar_one())

    # --- entity grain -------------------------------------------------------
    async def write_entities(self, records: Sequence[EntityRecord]) -> WriteResult:
        result = WriteResult()
        result.fetched = len(records)
        if not records:
            return result

        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for record in records:
            if not record.external_id or not record.level:
                result.skipped += 1
                continue
            fp = (record.level, record.external_id)
            if fp in seen:
                result.skipped += 1
                continue
            seen.add(fp)
            rows.append(self._entity_row(record))

        async with SessionLocal() as session:
            if rows:
                existing = await self._count_existing_entities(session, rows)
                await bulk_upsert(
                    session,
                    AdEntity.__table__,
                    rows,
                    conflict_columns=_ENTITY_CONFLICT,
                    update_columns=_ENTITY_UPDATE,
                )
                result.updated = existing
                result.inserted = len(rows) - existing
            await session.commit()
        return result

    def _entity_row(self, record: EntityRecord) -> dict[str, Any]:
        from app.models.base import utcnow

        return {
            "organization_id": self.organization_id,
            "connection_id": self.connection_id,
            "connector_id": self.connector_id,
            "provider": self.provider,
            "resource_id": self.resource_id,
            "level": record.level,
            "external_id": str(record.external_id),
            "name": record.name,
            "status": record.status,
            "parent_external_id": record.parent_external_id,
            "channel": record.channel,
            "objective": record.objective,
            "daily_budget": record.daily_budget,
            "lifetime_budget": record.lifetime_budget,
            "currency": record.currency,
            "start_date": record.start_date,
            "end_date": record.end_date,
            "raw": record.raw,
            "sync_run_id": self.sync_run_id,
            "updated_at": utcnow(),
        }

    async def _count_existing_entities(self, session, rows: list[dict[str, Any]]) -> int:
        from sqlalchemy import func, select, tuple_

        pairs = [(r["level"], r["external_id"]) for r in rows]
        stmt = (
            select(func.count())
            .select_from(AdEntity.__table__)
            .where(
                AdEntity.connection_id == self.connection_id,
                tuple_(AdEntity.level, AdEntity.external_id).in_(pairs),
            )
        )
        return int((await session.execute(stmt)).scalar_one())

    # --- skipped-record accounting ---------------------------------------
    async def _record_skips(self, session, stream: str, skips: list[tuple[dict[str, Any], str]]) -> None:
        rows = [
            {
                "organization_id": self.organization_id,
                "connection_id": self.connection_id,
                "sync_run_id": self.sync_run_id,
                "stream": stream,
                "reason": reason[:200],
                "payload": payload,
            }
            for payload, reason in skips[:500]  # cap: a broken stream must not flood
        ]
        await bulk_insert(session, SkippedRecord.__table__, rows)
        for _payload, reason in skips[:20]:
            logger.warning("skipped record in %s: %s", stream, reason)


def coerce_measures(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep only recognised measure keys; drop the rest to metrics JSON upstream."""
    return {k: v for k, v in raw.items() if k in MEASURE_COLUMNS or k == "currency"}


__all__ = ["DestinationWriter", "MEASURE_COLUMNS", "WriteResult", "coerce_measures"]
