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

from sqlalchemy import and_, or_

from app.connectors.base import EntityRecord, Record, StreamDefinition
from app.connectors.leadsquared.activity_catalog import event_for_stream_name
from app.connectors.validation import validate_records
from app.core.database import SessionLocal, bulk_insert, bulk_upsert
from app.core.logging import get_logger
from app.models import (
    AdEntity,
    FacebookPagesPerformance,
    GoogleAdsPerformance,
    GoogleAnalyticsPerformance,
    GoogleSearchConsolePerformance,
    InstagramInsightsPerformance,
    LeadsquaredActivity,
    LeadsquaredLead,
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
        "views",
    }
)

CONNECTOR_MODEL_MAP = {
    "google_ads": GoogleAdsPerformance,
    "google_analytics": GoogleAnalyticsPerformance,
    "google_search_console": GoogleSearchConsolePerformance,
    "meta_ads": MetaAdsPerformance,
    "instagram_insights": InstagramInsightsPerformance,
    "facebook_pages": FacebookPagesPerformance,
    # LeadSquared's "default" table for the generic single-table UI/data
    # endpoints — the leads table, since it is the one every LSQ connection
    # always has. Its activity streams are NOT single-table (see below) and
    # need STREAM_MODEL_OVERRIDES to route correctly.
    "leadsquared": LeadsquaredLead,
}

# Per-(connector, stream) destination override — the one thing
# CONNECTOR_MODEL_MAP cannot express, because it assumes one table per
# connector. LeadSquared is the first source with more than one destination
# table (a lead-grain table and a single activity-grain table shared by every
# tracked event type): its `leads` stream still resolves through
# CONNECTOR_MODEL_MAP above, but every activity stream needs to land in
# `leadsquared_activities` instead. Checked before CONNECTOR_MODEL_MAP, so it
# is a pure override — connectors absent here behave exactly as before.
STREAM_MODEL_OVERRIDES: dict[str, dict[str, Any]] = {
    # Kept for explicit per-stream routing of any future multi-table connector.
    # LeadSquared no longer needs entries here: every activity stream (all 84 types
    # plus any type discovered later) is routed by `_model_for` below.
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
    __slots__ = ("fetched", "inserted", "updated", "skipped", "persisted", "unchanged")

    def __init__(self) -> None:
        self.fetched = 0
        self.inserted = 0
        self.updated = 0
        self.skipped = 0
        # Existing rows that were re-submitted but NOT rewritten because the stored
        # row was already current (the guarded upsert left them alone).
        self.unchanged = 0
        # Submitted rows found in the table AFTER the transaction committed —
        # measured by a follow-up query, not inferred from the upsert. This is the
        # number reconciliation compares with the source's count.
        self.persisted = 0

    def add(self, other: WriteResult) -> None:
        self.fetched += other.fetched
        self.inserted += other.inserted
        self.updated += other.updated
        self.skipped += other.skipped
        self.persisted += other.persisted
        self.unchanged += other.unchanged

    def as_dict(self) -> dict[str, int]:
        return {
            "records_fetched": self.fetched,
            "records_inserted": self.inserted,
            "records_updated": self.updated,
            "records_skipped": self.skipped,
        }


def _lsq_upsert_guard(table, excluded):
    """When may an incoming LeadSquared row overwrite the stored one?

    Evaluated by the database inside the INSERT ... ON CONFLICT statement, so it is
    atomic with the write and holds no matter how two workers' commits interleave:

      * the stored row has no source timestamp yet (written before this column
        existed) -> take the incoming row;
      * the incoming row is strictly newer -> take it;
      * a tombstoned row reappeared at the source -> take it (clears `deleted_at`);
      * same timestamp but different payload (e.g. the complete-payload upgrade of
        an unchanged lead) -> take it;
      * otherwise (older, or identical) -> leave the stored row alone, which makes
        replaying an overlap window a no-op and stops a stale fetch from ever
        overwriting fresher data.
    """
    return or_(
        table.c.source_modified_on.is_(None),
        excluded.source_modified_on > table.c.source_modified_on,
        table.c.deleted_at.isnot(None),
        and_(
            excluded.source_modified_on == table.c.source_modified_on,
            table.c.raw.is_distinct_from(excluded.raw),
        ),
    )


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

    def _model_for(self, stream_name: str):
        """Resolve the destination table for one stream — almost always
        `self.model`, except for a connector with more than one destination
        table (LeadSquared: `leads` -> leadsquared_leads, every activity type ->
        leadsquared_activities)."""
        explicit = STREAM_MODEL_OVERRIDES.get(self.connector_id, {}).get(stream_name)
        if explicit is not None:
            return explicit
        if self.connector_id == "leadsquared" and event_for_stream_name(stream_name) is not None:
            return LeadsquaredActivity
        return self.model

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

        model = self._model_for(stream.name)
        if not model:
            raise ValueError(
                f"No performance model mapped for connector {self.connector_id} / stream {stream.name}"
            )

        require_date = stream.date_partitioned and stream.grain == "fact"
        validated = validate_records(list(records), stream.primary_key, require_date=require_date)

        rows: list[dict[str, Any]] = []
        for record in validated.valid:
            rows.append(self._report_row(stream, record, schema_version, model))

        async with SessionLocal() as session:
            if rows:
                existing = await self._count_existing(
                    session, stream.name, [r["record_key"] for r in rows], model
                )

                # Update every column except identity/ownership and `ingested_at`
                # (that records first-seen; a lookback re-fetch must not bump it).
                _frozen = {
                    "id",
                    "organization_id",
                    "connection_id",
                    "connector_id",
                    "provider",
                    "stream",
                    "resource_id",
                    "record_key",
                    "ingested_at",
                }
                update_cols = [c.name for c in model.__table__.columns if c.name not in _frozen]

                await bulk_upsert(
                    session,
                    model.__table__,
                    rows,
                    conflict_columns=("connection_id", "stream", "record_key"),
                    update_columns=tuple(update_cols),
                    update_where=_lsq_upsert_guard if "source_modified_on" in model.__table__.c else None,
                )
                result.updated = existing
                result.inserted = len(rows) - existing
            if validated.skipped:
                await self._record_skips(session, stream.name, validated.skipped)
                result.skipped = len(validated.skipped)
            await session.commit()
            if rows:
                # After the commit, so it sees exactly what a later reader will.
                keys = [r["record_key"] for r in rows]
                result.persisted = await self._count_existing(session, stream.name, keys, model)
                if self.sync_run_id is not None:
                    # A row the upsert actually wrote carries this run's id; a row the
                    # guard left alone keeps its old one. So "updated" is what was really
                    # rewritten, not merely what was re-submitted.
                    written = await self._count_written_by_this_run(session, stream.name, keys, model)
                    result.updated = max(0, written - result.inserted)
                    result.unchanged = max(0, existing - result.updated)

        return result

    def _report_row(
        self, stream: StreamDefinition, record: Record, schema_version: int, model: Any = None
    ) -> dict[str, Any]:
        model = model or self.model
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
        model_cols = {c.name for c in model.__table__.columns} if model else set()
        # Typed source-specific values (real datetimes/ints) go straight to their
        # columns; anything that is not a column of this table is dropped below.
        for key, value in (record.extra or {}).items():
            if key in model_cols:
                row[key] = value
        for col in MEASURE_COLUMNS:
            if col in model_cols:
                row[col] = measures.get(col)
        # A table can declare its own extra identity/join columns beyond the
        # shared measure set (e.g. leadsquared_leads.prospect_id,
        # leadsquared_activities.booking_id) — promote any dimension whose
        # name matches one, so a connector gets a real, indexed column just by
        # naming its dimensions after it, with no per-connector writer code.
        # A no-op for every pre-existing table: none of them declare extra
        # columns beyond what MEASURE_COLUMNS already covers, so this can
        # never re-route an existing connector's dimension into a column it
        # did not already have.
        for key, value in (record.dimensions or {}).items():
            if key in model_cols and key not in row:
                row[key] = value
        # Per-source tables differ (Search Console has no `currency`, GA4 no
        # `reach`, …). Drop any key that is not a real column on this table so
        # the INSERT does not reference a column that does not exist.
        return {k: v for k, v in row.items() if k in model_cols} if model_cols else row

    async def _count_written_by_this_run(self, session, stream: str, keys: list[str], model: Any) -> int:
        from sqlalchemy import func, select

        stmt = (
            select(func.count())
            .select_from(model.__table__)
            .where(
                model.connection_id == self.connection_id,
                model.stream == stream,
                model.record_key.in_(keys),
                model.sync_run_id == self.sync_run_id,
            )
        )
        return int((await session.execute(stmt)).scalar_one())

    async def _count_existing(self, session, stream: str, keys: list[str], model: Any = None) -> int:
        from sqlalchemy import func, select

        model = model or self.model
        if not keys or not model:
            return 0
        stmt = (
            select(func.count())
            .select_from(model.__table__)
            .where(
                model.connection_id == self.connection_id,
                model.stream == stream,
                model.record_key.in_(keys),
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


__all__ = [
    "CONNECTOR_MODEL_MAP",
    "STREAM_MODEL_OVERRIDES",
    "DestinationWriter",
    "MEASURE_COLUMNS",
    "WriteResult",
    "coerce_measures",
]
