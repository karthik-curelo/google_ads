"""Destination (warehouse) tables.

§11 asks for a common model that does not destroy provider-native detail. For
five marketing sources the useful shape is *not* twenty-five bespoke tables —
the whole reason to ingest Google Ads and Meta Ads together is to compare spend
in one query, and bespoke tables make that a union of incompatible schemas.

So the model is two tables:

  report_rows   the fact grain (one row per stream x date x dimension tuple),
                with the handful of measures every marketing provider shares
                promoted to typed columns, and the full provider-native
                dimensions/metrics/raw payload retained as JSON alongside.

  ad_entities   the entity grain (campaign, ad set/ad group, ad, keyword) —
                slowly-changing attributes that do not belong in a daily fact
                table, shared by Google Ads and Meta Ads.

Derived ratios (CTR, CPC, CPA, ROAS) are deliberately *not* stored. They are
computed on read from the stored measures, so they can never disagree with the
numbers they come from after a restatement.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Date,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, declared_attr

from app.models.base import Base, JSONType, PKType, TimestampType, utcnow

# Numeric(20, 6) holds ad spend and revenue without float drift, and is wide
# enough for GA4 revenue on a large property.
Money = Numeric(20, 6)


def make_record_key(stream: str, key_values: dict[str, Any]) -> str:
    """Stable identity for a fact row, used as the upsert conflict target.

    Hashed rather than concatenated because the natural key can be a page path
    or a search query — unbounded length, and unusable in a btree index.
    Separators are explicit so {"a": "b|c"} and {"a": "b", "?": "c"} cannot
    collide.
    """
    payload = json.dumps(
        {"stream": stream, "key": {k: key_values[k] for k in sorted(key_values)}},
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:40]


class PerformanceRowMixin:
    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)

    # --- ownership & provenance -------------------------------------------
    @declared_attr
    def organization_id(cls) -> Mapped[int]:
        return mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)

    @declared_attr
    def connection_id(cls) -> Mapped[int]:
        return mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), nullable=False)

    connector_id: Mapped[str] = mapped_column(String(80), nullable=False)
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    stream: Mapped[str] = mapped_column(String(120), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(200), nullable=False)
    record_key: Mapped[str] = mapped_column(String(40), nullable=False)

    # --- grain -------------------------------------------------------------
    date: Mapped[date | None] = mapped_column(Date, index=True)

    # --- provider-native detail (nothing is lost) -------------------------
    dimensions: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONType)

    # --- bookkeeping -------------------------------------------------------
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    sync_run_id: Mapped[int | None] = mapped_column(Integer)
    source_updated_at: Mapped[datetime | None] = mapped_column(TimestampType)
    ingested_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)

    @declared_attr
    def __table_args__(cls):
        return (
            UniqueConstraint("connection_id", "stream", "record_key", name=f"uq_{cls.__tablename__}_conn_stream_key"),
            Index(f"ix_{cls.__tablename__}_org_conn_date", "organization_id", "connector_id", "date"),
            Index(f"ix_{cls.__tablename__}_conn_stream_date", "connection_id", "stream", "date"),
            Index(f"ix_{cls.__tablename__}_sync_run", "sync_run_id"),
        )


class GoogleAdsPerformance(PerformanceRowMixin, Base):
    __tablename__ = "google_ads_performance"

    impressions: Mapped[int | None] = mapped_column(Integer)
    clicks: Mapped[int | None] = mapped_column(Integer)
    cost: Mapped[float | None] = mapped_column(Money)
    currency: Mapped[str | None] = mapped_column(String(8))
    conversions: Mapped[float | None] = mapped_column(Money)
    conversion_value: Mapped[float | None] = mapped_column(Money)


class MetaAdsPerformance(PerformanceRowMixin, Base):
    __tablename__ = "meta_ads_performance"

    impressions: Mapped[int | None] = mapped_column(Integer)
    clicks: Mapped[int | None] = mapped_column(Integer)
    reach: Mapped[int | None] = mapped_column(Integer)
    cost: Mapped[float | None] = mapped_column(Money)
    currency: Mapped[str | None] = mapped_column(String(8))
    conversions: Mapped[float | None] = mapped_column(Money)
    conversion_value: Mapped[float | None] = mapped_column(Money)


class GoogleAnalyticsPerformance(PerformanceRowMixin, Base):
    __tablename__ = "google_analytics_performance"

    sessions: Mapped[int | None] = mapped_column(Integer)
    users: Mapped[int | None] = mapped_column(Integer)
    new_users: Mapped[int | None] = mapped_column(Integer)
    page_views: Mapped[int | None] = mapped_column(Integer)
    engaged_sessions: Mapped[int | None] = mapped_column(Integer)
    bounce_rate: Mapped[float | None] = mapped_column(Numeric(12, 6))
    engagement_rate: Mapped[float | None] = mapped_column(Numeric(12, 6))
    event_count: Mapped[int | None] = mapped_column(Integer)
    conversions: Mapped[float | None] = mapped_column(Money)
    revenue: Mapped[float | None] = mapped_column(Money)
    currency: Mapped[str | None] = mapped_column(String(8))
    avg_session_duration: Mapped[float | None] = mapped_column(Numeric(14, 4))
    screen_page_views_per_session: Mapped[float | None] = mapped_column(Numeric(12, 4))
    user_engagement_duration: Mapped[float | None] = mapped_column(Numeric(16, 2))


class GoogleSearchConsolePerformance(PerformanceRowMixin, Base):
    __tablename__ = "google_search_console_performance"

    impressions: Mapped[int | None] = mapped_column(Integer)
    clicks: Mapped[int | None] = mapped_column(Integer)
    average_position: Mapped[float | None] = mapped_column(Numeric(10, 4))



class AdEntity(Base):
    """Campaign / ad group / ad / keyword attributes, Google Ads and Meta Ads."""

    __tablename__ = "ad_entities"
    __table_args__ = (
        UniqueConstraint("connection_id", "level", "external_id", name="uq_ad_entities_connection_level_id"),
        Index("ix_ad_entities_org_provider_level", "organization_id", "provider", "level"),
    )

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("connections.id", ondelete="CASCADE"), nullable=False
    )
    connector_id: Mapped[str] = mapped_column(String(80), nullable=False)
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(200), nullable=False)

    level: Mapped[str] = mapped_column(String(30), nullable=False)  # campaign|ad_group|ad|keyword
    external_id: Mapped[str] = mapped_column(String(120), nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(String(40))
    parent_external_id: Mapped[str | None] = mapped_column(String(120))
    channel: Mapped[str | None] = mapped_column(String(60))
    objective: Mapped[str | None] = mapped_column(String(80))
    daily_budget: Mapped[float | None] = mapped_column(Money)
    lifetime_budget: Mapped[float | None] = mapped_column(Money)
    currency: Mapped[str | None] = mapped_column(String(8))
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONType)

    sync_run_id: Mapped[int | None] = mapped_column(Integer)
    ingested_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TimestampType, default=utcnow, onupdate=utcnow, nullable=False
    )


class SkippedRecord(Base):
    """Records dropped during validation, with the reason.

    §19: never silently lose a record. If a provider hands back a row we cannot
    map, it lands here rather than vanishing, so the count in the run summary is
    always explainable.
    """

    __tablename__ = "skipped_records"

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sync_run_id: Mapped[int | None] = mapped_column(Integer, index=True)
    stream: Mapped[str] = mapped_column(String(120), nullable=False)
    reason: Mapped[str] = mapped_column(String(200), nullable=False)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONType)
    occurred_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)


__all__ = [
    "AdEntity",
    "SkippedRecord",
    "make_record_key",
    "GoogleAdsPerformance",
    "MetaAdsPerformance",
    "GoogleAnalyticsPerformance",
    "GoogleSearchConsolePerformance",
]
