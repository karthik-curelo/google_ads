"""Destination (warehouse) tables.

The fact grain is one physical table per source
(`<connector_id>_performance`), all sharing `PerformanceRowMixin`: the common
provenance columns, `date`, the full provider-native `dimensions` / `metrics` /
`raw` payload as JSON, and a small set of typed measure columns that make sense
for that source (spend/clicks/conversions for the ad sources, sessions/users for
GA4, clicks/impressions/position for Search Console, …). A cross-provider spend
comparison is a `UNION ALL` over the ad tables rather than a filter on one wide
table.

  <source>_performance   fact grain — google_ads_performance,
                          google_analytics_performance,
                          google_search_console_performance, meta_ads_performance,
                          instagram_insights_performance, facebook_pages_performance.

  ad_entities             entity grain (campaign, ad set/ad group, ad, keyword,
                          creative, sitemap, media, page, post) — slowly-changing
                          attributes shared by every source that has them.

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
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

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
            UniqueConstraint(
                "connection_id", "stream", "record_key", name=f"uq_{cls.__tablename__}_conn_stream_key"
            ),
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


class InstagramInsightsPerformance(PerformanceRowMixin, Base):
    __tablename__ = "instagram_insights_performance"

    reach: Mapped[int | None] = mapped_column(Integer)
    impressions: Mapped[int | None] = mapped_column(Integer)
    views: Mapped[int | None] = mapped_column(Integer)


class FacebookPagesPerformance(PerformanceRowMixin, Base):
    __tablename__ = "facebook_pages_performance"

    impressions: Mapped[int | None] = mapped_column(Integer)
    reach: Mapped[int | None] = mapped_column(Integer)
    clicks: Mapped[int | None] = mapped_column(Integer)


class LeadsquaredLead(PerformanceRowMixin, Base):
    """One row per LeadSquared lead (ProspectID), upserted on `ModifiedOn`.

    Record grain, not daily-aggregate — the one source in this warehouse that
    isn't a `<source>_performance` fact table in spirit, but reuses the exact
    same mixin/writer/cursor machinery (§Phase A of the LSQ implementation:
    docs/coverage/LSQ_VERIFICATION_2026-09-11.md is the design baseline).

    Only the true identity key (`prospect_id`) and the one field every
    data-quality/classification query needs (`source`) are promoted to typed
    columns. Everything else — campaign/ad/adset ids, GCLID, UTM, lead type,
    phone, email, slug — stays in `dimensions`/`raw` JSON on purpose: the
    verification report found live, real case-duplication in `Source`
    (`google_lp` vs `Google_lp`) and cross-platform ID contamination in the
    generic attribution fields, so nothing about their shape is stable enough
    to bake into a schema. Extracting by name from JSON at query time is the
    deliberate choice, not a shortcut (§Phase 14 / verification §7).
    """

    __tablename__ = "leadsquared_leads"

    prospect_id: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str | None] = mapped_column(String(120))

    @declared_attr
    def __table_args__(cls):
        return (
            UniqueConstraint(
                "connection_id", "stream", "record_key", name=f"uq_{cls.__tablename__}_conn_stream_key"
            ),
            UniqueConstraint("connection_id", "prospect_id", name=f"uq_{cls.__tablename__}_conn_prospect"),
            Index(f"ix_{cls.__tablename__}_org_conn_date", "organization_id", "connector_id", "date"),
            Index(f"ix_{cls.__tablename__}_conn_stream_date", "connection_id", "stream", "date"),
            Index(f"ix_{cls.__tablename__}_sync_run", "sync_run_id"),
            Index(f"ix_{cls.__tablename__}_prospect", "prospect_id"),
            Index(f"ix_{cls.__tablename__}_source", "connection_id", "source"),
        )


class LeadsquaredActivity(PerformanceRowMixin, Base):
    """One row per LeadSquared activity (`ProspectActivityId`) — every tracked
    event type (Booking Created / Post Booking Order Status / Booking
    Cancelled / Facebook Lead Ads Submissions) lands in this single physical
    table, distinguished by `stream`, exactly like `PerformanceRowMixin`
    already dedupes any other source's rows by `(connection_id, stream,
    record_key)`. Deliberately NOT one table per event type: the verification
    report confirmed each event type maps its custom fields (including
    "Booking ID") to a *different* `mx_Custom_N` slot, so a fixed per-event
    schema would need N tables anyway with no shared query surface — a single
    JSONB-backed table with the identity/join keys normalised at ingestion
    time is the model the verification report settled on (§Phase 14 / §B).

    `booking_id` is the one field worth resolving to a real column *at
    ingestion*, not query time: 206/208/223 each carry it at a different
    slot, and getting that mapping wrong silently breaks every downstream
    join, so it is solved once here rather than three times in SQL.
    """

    __tablename__ = "leadsquared_activities"

    prospect_activity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    related_prospect_id: Mapped[str | None] = mapped_column(String(64))
    booking_id: Mapped[str | None] = mapped_column(String(64))

    @declared_attr
    def __table_args__(cls):
        return (
            UniqueConstraint(
                "connection_id", "stream", "record_key", name=f"uq_{cls.__tablename__}_conn_stream_key"
            ),
            Index(f"ix_{cls.__tablename__}_org_conn_date", "organization_id", "connector_id", "date"),
            Index(f"ix_{cls.__tablename__}_conn_stream_date", "connection_id", "stream", "date"),
            Index(f"ix_{cls.__tablename__}_sync_run", "sync_run_id"),
            Index(f"ix_{cls.__tablename__}_prospect_activity", "prospect_activity_id"),
            Index(f"ix_{cls.__tablename__}_related_prospect", "related_prospect_id"),
            Index(f"ix_{cls.__tablename__}_booking_id", "booking_id"),
        )


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
    "InstagramInsightsPerformance",
    "FacebookPagesPerformance",
]
