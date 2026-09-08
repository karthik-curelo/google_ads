"""Control-plane tables: tenancy, OAuth identities, connections, runs, state.

Layout mirrors the Airbyte separation of concerns without its process model:

  oauth_identity   one authorised provider account (a Google login, a Meta user)
  connector_resource  something discoverable under that identity (GA4 property,
                      GSC site, Ads customer, Meta ad account)
  connection       identity x resource x connector x schedule x configured streams
  sync_run         one execution of a connection
  sync_state       per-stream cursor, the durable "where did we get to"
  sync_error       structured, user-actionable failure records

The key borrowed idea is the catalog / *configured* catalog split: the connector
declares which streams exist and how they can sync, while `connection.streams`
records the tenant's choice of what to sync and how. Nothing about a tenant's
selection lives in connector code.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, JSONType, PKType, TimestampType, utcnow

# --- status vocabularies ---------------------------------------------------
# Plain strings rather than DB enums: adding a status must not require a
# migration on Postgres, and SQLite has no native enum anyway.

IDENTITY_ACTIVE = "active"
IDENTITY_NEEDS_REAUTH = "needs_reauth"
IDENTITY_REVOKED = "revoked"

CONN_PENDING = "pending"
CONN_HEALTHY = "healthy"
CONN_SYNCING = "syncing"
CONN_NEEDS_REAUTH = "needs_reauth"
CONN_PERMISSION_DENIED = "permission_denied"
CONN_RATE_LIMITED = "rate_limited"
CONN_INVALID_CONFIG = "invalid_configuration"
CONN_PROVIDER_UNAVAILABLE = "provider_unavailable"
CONN_ERROR = "error"
CONN_PAUSED = "paused"

RUN_PENDING = "pending"
RUN_RUNNING = "running"
RUN_SUCCEEDED = "succeeded"
RUN_PARTIAL = "partial_success"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"
RUN_TERMINAL = {RUN_SUCCEEDED, RUN_PARTIAL, RUN_FAILED, RUN_CANCELLED}


class Organization(Base):
    """Tenant boundary. Every other row in the system hangs off one of these."""

    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)


class ApiToken(Base):
    """Bearer token → organization. Stored as a SHA-256 hash, never plaintext."""

    __tablename__ = "api_tokens"

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    label: Mapped[str] = mapped_column(String(120), default="default", nullable=False)
    created_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(TimestampType)


class OAuthState(Base):
    """Short-lived CSRF state for an in-flight authorization-code exchange.

    Persisted rather than held in memory so the callback works across workers and
    restarts. Rows are single-use (`consumed_at`) and expire.
    """

    __tablename__ = "oauth_states"

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    state: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    connector_id: Mapped[str | None] = mapped_column(String(80))
    scopes: Mapped[list[str] | None] = mapped_column(JSONType)
    redirect_after: Mapped[str | None] = mapped_column(String(500))
    # Reconnect flow: bind the exchange to an existing identity so a user cannot
    # be walked into attaching someone else's account.
    identity_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TimestampType, nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(TimestampType)


class OAuthIdentity(Base):
    """One authorised provider account, shared by every connector for that provider.

    This is the §22 requirement made physical: connecting Google once yields an
    identity that GA4, Search Console and Google Ads all draw tokens from, with
    scopes accumulated incrementally rather than three separate logins.
    """

    __tablename__ = "oauth_identities"
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "provider",
            "external_account_id",
            name="uq_oauth_identities_org_provider_account",
        ),
    )

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    external_account_id: Mapped[str] = mapped_column(String(200), nullable=False)
    email: Mapped[str | None] = mapped_column(String(320))
    display_name: Mapped[str | None] = mapped_column(String(200))

    access_token_encrypted: Mapped[str | None] = mapped_column(Text)
    refresh_token_encrypted: Mapped[str | None] = mapped_column(Text)
    access_token_expires_at: Mapped[datetime | None] = mapped_column(TimestampType)
    scopes: Mapped[list[str]] = mapped_column(JSONType, default=list, nullable=False)

    status: Mapped[str] = mapped_column(String(40), default=IDENTITY_ACTIVE, nullable=False)
    status_detail: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TimestampType, default=utcnow, onupdate=utcnow, nullable=False
    )
    revoked_at: Mapped[datetime | None] = mapped_column(TimestampType)

    connections: Mapped[list[Connection]] = relationship(
        back_populates="identity", cascade="all, delete-orphan"
    )


class ConnectorResource(Base):
    """A discovered, selectable resource under an identity (cached for the UI)."""

    __tablename__ = "connector_resources"
    __table_args__ = (
        UniqueConstraint(
            "oauth_identity_id",
            "connector_id",
            "resource_id",
            name="uq_connector_resources_identity_connector_resource",
        ),
        Index("ix_connector_resources_org_connector", "organization_id", "connector_id"),
    )

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    oauth_identity_id: Mapped[int] = mapped_column(
        ForeignKey("oauth_identities.id", ondelete="CASCADE"), nullable=False
    )
    connector_id: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(200), nullable=False)
    name: Mapped[str | None] = mapped_column(String(300))
    resource_type: Mapped[str | None] = mapped_column(String(60))
    parent_id: Mapped[str | None] = mapped_column(String(200))
    # Provider-native extras (currency, timezone, manager flag, account status…)
    # kept verbatim so nothing is lost to normalisation.
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONType)
    selectable: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    unsupported_reason: Mapped[str | None] = mapped_column(Text)
    discovered_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)


class Connection(Base):
    """A configured, schedulable pipeline: one connector against one resource."""

    __tablename__ = "connections"
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "connector_id",
            "resource_id",
            name="uq_connections_org_connector_resource",
        ),
        Index("ix_connections_due", "enabled", "next_run_at"),
    )

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    oauth_identity_id: Mapped[int] = mapped_column(
        ForeignKey("oauth_identities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    connector_id: Mapped[str] = mapped_column(String(80), nullable=False)
    name: Mapped[str] = mapped_column(String(300), nullable=False)

    resource_id: Mapped[str] = mapped_column(String(200), nullable=False)
    resource_name: Mapped[str | None] = mapped_column(String(300))
    resource_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSONType)

    # Connector-specific settings (validated against the connector's spec).
    config: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    # The configured catalog: [{stream, sync_mode, enabled}, ...]
    streams: Mapped[list[dict[str, Any]]] = mapped_column(JSONType, default=list, nullable=False)

    backfill_start_date: Mapped[date | None] = mapped_column(Date)
    lookback_days: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    schedule_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    status: Mapped[str] = mapped_column(String(40), default=CONN_PENDING, nullable=False)
    status_detail: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(String(60))
    last_error_message: Mapped[str | None] = mapped_column(Text)

    last_run_at: Mapped[datetime | None] = mapped_column(TimestampType)
    last_success_at: Mapped[datetime | None] = mapped_column(TimestampType)
    next_run_at: Mapped[datetime | None] = mapped_column(TimestampType)
    total_records_synced: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Cooperative lock. A scheduler claims a connection by CAS-ing these in a
    # single UPDATE, which is what prevents concurrent runs of one connection
    # without needing a broker.
    locked_at: Mapped[datetime | None] = mapped_column(TimestampType)
    locked_by: Mapped[str | None] = mapped_column(String(120))

    created_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TimestampType, default=utcnow, onupdate=utcnow, nullable=False
    )

    identity: Mapped[OAuthIdentity] = relationship(back_populates="connections")
    runs: Mapped[list[SyncRun]] = relationship(back_populates="connection", cascade="all, delete-orphan")


class SyncRun(Base):
    """One execution. Carries the phase/progress detail the UI streams."""

    __tablename__ = "sync_runs"
    __table_args__ = (Index("ix_sync_runs_connection_started", "connection_id", "started_at"),)

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True
    )
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    trigger: Mapped[str] = mapped_column(String(20), default="manual", nullable=False)
    sync_mode: Mapped[str] = mapped_column(String(20), default="incremental", nullable=False)
    status: Mapped[str] = mapped_column(String(20), default=RUN_PENDING, nullable=False)
    phase: Mapped[str | None] = mapped_column(String(40))
    phase_detail: Mapped[str | None] = mapped_column(Text)
    attempt: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    started_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(TimestampType)
    duration_ms: Mapped[int | None] = mapped_column(Integer)

    records_fetched: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_inserted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_skipped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    api_calls: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    slices_completed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    slices_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    warnings: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    error_code: Mapped[str | None] = mapped_column(String(60))
    error_message: Mapped[str | None] = mapped_column(Text)
    will_retry: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    state_before: Mapped[dict[str, Any] | None] = mapped_column(JSONType)
    state_after: Mapped[dict[str, Any] | None] = mapped_column(JSONType)

    connection: Mapped[Connection] = relationship(back_populates="runs")
    stream_stats: Mapped[list[SyncStreamStat]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class SyncStreamStat(Base):
    """Per-stream breakdown of a run — which report worked, which did not."""

    __tablename__ = "sync_stream_stats"
    __table_args__ = (UniqueConstraint("sync_run_id", "stream", name="uq_sync_stream_stats_run_stream"),)

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    sync_run_id: Mapped[int] = mapped_column(
        ForeignKey("sync_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    stream: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default=RUN_PENDING, nullable=False)
    sync_mode: Mapped[str] = mapped_column(String(20), default="incremental", nullable=False)
    records_fetched: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_inserted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_skipped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    api_calls: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    slices_completed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    slices_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cursor_value: Mapped[str | None] = mapped_column(String(120))
    error_code: Mapped[str | None] = mapped_column(String(60))
    error_message: Mapped[str | None] = mapped_column(Text)

    run: Mapped[SyncRun] = relationship(back_populates="stream_stats")


class SyncState(Base):
    """Durable per-stream cursor.

    Treated as an opaque payload by everything except the connector that wrote
    it, and only ever advanced after the destination has committed the records it
    covers — the Airbyte rule that makes a resumed sync correct rather than
    merely convenient.
    """

    __tablename__ = "sync_state"
    __table_args__ = (UniqueConstraint("connection_id", "stream", name="uq_sync_state_connection_stream"),)

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True
    )
    stream: Mapped[str] = mapped_column(String(120), nullable=False)
    cursor_field: Mapped[str | None] = mapped_column(String(120))
    cursor_value: Mapped[str | None] = mapped_column(String(120))
    state: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    records_synced: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TimestampType, default=utcnow, onupdate=utcnow, nullable=False
    )


class SyncError(Base):
    """Structured failure record — the data behind "what broke and how to fix it"."""

    __tablename__ = "sync_errors"

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    sync_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("sync_runs.id", ondelete="CASCADE"), index=True
    )
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True
    )
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    stream: Mapped[str | None] = mapped_column(String(120))
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str | None] = mapped_column(String(40))
    connector_id: Mapped[str | None] = mapped_column(String(80))
    retryable: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    recoverable: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    user_action: Mapped[str | None] = mapped_column(Text)
    http_status: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONType)
    occurred_at: Mapped[datetime] = mapped_column(TimestampType, default=utcnow, nullable=False)


class SourceSchema(Base):
    """Snapshot of a stream's discovered field set, for drift detection.

    Airbyte's rule is that schema drift must never abort a sync. We keep the last
    seen shape so drift is *reported* (new metric appeared, dimension vanished)
    while ingestion carries on.
    """

    __tablename__ = "source_schemas"
    __table_args__ = (
        UniqueConstraint("connection_id", "stream", name="uq_source_schemas_connection_stream"),
    )

    id: Mapped[int] = mapped_column(PKType, primary_key=True, autoincrement=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("connections.id", ondelete="CASCADE"), nullable=False, index=True
    )
    stream: Mapped[str] = mapped_column(String(120), nullable=False)
    json_schema: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    last_drift: Mapped[dict[str, Any] | None] = mapped_column(JSONType)
    updated_at: Mapped[datetime] = mapped_column(
        TimestampType, default=utcnow, onupdate=utcnow, nullable=False
    )


# Re-exported for the warehouse module's foreign keys.
__all__ = [
    "ApiToken",
    "Connection",
    "ConnectorResource",
    "Numeric",
    "OAuthIdentity",
    "OAuthState",
    "Organization",
    "SourceSchema",
    "SyncError",
    "SyncRun",
    "SyncState",
    "SyncStreamStat",
]
