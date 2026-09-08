"""The connector abstraction.

Deliberately mirrors the shape of the Airbyte protocol's source interface —
spec / check / discover / read — because that decomposition is what makes a
connector framework extensible: each verb has one job, and the platform can
drive any connector without knowing which provider is behind it.

What a connector *is* responsible for:
    declaring its streams and config schema   (spec)
    proving its credentials work              (check)
    listing what the identity can reach       (discover)
    turning a date window into records        (read_slice)

What a connector is deliberately *not* responsible for: the database, the
scheduler, retry bookkeeping, state persistence, progress reporting, or token
refresh. Those live in the platform, which is why adding a sixth provider is one
module plus a registry line (§24).

A connector never touches the ORM. Credentials arrive through a `TokenProvider`
so the whole surface is testable against mocked HTTP with no database at all.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any, ClassVar, Protocol, runtime_checkable

from app.connectors.errors import ConnectorError
from app.connectors.http import HttpClient


class SyncMode(StrEnum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class DestinationSyncMode(StrEnum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    # The mode analytics actually needs: providers restate recent days, so an
    # overlapping re-fetch must correct rows on the primary key.
    APPEND_DEDUP = "append_dedup"


class HealthStatus(StrEnum):
    """The §15 vocabulary, surfaced verbatim to the UI."""

    HEALTHY = "healthy"
    NEEDS_REAUTH = "needs_reauth"
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMITED = "rate_limited"
    INVALID_CONFIGURATION = "invalid_configuration"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    NOT_SUPPORTED = "not_supported"
    UNKNOWN = "unknown"


class AuthType(StrEnum):
    OAUTH2 = "oauth2"
    OAUTH2_WITH_DEVELOPER_TOKEN = "oauth2_with_developer_token"


# ---------------------------------------------------------------------------
# Credentials seam
# ---------------------------------------------------------------------------


@runtime_checkable
class TokenProvider(Protocol):
    """Supplies a valid access token, refreshing transparently.

    The connector calls `access_token()` per request batch and never sees a
    refresh token. `invalidate()` is called when the provider rejects a token
    that we believed was live, forcing one refresh before the retry.
    """

    async def access_token(self) -> str: ...

    async def invalidate(self) -> None: ...

    @property
    def scopes(self) -> Sequence[str]: ...

    @property
    def account_label(self) -> str | None: ...


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class StreamDefinition:
    """One report/table a connector can produce — the catalog entry.

    Note what is *absent*: whether the tenant wants it, and in which sync mode.
    That is the configured catalog, and it lives on the Connection row. Keeping
    the two apart is why one connector serves every tenant.
    """

    name: str
    description: str
    json_schema: dict[str, Any]
    primary_key: list[str]
    supported_sync_modes: list[SyncMode] = field(
        default_factory=lambda: [SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL]
    )
    # True when the connector owns the cursor and the user cannot override it —
    # for date-partitioned report APIs that is always the case.
    source_defined_cursor: bool = True
    default_cursor_field: str | None = "date"
    default_destination_sync_mode: DestinationSyncMode = DestinationSyncMode.APPEND_DEDUP
    # "fact" rows land in report_rows; "entity" rows land in ad_entities.
    grain: str = "fact"
    # Days per request window. The single most important knob in the framework:
    # it bounds response size (so a large property cannot produce a multi-hundred-MB
    # reply that hangs the run) and makes a backfill resumable at slice
    # granularity instead of restarting from scratch.
    slice_days: int = 7
    # Provider-native request shape (dimensions, metrics, GAQL, breakdowns).
    # Opaque to the platform, which is what lets a new report be a dict entry
    # rather than a class.
    spec: dict[str, Any] = field(default_factory=dict)
    # Streams that are not date-partitioned (entity lists) sync whole each time.
    date_partitioned: bool = True
    # Documented reason this stream may be unavailable for some accounts.
    requires: list[str] = field(default_factory=list)

    @property
    def supports_incremental(self) -> bool:
        return SyncMode.INCREMENTAL in self.supported_sync_modes


@dataclass(slots=True)
class StreamSlice:
    """One bounded unit of work: a date window, plus any provider partition key."""

    start_date: date | None = None
    end_date: date | None = None
    partition: dict[str, Any] = field(default_factory=dict)

    def label(self) -> str:
        if self.start_date and self.end_date:
            if self.start_date == self.end_date:
                return str(self.start_date)
            return f"{self.start_date}..{self.end_date}"
        return "full"


@dataclass(slots=True)
class Record:
    """A fact row destined for `report_rows`."""

    stream: str
    key_values: dict[str, Any]
    date: date | None = None
    dimensions: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    # Subset of metrics mapped onto the typed cross-provider columns.
    measures: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] | None = None
    cursor_value: str | None = None
    currency: str | None = None


@dataclass(slots=True)
class EntityRecord:
    """A slowly-changing entity destined for `ad_entities`."""

    stream: str
    level: str
    external_id: str
    name: str | None = None
    status: str | None = None
    parent_external_id: str | None = None
    channel: str | None = None
    objective: str | None = None
    daily_budget: float | None = None
    lifetime_budget: float | None = None
    currency: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    raw: dict[str, Any] | None = None
    cursor_value: str | None = None


AnyRecord = Record | EntityRecord


# ---------------------------------------------------------------------------
# Discovery & health
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ResourceDescriptor:
    """Something the authorised identity can sync: a property, site, or account."""

    resource_id: str
    name: str
    resource_type: str
    parent_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    # Discovery surfaces unusable resources rather than hiding them, so the UI can
    # explain *why* (a manager account holds no metrics; a personal Instagram
    # account has no insights endpoint) instead of showing an empty list.
    selectable: bool = True
    unsupported_reason: str | None = None


@dataclass(slots=True)
class HealthReport:
    status: HealthStatus
    message: str
    checked_at: datetime | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error: ConnectorError | None = None

    @property
    def ok(self) -> bool:
        return self.status is HealthStatus.HEALTHY


# ---------------------------------------------------------------------------
# Context & progress
# ---------------------------------------------------------------------------


class ProgressSink(Protocol):
    """How a connector reports what it is doing (§17)."""

    def phase(self, phase: str, detail: str | None = None) -> None: ...

    def note(self, detail: str) -> None: ...

    def slice_progress(self, stream: str, completed: int, total: int) -> None: ...


class NullProgress:
    def phase(self, phase: str, detail: str | None = None) -> None: ...

    def note(self, detail: str) -> None: ...

    def slice_progress(self, stream: str, completed: int, total: int) -> None: ...


@dataclass
class ConnectorContext:
    """Everything a connector needs, and nothing it does not.

    No session, no ORM objects, no request. `resource` is None during discovery,
    which is the one operation that runs before a connection exists.
    """

    token_provider: TokenProvider
    config: dict[str, Any] = field(default_factory=dict)
    resource_id: str | None = None
    resource_metadata: dict[str, Any] = field(default_factory=dict)
    progress: ProgressSink = field(default_factory=NullProgress)
    http: HttpClient | None = None
    # Env-level provider settings (API versions, developer token). Passed in so
    # connectors stay free of global config imports and are trivially fakeable.
    provider_settings: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# The connector
# ---------------------------------------------------------------------------


class BaseConnector(abc.ABC):
    """Base class for every source.

    Subclasses declare identity via class attributes and implement four verbs.
    Anything shared between providers of the same family (Google, Meta) belongs
    in an intermediate base, not duplicated here.
    """

    connector_id: str = ""
    name: str = ""
    provider: str = ""
    version: str = "1.0.0"
    category: str = "marketing"
    auth_type: AuthType = AuthType.OAUTH2
    documentation_url: str = ""
    icon: str = ""
    # Scopes this connector needs on top of the provider's base scopes.
    required_scopes: tuple[str, ...] = ()
    # JSON Schema for the per-connection `config` blob.
    config_schema: ClassVar[dict[str, Any]] = {}
    # Earliest date the provider will serve, if it enforces a retention window.
    max_history_days: int | None = None
    # Days the provider's data lags "today" (GSC finalises ~2 days late). The sync
    # window ends this many days before today so a run does not keep re-fetching
    # an incomplete tail forever.
    provider_lag_days: int = 0

    def __init__(self, ctx: ConnectorContext):
        self.ctx = ctx
        self._http: HttpClient | None = ctx.http

    # --- lifecycle ---------------------------------------------------------
    @property
    def http(self) -> HttpClient:
        if self._http is None:
            self._http = self._build_http_client()
        return self._http

    @abc.abstractmethod
    def _build_http_client(self) -> HttpClient:
        """Construct the provider's HTTP client, with its rate limits and classifier."""

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    # --- CHECK -------------------------------------------------------------
    @abc.abstractmethod
    async def check_connection(self) -> HealthReport:
        """Cheapest call that proves the credentials and resource are usable."""

    # --- DISCOVER ----------------------------------------------------------
    @abc.abstractmethod
    async def discover_resources(self) -> list[ResourceDescriptor]:
        """List syncable resources for the authorised identity."""

    # --- CATALOG -----------------------------------------------------------
    # Streams declared statically on the class. `get_streams()` may filter or
    # extend these using per-connection config; the registry and UI read the
    # static list, which needs no credentials.
    STREAMS: ClassVar[list[StreamDefinition]] = []

    @classmethod
    def declared_streams(cls) -> list[StreamDefinition]:
        return list(cls.STREAMS)

    def get_streams(self) -> list[StreamDefinition]:
        """Declare available streams. Override to consult self.ctx.config."""
        return self.declared_streams()

    def get_stream(self, name: str) -> StreamDefinition:
        for stream in self.get_streams():
            if stream.name == name:
                return stream
        from app.connectors import errors as E

        raise E.invalid_configuration(
            f"Stream {name!r} is not provided by {self.name}.",
            provider=self.provider,
            connector_id=self.connector_id,
        )

    async def get_schema(self, stream_name: str) -> dict[str, Any]:
        """Resolve a stream's JSON schema.

        Default is the static declaration. Connectors that can interrogate the
        provider for custom fields (GA4's metadata endpoint) override this so
        custom dimensions and metrics appear without a code change (§6).
        """
        return self.get_stream(stream_name).json_schema

    # --- READ --------------------------------------------------------------
    @abc.abstractmethod
    def read_slice(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[AnyRecord]:
        """Yield records for one slice.

        An async generator, not a list: records stream to the destination in
        batches so memory stays flat regardless of how much a provider returns.
        """

    def slices(
        self,
        stream: StreamDefinition,
        start_date: date | None,
        end_date: date | None,
    ) -> Iterable[StreamSlice]:
        """Split a date range into request windows. Overridable per connector."""
        from app.connectors.slicing import date_slices

        if not stream.date_partitioned or start_date is None or end_date is None:
            return [StreamSlice()]
        return date_slices(start_date, end_date, stream.slice_days)

    # --- metadata for the registry ----------------------------------------
    @classmethod
    def describe(cls) -> dict[str, Any]:
        return {
            "connector_id": cls.connector_id,
            "name": cls.name,
            "provider": cls.provider,
            "version": cls.version,
            "category": cls.category,
            "auth_type": str(cls.auth_type),
            "documentation_url": cls.documentation_url,
            "icon": cls.icon,
            "required_scopes": list(cls.required_scopes),
            "max_history_days": cls.max_history_days,
        }
