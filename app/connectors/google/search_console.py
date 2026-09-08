"""Google Search Console connector — official Search Analytics API v3 (§7).

Dimensions are configurable per stream (date / query / page / country / device /
searchAppearance and their combinations). Metrics are fixed by the API: clicks,
impressions, ctr, position.

GSC finalises data ~2 days late and keeps ~16 months, so the connector declares
`provider_lag_days = 2` and `max_history_days = 480`; the platform's lookback
window then re-fetches and upserts the still-settling tail (§12).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from urllib.parse import quote

from app.connectors import errors as E
from app.connectors.base import (
    EntityRecord,
    HealthReport,
    HealthStatus,
    Record,
    ResourceDescriptor,
    StreamDefinition,
    StreamSlice,
)
from app.connectors.google.base import GoogleConnector
from app.connectors.google.base import health_from_error as _health_from_error
from app.connectors.registry import RegistryEntry, registry
from app.connectors.validation import build_json_schema, clean_dimension, to_date, to_float, to_int
from app.oauth.google import SCOPE_SEARCH_CONSOLE_READONLY

API_BASE = "https://www.googleapis.com/webmasters/v3"
ROW_LIMIT = 25_000  # GSC hard maximum per request.
_METRICS = ["clicks", "impressions", "ctr", "position"]

# stream -> logical dimension list. `date` is the cursor and part of every
# stream's identity — but see `_APPEARANCE`: GSC will not group `searchAppearance`
# with any other dimension (a date *range* is fine, the `date` dimension is not),
# so that stream is sent to the API without `date` and fetched one day at a time.
_DIMENSIONS: dict[str, list[str]] = {
    "search_analytics_by_date": ["date"],
    "search_analytics_by_query": ["date", "query"],
    "search_analytics_by_page": ["date", "page"],
    "search_analytics_by_country": ["date", "country"],
    "search_analytics_by_device": ["date", "device"],
    "search_analytics_by_appearance": ["date", "searchAppearance"],
    "search_analytics_by_page_query": ["date", "page", "query"],
    # GSC hourly data — needs dataState=hourly_all and only ~10 days back.
    "search_analytics_by_hour": ["date", "hour"],
}

_APPEARANCE = "search_analytics_by_appearance"
_HOURLY = "search_analytics_by_hour"
_SITEMAPS = "sitemaps"
# search_type values GSC accepts on searchAnalytics.query.
_SEARCH_TYPES = ("web", "image", "video", "news", "discover", "googleNews")


def _api_dimensions(stream_name: str, dims: list[str]) -> list[str]:
    """What actually goes in the request `dimensions`. GSC refuses to group the
    `date` dimension alongside `searchAppearance` or `hour` (a date *range* is
    still fine), so those two streams are sent without it and the day is
    reconstructed per row (`hour` carries an ISO timestamp; appearance takes the
    1-day slice's date)."""
    if stream_name in (_APPEARANCE, _HOURLY):
        return [d for d in dims if d != "date"]
    return dims


def _slice_days(name: str, dims: list[str]) -> int:
    if name == _APPEARANCE:
        # 1-day slices: each request cannot carry the `date` dimension, so it
        # must map to exactly one day. Trivial against GSC's ~1200 req/min quota.
        return 1
    if name == _HOURLY:
        return 10  # hourly data is only served ~10 days back
    return 7 if len(dims) > 2 else 14


def _sa_stream(name: str, dims: list[str], *, suffix: str = "", search_type: str = "web") -> StreamDefinition:
    return StreamDefinition(
        name=f"{name}{suffix}",
        description=f"Search Analytics ({search_type}) grouped by {', '.join(dims)}",
        json_schema=build_json_schema(dims, _METRICS),
        primary_key=dims,
        slice_days=_slice_days(name, dims),
        grain="fact",
        spec={"dimensions": dims, "search_type": search_type, "base": name},
    )


def _sitemaps_stream() -> StreamDefinition:
    return StreamDefinition(
        name=_SITEMAPS,
        description="Submitted sitemaps with processing status and error/warning counts",
        json_schema=build_json_schema(
            ["path", "type", "lastSubmitted", "lastDownloaded"], ["error_count", "warning_count"]
        ),
        primary_key=["path"],
        grain="entity",
        date_partitioned=False,
        default_cursor_field=None,
        spec={},
    )


def _streams() -> list[StreamDefinition]:
    out = [_sa_stream(name, dims) for name, dims in _DIMENSIONS.items()]
    out.append(_sitemaps_stream())
    return out


class GoogleSearchConsoleConnector(GoogleConnector):
    connector_id = "google_search_console"
    name = "Google Search Console"
    version = "1.0.0"
    documentation_url = "https://developers.google.com/webmaster-tools/v1/searchanalytics/query"
    icon = "google-search-console"
    required_scopes = (SCOPE_SEARCH_CONSOLE_READONLY,)
    max_history_days = 480
    provider_lag_days = 2
    STREAMS = _streams()

    # --- CATALOG -------------------------------------------------------
    def get_streams(self) -> list[StreamDefinition]:
        """Core web streams plus, for each extra `search_types` entry in config
        (image / video / news / discover / googleNews), a suffixed copy of every
        search-analytics stream."""
        streams = list(self.declared_streams())
        cfg = self.ctx.config or {}
        extra = [t for t in (cfg.get("search_types") or []) if t in _SEARCH_TYPES and t != "web"]
        for search_type in extra:
            for name, dims in _DIMENSIONS.items():
                streams.append(_sa_stream(name, dims, suffix=f"_{search_type}", search_type=search_type))
        return streams

    # --- CHECK -----------------------------------------------------------
    async def check_connection(self) -> HealthReport:
        try:
            payload = await self.http.get(f"{API_BASE}/sites")
        except E.ConnectorError as exc:
            return _health_from_error(exc)

        site_url = self.ctx.resource_id
        if not site_url:
            return HealthReport(status=HealthStatus.HEALTHY, message="Connected to Search Console.")

        entry = next((s for s in payload.get("siteEntry", []) if s.get("siteUrl") == site_url), None)
        if entry is None:
            return HealthReport(
                status=HealthStatus.PERMISSION_DENIED,
                message=(
                    f"{site_url} is not in the connected account's Search Console. Add the account "
                    "as a user on the property, or reconnect with an owner account."
                ),
            )
        if entry.get("permissionLevel") == "siteUnverifiedUser":
            return HealthReport(
                status=HealthStatus.PERMISSION_DENIED,
                message=f"The connected account is an unverified user on {site_url}.",
            )
        return HealthReport(status=HealthStatus.HEALTHY, message=f"Connected to {site_url}.")

    # --- DISCOVER ------------------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        payload = await self.http.get(f"{API_BASE}/sites")
        out: list[ResourceDescriptor] = []
        for site in payload.get("siteEntry", []):
            url = site.get("siteUrl", "")
            level = site.get("permissionLevel", "")
            unverified = level == "siteUnverifiedUser"
            out.append(
                ResourceDescriptor(
                    resource_id=url,
                    name=url.replace("sc-domain:", "Domain: "),
                    resource_type="domain" if url.startswith("sc-domain:") else "url_prefix",
                    metadata={"permission_level": level},
                    selectable=not unverified,
                    unsupported_reason="Unverified user — no data access." if unverified else None,
                )
            )
        return out

    # --- READ --------------------------------------------------------
    async def read_slice(
        self, stream: StreamDefinition, slice_: StreamSlice
    ) -> AsyncIterator[Record | EntityRecord]:
        if stream.name == _SITEMAPS:
            async for entity in self._sitemap_entities():
                yield entity
            return

        base = stream.spec.get("base", stream.name)
        dims: list[str] = stream.spec["dimensions"]
        api_dims = _api_dimensions(base, dims)
        slice_date = slice_.start_date or date.today()
        start = slice_date.isoformat()
        end = (slice_.end_date or date.today()).isoformat()
        site = quote(self.ctx.resource_id, safe="")
        url = f"{API_BASE}/sites/{site}/searchAnalytics/query"
        # hourly data is a distinct data-state; everything else takes settling rows too.
        data_state = "hourly_all" if base == _HOURLY else self.ctx.config.get("data_state", "all")
        search_type = stream.spec.get("search_type") or self.ctx.config.get("search_type", "web")

        start_row = 0
        while True:
            body = {
                "startDate": start,
                "endDate": end,
                "dimensions": api_dims,
                "rowLimit": ROW_LIMIT,
                "startRow": start_row,
                "dataState": data_state,
                "type": search_type,
            }
            payload = await self.http.post(url, json=body)
            rows = payload.get("rows", [])
            for row in rows:
                yield self._to_record(stream, api_dims, row, slice_date)
            if len(rows) < ROW_LIMIT:
                break
            start_row += ROW_LIMIT

    async def _sitemap_entities(self) -> AsyncIterator[EntityRecord]:
        site = quote(self.ctx.resource_id, safe="")
        payload = await self.http.get(f"{API_BASE}/sites/{site}/sitemaps")
        for sm in payload.get("sitemap", []):
            contents = sm.get("contents") or []
            errors = sum(to_int(c.get("errors")) or 0 for c in contents)
            warnings = sum(to_int(c.get("warnings")) or 0 for c in contents)
            path = sm.get("path") or ""
            yield EntityRecord(
                stream=_SITEMAPS,
                level="sitemap",
                external_id=path,
                name=path,
                status="pending" if sm.get("isPending") else "submitted",
                start_date=to_date((sm.get("lastSubmitted") or "")[:10]),
                end_date=to_date((sm.get("lastDownloaded") or "")[:10]),
                raw={**sm, "error_count": errors, "warning_count": warnings},
            )

    def _to_record(
        self, stream: StreamDefinition, api_dims: list[str], row: dict, slice_date: date
    ) -> Record:
        keys = row.get("keys", [])
        dimensions = {name: clean_dimension(val) for name, val in zip(api_dims, keys, strict=False)}
        # `date` is a grouping dimension for most streams. Appearance can't carry
        # it (day = the 1-day slice); hourly can't either but its `hour` key is an
        # ISO timestamp, so the day comes from there.
        row_date = (
            to_date(dimensions.get("date"))
            or to_date((dimensions.get("hour") or "")[:10])
            or slice_date
        )
        dimensions.setdefault("date", row_date.isoformat())
        metrics = {
            "clicks": to_int(row.get("clicks")),
            "impressions": to_int(row.get("impressions")),
            "ctr": to_float(row.get("ctr")),
            "position": to_float(row.get("position")),
        }
        key_values = {
            k: (row_date.isoformat() if k == "date" else dimensions.get(k)) for k in stream.primary_key
        }
        return Record(
            stream=stream.name,
            key_values=key_values,
            date=row_date,
            dimensions=dimensions,
            metrics=metrics,
            measures={
                "clicks": metrics["clicks"],
                "impressions": metrics["impressions"],
                "average_position": metrics["position"],
            },
            raw=row,
        )


registry.register(
    RegistryEntry(
        connector_class=GoogleSearchConsoleConnector,
        requires_settings=("google_client_id", "google_client_secret"),
        prerequisites=(
            "The Google Search Console API must be enabled in your Google Cloud project.",
            "The connected account must be an owner or user on the Search Console property.",
        ),
        resource_label="Search Console Property",
        tags=("seo", "google", "search"),
    )
)
