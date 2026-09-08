"""Google Analytics 4 connector — official Analytics Data API v1beta (§6).

Streams are *report specs*, not classes: a dict of dimensions + metrics per
stream, so adding "landing page report" is a dict entry (§24). Every stream is
date-partitioned and incremental.

Large-result safety (§6, the known Airbyte GA4 failure mode): each request is
bounded three ways at once — a 7-day slice, a hard `limit` page size, and the
`propertyQuota` hook that slows the client down *before* GA4 throttles it. A
year of `pagePath` data can no longer arrive in one unbounded response.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from typing import Any

from app.connectors import errors as E
from app.connectors.base import (
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
from app.connectors.validation import build_json_schema, clean_dimension, to_date, to_number
from app.oauth.google import SCOPE_ANALYTICS_READONLY

ADMIN_BASE = "https://analyticsadmin.googleapis.com/v1beta"
DATA_BASE = "https://analyticsdata.googleapis.com/v1beta"
PAGE_LIMIT = 100_000  # GA4 allows 250k; 100k keeps a single response modest.

# GA4 metric name -> promoted cross-provider measure column.
_MEASURE_MAP = {
    "activeUsers": "users",
    "totalUsers": "users",
    "newUsers": "new_users",
    "sessions": "sessions",
    "screenPageViews": "page_views",
    "engagedSessions": "engaged_sessions",
    "conversions": "conversions",
    "keyEvents": "conversions",
    "totalRevenue": "revenue",
    "purchaseRevenue": "revenue",
    "eventCount": "event_count",
    "engagementRate": "engagement_rate",
    "bounceRate": "bounce_rate",
    "averageSessionDuration": "avg_session_duration",
    "screenPageViewsPerSession": "screen_page_views_per_session",
    "userEngagementDuration": "user_engagement_duration",
    "advertiserAdCost": "cost",
    "advertiserAdClicks": "clicks",
    "advertiserAdImpressions": "impressions",
}

# stream name -> (dimensions, metrics, primary_key)
_REPORTS: dict[str, tuple[list[str], list[str], list[str]]] = {
    "daily_overview": (
        ["date"],
        [
            # GA4 caps metrics at 10/request; `conversions` is GA4's alias of
            # `keyEvents` — requesting both is a "duplicate metrics" error.
            "activeUsers",
            "totalUsers",
            "newUsers",
            "sessions",
            "screenPageViews",
            "engagedSessions",
            "engagementRate",
            "averageSessionDuration",
            "conversions",
            "totalRevenue",
        ],
        ["date"],
    ),
    "traffic_acquisition": (
        ["date", "sessionDefaultChannelGroup", "sessionSource", "sessionMedium"],
        [
            "sessions",
            "engagedSessions",
            "activeUsers",
            "newUsers",
            "bounceRate",
            "averageSessionDuration",
            "conversions",
            "totalRevenue",
        ],
        ["date", "sessionDefaultChannelGroup", "sessionSource", "sessionMedium"],
    ),
    "page_performance": (
        ["date", "pagePath"],
        ["screenPageViews", "activeUsers", "engagedSessions", "userEngagementDuration"],
        ["date", "pagePath"],
    ),
    "events": (
        ["date", "eventName"],
        ["eventCount", "activeUsers", "totalRevenue"],
        ["date", "eventName"],
    ),
    "conversions": (
        ["date", "eventName"],
        ["conversions", "totalRevenue", "purchaseRevenue", "activeUsers"],
        ["date", "eventName"],
    ),
    "geography": (
        ["date", "country", "region", "city"],
        ["activeUsers", "newUsers", "sessions", "engagedSessions"],
        ["date", "country", "region", "city"],
    ),
    "device_platform": (
        ["date", "deviceCategory", "operatingSystem", "browser"],
        ["activeUsers", "sessions", "screenPageViews"],
        ["date", "deviceCategory", "operatingSystem", "browser"],
    ),
    "ecommerce": (
        # Item-scoped dimensions only allow item-scoped metrics — session/event
        # metrics like `transactions` are rejected as incompatible.
        ["date", "itemName", "itemId", "itemCategory"],
        ["itemsViewed", "itemsAddedToCart", "itemsPurchased", "itemRevenue"],
        ["date", "itemName", "itemId", "itemCategory"],
    ),
    # --- added for cross-cutting "overall analysis" ------------------------
    # Entry-page performance: which pages start sessions and whether they convert.
    "landing_pages": (
        ["date", "landingPage", "sessionDefaultChannelGroup"],
        [
            "sessions",
            "activeUsers",
            "newUsers",
            "engagedSessions",
            "userEngagementDuration",
            "conversions",
            "totalRevenue",
        ],
        ["date", "landingPage", "sessionDefaultChannelGroup"],
    ),
    # Every key event crossed with last-touch acquisition — conversion
    # attribution by channel/source/medium, the core marketing question.
    "key_events_by_channel": (
        ["date", "eventName", "sessionDefaultChannelGroup", "sessionSource", "sessionMedium"],
        ["eventCount", "conversions", "totalRevenue", "activeUsers"],
        ["date", "eventName", "sessionDefaultChannelGroup", "sessionSource", "sessionMedium"],
    ),
    # Last-touch campaign performance.
    "campaign_attribution": (
        ["date", "sessionCampaignName", "sessionSource", "sessionMedium", "sessionDefaultChannelGroup"],
        ["sessions", "activeUsers", "newUsers", "engagedSessions", "conversions", "totalRevenue"],
        ["date", "sessionCampaignName", "sessionSource", "sessionMedium", "sessionDefaultChannelGroup"],
    ),
    # First-touch (user acquisition) attribution — where users originally came from.
    "first_user_acquisition": (
        [
            "date",
            "firstUserSource",
            "firstUserMedium",
            "firstUserCampaignName",
            "firstUserDefaultChannelGroup",
        ],
        [
            "newUsers",
            "totalUsers",
            "sessions",
            "engagedSessions",
            "conversions",
            "totalRevenue",
            "transactions",
            "totalPurchasers",
            "firstTimePurchasers",
        ],
        [
            "date",
            "firstUserSource",
            "firstUserMedium",
            "firstUserCampaignName",
            "firstUserDefaultChannelGroup",
        ],
    ),
    # New vs returning behaviour split.
    "new_vs_returning": (
        ["date", "newVsReturning"],
        [
            "activeUsers",
            "sessions",
            "engagedSessions",
            "screenPageViews",
            "userEngagementDuration",
            "conversions",
            "totalRevenue",
            "transactions",
            "purchaseRevenue",
        ],
        ["date", "newVsReturning"],
    ),
    # Session-quality metrics (bounce, duration, depth) by channel and device.
    # These live in the row's `metrics` JSON — the warehouse has no promoted
    # column for them. ponytail: query metrics->>'bounceRate'; promote to a
    # column only if a dashboard needs to filter on it.
    "session_quality": (
        ["date", "sessionDefaultChannelGroup", "deviceCategory"],
        [
            "sessions",
            "engagedSessions",
            "engagementRate",
            "bounceRate",
            "averageSessionDuration",
            "screenPageViewsPerSession",
            "activeUsers",
        ],
        ["date", "sessionDefaultChannelGroup", "deviceCategory"],
    ),
    # Hour-of-day demand pattern. Cursor stays daily (date derived from
    # dateHour in _to_record); slices stay date-partitioned.
    "hourly_overview": (
        ["dateHour"],
        ["activeUsers", "sessions", "screenPageViews", "engagedSessions", "conversions", "totalRevenue"],
        ["dateHour"],
    ),
    # Age/gender split — needs Google Signals on the property; rows are empty
    # otherwise, which the connector drops harmlessly (keepEmptyRows=False).
    "demographics": (
        ["date", "userAgeBracket", "userGender"],
        ["activeUsers", "newUsers", "sessions", "engagedSessions", "conversions", "totalRevenue"],
        ["date", "userAgeBracket", "userGender"],
    ),
    # Affinity/in-market interests.
    "interests": (
        ["date", "brandingInterest"],
        ["activeUsers", "sessions", "engagedSessions", "conversions"],
        ["date", "brandingInterest"],
    ),
    # Device / OS / browser / resolution / app-platform detail.
    "tech_details": (
        ["date", "deviceCategory", "operatingSystem", "browser", "screenResolution", "language", "platform"],
        ["activeUsers", "newUsers", "sessions", "screenPageViews", "engagedSessions"],
        ["date", "deviceCategory", "operatingSystem", "browser", "screenResolution", "language", "platform"],
    ),
    # Page title + full path (complements page_performance which is path-only).
    "page_title": (
        ["date", "pageTitle", "pagePathPlusQueryString"],
        ["screenPageViews", "activeUsers", "engagedSessions", "userEngagementDuration"],
        ["date", "pageTitle", "pagePathPlusQueryString"],
    ),
    # GA4-observed Google Ads spend/return by campaign + ad group (needs the
    # GA4 <-> Google Ads account link).
    "google_ads_campaigns": (
        ["date", "sessionGoogleAdsCampaignName", "sessionGoogleAdsAdGroupName"],
        [
            "advertiserAdCost",
            "advertiserAdClicks",
            "advertiserAdImpressions",
            "sessions",
            "conversions",
            "totalRevenue",
            "returnOnAdSpend",
        ],
        ["date", "sessionGoogleAdsCampaignName", "sessionGoogleAdsAdGroupName"],
    ),
}


def _streams() -> list[StreamDefinition]:
    out: list[StreamDefinition] = []
    for name, (dims, metrics, pk) in _REPORTS.items():
        out.append(
            StreamDefinition(
                name=name,
                description=f"GA4 {name.replace('_', ' ')} report",
                json_schema=build_json_schema(dims, metrics),
                primary_key=pk,
                slice_days=7 if len(dims) > 2 else 30,
                grain="fact",
                spec={"dimensions": dims, "metrics": metrics},
            )
        )
    return out


class GoogleAnalyticsConnector(GoogleConnector):
    connector_id = "google_analytics"
    name = "Google Analytics 4"
    version = "1.0.0"
    documentation_url = "https://developers.google.com/analytics/devguides/reporting/data/v1"
    icon = "google-analytics"
    required_scopes = (SCOPE_ANALYTICS_READONLY,)
    STREAMS = _streams()

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._currency: str | None = ctx.resource_metadata.get("currency_code")
        self._quota_warned = False

    # --- CATALOG ---------------------------------------------------------
    def get_streams(self) -> list[StreamDefinition]:
        """Static reports plus a per-connection custom-report stream.

        `get_schema` surfaces the property's `customEvent:*` dimensions/metrics;
        this turns a chosen subset (config `ga4_custom_dimensions` /
        `ga4_custom_metrics`, GA4 api names) into an actually-requested stream.
        """
        streams = list(self.declared_streams())
        cfg = self.ctx.config or {}
        cdims = [str(d) for d in (cfg.get("ga4_custom_dimensions") or []) if d]
        cmets = [str(m) for m in (cfg.get("ga4_custom_metrics") or []) if m]
        if cdims or cmets:
            dims = ["date", *cdims]
            metrics = cmets or ["eventCount"]
            streams.append(
                StreamDefinition(
                    name="custom_report",
                    description="GA4 custom dimensions/metrics (per-connection config)",
                    json_schema=build_json_schema(dims, metrics),
                    primary_key=dims,
                    slice_days=7,
                    grain="fact",
                    spec={"dimensions": dims, "metrics": metrics},
                )
            )
        return streams

    # --- CHECK -------------------------------------------------------------
    async def check_connection(self) -> HealthReport:
        from datetime import UTC, datetime, timedelta

        try:
            summaries = await self.http.get(f"{ADMIN_BASE}/accountSummaries", params={"pageSize": 200})
        except E.ConnectorError as exc:
            return _health_from_error(exc)

        prop_id = self.ctx.resource_id
        if prop_id:
            found = any(
                ps.get("property") == f"properties/{prop_id}"
                for acc in summaries.get("accountSummaries", [])
                for ps in acc.get("propertySummaries", [])
            )
            if not found:
                return HealthReport(
                    status=HealthStatus.PERMISSION_DENIED,
                    message=(
                        f"Property {prop_id} is not visible to the connected Google account. "
                        "Grant it Viewer access in GA4 Admin, or reconnect with an account that has it."
                    ),
                )
            # Prove the Data API itself is enabled and reachable for this property.
            yesterday = (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
            try:
                await self.http.post(
                    f"{DATA_BASE}/properties/{prop_id}:runReport",
                    json={
                        "metrics": [{"name": "activeUsers"}],
                        "dateRanges": [{"startDate": yesterday, "endDate": yesterday}],
                        "limit": 1,
                    },
                )
            except E.ConnectorError as exc:
                return _health_from_error(exc)

        return HealthReport(status=HealthStatus.HEALTHY, message="Connected to Google Analytics 4.")

    # --- DISCOVER --------------------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        out: list[ResourceDescriptor] = []
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {"pageSize": 200}
            if page_token:
                params["pageToken"] = page_token
            payload = await self.http.get(f"{ADMIN_BASE}/accountSummaries", params=params)
            for acc in payload.get("accountSummaries", []):
                account = acc.get("account", "")
                for ps in acc.get("propertySummaries", []):
                    prop = ps.get("property", "")
                    prop_id = prop.split("/", 1)[-1]
                    out.append(
                        ResourceDescriptor(
                            resource_id=prop_id,
                            name=ps.get("displayName") or prop_id,
                            resource_type="property",
                            parent_id=account.split("/", 1)[-1] or None,
                            metadata={
                                "property_resource": prop,
                                "account": account,
                                "account_name": acc.get("displayName"),
                                "property_type": ps.get("propertyType"),
                            },
                        )
                    )
            page_token = payload.get("nextPageToken")
            if not page_token:
                break
        return out

    # --- SCHEMA (custom dimensions/metrics, §6) --------------------------
    async def get_schema(self, stream_name: str) -> dict[str, Any]:
        base = self.get_stream(stream_name).json_schema
        prop_id = self.ctx.resource_id
        if not prop_id:
            return base
        try:
            meta = await self.http.get(f"{DATA_BASE}/properties/{prop_id}/metadata")
        except E.ConnectorError:
            return base
        props = dict(base.get("properties", {}))
        for dim in meta.get("dimensions", []):
            api_name = dim.get("apiName")
            if api_name and api_name.startswith("customEvent:"):
                props[api_name] = {"type": ["string", "null"], "description": dim.get("uiName")}
        for met in meta.get("metrics", []):
            api_name = met.get("apiName")
            if api_name and api_name.startswith("customEvent:"):
                props[api_name] = {"type": ["number", "null"], "description": met.get("uiName")}
        return {**base, "properties": props}

    # --- READ -----------------------------------------------------------
    async def read_slice(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[Record]:
        dims: list[str] = stream.spec["dimensions"]
        metrics: list[str] = stream.spec["metrics"]
        start = (slice_.start_date or date.today()).isoformat()
        end = (slice_.end_date or date.today()).isoformat()
        currency = await self._get_currency()

        offset = 0
        while True:
            body = {
                "dimensions": [{"name": d} for d in dims],
                "metrics": [{"name": m} for m in metrics],
                "dateRanges": [{"startDate": start, "endDate": end}],
                "limit": PAGE_LIMIT,
                "offset": offset,
                "returnPropertyQuota": True,
                "keepEmptyRows": False,
            }
            payload = await self.http.post(
                f"{DATA_BASE}/properties/{self.ctx.resource_id}:runReport", json=body
            )
            self._absorb_quota(payload.get("propertyQuota"))

            dim_headers = [h["name"] for h in payload.get("dimensionHeaders", [])]
            met_headers = [h["name"] for h in payload.get("metricHeaders", [])]
            rows = payload.get("rows", [])
            for row in rows:
                yield self._to_record(stream, dim_headers, met_headers, row, currency)

            row_count = int(payload.get("rowCount", 0))
            offset += len(rows)
            if not rows or offset >= row_count:
                break

    def _to_record(
        self,
        stream: StreamDefinition,
        dim_headers: list[str],
        met_headers: list[str],
        row: dict[str, Any],
        currency: str | None,
    ) -> Record:
        dvals = [d.get("value") for d in row.get("dimensionValues", [])]
        mvals = [m.get("value") for m in row.get("metricValues", [])]
        dimensions = {name: clean_dimension(val) for name, val in zip(dim_headers, dvals, strict=False)}
        metrics = {name: to_number(val) for name, val in zip(met_headers, mvals, strict=False)}

        # `date` for daily streams; derive it from `dateHour` (YYYYMMDDHH) for the
        # hourly stream so the cursor and slicing stay day-grained.
        raw_date = dimensions.get("date") or (dimensions.get("dateHour") or "")[:8]
        row_date = to_date(raw_date)
        key_values = {
            k: (row_date.isoformat() if k == "date" else dimensions.get(k)) for k in stream.primary_key
        }

        measures: dict[str, Any] = {}
        for gm, col in _MEASURE_MAP.items():
            if gm in metrics and metrics[gm] is not None:
                measures[col] = metrics[gm]

        return Record(
            stream=stream.name,
            key_values=key_values,
            date=row_date,
            dimensions=dimensions,
            metrics=metrics,
            measures=measures,
            currency=currency,
            raw=row,
        )

    # --- helpers ------------------------------------------------------------
    async def _get_currency(self) -> str | None:
        if self._currency:
            return self._currency
        prop_id = self.ctx.resource_id
        if not prop_id:
            return None
        try:
            detail = await self.http.get(f"{ADMIN_BASE}/properties/{prop_id}")
            self._currency = detail.get("currencyCode")
        except E.ConnectorError:
            self._currency = None
        return self._currency

    def _absorb_quota(self, quota: dict[str, Any] | None) -> None:
        if not quota:
            return
        tph = quota.get("tokensPerHour") or {}
        remaining, consumed = tph.get("remaining"), tph.get("consumed")
        if remaining is None:
            return
        total = (remaining or 0) + (consumed or 0)
        if total and remaining / total < 0.15:
            # Under 15% of the hourly token budget left — quarter the rate for a
            # few minutes so the run finishes instead of hitting RESOURCE_EXHAUSTED.
            self.http.limiter.penalize(factor=4.0, duration=300.0)
            if not self._quota_warned:
                self.ctx.progress.note("GA4 quota low — slowing requests")
                self._quota_warned = True


registry.register(
    RegistryEntry(
        connector_class=GoogleAnalyticsConnector,
        requires_settings=("google_client_id", "google_client_secret"),
        prerequisites=(
            "A Google Cloud project with the Google Analytics Data API and Admin API enabled.",
            "The connected Google account must have at least Viewer access to the GA4 property.",
        ),
        resource_label="GA4 Property",
        tags=("analytics", "google", "web"),
    )
)
