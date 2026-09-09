"""Instagram Insights connector — official Instagram Graph API (§10).

Scope is deliberately conservative because the Instagram insights surface is the
most version- and eligibility-dependent of all five connectors:

  * Only Instagram **Business/Creator** accounts linked to a Facebook Page have
    an insights endpoint. A personal account has none — discovery surfaces that
    Page as non-selectable with the reason, and CHECK returns NOT_SUPPORTED
    rather than failing obscurely.
  * Meta removes account-level metrics between Graph versions (`impressions` went
    away for Instagram in v22). The connector requests a metric set and, when
    Meta rejects one (error code 100), drops it and retries — so a version bump
    degrades gracefully instead of breaking the stream.
  * Demographic / audience metrics need `metric_type=total_value`, 100+
    followers, and change shape per version; they are intentionally out of scope
    for v1 and documented as a known limitation.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

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
from app.connectors.meta.base import MetaConnector, health_from_error
from app.connectors.registry import RegistryEntry, registry
from app.connectors.validation import build_json_schema, to_date, to_int
from app.oauth.meta import (
    SCOPE_INSTAGRAM_BASIC,
    SCOPE_INSTAGRAM_MANAGE_INSIGHTS,
    SCOPE_PAGES_READ_ENGAGEMENT,
    SCOPE_PAGES_SHOW_LIST,
)

# Requested account-level daily metrics. Anything Meta rejects for the running
# Graph version is dropped at runtime (see _account_insights).
_ACCOUNT_METRICS = [
    "reach",
    "follower_count",
    "profile_views",
    "website_clicks",
    "accounts_engaged",
    "total_interactions",
    "views",
    "profile_links_taps",
]
_MEDIA_METRICS = ["reach", "saved", "likes", "comments", "shares", "total_interactions", "views"]
# Reel-specific metrics (media_product_type == "REELS").
_REEL_METRICS = [
    "reach",
    "likes",
    "comments",
    "shares",
    "saved",
    "total_interactions",
    "plays",
    "ig_reels_avg_watch_time",
    "ig_reels_video_view_total_time",
    "clips_replays_count",
]
_STORY_METRICS = ["reach", "replies", "navigation", "total_interactions"]
_DEMOGRAPHIC_BREAKDOWNS = ["age", "gender", "city", "country"]


class InstagramInsightsConnector(MetaConnector):
    connector_id = "instagram_insights"
    name = "Instagram Insights"
    version = "1.0.0"
    documentation_url = "https://developers.facebook.com/docs/instagram-platform/insights"
    icon = "instagram"
    required_scopes = (
        SCOPE_INSTAGRAM_BASIC,
        SCOPE_INSTAGRAM_MANAGE_INSIGHTS,
        SCOPE_PAGES_SHOW_LIST,
        SCOPE_PAGES_READ_ENGAGEMENT,
    )
    # Instagram account-level insights serve at most the last ~30 days per call.
    max_history_days = 30
    provider_lag_days = 1

    STREAMS = [
        StreamDefinition(
            name="account_insights",
            description="Daily account-level Instagram insights (reach, profile views, …)",
            json_schema=build_json_schema(["date"], _ACCOUNT_METRICS),
            primary_key=["date"],
            grain="fact",
            slice_days=25,  # stay under the API's ~30-day account-insights ceiling
        ),
        StreamDefinition(
            name="media",
            description="Instagram media objects (posts, reels, stories)",
            json_schema=build_json_schema(
                ["id", "media_type", "media_product_type", "timestamp", "permalink"], []
            ),
            primary_key=["id"],
            grain="entity",
            date_partitioned=False,
            default_cursor_field=None,
        ),
        StreamDefinition(
            name="media_insights",
            description="Per-media insights for recently published media",
            json_schema=build_json_schema(["media_id", "date"], _MEDIA_METRICS),
            primary_key=["media_id"],
            grain="fact",
            date_partitioned=False,
        ),
        StreamDefinition(
            name="reel_insights",
            description="Per-reel insights (plays, watch time, replays) for recent reels",
            json_schema=build_json_schema(["media_id", "date"], _REEL_METRICS),
            primary_key=["media_id"],
            grain="fact",
            date_partitioned=False,
        ),
        StreamDefinition(
            name="story_insights",
            description="Per-story insights for the last 24h of stories",
            json_schema=build_json_schema(["media_id", "date"], _STORY_METRICS),
            primary_key=["media_id"],
            grain="fact",
            date_partitioned=False,
        ),
        StreamDefinition(
            name="audience_demographics",
            description="Follower demographics by age / gender / city / country (lifetime snapshot)",
            json_schema=build_json_schema(["date", "breakdown", "value"], ["followers"]),
            primary_key=["date", "breakdown", "value"],
            grain="fact",
            slice_days=1,
        ),
    ]

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._account_metrics = list(ctx.config.get("account_metrics") or _ACCOUNT_METRICS)
        self._media_lookback_days = int(ctx.config.get("media_lookback_days", 30))

    def _ig_id(self) -> str:
        return self.ctx.resource_id or ""

    # --- CHECK -----------------------------------------------------
    async def check_connection(self) -> HealthReport:
        ig_id = self._ig_id()
        if not ig_id:
            return HealthReport(status=HealthStatus.HEALTHY, message="Connected to Meta.")
        try:
            node = await self._get(ig_id, {"fields": "id,username,followers_count,media_count"})
        except E.ConnectorError as exc:
            if exc.code in (E.ErrorCode.RESOURCE_NOT_FOUND, E.ErrorCode.INVALID_CONFIGURATION):
                return HealthReport(
                    status=HealthStatus.NOT_SUPPORTED,
                    message=(
                        "This asset has no Instagram insights endpoint. Instagram Insights needs "
                        "a Business or Creator account linked to a Facebook Page."
                    ),
                )
            return health_from_error(exc)
        return HealthReport(
            status=HealthStatus.HEALTHY,
            message=f"Connected to @{node.get('username')} ({node.get('followers_count', 0)} followers).",
        )

    # --- DISCOVER -----------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        out: list[ResourceDescriptor] = []
        fields = "id,name,instagram_business_account{id,username,followers_count,media_count}"
        async for page in self._paged("me/accounts", {"fields": fields}):
            iba = page.get("instagram_business_account")
            if not iba:
                out.append(
                    ResourceDescriptor(
                        resource_id=f"page:{page.get('id')}",
                        name=f"{page.get('name')} (no linked Instagram account)",
                        resource_type="facebook_page",
                        selectable=False,
                        unsupported_reason=(
                            "This Facebook Page has no linked Instagram Business/Creator account."
                        ),
                    )
                )
                continue
            out.append(
                ResourceDescriptor(
                    resource_id=str(iba.get("id")),
                    name=f"@{iba.get('username')}",
                    resource_type="instagram_business_account",
                    parent_id=str(page.get("id")),
                    metadata={
                        "page_id": page.get("id"),
                        "page_name": page.get("name"),
                        "username": iba.get("username"),
                        "followers_count": iba.get("followers_count"),
                    },
                )
            )
        return out

    # --- READ --------------------------------------------------
    async def read_slice(
        self, stream: StreamDefinition, slice_: StreamSlice
    ) -> AsyncIterator[Record | EntityRecord]:
        if stream.name == "account_insights":
            async for rec in self._account_insights(stream, slice_):
                yield rec
        elif stream.name == "media":
            async for rec in self._media_entities(stream):
                yield rec
        elif stream.name == "media_insights":
            async for rec in self._media_insights(stream, _MEDIA_METRICS):
                yield rec
        elif stream.name == "reel_insights":
            async for rec in self._media_insights(stream, _REEL_METRICS, product_type="REELS"):
                yield rec
        elif stream.name == "story_insights":
            async for rec in self._story_insights(stream):
                yield rec
        elif stream.name == "audience_demographics":
            async for rec in self._audience_demographics(stream):
                yield rec

    async def _account_insights(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[Record]:
        since = (slice_.start_date or (datetime.now(UTC).date() - timedelta(days=25))).isoformat()
        until = (slice_.end_date or datetime.now(UTC).date()).isoformat()
        metrics = list(self._account_metrics)

        payload: dict[str, Any] | None = None
        for _attempt in range(len(metrics) + 1):
            try:
                payload = await self._get(
                    f"{self._ig_id()}/insights",
                    {"metric": ",".join(metrics), "period": "day", "since": since, "until": until},
                )
                break
            except E.ConnectorError as exc:
                # A blanket (#10) on every metric is not a bad-metric problem —
                # it means the Meta app lacks Advanced Access to
                # instagram_manage_insights (granted only via App Review). Say so,
                # rather than the generic "reconnect with another account".
                if exc.code == E.ErrorCode.PERMISSION_ERROR:
                    raise E.invalid_configuration(
                        "Instagram account-level insights are not available to this Meta app "
                        "(the /insights endpoint returned (#10) for every metric).",
                        provider="meta",
                        user_action=(
                            "The Meta app needs Advanced Access to instagram_manage_insights, "
                            "granted via Meta App Review. The media and media_insights streams "
                            "do not need it and are unaffected — disable the account_insights "
                            "stream on this connection until the app is reviewed."
                        ),
                    ) from exc
                dropped = _rejected_metric(exc, metrics)
                if dropped is None:
                    raise
                metrics.remove(dropped)
                self.ctx.progress.note(f"Instagram metric '{dropped}' unavailable — skipping it")
                if not metrics:
                    return
        if payload is None:
            return

        by_date: dict[str, dict[str, Any]] = {}
        for series in payload.get("data", []):
            name = series.get("name")
            for point in series.get("values", []):
                day = _day_of(point.get("end_time"))
                if not day:
                    continue
                by_date.setdefault(day, {})[name] = point.get("value")

        for day, values in sorted(by_date.items()):
            row_date = to_date(day)
            metric_nums = {k: to_int(v) for k, v in values.items()}
            yield Record(
                stream=stream.name,
                key_values={"date": day},
                date=row_date,
                dimensions={"date": day},
                metrics=metric_nums,
                measures={"reach": metric_nums.get("reach"), "views": metric_nums.get("views")},
                raw={"date": day, "metrics": values},
            )

    async def _media_entities(self, stream: StreamDefinition) -> AsyncIterator[EntityRecord]:
        fields = "id,caption,media_type,media_product_type,timestamp,permalink,like_count,comments_count"
        async for media in self._paged(f"{self._ig_id()}/media", {"fields": fields}):
            ts = to_date((media.get("timestamp") or "")[:10])
            yield EntityRecord(
                stream=stream.name,
                level="media",
                external_id=str(media.get("id") or ""),
                name=(media.get("caption") or "")[:280] or None,
                status=media.get("media_type"),
                objective=media.get("media_product_type"),
                start_date=ts,
                raw=media,
            )

    async def _media_insights(
        self, stream: StreamDefinition, want_metrics: list[str], *, product_type: str | None = None
    ) -> AsyncIterator[Record]:
        cutoff = datetime.now(UTC).date() - timedelta(days=self._media_lookback_days)
        fields = "id,media_type,media_product_type,timestamp"
        async for media in self._paged(f"{self._ig_id()}/media", {"fields": fields}):
            ts = to_date((media.get("timestamp") or "")[:10])
            if ts and ts < cutoff:
                # Media is returned newest-first; once we pass the window, stop.
                break
            if product_type and media.get("media_product_type") != product_type:
                continue
            media_id = str(media.get("id") or "")
            metrics = list(want_metrics)
            payload: dict[str, Any] | None = None
            for _attempt in range(len(metrics) + 1):
                try:
                    payload = await self._get(f"{media_id}/insights", {"metric": ",".join(metrics)})
                    break
                except E.ConnectorError as exc:
                    dropped = _rejected_metric(exc, metrics)
                    if dropped is None:
                        payload = None
                        break
                    metrics.remove(dropped)
                    if not metrics:
                        break
            if not payload:
                continue
            values = {s.get("name"): _first_value(s) for s in payload.get("data", [])}
            nums = {k: to_int(v) for k, v in values.items()}
            yield Record(
                stream=stream.name,
                key_values={"media_id": media_id},
                date=ts,
                dimensions={"media_id": media_id, "media_type": media.get("media_type")},
                metrics=nums,
                measures={"reach": nums.get("reach"), "views": nums.get("views")},
                raw={"media_id": media_id, "insights": values, "media": media},
            )

    async def _story_insights(self, stream: StreamDefinition) -> AsyncIterator[Record]:
        try:
            stories = [s async for s in self._paged(f"{self._ig_id()}/stories", {"fields": "id,timestamp"})]
        except E.ConnectorError as exc:
            self.ctx.progress.note(f"Instagram stories unavailable — skipping ({exc.code})")
            return
        for story in stories:
            media_id = str(story.get("id") or "")
            ts = to_date((story.get("timestamp") or "")[:10])
            metrics = list(_STORY_METRICS)
            payload: dict[str, Any] | None = None
            for _attempt in range(len(metrics) + 1):
                try:
                    payload = await self._get(f"{media_id}/insights", {"metric": ",".join(metrics)})
                    break
                except E.ConnectorError as exc:
                    dropped = _rejected_metric(exc, metrics)
                    if dropped is None or not metrics:
                        payload = None
                        break
                    metrics.remove(dropped)
            if not payload:
                continue
            values = {s.get("name"): _first_value(s) for s in payload.get("data", [])}
            nums = {k: to_int(v) for k, v in values.items()}
            yield Record(
                stream=stream.name,
                key_values={"media_id": media_id},
                date=ts,
                dimensions={"media_id": media_id},
                metrics=nums,
                measures={"reach": nums.get("reach"), "views": nums.get("views")},
                raw={"media_id": media_id, "insights": values, "story": story},
            )

    async def _audience_demographics(self, stream: StreamDefinition) -> AsyncIterator[Record]:
        today = datetime.now(UTC).date()
        for breakdown in _DEMOGRAPHIC_BREAKDOWNS:
            try:
                payload = await self._get(
                    f"{self._ig_id()}/insights",
                    {
                        "metric": "follower_demographics",
                        "period": "lifetime",
                        "metric_type": "total_value",
                        "breakdown": breakdown,
                    },
                )
            except E.ConnectorError as exc:
                self.ctx.progress.note(
                    f"Instagram follower_demographics[{breakdown}] unavailable — skipping ({exc.code})"
                )
                continue
            for series in payload.get("data", []):
                tv = series.get("total_value") or {}
                for bucket in tv.get("breakdowns", []):
                    for result in bucket.get("results", []):
                        dim_values = result.get("dimension_values") or []
                        value = dim_values[0] if dim_values else None
                        if value is None:
                            continue
                        yield Record(
                            stream=stream.name,
                            key_values={"date": today.isoformat(), "breakdown": breakdown, "value": value},
                            date=today,
                            dimensions={"date": today.isoformat(), "breakdown": breakdown, "value": value},
                            metrics={"followers": to_int(result.get("value"))},
                            measures={},
                            raw=result,
                        )


def _day_of(end_time: str | None) -> str | None:
    return end_time[:10] if end_time else None


def _first_value(series: dict[str, Any]) -> Any:
    vals = series.get("values")
    if isinstance(vals, list) and vals:
        return vals[0].get("value")
    return series.get("total_value", {}).get("value") if isinstance(series.get("total_value"), dict) else None


def _rejected_metric(exc: E.ConnectorError, metrics: list[str]) -> str | None:
    """If Meta rejected the request for naming an unsupported metric, return it."""
    if exc.code not in (E.ErrorCode.INVALID_CONFIGURATION, E.ErrorCode.RESOURCE_NOT_FOUND):
        return None
    blob = f"{exc.message} {exc.technical_details}".lower()
    for metric in metrics:
        if metric.lower() in blob:
            return metric
    return None


registry.register(
    RegistryEntry(
        connector_class=InstagramInsightsConnector,
        requires_settings=("meta_app_id", "meta_app_secret"),
        prerequisites=(
            "An Instagram Business or Creator account linked to a Facebook Page.",
            "The connected user must have a role on that Page.",
        ),
        caveats=(
            "instagram_manage_insights and pages_read_engagement require Meta App Review "
            "outside development mode.",
            "Account-level insights serve only ~30 days of history per the API.",
            "Audience/demographic metrics are not implemented in v1 (version- and eligibility-dependent).",
        ),
        resource_label="Instagram Account",
        tags=("social", "meta", "instagram"),
    )
)
