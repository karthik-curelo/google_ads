"""Facebook Pages connector — official Pages API / Page Insights (§10, §24).

Distinct from Meta Ads: this is a Page's *organic* surface — the page itself, its
published posts, and the insight time-series for both. Page endpoints need a
**Page access token**, not the user token, so the connector resolves the page
token from `/me/accounts` (with the user token) and uses it for everything else.

`page_insights` metric availability shifts between Graph versions — the connector
requests a set and drops any single metric Meta rejects (error code 100), so a
version bump degrades instead of breaking the stream.
"""

from __future__ import annotations

import hashlib
import hmac
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
    SCOPE_PAGES_READ_ENGAGEMENT,
    SCOPE_PAGES_SHOW_LIST,
    SCOPE_READ_INSIGHTS,
)

_PAGE_FIELDS = [
    "id",
    "name",
    "category",
    "about",
    "fan_count",
    "followers_count",
    "link",
    "verification_status",
    "talking_about_count",
    "were_here_count",
]
_POST_FIELDS = (
    "id,message,created_time,permalink_url,status_type,is_published,"
    "shares,reactions.summary(true),comments.summary(true)"
)
_PAGE_INSIGHT_METRICS = [
    "page_impressions",
    "page_impressions_unique",
    "page_post_engagements",
    "page_fans",
    "page_fan_adds",
    "page_fan_removes",
    "page_views_total",
    "page_video_views",
]
_POST_INSIGHT_METRICS = [
    "post_impressions",
    "post_impressions_unique",
    "post_clicks",
    "post_reactions_by_type_total",
    "post_video_views",
]


def _streams() -> list[StreamDefinition]:
    return [
        StreamDefinition(
            name="pages",
            description="The Facebook Page (name, category, fan/follower counts)",
            json_schema=build_json_schema(_PAGE_FIELDS, []),
            primary_key=["id"],
            grain="entity",
            date_partitioned=False,
            default_cursor_field=None,
            spec={"level": "page"},
        ),
        StreamDefinition(
            name="posts",
            description="Published Page posts (message, type, reaction/comment/share counts)",
            json_schema=build_json_schema(
                ["id", "message", "created_time", "permalink_url", "status_type"], []
            ),
            primary_key=["id"],
            grain="entity",
            date_partitioned=False,
            default_cursor_field=None,
            spec={"level": "post"},
        ),
        StreamDefinition(
            name="page_insights",
            description="Daily Page-level insight metrics",
            json_schema=build_json_schema(["date"], _PAGE_INSIGHT_METRICS),
            primary_key=["date"],
            grain="fact",
            slice_days=25,
        ),
        StreamDefinition(
            name="post_insights",
            description="Per-post lifetime insight metrics for recent posts",
            json_schema=build_json_schema(["post_id", "date"], _POST_INSIGHT_METRICS),
            primary_key=["post_id"],
            grain="fact",
            date_partitioned=False,
        ),
    ]


class FacebookPagesConnector(MetaConnector):
    connector_id = "facebook_pages"
    name = "Facebook Pages"
    version = "1.0.0"
    documentation_url = "https://developers.facebook.com/docs/pages-api"
    icon = "facebook"
    required_scopes = (SCOPE_PAGES_SHOW_LIST, SCOPE_PAGES_READ_ENGAGEMENT, SCOPE_READ_INSIGHTS)
    provider_lag_days = 1
    STREAMS = _streams()

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._page_token_cache: str | None = None
        self._post_lookback_days = int(ctx.config.get("post_lookback_days", 90))

    # --- auth: page endpoints need the Page token, not the user token ----
    def _proof(self, token: str) -> str | None:
        if not self._app_secret:
            return None
        return hmac.new(self._app_secret.encode(), token.encode(), hashlib.sha256).hexdigest()

    async def _page_token(self) -> str | None:
        if self._page_token_cache is not None:
            return self._page_token_cache or None
        user_params = await super()._auth_params()
        payload = await self.http.get(
            f"{self.graph_base}/me/accounts",
            params={"fields": "id,name,access_token", **user_params},
        )
        self._page_token_cache = ""
        for page in payload.get("data", []):
            if str(page.get("id")) == str(self.ctx.resource_id):
                self._page_token_cache = page.get("access_token") or ""
                break
        return self._page_token_cache or None

    async def _auth_params(self) -> dict[str, str]:
        if self.ctx.resource_id:
            token = await self._page_token()
            if token:
                proof = self._proof(token)
                return {"access_token": token, **({"appsecret_proof": proof} if proof else {})}
        return await super()._auth_params()

    # --- CHECK ---------------------------------------------------------
    async def check_connection(self) -> HealthReport:
        page_id = self.ctx.resource_id
        if not page_id:
            return HealthReport(status=HealthStatus.HEALTHY, message="Connected to Meta.")
        try:
            if not await self._page_token():
                return HealthReport(
                    status=HealthStatus.PERMISSION_DENIED,
                    message=(
                        f"The connected user has no accessible token for Page {page_id}. "
                        "Grant a role on the Page and approve pages_show_list."
                    ),
                )
            node = await self._get(page_id, {"fields": "id,name,followers_count"})
        except E.ConnectorError as exc:
            return health_from_error(exc)
        return HealthReport(
            status=HealthStatus.HEALTHY,
            message=f"Connected to {node.get('name')} ({node.get('followers_count', 0)} followers).",
        )

    # --- DISCOVER ----------------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        user_params = await super()._auth_params()
        payload = await self.http.get(
            f"{self.graph_base}/me/accounts",
            params={"fields": "id,name,category,tasks", **user_params},
        )
        out: list[ResourceDescriptor] = []
        for page in payload.get("data", []):
            tasks = page.get("tasks") or []
            can_analyze = "ANALYZE" in tasks or not tasks
            out.append(
                ResourceDescriptor(
                    resource_id=str(page.get("id")),
                    name=page.get("name") or str(page.get("id")),
                    resource_type="facebook_page",
                    metadata={"category": page.get("category"), "tasks": tasks},
                    selectable=can_analyze,
                    unsupported_reason=None if can_analyze else "No ANALYZE task on this Page.",
                )
            )
        return out

    # --- READ ------------------------------------------------------
    async def read_slice(
        self, stream: StreamDefinition, slice_: StreamSlice
    ) -> AsyncIterator[Record | EntityRecord]:
        if stream.name == "pages":
            node = await self._get(self.ctx.resource_id, {"fields": ",".join(_PAGE_FIELDS)})
            yield self._page_entity(node)
        elif stream.name == "posts":
            async for post in self._paged(
                f"{self.ctx.resource_id}/published_posts", {"fields": _POST_FIELDS}
            ):
                yield self._post_entity(post)
        elif stream.name == "page_insights":
            async for rec in self._page_insights(stream, slice_):
                yield rec
        elif stream.name == "post_insights":
            async for rec in self._post_insights(stream):
                yield rec

    # --- mapping -------------------------------------------------
    def _page_entity(self, node: dict) -> EntityRecord:
        return EntityRecord(
            stream="pages",
            level="page",
            external_id=str(node.get("id") or ""),
            name=node.get("name"),
            status=node.get("verification_status"),
            objective=node.get("category"),
            raw=node,
        )

    def _post_entity(self, post: dict) -> EntityRecord:
        reactions = ((post.get("reactions") or {}).get("summary") or {}).get("total_count")
        comments = ((post.get("comments") or {}).get("summary") or {}).get("total_count")
        shares = (post.get("shares") or {}).get("count")
        ts = to_date((post.get("created_time") or "")[:10])
        return EntityRecord(
            stream="posts",
            level="post",
            external_id=str(post.get("id") or ""),
            name=(post.get("message") or "")[:280] or None,
            status=post.get("status_type"),
            start_date=ts,
            raw={
                **post,
                "reaction_count": reactions,
                "comment_count": comments,
                "share_count": shares,
            },
        )

    async def _page_insights(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[Record]:
        since = (slice_.start_date or (datetime.now(UTC).date() - timedelta(days=25))).isoformat()
        until = (slice_.end_date or datetime.now(UTC).date()).isoformat()
        metrics = list(_PAGE_INSIGHT_METRICS)
        payload: dict[str, Any] | None = None
        for _attempt in range(len(metrics) + 1):
            try:
                payload = await self._get(
                    f"{self.ctx.resource_id}/insights",
                    {"metric": ",".join(metrics), "period": "day", "since": since, "until": until},
                )
                break
            except E.ConnectorError as exc:
                dropped = _rejected_metric(exc, metrics)
                if dropped is None:
                    raise
                metrics.remove(dropped)
                self.ctx.progress.note(f"Page metric '{dropped}' unavailable — skipping it")
                if not metrics:
                    return
        if payload is None:
            return

        by_date: dict[str, dict[str, Any]] = {}
        for series in payload.get("data", []):
            name = series.get("name")
            for point in series.get("values", []):
                day = (point.get("end_time") or "")[:10]
                if not day:
                    continue
                by_date.setdefault(day, {})[name] = point.get("value")

        for day, values in sorted(by_date.items()):
            nums = {k: to_int(v) for k, v in values.items()}
            yield Record(
                stream=stream.name,
                key_values={"date": day},
                date=to_date(day),
                dimensions={"date": day},
                metrics=nums,
                measures={"impressions": nums.get("page_impressions")},
                raw={"date": day, "metrics": values},
            )

    async def _post_insights(self, stream: StreamDefinition) -> AsyncIterator[Record]:
        cutoff = datetime.now(UTC).date() - timedelta(days=self._post_lookback_days)
        async for post in self._paged(
            f"{self.ctx.resource_id}/published_posts", {"fields": "id,created_time"}
        ):
            ts = to_date((post.get("created_time") or "")[:10])
            if ts and ts < cutoff:
                break  # newest-first
            post_id = str(post.get("id") or "")
            metrics = list(_POST_INSIGHT_METRICS)
            payload: dict[str, Any] | None = None
            for _attempt in range(len(metrics) + 1):
                try:
                    payload = await self._get(f"{post_id}/insights", {"metric": ",".join(metrics)})
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
            nums = {k: to_int(v) for k, v in values.items() if not isinstance(v, dict)}
            yield Record(
                stream=stream.name,
                key_values={"post_id": post_id},
                date=ts,
                dimensions={"post_id": post_id},
                metrics={**values, **nums},
                measures={"impressions": nums.get("post_impressions")},
                raw={"post_id": post_id, "insights": values, "post": post},
            )


def _first_value(series: dict[str, Any]) -> Any:
    vals = series.get("values")
    if isinstance(vals, list) and vals:
        return vals[0].get("value")
    return series.get("total_value", {}).get("value") if isinstance(series.get("total_value"), dict) else None


def _rejected_metric(exc: E.ConnectorError, metrics: list[str]) -> str | None:
    if exc.code not in (E.ErrorCode.INVALID_CONFIGURATION, E.ErrorCode.RESOURCE_NOT_FOUND):
        return None
    blob = f"{exc.message} {exc.technical_details}".lower()
    for metric in metrics:
        if metric.lower() in blob:
            return metric
    return None


registry.register(
    RegistryEntry(
        connector_class=FacebookPagesConnector,
        requires_settings=("meta_app_id", "meta_app_secret"),
        prerequisites=(
            "A Facebook Page and a role on it for the connected user.",
            "pages_show_list, pages_read_engagement and read_insights (App Review outside dev mode).",
        ),
        caveats=(
            "Page Insights metric names change between Graph versions; unavailable metrics are dropped.",
            "Meta issues no refresh token — reconnect ~every 60 days.",
        ),
        resource_label="Facebook Page",
        tags=("social", "meta", "facebook"),
    )
)
