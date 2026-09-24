"""Meta Ads connector — official Graph API / Marketing API (§9).

Entity streams (campaigns, adsets, ads) land in `ad_entities`; the `*_insights`
streams are day-incremented facts from the `/insights` edge, one row per entity
per day (`time_increment=1`), sliced into bounded date windows.

Meta's conversion data lives in the polymorphic `actions` / `action_values`
arrays; the connector promotes purchase/lead counts to the shared `conversions`
/ `conversion_value` columns and keeps the full arrays in the metrics JSON so
nothing provider-native is lost (§11). The action types counted are
configurable per connection.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import date
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
from app.connectors.validation import build_json_schema, to_date, to_float, to_int
from app.oauth.meta import SCOPE_ADS_READ, SCOPE_BUSINESS_MANAGEMENT

# Requested on every insights call (campaign / adset / ad).
_INSIGHT_FIELDS = [
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "cpc",
    "cpm",
    "cpp",
    "ctr",
    "inline_link_clicks",
    "inline_post_engagement",
    "unique_clicks",
    "unique_ctr",
    "outbound_clicks",
    "unique_outbound_clicks",
    "cost_per_inline_link_click",
    "cost_per_unique_click",
    "website_ctr",
    "actions",
    "action_values",
    "cost_per_action_type",
    "purchase_roas",
    "website_purchase_roas",
    "video_thruplay_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
]
# Only selectable at ad level — Meta rejects them at campaign/adset level.
_AD_LEVEL_FIELDS = ["quality_ranking", "engagement_rate_ranking", "conversion_rate_ranking"]
# Array-typed insight fields: kept verbatim in metrics JSON, summed to a scalar too.
_ARRAY_FIELDS = frozenset(
    {
        "actions",
        "action_values",
        "website_ctr",
        "cost_per_action_type",
        "purchase_roas",
        "website_purchase_roas",
        "outbound_clicks",
        "unique_outbound_clicks",
        "video_thruplay_watched_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
        "video_30_sec_watched_actions",
        "video_avg_time_watched_actions",
    }
)
_DEFAULT_CONVERSION_ACTIONS = (
    "offsite_conversion.fb_pixel_purchase",
    "purchase",
    "omni_purchase",
    "lead",
    "offsite_conversion.fb_pixel_lead",
    "complete_registration",
)

_INSIGHT_LEVELS = {
    "campaign_insights": ("campaign", ["campaign_id", "campaign_name"], ["date", "campaign_id"]),
    "adset_insights": ("adset", ["adset_id", "adset_name", "campaign_id"], ["date", "adset_id"]),
    "ad_insights": (
        "ad",
        ["ad_id", "ad_name", "adset_id", "campaign_id"],
        ["date", "ad_id"],
    ),
}
# Ad-level insights split by a Meta `breakdowns` combination. The breakdown keys
# become dimensions and part of the primary key.
_BREAKDOWN_STREAMS = {
    "ad_insights_by_age_gender": ["age", "gender"],
    "ad_insights_by_platform": ["publisher_platform", "platform_position", "impression_device"],
    "ad_insights_by_country": ["country"],
    "ad_insights_by_region": ["region"],
    "ad_insights_by_device": ["impression_device", "device_platform"],
}
_ENTITY_EDGES = {
    "campaigns": (
        "campaign",
        "campaigns",
        [
            "id",
            "name",
            "status",
            "effective_status",
            "objective",
            "buying_type",
            "bid_strategy",
            "daily_budget",
            "lifetime_budget",
            "budget_remaining",
            "start_time",
            "stop_time",
        ],
    ),
    "adsets": (
        "adset",
        "adsets",
        [
            "id",
            "name",
            "status",
            "effective_status",
            "campaign_id",
            "optimization_goal",
            "billing_event",
            "bid_amount",
            "daily_budget",
            "lifetime_budget",
            "start_time",
            "end_time",
        ],
    ),
    "ads": (
        "ad",
        "ads",
        ["id", "name", "status", "effective_status", "adset_id", "campaign_id", "creative"],
    ),
    "ad_creatives": (
        "creative",
        "adcreatives",
        [
            "id",
            "name",
            "title",
            "body",
            "image_url",
            "thumbnail_url",
            "video_id",
            "call_to_action_type",
            "object_story_id",
            "effective_object_story_id",
            "instagram_permalink_url",
        ],
    ),
    "adaccounts": (
        "account",
        "",  # the account node itself, not an edge
        [
            "id",
            "account_id",
            "name",
            "currency",
            "account_status",
            "disable_reason",
            "spend_cap",
            "amount_spent",
            "balance",
            "timezone_name",
            "business_name",
            "funding_source",
        ],
    ),
}


# Per-edge page-size override, applied instead of MetaConnector.page_size (200). Only
# `adcreatives` needs one today — see `_paged`'s docstring for why.
_ENTITY_PAGE_SIZE = {"ad_creatives": 25}


def _streams() -> list[StreamDefinition]:
    out: list[StreamDefinition] = []
    for name, (level, path, fields) in _ENTITY_EDGES.items():
        out.append(
            StreamDefinition(
                name=name,
                description=f"Meta Ads {name} (entity attributes)",
                json_schema=build_json_schema(fields, []),
                primary_key=["id"],
                grain="entity",
                date_partitioned=False,
                default_cursor_field=None,
                spec={"level": level, "path": path, "fields": fields},
            )
        )
    for name, (level, extra_dims, pk) in _INSIGHT_LEVELS.items():
        fields = [*_INSIGHT_FIELDS, *(_AD_LEVEL_FIELDS if level == "ad" else [])]
        out.append(
            StreamDefinition(
                name=name,
                description=f"Meta Ads insights at {level} level (daily)",
                json_schema=build_json_schema(["date", *extra_dims], fields),
                primary_key=pk,
                grain="fact",
                slice_days=14 if level == "campaign" else 7,
                spec={"level": level, "extra_dims": extra_dims, "fields": fields},
            )
        )
    ad_dims = ["ad_id", "ad_name", "adset_id", "campaign_id"]
    for name, breakdowns in _BREAKDOWN_STREAMS.items():
        out.append(
            StreamDefinition(
                name=name,
                description=f"Meta Ads ad-level insights broken down by {', '.join(breakdowns)}",
                json_schema=build_json_schema(["date", *ad_dims, *breakdowns], _INSIGHT_FIELDS),
                primary_key=["date", "ad_id", *breakdowns],
                grain="fact",
                slice_days=7,
                spec={
                    "level": "ad",
                    "extra_dims": ad_dims,
                    "fields": _INSIGHT_FIELDS,
                    "breakdowns": breakdowns,
                },
            )
        )
    return out


class MetaAdsConnector(MetaConnector):
    connector_id = "meta_ads"
    name = "Meta Ads"
    version = "1.0.0"
    documentation_url = "https://developers.facebook.com/docs/marketing-apis"
    icon = "meta-ads"
    required_scopes = (SCOPE_ADS_READ, SCOPE_BUSINESS_MANAGEMENT)
    STREAMS = _streams()

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._currency: str | None = ctx.resource_metadata.get("currency")
        self._conv_actions = tuple(ctx.config.get("conversion_action_types") or _DEFAULT_CONVERSION_ACTIONS)

    def _act(self) -> str:
        rid = self.ctx.resource_id or ""
        return rid if rid.startswith("act_") else f"act_{rid.lstrip('act_')}"

    async def _require_ads_scope(self) -> None:
        """Meta answers every ads-edge request from a token without `ads_read`
        with a bare `(#100) Unsupported get request`. Catch the missing grant
        here and say what to actually do about it."""
        await self.ctx.token_provider.access_token()  # forces the identity + its scopes to load
        granted = set(self.ctx.token_provider.scopes or [])
        if not granted & {SCOPE_ADS_READ, "ads_management"}:
            raise E.invalid_configuration(
                "This Meta connection was not granted the ads_read permission, so it "
                "cannot see any ad accounts.",
                provider="meta",
                user_action=(
                    "Disconnect this Meta account and reconnect via Meta Ads (not Instagram), "
                    "approving the Ads / ads_read permission. The Meta app must have the "
                    "Marketing API product added; outside Development Mode ads_read requires "
                    "Meta App Review."
                ),
            )

    # --- CHECK -------------------------------------------------------
    async def check_connection(self) -> HealthReport:
        try:
            await self._require_ads_scope()
        except E.ConnectorError as exc:
            return health_from_error(exc)
        try:
            acc = await self._get(self._act(), {"fields": "id,name,currency,account_status,disable_reason"})
        except E.ConnectorError as exc:
            return health_from_error(exc)
        status = acc.get("account_status")
        if status not in (1, None):  # 1 == ACTIVE
            return HealthReport(
                status=HealthStatus.INVALID_CONFIGURATION,
                message=f"Meta ad account {acc.get('id')} is not active (account_status={status}).",
            )
        self._currency = acc.get("currency") or self._currency
        return HealthReport(status=HealthStatus.HEALTHY, message=f"Connected to {acc.get('name')}.")

    # --- DISCOVER --------------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        await self._require_ads_scope()
        out: list[ResourceDescriptor] = []
        fields = "id,account_id,name,currency,account_status,timezone_name,business_name"
        async for acc in self._paged("me/adaccounts", {"fields": fields}):
            active = acc.get("account_status") == 1
            out.append(
                ResourceDescriptor(
                    resource_id=acc.get("id") or f"act_{acc.get('account_id')}",
                    name=acc.get("name") or acc.get("id"),
                    resource_type="ad_account",
                    metadata={
                        "currency": acc.get("currency"),
                        "timezone": acc.get("timezone_name"),
                        "business": acc.get("business_name"),
                        "account_status": acc.get("account_status"),
                    },
                    selectable=active,
                    unsupported_reason=None if active else "Ad account is not active.",
                )
            )
        return out

    # --- READ ----------------------------------------------------
    async def read_slice(
        self, stream: StreamDefinition, slice_: StreamSlice
    ) -> AsyncIterator[Record | EntityRecord]:
        if stream.grain == "entity":
            spec = stream.spec
            fields = ",".join(spec["fields"])
            if not spec["path"]:  # the ad-account node itself, not an edge
                node = await self._get(self._act(), {"fields": fields})
                yield self._entity(stream, spec["level"], node)
                return
            async for node in self._paged(
                f"{self._act()}/{spec['path']}", {"fields": fields}, limit=_ENTITY_PAGE_SIZE.get(stream.name)
            ):
                yield self._entity(stream, spec["level"], node)
            return

        level = stream.spec["level"]
        extra = stream.spec["extra_dims"]
        fields = stream.spec.get("fields", _INSIGHT_FIELDS)
        breakdowns = stream.spec.get("breakdowns") or []
        since = (slice_.start_date or date.today()).isoformat()
        until = (slice_.end_date or date.today()).isoformat()
        params = {
            "level": level,
            "fields": ",".join([*fields, *extra]),
            "time_increment": 1,
            "time_range": json.dumps({"since": since, "until": until}),
        }
        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        windows = self.ctx.config.get("action_attribution_windows")
        if windows:
            params["action_attribution_windows"] = ",".join(windows)
        currency = self._currency or await self._fetch_currency()
        async for row in self._paged(f"{self._act()}/insights", params):
            yield self._fact(stream, level, [*extra, *breakdowns], row, currency)

    # --- mapping ---------------------------------------------------
    def _fact(self, stream, level: str, extra: list[str], row: dict, currency: str | None) -> Record:
        row_date = to_date(row.get("date_start"))
        id_field = f"{level}_id"
        dimensions = {"date": row_date.isoformat() if row_date else None}
        for key in extra:
            dimensions[key] = row.get(key)
        # Which attribution rule produced these conversion numbers — otherwise
        # identical-looking figures across time can mean different windows.
        windows = self.ctx.config.get("action_attribution_windows")
        dimensions["attribution_windows"] = ",".join(windows) if windows else "default"

        fields = stream.spec.get("fields", _INSIGHT_FIELDS)
        metrics: dict[str, Any] = {}
        for f in fields:
            if f in _ARRAY_FIELDS:
                metrics[f] = row.get(f)  # keep the polymorphic array verbatim
                metrics[f"{f}__sum"] = _sum_all(row.get(f)) or None
            else:
                metrics[f] = to_float(row.get(f))

        conv_count = _sum_actions(row.get("actions"), self._conv_actions)
        conv_value = _sum_actions(row.get("action_values"), self._conv_actions)
        measures = {
            "impressions": to_int(row.get("impressions")),
            "clicks": to_int(row.get("clicks")),
            "cost": to_float(row.get("spend")),
            "reach": to_int(row.get("reach")),
            "conversions": conv_count or None,
            "conversion_value": conv_value or None,
        }
        key_values: dict[str, Any] = {}
        for k in stream.primary_key:
            if k == "date":
                key_values[k] = row_date.isoformat() if row_date else ""
            elif k == id_field:
                key_values[k] = str(row.get(id_field) or "")
            else:  # a breakdown or extra dimension carries its own value
                key_values[k] = str(row.get(k) or "")
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

    def _entity(self, stream, level: str, node: dict) -> EntityRecord:
        return EntityRecord(
            stream=stream.name,
            level=level,
            external_id=str(node.get("id") or ""),
            name=node.get("name"),
            status=node.get("status") or node.get("effective_status"),
            parent_external_id=node.get("adset_id") or node.get("campaign_id"),
            objective=node.get("objective") or node.get("call_to_action_type"),
            daily_budget=_money(node.get("daily_budget")),
            lifetime_budget=_money(node.get("lifetime_budget")),
            currency=node.get("currency") or self._currency,
            start_date=to_date((node.get("start_time") or "")[:10]),
            end_date=to_date((node.get("stop_time") or node.get("end_time") or "")[:10]),
            raw=node,
        )

    async def _fetch_currency(self) -> str | None:
        try:
            acc = await self._get(self._act(), {"fields": "currency"})
            self._currency = acc.get("currency")
        except E.ConnectorError:
            self._currency = None
        return self._currency


def _sum_all(arr: Any) -> float:
    """Sum every {value} in one of Meta's polymorphic metric arrays."""
    if not isinstance(arr, list):
        return 0.0
    total = 0.0
    for item in arr:
        if isinstance(item, dict):
            total += to_float(item.get("value")) or 0.0
    return total


def _sum_actions(actions: Any, wanted: tuple[str, ...]) -> float:
    if not isinstance(actions, list):
        return 0.0
    total = 0.0
    for action in actions:
        if not isinstance(action, dict):
            continue
        if action.get("action_type") in wanted:
            total += to_float(action.get("value")) or 0.0
    return total


def _money(value: Any) -> float | None:
    """Meta budgets are minor units (cents) as strings."""
    cents = to_float(value)
    return None if cents is None else cents / 100.0


registry.register(
    RegistryEntry(
        connector_class=MetaAdsConnector,
        requires_settings=("meta_app_id", "meta_app_secret"),
        prerequisites=(
            "A Meta app with Marketing API access and the ads_read permission.",
            "The connected user must have an admin/analyst role on the ad account or its Business.",
        ),
        caveats=(
            "Outside development mode, ads_read and business_management require Meta App Review.",
            "Meta issues no refresh token — a long-lived token expires in ~60 days and the "
            "connection must be reconnected.",
        ),
        resource_label="Meta Ad Account",
        tags=("ads", "meta", "facebook"),
    )
)
