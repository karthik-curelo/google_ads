"""Google Ads connector — official Google Ads API (REST, GAQL) (§8).

Query shape is GAQL sent to `googleAds:searchStream`; streams are GAQL specs.
Two grains: entity streams (campaigns, ad_groups) land in `ad_entities`;
`*_performance` streams are date-segmented facts.

Prerequisites are surfaced, not hidden (§8): without an approved developer token
the connector reports INVALID_CONFIGURATION with the exact fix, rather than a
raw 403 from Google.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from typing import Any

from app.connectors import errors as E
from app.connectors.base import (
    AuthType,
    EntityRecord,
    HealthReport,
    HealthStatus,
    Record,
    ResourceDescriptor,
    StreamDefinition,
    StreamSlice,
)
from app.connectors.google.base import GoogleConnector, health_from_error
from app.connectors.registry import RegistryEntry, registry
from app.connectors.validation import build_json_schema, micros_to_units, to_date, to_number
from app.oauth.google import SCOPE_ADWORDS

# Metrics valid on every reportable resource below.
_BASE_METRICS = [
    "metrics.impressions",
    "metrics.clicks",
    "metrics.cost_micros",
    "metrics.conversions",
    "metrics.conversions_value",
    "metrics.all_conversions",
    "metrics.all_conversions_value",
    "metrics.view_through_conversions",
    "metrics.ctr",
    "metrics.average_cpc",
    "metrics.average_cpm",
    "metrics.cost_per_conversion",
    "metrics.conversions_from_interactions_rate",
]
# Impression-share metrics. search_budget_lost_impression_share is campaign-only;
# the other two are also selectable on ad_group. None are valid on ad/keyword/geo.
_IS_CAMPAIGN = [
    "metrics.search_impression_share",
    "metrics.search_rank_lost_impression_share",
    "metrics.search_budget_lost_impression_share",
]
_IS_ADGROUP = [
    "metrics.search_impression_share",
    "metrics.search_rank_lost_impression_share",
]
_MEASURE_MAP = {
    "metrics.impressions": ("impressions", to_number),
    "metrics.clicks": ("clicks", to_number),
    "metrics.cost_micros": ("cost", micros_to_units),
    "metrics.conversions": ("conversions", to_number),
    "metrics.conversions_value": ("conversion_value", to_number),
    "metrics.average_cpc": (None, micros_to_units),  # kept in metrics json only
    "metrics.average_cpm": (None, micros_to_units),
    "metrics.cost_per_conversion": (None, micros_to_units),
}

_ENTITY_STREAMS = {
    "campaigns": {
        "grain": "entity",
        "level": "campaign",
        "resource": "campaign",
        "id_field": "campaign.id",
        "name_field": "campaign.name",
        "select": [
            "campaign.id",
            "campaign.name",
            "campaign.status",
            "campaign.advertising_channel_type",
            # v25 breaking change: start_date / end_date renamed to start_date_time / end_date_time
            "campaign.start_date_time",
            "campaign.end_date_time",
            "campaign_budget.amount_micros",
        ],
        "pk": ["campaign.id"],
    },
    "ad_groups": {
        "grain": "entity",
        "level": "ad_group",
        "resource": "ad_group",
        "id_field": "ad_group.id",
        "name_field": "ad_group.name",
        "select": [
            "ad_group.id",
            "ad_group.name",
            "ad_group.status",
            "ad_group.type",
            "ad_group.cpc_bid_micros",
            "campaign.id",
            "campaign.name",
        ],
        "pk": ["ad_group.id"],
    },
    "ads": {
        "grain": "entity",
        "level": "ad_group_ad",
        "resource": "ad_group_ad",
        "id_field": "ad_group_ad.ad.id",
        "name_field": "ad_group_ad.ad.name",
        "select": [
            "ad_group_ad.ad.id",
            "ad_group_ad.ad.name",
            "ad_group_ad.ad.type",
            "ad_group_ad.status",
            "ad_group_ad.ad.final_urls",
            "ad_group_ad.ad_strength",
            "ad_group.id",
            "campaign.id",
        ],
        "pk": ["ad_group_ad.ad.id"],
    },
    "conversion_actions": {
        "grain": "entity",
        "level": "conversion_action",
        "resource": "conversion_action",
        "id_field": "conversion_action.id",
        "name_field": "conversion_action.name",
        "select": [
            "conversion_action.id",
            "conversion_action.name",
            "conversion_action.status",
            "conversion_action.type",
            "conversion_action.category",
            "conversion_action.counting_type",
            "conversion_action.value_settings.default_value",
        ],
        "pk": ["conversion_action.id"],
    },
}
_PERF_STREAMS = {
    "campaign_performance": {
        "resource": "campaign",
        "dims": ["segments.date", "campaign.id", "campaign.name"],
        "metrics": _BASE_METRICS + _IS_CAMPAIGN,
        "pk": ["segments.date", "campaign.id"],
        "slice_days": 30,
    },
    "campaign_device_performance": {
        "resource": "campaign",
        "dims": ["segments.date", "campaign.id", "campaign.name", "segments.device"],
        "metrics": _BASE_METRICS,
        "pk": ["segments.date", "campaign.id", "segments.device"],
        "slice_days": 14,
    },
    "ad_group_performance": {
        "resource": "ad_group",
        "dims": ["segments.date", "campaign.id", "ad_group.id", "ad_group.name"],
        "metrics": _BASE_METRICS + _IS_ADGROUP,
        "pk": ["segments.date", "ad_group.id"],
        "slice_days": 14,
    },
    "ad_performance": {
        "resource": "ad_group_ad",
        "dims": ["segments.date", "campaign.id", "ad_group.id", "ad_group_ad.ad.id", "ad_group_ad.ad.name"],
        "metrics": _BASE_METRICS,
        "pk": ["segments.date", "ad_group_ad.ad.id"],
        "slice_days": 7,
    },
    "keyword_performance": {
        "resource": "keyword_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "ad_group_criterion.criterion_id",
            "ad_group_criterion.keyword.text",
            "ad_group_criterion.keyword.match_type",
        ],
        "metrics": _BASE_METRICS,
        "pk": ["segments.date", "ad_group.id", "ad_group_criterion.criterion_id"],
        "slice_days": 7,
    },
    "search_term_performance": {
        "resource": "search_term_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "search_term_view.search_term",
            "search_term_view.status",
            "segments.search_term_match_type",
        ],
        "metrics": _BASE_METRICS,
        "pk": ["segments.date", "ad_group.id", "search_term_view.search_term"],
        "slice_days": 7,
    },
    "geo_performance": {
        "resource": "geographic_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "geographic_view.country_criterion_id",
            "geographic_view.location_type",
        ],
        "metrics": _BASE_METRICS,
        "pk": [
            "segments.date",
            "campaign.id",
            "geographic_view.country_criterion_id",
            "geographic_view.location_type",
        ],
        "slice_days": 14,
    },
    "age_range_performance": {
        "resource": "age_range_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "ad_group_criterion.age_range.type",
        ],
        "metrics": _BASE_METRICS,
        "pk": ["segments.date", "ad_group.id", "ad_group_criterion.age_range.type"],
        "slice_days": 14,
    },
    "gender_performance": {
        "resource": "gender_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "ad_group_criterion.gender.type",
        ],
        "metrics": _BASE_METRICS,
        "pk": ["segments.date", "ad_group.id", "ad_group_criterion.gender.type"],
        "slice_days": 14,
    },
}


def _streams() -> list[StreamDefinition]:
    out: list[StreamDefinition] = []
    for name, spec in _ENTITY_STREAMS.items():
        out.append(
            StreamDefinition(
                name=name,
                description=f"Google Ads {name} (entity attributes)",
                json_schema=build_json_schema(spec["select"], []),
                primary_key=spec["pk"],
                grain="entity",
                date_partitioned=False,
                default_cursor_field=None,
                spec=spec,
            )
        )
    for name, spec in _PERF_STREAMS.items():
        out.append(
            StreamDefinition(
                name=name,
                description=f"Google Ads {name.replace('_', ' ')} (daily metrics)",
                json_schema=build_json_schema(spec["dims"], spec["metrics"]),
                primary_key=spec["pk"],
                grain="fact",
                slice_days=spec["slice_days"],
                spec=spec,
            )
        )
    return out


class GoogleAdsConnector(GoogleConnector):
    connector_id = "google_ads"
    name = "Google Ads"
    version = "1.0.0"
    auth_type = AuthType.OAUTH2_WITH_DEVELOPER_TOKEN
    documentation_url = "https://developers.google.com/google-ads/api/docs/start"
    icon = "google-ads"
    required_scopes = (SCOPE_ADWORDS,)
    rate_per_second = 2.0
    STREAMS = _streams()

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        ps = ctx.provider_settings
        self._dev_token = (ps.get("google_ads_developer_token") or "").strip()
        self._api_version = (ps.get("google_ads_api_version") or "v25").strip()
        self._login_customer_id = (
            str(ctx.config.get("login_customer_id") or ps.get("google_ads_login_customer_id") or "")
            .replace("-", "")
            .strip()
        )
        self._currency: str | None = ctx.resource_metadata.get("currency_code")

    @property
    def _base(self) -> str:
        return f"https://googleads.googleapis.com/{self._api_version}"

    async def _auth_headers(self):
        headers = dict(await super()._auth_headers())
        headers["developer-token"] = self._dev_token
        if self._login_customer_id:
            headers["login-customer-id"] = self._login_customer_id
        return headers

    # --- CHECK ---------------------------------------------------------
    async def check_connection(self) -> HealthReport:
        if not self._dev_token:
            return HealthReport(
                status=HealthStatus.INVALID_CONFIGURATION,
                message="Google Ads needs an approved developer token.",
                details={
                    "user_action": (
                        "Set GOOGLE_ADS_DEVELOPER_TOKEN in the environment. Obtain it from your "
                        "Google Ads manager account under Tools → API Center, and complete "
                        "Google's API access application."
                    )
                },
            )
        try:
            accessible = await self.http.get(f"{self._base}/customers:listAccessibleCustomers")
        except E.ConnectorError as exc:
            return health_from_error(exc)

        cid = _digits(self.ctx.resource_id)
        if cid:
            names = {rn.split("/", 1)[-1] for rn in accessible.get("resourceNames", [])}
            reachable_via_mcc = bool(self._login_customer_id)
            if cid not in names and not reachable_via_mcc:
                return HealthReport(
                    status=HealthStatus.PERMISSION_DENIED,
                    message=(
                        f"Customer {cid} is not directly accessible to the connected account. "
                        "If it sits under a manager account, set GOOGLE_ADS_LOGIN_CUSTOMER_ID."
                    ),
                )
            try:
                await self._search(cid, "SELECT customer.id FROM customer LIMIT 1")
            except E.ConnectorError as exc:
                return health_from_error(exc)
        return HealthReport(status=HealthStatus.HEALTHY, message="Connected to Google Ads.")

    # --- DISCOVER -----------------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        if not self._dev_token:
            return []
        accessible = await self.http.get(f"{self._base}/customers:listAccessibleCustomers")
        seen: dict[str, ResourceDescriptor] = {}
        for rn in accessible.get("resourceNames", []):
            root = rn.split("/", 1)[-1]
            query = (
                "SELECT customer_client.id, customer_client.descriptive_name, "
                "customer_client.manager, customer_client.currency_code, "
                "customer_client.time_zone, customer_client.level FROM customer_client"
            )
            try:
                batches = await self._search(root, query)
            except E.ConnectorError:
                # Fall back to just the account itself.
                batches = []
            if not batches:
                seen.setdefault(
                    root,
                    ResourceDescriptor(resource_id=root, name=f"Customer {root}", resource_type="customer"),
                )
                continue
            for row in batches:
                cc = row.get("customerClient", {})
                cid = _digits(str(cc.get("id", "")))
                if not cid:
                    continue
                is_manager = bool(cc.get("manager"))
                seen[cid] = ResourceDescriptor(
                    resource_id=cid,
                    name=cc.get("descriptiveName") or f"Customer {cid}",
                    resource_type="manager" if is_manager else "customer",
                    parent_id=root if cid != root else None,
                    metadata={
                        "currency_code": cc.get("currencyCode"),
                        "time_zone": cc.get("timeZone"),
                        "manager": is_manager,
                        "login_customer_id": root if cid != root else None,
                    },
                    selectable=not is_manager,
                    unsupported_reason=(
                        "Manager (MCC) accounts hold no ad metrics — pick a client account under it."
                        if is_manager
                        else None
                    ),
                )
        return list(seen.values())

    # --- READ -------------------------------------------------------
    async def read_slice(
        self, stream: StreamDefinition, slice_: StreamSlice
    ) -> AsyncIterator[Record | EntityRecord]:
        cid = _digits(self.ctx.resource_id)
        spec = stream.spec
        currency = await self._get_currency(cid)

        if stream.grain == "entity":
            select = ", ".join(spec["select"])
            query = f"SELECT {select} FROM {spec['resource']}"
            for row in await self._search(cid, query):
                flat = _flatten(row)
                yield self._entity(stream, spec, flat)
            return

        dims = spec["dims"]
        select = ", ".join([*dims, *spec["metrics"]])
        start = (slice_.start_date or date.today()).isoformat()
        end = (slice_.end_date or date.today()).isoformat()
        query = f"SELECT {select} FROM {spec['resource']} WHERE segments.date BETWEEN '{start}' AND '{end}'"
        for row in await self._search(cid, query):
            flat = _flatten(row)
            yield self._fact(stream, spec, flat, currency)

    # --- GAQL ---------------------------------------------------------
    async def _search(self, customer_id: str, query: str) -> list[dict[str, Any]]:
        payload = await self.http.post(
            f"{self._base}/customers/{customer_id}/googleAds:searchStream",
            json={"query": query},
        )
        rows: list[dict[str, Any]] = []
        # searchStream returns a JSON array of {results: [...]} batches.
        batches = payload if isinstance(payload, list) else [payload]
        for batch in batches:
            rows.extend(batch.get("results", []))
        return rows

    def _fact(self, stream, spec, flat: dict[str, Any], currency: str | None) -> Record:
        row_date = to_date(flat.get("segments.date"))
        dimensions = {k: flat.get(k) for k in spec["dims"]}
        metrics = {k: to_number(flat.get(k)) for k in spec["metrics"]}
        measures: dict[str, Any] = {}
        for gkey, (col, fn) in _MEASURE_MAP.items():
            if col and flat.get(gkey) is not None:
                measures[col] = fn(flat[gkey])
        key_values = {
            k: (row_date.isoformat() if k == "segments.date" else str(flat.get(k) or ""))
            for k in stream.primary_key
        }
        return Record(
            stream=stream.name,
            key_values=key_values,
            date=row_date,
            dimensions=dimensions,
            metrics=metrics,
            measures=measures,
            currency=currency,
            raw=flat,
        )

    def _entity(self, stream, spec, flat: dict[str, Any]) -> EntityRecord:
        level = spec["level"]
        id_field = spec.get("id_field", f"{level}.id")
        name_field = spec.get("name_field", f"{level}.name")
        status_field = "ad_group_ad.status" if level == "ad_group_ad" else f"{level}.status"
        ext_id = str(flat.get(id_field) or "")
        budget = flat.get("campaign_budget.amount_micros")
        # ad_group_ad hangs off an ad group; ad_group hangs off a campaign.
        parent = {
            "ad_group": flat.get("campaign.id"),
            "ad_group_ad": flat.get("ad_group.id"),
        }.get(level)
        # v25 renamed start_date/end_date to start_date_time/end_date_time.
        # to_date() already strips the time portion so both formats work.
        start_raw = flat.get("campaign.start_date_time") or flat.get("campaign.start_date")
        end_raw = flat.get("campaign.end_date_time") or flat.get("campaign.end_date")
        return EntityRecord(
            stream=stream.name,
            level=level,
            external_id=ext_id,
            name=flat.get(name_field),
            status=flat.get(status_field),
            parent_external_id=str(parent) if parent else None,
            channel=flat.get("campaign.advertising_channel_type"),
            objective=flat.get("ad_group_ad.ad.type") or flat.get("ad_group.type"),
            daily_budget=micros_to_units(budget) if budget is not None else None,
            start_date=to_date((start_raw or "")[:10]),
            end_date=to_date((end_raw or "")[:10]),
            raw=flat,
        )

    async def _get_currency(self, cid: str) -> str | None:
        if self._currency:
            return self._currency
        try:
            rows = await self._search(cid, "SELECT customer.currency_code FROM customer LIMIT 1")
            if rows:
                self._currency = _flatten(rows[0]).get("customer.currency_code")
        except E.ConnectorError:
            self._currency = None
        return self._currency


def _digits(value: str | None) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def _flatten(obj: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """GAQL rows are nested; flatten to snake_case dotted keys matching the query."""
    out: dict[str, Any] = {}
    for key, value in obj.items():
        snake = "".join(f"_{c.lower()}" if c.isupper() else c for c in key)
        path = f"{prefix}.{snake}" if prefix else snake
        if isinstance(value, dict):
            out.update(_flatten(value, path))
        else:
            out[path] = value
    return out


registry.register(
    RegistryEntry(
        connector_class=GoogleAdsConnector,
        requires_settings=("google_client_id", "google_client_secret"),
        prerequisites=(
            "An approved Google Ads API developer token (GOOGLE_ADS_DEVELOPER_TOKEN).",
            "For accounts under a manager (MCC), set GOOGLE_ADS_LOGIN_CUSTOMER_ID.",
            "The connected Google account must have access to the Google Ads customer.",
        ),
        caveats=(
            "Google Ads API access requires completing Google's application; a test "
            "developer token only reaches test accounts.",
        ),
        resource_label="Google Ads Account",
        tags=("ads", "google", "ppc"),
    )
)
