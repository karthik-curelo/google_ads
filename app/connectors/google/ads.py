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
# Conservative metric set for the less-common resources added for screenshot-report
# parity (landing pages, user location, DSA terms, shopping, hourly). The Ads
# connector has no per-metric drop, so one unsupported field fails the whole
# stream — these nine are selectable on every resource we attach them to.
# ponytail: widen per-stream only if a dashboard needs a field that isn't here.
_LEAN_METRICS = [
    "metrics.impressions",
    "metrics.clicks",
    "metrics.cost_micros",
    "metrics.conversions",
    "metrics.conversions_value",
    "metrics.all_conversions",
    "metrics.all_conversions_value",
    "metrics.ctr",
    "metrics.average_cpc",
]
# Asset-level performance (RSA headlines/descriptions/images/videos). Verified
# selectable together on ad_group_ad_asset_view for customer 9232673741.
_ASSET_METRICS = [
    "metrics.impressions",
    "metrics.clicks",
    "metrics.ctr",
    "metrics.cost_micros",
    "metrics.conversions",
    "metrics.conversions_value",
]
# Conversion-action-level performance. Deliberately ONLY these two metrics.
# impressions/clicks/cost_micros are NOT decomposable by conversion action —
# segmenting `campaign` by segments.conversion_action* repeats the *entire*
# day's impressions/clicks/cost on every action row (they describe the
# campaign-day, not the action), so summing them across action rows would
# double- or triple-count spend. conversions/conversions_value ARE correctly
# split per action by Google and were live-verified (2026-09-09, customer
# 9232673741) to reconcile exactly: summed across every conversion_action row
# for a given (date, campaign), they equal campaign-level metrics.conversions
# for that same (date, campaign) — 107/107 pairs matched with zero mismatch
# on a 5-day sample (112 breakdown rows -> 107 aggregate pairs).
_CONVERSION_ACTION_METRICS = [
    "metrics.conversions",
    "metrics.conversions_value",
]
# Auction Insights. These six metrics + segments.auction_insight_domain ARE in
# the v25 schema (campaign / ad_group / keyword_view). On this developer token
# they return HTTP 403 authorizationError=METRIC_ACCESS_DENIED ("the developer
# doesn't have access to metrics: ...") — verified against customer 9232673741,
# 2026-09-09. That is a Google-side access restriction on these specific metrics,
# separate from ordinary API access; it must be requested through Google. The
# streams below carry `permission_optional` so read_slice turns that 403 into a
# clean 0-row success and they start returning data automatically once access is
# granted. They are ratios (0..1 / percentages), never additive, so they stay in
# the metrics JSON and get no promoted column.
_AUCTION_INSIGHT_METRICS = [
    "metrics.auction_insight_search_impression_share",
    "metrics.auction_insight_search_overlap_rate",
    "metrics.auction_insight_search_position_above_rate",
    "metrics.auction_insight_search_outranking_share",
    "metrics.auction_insight_search_top_impression_percentage",
    "metrics.auction_insight_search_absolute_top_impression_percentage",
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
    # --- config / criterion snapshots (screenshot-report parity) ----------
    # "Ad schedule" report: the configured day/hour blocks + their bid modifier.
    # ad_schedule_view has no hour segment, so the schedule grid is rebuilt from
    # these start/end hours crossed with campaign_hourly_performance.
    "ad_schedule_criteria": {
        "grain": "entity",
        "level": "ad_schedule",
        "resource": "campaign_criterion",
        "where": "campaign_criterion.type = 'AD_SCHEDULE'",
        "id_field": "campaign_criterion.criterion_id",
        "ext_id_fields": ["campaign.id", "campaign_criterion.criterion_id"],
        "status_field": "campaign_criterion.status",
        "parent_field": "campaign.id",
        "select": [
            "campaign.id",
            "campaign_criterion.criterion_id",
            "campaign_criterion.status",
            "campaign_criterion.bid_modifier",
            "campaign_criterion.ad_schedule.day_of_week",
            "campaign_criterion.ad_schedule.start_hour",
            "campaign_criterion.ad_schedule.start_minute",
            "campaign_criterion.ad_schedule.end_hour",
            "campaign_criterion.ad_schedule.end_minute",
        ],
        "pk": ["campaign_criterion.criterion_id"],
    },
    # "Advanced bid adjustment" report: every campaign-level criterion carrying a
    # bid_modifier (device, location, ad schedule, audience, …).
    "campaign_bid_modifiers": {
        "grain": "entity",
        "level": "campaign_criterion",
        "resource": "campaign_criterion",
        # Only the criterion types that carry a bid modifier — otherwise this
        # pulls every negative keyword / language / location criterion too (~50k).
        "where": (
            "campaign_criterion.type IN "
            "('DEVICE','LOCATION','AD_SCHEDULE','AGE_RANGE','GENDER',"
            "'INCOME_RANGE','PARENTAL_STATUS')"
        ),
        "id_field": "campaign_criterion.criterion_id",
        "ext_id_fields": ["campaign.id", "campaign_criterion.criterion_id"],
        "status_field": "campaign_criterion.status",
        "parent_field": "campaign.id",
        "select": [
            "campaign.id",
            "campaign_criterion.criterion_id",
            "campaign_criterion.type",
            "campaign_criterion.status",
            "campaign_criterion.bid_modifier",
            "campaign_criterion.device.type",
        ],
        "pk": ["campaign_criterion.criterion_id"],
    },
    # Device bid modifiers set at ad-group level.
    "ad_group_bid_modifiers": {
        "grain": "entity",
        "level": "ad_group_bid_modifier",
        "resource": "ad_group_bid_modifier",
        "id_field": "ad_group_bid_modifier.criterion_id",
        "ext_id_fields": ["ad_group.id", "ad_group_bid_modifier.criterion_id"],
        "parent_field": "ad_group.id",
        "select": [
            "campaign.id",
            "ad_group.id",
            "ad_group_bid_modifier.criterion_id",
            "ad_group_bid_modifier.bid_modifier",
            "ad_group_bid_modifier.device.type",
        ],
        "pk": ["ad_group_bid_modifier.criterion_id"],
    },
    # Performance Max asset groups (empty on Search-only accounts).
    "asset_groups": {
        "grain": "entity",
        "level": "asset_group",
        "resource": "asset_group",
        "id_field": "asset_group.id",
        "name_field": "asset_group.name",
        "status_field": "asset_group.status",
        "parent_field": "campaign.id",
        "select": [
            "campaign.id",
            "asset_group.id",
            "asset_group.name",
            "asset_group.status",
            "asset_group.final_urls",
        ],
        "pk": ["asset_group.id"],
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
    # search_term_view decomposes by whichever keyword segment is requested: a
    # search term can legitimately be triggered by more than one keyword in the
    # same ad group (live-verified 2026-09-09 on customer 9232673741 — 830
    # distinct (search_term, ad_group) pairs map to >1 keyword; e.g. "mri near
    # me" under ad group 194345089574 is triggered by two different keyword
    # criteria). Without segments.keyword.*, Google pre-aggregates those into
    # one row; adding it reveals the true grain (14,539 -> 15,002 rows on a
    # 5-day sample) — so segments.keyword.ad_group_criterion (the stable,
    # always-unique-per-keyword resource name) MUST join the primary key or
    # the newly-revealed rows collapse into each other as duplicates.
    # segments.keyword.info.text/.match_type are descriptive only (derivable
    # from the criterion via keyword_performance) — the resource name is the
    # join key, names are not, per the stable-ID-over-name rule.
    "search_term_performance": {
        "resource": "search_term_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "search_term_view.search_term",
            "search_term_view.status",
            "segments.search_term_match_type",
            "segments.keyword.ad_group_criterion",
            "segments.keyword.info.text",
            "segments.keyword.info.match_type",
        ],
        "metrics": _BASE_METRICS,
        # match_type is part of the natural key — the same query text served under
        # BROAD and PHRASE in one ad group on one day is two distinct rows;
        # without it the second collapses onto the first and is dropped as a
        # within-batch duplicate. keyword.ad_group_criterion is now part of the
        # key for the same reason (see comment above) — a search term matching
        # two different keywords on the same day is two distinct rows.
        "pk": [
            "segments.date",
            "ad_group.id",
            "search_term_view.search_term",
            "segments.search_term_match_type",
            "segments.keyword.ad_group_criterion",
        ],
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
    # --- screenshot-report parity (added 2026-09-09) ----------------------
    # "Landing page report": cost/clicks/conversions per final URL.
    "landing_page_performance": {
        "resource": "landing_page_view",
        "dims": ["segments.date", "campaign.id", "campaign.name", "landing_page_view.unexpanded_final_url"],
        "metrics": _LEAN_METRICS,
        "pk": ["segments.date", "campaign.id", "landing_page_view.unexpanded_final_url"],
        "slice_days": 14,
    },
    # ponytail: expanded_landing_page_view dropped — same numbers as
    # landing_page_view but one row per post-expansion URL variant, which on a
    # high-traffic account is a very large unbounded result for marginal analyst
    # value. Add it back with tiny slice_days if a dashboard genuinely needs the
    # expanded URL grain.
    # "Matched locations report": user's physical/interest location vs. what was
    # targeted (targeting_location flag) — geographic_view is targeted-only.
    "user_location_performance": {
        "resource": "user_location_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "user_location_view.country_criterion_id",
            "user_location_view.targeting_location",
        ],
        "metrics": _LEAN_METRICS,
        "pk": [
            "segments.date",
            "campaign.id",
            "user_location_view.country_criterion_id",
            "user_location_view.targeting_location",
        ],
        "slice_days": 14,
    },
    # "Ad schedule day and hour report": hour-of-day + day-of-week performance.
    "campaign_hourly_performance": {
        "resource": "campaign",
        "dims": ["segments.date", "segments.day_of_week", "segments.hour", "campaign.id", "campaign.name"],
        "metrics": _LEAN_METRICS,
        "pk": ["segments.date", "segments.hour", "campaign.id"],
        "slice_days": 3,
    },
    # "Dynamic ad target report": DSA auto-generated search terms + headline +
    # landing page (search_term_view has none of the DSA attribution fields).
    "dynamic_search_term_performance": {
        "resource": "dynamic_search_ads_search_term_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "dynamic_search_ads_search_term_view.search_term",
            "dynamic_search_ads_search_term_view.headline",
            "dynamic_search_ads_search_term_view.landing_page",
            "dynamic_search_ads_search_term_view.has_negative_keyword",
            "dynamic_search_ads_search_term_view.has_matching_keyword",
        ],
        "metrics": _LEAN_METRICS,
        "pk": [
            "segments.date",
            "ad_group.id",
            "dynamic_search_ads_search_term_view.search_term",
            "dynamic_search_ads_search_term_view.headline",
        ],
        "slice_days": 7,
    },
    # Performance Max search terms (search_term_view excludes PMax).
    "campaign_search_term_performance": {
        "resource": "campaign_search_term_view",
        "dims": ["segments.date", "campaign.id", "campaign_search_term_view.search_term"],
        "metrics": _LEAN_METRICS,
        "pk": ["segments.date", "campaign.id", "campaign_search_term_view.search_term"],
        "slice_days": 7,
    },
    # Shopping / PMax product-level performance (empty on Search-only accounts).
    "shopping_performance": {
        "resource": "shopping_performance_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "segments.product_item_id",
            "segments.product_title",
            "segments.product_brand",
            "segments.product_type_l1",
        ],
        "metrics": _LEAN_METRICS,
        "pk": ["segments.date", "campaign.id", "segments.product_item_id"],
        "slice_days": 7,
    },
    # "Auction insights report" — competitor-domain overlap / impression share.
    # allowlist-gated (see _AUCTION_INSIGHT_METRICS); 0 rows until allowlisted.
    "auction_insight_campaign_performance": {
        "resource": "campaign",
        "dims": ["segments.date", "campaign.id", "campaign.name", "segments.auction_insight_domain"],
        "metrics": _AUCTION_INSIGHT_METRICS,
        "pk": ["segments.date", "campaign.id", "segments.auction_insight_domain"],
        "slice_days": 14,
        "permission_optional": True,
    },
    "auction_insight_ad_group_performance": {
        "resource": "ad_group",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "ad_group.name",
            "segments.auction_insight_domain",
        ],
        "metrics": _AUCTION_INSIGHT_METRICS,
        "pk": ["segments.date", "ad_group.id", "segments.auction_insight_domain"],
        "slice_days": 14,
        "permission_optional": True,
    },
    # "Targeted content report" — where ads actually served on the Display
    # Network / YouTube. Verified live on customer 9232673741 (YouTube channel
    # and video placements from a Video/Demand-Gen campaign). Empty on
    # Search-only accounts, but this account is not Search-only.
    "group_placement_performance": {
        "resource": "group_placement_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "group_placement_view.resource_name",
            "group_placement_view.placement",
            "group_placement_view.display_name",
            "group_placement_view.placement_type",
            "group_placement_view.target_url",
        ],
        "metrics": _LEAN_METRICS,
        # `placement` is null for Google's "unknown/unavailable" bucket, so it
        # cannot be the key on its own. resource_name is always present and
        # unique per placement context.
        "pk": ["segments.date", "group_placement_view.resource_name"],
        # YouTube served-placement data is very long-tail (~35k rows / 2 weeks on
        # this account); small slices bound the per-request buffer in _search.
        "min_impressions": 5,
        "slice_days": 3,
    },
    "detail_placement_performance": {
        "resource": "detail_placement_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "detail_placement_view.resource_name",
            "detail_placement_view.placement",
            "detail_placement_view.display_name",
            "detail_placement_view.placement_type",
            "detail_placement_view.group_placement_target_url",
        ],
        "metrics": _LEAN_METRICS,
        "pk": ["segments.date", "detail_placement_view.resource_name"],
        "min_impressions": 5,
        "slice_days": 3,
    },
    # PMax placements are a SEPARATE resource — group_/detail_placement_view do
    # not include Performance Max. Verified live on customer 9232673741 (PMax
    # YouTube video placements). Google exposes ONLY metrics.impressions here
    # (clicks/cost/conversions are PROHIBITED_METRIC on this resource — it is a
    # brand-safety transparency report, not a performance one).
    "performance_max_placement_performance": {
        "resource": "performance_max_placement_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "performance_max_placement_view.resource_name",
            "performance_max_placement_view.placement",
            "performance_max_placement_view.display_name",
            "performance_max_placement_view.placement_type",
            "performance_max_placement_view.target_url",
        ],
        "metrics": ["metrics.impressions"],
        "pk": ["segments.date", "campaign.id", "performance_max_placement_view.resource_name"],
        # no min_impressions filter here — verified modest volume, and impressions
        # filterability on this transparency-only resource is not guaranteed.
        "slice_days": 3,
    },
    # "Asset-Wise CTR" report, half 1 of 2: per-asset (headline/description/
    # image/video) performance for RSA / Display / Video ads — i.e. every
    # campaign type that has an ad_group_ad. Verified live on customer
    # 9232673741. Does NOT cover Performance Max — PMax has no ad_group_ad, so
    # this view structurally excludes it; see asset_group_asset_performance
    # below for the PMax half of this report.
    "ad_group_ad_asset_performance": {
        "resource": "ad_group_ad_asset_view",
        "dims": [
            "segments.date",
            "campaign.id",
            "ad_group.id",
            "ad_group_ad_asset_view.ad_group_ad",
            "ad_group_ad_asset_view.asset",
            "ad_group_ad_asset_view.field_type",
            "ad_group_ad_asset_view.performance_label",
            "asset.type",
        ],
        "metrics": _ASSET_METRICS,
        "pk": [
            "segments.date",
            "ad_group_ad_asset_view.ad_group_ad",
            "ad_group_ad_asset_view.asset",
            "ad_group_ad_asset_view.field_type",
        ],
        "slice_days": 7,
    },
    # "Asset-Wise CTR" report, half 2 of 2: per-asset performance inside PMax
    # asset groups. Earlier audit pass wrongly claimed PMax per-asset metrics
    # "are not exposed by Google at all" — that was wrong. asset_group_asset
    # carries real metrics.impressions/clicks/ctr/cost_micros/conversions per
    # (date, asset) pair; live-verified 2026-09-09 against customer
    # 9232673741 (7 real PMax asset groups on this account), e.g. one asset:
    # 1,079 impressions / 63 clicks / 3 conversions on 2026-09-01. `resource_
    # name` is always populated and globally unique (asset_group~asset~field_
    # type), so it's the pk component, same fix as the placement views.
    # `performance_label` is NOT a field on this resource (only on
    # ad_group_ad_asset_view) — confirmed via googleAdsFields metadata search.
    "asset_group_asset_performance": {
        "resource": "asset_group_asset",
        "dims": [
            "segments.date",
            "campaign.id",
            "asset_group.id",
            "asset_group.name",
            "asset_group_asset.resource_name",
            "asset_group_asset.asset",
            "asset_group_asset.field_type",
            "asset_group_asset.status",
            "asset.type",
        ],
        "metrics": _ASSET_METRICS,
        "pk": ["segments.date", "asset_group_asset.resource_name"],
        "slice_days": 7,
    },
    # Conversion-action-level performance — a SEPARATE, normalized stream, not
    # a modification of campaign_performance. campaign_performance's
    # conversions/conversion_value stay an aggregate across all conversion
    # actions, unchanged grain, unchanged upsert key, no risk of double-
    # counting there. This stream answers "how many were Purchases vs Leads
    # vs Phone Calls" by adding segments.conversion_action(_name/_category) —
    # confirmed selectable on `campaign` in v25, live-verified 2026-09-09.
    # segments.conversion_action is the RESOURCE NAME (stable ID,
    # customers/X/conversionActions/Y) — Y is the same id already stored in
    # ad_entities (level='conversion_action'), so this joins to that table by
    # id, not by the mutable conversion_action_name string.
    # pk = (date, campaign, conversion_action) — one row per action per
    # campaign per day; see _CONVERSION_ACTION_METRICS for why only
    # conversions/conversions_value are requested here (not impressions/
    # clicks/cost, which are NOT additive across this segment).
    "campaign_conversion_action_performance": {
        "resource": "campaign",
        "dims": [
            "segments.date",
            "campaign.id",
            "campaign.name",
            "segments.conversion_action",
            "segments.conversion_action_name",
            "segments.conversion_action_category",
        ],
        "metrics": _CONVERSION_ACTION_METRICS,
        "pk": ["segments.date", "campaign.id", "segments.conversion_action"],
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
            if spec.get("where"):
                query += f" WHERE {spec['where']}"
            for row in await self._search(cid, query):
                flat = _flatten(row)
                yield self._entity(stream, spec, flat)
            return

        dims = spec["dims"]
        select = ", ".join([*dims, *spec["metrics"]])
        start = (slice_.start_date or date.today()).isoformat()
        end = (slice_.end_date or date.today()).isoformat()
        query = f"SELECT {select} FROM {spec['resource']} WHERE segments.date BETWEEN '{start}' AND '{end}'"
        if spec.get("min_impressions"):
            # Placement views (esp. detail_placement_view = individual YouTube
            # videos) are a huge 1-impression long-tail with no analytical value
            # at that grain. Floor it to placements worth reviewing — the same
            # thing an analyst does in the UI.
            query += f" AND metrics.impressions >= {int(spec['min_impressions'])}"
        try:
            rows = await self._search(cid, query)
        except E.ConnectorError as exc:
            # Allowlist-gated fields (Auction Insights) 403 with
            # METRIC_ACCESS_DENIED on a non-allowlisted token. Treat as an empty
            # result so the stream stays green and self-heals once allowlisted,
            # rather than failing every run.
            if spec.get("permission_optional") and exc.code == E.ErrorCode.PERMISSION_ERROR:
                self.ctx.progress.note(
                    f"{stream.name}: developer token is not allowlisted for these metrics "
                    f"- skipping (0 rows). Request allowlisting via your Google Ads rep."
                )
                return
            raise
        for row in rows:
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
        key_values: dict[str, Any] = {}
        for k in stream.primary_key:
            if k == "segments.date":
                key_values[k] = row_date.isoformat() if row_date else ""
            else:
                v = flat.get(k)
                # `str(v or "")` would turn a legitimate 0 / False (segments.hour
                # midnight, targeting_location=false) into "" and get the row
                # dropped as "missing primary key".
                key_values[k] = "" if v is None else str(v)
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
        status_field = spec.get("status_field") or (
            "ad_group_ad.status" if level == "ad_group_ad" else f"{level}.status"
        )
        # A GAQL criterion_id is only unique *within its campaign / ad group*, so
        # a bare criterion_id collides across parents and rows get deduped away on
        # (connection, level, external_id). `ext_id_fields` joins the parent id in.
        if spec.get("ext_id_fields"):
            ext_id = "~".join(str(flat.get(f) or "") for f in spec["ext_id_fields"])
        else:
            ext_id = str(flat.get(id_field) or "")
        budget = flat.get("campaign_budget.amount_micros")
        # ad_group_ad hangs off an ad group; ad_group hangs off a campaign;
        # criterion/asset-group snapshots name their parent explicitly in spec.
        parent_field = spec.get("parent_field") or {
            "ad_group": "campaign.id",
            "ad_group_ad": "ad_group.id",
        }.get(level)
        parent = flat.get(parent_field) if parent_field else None
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
