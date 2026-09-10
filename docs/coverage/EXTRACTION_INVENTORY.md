# Extraction Inventory — every value pulled from each service

*Source of truth: the connector code as of 2026-09-09. Use this to compare against
each provider's full API surface and decide if anything important is missing.*

For the machine-generated capability-vs-implemented diff and coverage %, see
`GAP_REPORT.md` in this folder (regenerate with `python scripts/coverage_scan.py --write`).
Per-provider capability inventories: `_reference/<source>.json`.

**How to read this:**
- **Dimensions** = the group-by keys sent in the request; stored in the row's `dimensions` jsonb.
- **Metrics / fields** = the numeric (or object) values requested; stored in `metrics` jsonb full-precision.
- **Promoted measures** = the subset copied into typed table columns for fast SQL.
- A stream is one report shape. Enable/disable per connection via `connections.streams`.

---

## 1. Google Analytics 4 — `google_analytics`

**API:** Analytics Data API v1beta `runReport`. **Metric cap:** 10 per request.
**Currency:** read once from the property, attached to every row.

**Promoted measure columns** (from `_MEASURE_MAP`): `users` (activeUsers/totalUsers),
`new_users`, `sessions`, `page_views` (screenPageViews), `engaged_sessions`,
`conversions` (conversions/keyEvents), `revenue` (totalRevenue/purchaseRevenue),
`event_count`, `engagement_rate`, `bounce_rate`, `avg_session_duration`,
`screen_page_views_per_session`, `user_engagement_duration`, `cost` (advertiserAdCost),
`clicks` (advertiserAdClicks), `impressions` (advertiserAdImpressions).

| Stream | Dimensions | Metrics requested |
|---|---|---|
| `daily_overview` | date | activeUsers, totalUsers, newUsers, sessions, screenPageViews, engagedSessions, engagementRate, averageSessionDuration, conversions, totalRevenue |
| `hourly_overview` | dateHour | activeUsers, sessions, screenPageViews, engagedSessions, conversions, totalRevenue |
| `traffic_acquisition` | date, sessionDefaultChannelGroup, sessionSource, sessionMedium | sessions, engagedSessions, activeUsers, newUsers, bounceRate, averageSessionDuration, conversions, totalRevenue |
| `campaign_attribution` | date, sessionCampaignName, sessionSource, sessionMedium, sessionDefaultChannelGroup | sessions, activeUsers, newUsers, engagedSessions, conversions, totalRevenue |
| `first_user_acquisition` | date, firstUserSource, firstUserMedium, firstUserCampaignName, firstUserDefaultChannelGroup | newUsers, totalUsers, sessions, engagedSessions, conversions, totalRevenue, transactions, totalPurchasers, firstTimePurchasers |
| `google_ads_campaigns` | date, sessionGoogleAdsCampaignId, sessionGoogleAdsCampaignName, sessionGoogleAdsAdGroupId, sessionGoogleAdsAdGroupName, sessionGoogleAdsKeyword | advertiserAdCost, advertiserAdClicks, advertiserAdImpressions, sessions, conversions, totalRevenue, returnOnAdSpend |
| `landing_pages` | date, landingPage, sessionDefaultChannelGroup | sessions, activeUsers, newUsers, engagedSessions, userEngagementDuration, conversions, totalRevenue |
| `page_performance` | date, pagePath | screenPageViews, activeUsers, engagedSessions, userEngagementDuration |
| `page_title` | date, pageTitle, pagePathPlusQueryString | screenPageViews, activeUsers, engagedSessions, userEngagementDuration |
| `events` | date, eventName | eventCount, activeUsers, totalRevenue |
| `conversions` | date, eventName | conversions, totalRevenue, purchaseRevenue, activeUsers |
| `key_events_by_channel` | date, eventName, sessionDefaultChannelGroup, sessionSource, sessionMedium | eventCount, conversions, totalRevenue, activeUsers |
| `geography` | date, country, region, city | activeUsers, newUsers, sessions, engagedSessions |
| `device_platform` | date, deviceCategory, operatingSystem, browser | activeUsers, sessions, screenPageViews |
| `tech_details` | date, deviceCategory, operatingSystem, browser, screenResolution, language, platform | activeUsers, newUsers, sessions, screenPageViews, engagedSessions |
| `session_quality` | date, sessionDefaultChannelGroup, deviceCategory | sessions, engagedSessions, engagementRate, bounceRate, averageSessionDuration, screenPageViewsPerSession, activeUsers |
| `new_vs_returning` | date, newVsReturning | activeUsers, sessions, engagedSessions, screenPageViews, userEngagementDuration, conversions, totalRevenue, transactions, purchaseRevenue |
| `demographics` | date, userAgeBracket, userGender | activeUsers, newUsers, sessions, engagedSessions, conversions, totalRevenue |
| `interests` | date, brandingInterest | activeUsers, sessions, engagedSessions, conversions |
| `ecommerce` | date, itemName, itemId, itemCategory | itemsViewed, itemsAddedToCart, itemsPurchased, itemRevenue |
| `custom_report` *(only if configured)* | date + `config.ga4_custom_dimensions` | `config.ga4_custom_metrics` (default eventCount) |

**Not pulled (GA4 has it, we don't):** week / hour / year / month / dayOfWeek date
parts; `sessionSourceMedium`, `firstUserSourceMedium`, `sessionCampaignId`,
`landingPagePlusQueryString`, `fullPageUrl`, `hostName`,
`audienceName`, `transactionId`; ratio metrics `sessionsPerUser`,
`eventsPerSession`, `eventCountPerUser`, `userKeyEventRate`, `sessionKeyEventRate`,
`averageRevenuePerUser`, `averagePurchaseRevenue`, `cartToViewRate`,
`purchaserRate`; `customEvent:*` / `customUser:*` are *discovered* via `get_schema`
but only requested if named in connection config. Full list: `GAP_REPORT.md`
(coverage ~58% of the reference surface, most of the gap being low-value date-part
and ratio fields that are derivable on read).

---

## 2. Google Search Console — `google_search_console`

**API:** Search Analytics API v3 `searchAnalytics.query` + `sitemaps.list`.
**Metrics are fixed by the API** — always all four: `clicks`, `impressions`, `ctr`,
`position`. `provider_lag_days=2`, `max_history_days=480`.

**Promoted measure columns:** `clicks`, `impressions`, `average_position` (position).
*(`ctr` stays in `metrics` jsonb — derive it as clicks/impressions.)*

| Stream | Dimensions (group-by) | Notes |
|---|---|---|
| `search_analytics_by_date` | date | daily totals |
| `search_analytics_by_query` | date, query | |
| `search_analytics_by_page` | date, page | |
| `search_analytics_by_page_query` | date, page, query | |
| `search_analytics_by_country` | date, country | ISO-3 code |
| `search_analytics_by_device` | date, device | DESKTOP / MOBILE / TABLET |
| `search_analytics_by_appearance` | searchAppearance *(date is the 1-day slice — GSC won't group it with appearance)* | rich-result types |
| `search_analytics_by_hour` | hour *(ISO timestamp; `dataState=hourly_all`, ~10 days back)* | |
| `search_analytics_by_<type>` *(per `config.search_types`)* | same as above + `search_type` | image / video / news / discover / googleNews |
| `sitemaps` *(entity)* | path, type, lastSubmitted, lastDownloaded, error_count, warning_count | → `ad_entities` level `sitemap` |

**Request options honoured from config:** `data_state` (default `all` — includes
still-settling rows), `search_type` (default `web`), `search_types` (adds the
suffixed streams).

**Not pulled:** nothing material. GSC exposes no other dimensions or metrics on this
API. Coverage 100% of the reference surface. Ceiling is Search Console's own
25,000-rows/request cap and the sub-threshold privacy filtering — `by_query` totals
run slightly under `by_date`.

---

## 3. Google Ads — `google_ads`

**API:** Google Ads API (GAQL) `googleAds:searchStream`, version `v25`.
Two grains: **entity** streams → `ad_entities`; **`*_performance`** → daily facts.

**Base metric set** (`_BASE_METRICS`, on every performance stream):
`impressions`, `clicks`, `cost_micros`, `conversions`, `conversions_value`,
`all_conversions`, `all_conversions_value`, `view_through_conversions`, `ctr`,
`average_cpc`, `average_cpm`, `cost_per_conversion`,
`conversions_from_interactions_rate`.
Campaign streams also add `search_impression_share`,
`search_rank_lost_impression_share`, `search_budget_lost_impression_share`;
ad-group streams add the first two only. *(Impression-share fields are not valid on
ad / keyword / geo / age / gender resources — Google rejects them there.)*

**Promoted measure columns:** `impressions`, `clicks`, `cost` (cost_micros ÷ 1e6),
`conversions`, `conversion_value`. `average_cpc` / `average_cpm` /
`cost_per_conversion` stay in `metrics` jsonb only.

**`_LEAN_METRICS`** (used by the resources added 2026-09-09, where the full base
set risks a single-field rejection): impressions, clicks, cost_micros, conversions,
conversions_value, all_conversions, all_conversions_value, ctr, average_cpc.

### Entity / config-snapshot streams (→ `ad_entities`)

| Stream | Resource | Fields selected |
|---|---|---|
| `campaigns` | campaign | campaign.id, name, status, advertising_channel_type, start_date_time, end_date_time, campaign_budget.amount_micros |
| `ad_groups` | ad_group | ad_group.id, name, status, type, cpc_bid_micros, campaign.id, campaign.name |
| `ads` | ad_group_ad | ad_group_ad.ad.id, name, type, status, final_urls, ad_strength, ad_group.id, campaign.id |
| `conversion_actions` | conversion_action | conversion_action.id, name, status, type, category, counting_type, value_settings.default_value |
| `ad_schedule_criteria` *(new)* | campaign_criterion `WHERE type=AD_SCHEDULE` | campaign.id, criterion_id, status, bid_modifier, ad_schedule.{day_of_week, start_hour, start_minute, end_hour, end_minute} |
| `campaign_bid_modifiers` *(new)* | campaign_criterion | campaign.id, criterion_id, type, status, bid_modifier, device.type |
| `ad_group_bid_modifiers` *(new)* | ad_group_bid_modifier | campaign.id, ad_group.id, criterion_id, bid_modifier, device.type |
| `asset_groups` *(new)* | asset_group | campaign.id, asset_group.id, name, status, final_urls (Performance Max) |

### Performance streams (daily facts)

| Stream | Resource | Dimensions (beyond segments.date) | Metrics |
|---|---|---|---|
| `campaign_performance` | campaign | campaign.id, campaign.name | base + 3 impression-share |
| `campaign_device_performance` | campaign | campaign.id, campaign.name, segments.device | base |
| `campaign_hourly_performance` *(new)* | campaign | segments.day_of_week, segments.hour, campaign.id, campaign.name | lean |
| `ad_group_performance` | ad_group | campaign.id, ad_group.id, ad_group.name | base + 2 impression-share |
| `ad_performance` | ad_group_ad | campaign.id, ad_group.id, ad.id, ad.name | base |
| `keyword_performance` | keyword_view | campaign.id, ad_group.id, criterion_id, keyword.text, keyword.match_type | base |
| `search_term_performance` | search_term_view | campaign.id, ad_group.id, search_term, status, segments.search_term_match_type, **segments.keyword.ad_group_criterion, segments.keyword.info.{text,match_type}** *(added 2026-09-09 — the triggering keyword, see below)* | base |
| `campaign_search_term_performance` *(new)* | campaign_search_term_view | campaign.id, search_term (Performance Max search terms) | lean |
| `dynamic_search_term_performance` *(new)* | dynamic_search_ads_search_term_view | campaign.id, ad_group.id, search_term, headline, landing_page, has_negative_keyword, has_matching_keyword | lean |
| `landing_page_performance` *(new)* | landing_page_view | campaign.id, campaign.name, unexpanded_final_url | lean |
| `geo_performance` | geographic_view | campaign.id, country_criterion_id, location_type (targeted location) | base |
| `user_location_performance` *(new)* | user_location_view | campaign.id, country_criterion_id, targeting_location (physical/interest location) | lean |
| `age_range_performance` | age_range_view | campaign.id, ad_group.id, age_range.type | base |
| `gender_performance` | gender_view | campaign.id, ad_group.id, gender.type | base |
| `shopping_performance` *(new)* | shopping_performance_view | campaign.id, segments.product_item_id, product_title, product_brand, product_type_l1 | lean |
| `group_placement_performance` *(new)* | group_placement_view | campaign.id, group_placement_view.{placement, display_name, placement_type, target_url} — `WHERE metrics.impressions >= 5` (long-tail floor) | lean |
| `detail_placement_performance` *(new)* | detail_placement_view | campaign.id, detail_placement_view.{placement, display_name, placement_type, group_placement_target_url} — `WHERE metrics.impressions >= 5` | lean |
| `performance_max_placement_performance` *(new)* | performance_max_placement_view | campaign.id, resource_name, placement, display_name, placement_type, target_url — **PMax is a separate resource**; Google exposes `metrics.impressions` only here | impressions only |
| `ad_group_ad_asset_performance` *(new)* | ad_group_ad_asset_view | campaign.id, ad_group.id, ad_group_ad, asset, field_type, performance_label, asset.type | impressions, clicks, ctr, cost_micros, conversions, conversions_value |
| `asset_group_asset_performance` *(new)* | asset_group_asset | campaign.id, asset_group.id, asset_group.name, asset_group_asset.resource_name, asset_group_asset.asset, asset_group_asset.field_type, asset_group_asset.status, asset.type — the **PMax half** of Asset-Wise CTR (`ad_group_ad_asset_view` structurally excludes PMax) | impressions, clicks, ctr, cost_micros, conversions, conversions_value |
| `auction_insight_campaign_performance` *(new, permission-gated)* | campaign | campaign.id, campaign.name, segments.auction_insight_domain | 6× `metrics.auction_insight_search_*` (ratios, JSON only) |
| `auction_insight_ad_group_performance` *(new, permission-gated)* | ad_group | campaign.id, ad_group.id, ad_group.name, segments.auction_insight_domain | 6× `metrics.auction_insight_search_*` |
| `campaign_conversion_action_performance` *(new 2026-09-09)* | campaign | campaign.id, campaign.name, **segments.conversion_action** (stable ID), segments.conversion_action_name, segments.conversion_action_category | **only** `metrics.conversions`, `metrics.conversions_value` — deliberately excludes impressions/clicks/cost, which are not additive across this segment |

### Search-term → triggering-keyword (added 2026-09-09)

`search_term_performance` now requests `segments.keyword.ad_group_criterion` /
`.info.text` / `.info.match_type` alongside the existing search-term fields.
Live-verified on customer 9232673741: the same search term in the same ad
group can legitimately be triggered by **more than one keyword** (4,634
distinct `(search_term, ad_group)` pairs do this in the current 33-day
backfill; e.g. "mri near me" under one ad group is triggered by two separate
keyword criteria). Without the keyword segment, Google pre-aggregates these
into a single row; requesting it decomposes them correctly (100,837 rows vs.
~97,300 under the old grain for the same window). `segments.keyword.
ad_group_criterion` (the resource name, format `adGroupCriteria/<ad_group_id>
~<criterion_id>`) therefore joined the primary key — it is always populated
on this account (0/100,837 null) but the mapping code preserves an empty-
string fallback for the case where Google omits the segment, so a future
unresolved-keyword row cannot silently collapse onto a resolved one.

### Conversion-action-level performance (added 2026-09-09)

A **separate, normalized** stream — `campaign_performance`'s own
`conversions`/`conversion_value` are untouched (still an aggregate, same
grain, same upsert key, zero regression risk). `campaign_conversion_action_
performance` adds `segments.conversion_action(_name/_category)` to answer
"how many conversions were Purchases vs. Leads vs. Phone Calls", joined to
the existing `conversion_actions` entity table (`ad_entities`, level=
`conversion_action`, 78 definitions) by the **stable numeric ID** embedded in
`segments.conversion_action`'s resource name — not by the mutable action
name. Metrics are deliberately limited to `conversions`/`conversions_value`:
`impressions`/`clicks`/`cost_micros` are **not** decomposable by conversion
action (Google repeats the whole campaign-day's value on every action row),
so including them here would silently double/triple-count spend on any
campaign-day with more than one active conversion action. Live-verified
reconciliation against `campaign_performance`'s aggregate: 99/102 (date,
campaign) pairs match exactly; the 3 that don't are the 2 most recent days
(today + yesterday) with small deltas (<3 conversions), consistent with
Google's normal conversion-attribution settling lag — not a bug, and it
self-corrects on the next lookback re-fetch like every other still-settling
metric on this platform. Value reconciled 102/102 exactly.

**Maps to the Google Ads UI reports:** Campaign / Ad group / Ad / Search keyword /
Search terms → the matching perf streams. Ad schedule + day/hour →
`ad_schedule_criteria` × `campaign_hourly_performance`. Matched locations →
`user_location_performance`. Landing page → `landing_page_performance`. Dynamic ad
target → `dynamic_search_term_performance`. Advanced bid adjustment →
`campaign_bid_modifiers` + `ad_group_bid_modifiers`. Asset / PMax → `asset_groups`,
`campaign_search_term_performance`, `shopping_performance`. **Asset-Wise CTR** →
`ad_group_ad_asset_performance` (Search/Display/Video) + `asset_group_asset_performance`
(PMax). **Targeted content / placements** →
`group_placement_performance` + `detail_placement_performance` (Display/Video) +
`performance_max_placement_performance` (PMax — separate resource); all
live-verified with YouTube placement data on customer 9232673741. **Auction
Insights** → `auction_insight_campaign_performance` / `_ad_group_performance`.

**Auction Insights — the exact status.** `segments.auction_insight_domain` and the
six `metrics.auction_insight_search_*` fields **are** in the v25 schema
(selectable on `campaign`, `ad_group`, `keyword_view`). On this developer token
they return `HTTP 403 authorizationError = METRIC_ACCESS_DENIED` ("the developer
doesn't have access to metrics: …") — verified against customer 9232673741,
2026-09-09. That is a Google-side access restriction on these specific metrics,
requested through Google; it is **not** an empty result. *(No claim is made here
about whether that access programme is open or closed — only the 403 is verified
first-hand.)* The two streams are implemented and carry `permission_optional`:
the 403 is caught → clean **0-row success**, and they begin returning data the
moment the token is granted access. So this is **permission-gated**, like
Instagram `instagram_manage_insights` — *not* "no API resource".

**PMax per-asset metrics — corrected.** An earlier pass of this document
claimed Google does not expose per-asset impressions/clicks for Performance
Max. That was wrong: `ad_group_ad_asset_performance` (resource
`ad_group_ad_asset_view`) covers Search/Display/Video RSA assets, but PMax has
no `ad_group_ad` at all, so that view structurally can't include it — a
**different** resource, `asset_group_asset`, carries real per-asset metrics
(impressions/clicks/ctr/cost/conversions) for assets inside PMax asset
groups. Live-verified 2026-09-09 against customer 9232673741 (which runs 7
real PMax asset groups): implemented as `asset_group_asset_performance`,
2,205 rows, 0 skips. `asset_group_asset` has no `performance_label` field
(that's `ad_group_ad_asset_view`-only) — the coarser
`asset_group_top_combination_view` combinations report remains a separate,
still-not-pulled, lower-priority resource (see "Still not pulled" below).

**Still not pulled:** `segments.ad_network_type` / `segments.click_type`;
`metrics.video_views` / `metrics.interactions`; `managed_placement_view` (thin —
only `resource_name`; 0 rows here; the group/detail placement views carry the
data); `topic_view` (Display topic targeting — not used by this account);
`change_event` (change history — **verified working with real data**, but no
change-history report in the screenshot scope → P2, ready-to-drop spec in
`GAP_REPORT_ADDENDUM.md`); `label`; `asset_group_top_combination_view` (the
Combinations report — which asset *combinations* Google served together —
distinct from and coarser than the per-asset metrics `asset_group_asset_performance`
already covers; not in the screenshot scope).

---

## 4. Meta Ads — `meta_ads`

**API:** Graph / Marketing API `/act_<id>/insights` (`time_increment=1`) + entity edges.
**Attribution windows:** `config.action_attribution_windows` passed through when set.
**Conversion action types counted** into `conversions` / `conversion_value`
(`config.conversion_action_types`, default): `offsite_conversion.fb_pixel_purchase`,
`purchase`, `omni_purchase`, `lead`, `offsite_conversion.fb_pixel_lead`,
`complete_registration`.

**Insight fields requested on every insights stream** (`_INSIGHT_FIELDS`):
impressions, clicks, spend, reach, frequency, cpc, cpm, cpp, ctr,
inline_link_clicks, inline_post_engagement, unique_clicks, unique_ctr,
outbound_clicks, unique_outbound_clicks, cost_per_inline_link_click,
cost_per_unique_click, website_ctr, **actions**, **action_values**,
cost_per_action_type, purchase_roas, website_purchase_roas,
video_thruplay_watched_actions, video_p25/p50/p75/p100_watched_actions,
video_30_sec_watched_actions, video_avg_time_watched_actions.
Array-typed fields are kept verbatim **and** flattened to `<field>__sum`.
`ad_insights` additionally requests `quality_ranking`, `engagement_rate_ranking`,
`conversion_rate_ranking` (ad-level only).

**Promoted measure columns:** `impressions`, `clicks`, `cost` (spend), `reach`,
`conversions` (summed from `actions`), `conversion_value` (summed from `action_values`).

### Insight streams (daily facts)

| Stream | Level | Dimensions | Breakdowns |
|---|---|---|---|
| `campaign_insights` | campaign | date, campaign_id, campaign_name | — |
| `adset_insights` | adset | date, adset_id, adset_name, campaign_id | — |
| `ad_insights` | ad | date, ad_id, ad_name, adset_id, campaign_id | — |
| `ad_insights_by_age_gender` | ad | + age, gender | age, gender |
| `ad_insights_by_platform` | ad | + publisher_platform, platform_position, impression_device | publisher_platform, platform_position, impression_device |
| `ad_insights_by_country` | ad | + country | country |
| `ad_insights_by_region` | ad | + region | region |
| `ad_insights_by_device` | ad | + impression_device, device_platform | impression_device, device_platform |

### Entity streams (→ `ad_entities`)

| Stream | Node / edge | Fields |
|---|---|---|
| `campaigns` | /campaigns | id, name, status, effective_status, objective, buying_type, bid_strategy, daily_budget, lifetime_budget, budget_remaining, start_time, stop_time |
| `adsets` | /adsets | id, name, status, effective_status, campaign_id, optimization_goal, billing_event, bid_amount, daily_budget, lifetime_budget, start_time, end_time |
| `ads` | /ads | id, name, status, effective_status, adset_id, campaign_id, creative |
| `ad_creatives` | /adcreatives | id, name, title, body, image_url, thumbnail_url, video_id, call_to_action_type, object_story_id, effective_object_story_id, instagram_permalink_url |
| `adaccounts` | account node | id, account_id, name, currency, account_status, disable_reason, spend_cap, amount_spent, balance, timezone_name, business_name, funding_source |

**Not pulled / partial:** `actions[]` is kept raw and summed, **not** split into a
column per `action_type` (so e.g. "add-to-cart" vs "purchase" needs a jsonb unpack);
`action_breakdowns` `action_destination` / `action_target_id` not requested;
`hourly_stats_aggregated_by_advertiser_time_zone` breakdown not implemented.
Coverage ~92%.

---

## 5. Instagram — `instagram_insights`  *(schema live; account/media insights blocked on Meta App Review)*

**API:** Instagram Graph API `/insights` + `/media`. `max_history_days=30`,
`provider_lag_days=1`. Unavailable metrics are dropped at runtime per Graph version.

**Promoted measure columns:** `reach`, `views`. *(`impressions` column exists but
Instagram removed the metric in Graph v22.)*

| Stream | Grain | Dimensions | Metrics requested |
|---|---|---|---|
| `account_insights` | fact (daily) | date | reach, follower_count, profile_views, website_clicks, accounts_engaged, total_interactions, views, profile_links_taps *(overridable via `config.account_metrics`)* |
| `media` | entity | id, media_type, media_product_type, timestamp, permalink | *(caption, like_count, comments_count in raw)* → `ad_entities` level `media` |
| `media_insights` | fact | media_id, media_type | reach, saved, likes, comments, shares, total_interactions, views |
| `reel_insights` | fact | media_id | reach, likes, comments, shares, saved, total_interactions, plays, ig_reels_avg_watch_time, ig_reels_video_view_total_time, clips_replays_count |
| `story_insights` | fact | media_id | reach, replies, navigation, total_interactions |
| `audience_demographics` | fact | date, breakdown, value | follower_demographics by age / gender / city / country (lifetime `total_value`) |

**Not pulled:** the reference surface is fully covered (100%). The real gap is
**access**, not fields: `account_insights` returns `(#10)` for every metric until the
Meta app has Advanced Access to `instagram_manage_insights` (App Review). `media`
and `media_insights` work today.

---

## 6. Facebook Pages — `facebook_pages`  *(schema live; blocked on a Page role grant)*

**API:** Pages API + Page Insights, using a **Page access token** resolved from
`/me/accounts`. `provider_lag_days=1`. Unavailable metrics dropped per version.

**Promoted measure columns:** `impressions`, `reach`, `clicks`.

| Stream | Grain | Dimensions | Metrics / fields requested |
|---|---|---|---|
| `pages` | entity | id, name, category, about, fan_count, followers_count, link, verification_status, talking_about_count, were_here_count | → `ad_entities` level `page` |
| `posts` | entity | id, message, created_time, permalink_url, status_type, is_published, shares, reactions.summary, comments.summary | → `ad_entities` level `post` |
| `page_insights` | fact (daily) | date | page_impressions, page_impressions_unique, page_post_engagements, page_fans, page_fan_adds, page_fan_removes, page_views_total, page_video_views |
| `post_insights` | fact | post_id | post_impressions, post_impressions_unique, post_clicks, post_reactions_by_type_total, post_video_views |

**Not pulled:** covered 100% of the reference surface. Blocked by: no `ANALYZE`
task on the Page for the connected Meta user (grant Analyst/Editor), and
`read_insights` / `pages_read_engagement` need App Review outside dev mode.

---

## 7. Shared entity attributes (`ad_entities`) — what we keep per object

For every `campaign` / `ad_group` / `adset` / `ad` / `keyword` / `creative` /
`conversion_action` / `media` / `page` / `post` / `sitemap`:

`name`, `status`, `parent_external_id`, `channel` (advertising channel type),
`objective`, `daily_budget`, `lifetime_budget`, `currency`, `start_date`,
`end_date`, and the full provider object in `raw`. Updated in place on every sync.

---

## 8. Summary — coverage vs each provider's full surface

| Service | Streams | Coverage vs reference | Main deliberate omissions |
|---|---|---|---|
| Google Analytics 4 | 20 (+1 configurable) | ~58% | date-part dims, ratio metrics (derivable), un-configured custom dims/metrics |
| Google Search Console | 8 (+per search-type) | 100% | none (API has no more) |
| Google Ads | 31 | ~96% | Auction Insights (access-restricted, implemented+dormant), `change_event` (P2), `managed_placement_view` (thin), `topic_view`, `asset_group_top_combination_view`, a few diagnostic segments |
| Meta Ads | 13 | ~92% | per-`action_type` child table, extra action/hourly breakdowns |
| Instagram | 6 | 100% of reference | blocked on App Review, not code |
| Facebook Pages | 4 | 100% of reference | blocked on Page role, not code |

Regenerate the exact diff any time: `python scripts/coverage_scan.py --write` →
updates `GAP_REPORT.md` + the per-source `.yaml` files here.
