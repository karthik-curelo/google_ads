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
| `google_ads_campaigns` | date, sessionGoogleAdsCampaignName, sessionGoogleAdsAdGroupName | advertiserAdCost, advertiserAdClicks, advertiserAdImpressions, sessions, conversions, totalRevenue, returnOnAdSpend |
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
`sessionGoogleAdsKeyword`, `landingPagePlusQueryString`, `fullPageUrl`, `hostName`,
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

### Entity streams (→ `ad_entities`)

| Stream | Resource | Fields selected |
|---|---|---|
| `campaigns` | campaign | campaign.id, name, status, advertising_channel_type, start_date_time, end_date_time, campaign_budget.amount_micros |
| `ad_groups` | ad_group | ad_group.id, name, status, type, cpc_bid_micros, campaign.id, campaign.name |
| `ads` | ad_group_ad | ad_group_ad.ad.id, name, type, status, final_urls, ad_strength, ad_group.id, campaign.id |
| `conversion_actions` | conversion_action | conversion_action.id, name, status, type, category, counting_type, value_settings.default_value |

### Performance streams (daily facts)

| Stream | Resource | Dimensions (beyond segments.date) | Metrics |
|---|---|---|---|
| `campaign_performance` | campaign | campaign.id, campaign.name | base + 3 impression-share |
| `campaign_device_performance` | campaign | campaign.id, campaign.name, segments.device | base |
| `ad_group_performance` | ad_group | campaign.id, ad_group.id, ad_group.name | base + 2 impression-share |
| `ad_performance` | ad_group_ad | campaign.id, ad_group.id, ad.id, ad.name | base |
| `keyword_performance` | keyword_view | campaign.id, ad_group.id, criterion_id, keyword.text, keyword.match_type | base |
| `search_term_performance` | search_term_view | campaign.id, ad_group.id, search_term, search_term_view.status, segments.search_term_match_type | base |
| `geo_performance` | geographic_view | campaign.id, country_criterion_id, location_type | base |
| `age_range_performance` | age_range_view | campaign.id, ad_group.id, age_range.type | base |
| `gender_performance` | gender_view | campaign.id, ad_group.id, gender.type | base |

**Not pulled (Google Ads has it, we don't):** `segments.ad_network_type`,
`segments.conversion_action_name`, `segments.day_of_week`, `segments.click_type`;
`metrics.video_views`, `metrics.interactions`; resources `asset`, `asset_group`
(Performance Max), `shopping_performance_view`, `label`, `change_event`. Coverage
~73%. None of the gaps block standard PPC reporting; add a stream dict in
`_PERF_STREAMS` if Performance Max or Shopping is needed.

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
| Google Ads | 13 | ~73% | Performance Max / Shopping resources, `video_views`, `interactions`, a few segments |
| Meta Ads | 13 | ~92% | per-`action_type` columns, extra action/hourly breakdowns |
| Instagram | 6 | 100% of reference | blocked on App Review, not code |
| Facebook Pages | 4 | 100% of reference | blocked on Page role, not code |

Regenerate the exact diff any time: `python scripts/coverage_scan.py --write` →
updates `GAP_REPORT.md` + the per-source `.yaml` files here.
