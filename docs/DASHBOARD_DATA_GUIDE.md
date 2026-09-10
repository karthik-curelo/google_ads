# Marketing Connector Platform — Data Guide for Dashboards

*Snapshot: 2026-09-09. Use this to build analytics tools / dashboards on top of the warehouse.*

---

## 1. What this platform does

It authorises against marketing APIs (OAuth2), pulls data on a schedule, and lands it
in a Postgres warehouse in a shape that is stable to query. Sources covered:

| Provider | Connector id | What it brings |
|---|---|---|
| Google Analytics 4 | `google_analytics` | sessions / users / engagement / conversions / revenue, by channel, page, geo, device, campaign, event |
| Google Search Console | `google_search_console` | clicks / impressions / CTR / position, by query, page, country, device, hour, search appearance |
| Google Ads | `google_ads` | spend / clicks / conversions / impression share, by campaign, ad group, ad, keyword, search term, age, gender, geo, device |
| Meta Ads | `meta_ads` | spend / clicks / reach / conversions / ROAS / video metrics, by campaign, ad set, ad, and breakdowns (age·gender, platform, country, region, device) |
| Instagram | `instagram_insights` | account & media reach / impressions / views *(schema ready; blocked on Meta App Review — see §9)* |
| Facebook Pages | `facebook_pages` | page & post impressions / reach / clicks *(schema ready; blocked on a Page role grant — see §9)* |

The design rule: **nothing the provider returned is dropped.** Every row keeps the full
provider-native payload as JSON (`dimensions`, `metrics`, `raw`), and a handful of the
most-used measures are *also* copied into typed columns for fast aggregation.

---

## 2. Live connection inventory

| conn id | connector | resource (property / account / site) | status |
|---|---|---|---|
| 1 | google_analytics | `458037317` (GA4 property) | healthy |
| 8 | google_analytics | `318118377` | healthy |
| 9 | google_analytics | `321508872` | healthy |
| 2 | google_search_console | `https://curelohealth.com/` | healthy |
| 7 | google_ads | `9232673741` (customer id) | healthy |
| 4 | meta_ads | `act_1226814221486092` | healthy |
| 5 | meta_ads | `act_1219296233241927` | healthy |
| 6 | meta_ads | `act_1880365855926614` | healthy |
| 10 | instagram_insights | `17841407859929847` (IG business account) | healthy (media only) |

`resource_id` on every warehouse row ties back to this column. Filter by `resource_id`
(the raw provider id) or `connection_id` to scope a dashboard to one property/account.

---

## 3. Storage architecture

**One fact table per source, plus one shared entity table.** There is no single wide
table and no view layer — query the physical tables directly.

| Table | Grain | Rows are | Written from |
|---|---|---|---|
| `google_analytics_performance` | fact (daily) | one metric row per dimension combo per day | `google_analytics` |
| `google_search_console_performance` | fact (daily) | one row per query/page/… per day | `google_search_console` |
| `google_ads_performance` | fact (daily) | one row per campaign/keyword/… per day | `google_ads` |
| `meta_ads_performance` | fact (daily) | one row per ad/breakdown per day | `meta_ads` |
| `instagram_insights_performance` | fact (daily) | one row per metric/media per day | `instagram_insights` |
| `facebook_pages_performance` | fact (daily) | one row per metric/post per day | `facebook_pages` |
| `ad_entities` | entity (SCD) | current attributes of a campaign / ad set / ad / keyword / creative / media / … | every source that has an object tree |
| `skipped_records` | audit | rows that failed validation, with the reason | all |

Each fact table is partitioned logically by the `stream` column — a stream is one
report shape (e.g. `campaign_performance`, `search_analytics_by_query`). Always filter
on `stream` in a dashboard query; different streams have different dimension keys.

---

## 4. Columns every fact table has (`PerformanceRowMixin`)

| Column | Type | Meaning |
|---|---|---|
| `id` | bigint PK | surrogate |
| `organization_id` | bigint | tenant (single tenant today = 1) |
| `connection_id` | bigint | which configured connection produced the row (→ §2) |
| `connector_id` | text | `google_ads`, `meta_ads`, … |
| `provider` | text | `google`, `meta` |
| `stream` | text | report shape — **the main filter** (see per-table lists below) |
| `resource_id` | text | raw provider property/account/site id |
| `record_key` | text(40) | `sha256(stream + sorted primary-key values)[:40]` — the dedup identity |
| `date` | date | the day the metrics describe (null only for non-dated entity-ish streams); **indexed** |
| `dimensions` | jsonb | provider-native dimension values, e.g. `{"query":"...","country":"IND"}` |
| `metrics` | jsonb | provider-native metric values, full precision, provider names |
| `raw` | jsonb | the untouched API row (escape hatch) |
| `schema_version` | int | bump signals a shape change |
| `sync_run_id` | int | the run that last wrote this row (join to `sync_runs`) |
| `source_updated_at` | timestamptz | provider's own last-modified, when given |
| `ingested_at` | timestamptz | first time we saw this row — **frozen**, never bumped by a re-sync |

**Uniqueness:** `UNIQUE(connection_id, stream, record_key)`. Re-syncing a day
*updates* the row in place (measures + JSON overwritten with restated values), never
duplicates it.

**Indexes** on every fact table: `(organization_id, connector_id, date)`,
`(connection_id, stream, date)`, `(sync_run_id)`, `(date)`.

JSON access on Postgres: `dimensions->>'query'`, `(metrics->>'ctr')::numeric`.

---

## 5. Per-table reference

### 5.1 `google_analytics_performance`

Typed measure columns: `sessions`, `users`, `new_users`, `page_views`,
`engaged_sessions`, `bounce_rate`, `engagement_rate`, `event_count`, `conversions`,
`revenue`, `currency`, `avg_session_duration`, `screen_page_views_per_session`,
`user_engagement_duration`.

`metrics` jsonb also carries the raw GA4 names (`activeUsers`, `engagedSessions`,
`screenPageViews`, `userEngagementDuration`, `keyEvents`, `totalRevenue`, …).

| stream | dimension keys (in `dimensions`) | notes |
|---|---|---|
| `daily_overview` | `date` | one row/day, top-line totals |
| `hourly_overview` | `dateHour` | `YYYYMMDDHH`; `date` derived from it |
| `traffic_acquisition` | `date, sessionDefaultChannelGroup, sessionMedium, sessionSource` | |
| `campaign_attribution` | `date, sessionCampaignName, sessionDefaultChannelGroup, sessionMedium, sessionSource` | |
| `first_user_acquisition` | `date, firstUser{CampaignName,DefaultChannelGroup,Medium,Source}` | acquisition (first touch) |
| `google_ads_campaigns` | `date, sessionGoogleAdsCampaignId, sessionGoogleAdsCampaignName, sessionGoogleAdsAdGroupId, sessionGoogleAdsAdGroupName, sessionGoogleAdsKeyword` | GA4's view of Ads; the `*Id` keys join to `google_ads_performance.dimensions->>'campaign.id'` / `'ad_group.id'` |
| `landing_pages` | `date, landingPage, sessionDefaultChannelGroup` | |
| `page_performance` | `date, pagePath` | |
| `page_title` | `date, pageTitle, pagePathPlusQueryString` | largest stream (~32k rows) |
| `events` | `date, eventName` | all events, count |
| `conversions` | `date, eventName` | key-event subset |
| `key_events_by_channel` | `date, eventName, sessionDefaultChannelGroup, sessionMedium, sessionSource` | |
| `geography` | `date, country, region, city` | |
| `device_platform` | `date, deviceCategory, operatingSystem, browser` | |
| `tech_details` | `date, deviceCategory, operatingSystem, browser, platform, language, screenResolution` | |
| `session_quality` | `date, deviceCategory, sessionDefaultChannelGroup` | |
| `new_vs_returning` | `date, newVsReturning` | |
| `custom_report` | config-driven | present only if `config.ga4_custom_dimensions/metrics` set |
| `demographics` / `interests` | `date, userAgeBracket, userGender` / `brandingInterest` | may be sampling-thresholded by GA4 |

### 5.2 `google_search_console_performance`

Typed measure columns: `clicks`, `impressions`, `average_position`.
`metrics` jsonb: `clicks`, `impressions`, `ctr`, `position` (GSC-native).

| stream | dimension keys | notes |
|---|---|---|
| `search_analytics_by_date` | `date` | daily totals |
| `search_analytics_by_query` | `date, query` | ~16k rows |
| `search_analytics_by_page` | `date, page` | |
| `search_analytics_by_page_query` | `date, page, query` | largest (~18k); page × query |
| `search_analytics_by_country` | `date, country` | ISO-3 country code |
| `search_analytics_by_device` | `date, device` | DESKTOP / MOBILE / TABLET |
| `search_analytics_by_hour` | `date, hour` | `hour` is an ISO timestamp; needs `dataState=hourly_all` (handled) |
| `search_analytics_by_appearance` | `date, searchAppearance` | rich-result types |
| `search_analytics_by_query_<type>` etc. | + `search_type` | added per `config.search_types` (web/image/video/news/discover) |
| `sitemaps` | — | lands in `ad_entities` as `level='sitemap'`, not here |

GSC caps result rows (~25–50k/query/day) and drops rows below a privacy threshold —
totals from `by_query` will slightly undercount `by_date`. That is a Search Console
limit, not a pipeline bug.

### 5.3 `google_ads_performance`

Typed measure columns: `impressions`, `clicks`, `cost`, `currency`, `conversions`,
`conversion_value`. `cost` is already converted from micros to currency units.

`metrics` jsonb carries the full GAQL set:
`metrics.cost_micros`, `metrics.average_cpc`, `metrics.average_cpm`, `metrics.ctr`,
`metrics.all_conversions`, `metrics.all_conversions_value`,
`metrics.conversions_from_interactions_rate`, `metrics.cost_per_conversion`,
`metrics.view_through_conversions`, `metrics.search_impression_share`,
`metrics.search_budget_lost_impression_share`,
`metrics.search_rank_lost_impression_share`.

| stream | key dimension keys | notes |
|---|---|---|
| `campaign_performance` | `campaign.id, campaign.name, segments.date` | + impression-share metrics |
| `campaign_device_performance` | `campaign.id, segments.date, segments.device` | |
| `campaign_hourly_performance` | `campaign.id, segments.date, segments.day_of_week, segments.hour` | hour-of-day / day-of-week performance |
| `ad_group_performance` | `ad_group.id, ad_group.name, campaign.id, segments.date` | + impression-share (no budget-lost) |
| `ad_performance` | `ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group.id, campaign.id, segments.date` | |
| `keyword_performance` | `ad_group_criterion.criterion_id, .keyword.text, .keyword.match_type, ad_group.id, campaign.id, segments.date` | |
| `search_term_performance` | `search_term_view.search_term, .status, segments.search_term_match_type, ad_group.id, campaign.id, segments.date` | largest; excludes PMax |
| `campaign_search_term_performance` | `campaign_search_term_view.search_term, campaign.id, segments.date` | Performance Max search terms |
| `dynamic_search_term_performance` | `dynamic_search_ads_search_term_view.search_term, .headline, .landing_page, ad_group.id, campaign.id, segments.date` | DSA auto-targeting |
| `landing_page_performance` | `landing_page_view.unexpanded_final_url, campaign.id, segments.date` | paid landing-page metrics |
| `expanded_landing_page_performance` | `expanded_landing_page_view.expanded_final_url, campaign.id, segments.date` | after URL expansion |
| `age_range_performance` | `ad_group_criterion.age_range.type, ad_group.id, campaign.id, segments.date` | |
| `gender_performance` | `ad_group_criterion.gender.type, ad_group.id, campaign.id, segments.date` | |
| `geo_performance` | `geographic_view.country_criterion_id, .location_type, campaign.id, segments.date` | **targeted** locations |
| `user_location_performance` | `user_location_view.country_criterion_id, .targeting_location, campaign.id, segments.date` | **physical/interest** location; `targeting_location` bool = was it targeted |
| `shopping_performance` | `segments.product_item_id, .product_title, .product_brand, .product_type_l1, campaign.id, segments.date` | Shopping/PMax products (empty on Search-only) |

Config/criterion snapshots go to `ad_entities` (`provider='google'`):
`campaign` / `ad_group` / `ad_group_ad` / `conversion_action` (names, status, budgets),
plus `ad_schedule` (day/hour blocks + bid modifier), `campaign_criterion` &
`ad_group_bid_modifier` (device/location/schedule bid adjustments), `asset_group`
(Performance Max). Join on `dimensions->>'campaign.id' = external_id` where
`level='campaign'`, etc.

### 5.4 `meta_ads_performance`

Typed measure columns: `impressions`, `clicks`, `reach`, `cost`, `currency`,
`conversions`, `conversion_value`. `cost` = Meta `spend`.

`metrics` jsonb is rich: `spend, cpc, cpm, cpp, ctr, frequency, inline_link_clicks,
inline_post_engagement, outbound_clicks, unique_clicks, unique_ctr, website_ctr,
cost_per_unique_click, cost_per_inline_link_click, purchase_roas,
website_purchase_roas, actions, action_values, cost_per_action_type,
video_p25/p50/p75/p100_watched_actions, video_thruplay_watched_actions,
video_30_sec_watched_actions, video_avg_time_watched_actions`.
Polymorphic arrays (`actions`, `purchase_roas`, …) also get a flattened
`<name>__sum` key so you can chart them without unpacking the array.

| stream | key dimension keys | notes |
|---|---|---|
| `campaign_insights` | `campaign_id, campaign_name, date` | |
| `adset_insights` | `adset_id, adset_name, campaign_id, date` | |
| `ad_insights` | `ad_id, ad_name, adset_id, campaign_id, date` | + `quality_ranking`, `engagement_rate_ranking`, `conversion_rate_ranking` in `metrics` |
| `ad_insights_by_age_gender` | `... , age, gender` | largest (~36k) |
| `ad_insights_by_platform` | `... , publisher_platform, platform_position, impression_device` | ~35k |
| `ad_insights_by_country` | `... , country` | |
| `ad_insights_by_region` | `... , region` | |
| `ad_insights_by_device` | `... , device_platform, impression_device` | |

Campaign / ad set / ad / creative attributes → `ad_entities` (`provider='meta'`,
levels `campaign`, `adset`, `ad`, `creative`, `account`, `media`).

### 5.5 `instagram_insights_performance` — *schema live, no rows yet*

Typed columns: `reach`, `impressions`, `views`.
Streams when unblocked: `account_insights`, `media_insights`, `reel_insights`,
`story_insights`, `audience_demographics` (pk `date, breakdown, value`).
Media objects already populate `ad_entities` (`level='media'`, 877 rows).

### 5.6 `facebook_pages_performance` — *schema live, no rows yet*

Typed columns: `impressions`, `reach`, `clicks`.
Streams when unblocked: `page_insights`, `post_insights`. Pages/posts populate
`ad_entities` (`level='page'` / `level='post'`).

---

## 6. `ad_entities` (dimension / lookup table)

One row per object, current attributes (slowly-changing — updated in place).

| Column | Notes |
|---|---|
| `connection_id`, `connector_id`, `provider`, `resource_id` | provenance |
| `level` | `campaign` \| `ad_group` \| `ad` \| `ad_group_ad` \| `keyword` \| `conversion_action` \| `creative` \| `adset` \| `account` \| `media` \| `sitemap` \| `page` \| `post` |
| `external_id` | the provider's id for the object — **join key** |
| `name`, `status` | |
| `parent_external_id` | e.g. an ad group's campaign id |
| `channel`, `objective` | advertising channel type / campaign objective |
| `daily_budget`, `lifetime_budget`, `currency` | |
| `start_date`, `end_date` | |
| `raw` | full object |
| `updated_at` | last change seen |

`UNIQUE(connection_id, level, external_id)`.

Current contents:

| provider | level | rows |
|---|---|---|
| google | campaign | 106 |
| google | ad_group | 445 |
| google | ad_group_ad | 1239 |
| google | conversion_action | 78 |
| google | sitemap | 1 |
| meta | campaign | 66 |
| meta | adset | 275 |
| meta | ad | 874 |
| meta | creative | 10332 |
| meta | account | 3 |
| meta | media | 877 |

Typical join:

```sql
SELECT e.name AS campaign, SUM(f.cost) AS spend, SUM(f.conversions) AS conv
FROM meta_ads_performance f
JOIN ad_entities e
  ON e.connection_id = f.connection_id
 AND e.level = 'campaign'
 AND e.external_id = f.dimensions->>'campaign_id'
WHERE f.stream = 'campaign_insights'
  AND f.date >= CURRENT_DATE - 30
GROUP BY 1 ORDER BY spend DESC;
```

---

## 7. Freshness & correctness guarantees (so a dashboard can trust the numbers)

- **Daily sync at 10:00 IST** on every connection (`config.daily_at = "10:00"`,
  `daily_at_offset_minutes = 330`), fixed wall-clock, no drift. Manual trigger:
  `POST /api/v1/connections/{id}/sync`.
- **No duplicates on re-run** — every write is `INSERT … ON CONFLICT
  (connection_id, stream, record_key) DO UPDATE`. Same source row → same
  `record_key` → same warehouse row.
- **Restatements are captured** — each run re-fetches `cursor − lookback_days`
  (default 3) forward, so the last few days' numbers are *corrected in place* as the
  provider finalises them. Don't cache "yesterday" as immutable; a value can change
  for ~3 days after first landing.
- `ingested_at` = first seen (stable); `sync_run_id` / a fresh query tells you the
  last refresh.
- **Derived ratios (CTR, CPC, CPA, ROAS) are not stored** — compute them on read from
  the stored measures so they never disagree after a restatement. Provider-supplied
  ratio values are still in `metrics` jsonb if you want the provider's own rounding.

Run history: `sync_runs`, `sync_stream_stats` (per-stream row counts / status),
`sync_state` (per-stream cursor), `skipped_records` (validation drops with reason).

---

## 8. How to read the data

**Direct SQL (recommended for dashboards).** Postgres, database `Google_analytics`.
JSON columns are `jsonb`. Examples:

```sql
-- GA4: sessions & conversions by channel, last 28 days, one property
SELECT date,
       dimensions->>'sessionDefaultChannelGroup' AS channel,
       SUM(sessions)    AS sessions,
       SUM(conversions) AS conversions
FROM google_analytics_performance
WHERE stream = 'traffic_acquisition'
  AND resource_id = '458037317'
  AND date >= CURRENT_DATE - 28
GROUP BY 1, 2
ORDER BY 1, sessions DESC;

-- Search Console: top queries by clicks this week
SELECT dimensions->>'query' AS query,
       SUM(clicks) AS clicks, SUM(impressions) AS impr,
       ROUND(100.0*SUM(clicks)/NULLIF(SUM(impressions),0), 2) AS ctr_pct,
       ROUND(AVG(average_position), 1) AS avg_pos
FROM google_search_console_performance
WHERE stream = 'search_analytics_by_query'
  AND date >= CURRENT_DATE - 7
GROUP BY 1 ORDER BY clicks DESC LIMIT 50;

-- Cross-provider paid spend per day (Google Ads + Meta Ads)
SELECT date, 'google_ads' AS source, SUM(cost) AS spend
FROM google_ads_performance  WHERE stream = 'campaign_performance' GROUP BY date
UNION ALL
SELECT date, 'meta_ads', SUM(cost)
FROM meta_ads_performance     WHERE stream = 'campaign_insights'   GROUP BY date
ORDER BY date DESC;

-- Meta Ads: ROAS by campaign, last 30 days
SELECT e.name AS campaign,
       SUM(f.cost) AS spend,
       SUM((f.metrics->>'website_purchase_roas__sum')::numeric) AS roas_sum
FROM meta_ads_performance f
JOIN ad_entities e ON e.connection_id=f.connection_id AND e.level='campaign'
                  AND e.external_id=f.dimensions->>'campaign_id'
WHERE f.stream='campaign_insights' AND f.date >= CURRENT_DATE - 30
GROUP BY 1 ORDER BY spend DESC;
```

**REST API.** `GET /api/v1/connections/{id}/data` returns a flattened tabular view of
the right per-source table for that connection (column set in `COLUMN_CONFIGS`,
`app/api/routers/connections.py`). Auth: `Authorization: Bearer <API_TOKEN>`.
Good for quick tables; for real dashboards go straight to SQL.

---

## 9. Known gaps / gotchas

| Thing | Status |
|---|---|
| `instagram_insights_performance` empty | Needs Meta **App Review** for `instagram_manage_insights` Advanced Access. Connector + schema are done; `media` entities already flow. |
| `facebook_pages_performance` empty | Needs an **Analyst/Editor role** on the FB Page for the connected Meta user. Code-complete. |
| Google Ads & Meta campaign ids are **not joinable** | Different id spaces. "Cross-provider" = `UNION ALL` + rollup on channel, never a shared key. |
| GSC row cap + privacy threshold | `by_query` sums slightly under `by_date`. Provider limit. |
| GA4 sampling / cardinality | high-cardinality dims (`page_title`, demographics) can be sampled or `(other)`-bucketed by GA4 on large properties. |
| `date` can be null | only on non-dated streams; all the dashboard streams above are dated. |
| Numbers move for ~3 days | lookback re-fetch corrects them — expected, not a bug. |
| Ratio columns | not stored; compute from measures (see §7). |

---

## 10. File pointers

- Table definitions: `app/models/warehouse.py`
- Shared column mixin + `make_record_key`: `app/models/warehouse.py` (`PerformanceRowMixin`)
- Connector → table routing, measure-column list: `app/sync/writer.py`
  (`CONNECTOR_MODEL_MAP`, `MEASURE_COLUMNS`)
- Scheduling / incremental / lookback: `app/sync/runner.py`, `app/sync/state.py`,
  `app/sync/scheduler.py`
- Deeper architecture notes: `docs/WAREHOUSE.md`
- Coverage/gap audit: `docs/coverage/GAP_REPORT.md`, `docs/coverage/PLAN.md`
