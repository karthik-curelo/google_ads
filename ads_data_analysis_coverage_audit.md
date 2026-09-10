# Ads & Analytics Data Coverage Audit

**Audit date:** 2026-09-09  
**Scope:** Google Ads, Meta Ads, Google Analytics 4 (GA4), Google Search Console (GSC)  
**Objective:** Verify whether the currently extracted API data is sufficient to reproduce the analysis/report types represented in the supplied Google Ads export screenshot and to support reliable cross-channel performance analysis.

> **Update (2026-09-09, same day): most P0 Google Ads gaps are now implemented and
> live-verified** on customer `9232673741`. New streams in
> `app/connectors/google/ads.py` (13 → 23):
> `landing_page_performance`, `user_location_performance` (matched vs targeted),
> `campaign_hourly_performance` (`segments.hour` + `segments.day_of_week`),
> `ad_schedule_criteria`, `campaign_bid_modifiers`, `ad_group_bid_modifiers`,
> `dynamic_search_term_performance`, `campaign_search_term_performance` (PMax),
> `asset_groups`, `shopping_performance`. GA4 `google_ads_campaigns` now carries
> `sessionGoogleAdsCampaignId` / `AdGroupId` / `Keyword` for ID-based joins (§4.1);
> Meta rows now stamp `attribution_windows` into `dimensions` (§5.4).
> **Still open, deliberately:** Auction Insights (no Google Ads API resource —
> platform ceiling, §2.5); content/placement views (Display/Video only, this
> account is Search); `change_event`; the Meta per-`action_type` child fact table
> (§5.1 — new table + migration, scheduled as its own change). Current field-level
> list: `docs/coverage/EXTRACTION_INVENTORY.md` §3; machine diff:
> `docs/coverage/GAP_REPORT.md`.

## Executive conclusion

The current extraction is a strong foundation, but it is **not yet complete for analysis parity** with the report types visible in the screenshot.

The internal inventory reports high API-surface coverage for GSC (100%), Meta Ads (~92%), and lower coverage for Google Ads (~73%) and GA4 (~58%). Those percentages should **not** be treated as analysis readiness scores. Several high-value Google Ads reporting capabilities visible in the screenshot are outside the current implemented streams even though the Google Ads API supports them.

### Highest-priority gaps

1. **Google Ads Auction Insights** — not currently extracted as a dedicated report despite the API exposing Auction Insights metrics and competitor-domain segmentation.
2. **Google Ads Ad Schedule / day-of-week / hour** — the API supports both the configured schedule criteria and a performance view, but the current extractor has neither a schedule-performance stream nor schedule criterion/bid-modifier data.
3. **Google Ads location targeting vs matched/user location** — the current `geographic_view`-style extraction is useful, but it is not a complete replacement for `location_view`, `user_location_view`, or `matched_location_interest_view`.
4. **Google Ads bid adjustments** — campaign/ad-group criteria and bid modifiers are not currently extracted, so the system cannot explain *why* a bid was raised/lowered by device, audience/criterion, location, schedule, etc.
5. **Google Ads Dynamic Search Ads / dynamic ad target reporting** — `dynamic_search_ads_search_term_view` and webpage-target criterion attributes are not currently extracted.
6. **Google Ads content / placement analysis** — managed, automatic/group, and detail placement views are not currently extracted.
7. **Google Ads Performance Max / Shopping product-level analysis** — `asset_group`, asset relationships, and `shopping_performance_view` are missing. This is a major analytical blind spot for accounts using PMax or Shopping.
8. **GA4 paid-search attribution joins** — the extractor currently uses campaign/source/medium names, but GA4 exposes session-level Google Ads IDs, ad-group IDs, creative IDs, query and keyword fields that should be captured to create robust joins to Google Ads.
9. **Meta Ads action normalization and hourly breakdowns** — raw `actions[]` are preserved, but per-action-type normalization and action breakdowns are incomplete; hourly advertiser/audience time-zone breakdowns are also missing.
10. **GSC long-tail completeness** — the API does not guarantee all rows and has a 25,000-row request ceiling, so the data model should explicitly distinguish API completeness from true Search Console completeness.

---

# 1. What is already implemented

The connector inventory shows:

- **GA4:** 20 report streams plus one configurable stream, with acquisition, campaigns, landing pages, pages, events/conversions, geography, device/technology, demographics, interests and ecommerce coverage.
- **GSC:** date/query/page/page+query/country/device/search appearance/hour streams plus configurable search types and sitemaps.
- **Google Ads:** campaign, ad group, ad and conversion-action entities; daily performance for campaigns, ad groups, ads, keywords, search terms, geo, age and gender.
- **Meta Ads:** campaign/ad set/ad insight streams plus age/gender, platform/placement, country, region and device breakdowns; campaign/ad set/ad/creative/account entities.

The supplied extraction inventory documents the exact current fields and deliberate omissions.  

The current machine-generated gap report gives approximately **58% GA4, 73% Google Ads, 100% GSC, 92% Meta Ads, 100% Instagram and 100% Facebook Pages** against the reference surfaces used by the project.

These figures are useful as engineering coverage indicators, but they do not capture all report-level requirements visible in the screenshot.

---

# 2. Screenshot report-by-report audit

The screenshot contains report/export types including landing-page, ad-schedule, matched-location, search-term, auction-insights, time-series, bid-adjustment, dynamic-ad-target, targeted-content, location, keyword, ad, ad-group and campaign reporting.

## 2.1 Landing Page Report — PARTIAL

### Current coverage
GA4 already has a `landing_pages` stream with date, landing page and channel-group dimensions plus sessions, users, engagement, conversions and revenue.

### Missing / recommended
For deeper landing-page analysis, add:

- GA4 `landingPagePlusQueryString`
- GA4 `fullPageUrl`
- GA4 `pageReferrer`
- Google Ads `expanded_landing_page_view`
- Google Ads final/expanded URL performance where supported

GA4's current API schema exposes `fullPageUrl`, and the API also supports session Google Ads attribution dimensions and other landing-page-related dimensions.

### Why this matters
The current landing-page stream is sufficient for a standard GA4 landing-page report, but it is not sufficient for a complete **paid-media landing-page diagnostic** because query-string variants, referrers and Google Ads URL-level performance are not all represented.

**Priority: P1**

---

## 2.2 Ad Schedule Day & Hour Report — MISSING

### Current coverage
Google Ads daily campaign performance is collected, but the current performance streams do not collect `segments.day_of_week` or `segments.hour`. The current entity inventory also does not collect ad-schedule criteria.

### API capability verified
Google Ads API v25 exposes:

- `ad_schedule_view` for campaign performance by AdSchedule criteria.
- `campaign_criterion.ad_schedule.day_of_week`
- `campaign_criterion.ad_schedule.start_hour`
- `campaign_criterion.ad_schedule.end_hour`
- start/end minutes.

### Required extraction
Create at least two related datasets:

**A. Configured schedule table**
- customer_id
- campaign_id
- criterion_id
- day_of_week
- start_hour/start_minute
- end_hour/end_minute
- bid_modifier
- enabled/negative state where applicable

**B. Schedule performance fact**
- date
- campaign_id
- day_of_week
- hour
- impressions
- clicks
- cost
- conversions
- conversion_value
- CTR
- CPC
- CPA
- conversion rate / value-per-cost where supported

**Priority: P0**

---

## 2.3 Matched Locations Report — PARTIAL / FUNCTIONALLY INCOMPLETE

### Current coverage
The current Google Ads `geo_performance` stream uses `geographic_view`-style data with `country_criterion_id` and `location_type`.

### API distinction that matters
Google Ads exposes different concepts:

- `geographic_view`: country-level metrics that can reflect physical location or area of interest.
- `user_location_view`: actual physical user location and a flag indicating whether that location was targeted.
- `location_view`: performance by a campaign's location criterion.
- `matched_location_interest_view`: locations where users showed interest that matched location-interest targeting; the current v25 documentation states this is currently available for AI Max campaigns.

### Required extraction
Add:

- `location_view`
- `user_location_view`
- `matched_location_interest_view` when the relevant campaign types are present
- the underlying `campaign_criterion` location-target configuration

### Why this matters
Without these streams, analysis can confuse **where the advertiser targeted**, **where the user physically was**, and **where the user showed interest**. Those are different analytical questions.

**Priority: P0**

---

## 2.4 Search Terms Report — PARTIAL

### Current coverage
The current Google Ads extractor has `search_term_performance` with campaign, ad-group, search term, status and match-type-related data.

### Important API limitation
Google documents that `search_term_view` does **not include Performance Max data**. For Performance Max search-term data, Google says to use `campaign_search_term_view`.

### Required extraction
Keep the current search-term stream and add:

- `campaign_search_term_view` for Performance Max
- dynamic-search-term stream for DSA where applicable
- conversion-action segmentation where useful
- day-of-week/hour/device/network segments for diagnostic analysis

**Priority: P0 for accounts using PMax; P1 otherwise**

---

## 2.5 Auction Insights Report — MISSING

### Current coverage
Not present as a dedicated performance stream in the implementation inventory.

### API capability verified
Google Ads v25 exposes Auction Insights through `segments.auction_insight_domain` and metrics including:

- `auction_insight_search_impression_share`
- `auction_insight_search_overlap_rate`
- `auction_insight_search_position_above_rate`
- `auction_insight_search_outranking_share`
- `auction_insight_search_top_impression_percentage`
- `auction_insight_search_absolute_top_impression_percentage`

Google's field documentation states these are Auction Insights metrics and identifies the participant domain as `segments.auction_insight_domain`.

### Required extraction
Create dedicated Auction Insights facts at campaign and/or ad-group/keyword grain, at minimum:

- date
- campaign_id / ad_group_id / keyword where supported
- auction_insight_domain
- impression_share
- overlap_rate
- position_above_rate
- outranking_share
- top_impression_percentage
- absolute_top_impression_percentage

### Why this matters
The screenshot explicitly contains an Auction Insights report. This is a **direct analysis gap**, not merely a theoretical API omission.

**Priority: P0**

---

## 2.6 Time-Series Chart — PARTIAL

### Current coverage
Daily time-series data exists across several Google Ads and GA4 streams.

### Missing for a more complete charting layer
- Google Ads hourly performance (`segments.hour`)
- Google Ads day-of-week segments
- Meta hourly performance
- Meta advertiser-time-zone and audience-time-zone hourly breakdowns

### Important distinction
Daily trend charts are already supported. Hourly trend analysis is not consistently supported across all four sources.

**Priority: P1**

---

## 2.7 Advanced Bid Adjustment Report — MISSING

### Current coverage
Age/gender/device performance is collected, but the actual criterion/bid-modifier configuration is not.

### API capability verified
Google Ads v25 exposes `campaign_criterion` and `ad_group_criterion`, including `bid_modifier`. Ad-group criteria include age, location, placement, topic, webpage and other targeting criteria.

### Required extraction
At minimum:

- campaign-level criteria and bid modifiers
- ad-group criteria and bid modifiers
- device bid modifiers
- location bid modifiers
- age/gender bid modifiers
- ad-schedule bid modifiers
- placement/topic/audience bid modifiers where applicable
- base bid fields (`cpc_bid_micros`, `cpm_bid_micros`, etc.)

The `ad_group_bid_modifier` resource should also be evaluated for device modifier analysis.

### Why this matters
Performance tells you **what happened**; bid-modifier data tells you **what targeting adjustment was configured**. You need both to explain optimization decisions.

**Priority: P0**

---

## 2.8 Dynamic Ad Target Report — MISSING

### Current coverage
No dedicated Dynamic Search Ads target stream is listed.

### API capability verified
Google Ads exposes `dynamic_search_ads_search_term_view`, including:

- search term
- dynamically generated headline
- dynamically selected landing page
- page-feed URL
- matching/negative keyword flags
- matching/negative URL flags

Google also exposes webpage criteria under `ad_group_criterion.webpage`, including targeting conditions, criterion name, coverage percentage and sample URLs.

### Required extraction
Add:

- `dynamic_search_ads_search_term_view`
- `ad_group_criterion` webpage criteria
- webpage condition payload
- criterion name
- coverage percentage
- sample URLs
- DSA search term, headline and selected landing page

**Priority: P0 for DSA accounts; P1 otherwise**

---

## 2.9 Targeted Content / Placement Report — MISSING

### Current coverage
No dedicated content/placement streams are listed.

### API capability verified
Google Ads provides multiple placement views:

- `managed_placement_view` — explicitly targeted placements.
- `group_placement_view` — where ads actually served, including targeted and automatic placements.
- `detail_placement_view` — more detailed placement analysis.

Google's documentation maps `managed_placement_view` to the Placements section of the Google Ads UI.

### Required extraction
For relevant Display/YouTube campaigns add:

- managed placements
- automatic/group placements
- detailed placements
- placement URL/channel/video/app identifiers
- impressions/clicks/cost/conversions/value
- device/network where compatible

Also consider `topic_view`, `display_keyword_view`, `group_content_suitability_placement_view`, audience views and related content-targeting resources when those campaign types are present.

**Priority: P1 for Display/YouTube; P2 for Search-only accounts**

---

## 2.10 Location Report — PARTIAL

The current geographic performance stream is useful, but a complete location-reporting layer should separate:

1. **Configured target locations** (`campaign_criterion` / `location_view`).
2. **Actual user physical location** (`user_location_view`).
3. **Location/area of interest** (`geographic_view`, and `matched_location_interest_view` where supported).

**Priority: P0**

---

## 2.11 Search Keyword Report — MOSTLY COVERED

The current `keyword_performance` stream contains campaign, ad-group, criterion ID, keyword text and match type plus performance metrics.

### Recommended additions
Add:

- keyword status/approval information from `ad_group_criterion`
- keyword-level bid / CPC bid fields
- Quality Score-related analysis where available/appropriate
- search impression share / rank-lost/budget-lost metrics where Google permits the resource
- day/hour/device/network/match-source segments for diagnostic analysis

**Priority: P1**

---

## 2.12 Ad Report — MOSTLY COVERED

The current `ad_performance` and ad entity streams cover the core ad reporting need.

### Recommended additions
- asset-level performance
- final URL / expanded URL performance
- asset relationships and asset group relationships for PMax
- video metrics for video campaigns where relevant
- policy/approval information if operational reporting requires it

**Priority: P1**

---

## 2.13 Ad Group Report — COVERED FOR CORE PPC

The current ad-group entity and performance streams are sufficient for standard spend/click/conversion analysis.

Recommended additions are primarily segmentation and bid/criterion configuration, not another basic ad-group report.

**Priority: P1**

---

## 2.14 Campaign Report — COVERED FOR CORE PPC, INCOMPLETE FOR ADVANCED ANALYSIS

The current campaign entity and daily performance streams cover the standard campaign KPI layer.

### Important missing dimensions/capabilities
- day/hour
- network type
- click type
- conversion-action name
- PMax asset groups/assets
- Shopping product-level performance
- labels/change history
- richer bidding/targeting configuration

**Priority: P1; P0 when PMax/Shopping or optimization diagnostics are in scope**

---

# 3. Google Ads — additional gaps that are strategically important

## 3.1 Performance Max

The current extraction inventory explicitly omits `asset` and `asset_group`. Google Ads v25 supports these resources, and `asset_group` is a first-class PMax resource.

For serious PMax analysis, add at least:

- asset groups
- asset-group-to-asset relationships
- asset metadata/type
- asset performance where supported
- campaign search-term views for PMax
- final URL expansion / landing-page data where useful
- product-level Shopping data when a Merchant Center feed is involved

## 3.2 Shopping

Google Ads v25 exposes `shopping_performance_view`, which provides product-dimension performance including product brand/category/custom attributes/product type and related product dimensions.

The current implementation has no Shopping product-performance stream.

This is a material gap for ROAS, product, brand and category optimization in Shopping/PMax accounts.

**Priority: P0 when Shopping/PMax is used.**

## 3.3 Change history

Google Ads v25 exposes `change_event`, including changed fields, old/new resource snapshots, operation type, client type and user email, covering changes in the recent change window.

This is not required for campaign KPI reporting, but it is highly valuable for answering analytical questions such as:

- Why did performance change after a specific date?
- Who changed the budget/bid/targeting?
- Was a campaign or ad modified before the KPI shift?

**Priority: P1 for optimization/root-cause analytics.**

---

# 4. GA4 audit

The internal inventory already has broad standard GA4 reporting, but several omissions become important when GA4 is used to join Google Ads, Meta and Search Console data.

## 4.1 Critical join dimensions to add

The GA4 Data API currently exposes session-level Google Ads fields including:

- `sessionGoogleAdsCustomerId`
- `sessionGoogleAdsCampaignId`
- `sessionGoogleAdsCampaignName`
- `sessionGoogleAdsAdGroupId`
- `sessionGoogleAdsAdGroupName`
- `sessionGoogleAdsCreativeId`
- `sessionGoogleAdsKeyword`
- `sessionGoogleAdsQuery`
- `sessionGoogleAdsAdNetworkType`

It also exposes manual campaign identifiers/content dimensions such as:

- `sessionManualCampaignId`
- `sessionManualAdContent`
- `sessionManualTerm`

These are stronger join keys than relying only on campaign names/source/medium.

**Recommendation: make provider IDs first-class normalized dimensions.**

## 4.2 Landing/page diagnostics

Add where analysis requires them:

- `fullPageUrl`
- `landingPagePlusQueryString`
- `pageReferrer`
- `hostName`

## 4.3 Custom measurement

The API supports property-specific custom dimensions/metrics via `customEvent:*`, `customUser:*`, and `customItem:*`, after the property registers them. The existing code discovers these capabilities but only queries them when explicitly configured.

This is technically correct, but the production system should automatically discover the property's registered schema and provide a configuration-driven extraction registry.

## 4.4 Ratio metrics

The current inventory omits several ratios such as sessions-per-user, event-count-per-user, events-per-session, average revenue per user and average purchase revenue.

Most of these are analytically derivable from stored primitives, so they are **not urgent extraction gaps**. Store the primitive measures with full precision and calculate ratios in the semantic/query layer.

**GA4 priority: P1 for attribution/join dimensions; P2 for most derived ratios.**

---

# 5. Meta Ads audit

The current Meta implementation is strong for standard ad/campaign/ad-set performance and common demographic/platform breakdowns.

## 5.1 Normalize actions by action_type

The current implementation keeps `actions[]` and `action_values[]` raw and creates aggregate sums. This preserves the data, but an analysis layer cannot efficiently answer questions such as:

- purchase vs lead vs add-to-cart
- outbound click vs link click
- registrations vs purchases
- cost per specific action
- conversion value by action type

without repeatedly unpacking JSON.

### Recommended design
Create a child fact table such as:

`meta_ad_action_daily`

with:

- date
- account_id
- campaign_id
- adset_id
- ad_id
- action_type
- action_destination
- action_target_id
- value
- attribution window / attribution setting
- source insight row key

Keep the original `actions[]` and `action_values[]` for traceability.

## 5.2 Action breakdowns

Meta's current API ecosystem exposes action breakdowns including `action_type`, `action_destination` and `action_target_id`, among others. The current implementation does not request all of these.

`action_target_id` should be implemented selectively because it can create extremely high-cardinality output.

## 5.3 Hourly analysis

The current implementation is daily. Meta supports hourly statistics broken down by advertiser time zone and audience time zone.

Add this only as a separate, opt-in high-cardinality stream.

**Priority: P1**

## 5.4 Attribution metadata

For cross-platform comparisons, persist Meta attribution configuration/setting along with conversion metrics. Otherwise, identical-looking conversion numbers can represent different attribution rules over time.

**Priority: P0 for cross-platform ROI analysis.**

---

# 6. Google Search Console audit

The extraction is very close to the maximum practical Search Analytics API surface.

The API returns the four fixed metrics:

- clicks
- impressions
- CTR
- position

and supports dimensions such as date, query, page, country, device and search appearance, plus hourly querying.

## 6.1 Important platform limitation

Google states that Search Analytics is subject to internal limits and does **not guarantee that all data rows will be returned**. The API row limit is capped at 25,000 per request.

Therefore:

- API coverage can be 100% of the documented API surface while dataset completeness is still less than 100%.
- Long-tail query analysis must be designed around pagination, multiple filtered queries and the service's own privacy/row-return limitations.
- The data model should retain `data_state`, request parameters and extraction timestamps.

## 6.2 Hourly data

Hourly data uses `dataState=hourly_all` and may contain partial/fresh data. The API metadata identifies the first incomplete hour/date.

Persist this metadata so an analyst does not treat an incomplete recent period as final.

**Priority: P1 engineering/data-quality requirement, not a missing field.**

---

# 7. Cross-platform data model requirements

Even after all provider-specific gaps are filled, reliable analysis requires a common semantic layer.

## 7.1 Mandatory normalized keys

At minimum:

- platform
- account/customer/property ID
- account currency
- account/property time zone
- date
- campaign ID
- campaign name
- ad group/ad set ID
- ad ID
- keyword/search term where applicable
- landing page URL
- source / medium
- channel
- conversion/action type
- attribution setting/window

## 7.2 Never use names as primary joins

Do not join Google Ads to GA4 using only campaign name or ad-group name. Use IDs wherever the providers expose them, and retain names as descriptive attributes.

## 7.3 Conversion semantics

Do not treat these as automatically equivalent:

- Google Ads conversions
- GA4 key events/conversions
- Meta actions/conversions
- GSC clicks

The system should retain the provider-specific conversion definition, attribution model/window and reporting date semantics.

## 7.4 Currency and timezone normalization

Store:

- original provider currency
- original provider timezone
- normalized reporting timezone
- normalized currency only if a valid FX policy exists

Do not silently sum costs across currencies or time zones.

---

# 8. Recommended implementation backlog

## P0 — required before claiming analysis completeness

### Google Ads
- Auction Insights stream
- Ad Schedule configuration + schedule performance
- Day-of-week/hour performance
- Campaign/ad-group criterion + bid modifiers
- Location targeting (`location_view` + criterion data)
- User physical location (`user_location_view`)
- Matched location interest where applicable
- PMax search term stream (`campaign_search_term_view`)
- Dynamic Search Ads search-term/target streams
- PMax asset/asset-group coverage
- Shopping product-performance stream if Shopping/PMax is used

### GA4
- Session Google Ads IDs and attribution dimensions
- Session Google Ads query/keyword
- Landing page query-string/full URL support

### Meta
- Normalize `actions[]` / `action_values[]` by `action_type`
- Persist attribution setting/window metadata

## P1 — strongly recommended

- Google Ads placement/content views
- Google Ads day/hour/network/click-type diagnostic segments
- Google Ads change-event history
- GA4 custom-dimension/custom-metric registry and configurable extraction
- Meta hourly insight stream
- Meta `action_destination` / `action_target_id` where needed
- GA4 page referrer, audience and transaction-level dimensions where relevant
- Search Console extraction-quality metadata and robust long-tail query handling

## P2 — useful but largely derivable / niche

- GA4 ratio metrics that can be derived from primitives
- Google Ads low-value resource/segment fields not used by the analysis layer
- Very high-cardinality Meta action/asset/product breakdowns unless a specific report needs them

---

# 9. Final readiness assessment

| Analysis area | Current status | Assessment |
|---|---|---|
| Campaign KPI reporting | Strong | Ready |
| Ad group KPI reporting | Strong | Ready |
| Ad-level KPI reporting | Strong | Ready with advanced-asset caveats |
| Keyword reporting | Strong | Ready for standard Search |
| Search-term reporting | Partial | Add PMax and DSA coverage |
| Auction Insights | Missing | **Must add** |
| Ad scheduling / hour | Missing | **Must add** |
| Bid adjustment diagnostics | Missing | **Must add** |
| Location reporting | Partial | **Separate target vs user vs interest** |
| Landing-page analysis | Partial | Add URL/paid-search join detail |
| Content / placement analysis | Missing | Add for Display/YouTube |
| PMax analysis | Partial | **Major gap** |
| Shopping product analysis | Missing | **Major gap when applicable** |
| GA4 acquisition analysis | Strong | Add provider IDs/query for robust joins |
| GA4 website behavior | Strong | Ready for standard reporting |
| GA4 custom business metrics | Partial | Configurable extraction needed |
| Meta campaign/adset/ad reporting | Strong | Ready for standard reporting |
| Meta conversion/action analysis | Partial | Normalize actions |
| Meta hourly optimization | Missing | Add when needed |
| GSC SEO reporting | Strong | API surface essentially complete |
| GSC complete long-tail extraction | Platform-limited | Design around API limits |
| Cross-platform ROI | Partial | Attribution/ID/currency/timezone normalization required |

# 10. Bottom line

**The existing system is sufficient for a solid first-generation marketing analytics layer, but it is not yet sufficient to claim that it can reproduce all of the analysis/reporting represented by the screenshot.**

The most important issue is that the current Google Ads gap report focuses on a defined reference surface, while the screenshot contains additional report concepts that require separate Google Ads resources/segments. In particular, **Auction Insights, Ad Schedule, bid modifiers, matched/user/targeted locations, Dynamic Search Ads, placement/content data, and PMax/Shopping data must be added explicitly.**

On the cross-platform side, the most important architectural change is to use **provider IDs + attribution metadata + currency/timezone normalization** rather than relying primarily on names.

Once the P0 backlog is implemented, the analytics layer will be substantially closer to report parity and reliable automated analysis across Google Ads + Meta Ads + GA4 + GSC.

---

# Primary documentation reviewed

## Google Ads API v25
- Reports / field overview: https://developers.google.com/google-ads/api/fields/v25/overview
- Campaign fields: https://developers.google.com/google-ads/api/fields/v25/campaign
- Campaign criterion: https://developers.google.com/google-ads/api/fields/v25/campaign_criterion
- Ad group criterion: https://developers.google.com/google-ads/api/fields/v25/ad_group_criterion
- Ad schedule view: https://developers.google.com/google-ads/api/fields/v25/ad_schedule_view
- Dynamic Search Ads reporting: https://developers.google.com/google-ads/api/docs/dynamic-search-ads/reporting
- Dynamic Search Ads search-term view: https://developers.google.com/google-ads/api/fields/v25/dynamic_search_ads_search_term_view
- Location view: https://developers.google.com/google-ads/api/fields/v25/location_view
- User location view: https://developers.google.com/google-ads/api/fields/v25/user_location_view
- Matched location interest view: https://developers.google.com/google-ads/api/fields/v25/matched_location_interest_view
- Placement reporting: https://developers.google.com/google-ads/api/docs/reporting/placement-views
- Shopping performance view: https://developers.google.com/google-ads/api/fields/v25/shopping_performance_view
- Change event: https://developers.google.com/google-ads/api/fields/v25/change_event

## Google Analytics Data API
- Data API overview: https://developers.google.com/analytics/devguides/reporting/data/v1
- API dimensions and metrics: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
- Create a report: https://developers.google.com/analytics/devguides/reporting/data/v1/basics

## Google Search Console Search Analytics API
- Search Analytics query: https://developers.google.com/webmaster-tools/v1/searchanalytics/query

## Meta Ads / Marketing API
- Meta Marketing API SDK/reference ecosystem for Insights fields, breakdowns and action breakdowns was cross-checked using current published SDK/reference material. Because the public Meta developer documentation is not consistently crawlable, the implementation recommendation should be validated against the exact Graph API version used by the connector before deployment.

---

## Internal source documents used

- `EXTRACTION_INVENTORY.md` — connector-level source of truth for what the current code actually extracts.
- `GAP_REPORT.md` — machine-generated capability-vs-implementation diff for the project's existing reference surface.
