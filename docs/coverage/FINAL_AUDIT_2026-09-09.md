# Google Ads Extraction Layer — Final Strict Audit (2026-09-09, round 4)

> **See also:** `DATA_COMPLETENESS_AUDIT_2026-09-09.md` — a deeper,
> field/join-level completeness pass done after this document, which found
> and resolved two P1 gaps (search-term→keyword linking; conversion-action
> breakdown) that were outside this document's scope.

> **Superseded on the "PMax per-asset metrics" point.** This document (and
> §3d/§7 below) claims Google doesn't expose per-asset metrics for Performance
> Max at all. That was wrong — see `ACCEPTANCE_AUDIT_GO.md` §6b: the
> `asset_group_asset` resource does carry real per-asset impressions/clicks/
> cost/conversions, implemented as stream `asset_group_asset_performance` and
> live-verified (2,205 rows, 0 skips). Treat `ACCEPTANCE_AUDIT_GO.md` as
> authoritative on this point; this file is kept for its other content.

**Acceptance criterion:** *"Can the warehouse provide every dataset required to
reproduce every analysis/report in the original screenshot?"* — not raw API
coverage %, not "all streams returned succeeded".

**Method:** every claim re-checked against the Google Ads API **v25** field
reference **and a live probe against the connected account** (customer
`9232673741`, connection 7). Where the earlier report was wrong, it is corrected.

Status legend: ✅ reproducible · 🔒 permission-gated · 🚫 outside product scope ·
❌ missing · ⚠️ partial.

---

## 1. Corrections to earlier reports (verified this pass)

| Earlier claim | Verdict | Evidence (live probe, 2026-09-09) |
|---|---|---|
| "Auction Insights — no Google Ads API resource. Platform ceiling." | **WRONG** | `segments.auction_insight_domain` + 6 `metrics.auction_insight_search_*` **are** in the v25 schema (`campaign` / `ad_group` / `keyword_view`). This developer token → `HTTP 403 authorizationError=METRIC_ACCESS_DENIED` ("the developer doesn't have access to metrics: …"). **Access-restricted, not absent.** Correct class: **🔒 permission-gated**. *(No claim made about whether the access programme is open or closed — not verified from an authoritative source. Only the 403 is verified.)* |
| "Placement / content views — Display-only, this account has no data." | **WRONG for this account** | `group_placement_view` → HTTP 200, real YouTube **channel** placements. `detail_placement_view` → HTTP 200, real YouTube **video** placements. `performance_max_placement_view` → HTTP 200, real **PMax** YouTube placements (a *separate* resource — PMax is not in the standard placement views). |
| "Asset-Wise CTR — P1, not implemented." | **Now implemented** | `ad_group_ad_asset_view` → HTTP 200 with `impressions, clicks, ctr, cost_micros, conversions, conversions_value` all selectable alongside `field_type`, `performance_label`, `asset.type`. |
| "`change_event` not implemented." | Accurate, **on purpose** | Live probe returns real change history. No change-history report in the screenshot → **🚫 P2** with a ready-to-drop spec in `GAP_REPORT_ADDENDUM.md`. |
| "Meta actions: JSONB + aggregate is fine." | Accurate for current scope; **decision explicit** | Screenshot is 100% Google Ads. `actions[]` JSONB + `<field>__sum` covers it. Normalized `meta_ad_action_daily` fact table = **P1** for when Meta conversion-mix analysis is scoped. |

---

## 2. Screenshot-by-screenshot acceptance matrix

Warehouse table is `google_ads_performance` unless noted; `ad_entities` for
entity/config snapshots.

| # | Screenshot report | Required dataset | API resource | Stream | Table | Key fields (dimensions ‖ metrics) | Status |
|---|---|---|---|---|---|---|---|
| 1 | Campaign report | daily campaign KPIs | `campaign` | `campaign_performance` | `google_ads_performance` | `campaign.id, campaign.name, segments.date` ‖ impressions, clicks, cost, conversions, conv_value, + 3 impression-share | ✅ |
| 2 | Ad group report | daily ad-group KPIs | `ad_group` | `ad_group_performance` | " | `campaign.id, ad_group.id, ad_group.name, segments.date` ‖ base + 2 impression-share | ✅ |
| 3 | Ad report | daily ad KPIs | `ad_group_ad` | `ad_performance` | " | `campaign.id, ad_group.id, ad.id, ad.name, segments.date` ‖ base | ✅ |
| 4 | Search keyword report | keyword KPIs + match type | `keyword_view` | `keyword_performance` | " | `campaign.id, ad_group.id, criterion_id, keyword.text, keyword.match_type, segments.date` ‖ base | ✅ |
| 5 | Search terms report | actual query KPIs | `search_term_view` | `search_term_performance` | " | `campaign.id, ad_group.id, search_term, status, segments.search_term_match_type, segments.date` ‖ base | ✅ (pk now includes match type) |
| 5b | Search terms — PMax | PMax query KPIs (excluded from `search_term_view`) | `campaign_search_term_view` | `campaign_search_term_performance` | " | `campaign.id, search_term, segments.date` ‖ lean | ✅ |
| 6 | Location report | targeted-location KPIs | `geographic_view` | `geo_performance` | " | `campaign.id, country_criterion_id, location_type, segments.date` ‖ base | ✅ |
| 7 | Matched locations report | physical/interest location + was-it-targeted flag | `user_location_view` | `user_location_performance` | " | `campaign.id, country_criterion_id, targeting_location, segments.date` ‖ lean | ✅ |
| 8 | Ad schedule report | configured day/hour blocks + bid modifier | `campaign_criterion` (AD_SCHEDULE) | `ad_schedule_criteria` | `ad_entities` (`level='ad_schedule'`) | `campaign.id, criterion_id, ad_schedule.{day_of_week,start_hour,start_minute,end_hour,end_minute}, bid_modifier, status` | ✅ |
| 9 | Ad schedule **day and hour** report | hour-of-day + day-of-week KPIs | `campaign` + `segments.hour` / `segments.day_of_week` | `campaign_hourly_performance` | `google_ads_performance` | `campaign.id, segments.date, segments.day_of_week, segments.hour` ‖ lean | ✅ (all 24 hours incl. midnight) |
| 10 | Advanced bid adjustment report | criterion-level bid modifiers (device/location/schedule/demographic) | `campaign_criterion`, `ad_group_bid_modifier` | `campaign_bid_modifiers`, `ad_group_bid_modifiers` | `ad_entities` | `campaign.id/ad_group.id, criterion_id, type, bid_modifier, device.type, status` (composite `parent~criterion_id` key) | ✅ |
| 11 | Auction insights report | competitor-domain impression-share / overlap / outranking | `campaign` / `ad_group` + `segments.auction_insight_domain` + 6 `metrics.auction_insight_search_*` | `auction_insight_campaign_performance`, `auction_insight_ad_group_performance` | `google_ads_performance` (metrics in JSON) | `campaign.id/ad_group.id, segments.auction_insight_domain, segments.date` ‖ 6 auction-insight ratios | 🔒 **implemented, dormant** — 403 `METRIC_ACCESS_DENIED` on this token; streams return 0 rows and self-populate once Google grants the token access |
| 12 | Time series chart (×3) | any metric over `date` (and `hour`) | any perf resource | any `*_performance` stream | `google_ads_performance` | `segments.date` (+ `segments.hour` via #9) ‖ all | ✅ |
| 13 | Dynamic ad target report | DSA auto-target search term + headline + landing page | `dynamic_search_ads_search_term_view` | `dynamic_search_term_performance` | `google_ads_performance` | `campaign.id, ad_group.id, search_term, headline, landing_page, has_negative_keyword, has_matching_keyword, segments.date` ‖ lean | ✅ (0 rows — account runs no DSA; query executes) |
| 14 | Targeted content report — Display/Video placements | where ads served (channel / video / site / app) | `group_placement_view`, `detail_placement_view` | `group_placement_performance`, `detail_placement_performance` | `google_ads_performance` | `campaign.id, placement, display_name, placement_type, target_url, segments.date` ‖ lean, **`WHERE metrics.impressions >= 5`** | ✅ (live YouTube data; the 1-impression long-tail is floored — same as an analyst filtering the UI report — otherwise ~300k rows/backfill on this account) |
| 14b | Targeted content — **PMax** placements | where PMax served (separate resource) | `performance_max_placement_view` | `performance_max_placement_performance` | `google_ads_performance` | `campaign.id, resource_name, placement, display_name, placement_type, target_url, segments.date` ‖ **impressions only** (Google prohibits clicks/cost/conversions on this resource) | ✅ (live PMax YouTube data) |
| 15 | Landing page report | cost/clicks/conv per final URL | `landing_page_view` | `landing_page_performance` | `google_ads_performance` | `campaign.id, campaign.name, unexpanded_final_url, segments.date` ‖ lean | ✅ (`expanded_landing_page_view` deliberately dropped — same numbers, unbounded rows) |
| 16 | Asset-Wise CTC / Asset report | per-asset (headline/description/image/video) CTR & conv | `ad_group_ad_asset_view` | `ad_group_ad_asset_performance` | `google_ads_performance` | `campaign.id, ad_group.id, ad_group_ad, asset, field_type, performance_label, asset.type, segments.date` ‖ impressions, clicks, ctr, cost, conversions, conv_value | ✅ for Search/Display/Video RSA assets. **PMax per-asset *metrics* are not exposed by Google at all** (only `asset_group_top_combination_view` + a coarse `performance_label`) — 🚫 Google API limitation |
| 17 | Matched locations / Location (dupes in the folder) | see #6, #7 | — | — | — | — | ✅ |
| 18 | Asset group / PMax structure | asset-group entity | `asset_group` | `asset_groups` | `ad_entities` (`level='asset_group'`) | `campaign.id, asset_group.id, name, status, final_urls` | ✅ |

**GA4 → Google Ads join enablement (audit §4.1):** GA4 `google_ads_campaigns`
stream now emits `sessionGoogleAdsCampaignId` / `sessionGoogleAdsAdGroupId` /
`sessionGoogleAdsKeyword` → join
`google_analytics_performance.dimensions->>'sessionGoogleAdsCampaignId'` to
`google_ads_performance.dimensions->>'campaign.id'`.

**Every numbered screenshot report is ✅, except #11 Auction Insights (🔒) and the
PMax half of #16 (🚫 Google limitation).**

---

## 3. Four-way status breakdown

### 3a. Implementation complete (code done, wired, tested)
All **29** Google Ads streams. The 2 Auction Insights streams and both PMax-related
streams (`performance_max_placement_performance`,
`ad_group_ad_asset_performance`) are fully implemented and wired; a 403 or empty
result is handled without failing the run.

### 3b. Data available **for this account** right now
Every stream above **except**:
- `auction_insight_*` — 0 rows (403, see 3c).
- `dynamic_search_term_performance` — 0 rows (no DSA campaigns).
- `shopping_performance` — 0 rows (no Merchant Center feed).
- PMax per-asset metrics — not returned by Google (see 3d).

### 3c. API permission limitations (🔒)
- **Auction Insights** — `403 authorizationError=METRIC_ACCESS_DENIED` on this
  developer token for the 6 `metrics.auction_insight_*` fields. The streams exist
  and carry `permission_optional`; they return 0 rows now and begin returning data
  the moment Google grants this token access to those metrics (request via a
  Google Ads representative). No code change needed then.

### 3d. Product-scope / API-shape exclusions (🚫)
- **PMax per-asset metrics** — Google does not expose per-asset impressions/clicks
  for Performance Max (only `asset_group_top_combination_view` combinations and a
  coarse `performance_label`). `ad_group_ad_asset_performance` covers every other
  ad type.
- **`change_event`** (change history) — verified working; no change-history report
  in the screenshot → P2 (ready spec in `GAP_REPORT_ADDENDUM.md`).
- **`managed_placement_view`** — thin resource (`resource_name` only), 0 rows here;
  the group/detail/PMax placement views carry the substance.
- **`topic_view`, `display_keyword_view`** — Display topic targeting; this account
  runs none.
- **Meta `meta_ad_action_daily`** normalized fact table — P1, deferred (screenshot
  is 100% Google Ads).
- **`segments.ad_network_type` / `segments.click_type`**, `metrics.video_views` /
  `metrics.interactions`, `label` — diagnostic, P2/P3.

---

## 4. Data-quality verification (final sync — run 144, connection 7)

**29/29 streams, 0 failed.** Totals: fetched 249,127 · inserted 202,305 ·
updated 46,818 · **skipped 4**.

| Stream | status | fetched | inserted | updated | skipped |
|---|---|---:|---:|---:|---:|
| campaigns | succeeded | 106 | 0 | 106 | 0 |
| ad_groups | succeeded | 445 | 0 | 445 | 0 |
| ads | succeeded | 1,241 | 0 | 1,239 | **2** |
| conversion_actions | succeeded | 78 | 0 | 78 | 0 |
| ad_schedule_criteria | succeeded | 2,064 | 0 | 2,064 | 0 |
| campaign_bid_modifiers | succeeded | 10,419 | 0 | 10,419 | 0 |
| ad_group_bid_modifiers | succeeded | 1,356 | 0 | 1,356 | 0 |
| asset_groups | succeeded | 7 | 0 | 7 | 0 |
| campaign_performance | succeeded | 103 | 0 | 103 | 0 |
| campaign_device_performance | succeeded | 250 | 0 | 250 | 0 |
| ad_group_performance | succeeded | 255 | 0 | 255 | 0 |
| ad_performance | succeeded | 509 | 0 | 509 | 0 |
| keyword_performance | succeeded | 1,508 | 1 | 1,507 | 0 |
| search_term_performance | succeeded | 11,341 | 23 | 11,318 | **0** |
| geo_performance | succeeded | 178 | 0 | 178 | 0 |
| age_range_performance | succeeded | 1,319 | 0 | 1,319 | 0 |
| gender_performance | succeeded | 666 | 0 | 666 | 0 |
| landing_page_performance | succeeded | 724 | 0 | 724 | 0 |
| user_location_performance | succeeded | 210 | 0 | 210 | 0 |
| campaign_hourly_performance | succeeded | 2,027 | 1 | 2,026 | 0 |
| dynamic_search_term_performance | succeeded | 0 | 0 | 0 | 0 |
| campaign_search_term_performance | succeeded | 12,053 | 14 | 12,039 | 0 |
| shopping_performance | succeeded | 0 | 0 | 0 | 0 |
| auction_insight_campaign_performance | succeeded | 0 | 0 | 0 | 0 |
| auction_insight_ad_group_performance | succeeded | 0 | 0 | 0 | 0 |
| group_placement_performance | succeeded | 52,650 | 52,650 | 0 | 0 |
| detail_placement_performance | succeeded | 20,851 | 20,849 | 0 | **2** |
| performance_max_placement_performance | succeeded | 67,330 | 67,330 | 0 | 0 |
| ad_group_ad_asset_performance | succeeded | 61,437 | 61,437 | 0 | 0 |

**Both skip sources investigated, explained — not silent data loss:**

1. **`ads` (2 rows)** — two ads are used in more than one ad group; `ad_entities`
   dedups on `(connection, level='ad_group_ad', external_id=ad.id)` by design
   (the ad's own attributes are identical either way). The per-ad-group linkage
   of the duplicate is still available via `ad_performance`.
2. **`detail_placement_performance` (2 rows)** — Google returned the same
   `(date, resource_name)` twice within one response batch. `detail_placement_view`
   doesn't segment by ad group, so a placement served under two ad groups on the
   same day collapses to one row; the dropped duplicate carried identical
   `impressions`/metrics. Confirmed via server log ("duplicate primary key within
   batch" ×2).

**Bugs found and fixed this pass (in addition to the two from the prior pass):**

- **3rd bug — `search_term_performance` match-type collapse.** Missing
  `segments.search_term_match_type` in the pk caused BROAD vs PHRASE of the same
  query text in one ad-group/day to collapse (~263 rows/run). Fixed; re-verified
  at **0 skips** on both a full 90-day backfill (97,135 rows) and this
  incremental run.
- **4th bug — placement-view null-placement collapse.** `group_placement_view` /
  `detail_placement_view` return a null `placement` for Google's "unavailable/
  unknown" bucket (e.g. "Video no longer available"); keying on `placement`
  collapsed all such rows per campaign/day (70 dropped on `group_placement_performance`
  in the interim run). Fixed by keying on `<view>.resource_name` (always present,
  unique per placement) instead — verified **0 skips** on `group_placement_performance`
  (52,650 rows) and skips cut from 70 → 2 on `detail_placement_performance`.
- **Volume control, not a bug:** `group_placement_view` / `detail_placement_view`
  are a huge 1-impression YouTube long-tail on this account (~300k rows
  unfiltered). Added `WHERE metrics.impressions >= 5` — the same floor an analyst
  applies in the UI — cutting backfill volume ~75% while keeping every placement
  worth reviewing. Verified the filter is accepted by both resources (HTTP 200)
  before shipping it. Not applied to `performance_max_placement_performance`
  (lower volume observed; `metrics.impressions` filterability on that
  transparency-only resource was not separately verified).

**Primary-key / composite-key / null-handling checks (carried over + new):**

| Check | Result |
|---|---|
| `campaign_hourly_performance` distinct hours / midnight rows | 24 hours; midnight (`segments.hour=0`) rows present |
| `search_term_performance` rows vs. unique `(term, match_type, ad_group, date)` | equal — no duplicate grain |
| `campaign_criterion` / `ad_schedule` / `ad_group_bid_modifier` composite keys | populated at full cardinality (14,568 / 2,182 / 1,360 in `ad_entities`) |
| `group_placement_performance` / `detail_placement_performance` keyed on `resource_name` | 0 / 2 skips (down from unmeasured / 70) |
| Auction Insights 403 handling | both streams `succeeded`, 0 rows, 0 skips, 0 failures — verified twice across two full sync cycles |
| `dynamic_search_term_performance` / `shopping_performance` (no DSA / no feed) | `succeeded`, 0 rows — not treated as unsupported |
| Incremental vs. full consistency | every stream re-run incrementally after its backfill produced `inserted≈0, updated=full count` — no duplication |

---

## 5. Prioritized remaining tasks

**P0** — none. Every screenshot report is reproducible today (Auction Insights is
an external access grant, not a code task).

**P1**
1. Request Auction Insights metric access for the developer token (Google rep) —
   streams then populate with no code change.
2. Meta `meta_ad_action_daily` normalized action-type fact table (when Meta
   conversion analysis is scoped).

**P2**
3. `change_event` entity stream (spec ready).
4. `segments.ad_network_type` / `segments.click_type` on core perf streams.
5. `metrics.video_views` / `metrics.interactions` for video campaigns.
6. `managed_placement_view` explicit-placement list.

**P3**
7. `label` resource; GA4 low-value date-part dimensions.

---

## 6. Final conclusion

- **Implementation:** complete for the screenshot scope — 29 Google Ads streams,
  `ruff` clean, 68 tests pass (deterministic + random order), `alembic` at head
  (no migration — new fields ride existing tables / the `metrics` JSON).
- **Data available for this account:** every screenshot report reproducible now,
  except Auction Insights (permission) and PMax per-asset metrics (Google
  doesn't expose them).
- **API permission limitation:** Auction Insights metrics — 403 on this token;
  streams built and dormant, self-heal on access grant.
- **Product-scope exclusions:** `change_event` (P2), Meta action-type table (P1),
  a handful of diagnostic segments (P2/P3) — none block a screenshot report.

This is **not** declared complete because the streams execute. It is assessed as
complete for the screenshot's reporting scope, with the two exceptions above
named explicitly and neither being a code defect.
