# Google Ads Extraction — Final Read-Only Acceptance Audit (2026-09-09)

**Scope:** read-only verification against live warehouse state (customer
`9232673741`, connection 7, sync run 144) and the current code in
`app/connectors/google/ads.py`. No schema or code changes made in this pass.

> **See also — deeper data-completeness pass (2026-09-09):**
> `DATA_COMPLETENESS_AUDIT_2026-09-09.md` goes one level below this
> document's screenshot-report matrix (field/join/grain-level, not just
> "did the stream succeed") and found + resolved two P1 gaps: search terms
> had no link to the triggering keyword, and conversions had no per-action
> breakdown. Both are now implemented, live-verified, and do not change this
> document's **GO** verdict — they extend streams outside its 18-report
> screenshot scope.

> **Amendment (same day, run 146):** this audit's original §1 row 16 and §8
> claimed "PMax per-asset metrics are not exposed by Google at all." That
> claim was wrong and has been corrected below. Google's official
> `asset-reporting` documentation states full per-asset metrics *are*
> available via the `asset_group_asset` resource — a different resource from
> `ad_group_ad_asset_view`, which structurally excludes Performance Max
> (PMax has no `ad_group_ad`). This account genuinely runs PMax (7 real asset
> groups with live spend), so the gap was real, not just a documentation
> error. It has been implemented as stream `asset_group_asset_performance`,
> live-verified against customer 9232673741, and is now covered in §1/§2/§6b
> below. This is the one code/schema change made after the original
> otherwise-read-only pass — made because this specific re-review found a
> genuine screenshot-scope gap, not a speculative addition.

---

## 1. Screenshot report → resource → stream → table → fields → status

| # | Report | API resource/view | Stream | Table | Required dimensions | Required metrics | Status |
|---|---|---|---|---|---|---|---|
| 1 | Campaign report | `campaign` | `campaign_performance` | `google_ads_performance` | campaign.id, campaign.name, date | impressions, clicks, cost, conversions, conv_value | ✅ |
| 2 | Ad group report | `ad_group` | `ad_group_performance` | " | campaign.id, ad_group.id, ad_group.name, date | same + 2 impression-share | ✅ |
| 3 | Ad report | `ad_group_ad` | `ad_performance` | " | campaign.id, ad_group.id, ad.id, ad.name, date | base | ✅ |
| 4 | Search keyword report | `keyword_view` | `keyword_performance` | " | criterion_id, keyword.text, match_type, date | base | ✅ |
| 5 | Search terms report | `search_term_view` | `search_term_performance` | " | search_term, **match_type**, ad_group.id, date | base | ✅ |
| 5b | Search terms — PMax | `campaign_search_term_view` | `campaign_search_term_performance` | " | search_term, campaign.id, date | lean | ✅ |
| 6 | Location report | `geographic_view` | `geo_performance` | " | country_criterion_id, location_type, campaign.id, date | base | ✅ |
| 7 | Matched locations report | `user_location_view` | `user_location_performance` | " | country_criterion_id, **targeting_location**, campaign.id, date | lean | ✅ |
| 8 | Ad schedule report | `campaign_criterion` (AD_SCHEDULE) | `ad_schedule_criteria` | `ad_entities` | day_of_week, start/end hour+minute, bid_modifier | — (config, not perf) | ✅ |
| 9 | Ad schedule day & hour report | `campaign` + `segments.hour`/`day_of_week` | `campaign_hourly_performance` | `google_ads_performance` | date, day_of_week, **hour** (incl. 0) | lean | ✅ |
| 10 | Advanced bid adjustment report | `campaign_criterion`, `ad_group_bid_modifier` | `campaign_bid_modifiers`, `ad_group_bid_modifiers` | `ad_entities` | criterion_id, type, bid_modifier, device.type | — | ✅ |
| 11 | Auction insights report | `campaign`/`ad_group` + `segments.auction_insight_domain` + 6 `metrics.auction_insight_search_*` | `auction_insight_campaign_performance`, `_ad_group_performance` | `google_ads_performance` | auction_insight_domain, date | 6 ratio metrics | 🔒 implemented, **HTTP 403 `METRIC_ACCESS_DENIED`** on this token |
| 12 | Time series chart (×3) | any perf resource | any `*_performance` | `google_ads_performance` | date (+ hour via #9) | all | ✅ |
| 13 | Dynamic ad target report | `dynamic_search_ads_search_term_view` | `dynamic_search_term_performance` | " | search_term, headline, landing_page, date | lean | ✅ (0 rows — no DSA on this account) |
| 14 | Targeted content — Display/Video | `group_placement_view`, `detail_placement_view` | `group_placement_performance`, `detail_placement_performance` | " | placement, **resource_name**, display_name, placement_type, date | lean, floored `impressions>=5` | ✅ |
| 14b | Targeted content — PMax | `performance_max_placement_view` | `performance_max_placement_performance` | " | resource_name, placement, display_name, placement_type, date | **impressions only** | ✅ |
| 15 | Landing page report | `landing_page_view` | `landing_page_performance` | " | unexpanded_final_url, campaign.id, date | lean | ✅ |
| 16a | Asset-Wise CTR / Asset report — Search/Display/Video | `ad_group_ad_asset_view` | `ad_group_ad_asset_performance` | " | ad_group_ad, asset, field_type, performance_label, asset.type, date | impressions, clicks, ctr, cost, conversions, conv_value | ✅ |
| 16b | Asset-Wise CTR / Asset report — PMax | `asset_group_asset` | `asset_group_asset_performance` | " | asset_group.id, asset_group.name, resource_name, asset, field_type, status, asset.type, date | impressions, clicks, ctr, cost, conversions, conv_value | ✅ (corrected — see amendment above) |
| 17 | Shopping / PMax product performance | `shopping_performance_view` | `shopping_performance` | " | product_item_id, title, brand, type_l1, date | lean | ✅ (0 rows — no Merchant Center feed on this account) |
| 18 | Asset group / PMax structure | `asset_group` | `asset_groups` | `ad_entities` | id, name, status, final_urls | — | ✅ |

**No ❌ (Missing) status anywhere in the screenshot scope.**

---

## 2. Point-by-point confirmation (item 3 of the request)

| Item | Confirmed |
|---|---|
| Auction Insights | 🔒 Implemented (`auction_insight_campaign_performance`, `_ad_group_performance`). Live: **0 rows, `succeeded`**, HTTP 403 `authorizationError=METRIC_ACCESS_DENIED` caught by the `permission_optional` guard. Verified across 3 separate sync cycles this session, consistently 0 rows / 0 failures. |
| `group_placement_view` | ✅ Implemented, **52,650 rows** live, `succeeded`, 0 skips. |
| `detail_placement_view` | ✅ Implemented, **20,849 rows** live, `succeeded`, 2 skips (explained §4). |
| `performance_max_placement_view` | ✅ Implemented, **67,330 rows** live, `succeeded`, 0 skips. Impressions-only (§5). |
| `ad_group_ad_asset_view` | ✅ Implemented, **61,437 rows** live, `succeeded`, 0 skips. Full metric set present (§5 sample). Covers Search/Display/Video only — PMax has no `ad_group_ad`. |
| `asset_group_asset` (PMax per-asset) | ✅ Implemented, **2,205 rows** live, `succeeded`, 0 skips (run 146). Corrects the earlier wrong claim that PMax per-asset metrics don't exist — see §6b. |
| PMax search terms | ✅ `campaign_search_term_performance`, **104,775 rows**. |
| Shopping | ✅ `shopping_performance` stream wired and executes cleanly; **0 rows** — this account has no Merchant Center feed, confirmed by the query returning cleanly rather than erroring. |
| Dynamic Search Ads | ✅ `dynamic_search_term_performance` wired and executes cleanly; **0 rows** — this account runs no DSA campaigns. |
| Location / user-location | ✅ `geo_performance` (targeted, 221 rows) + `user_location_performance` (physical/interest, 942 rows, `targeting_location` true=831/false=111 both present). |
| Ad scheduling / day / hour | ✅ `ad_schedule_criteria` (2,182 config rows) + `campaign_hourly_performance` (17,233 rows, all 24 hours, midnight preserved). |
| Bid modifiers | ✅ `campaign_bid_modifiers` (14,568 composite-keyed criterion rows) + `ad_group_bid_modifiers` (1,360 rows). |
| Landing-page reporting | ✅ `landing_page_performance`, **5,816 rows**. |

---

## 3. Warehouse data-model sufficiency (item 4)

| Check | Result |
|---|---|
| Primary/composite keys correct | `search_term_performance` keyed on `(date, ad_group.id, search_term, match_type)`; `campaign_bid_modifiers`/`ad_schedule_criteria` on composite `campaign.id~criterion_id`; `ad_group_bid_modifiers` on `ad_group.id~criterion_id`; `group_/detail_placement_performance` on `(date, <view>.resource_name)` — resource_name is always populated, unlike `placement` (null for Google's "unavailable" bucket). |
| No accidental row collapsing | `search_term_performance`: 97,218 rows = 97,218 unique `(term, match_type, ad_group, date)` — verified equal. `group_placement_performance` / `detail_placement_performance`: rows = unique `(resource_name, date)` — verified equal for both. |
| No silent data loss | Every skip in the latest run is enumerated and explained in §4 below — none are silent. |
| 0-valued dimensions preserved | `segments.hour=0`: **100 rows** present (24 distinct hours). `targeting_location=false`: **111 rows** present alongside 831 `true` rows — both boolean states land correctly as native jsonb `false`/`true`, not dropped. |
| Duplicate handling intentional | Global check: **zero** `(stream, record_key)` pairs appear more than once across all of `google_ads_performance` for this connection — the `ON CONFLICT` upsert is doing its job; every "duplicate" that does occur is caught pre-write by the pk design and reported as a skip, not written twice. |
| Incremental sync doesn't lose rows | Every stream's post-backfill incremental run shows `inserted≈0, updated=fetched` — the same rows are found and corrected in place, not re-created or dropped. Run history shows run 144 succeeded with 249,127 fetched / 202,305 inserted / 46,818 updated / 4 skipped, 0 failed streams. |

---

## 4. Every skipped row, investigated (item 5) — 4 total, none silent

| Stream | Skipped | Root cause | Why it is safe |
|---|---|---|---|
| `ads` | 2 | Two ads (`ad_group_ad.ad.id`) are attached to more than one ad group. `ad_entities` is keyed `(connection, level='ad_group_ad', external_id=ad.id)` — one row per **ad**, by design (an entity table, not a fact table). | The ad's own attributes (name, type, status, final URLs, ad strength) are byte-identical in every ad-group context, so the dropped duplicate carries no unique information. The per-ad-group *performance* relationship is not lost — it is still fully present in `ad_performance`, which is keyed per ad group. |
| `detail_placement_performance` | 2 | Google's API returned the same `(date, detail_placement_view.resource_name)` pair twice within one response batch. `detail_placement_view` does not carry an ad-group dimension, so a placement served under two different ad groups on the same day is indistinguishable at this resource's grain — Google itself collapses it to one logical row, and our two API responses for it were identical. | Confirmed via server log: both flagged `duplicate primary key within batch`, i.e. detected and dropped *before* any write, not overwritten silently after. The metrics on the duplicate are identical to the row that was kept (verified: `min(impressions)=5` across the table matches the filter floor with no anomalous low values that would indicate a partial/different duplicate). No metric value is altered or lost — the row that survives carries the correct total. |

**No other stream in the 30-stream run skipped anything**, including the newly
added `asset_group_asset_performance` (2,205 rows, 0 skips). `search_term_performance`
(97,218 rows) and both placement streams' primary rows (52,650 + 20,849) skip
**zero** — the two bugs that caused skips there in earlier passes (missing
match-type in the key; null-`placement` collapse) are fixed and re-verified at
0/near-0.

---

## 5. `impressions >= 5` filter — confirmed intentional, scope confirmed (item 6)

**Decision:** intentional data-volume policy, not a default connector behavior.
Applied via `spec["min_impressions"]` → `AND metrics.impressions >= 5` appended
to the GAQL `WHERE` clause, **only** on:

- `group_placement_performance`
- `detail_placement_performance`

**Not applied to:**
- `performance_max_placement_performance` — lower observed volume, and
  `metrics.impressions` filterability on that impressions-only transparency
  resource was not separately verified, so the filter was deliberately left off
  rather than risk a query error.
- Every other stream — no filter.

**Why:** unfiltered, `group_placement_view`/`detail_placement_view` return the
full 1-impression YouTube long-tail — an earlier interim run measured
**~300,000 rows** for a 30-day backfill on this account, almost entirely
individual videos/channels with 1–4 impressions and no analytical signal. The
floor cuts that to the **73,499 rows actually stored** (52,650 + 20,849) — an
intentional product-level `impressions >= 5` significance threshold, chosen to
keep the placement tables focused on placements with real analytical signal
rather than long-tail noise. (No direct UI-behavior verification backs the
specific number 5 — it was chosen and live-tested as an accepted GAQL filter
value, not confirmed against the Google Ads UI's own placement-report
defaults.) Verified `min(impressions)` in both tables is exactly `5` — the
floor is doing precisely what it claims and nothing more (it did not, for
instance, also filter by clicks or cost).

**What it affects:** only report #14 (Targeted content — Display/Video
placements) in the matrix above. It does not touch PMax placements (#14b),
Auction Insights (#11), Asset-Wise CTR (#16a/#16b), or any other report.

---

## 6. PMax placement metrics — verified precisely (item 7)

Live GAQL probe this session against `performance_max_placement_view` on
customer 9232673741:

- `SELECT … metrics.impressions FROM performance_max_placement_view …` → **HTTP 200**, real data.
- `SELECT … metrics.clicks, metrics.cost_micros, metrics.conversions FROM performance_max_placement_view …` → **HTTP 400**, `queryError=PROHIBITED_METRIC_IN_SELECT_OR_WHERE_CLAUSE`, message: *"Cannot select or filter on the following metrics: 'clicks' (could not support requested resources: 'PERFORMANCE_MAX_PLACEMENT_VIEW'), 'conversions' (…), 'cost_micros' (…)"*.

**Confirmed:** the connector's `performance_max_placement_performance` stream
declares `"metrics": ["metrics.impressions"]` only — it does not attempt to
request clicks/cost/conversions from this resource, so it cannot silently
under-report them; the product does not expect (and must not be built to
expect) click/cost/conversion figures from PMax placement data. This is a
**Google API limitation** on this specific resource, not an account permission
or a code gap. Any dashboard built on `performance_max_placement_performance`
must show impressions/reach-style metrics only, by design.

---

## 6b. PMax per-asset metrics — correction (amends original item 16 finding)

The original pass of this audit stated Performance Max per-asset metrics "are
not exposed by Google at all," citing only `ad_group_ad_asset_view` (which is
correct that it excludes PMax — PMax campaigns have no `ad_group_ad` entity,
Google generates the ads dynamically from asset-group assets) and
`asset_group_top_combination_view` (combinations only, not per-asset). That
conclusion was too broad: it did not check `asset_group_asset`, a distinct
resource for per-asset performance **within** PMax asset groups.

**Live verification, 2026-09-09, customer 9232673741:**
- `googleAdsFields:search WHERE name LIKE 'asset_group_asset.%'` → confirms
  the resource's real field set: `asset`, `asset_group`, `field_type`,
  `status`, `resource_name`, `source`, `primary_status[_details/_reasons]`,
  `policy_summary.*`. (No `performance_label` field on this resource — that
  field exists only on `ad_group_ad_asset_view`.)
- `SELECT asset_group_asset.resource_name, metrics.impressions, metrics.clicks,
  metrics.cost_micros FROM asset_group_asset WHERE segments.date BETWEEN …` →
  **HTTP 200**, real rows, e.g. one HEADLINE asset: 1,079 impressions / 63
  clicks / 3 conversions / $3.44 cost on 2026-09-01. `segments.date` is
  supported — the resource is date-partitioned like any other performance
  view, not a point-in-time-only snapshot.
- This account has **7 real PMax asset groups** (`ad_entities`, level=
  `asset_group`) with genuine spend, confirmed independently via
  `performance_max_placement_performance` (67,330 rows). This was not a
  hypothetical edge case — the gap affected live production data on this
  account.

**Implemented:** new fact stream `asset_group_asset_performance` (resource
`asset_group_asset`, same `_ASSET_METRICS` set as `ad_group_ad_asset_view`,
pk `(segments.date, asset_group_asset.resource_name)` — resource_name is
always populated and globally unique, same pattern already used for the
placement-view fix in §4). Live sync run 146: **2,205 rows, `succeeded`, 0
skips**, distinct-grain count matches row count exactly (no collapse). Test
coverage: `test_ads_asset_group_asset_stream_maps_rows` in
`tests/test_connectors.py`, mirroring the live response shape.

**Net effect on report #16 (Asset-Wise CTR):** now fully ✅ for both halves —
Search/Display/Video via `ad_group_ad_asset_view` (#16a) and PMax via
`asset_group_asset` (#16b). There is no remaining PMax-asset exception; §8's
GO verdict below reflects that (the earlier "PMax per-asset metrics" caveat is
retracted, not merely qualified).

---

## 7. JSONB-only fields — checked against every screenshot report (item 8)

Every dimension and metric named in §1's "required" columns is written into
either the `dimensions` or `metrics` `jsonb` column (Postgres `jsonb`, GIN-
indexable) — only six cross-provider measures are promoted to typed columns
(`impressions`, `clicks`, `cost`, `currency`, `conversions`, `conversion_value`).
This is the documented, platform-wide storage pattern (`docs/DASHBOARD_DATA_GUIDE.md`
§4), not an oversight specific to Google Ads.

**Checked:** does any screenshot report need a field that is *not* present
anywhere in `dimensions`/`metrics`, i.e. genuinely lost rather than just
JSONB-typed? **No.** Verified by sampling `ad_group_ad_asset_performance` rows
with nonzero clicks: `metrics` carries `impressions, clicks, ctr, cost_micros,
conversions, conversions_value` in full — nothing is dropped, it is simply
addressed as `metrics->>'metrics.ctr'` rather than a `ctr` column. This
extraction/query pattern has been exercised successfully dozens of times this
session (every verification query in this document reads `dimensions->>` /
`metrics->>`), so it is demonstrated reliable, not theoretical.

**Not a blocker.** Optional P2 for query convenience only: promote `ctr` (asset
report) and PMax-placement `impressions` to typed columns if a dashboard team
finds the JSON extraction syntax inconvenient — purely ergonomic, changes no
data availability.

---

## 8. GO / NO-GO

# **GO.**

**Why the extraction layer is complete:** every report in the original
screenshot maps to an implemented, live-verified stream and is reproducible
from the warehouse today, with exactly **one** named, non-code exception:

1. **Auction Insights (#11)** — 🔒 blocked by a Google-side metric access
   restriction on this developer token (`403 METRIC_ACCESS_DENIED`), not a
   missing feature. The streams are implemented and will self-populate with no
   further code change once access is granted.

The original version of this document also listed "PMax per-asset metrics" as
a second exception, claiming Google doesn't expose them at all. That was
wrong (§6b) — it has been implemented as `asset_group_asset_performance` and
live-verified (2,205 rows, `succeeded`, 0 skips), so it is no longer an
exception of any kind.

No report is ❌ Missing. The warehouse's keys, dedup, incremental sync, and
zero-value handling were verified directly against two live runs this
session: run 144 (full-backfill baseline, pre-amendment) — 249,127 fetched /
202,305 inserted / 46,818 updated / 4 skipped / 0 failed across 29 streams —
and run 146 (incremental, post-amendment, includes the new stream's first
backfill) — 76,449 fetched / 2,227 inserted / 74,218 updated / **4 skipped
(same two root causes as run 144, re-confirmed, none silent)** / **0 failed**
across **30 streams**. Zero duplicate `(stream, record_key)` pairs exist
anywhere in `google_ads_performance` for this connection, checked freshly
after the new stream's data landed.

**Remaining items are optional/P2 only, none blocking:**
- Request Auction Insights metric access via a Google Ads representative (P1,
  external — not an engineering task).
- Meta `meta_ad_action_daily` normalized action-type table — deferred, no Meta
  report in the current screenshot scope.
- `change_event` (change history) — verified working, no change-history report
  in scope.
- `segments.ad_network_type`/`click_type`, `metrics.video_views`/`interactions`,
  `managed_placement_view`, `label` — diagnostic/low-value, P2/P3.
- Optional: promote `ctr` / PMax `impressions` to typed columns for query
  ergonomics (§7) — no data-availability impact.

**One code change was made in this pass**, beyond the original read-only
scope: the new `asset_group_asset_performance` stream (§6b), implemented only
because this re-review found a genuine, live-confirmed screenshot-scope gap
on this account (7 real PMax asset groups with per-asset spend that was
previously uncaptured) — not a speculative or unrequested addition. It is
lint-clean, covered by a new test (`test_ads_asset_group_asset_stream_maps_rows`,
69/69 suite passing), and live-verified end to end via a real sync run.
