# LeadSquared Integration — Deep Discovery & Architecture Validation

**Date:** 2026-09-11
**Scope:** Read-only live probes against the LeadSquared account (`api-in21.leadsquared.com`) + cross-reference against the existing `google_ads_performance` / `meta_ads_performance` / `ad_entities` warehouse tables. No writes to LSQ. No application/schema/ingestion code changed. Probe scripts were temporary and deleted after use.

**Every claim below is tagged:** 🟢 **LIVE FACT** (observed directly against the account this session) · 🟡 **INFERENCE** (derived from live data but not 100% certain) · 🔵 **RECOMMENDATION** (a design choice, not a fact).

---

## The key question, answered up front

> **Can we deterministically trace paid Google/Meta advertising → LSQ lead → downstream booking/payment/revenue, and what is the strongest reliable join path at each stage?**

**Yes, for the majority of paid volume, with caveats.** 🟢 A live LSQ lead's `mx_Source_Campaign_ID` (23226177337) exactly matches a real `campaign.id` already in `google_ads_performance`/`ad_entities`. At scale, ID-level match rates against the warehouse are **50–96%** depending on the entity level and platform (table in Phase 3/4 below) — high enough to build on, not high enough to claim 100% determinism. The weakest link is the ad-set/ad-group level for Google (69.9%/57.2%), and the strongest is Meta ad-id (96.0%). GCLID, despite being present on ~15K leads, **cannot be used as a warehouse join key at all** — Google Ads reporting doesn't expose GCLID, so there is nothing in the warehouse to join it against. Name-based joins are not viable as a primary key (up to 7.4% of sampled leads have a campaign name with no ID, or an ID with no name).

---

## Phase 1 — Lead metadata: live vs. assumed

🟢 **198 fields, live.** All 23 specifically-requested fields exist and are populated as expected (table in the probe output; DataTypes confirmed via `LeadsMetaData.Get`, not the markdown doc). Two corrections to the markdown doc / earlier assumptions:

| Field | Doc assumed | 🟢 Live reality |
|---|---|---|
| `Source` | generic dropdown | **DisplayName is "Patient Source"** — a curated dropdown, not free text. New values require a config change on LSQ's side, so the source list is a closed, stable set (good for a connector — no need to handle arbitrary strings). |
| `mx_Lead_Type` | assumed binary new/repeat | **5 real values**, not 2: `P1 - Curelo New`, `P2 - Curelo Repeat`, `L1 - Lab New`, `L2 - Lab Repeat`, `C - Corporate Lead`. "Lab" and "Corporate" are distinct businesses bundled into the same lead pool — this matters for Phase 7. |
| `mx_Slug` | assumed free text | Dropdown of landing-page slugs (`google-lp-fbc-1299`, `meta-lp`, `cfbc`, etc.) — a second, finer-grained attribution signal beyond `Source`, worth capturing. |
| `EmailAddress`/`Phone`/`CreatedOn`/`ModifiedOn` | assumed | 🟢 Confirmed searchable, correct types. `Phone` and `EmailAddress` are both marked `IsSearchable: true` — either is a viable independent customer identity key alongside `ProspectID` (Phase 13). |

🟢 No status/stage field exists on the **Lead** object itself in this account's schema (no `LeadStage`, `LeadStatus` schema name among the 198). Lifecycle/stage tracking here is done entirely through **Activities** (Phase 5) and **Opportunities** (Phase 9), not a lead-level status field. This confirms the earlier hunch from before this session.

Full 198-field catalogue and the bucketed (status/owner/date/attribution/revenue) breakdown were saved during the probe; happy to re-pull specific fields on request — not reproduced in full here to keep this report readable.

---

## Phase 2 — Source inventory (🟢 live, 125,000-lead sample of 156,448 in the trailing 30 days — 80% coverage)

62 distinct `Source` values observed live (not the ~10 assumed earlier). Top sources by volume, with attribution population rates:

| Source | % of leads | Campaign ID | Ad ID | GCLID | UTM source | Classification |
|---|---|---|---|---|---|---|
| Meta_Form | 24.3% | 99.4% | 99.5% | 0.6% | 14.7% | 🟢 **Meta paid** |
| Manage_Portal | 16.7% | 0.7% | 0.6% | 0.2% | 0.3% | 🟡 Internal/agent-entered, not ad-attributed |
| Cold Calling | 16.3% | 1.0% | 0.9% | 0.3% | 0.5% | 🟡 Outbound, not ad-attributed |
| Lifecycle P1 - WA | 7.5% | 0.7% | 0.6% | 0.2% | 0.4% | 🟡 Repeat-touch/follow-up, not new acquisition |
| google_lp | 8.6% | 23.0% | 40.1% | **98.3%** | 98.6% | 🟢 **Google paid** (landing page) |
| Direct Traffic | 4.7% | 1.5% | 1.4% | 0.6% | 0.9% | 🟡 Organic/unattributed |
| App_Organic | 3.6% | 2.6% | 2.7% | 1.4% | 1.7% | 🟢 Organic (by name + low attribution) |
| Lifecycle - WA | 1.6% | 1.6% | 1.3% | 0.5% | 0.7% | 🟡 Follow-up, not acquisition |
| DSA_Meta | 1.5% | 95.3% | 100.0% | 0.3% | 0.7% | 🟢 **Meta paid** (DSA = a specific ad program) |
| Affiliate P1_WA | 1.4% | 0.3% | 0.2% | 0.1% | 0.1% | 🟢 Affiliate/partner, not paid-ad |
| Google_lp (capital G) | 1.3% | 99.4% | 18.1% | 99.5% | 99.8% | 🟢 **Google paid** — a *second, differently-cased* dup of `google_lp` |
| Outbound Phone call | 1.1% | 2.2% | 2.0% | 0.6% | 1.1% | 🟡 Outbound |
| cfbc | 1.0% | 27.2% | 46.6% | 90.7% | 90.7% | 🟢 **Google paid** (a specific campaign/LP code) |
| MDS_Media | 2.0% | 3.2% | 3.2% | 0.6% | 100.0% | 🟡 Affiliate/media-partner (utm_source populated but not campaign IDs — different attribution scheme) |

🟢 **Confirmed live, important:** `google_lp` and `Google_lp` are **two separate dropdown values** differing only by capitalization (23,853 vs 1,681 in-sample rows respectively) — a genuine data-quality artifact in the source LSQ account, not a probe error. Any classification logic must be case-insensitive or explicitly enumerate both.

🟢 Full Google-paid family observed: `google_lp`, `Google_lp`, `cfbc`, `Google_PMax`, `Google-imaging-lp`, `Google_Call`, `Google_WA`, `Google_Outbound`, `Google_RDX_Call`, `Google_DG`, `Google_RCS`, `Google_Call_Guj`, `Google P1 - WA` (this last one has near-zero attribution — a WA follow-up bucket, not fresh acquisition, despite the "Google" name).

🟢 Full Meta-paid family observed: `Meta_Form`, `Meta_Call`, `Meta_WA`, `meta_lp`, `Meta_lp`, `Meta_Outbound`, `Meta_Social`, `DSA_Meta`, `Camp_Meta`.

🟡 `AI Bot`, `WhatsApp Outreach`, `365 Digital` mentioned in the internal payload doc — only `365 Digital` (11 leads) actually appeared live in this 30-day window; `AI Call` (3 leads) appeared instead of "AI Bot". These are real but extremely low-volume; not worth dedicated handling yet.

🔵 **Recommendation:** classify by `Source` using a maintained allow-list (not a heuristic), refreshed periodically since LSQ's own dropdown can add values without notice — and flag any *new, unrecognized* `Source` value during sync rather than silently bucketing it as "Other" (the audit above already shows near-duplicate casing slipping through).

---

## Phase 3 — Google attribution validation (🟢 live, cross-referenced against the warehouse DB)

| Join key | LSQ-side distinct IDs (sample) | Found in `ad_entities` | Match rate |
|---|---|---|---|
| `mx_Source_Campaign_ID` → `ad_entities.external_id` (level=campaign) | 60 | 43 | **71.7%** |
| `mx_Ad_Id` → `ad_entities.external_id` (level=ad_group_ad) | 257 | 147 | **57.2%** |
| `mx_Adset_Id` (doc: "ad_group_id for google") → `ad_entities.external_id` (level=ad_group) | 166 | 116 | **69.9%** |
| `mx_utm_keyword_id` | 0% of sampled Google leads populate any keyword-ID-bearing rows we could resolve | n/a | **No keyword-level entity exists** (see below) |
| `mx_GCLid` | 15,207 leads have a GCLID | 0 — 🟢 confirmed GCLID string does not appear anywhere in `google_ads_performance.dimensions` or `.raw` | **0% — not usable, ever, with the current warehouse** |

🟢 **Critical, live-confirmed limitation:** the Google Ads Reporting API (which `google_ads_performance` is built from) does not expose GCLID at all — it's a click-level identifier that only appears in conversion-upload/offline-conversion APIs, never in performance reporting. This isn't a bug in the connector; it's a platform limitation. **GCLID cannot be a join key against this warehouse as it exists today.** It's still worth ingesting from LSQ (useful for external tools, support/debugging, and a future offline-conversion-upload use case), but the join hierarchy must not depend on it.

🟢 **No keyword-level `ad_entities` row exists at all** (confirmed via `ad_entities_levels_present`: google levels are `ad_group, ad_group_ad, ad_group_bid_modifier, ad_schedule, asset_group, campaign, campaign_criterion, conversion_action, sitemap` — no `keyword`). The 31-stream Google Ads connector captures keyword performance only as fact-table rows (`search_term_performance` dimensions), not as a standalone dimension/entity table. So `mx_utm_keyword_id` has **nothing dedicated to join against** — it would need a new lookup built from the fact table's `dimensions` JSON, not a simple FK.

🟡 **Data-quality finding, not yet fully explained:** ~30% of sampled Google-attributed campaign/ad/ad-group IDs from LSQ don't resolve against the warehouse. Spot-checking the unmatched IDs shows a **mix of two causes**: (a) genuinely old/paused/deleted campaigns that predate the current 33-day Google Ads sync window in the warehouse (the connector's `backfill_start_date`/lookback don't reach back far enough — this is a coverage gap, not a join-design flaw); (b) some IDs in the "Google-bucketed" unmatched sample are actually **Meta-format IDs** (18-digit, e.g. `23853121772240798`) leaking into a Google-classified lead — i.e. some leads carry a stale/wrong-platform ID in the generic `mx_Source_Campaign_ID` field, most likely from a multi-touch lead whose `Source` was updated on a later touch but the attribution ID field wasn't. This is a genuine upstream data-quality issue in LSQ, not a defect in the probe.

🔵 **Recommendation:** before finalizing match-rate expectations, extend the Google Ads warehouse's historical coverage (already flagged as an 8-stream gap fixed earlier this session for one connection — worth checking whether it recurs) and add an ID-format sanity check (`^\d{9,10}$` vs `^\d{15,18}\d{7}$`-ish patterns differ meaningfully between Google's and Meta's ID spaces) as a cheap pre-join validation, since the platforms' ID formats are visibly distinguishable.

---

## Phase 4 — Meta attribution validation (🟢 live)

| Join key | LSQ-side distinct IDs (sample) | Found in `ad_entities` | Match rate |
|---|---|---|---|
| `mx_Source_Campaign_ID` → level=campaign | 52 | 26 | **50.0%** |
| `mx_Adset_Id` → level=adset | 139 | 99 | **71.2%** |
| `mx_Ad_Id` → level=ad | 326 | 313 | **96.0%** |

🟢 Meta's **ad-id match rate (96%) is the strongest single join key found in this entire discovery pass** — noticeably better than anything on the Google side. Ad-level is the most granular and also the most reliable.

🟢 **Confirmed: no `mx_FBClid`/`mx_fbclid` field exists** in the 198-field schema (re-confirmed this session, matching the earlier finding). Meta attribution relies entirely on the numeric campaign/adset/ad IDs.

🟢 **New finding this pass — a materially better Meta attribution source exists and wasn't in scope before:** Activity Event **204, "Facebook Lead Ads Submissions"** (149,656 → actually 15,921 in 30 days; see Phase 5 table) fires directly from the native Facebook Lead Ads integration and carries a *richer, cleaner* attribution payload than the lead's own `mx_*` fields — in the live sample it includes the FB campaign name (`mx_Custom_1`), FB ad name (`mx_Custom_4`), FB adset name (`mx_Custom_5`), a Meta-internal lead ID (`mx_Custom_6`), Ad ID (`mx_Custom_8`), Campaign ID (`mx_Custom_9`), and Adset ID (`mx_Custom_16`) — all captured at the moment of submission, before any later-touch overwriting can happen. 🔵 **Recommendation:** for Meta specifically, event 204 (where present) may be a *more reliable* attribution source than the lead record's own `mx_*` fields, and worth ingesting as a distinct, corroborating stream.

---

## Phase 5 — Full activity-type catalogue (🟢 live: 84 activity types total, not just the ~17 previously assumed)

`ActivityTypes.Get` returned all 84 configured types. Full list saved during the probe; the business-relevant subset with **live 30-day volumes**:

| Code | Name | 30-day volume | Relevance |
|---|---|---|---|
| 203 | WhatsApp Message | 132,732 | Engagement, not conversion |
| 204 | Facebook Lead Ads Submissions | 15,921 | 🟢 Attribution (see Phase 4) |
| 206 | **Booking Created** | 23,067 | 🟢 Primary conversion candidate |
| 208 | Post Booking Order Status | 149,656 | 🟡 Fulfillment pipeline — fires **multiple times per booking** as status changes (sample submitted, collected, reported, etc.), not a single terminal event; has revenue-shaped custom fields |
| 209 | Upload Prescription | 248 | Not conversion-relevant |
| 213 | Diet Consultation Created | 723 | Secondary product line, not core lab-test funnel |
| 214–219, 222 | Diet consultation lifecycle (reschedule/cancel/summary/etc.) | 6–35 each | Low volume, secondary product |
| 223 | **Booking Cancelled** | 2,790 | 🟢 Reversal signal |
| 224 | Partial Report Uploaded | 454 | Fulfillment, not conversion |
| 225 | **Payment Success** | 110 | 🟢 Strongest conversion signal, but **very low volume relative to bookings** (110 vs 23,067 — 0.5%) |
| 227 | Booking Edited | 7,968 | Mutation event, not a distinct funnel stage |
| 246 | Payment Pending | 13,528 | Pre-conversion state |
| 250 | Click on Real Stories | 0 (30d) | Marketing engagement, unrelated |
| 32 | Duplicate Opportunity Detected | 30,270 | System/automation noise from the Opportunities feature (see Phase 9) — **not a funnel signal** |

🟢 **Important volume mismatch, live-confirmed:** Payment Success (225) is only 110 records in 30 days against 23,067 Booking Created (206) and 13,528 Payment Pending (246). This strongly suggests **Payment Success (225) is not consistently used/fired** as the payment-completion signal for this business — most payment activity is tracked through **208 (Post Booking Order Status)**'s status sub-field instead (`mx_Custom_6` values observed live: `"customer_confirmed"`, plus a `mx_Custom_4` status JSON blob seen on a 225 sample: `{"Status":"Pending"}`). 🔵 **Do not treat 225 as the primary "paid" signal** — Phase 6 elaborates.

🟢 Activity max `PageSize` is **1,000**, not 5,000 (the lead endpoint's limit) — confirmed live via a `MXInvalidInputException` ("PageSize can not be more than 1000") when 2,000 was requested against `RetrieveByActivityEvent`. This directly affects the Phase 10 cost model.

---

## Phase 6 — Business funnel reconstruction (🟢 live, full 30-day pull of events 206/223/225)

| Metric | 🟢 Live value |
|---|---|
| Booking Created (206) rows | 23,071 |
| — distinct prospects | 17,685 (3,229 prospects have **more than one** booking-created event in 30 days) |
| Booking Cancelled (223) rows | 2,790 → 2,332 distinct prospects |
| Payment Success (225) rows | 110 → 87 distinct prospects |
| Prospects with both booking + cancellation | 2,118 |
| Prospects with both booking + payment-success | **85** (out of 17,685 booked prospects — 0.5%) |
| Prospects with booking but neither payment-success nor cancellation | 15,493 (87.6% of booked prospects) |

🟢 **This settles the open question from Phase 5: Payment Success (225) cannot be the revenue-conversion signal — it covers under 1% of bookings.** The other 87.6% of bookings are neither explicitly paid-success nor cancelled in this activity model; they're tracked through **208's status sub-field** instead (live sample statuses seen: `customer_confirmed`, `Phlebotomist Sample Submitted`; a payment-success 208-adjacent JSON blob on one 225 record showed `{"Status":"Pending"}`). 208 also carries the clearest revenue fields observed anywhere in this discovery: `mx_Custom_15` (looked like a gross amount, e.g. "7150"), `mx_Custom_20` ("799"), `mx_Custom_21` ("400"), `mx_Custom_22` ("399") on the live sample — consistent with the earlier internal-doc mapping (Actual Amount / Booking Amount / Curelo Commission) but the **exact semantics of each `mx_Custom_N` slot need confirmation from Curelo's ops/CRM owner**, not guessed from one sample record — 208 fires with a *variable* custom-field set depending on which status transition it represents (compare the "customer_confirmed" sample above, which has no amount fields populated, against the earlier session's cancelled/booking-created samples).

🟡 **Working funnel model** (subject to the ops-team confirmation above):

```
Lead (LeadManagement)
  → Booking Created (206)                      — commitment signal, 23K/30d
      → Booking Cancelled (223)                — 12% of bookings, explicit reversal
      → Post Booking Order Status (208)         — the real fulfillment/payment pipeline,
                                                   fires N times per booking with a
                                                   status field; look for a terminal
                                                   "collected"/"reported"/paid status
                                                   rather than treating any 208 row as
                                                   itself a conversion
      → Payment Success (225)                   — real but rare explicit signal (0.5%),
                                                   likely only used for a specific
                                                   payment flow (e.g. online prepay),
                                                   not the general case
```

🔵 **Recommendation:** do not finalize "conversion" as 206, or as 225, alone. Build the connector to ingest **206, 223, and 208 (all rows, keeping the status transitions)** as three streams, and work with Curelo's ops owner to identify the specific `208` status value(s) that represent "fulfilled/collected" before defining a single "converted" boolean in any downstream view. This is exactly the trap the user's original instructions warned against ("do not choose Booking Created as the final conversion definition until this analysis is complete") — and the data confirms that caution was warranted.

---

## Phase 7 — Repeat vs. new customer (🟢 live, full 125K sample)

| `mx_Lead_Type` | Count | % |
|---|---|---|
| *(null/unset)* | 59,288 | 47.4% |
| P1 - Curelo New | 48,213 | 38.6% |
| P2 - Curelo Repeat | 17,338 | 13.9% |
| L1 - Lab New | 94 | 0.1% |
| L2 - Lab Repeat | 17 | 0.0% |
| C - Corporate Lead | 6 | 0.0% |
| `P1`/`P2` (bare, no prefix text) | 24 / 19 | legacy/inconsistent values, pre-dating the current dropdown labels |

🟢 Nearly half of all leads have **no** `mx_Lead_Type` set — this field is populated later in the lifecycle (likely at/after booking), not at lead-creation time, so it cannot be used as a lead-creation-time acquisition filter; it's only reliable once joined to the booking-stage activity data (which does populate `mx_Custom_8`/similar with the same P1/P2 values, confirmed in the Phase 6 booking-206 sample).

🔵 **Recommendation (per the user's explicit instruction not to decide this yet):** expose three independently queryable metrics rather than one blended number — **(1) new-lead acquisition** (lead-creation events, filtered to P1/L1/C or unset-at-creation), **(2) repeat-customer activity** (P2/L2, a retention metric, not an acquisition metric), **(3) total downstream conversions** (booking/fulfillment regardless of new/repeat) — and let the dashboard consumer pick the lens, rather than baking a new-vs-repeat decision into the ETL.

---

## Phase 8 — Cancellation / reversal semantics (🟢 live)

🟢 A booking can be cancelled (2,790 confirmed cancellation events/30d, code 223) — live sample shows `mx_Custom_1: "cancelled"`, `mx_Custom_6: "Booked by mistake"` (a reason-code field), and a monetary field `mx_Custom_5: "675.0"` (the cancelled booking's value). 🟢 2,118 of the 17,685 booked prospects (12%) have **both** a booking-created and a cancellation event.

🟡 Rescheduling exists as its own signal only for the **Diet Consultation** sub-flow (event 214, 35/30d) — no equivalent "Booking Rescheduled" event code was found for the core lab-test booking flow; a reschedule there most likely shows up as a status transition inside 208 rather than a dedicated event (consistent with 208's variable, JSON-blob-bearing custom fields).

🔵 **Recommendation (per instruction not to calculate revenue prematurely):** revenue should default to **net realized amount** — i.e., booking value minus any subsequent cancellation for the same booking ID (`mx_Custom_2`/`mx_Custom_3`-style booking-ID fields observed across 206/208/223 samples, human-readable e.g. `"601560"`, `"601777"`) — not gross booking value, since 12% of bookings are known to reverse. The exact booking-ID field position varies slightly by event type in the live samples (e.g. booking ID appears as `mx_Custom_2` on 206 but `mx_Custom_3` on 223) — **this must be confirmed per-event-type against `GetActivitySetting` (Phase-5's per-type field-metadata endpoint) rather than assumed from one sample**, since the same slot number means different things on different activity types.

---

## Phase 9 — Opportunities & Sales Activities (🟢 live — a real, active feature, previously unknown to this integration effort)

🟢 **The Opportunities feature is enabled** on this account (`GetOpportunityTypes` → 200, not the 404/disabled response). One opportunity type exists: **"Service"** (`EventCode: 12000`), created/modified by a real user (Riya Goyal), described as tracking "all the sales information."

🟢 **Opportunities are actively used via automation**, not manually — Activity Event **32 ("Duplicate Opportunity Detected")** fired 30,270 times in 30 days, and its payload shows the automation creating/checking `Service`-type opportunities with `Stage: "Prospect"` for essentially every lead, keyed by phone number and service-interest matching rules. This looks like an **automatic opportunity-per-lead creation rule**, not manual sales pipeline management.

🟡 **This is a meaningful overlap risk the user explicitly asked to guard against ("avoid double-counting revenue"):** if Opportunities carry their own amount/stage fields *in addition to* the Booking/Payment activity trail already analyzed in Phase 6, a naive "sum everything that looks like revenue" approach across both Activities and Opportunities would double-count. **This needs one more live check before building anything** — pulling actual Opportunity *records* (via `Opportunity Advanced Search`, not just the *type* definition) was not completed this pass (out of scope for a first pass, and the endpoint takes an `AdvancedSearch` query-language parameter that needs a worked example against this account's actual Service-type field layout — a 15-minute follow-up, not a blocker). 🔵 **Recommendation:** treat this as an explicit open item — do not build revenue aggregation until it's confirmed whether "Service" opportunities carry monetary fields independent of the booking/208 amounts.

---

## Phase 10 — Volume & API cost model (🟢 live rate limits + volumes; 🔵 sizing)

🟢 **Confirmed plan limits** (from LeadSquared's published rate-limit page — plan tier not independently confirmed against the account, so treat the exact ceiling as the lower/Pro-tier number until verified): 10,000 calls/day base (up to 250,000/day with add-ons), 10 calls/5 sec standard, 5 calls/5 sec for bulk endpoints. 🟢 **Confirmed live:** `Leads.RecentlyModified` max `PageSize` = 5,000; `CustomActivity/RetrieveByActivityEvent` max `PageSize` = 1,000 (this session's own probe hit the exact `MXInvalidInputException` boundary).

🟢 **Live volumes, trailing 30 days:**

| Stream | 30-day volume | Daily average |
|---|---|---|
| Leads (all sources, modified) | 156,448 | ~5,215/day |
| Booking Created (206) | 23,071 | ~769/day |
| Post Booking Order Status (208) | 149,656 | ~4,989/day |
| Booking Cancelled (223) | 2,790 | ~93/day |
| Payment Success (225) | 110 | ~4/day |
| WhatsApp Message (203) | 132,732 | ~4,424/day |

🔵 **Backfill call estimate** (90-day historical window, matching this platform's `DEFAULT_BACKFILL_DAYS=90` convention): leads ≈ 156,448 × 3 (90d/30d) ÷ 5,000 per page ≈ **~95 calls**. 208 (highest-volume activity, if ingested) ≈ 149,656 × 3 ÷ 1,000 ≈ **~450 calls**. 206 ≈ **~70 calls**. 223 ≈ **~9 calls**. Total backfill for leads + the three core activity streams: **well under 1,000 calls** — trivially inside even the base 10,000/day limit, completed in minutes given the 5-calls/5-sec bulk throttle (~1 call/sec sustained ⇒ under 15 minutes total).

🔵 **Daily incremental estimate**, using the same bulk date-range pattern the other 5 connectors already use (not per-lead calls, which the user correctly flagged to avoid): leads ≈ 2 calls/day, 208 ≈ 5 calls/day, 206 ≈ 1 call/day, 223 ≈ 1 call/day. **Under 10 calls/day** for the core streams — negligible against any plan tier.

🔵 **Recommendation:** a 3-hour rolling sync interval (matching the tier already chosen this session for Google Ads/Meta Ads) is comfortably sufficient — there's no rate-limit or volume pressure to justify anything tighter, and it keeps the LSQ connector consistent with the existing scheduling tiers rather than introducing a fourth cadence.

---

## Phase 11 — Time semantics (🟢 live, empirically settled — not just doc-derived)

🟢 **Definitively confirmed live: LSQ `CreatedOn`/`ModifiedOn` timestamps are in UTC**, not IST. Method: queried the most-recently-modified leads twice with the *same* `FromDate` but two different `ToDate` values — one computed as "now" in true UTC (06:38:11), one computed as "now" if LSQ's clock were actually IST (12:08:11, i.e. UTC+5:30 applied a second time). Both queries returned **identical** results (467 records, the same 3 most-recent leads, `ModifiedOn` values clustering at 06:27–06:31). Since the real most-recent activity timestamps line up with true UTC-now (within a normal few-minutes lag) and not with the falsely-shifted IST-now, the account's timestamps are UTC. This matches the documentation's stated format but was not simply assumed — it was tested.

🟢 **`FromDate`/`ToDate` on `Leads.RecentlyModified` filters by `ModifiedOn`, not `CreatedOn`** — confirmed by constructing a window tightly around one lead's own `CreatedOn` value (which returned zero results) versus the same lead's `ModifiedOn` value (which returned a match). This is exactly correct behavior for an endpoint literally named "RecentlyModified," and is in fact the *right* semantic for incremental sync (we want "what changed," not "what was created") — but it means a connector must **not** assume `CreatedOn`-based windowing will find genuinely new leads that were modified outside the query window; the cursor field for incremental sync must be `ModifiedOn`.

🟡 **Boundary inclusivity (start/end exact-match) was not conclusively determined** — a zero-width or ±1-second probe window returned inconsistent results likely due to true sub-second timestamp precision not being visible in the truncated `HH:MM:SS` display (the underlying value likely carries milliseconds LSQ doesn't render). 🔵 **Recommendation:** handle this the same way every other connector in this codebase already handles provider-side boundary ambiguity — an overlapping lookback window (this platform's existing `lookback_days`/`DEFAULT_LOOKBACK_DAYS=3` pattern) plus upsert-on-conflict, rather than trying to nail exact inclusive/exclusive semantics. This sidesteps the ambiguity entirely and is already the established pattern (`connections.lookback_days`).

---

## Phase 12 — Data quality audit (🟢 live, full 125K sample + per-source table in Phase 2)

| Check | 🟢 Live count (of 125,000) |
|---|---|
| Campaign ID present, campaign name missing | 1,897 (1.5%) |
| Campaign name present, campaign ID missing | 9,312 (7.4%) |
| Ad ID present, ad name missing | 5,130 (4.1%) |
| GCLID present, campaign ID missing | 9,079 (7.3% — more than half of all 15,207 GCLID-bearing leads have no campaign ID at all) |
| UTM keyword present, keyword-ID missing | 0 |
| Duplicate `ProspectID`s in the sample | 0 |

🟢 **The GCLID-without-campaign-ID finding is the most actionable one here:** 60% of leads that carry a GCLID have a blank `mx_Source_Campaign_ID`. Since GCLID can't be joined to the warehouse anyway (Phase 3), this doesn't block the recommended join hierarchy — but it does mean GCLID-only leads (no campaign/ad/adset ID at all) are attribution-orphans under the recommended design, worth surfacing as an explicit "unattributable paid lead" count rather than silently dropping them.

🟢 **Cross-platform ID contamination** (flagged in Phase 3): a portion of leads bucketed by `Source` as "Google" carry Meta-format numeric IDs in `mx_Source_Campaign_ID`, and vice versa is plausible though not separately confirmed. This is the single most important data-quality caveat for the join design — **`Source` alone is not sufficient to decide which platform's `ad_entities` table to join against; the ID's own format should be validated first.**

🟢 **Zero duplicate `ProspectID`s** in the 125K sample — the identity model is clean on the primary key.

🟡 **Multi-touch / multiple-sources-over-time per prospect was not directly measured** this pass (would require pulling a `Lead by ID` history or a second RecentlyModified pass keyed by `ProspectID` looking for `Source` changes across time — not attempted, flagged as a follow-up if needed) — but the cross-platform ID contamination above is circumstantial evidence that it happens.

---

## Phase 13 — Identity / join design (synthesizing the above)

**Canonical identifiers, 🟢 live-confirmed to exist:**

| Entity | Canonical ID | 🟢 Confirmed |
|---|---|---|
| Lead | `ProspectID` (GUID) | Zero duplicates in 125K sample |
| Activity (any type) | `ProspectActivityId` (GUID) | Present on every activity record sampled |
| Booking | a human-readable numeric ID inside the activity's custom fields (e.g. `"601777"`) — **not** a top-level LSQ field; position varies by activity type | Present but needs per-type confirmation (Phase 8) |
| Payment | no dedicated ID observed distinct from the booking ID | Not confirmed as a separate entity |
| Opportunity | `OpportunityId` (GUID, per the doc-derived `Opportunity Advanced Search` response shape) | Type-level only confirmed live; record-level not pulled this pass (Phase 9) |
| Google campaign/ad-group/ad | `ad_entities.external_id` (level-scoped) | 🟢 |
| Meta campaign/adset/ad | `ad_entities.external_id` (level-scoped) | 🟢 |
| Google keyword | **no warehouse entity exists** | 🟢 confirmed absent |

🔵 **Recommended join hierarchy** (strongest → weakest, separated into three distinct kinds of join as the instructions require):

1. **Identity join** (LSQ-internal, always deterministic): `Lead.ProspectID` ↔ `Activity.RelatedProspectId`. This is a GUID-to-GUID match, 100% reliable — every activity table in the proposed connector design keys off this.

2. **Attribution join** (LSQ → ad platform, probabilistic in practice even though structurally deterministic):
   - **Meta:** `mx_Ad_Id` → `ad_entities` (provider=meta, level=ad) — **96% match rate, the strongest link found**. Fall back to `mx_Adset_Id` (71%) or `mx_Source_Campaign_ID` (50%) only when ad-id is missing.
   - **Google:** `mx_Source_Campaign_ID` → `ad_entities` (provider=google, level=campaign) — **72% match rate**, the best available Google key. `mx_Adset_Id` (doc: doubles as Google ad-group id) → level=ad_group is close behind (70%). `mx_Ad_Id` → level=ad_group_ad is weakest (57%).
   - **Before either lookup:** validate the ID's numeric-format pattern against the platform it's being joined to (Phase 12 cross-contamination finding) rather than trusting `Source` alone to pick the table.
   - **Never** use `mx_GCLid` as a warehouse join key (Phase 3) — store it, don't join on it.
   - **Never** use campaign/ad *names* as the primary join (per the explicit instruction) — the name-vs-ID mismatch rates in Phase 12 rule that out; names are a display fallback only, for the ~1–7% of rows where the ID side is missing.

3. **Lifecycle join** (LSQ-internal, within the funnel): `Lead.ProspectID` = `Booking(206).RelatedProspectId` = `PostBookingStatus(208).RelatedProspectId` = `Cancellation(223).RelatedProspectId`, further keyed by the embedded booking-ID custom field (once its exact per-activity-type field position is confirmed) to link a specific booking to its own specific status/cancellation events, rather than just "this prospect has some booking and some cancellation" (which the current data already shows can both be true for the *same* prospect across *different* bookings — 3,229 prospects have multiple 206 events).

---

## Phase 14 — Proposed architecture (informed by all of the above; **no code written**)

### Raw / normalized tables (new, matching this platform's existing `<connector>_performance` + entity pattern where it fits, and departing from it where the data genuinely doesn't fit — LSQ is individual-record grain, not daily-aggregate)

- **`leadsquared_leads`** — one row per `ProspectID` snapshot (upsert on `ProspectID`, keyed to `ModifiedOn` as the cursor). Columns: identity (`ProspectID`, `Phone`, `EmailAddress`), attribution (`Source`, `SourceCampaign`, `mx_Source_Campaign_ID`, `mx_Ad_Id`, `mx_Ad_Name`, `mx_Adset_Id`, `mx_Adset_Name`, `mx_GCLid`, `mx_utm_*`, `mx_Slug`), classification (`mx_Lead_Type`, `mx_Product_Service_Interest`), timestamps (`CreatedOn`, `ModifiedOn`), plus a `raw` JSONB catch-all (matching `PerformanceRowMixin`'s existing "never lose a field" convention).
- **`leadsquared_activities`** — one row per `ProspectActivityId`, **one physical table across all tracked event codes** (not one table per code) with `activity_event` (int) + `activity_event_note` + the full `mx_Custom_*` payload as JSONB `raw` (since the same slot number means different things per event type, per Phase 8 — don't try to flatten these into typed columns prematurely). Foreign-key-shaped column `related_prospect_id` for the lifecycle join. Streams: 206 (Booking Created), 223 (Booking Cancelled), 208 (Post Booking Order Status), 204 (Facebook Lead Ads Submissions, for the Meta attribution corroboration in Phase 4) as the initial set — **not** 225 (Payment Success), given its confirmed near-total absence from the actual funnel (Phase 6), pending the ops-team confirmation of what 208's status values actually mean.

### Attribution / lifecycle views (not tables — computed, matching this platform's existing "derived ratios are never stored" philosophy)

- A view joining `leadsquared_leads` → `ad_entities` using the Phase-13 hierarchy, exposing a `matched_platform`, `matched_level`, and `match_confidence` (deterministic per the table above, not a fuzzy score) per lead — so "how much of our paid lead volume is actually traceable" is itself a queryable, auditable number, not an assumption baked silently into a join.
- A funnel view chaining lead → booking(206) → status(208)/cancellation(223), **once** the booking-ID field position and the 208 terminal-status value(s) are confirmed with Curelo's ops owner (Phase 6/8's explicit open item) — this view should not be built before that conversation happens.

### Sync mechanics

- **Auth:** static `accessKey`/`secretKey` query params — a new, simpler provider pattern alongside the existing OAuth `DatabaseTokenProvider`, not a variant of it.
- **Cursor:** `ModifiedOn`, confirmed live as the actual filter semantic (Phase 11) — not `CreatedOn`.
- **Backfill:** 90 days, ~1,000 calls total, well inside rate limits (Phase 10).
- **Incremental:** rolling interval (3h, matching the existing Google Ads/Meta Ads tier) with the existing `lookback_days` overlap-and-upsert pattern to absorb the unresolved exact-boundary-inclusivity question (Phase 11) rather than requiring a resolved answer.
- **Dedup/upsert:** `ProspectID` for leads, `ProspectActivityId` for activities — both GUIDs, both confirmed zero-duplicate in this session's sampling.
- **Rate-limit handling:** page at the confirmed per-endpoint caps (5,000 leads / 1,000 activities), throttle to the bulk-endpoint limit (5 calls/5 sec), retry-with-backoff on 429 — matching the existing connector framework's conventions.
- **Data-quality checks:** surface (not silently drop) (a) leads with a GCLID but no campaign ID, (b) leads whose attribution-ID format doesn't match their `Source`-implied platform, (c) new/unrecognized `Source` values not in the classification allow-list — all three are real, live-confirmed occurrences, not hypothetical edge cases.
- **Reconciliation check:** the "how much paid volume is traceable" view above, run after every sync, compared against the raw `Source`-level paid-lead count — a sustained drop would catch a join or ingestion regression early.

### Minimum MCP analytical primitives for later (not built yet)

- "Leads by source, with attribution completeness" (the Phase 2 table, live-refreshable)
- "Attributed leads → booking → fulfillment funnel, by campaign/ad" (once Phase 6/8/9's open items are resolved)
- "Unattributable paid leads" (GCLID-without-campaign-ID and similar orphans, Phase 12)
- "New vs. repeat customer split" (Phase 7's three-metric recommendation)

---

## Open items before coding starts (carried over from the chat-level summary, now grounded in live data)

1. **208's status-field semantics** — which `mx_Custom_6`-style status value(s) represent a completed/fulfilled/paid booking. Needs Curelo's ops/CRM owner, not guessable from samples (Phase 6/8).
2. **Opportunity records** (not just the type definition) — pull real "Service" opportunity records via `Opportunity Advanced Search` to rule out double-counted revenue against the booking/208 trail (Phase 9).
3. **Booking-ID field position per activity type** — confirm via `GetActivitySetting` for each of 206/208/223 rather than assuming the sample-observed slot generalizes (Phase 8).
4. **New-vs-repeat inclusion in acquisition reporting** — a business decision, not a technical one (Phase 7); this report deliberately proposes three separate metrics instead of deciding.
5. **Plan-tier rate limit confirmation** — the published limits (Phase 10) weren't matched against this specific account's actual plan; worth a one-line confirmation from LSQ's account dashboard or support before finalizing sync cadence (though at the observed volumes it's unlikely to matter either way).

No code, migrations, or schema changes were made. This document and the underlying live-probe outputs (now cleared from the scratch directory) are the only artifacts produced this session.
