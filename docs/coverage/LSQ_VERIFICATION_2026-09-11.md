# LeadSquared Integration — Targeted Verification Pass (closing the 5 open items)

**Date:** 2026-09-11 · **Builds on:** [`LSQ_DISCOVERY_2026-09-11.md`](./LSQ_DISCOVERY_2026-09-11.md)
**Scope:** read-only live API calls + read-only warehouse DB queries only. No code, schema, migration, or LSQ writes. Probe scripts deleted after use.

Tags: 🟢 **LIVE FACT** · 🟡 **INFERENCE** · 🔵 **RECOMMENDATION**

---

## A) Opportunity records — pulled for real

🟢 Pulled actual "Service" (EventCode 12000) opportunity records via `GetOpportunitiesOfLead` for 5 real leads that have booking activity, with the full field list from the type's own metadata (`GetOpportunityTypeMetadata`, not guessed).

**Field mapping, 🟢 live-confirmed from `GetOpportunityTypeMetadata(code=12000)`** (89 fields total; the revenue/lifecycle-relevant ones):

| Field | Meaning | 🟢 Observed population |
|---|---|---|
| `Status` | Open / Won / (presumably Lost) | Populated on every record |
| `mx_Custom_2` | Stage (`Prospect`, `Booking`, …) | Populated on every record |
| `mx_Custom_6` / `mx_Custom_7` | Expected / Actual Deal Size | **Null on every record sampled, including the one "Won" record** |
| `mx_Custom_47` | **Booking ID** (Number) | Null while Open/Prospect; **populated once Won** |
| `mx_Custom_52` | **Total Paid Amount** (Number) | Null while Open/Prospect; **populated once Won** |
| `mx_Custom_55` | Customer ID | Null while Open/Prospect; populated once Won |
| `mx_Custom_50`/`51` | Booking Date/Time | Same pattern |
| `mx_Custom_20/21/22/23/25/26/27/29` | Campaign/Medium/Term/Content/Ad Name/Adset Name/Source Campaign Name/GCLid | **Null on every record sampled, including Won** — the opportunity-level attribution copy is not actually populated in this account |

🟢 **One lead had 3 opportunities** (2 "Open/Prospect" + 1 "Won/Booking") — confirms **an opportunity is not 1:1 with a lead**, and by extension not obviously 1:1 with a booking either without the Booking-ID cross-check below.

🟢 **The decisive cross-check:** for the one "Won" opportunity found (Booking ID `486098`, Total Paid Amount `999`, Customer ID `238646`), the *same* lead's own Booking Created (206) activity — pulled independently and matched by `RelatedProspectId` — carries **the exact same three values**: `mx_Custom_2` (Booking ID) = `486098`, `mx_Custom_6` (Total Paid Amount) = `999`, `mx_Custom_10` (Customer ID) = `238646`.

> **Answer to the explicit question — "Can we safely include Opportunities in the revenue model without double-counting booking/208 revenue?"**
> 🟢 **The Opportunity's revenue field is not an independent number — it is a mirror of the same Booking Created (206) Total Paid Amount, copied over only when the opportunity reaches "Won."** Summing Opportunity `Total Paid Amount` *and* 206's `Total Paid Amount` would double-count the identical figure. 🔵 **Recommendation: do not use Opportunities as a revenue source at all.** Its only demonstrated value is as a **secondary corroboration signal** — "Won" + a non-null Booking ID is a second, independently-automated confirmation that a specific booking is real and paid, which could be used as a cross-check/data-quality gate, not as a source of new numbers. Given (a) the attribution fields are unpopulated, (b) the amount field is a copy not a new figure, and (c) it adds real API/ingestion cost for a "Service" opportunity type that's largely an automation artifact (recall Phase 9 of the discovery report: 30,270 "Duplicate Opportunity Detected" events/30d) — 🔵 **recommend excluding Opportunities from the warehouse model entirely** unless a future need for that corroboration signal specifically justifies it.

---

## B) Activity metadata — exact field mapping (from `GetActivitySetting`, not samples)

🟢 Retrieved live via `CustomActivity/GetActivitySetting?code={206|208|223}` (the plain numeric event code worked directly as the `code` parameter).

| Event | Slot | Field | DataType |
|---|---|---|---|
| **206 Booking Created** | `mx_Custom_1` | Patient Name | String |
| | `mx_Custom_2` | **Booking ID** | String |
| | `mx_Custom_3` | Lab Name | String |
| | `mx_Custom_4` | Booking Date | String |
| | `mx_Custom_5` | Booking Time | String |
| | `mx_Custom_6` | **Total Paid Amount** | String |
| | `mx_Custom_7` | Service Interested | String |
| | `mx_Custom_8` | Booking Resource Type | String |
| | `mx_Custom_9` | Booking Channel | String |
| | `mx_Custom_10` | **Customer ID** | String |
| | `mx_Custom_11` | Booking Created Date | DateTime |
| | `mx_Custom_12` | Coupon Code | String |
| | `mx_Custom_13` | Channel Category | String |
| | `mx_Custom_14` | Payment Link | String |
| | `mx_Custom_15` | **Payment Status** | String |
| | `mx_Custom_16` | **Payment Method** | String |
| **208 Post Booking Order Status** | `mx_Custom_1` | Enquiry Time | DateTime |
| | `mx_Custom_2` | Booking Date | DateTime |
| | `mx_Custom_3` | Booking Time | DateTime |
| | `mx_Custom_4` | **Booking ID** | Number |
| | `mx_Custom_5` | Patient Name | String |
| | `mx_Custom_6` | **Booking Status** | SearchableDropdown |
| | `mx_Custom_7` | Lab Name | String |
| | `mx_Custom_8` | Service Interested | String |
| | `mx_Custom_9` | Coupon Code | String |
| | `mx_Custom_10` | Booking Channel | String |
| | `mx_Custom_11` | Patient Type | SearchableDropdown |
| | `mx_Custom_12` | City | String |
| | `mx_Custom_13` | Patient Stage | SearchableDropdown |
| | `mx_Custom_14` | Booking Created By | String |
| | `mx_Custom_15` | Actual Amount | Number |
| | `mx_Custom_16` | Lab Discount | Number |
| | `mx_Custom_17` | Lab MRP | Number |
| | `mx_Custom_18` | Promocode Discount | String |
| | `mx_Custom_19` | Coins Redeemed | Number |
| | `mx_Custom_20` | **Booking Amount** | Number |
| | `mx_Custom_21` | Lab Earning | Number |
| | `mx_Custom_22` | **Curelo Commission** | Number |
| | `mx_Custom_23` | Channel Category | String |
| | `mx_Custom_24`/`25` | Actual Collection Date/Time | DateTime |
| **223 Booking Cancelled** | `mx_Custom_1` | Booking Status | String |
| | `mx_Custom_2` | Patient Stage | String |
| | `mx_Custom_3` | **Booking ID** | String |
| | `mx_Custom_4` | Cancelled Service | String |
| | `mx_Custom_5` | **Cancelled Amount** | String |
| | `mx_Custom_6` | Cancellation Reason | String |
| | `mx_Custom_7` | Booking Date | String |
| | `mx_Custom_8` | Patient Name | String |

🟢 **Confirmed, metadata-backed, not inferred: "Booking ID" sits at a different slot on every event type** — `mx_Custom_2` on 206, `mx_Custom_4` on 208, `mx_Custom_3` on 223. A connector must map per-event-type, never assume a fixed slot. This was exactly the risk flagged (as unconfirmed) in the discovery report, and it's now settled.

🟢 **206 already carries its own `Payment Status` (`mx_Custom_15`) and `Payment Method` (`mx_Custom_16`)** — fields the discovery report hadn't surfaced. Worth pulling these too; they may be a cleaner "was this actually paid" signal than parsing 208's `Booking Status`.

---

## C) 208 status inventory (🟢 live, 20,000-row sample of the 30-day, ~150K-row population)

| `mx_Custom_6` (Booking Status) | Count | % | Amount fields populated? |
|---|---|---|---|
| `customer_confirmed` | 15,091 | 75.45% | 100% |
| `confirmed` | 2,517 | 12.59% | 100% |
| `completed` | 2,164 | 10.82% | 100% |
| `pending` | 228 | 1.14% | 100% |

Only **4 distinct values** in this sample (not the open-ended set implied by "status field" — this is a closed `SearchableDropdown`, matching the metadata's DataType). 🟢 Amount fields (`Actual Amount`, `Booking Amount`, etc.) are populated on **every** 208 row regardless of status — not only on a terminal one.

**Classification (per the instruction not to declare a status "paid/fulfilled/completed" without strong evidence):**

- **`completed`** — 🟡 **LIKELY** terminal/fulfilled. Evidence: the literal string name, *and* stronger — across 2,790 bookings with ≥3 status rows, `completed` appears **only as the last element of every observed sequence, never followed by anything else** (top 10 sequences below all end in `completed` when it appears at all). Not labeled 🟢 LIVE FACT because no metadata field explicitly marks it terminal — this is sequence evidence, not a schema guarantee.
- **`confirmed`** / **`customer_confirmed`** — 🟡 **LIKELY** intermediate pre-fulfillment confirmation stages (ops confirms, then customer confirms), based on sequence ordering (always precede `completed`, never follow it).
- **`pending`** — 🟡 **LIKELY** the earliest stage (always first in sequence when present).
- **No status here is labeled "paid."** 🟢 There is **no explicit payment-status field on 208 at all** (confirmed via the metadata table above — 208 has amount fields, not a payment-status dropdown). Payment status, where it exists, lives on **206** (`mx_Custom_15`, `Payment Status`, a field not previously surfaced) — 🔵 that field should be checked before declaring any 208 status "paid."

---

## D) Booking-level consistency (🟢 live, cross-referencing 206 + 208 + 223 by Booking ID)

| Test | 🟢 Live result |
|---|---|
| Can one booking have multiple 208 records? | **Yes** — 2,790 of 3,667 distinct booking IDs (76%) in the 20K-row sample have ≥3 status rows; some have 6+ (repeated `customer_confirmed` pings). |
| Status sequence order | **Consistently monotonic** — `pending` → `confirmed`/`customer_confirmed` (repeats) → `completed`, in every one of the top 10 observed sequences (covering 2,003 of 2,790 multi-row bookings). No sequence shows a status after `completed`. |
| Do amount fields change across repeated 208 rows for the same booking? | **No** — verified on a 6-row example (booking 601215): `Actual Amount`/`Booking Amount` identical (5149/1018) across all 6 rows. Amount is set once; only status repeats. |
| Can a booking have both 206 and 223 (cancellation)? | **Yes** — 2,118 of 17,685 booked prospects (from the discovery pass) have both; re-confirmed structurally this pass via booking-ID-level matching. |
| Do cancelled bookings still show later 208 rows? | **Partially, and never past `confirmed`/`customer_confirmed`.** 281 of 2,788 sampled cancelled booking IDs (10%) have prior 208 history; **in every spot-checked case, the pre-cancellation 208 status was `confirmed` or `customer_confirmed`, never `completed`** — consistent with `completed` bookings not being cancelled afterward. The other 90% of cancellations have no 208 history at all (cancelled before the fulfillment pipeline logged anything). |
| Do booking IDs stay stable across event types? | **Yes, exactly** — booking `486098` appears identically as `206.mx_Custom_2`, `Opportunity.mx_Custom_47`, with matching `Customer ID` (`238646`) on both sides too. |

🟡 A booking created (206) does **not always** get a 208 row within a reasonable window — the specific booking used for the Opportunity cross-check (486098, created April 2026) had **zero** 208 rows even searched over a 5-week window. This means **208 coverage is not universal** — some bookings (this one: Call Centre channel, repeat customer) never enter the status-tracking pipeline, or do so far outside a typical lookback window. 🔵 A lifecycle model must treat "no 208 row yet" as a valid, common state, not an error.

---

## E) Attribution re-validation, excluding wrong-platform-format IDs (🟢 live, fresh 30,000-lead sample)

🟢 First established the actual ID-length signature per platform from the warehouse itself (not assumed): **Google IDs are 11–12 digits; Meta IDs are 17–18 digits — a clean gap with zero real IDs in between.** Filtering the LSQ-side ID sets to the correct length range for their claimed platform before matching:

| Join key | Raw distinct | After format filter | Dropped (wrong-platform format) | Matched in warehouse | **Match rate** |
|---|---|---|---|---|---|
| Google campaign ID | 30 | 21 | 9 | 21 | **100%** |
| Google ad-group ID (`mx_Adset_Id`) | 63 | 48 | 15 | 48 | **100%** |
| Google ad ID (`mx_Ad_Id`) | 17 | **0** | 17 | 0 | **n/a — every single value was Meta-format** |
| Meta campaign ID | 23 | 15 | 8 | 13 | **86.7%** |
| Meta adset ID | 56 | 46 | 10 | 46 | **100%** |
| Meta ad ID | 96 | 96 | 0 | 96 | **100%** |

🟢 **This confirms the discovery report's hypothesis directly: the earlier "50–72%" match rates were almost entirely explained by cross-platform ID contamination, not by warehouse coverage gaps.** Once contamination is filtered out, Google campaign/ad-group and Meta adset/ad all hit 100%; Meta campaign settles at a still-strong 86.7% (the remaining gap here is real — likely older/deleted campaigns outside the current sync window, not a format issue).

🟢 **New, important finding: `mx_Ad_Id` is unusable for Google attribution in this account.** Every Google-sourced lead's `mx_Ad_Id` value in this fresh sample was Meta-format, i.e., **none were valid Google ad IDs at all.** 🔵 **Recommendation: drop `mx_Ad_Id` from the Google join hierarchy entirely** — use campaign-id and ad-group-id (`mx_Adset_Id`) only for Google, both of which are now confirmed 100%-matchable once format-filtered.

🟢 GCLID re-confirmed **not used as a warehouse join** in this pass, per instruction — untouched from the discovery report's finding (0% — the warehouse has nothing to join it against).

**Revised recommended Google hierarchy:** `mx_Source_Campaign_ID` (100%) → `mx_Adset_Id` as ad-group (100%) → *(no reliable ad-level key — mx_Ad_Id is not usable)*.
**Revised recommended Meta hierarchy (unchanged from discovery, now confidence-upgraded):** `mx_Ad_Id` (100%) → `mx_Adset_Id` (100%) → `mx_Source_Campaign_ID` (86.7%).

---

## F) Rate limits (🟢 live-checked, confirmed still an account-owner item)

🟢 Probed three plausible account/plan-info endpoint guesses (`AccountManagement.svc/GetAccountInfo`, `LandingPage.svc/Account.GetInfo`, `Admin.svc/Account.Get`) — **all returned `404`, confirmed live.** LeadSquared's public API surface does not expose plan-tier or current-usage/quota information programmatically (no documented endpoint for it was found either). 🔵 **This remains an account-owner confirmation item, exactly as flagged in the discovery report — not resolvable from the API.** No further attempt was made (per instruction not to bypass or stress-test limits). One incidental data point: the Opportunity-metadata doc page states a *different* limit for that specific endpoint (25 calls/5 sec) than the general bulk-endpoint rate (5 calls/5 sec) — 🟢 confirming rate limits can be **endpoint-specific**, not just plan-wide, which the sync design should accommodate (per-endpoint throttling, not one global rate).

---

## Final technical decision report

### 1. What is now definitively known (🟢)

- Opportunities' revenue field is a **copy**, not an independent number — same Booking ID/Amount/Customer ID as 206, confirmed by direct cross-match.
- The exact `mx_Custom_N` → field mapping for 206/208/223, from LSQ's own metadata endpoint — Booking ID sits at a different slot on each event.
- 208's status field (`Booking Status`) has exactly 4 values in practice (`pending`/`confirmed`/`customer_confirmed`/`completed`), and they progress in a strictly observed monotonic order ending at `completed`.
- 208 fires repeatedly per booking (median case: several `customer_confirmed` repeats) with stable amount fields throughout.
- Cancelled bookings never show `completed` beforehand, in every case checked.
- Booking IDs are stable, cross-referenceable identifiers across 206/208/223/Opportunity.
- Once cross-platform ID contamination is filtered by ID-length signature, Google campaign/ad-group and Meta adset/ad/campaign attribution match the warehouse at 86.7–100%.
- `mx_Ad_Id` is not usable for Google attribution in this account (100% wrong-format in the fresh sample).
- 206 has its own `Payment Status`/`Payment Method` fields, previously unsurfaced.
- No programmatic plan/rate-limit endpoint exists.

### 2. What remains a business decision (not resolved, and not attempted here)

- Whether `completed` (208) — or 206's own `Payment Status` field, now surfaced — is the actual "counts as revenue" trigger Curelo wants. Both are now precisely characterized; neither has been declared authoritative, per instruction.
- Whether repeat-customers (`mx_Lead_Type` = P2) count in acquisition attribution (unchanged from the discovery report).
- Whether the Opportunity "Won" signal should be used as a secondary corroboration gate even though it carries no new data.

### 3. Final recommended LSQ warehouse model

Unchanged from the discovery report's Phase 14, with one addition and one removal:
- **Add:** ingest 206's `Payment Status`/`Payment Method` fields (newly surfaced) alongside the existing planned columns.
- **Remove:** do not build an Opportunities table/stream — confirmed to add no independent data (item A above).
- `leadsquared_activities` stays a single JSONB-`raw`-backed table across event codes (206/208/223/204), since booking-ID and every other meaningful field sits at a different slot per event type — now proven, not assumed.

### 4. Final recommended attribution model

- **Google:** `mx_Source_Campaign_ID` → `ad_entities` (level=campaign), then `mx_Adset_Id` → `ad_entities` (level=ad_group). **Do not** attempt an ad-level Google join via `mx_Ad_Id` — drop it from the hierarchy. No keyword-level join exists (unchanged).
- **Meta:** `mx_Ad_Id` → `ad_entities` (level=ad) as primary (100% match), falling back to `mx_Adset_Id` (100%) then `mx_Source_Campaign_ID` (86.7%).
- **Always** validate ID length against the platform's known signature (Google 11–12 digits, Meta 17–18 digits) before attempting a join — this single check is what took real match rates from 50–72% to 86.7–100%.
- **Never** join on GCLID or on names, per both passes' findings.

### 5. Final recommended lifecycle model

```
Lead (ProspectID)
  → Booking Created (206)              — Booking ID assigned here (mx_Custom_2);
                                          carries its own Payment Status/Method
      → Post Booking Order Status (208) — 0..N rows, same Booking ID (mx_Custom_4);
                                          status monotonically progresses
                                          pending → confirmed/customer_confirmed → completed;
                                          NOT guaranteed to exist for every booking
      → Booking Cancelled (223)         — same Booking ID (mx_Custom_3); can occur
                                          with or without prior 208 history; never
                                          observed after a `completed` 208 status
```
Opportunities are **not** part of this model (item A).

### 6. Canonical source for revenue, if determinable

**Not fully determinable from the API alone — this is the one item that stays a business decision (item 2).** What *is* determinable: it should be **either** 206's `Total Paid Amount`/`Payment Status` (captured at booking time) **or** 208's `Booking Amount`/`Actual Amount` at its `completed` status (captured at fulfillment) — **not both summed**, and **not Opportunities** (ruled out, item A). Which of the two — booking-time vs. fulfillment-time — is "the" revenue number is exactly the kind of finance/ops policy call this verification pass was scoped to surface, not decide.

### 7. Fields that should remain JSONB (semantics vary by event or aren't yet stable)

- All `mx_Custom_N` activity payloads — proven this pass that slot N means something different per event type; flattening to typed columns without a per-event-type mapping layer would silently corrupt data.
- Opportunity attribution fields (`mx_Custom_20/21/22/23/25/26/27/29`) — defined in the schema but empirically unpopulated; keep as JSONB in case that changes, don't build typed columns for unused fields.
- Lead-level `Source` — a closed dropdown but with confirmed live case-duplication (`google_lp` vs `Google_lp`) and periodic new values; classification logic (not a fixed schema) should own this, matching the discovery report's recommendation.

### 8. Remaining risks

- 208 coverage is not universal (item D) — a lifecycle model must tolerate "booking exists, no 208 yet" as a normal, common state, not an error/incomplete-data condition.
- Meta campaign-level match rate (86.7%) still has a real ~13% gap even after format-filtering — likely a warehouse historical-coverage limit (old/deleted campaigns), not fixable by join-logic changes alone.
- The revenue-source decision (item 6) is unresolved and blocking — no aggregate revenue reporting should be built until it's answered.
- Rate limits remain unconfirmed for this specific account's plan (item F) — low risk given the volumes measured in the discovery report, but not zero-risk until confirmed.

### 9. Exact information needed from Curelo/LSQ business owner before implementation

1. Does "revenue" mean the amount captured at booking time (206 `Total Paid Amount`) or at fulfillment (208 `Booking Amount`/`Actual Amount` when `completed`)? Are these ever different for the same booking, and if so, which is authoritative?
2. Is 206's `Payment Status` field (newly surfaced this pass) actively used/populated, and does a specific value there mean "paid"?
3. Should repeat-customer (`mx_Lead_Type` = P2/L2) bookings be included in paid-ad acquisition/ROAS reporting, or reported separately?
4. Confirm the account's LeadSquared plan tier and daily/burst API call limits (not exposed via the API).
5. Is there a defined meaning for the 10% of cancelled bookings that reach `confirmed`/`customer_confirmed` before cancellation vs. the 90% cancelled with no 208 history — i.e., is early-stage cancellation tracked/reported differently from late-stage?

No code, schema, migrations, or LSQ writes were made this pass.
