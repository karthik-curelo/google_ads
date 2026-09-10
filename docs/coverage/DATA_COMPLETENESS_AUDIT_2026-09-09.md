# Google Ads Data-Completeness Audit — Findings + Resolution (2026-09-09)

**Scope:** not "did the stream succeed" but "can every field-level requirement
implied by the screenshot's reports actually be reproduced from the
warehouse." This audit follows `ACCEPTANCE_AUDIT_GO.md` (which verified the
screenshot's 18 reports at the stream level) and goes one level deeper: field
presence, join completeness, and conversion-action granularity.

**Result: two genuine P1 gaps found, both independently confirmed by the
user against live Google Ads API v25 docs, both now implemented and
live-verified.** No other gap from that audit pass rose to P1 — see
`ACCEPTANCE_AUDIT_GO.md` for the full screenshot-report matrix, which is
unaffected by this change (both fixes are additive).

---

## Finding A — search terms had no link to the triggering keyword

**Before:** `search_term_performance` stored `search_term` + `match_type` +
`ad_group.id`, but nothing identifying *which keyword* caused Google to serve
an ad for that search. You could tell which ad group a search term came
from, not which keyword.

**Verification that this is real, not theoretical:** live-probed
`segments.keyword.ad_group_criterion` / `.info.text` / `.info.match_type` on
`search_term_view` against customer 9232673741 — HTTP 200, real data. Then
compared row counts for the identical 5-day window with vs. without the
keyword segment: **14,539 rows without it, 15,002 with it** — Google
pre-aggregates multiple triggering keywords into one row when the keyword
segment isn't requested, and decomposes them correctly when it is. Directly
found **830 `(search_term, ad_group)` pairs mapping to more than one distinct
keyword** in that 5-day sample (e.g. "mri near me" under ad group
`194345089574` triggered by two separate keyword criteria).

### Implementation
- `app/connectors/google/ads.py`, `search_term_performance` spec: added
  `segments.keyword.ad_group_criterion`, `segments.keyword.info.text`,
  `segments.keyword.info.match_type` to `dims`.
- **Primary key extended** to include `segments.keyword.ad_group_criterion`
  (the resource name — stable, always-unique-per-keyword ID; preferred over
  the mutable keyword text per the stable-ID rule). Necessary, not optional:
  without it, the newly-revealed per-keyword rows would collapse into each
  other as "duplicate primary key within batch" — the same failure mode
  fixed for placement views earlier this session.
- Null-safety preserved: the existing `_fact()` key-building logic (`"" if v
  is None else str(v)`) means a search-term row Google returns *without* a
  resolvable keyword survives as its own row (empty-string keyword key)
  rather than being dropped or silently merged into a resolved row.
- **Backfill:** the pk change means old rows (keyed without the keyword
  component) are a different logical grain than new rows — they were
  **purged** (97,327 rows, 2026-08-08 → 2026-09-09) and `sync_state` reset to
  force a full re-backfill under the new grain, rather than letting stale and
  new-grain rows coexist.
- Test: `test_ads_search_term_stream_preserves_multi_keyword_grain` in
  `tests/test_connectors.py` — three synthetic API rows (same search term,
  two different keywords, one row with no keyword at all) must produce three
  distinct warehouse rows.

### Live verification (sync run 149, full backfill)
| Check | Result |
|---|---|
| Stream status | `succeeded`, 0 failed |
| Rows fetched / inserted / updated / skipped | 100,837 / 100,837 / 0 / 0 |
| Total rows vs. distinct grain `(date, ad_group, term, match_type, keyword_criterion)` | 100,837 = 100,837 — **zero collapse** |
| Distinct `record_key` | 100,837 (matches row count — zero duplicates) |
| Rows with a resolved keyword | 100,837 / 100,837 (100% — this account's search-term data always resolves a keyword; the null-safe path is defensive, exercised by the unit test, not by this account's live data) |
| `(search_term, ad_group)` pairs mapping to >1 distinct keyword | **4,634**, over the full 33-day backfill |
| Distinct keyword criteria appearing in search-term data | 512 |

---

## Finding B — no per-conversion-action breakdown

**Before:** `conversions`/`conversion_value` on every performance stream
were aggregate totals across all conversion actions. The 78 conversion-action
*definitions* were captured (`ad_entities`, level=`conversion_action`) but
never joined to a day's performance — you couldn't tell how many of a
campaign's conversions were `Purchase` vs. `Submit lead form` vs. `Phone call
lead`.

**Verification:** live-probed `segments.conversion_action` (resource name),
`segments.conversion_action_name`, `segments.conversion_action_category` on
`campaign` — HTTP 200, real per-action rows. Critically also verified **which
metrics are safe to request alongside this segment**: `metrics.impressions`/
`clicks`/`cost_micros` are **not** decomposable by conversion action — Google
repeats the *entire* campaign-day's value on every action row, so summing
them across action rows would double- or triple-count spend. `conversions`/
`conversions_value` **are** correctly split per action.

### Implementation — separate, normalized stream, zero risk to existing streams
- `campaign_performance` and `ad_group_performance`: **unchanged**. Same
  grain, same dims, same upsert key, same metric set. No regression surface.
- New stream `campaign_conversion_action_performance`:
  - dims: `segments.date`, `campaign.id`, `campaign.name`,
    `segments.conversion_action`, `segments.conversion_action_name`,
    `segments.conversion_action_category`.
  - metrics: **only** `metrics.conversions`, `metrics.conversions_value` —
    deliberately excludes impressions/clicks/cost for the reason above.
  - pk: `(date, campaign.id, segments.conversion_action)` — the **resource
    name** (stable ID) is the key and the join target, not the mutable
    `conversion_action_name` string. The numeric ID embedded in that
    resource name (`customers/X/conversionActions/<id>`) is the same `id`
    stored in `ad_entities.external_id` for level `conversion_action`,
    giving a stable-ID join to the 78 existing definitions.
  - `ad_group_conversion_action_performance` was **not** added — the
    screenshot/reporting scope only calls for campaign-level breakdown, and
    the reconciliation test (below) was cleanest to define and verify at
    that grain. Flagged as an optional P2 if ad-group-level breakdown is
    later requested; the resource/segment support already confirmed here
    extends the same way to `ad_group`.
- Test: `test_ads_conversion_action_performance_maps_rows_and_is_separate_stream`
  in `tests/test_connectors.py` — asserts the stream requests exactly the
  two intended metrics, maps two different conversion actions to two
  distinct rows, and that summed conversions match the expected total.

### Live verification (sync run 149, first backfill of this new stream)
| Check | Result |
|---|---|
| Stream status | `succeeded`, 0 failed |
| Rows fetched / inserted / updated / skipped | 741 / 741 / 0 / 0 |
| Distinct `record_key` | 741 (matches row count — zero duplicates) |
| Distinct conversion actions with data in this window | 5 (`SUBMIT_LEAD_FORM` 617 rows, `ENGAGEMENT` 33, `DOWNLOAD` 33, `CONVERTED_LEAD` 29, `QUALIFIED_LEAD` 29 — of the 78 total defined, most are historical/removed and inactive) |
| Join to `ad_entities` (conversion_action definitions) by stable ID | 5 / 5 resolved |
| **Reconciliation — conversions**: sum of action-level rows vs. `campaign_performance` aggregate, per `(date, campaign)` | **99 / 102 pairs match exactly** (zero-tolerance, 0.01 epsilon) |
| **Reconciliation — conversion_value** | **102 / 102 pairs match exactly** |
| Non-reconciling cases explained | The 3 conversions mismatches are confined to the **2 most recent dates** (2026-09-09 = today, 2026-09-08 = yesterday), each off by <3 conversions (108.68 vs. 111.68; 73.22 vs. 75.22; 503.58 vs. 504.35). This is Google's normal conversion-attribution settling lag — the aggregate and the segmented query were evaluated at slightly different moments against still-updating recent-day data, exactly the same phenomenon `provider_lag_days`/the lookback re-fetch window already exists to handle elsewhere on this platform. It self-corrects on the next incremental sync's lookback pass. Not a data-integrity defect. |

---

## Net effect on prior audit conclusions

- `ACCEPTANCE_AUDIT_GO.md` — unaffected; both changes are additive to
  streams outside its 18-report screenshot matrix (Finding A extends an
  existing report's dimensions without changing its "report" status; Finding
  B is a wholly new stream not in the original screenshot's report list).
  Its **GO** verdict stands.
- `FINAL_AUDIT_2026-09-09.md` — superseded on nothing specific (it never
  claimed keyword-linking or conversion-action breakdown were complete or
  missing; those simply weren't in its scope). No correction needed there.
- This document is the durable record of the deeper data-completeness pass
  that found these two gaps (previously only reported in-conversation, never
  written to a file) and their resolution — both are now ✅ **implemented,
  live-verified, tested**, not outstanding.

**Stream count:** Google Ads connector now ships **31 streams** (30 + this
session's `campaign_conversion_action_performance`); `search_term_performance`
is unchanged in count but changed in grain (see Finding A).

---

## Closure audit (same day, run 150 — read-only, one lookback re-sync)

Re-verified both fixes independently, plus reran a fresh incremental
(lookback) sync to test whether recent-date reconciliation gaps close on
their own, as predicted.

**Finding A re-verified:** all 8 required fields (`search_term`,
`match_type`, `ad_group.id`, keyword resource name, keyword text, keyword
match type, `date`, all 13 base metrics) present on 100,837/100,837 rows.
Grain re-confirmed unique: total rows = distinct `(date, ad_group, term,
match_type, keyword)` = distinct `record_key` = 100,837. Directly inspected
the "mri near me" / ad group `194345089574` case end-to-end across its full
33-day history: on every day it appears, the two (or more) keywords that
triggered it land as separate rows with independent impressions/clicks/cost
(e.g. 2026-08-08: keyword "mri scan near me" → 1 impr/0 clicks; keyword "mri
near me" → 55 impr/8 clicks — not merged). Zero duplicate `(stream,
record_key)` pairs anywhere for this connection.

**Finding B re-verified:** all 7 required fields present on 741/741 rows;
grain `(date, campaign, conversion_action)` unique. `campaign_performance`/
`ad_group_performance` confirmed untouched — their `dimensions` keys are
still exactly `{campaign.id, campaign.name, segments.date}` /
`{ad_group.id, ad_group.name, campaign.id, segments.date}`, no
`conversion_action` leakage, same upsert key, same row counts as before this
work.

**Reconciliation re-run after a fresh lookback sync (run 150):** mismatches
dropped from **3 → 1**. The two dates that mismatched before (2026-09-08 and
one 2026-09-09 pair) now reconcile exactly after being re-touched by the
lookback window — direct proof the earlier gap was settling lag, not a
defect. The one remaining mismatch is `Search_CBC_Delhi_NCR` on **2026-09-09
(today, still in progress)** — breakdown 1.5 vs. aggregate 0.5, a 1.0
conversion difference — expected for the still-open current day and will
close on tomorrow's sync once today's data is final. Conversion-value
reconciled exactly on every pair, both times.

**New, separate finding — NOT caused by this work:** while re-verifying the
screenshot acceptance matrix, discovered that 8 pre-existing "core" streams
(`campaign_performance`, `ad_group_performance`, `ad_performance`,
`keyword_performance`, `geo_performance`, `age_range_performance`,
`gender_performance`, `campaign_device_performance`) currently hold only a
**5-day window** (2026-09-05 → 2026-09-09) in the warehouse, while every
other Google Ads stream (including the two touched by this session's work)
holds the full 33-day backfill (2026-08-08 → 2026-09-09). Neither Finding A
nor Finding B's spec touches any of these 8 streams, and no purge was run
against them in this session — this predates and is independent of today's
work. It is not a code defect (the stream specs are correct; `slice_days`
and incremental lookback are working as designed) but a **live-data
staleness/backfill-scope gap**: a `full_refresh` on connection 7 would
restore full history for these 8 streams. Flagged here because it directly
affects how much history the "Time series" report (screenshot item #12) and
several other screenshot reports can currently show for those specific
metrics, even though the two P1 items this document tracks are fully
resolved.
