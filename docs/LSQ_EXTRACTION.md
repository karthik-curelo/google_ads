# LeadSquared extraction — architecture and runbook

Scope: **LeadSquared API → connector → PostgreSQL → scheduler/sync.** Nothing here touches the
analytics layer (MCP tools, attribution, funnel/revenue definitions).

Everything under "What the API does" was verified against the live account (api-in21) on
2026-09-19/20 by direct experiment, not read from documentation or code comments.

## What the API does

| Fact | Evidence / consequence |
|---|---|
| `Leads.RecentlyModified` filters on **`LeadLastModifiedOn`** | 100% of returned leads fall in the window on that column; only 90% on the `ModifiedOn` attribute. It moves on any new activity, so leads are re-read often. |
| `RetrieveByActivityEvent` filters on **`ModifiedOn`**, not `CreatedOn` | Activities created in August and edited on Sep 1 are invisible to an August window and returned by a Sep 1 window. A modification cursor is exactly what an incremental sync wants. |
| `RecordCount` is the **true total** for the window, whatever the page size | Independent source count for one cheap request (`PageSize=1`). |
| Paging is offset/limit. **No page token, cursor or continuation field.** | Envelope is `{RecordCount, Leads|List}` only. |
| Ordering by `CreatedOn` is **non-deterministic** when timestamps tie | The identical request returned 0 / 4 / 4 / 8 duplicate rows across four pulls; the union of pulls equalled `RecordCount`, so every pull silently dropped as many rows. Cause of the confirmed loss of 4 `booking_created` rows on 2026-08-27. |
| Ordering by the record's **unique id** is stable | 3/3 identical pulls for activities (`ProspectActivityId`) and leads (`ProspectID`). |
| Page caps: leads 5000, activities 1000; an explicit `ActivityEvent` is mandatory | There is no "all activity types" call → one stream per type. |
| Timestamps are UTC with **sub-second precision** (printed `.000`); From/ToDate are parsed as whole seconds (`ToDate=04` = up to `04.000`) | Adjacent windows leave a one-second crack: on the account, `[00..09]` held 10 leads while `[00..04]` + `[05..09]` held 9 (~1 in 10 lost per boundary during a 40-leads/second bulk hour; it went unnoticed on quiet days). Windows are closed and **share** their boundary second (`[00..05]` + `[05..09]` = 10). A zero-width window `[s..s]` matches only rows exactly on `.000`. |
| Every lead carries an echoed **`Total`** attribute = the *query's* total | Same lead came back as `153` then `108`. Not a lead field; excluded from `raw` (it made every re-read look like a change). |
| **HTTP 500 means a logical error**: `{"Status":"Error","ExceptionType":"MX…Exception"}` | Bad page size, bad date, missing ActivityEvent, unknown activity id are all 500s. Only bad credentials (401) and bad path (404) differ. Retrying a 500 repeats the same answer. |
| Deleted activities → `GetActivityDetails` answers `MXUnknownProspectActivityException`; unknown lead → `Leads.GetById` returns `[]` | The only reliable deletion signal (see below). |
| A real 429 occurred at ~0.77 requests/s sustained | Default limiter 0.5/s, and it halves itself for a while after a 429. |
| Data begins in **2025**: nothing has a modification date before 2025-01-01 | The *available* history starts 2025-01-01. **Operating decision (owner, 2026-09-20): load only from 2026-09-01** — older history is not needed in the warehouse. |
| 84 activity types; 49 hold data (6.48M rows); 206 lead attributes | `ProspectStage`, `FirstName`, owner, disposition etc. are populated and were previously dropped. |

## Architecture

```
scheduler / CLI / "Sync now"
      │  claim (atomic; leases.py)
      ▼
run_connection ──heartbeat──▶ connection lease (locked_by, lease_expires_at)
      │
      ▼  per stream (cursor_kind="timestamp")
connector.read_range()  ──▶ WindowFetcher   one verified-complete window at a time (closed, boundary second SHARED)
      │                        (window.py)      • sized to fit ONE page: no offset paging over live data
      │                                          • verified against the same response's RecordCount
      ▼                                          • ordered by unique id; only a single over-full second pages
DestinationWriter.write_records   one transactional upsert, guarded (source_modified_on) so an older/replayed
      │                            fetch can never overwrite a newer row; persisted count measured AFTER commit
      ▼
_reconcile_window   source_count == fetched == distinct(+unmappable) == records built == persisted(+skipped)
      │                else RECONCILIATION_FAILED (retryable) and the checkpoint stays put
      ▼
commit_state_ts     monotonic timestamp checkpoint, advanced ONLY after the window above is verified
```

* **Complete lead payload**: `Columns` is never sent, so all 206 attributes are stored in `raw`. The
  ~24 attribution fields analytics uses are still normalised into `dimensions`; `ProspectStage`,
  `owner_id`, `source_modified_on`, `source_created_on` are typed columns. Nothing else is promoted.
* **Every activity type**: 84 streams (`booking_created`, `post_booking_order_status`,
  `booking_cancelled`, `facebook_lead_ads_submissions` keep their names; the rest are `activity_<code>`),
  all into `leadsquared_activities`, told apart by `activity_event` (+ `activity_event_name`); the full source
  row, every `mx_Custom_N` slot, is in `raw`. `check_connection()` diffs the live type list against the
  catalog on every run, and a **new** type becomes a stream automatically.
* **Incremental**: each run re-reads a 24h overlap before the checkpoint (idempotent no-op where nothing
  changed) up to 30s before "now". A legacy date cursor (written before timestamp cursors existed) is ignored
  and the stream is re-swept from `backfill_start_date`.
* **Idempotent**: `UNIQUE(connection_id, stream, record_key)` + `INSERT … ON CONFLICT DO UPDATE … WHERE`
  guard, in one statement. `records_updated` counts rows actually rewritten; unchanged re-reads are `unchanged`.
* **Race safety** (`app/sync/leases.py`): the connection row is a **lease**. Claiming is one conditional
  `UPDATE` (due claim: `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING id`). The holder
  heartbeats and writes its own `lease_expires_at`, so a slow-heartbeat laptop CLI is never reaped by a
  scheduler configured with a shorter lease. A worker that loses its lease is fenced (cancelled) and cannot
  clear its successor's lock. Different connections/connectors run concurrently; one connection never does.
* **Rate limits** are shared per provider API in-process (`shared_rate_limiter`): LeadSquared per account,
  Google per API, Meta per app. **Ceiling**: shared per *process*; several processes each get the full budget
  (a cross-process limiter would need a service — not built).
* **Daily API budget** (`LEADSQUARED_DAILY_API_BUDGET`, default 6000 per connection per rolling 24h): a
  backfill must not spend the account's 10,000/day quota, which the live booking automation also uses. Streams
  share what is left fairly; one that hits its share stops at a checkpoint and resumes next run.
* **Run health**: a manual success cannot hide a broken schedule. `consecutive_scheduled_failures` keeps the
  connection in error until a *scheduled* run succeeds. Startup **preflight** (and `/healthz` →
  `"degraded"`) flags connections whose connector settings are missing from *this* process.

## Runbook

```bash
# 0. Deploy order: migrations first (additive, old code keeps working), then code, then restart.
alembic upgrade head           # head b7c3d91e4a52 (additive; safe under the previous release)

# 1. The service's own .env MUST hold LEADSQUARED_ACCESS_KEY/SECRET_KEY/HOST (this was the outage).
python -m app.sync.cli preflight

# 2. Set the connection's backfill_start_date to 2026-09-01 (the owner's chosen boundary; the API itself
#    goes back to 2025-01-01, so widening later is just a different date + another run). Let the scheduler
#    do it, or run it explicitly:
python -m app.sync.cli sync --connection <id> --timeout 86400
#    Cost for one month is ~1,000-1,500 API calls (~45 min at 0.5/s); the default daily budget is ample.
#    (A full 2025-onward history would be ~11,600 calls / ~6.5h - more than the account's daily quota.)

# 3. Source vs warehouse, like for like (RecordCount vs rows whose source_modified_on is in range)
python -m app.sync.cli counts --connection <id> --since 2026-09-01

# 4. Deletions (dry run by default). Needs the checkpoint past the range + 24h.
python -m app.sync.cli reconcile --connection <id> --stream booking_created --from 2026-06-01 --to 2026-08-31
python -m app.sync.cli reconcile … --apply      # tombstones only; rows are never deleted
```

## Which streams to sync (owner's choice: the "core set")

The connector *can* extract all 84 activity types, but a connection's `streams` list decides which ones
run. An empty list means "every declared stream". The other 76 types are ~61% of the activity rows
(Phone Call - Outbound alone is ~45% of activity storage) and nothing downstream needs them yet, so
production uses this explicit list (leads + 8 activity types, ~46% less storage than all 84):

| stream | LSQ event |
|---|---|
| `leads` | (lead record) |
| `booking_created` | 206 Booking Created |
| `activity_227` | 227 Booking Edited |
| `booking_cancelled` | 223 Booking Cancelled |
| `post_booking_order_status` | 208 Post Booking Order Status |
| `facebook_lead_ads_submissions` | 204 Facebook Lead Ads Submissions |
| `activity_23` | 23 Lead Capture |
| `activity_97` | 97 Dynamic Form - Submission |
| `activity_203` | 203 WhatsApp Message |

Changing it later is a config change, not a code change: `PATCH /connections/<id>` with the **complete**
new `streams` list (it replaces the old one), each item `{"stream": "<name>", "sync_mode": "incremental",
"enabled": true}`. Names are `activity_<event code>`, except the four legacy names above. Consequences:

* A stream with no checkpoint (never synced) backfills from the connection's `backfill_start_date`.
* A stream that is switched off keeps its rows and its checkpoint. Switched back on, it resumes from that
  checkpoint, so nothing between the two dates is skipped.
* Types not in the list are ignored, including ones LeadSquared creates later — no surprise growth.
  `check_connection` reports such types as `undeclared_activity_types`.

## Deletions

LeadSquared has **no deleted-since feed**. A deleted record simply stops appearing, which an incremental sync
cannot observe. The warehouse is therefore *upsert history*, not a mirror, until a sweep runs. The sweep compares
per-window source and warehouse counts, drills into windows where the warehouse holds more, and confirms every
candidate with an independent by-id lookup before setting `deleted_at`. Rows are kept; a record that reappears
clears its own tombstone; a sweep refuses to tombstone an implausibly large share of a range. Query "live" rows
with `deleted_at IS NULL`. Not automated: run it periodically (e.g. weekly) or before source-vs-warehouse audits.

## Known limits

* Rate limiting is per process (see above). Run **one** scheduler process, or accept N× the budget.
* The 24h overlap assumes the source commits within 24h of a row's timestamp. A row that becomes visible
  later with an old timestamp is only found by a re-sweep (`POST /api/v1/connections/{id}/sync?reset=true`
  clears the stream checkpoints and re-reads from `backfill_start_date`) or by the counts check.
* Timestamps are UTC; `date` columns are UTC dates (IST is UTC+5:30 — a UTC day is not a business day).
* `sync_state.records_synced` and `connections.total_records_synced` count writes, not rows.
