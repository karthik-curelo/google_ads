# Warehouse data model

## Where synced data lands

One **fact table per source** plus one shared **entity** table, written by
`app/sync/writer.py` (`CONNECTOR_MODEL_MAP` routes each connector to its table):

| Table | Grain | Written by |
|---|---|---|
| `google_analytics_performance` | fact | `google_analytics` |
| `google_search_console_performance` | fact | `google_search_console` |
| `google_ads_performance` | fact | `google_ads` |
| `meta_ads_performance` | fact | `meta_ads` |
| `instagram_insights_performance` | fact | `instagram_insights` |
| `facebook_pages_performance` | fact | `facebook_pages` |
| `ad_entities` | entity | every source that has campaign-tree / object rows (Ads campaigns·ad groups·ads·keywords·creatives·conversion actions, GSC sitemaps, IG media, FB pages·posts) |
| `skipped_records` | — | rows dropped in validation, with the reason (§19) |

All fact tables share `PerformanceRowMixin`: `organization_id`, `connection_id`,
`connector_id`, `provider`, `stream`, `resource_id`, `record_key`, `date`, the
full provider-native `dimensions` / `metrics` / `raw` as JSON, and bookkeeping
(`schema_version`, `sync_run_id`, `source_updated_at`, `ingested_at`). Each table
then adds the typed measure columns that make sense for its source — spend /
clicks / conversions for the ad sources, `sessions` / `users` for GA4,
`clicks` / `impressions` / `average_position` for Search Console, etc. Nothing
provider-native is lost; the typed columns are just a fast path.

Per property / site: filter on `resource_id` (raw provider id) or
`connection_id`. Per stream: filter on `stream`. A cross-provider spend
comparison is a `UNION ALL` over `google_ads_performance` + `meta_ads_performance`
(rolled up on `dimensions` / channel), never a filter on one wide table.

## No duplication on re-runs (guaranteed)

Nothing extra is needed to "add only new data":

- Every fact table has `UNIQUE(connection_id, stream, record_key)` and every
  write is `INSERT … ON CONFLICT DO UPDATE`. `record_key` is a deterministic
  `sha256(stream + sorted primary-key values)[:40]`, so the same source row
  always maps to the same warehouse row. `ingested_at` is frozen on update; the
  measure columns and JSON are overwritten with the restated values.
- The per-`(connection, stream)` cursor lives in `sync_state`, only ever moves
  forward (`advance_cursor`), and is committed *after* the rows it covers are
  durably written. Concurrent commits (a manual "Sync now" landing mid-schedule)
  race-retry onto the same row instead of failing.
- Run 2+ re-fetches only `cursor − lookback_days` (default 3) onward. Analytics
  providers restate the last few days; those overlapping rows are **corrected**
  in place, not duplicated.
- `ad_entities` has the same protection on `UNIQUE(connection_id, level,
  external_id)`.

`full_refresh` still routes through the same upsert — it re-reads a wider window,
it does not truncate, and it will not delete rows that later vanish from the
source.

## Scheduling

`SyncScheduler` (in-process, `SCHEDULER_ENABLED=true`) polls every
`SCHEDULER_POLL_SECONDS` for connections whose `next_run_at` is due, claims each
with a DB compare-and-swap lock, and runs up to `MAX_CONCURRENT_SYNCS` at once.

- `schedule_interval_seconds` on the connection sets a plain interval
  (`next_run_at = now + interval`).
- `config.daily_at` (`"HH:MM"`, with optional `config.daily_at_offset_minutes`
  for a non-UTC wall clock) pins the run to a fixed time of day with **no drift**
  — after each run `next_run_at` snaps to the next occurrence of that time.
- Repeated failure backs off exponentially (capped 6h), ignoring `daily_at` so a
  broken connection retries sooner than a full day.

Manual: `POST /api/v1/connections/{id}/sync`.

## Querying

There is no generated view layer — query the tables directly. The JSON columns
are `jsonb` on Postgres, so dimensions read as `dimensions->>'channel_group'`.

```sql
-- top entry pages by conversions, last 30 days
SELECT dimensions->>'landingPage' AS landing_page,
       SUM(sessions) AS sessions, SUM(conversions) AS conversions
FROM google_analytics_performance
WHERE stream = 'landing_pages'
  AND resource_id = '458037317'
  AND date >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY conversions DESC NULLS LAST
LIMIT 20;

-- cross-provider daily spend
SELECT date, 'google_ads' AS source, SUM(cost) AS spend
FROM google_ads_performance WHERE stream = 'campaign_performance' GROUP BY date
UNION ALL
SELECT date, 'meta_ads', SUM(cost)
FROM meta_ads_performance WHERE stream = 'campaign_insights' GROUP BY date
ORDER BY date DESC;
```

The `GET /api/v1/connections/{id}/data` endpoint returns a tabular view of the
right per-source table for a connection (see `COLUMN_CONFIGS` in
`app/api/routers/connections.py`).

## Adding a promoted measure column

1. Add the column to the source's `*Performance` class in
   `app/models/warehouse.py` (nullable).
2. Add its name to `MEASURE_COLUMNS` in `app/sync/writer.py`.
3. Have the connector put the value in `record.measures[<column>]` (its
   `_MEASURE_MAP` or equivalent).
4. `alembic revision --autogenerate -m "..."`, add `import app.models.base` to
   the generated file if it references `UTCDateTime`, add a JSON→column backfill
   in `upgrade()` for already-synced rows, then `alembic upgrade head`.
5. Restart the app.
