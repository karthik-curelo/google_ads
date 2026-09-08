# Warehouse data model

## Where synced data lands

Two physical tables, written by `app/sync/writer.py`:

| Table | Grain | Contents |
|---|---|---|
| `report_rows` | fact (one row per stream × date × dimension tuple) | every GA4 / Search Console / Ads daily-metric stream |
| `ad_entities` | entity | Google/Meta campaign · ad group · ad · keyword attributes |

Provider-native detail is never dropped: the full `dimensions`, `metrics` and
`raw` payloads ride along as JSON; the measures shared across providers
(`sessions`, `users`, `clicks`, `cost`, `revenue`, `bounce_rate`, …) are also
promoted to typed columns for fast SQL.

Per property / site: filter on `resource_id` (raw provider id) or
`connection_id`. Per stream: filter on `stream`.

## No duplication on re-runs (already guaranteed)

Nothing extra is needed to "add only new data":

- `report_rows` has `UNIQUE(connection_id, stream, record_key)` and every write
  is `INSERT … ON CONFLICT DO UPDATE`. `record_key` is a deterministic
  `sha256(stream + sorted primary-key values)`, so the same source row always
  maps to the same warehouse row.
- The per-`(connection, stream)` cursor lives in `sync_state`, only ever moves
  forward, and is committed *after* the rows it covers are durably written.
- Run 2+ re-fetches only `cursor − lookback_days` (default 3) onward. Analytics
  providers restate the last few days; those overlapping rows are **corrected**
  in place, not duplicated.
- `ad_entities` has the same protection on `UNIQUE(connection_id, level,
  external_id)`.

`full_refresh` still routes through the same upsert — it re-reads a wider window,
it does not truncate. It will not delete rows that later vanish from the source.

## Typed per-stream views

`app/warehouse/views.py` generates one SQL view per `(connector, stream)` over
`report_rows` / `ad_entities`, projecting the JSON into typed, named columns.
A view is just a saved query, so it inherits the de-dup / incremental guarantees
above — there is no second copy of the data.

- Name: `v_<short>_<stream>` — `ga4`, `gsc`, `gads`, `meta_ads`, `ig`.
  e.g. `v_ga4_landing_pages`, `v_gsc_search_analytics_by_query`,
  `v_gads_campaign_performance`, `v_ga4_entities`.
- Every fact view carries `property_id` (= `resource_id`), `connection_id`,
  `date`, `currency`, `synced_at`, `sync_run_id`, plus one column per
  dimension/metric. One view serves every property — filter on `property_id`.
- `warehouse_catalog` lists every `(view_name, connector_id, provider, stream,
  grain, column_name, role)` so an agent / MCP tool can discover what is
  queryable without dialect-specific `information_schema`. `role` ∈
  `key | dimension | metric | measure | meta`.

Views are rebuilt from the connector registry on every app start, so a new
stream (a dict entry in a connector) gets its view automatically. To rebuild
without a restart:

```
POST /api/v1/warehouse/rebuild-views      # auth: Bearer <token>
python -m app.warehouse.views             # CLI (uses DATABASE_URL)
```

### Example

```sql
-- top entry pages by conversions, last 30 days
SELECT landing_page, SUM(sessions) AS sessions, SUM(conversions) AS conversions
FROM v_ga4_landing_pages
WHERE property_id = '458037317'
GROUP BY landing_page
ORDER BY conversions DESC NULLS LAST
LIMIT 20;

-- session quality by channel
SELECT date, channel_group, device_category,
       sessions, bounce_rate, engagement_rate, average_session_duration
FROM v_ga4_session_quality
WHERE property_id = '458037317' AND date >= DATE '2026-08-01';
```

## Adding a promoted column

1. Add the column to `ReportRow` in `app/models/warehouse.py` (nullable).
2. Add its name to `MEASURE_COLUMNS` in `app/sync/writer.py`.
3. Map the provider metric → column in that connector's measure map
   (`_MEASURE_MAP` in `app/connectors/google/analytics.py` / `ads.py`), and in
   `_PROMOTED` in `app/warehouse/views.py`.
4. `alembic revision --autogenerate` (add a JSON→column backfill in `upgrade()`
   for rows already synced), then `alembic upgrade head`.
5. Restart, or `POST /api/v1/warehouse/rebuild-views`.
