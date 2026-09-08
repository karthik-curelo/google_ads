# Marketing Connector Platform

A pluggable data-ingestion platform for marketing / analytics sources, built into
a single FastAPI application. Architecture is borrowed from the **Airbyte source
protocol** (check / discover / configure / read + state) without cloning Airbyte
itself — see [Airbyte lessons](#airbyte-lessons-reused).

**Connectors shipped:** Google Analytics 4 · Google Search Console · Google Ads ·
Meta Ads · Instagram Insights.

Adding a sixth is one module + one `registry.register(...)` line — no changes to
the scheduler, OAuth layer, database, API, or UI.

---

## Architecture

```
                        FastAPI app  (app/main.py)
                              │
        ┌─────────────────────┼─────────────────────────┐
        │                     │                         │
   API routers          Sync scheduler            Static driver UI
  (app/api)          (in-process asyncio loop)      (app/web)
        │                     │
        │            ┌────────┴────────┐
        │            │   Sync runner   │  CHECK→DISCOVER→read slices→
        │            │  (app/sync)     │  write→STATE COMMIT, per connection
        │            └────────┬────────┘
        │                     │
   OAuth service        Connector framework          Destination writer
  (app/oauth)          (app/connectors)              (app/sync/writer.py)
   Google / Meta        BaseConnector + registry      report_rows / ad_entities
   token storage        + 5 connectors                (batched upsert)
        │                     │                              │
        └─────────────────────┴──────────────────────────────┘
                              │
                     Database  (app/models)
              SQLite for dev/tests · PostgreSQL for production
```

| Layer | Package | Responsibility |
|---|---|---|
| Connector framework | `app/connectors` | `BaseConnector` (spec/check/discover/read), typed error taxonomy, retrying+rate-limited HTTP, registry, date slicing, validation |
| OAuth | `app/oauth` | Provider-agnostic OAuth 2.0; one Google identity serves GA4+GSC+Ads; encrypted token storage; refresh/reauth/disconnect |
| Sync engine | `app/sync` | Per-run phase machine, per-stream cursor state, destination writer, in-process scheduler with a DB-row lock |
| API | `app/api` | `/integrations`, `/oauth`, `/connections`, `/sync-runs`, `/warehouse` (§30) |
| Models | `app/models` | Control-plane tables + the two-table warehouse (`report_rows`, `ad_entities`) |
| Warehouse views | `app/warehouse` | Auto-generated typed per-stream SQL views + `warehouse_catalog` — see [docs/WAREHOUSE.md](docs/WAREHOUSE.md) |
| UI | `app/web/index.html` | Self-contained driver page: connect → discover → configure → live sync progress |

---

## Quick start (local, SQLite, no containers)

```bash
python -m venv .venv && . .venv/Scripts/activate      # or .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# generate an encryption key and paste it into .env as ENCRYPTION_KEY:
python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"

alembic upgrade head          # or just start the app — dev auto-creates the schema
uvicorn app.main:app --reload
```

Open **http://localhost:8000/** for the driver UI, or **/docs** for the OpenAPI
console. On first start with no `API_TOKEN` set, a development bearer token is
printed to the log once — paste it into the UI's token box.

To actually connect a provider you need OAuth credentials — see
**[docs/OAUTH_SETUP.md](docs/OAUTH_SETUP.md)**.

### PostgreSQL (production target)

```bash
pip install -e ".[postgres]"
export DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/connectors
alembic upgrade head
uvicorn app.main:app --host 0.0.0.0 --port 8000    # ENVIRONMENT=production, DEBUG=false
```

---

## How a sync works (§28 lifecycle)

1. **CHECK** — cheapest call that proves the credentials + resource are usable.
   A permanent auth error fails the run immediately and drives the connection to
   `needs_reauth` (never retried — §14).
2. **resolve window** — first run: the configured backfill range. Later runs:
   `cursor − lookback_days … today`, because analytics providers restate recent
   days.
3. **slice** — the window is cut into bounded date windows (`slice_days`), so a
   large property cannot produce one unbounded response that hangs the run (§6).
4. **read → write** — records stream from the connector to the destination in
   batches; `report_rows` is **upserted** on `(connection, stream, record_key)`
   so an overlapping re-fetch corrects rows instead of duplicating them.
5. **STATE COMMIT** — the per-stream cursor advances only *after* the rows it
   covers are committed, and never moves backwards.
6. **COMPLETE** — `succeeded` / `partial_success` / `failed`, connection status
   and `next_run_at` updated; repeated failures back off exponentially.

Progress (`phase`, `slice 34/52`, record counts) is written to the `sync_runs`
row throttled, and surfaced by `GET /api/v1/sync-runs/{id}` and the UI.

---

## API

```
GET    /api/v1/integrations                      list connectors + availability + streams
GET    /api/v1/integrations/{id}
POST   /api/v1/integrations/{id}/connect          → { authorization_url }
GET    /api/v1/oauth/{provider}/callback          provider redirect target
GET    /api/v1/identities                         connected accounts
DELETE /api/v1/identities/{id}                    disconnect + revoke

POST   /api/v1/connections/discover               list syncable resources for an identity
POST   /api/v1/connections                        create a pipeline
GET    /api/v1/connections  ·  GET /api/v1/connections/{id}
PATCH  /api/v1/connections/{id}  ·  DELETE /api/v1/connections/{id}
POST   /api/v1/connections/{id}/sync              trigger now (?sync_mode=&reset=)
POST   /api/v1/connections/{id}/pause  ·  /resume  ·  /reconnect
GET    /api/v1/connections/{id}/health            live connection check
GET    /api/v1/connections/{id}/runs  ·  /data
GET    /api/v1/sync-runs  ·  GET /api/v1/sync-runs/{id}
```

All routes except the OAuth callback require `Authorization: Bearer <token>`;
every row is scoped to the token's organization (§20).

---

## Testing

```bash
pytest            # 53 tests, ~8s, no network (provider APIs mocked with respx)
ruff check .
ruff format --check .
```

Coverage: slicing/cursor math, validation & coercion, retry/backoff/rate-limit,
crypto + secret masking, the error model, OAuth exchange/refresh/classify,
state load/commit, the destination writer (insert vs upsert vs skip), each
connector's discover/read/pagination/classification against mocked APIs, a full
scheduler→runner→writer→state end-to-end (success, retryable failure,
concurrent-run prevention), and the API surface incl. tenant isolation.

---

## Airbyte lessons reused

Concepts and design patterns only — no Elastic-License-2.0 implementation code
was copied. The Airbyte **Protocol** (MIT) is the reference for the message
model.

- **check / discover / configured-catalog / read** decomposition — one verb, one
  job; the platform drives any connector without knowing the provider.
- **Catalog vs configured catalog** — the connector declares streams; the tenant's
  selection lives on the `Connection` row, so one connector serves every tenant.
- **Datetime cursor + state committed after the write** — makes a resumed sync
  correct, not just convenient; monotonic advance prevents lookback rewind.
- **`config_error` vs `system_error`** → our `recoverable` / `retryable` booleans,
  which decide whether to retry and whether to prompt the user.
- **Schema drift never aborts a sync** — drift is recorded, ingestion continues.
- **Never silently drop a record** — unmappable rows become `skipped_records` with
  a reason.
- **Connector registry / catalog** — one place the API, UI and engine read from.
- **Bounded slices** — the mitigation for the known GA4 large-response failure.

## Known limitations

See the bottom of this session's report and each connector's `caveats` in
`GET /api/v1/integrations`. In short: Google Ads needs an approved developer
token; Meta `ads_read` / `instagram_manage_insights` need App Review outside
development mode; Meta issues no refresh token (≈60-day reconnect); Instagram
account-level insights serve ~30 days and audience/demographic metrics are not
implemented in v1; the scheduler is single-process (documented upgrade path in
`app/sync/scheduler.py`).
#   g o o g l e _ a d s  
 