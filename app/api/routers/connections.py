"""/connections — configured pipelines and their lifecycle (§30, §31).

Create a connection (connector × identity × resource × schedule × streams),
then sync / pause / resume / reconnect / inspect it. Resource discovery for the
connect wizard also lives here (`POST /connections/discover`).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, text

from app.api.deps import OrgDep, SchedulerDep, SessionDep, build_connector
from app.connectors import errors as E
from app.connectors.base import HealthReport
from app.connectors.registry import load_connectors
from app.core.config import get_settings
from app.core.database import destination_info
from app.core.logging import get_logger
from app.models import (
    CONN_PAUSED,
    CONN_PENDING,
    AdEntity,
    Connection,
    ConnectorResource,
    OAuthIdentity,
    SyncRun,
)
from app.oauth.service import begin_authorization
from app.sync.scheduler import trigger_sync_detached
from app.sync.state import reset_state
from app.sync.writer import CONNECTOR_MODEL_MAP

logger = get_logger(__name__)

router = APIRouter(prefix="/connections", tags=["connections"])


# --------------------------------------------------------------------------- #
# schemas
# --------------------------------------------------------------------------- #
class DiscoverRequest(BaseModel):
    connector_id: str
    identity_id: int
    config: dict[str, Any] = Field(default_factory=dict)


class StreamSelection(BaseModel):
    stream: str
    sync_mode: str = "incremental"
    enabled: bool = True


class ConnectionCreate(BaseModel):
    connector_id: str
    identity_id: int
    resource_id: str
    name: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    streams: list[StreamSelection] = Field(default_factory=list)
    backfill_start_date: date | None = None
    backfill_days: int | None = None
    lookback_days: int | None = None
    schedule_interval_seconds: int | None = None
    enabled: bool = True


class ConnectionUpdate(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None
    streams: list[StreamSelection] | None = None
    lookback_days: int | None = None
    schedule_interval_seconds: int | None = None
    enabled: bool | None = None


# --------------------------------------------------------------------------- #
# discovery
# --------------------------------------------------------------------------- #
@router.post("/discover")
async def discover_resources(body: DiscoverRequest, org: OrgDep, session: SessionDep):
    registry = load_connectors()
    if body.connector_id not in registry:
        raise HTTPException(404, f"Unknown connector {body.connector_id!r}")
    identity = await session.get(OAuthIdentity, body.identity_id)
    if identity is None or identity.organization_id != org.id:
        raise HTTPException(404, "Identity not found")

    connector = build_connector(body.connector_id, identity.id, config=body.config)
    try:
        resources = await connector.discover_resources()
    except E.ConnectorError as exc:
        raise HTTPException(400, exc.as_user_dict()) from exc
    finally:
        await connector.aclose()

    # Cache for the UI (upsert on identity+connector+resource).
    for res in resources:
        existing = (
            await session.execute(
                select(ConnectorResource).where(
                    ConnectorResource.oauth_identity_id == identity.id,
                    ConnectorResource.connector_id == body.connector_id,
                    ConnectorResource.resource_id == res.resource_id,
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = ConnectorResource(
                organization_id=org.id,
                oauth_identity_id=identity.id,
                connector_id=body.connector_id,
                resource_id=res.resource_id,
            )
            session.add(existing)
        existing.name = res.name
        existing.resource_type = res.resource_type
        existing.parent_id = res.parent_id
        existing.metadata_json = res.metadata
        existing.selectable = res.selectable
        existing.unsupported_reason = res.unsupported_reason

    return {
        "resources": [
            {
                "resource_id": r.resource_id,
                "name": r.name,
                "resource_type": r.resource_type,
                "parent_id": r.parent_id,
                "selectable": r.selectable,
                "unsupported_reason": r.unsupported_reason,
                "metadata": r.metadata,
            }
            for r in resources
        ]
    }


# --------------------------------------------------------------------------- #
# CRUD
# --------------------------------------------------------------------------- #
@router.get("")
async def list_connections(org: OrgDep, session: SessionDep):
    rows = (
        (
            await session.execute(
                select(Connection).where(Connection.organization_id == org.id).order_by(Connection.id.desc())
            )
        )
        .scalars()
        .all()
    )
    return {"connections": [_serialize(c) for c in rows]}


@router.post("", status_code=201)
async def create_connection(body: ConnectionCreate, org: OrgDep, session: SessionDep):
    registry = load_connectors()
    if body.connector_id not in registry:
        raise HTTPException(404, f"Unknown connector {body.connector_id!r}")
    entry = registry.get(body.connector_id)
    identity = await session.get(OAuthIdentity, body.identity_id)
    if identity is None or identity.organization_id != org.id:
        raise HTTPException(404, "Identity not found")
    if identity.provider != entry.connector_class.provider:
        raise HTTPException(400, "Identity provider does not match the connector")

    dup = (
        await session.execute(
            select(Connection).where(
                Connection.organization_id == org.id,
                Connection.connector_id == body.connector_id,
                Connection.resource_id == body.resource_id,
            )
        )
    ).scalar_one_or_none()
    if dup is not None:
        raise HTTPException(409, f"A connection for this resource already exists (id={dup.id})")

    settings = get_settings()
    declared = {s.name for s in entry.connector_class.declared_streams()}
    streams = [s.model_dump() for s in body.streams if s.stream in declared]
    if not streams:
        streams = [{"stream": n, "sync_mode": "incremental", "enabled": True} for n in declared]

    backfill_start = body.backfill_start_date
    if backfill_start is None:
        days = body.backfill_days or settings.default_backfill_days
        backfill_start = datetime.now(UTC).date() - timedelta(days=days)

    # Cache resource metadata (currency etc.) onto the connection so the
    # connector can skip a discovery call at sync time.
    res_row = (
        await session.execute(
            select(ConnectorResource).where(
                ConnectorResource.oauth_identity_id == identity.id,
                ConnectorResource.connector_id == body.connector_id,
                ConnectorResource.resource_id == body.resource_id,
            )
        )
    ).scalar_one_or_none()

    conn = Connection(
        organization_id=org.id,
        oauth_identity_id=identity.id,
        connector_id=body.connector_id,
        name=body.name or (res_row.name if res_row else body.resource_id),
        resource_id=body.resource_id,
        resource_name=res_row.name if res_row else None,
        resource_metadata=(res_row.metadata_json if res_row else None) or {},
        config=body.config,
        streams=streams,
        backfill_start_date=backfill_start,
        lookback_days=body.lookback_days or settings.default_lookback_days,
        schedule_interval_seconds=body.schedule_interval_seconds,
        enabled=body.enabled,
        status=CONN_PENDING,
        next_run_at=datetime.now(UTC) if body.enabled else None,
    )
    session.add(conn)
    await session.flush()
    return _serialize(conn)


@router.get("/{connection_id}")
async def get_connection(connection_id: int, org: OrgDep, session: SessionDep):
    conn = await _own(session, org, connection_id)
    latest = (
        await session.execute(
            select(SyncRun)
            .where(SyncRun.connection_id == conn.id)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    data = _serialize(conn)
    data["latest_run"] = _serialize_run(latest) if latest else None
    return data


@router.patch("/{connection_id}")
async def update_connection(connection_id: int, body: ConnectionUpdate, org: OrgDep, session: SessionDep):
    conn = await _own(session, org, connection_id)
    if body.name is not None:
        conn.name = body.name
    if body.config is not None:
        conn.config = body.config
    if body.streams is not None:
        conn.streams = [s.model_dump() for s in body.streams]
    if body.lookback_days is not None:
        conn.lookback_days = body.lookback_days
    if body.schedule_interval_seconds is not None:
        conn.schedule_interval_seconds = body.schedule_interval_seconds
    if body.enabled is not None:
        conn.enabled = body.enabled
        if body.enabled and conn.next_run_at is None:
            conn.next_run_at = datetime.now(UTC)
    return _serialize(conn)


@router.delete("/{connection_id}", status_code=204)
async def delete_connection(connection_id: int, org: OrgDep, session: SessionDep):
    conn = await _own(session, org, connection_id)
    await session.delete(conn)


# --------------------------------------------------------------------------- #
# lifecycle actions
# --------------------------------------------------------------------------- #
@router.post("/{connection_id}/sync", status_code=202)
async def sync_now(
    connection_id: int,
    org: OrgDep,
    session: SessionDep,
    scheduler: SchedulerDep,
    sync_mode: str | None = Query(default=None, pattern="^(incremental|full_refresh)$"),
    reset: bool = Query(default=False, description="Clear cursors first (full re-backfill)"),
):
    conn = await _own(session, org, connection_id)
    if reset:
        await reset_state(conn.id)
    await session.commit()

    if scheduler is not None:
        queued = await scheduler.trigger(conn.id, sync_mode=sync_mode)
    else:
        import asyncio

        asyncio.create_task(trigger_sync_detached(conn.id, sync_mode=sync_mode))  # noqa: RUF006
        queued = True
    if not queued:
        raise HTTPException(409, "A sync for this connection is already running")
    return {"queued": True, "connection_id": conn.id}


@router.post("/{connection_id}/pause")
async def pause(connection_id: int, org: OrgDep, session: SessionDep):
    conn = await _own(session, org, connection_id)
    conn.enabled = False
    conn.status = CONN_PAUSED
    conn.next_run_at = None
    return _serialize(conn)


@router.post("/{connection_id}/resume")
async def resume(connection_id: int, org: OrgDep, session: SessionDep):
    conn = await _own(session, org, connection_id)
    conn.enabled = True
    conn.status = CONN_PENDING
    conn.next_run_at = datetime.now(UTC)
    return _serialize(conn)


@router.post("/{connection_id}/reconnect")
async def reconnect(connection_id: int, org: OrgDep, session: SessionDep, redirect_after: str | None = None):
    conn = await _own(session, org, connection_id)
    identity = await session.get(OAuthIdentity, conn.oauth_identity_id)
    url, state = await begin_authorization(
        session,
        organization_id=org.id,
        connector_id=conn.connector_id,
        redirect_after=redirect_after,
        identity_id=conn.oauth_identity_id,
        login_hint=identity.email if identity else None,
    )
    return {"authorization_url": url, "state": state}


@router.get("/{connection_id}/health")
async def connection_health(connection_id: int, org: OrgDep, session: SessionDep):
    conn = await _own(session, org, connection_id)
    connector = build_connector(
        conn.connector_id,
        conn.oauth_identity_id,
        resource_id=conn.resource_id,
        resource_metadata=conn.resource_metadata or {},
        config=conn.config or {},
    )
    try:
        report: HealthReport = await connector.check_connection()
    except E.ConnectorError as exc:
        raise HTTPException(400, exc.as_user_dict()) from exc
    finally:
        await connector.aclose()
    return {
        "status": str(report.status),
        "message": report.message,
        "ok": report.ok,
        "details": report.details,
    }


# --------------------------------------------------------------------------- #
# observability
# --------------------------------------------------------------------------- #
@router.get("/{connection_id}/runs")
async def connection_runs(
    connection_id: int, org: OrgDep, session: SessionDep, limit: int = Query(20, le=100)
):
    conn = await _own(session, org, connection_id)
    rows = (
        (
            await session.execute(
                select(SyncRun)
                .where(SyncRun.connection_id == conn.id)
                .order_by(SyncRun.started_at.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return {"runs": [_serialize_run(r) for r in rows]}


# One performance table per source — single source of truth is the writer's map.
MASTER_TABLES: dict[str, str] = {
    cid: model.__tablename__ for cid, model in CONNECTOR_MODEL_MAP.items()
}

COLUMN_CONFIGS: dict[str, list[dict[str, str]]] = {
    "google_ads_performance": [
        {"key": "date", "label": "Date", "type": "date"},
        {"key": "campaign_name", "label": "Campaign", "type": "text"},
        {"key": "ad_group_name", "label": "Ad Group", "type": "text"},
        {"key": "search_term", "label": "Search Term", "type": "text"},
        {"key": "cost", "label": "Cost", "type": "currency"},
        {"key": "impressions", "label": "Impr", "type": "number"},
        {"key": "clicks", "label": "Clicks", "type": "number"},
        {"key": "conversions", "label": "Conv", "type": "number"},
        {"key": "ctr", "label": "CTR", "type": "percent"},
        {"key": "cpc", "label": "Avg CPC", "type": "currency"},
        {"key": "roas", "label": "ROAS", "type": "number"},
    ],
    "meta_ads_performance": [
        {"key": "date", "label": "Date", "type": "date"},
        {"key": "campaign_name", "label": "Campaign", "type": "text"},
        {"key": "adset_name", "label": "Ad Set", "type": "text"},
        {"key": "ad_name", "label": "Ad Name", "type": "text"},
        {"key": "spend", "label": "Spend", "type": "currency"},
        {"key": "impressions", "label": "Impr", "type": "number"},
        {"key": "reach", "label": "Reach", "type": "number"},
        {"key": "clicks", "label": "Clicks", "type": "number"},
        {"key": "conversions", "label": "Conv", "type": "number"},
        {"key": "cpm", "label": "CPM", "type": "currency"},
        {"key": "cpc", "label": "CPC", "type": "currency"},
        {"key": "ctr", "label": "CTR", "type": "percent"},
        {"key": "roas", "label": "ROAS", "type": "number"},
    ],
    "google_analytics_performance": [
        {"key": "date", "label": "Date", "type": "date"},
        {"key": "channel_group", "label": "Channel Group", "type": "text"},
        {"key": "landing_page", "label": "Landing Page", "type": "text"},
        {"key": "event_name", "label": "Event", "type": "text"},
        {"key": "sessions", "label": "Sessions", "type": "number"},
        {"key": "active_users", "label": "Users", "type": "number"},
        {"key": "new_users", "label": "New Users", "type": "number"},
        {"key": "page_views", "label": "Page Views", "type": "number"},
        {"key": "bounce_rate", "label": "Bounce Rate", "type": "percent"},
        {"key": "conversions", "label": "Conversions", "type": "number"},
        {"key": "revenue", "label": "Revenue", "type": "currency"},
    ],
    "google_search_console_performance": [
        {"key": "date", "label": "Date", "type": "date"},
        {"key": "query", "label": "Search Query", "type": "text"},
        {"key": "page", "label": "Page URL", "type": "text"},
        {"key": "device", "label": "Device", "type": "text"},
        {"key": "country", "label": "Country", "type": "text"},
        {"key": "clicks", "label": "Clicks", "type": "number"},
        {"key": "impressions", "label": "Impr", "type": "number"},
        {"key": "ctr", "label": "CTR", "type": "percent"},
        {"key": "average_position", "label": "Avg Position", "type": "number"},
    ],
}


@router.get("/{connection_id}/data")
async def connection_data(
    connection_id: int,
    org: OrgDep,
    session: SessionDep,
    stream: str | None = None,
    grain: str = Query("fact", pattern="^(fact|entity)$"),
    limit: int = Query(100, le=1000),
):
    conn = await _own(session, org, connection_id)
    model_map = CONNECTOR_MODEL_MAP

    if grain == "entity":
        stmt = select(AdEntity).where(AdEntity.connection_id == conn.id)
        rows = (await session.execute(stmt.limit(limit))).scalars().all()
        serialized = [_row_dict(r) for r in rows]
        cols = [
            {"key": "level", "label": "Level", "type": "text"},
            {"key": "external_id", "label": "ID", "type": "text"},
            {"key": "name", "label": "Name", "type": "text"},
            {"key": "status", "label": "Status", "type": "text"},
            {"key": "channel", "label": "Channel", "type": "text"},
            {"key": "daily_budget", "label": "Daily Budget", "type": "currency"},
        ]
        return {
            "table_name": f"v_{conn.connector_id}_entities",
            "columns": cols,
            "rows": serialized,
            "raw_rows": serialized,
            "total": len(serialized),
        }

    master_table = MASTER_TABLES.get(conn.connector_id)
    if master_table and not stream:
        try:
            sql_query = text(f'SELECT * FROM "{master_table}" WHERE connection_id = :cid ORDER BY date DESC NULLS LAST LIMIT :lim')
            res = await session.execute(sql_query, {"cid": conn.id, "lim": limit})
            tabular_rows = []
            for r in res:
                d = dict(r._mapping)
                cleaned = {}
                for k, v in d.items():
                    if isinstance(v, (datetime, date)):
                        cleaned[k] = v.isoformat()
                    elif isinstance(v, Decimal):
                        cleaned[k] = float(v)
                    else:
                        cleaned[k] = v
                tabular_rows.append(cleaned)

            raw_stmt = select(model_map[conn.connector_id]).where(model_map[conn.connector_id].connection_id == conn.id).order_by(model_map[conn.connector_id].date.desc()).limit(limit)
            raw_rows = (await session.execute(raw_stmt)).scalars().all()
            serialized_raw = [_row_dict(r) for r in raw_rows]

            cols = COLUMN_CONFIGS.get(master_table, [])
            if not cols and tabular_rows:
                cols = [{"key": k, "label": k.replace("_", " ").title(), "type": "text"} for k in tabular_rows[0]]

            return {
                "table_name": master_table,
                "columns": cols,
                "rows": tabular_rows,
                "raw_rows": serialized_raw,
                "total": len(tabular_rows),
            }
        except Exception as e:
            logger.warning("Could not query master table %s: %s", master_table, e)

    FallbackModel = model_map.get(conn.connector_id)

    if not FallbackModel:
        return {
            "table_name": "unknown",
            "columns": [],
            "rows": [],
            "raw_rows": [],
            "total": 0,
        }

    # Fallback to the dedicated model
    stmt = select(FallbackModel).where(FallbackModel.connection_id == conn.id)
    if stream:
        stmt = stmt.where(FallbackModel.stream == stream)
    stmt = stmt.order_by(FallbackModel.date.desc()).limit(limit)
    rows = (await session.execute(stmt)).scalars().all()
    serialized_rows = [_row_dict(r) for r in rows]
    return {
        "table_name": f"{FallbackModel.__tablename__}",
        "columns": [],
        "rows": serialized_rows,
        "raw_rows": serialized_rows,
        "total": len(serialized_rows),
    }


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
async def _own(session, org, connection_id: int) -> Connection:
    conn = await session.get(Connection, connection_id)
    if conn is None or conn.organization_id != org.id:
        raise HTTPException(404, "Connection not found")
    return conn


def _serialize(c: Connection) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "connector_id": c.connector_id,
        "identity_id": c.oauth_identity_id,
        "resource_id": c.resource_id,
        "resource_name": c.resource_name,
        "config": c.config,
        "streams": c.streams,
        "enabled": c.enabled,
        "status": c.status,
        "status_detail": c.status_detail,
        "last_error_code": c.last_error_code,
        "last_error_message": c.last_error_message,
        "backfill_start_date": c.backfill_start_date.isoformat() if c.backfill_start_date else None,
        "lookback_days": c.lookback_days,
        "schedule_interval_seconds": c.schedule_interval_seconds,
        "last_run_at": _iso(c.last_run_at),
        "last_success_at": _iso(c.last_success_at),
        "next_run_at": _iso(c.next_run_at),
        "total_records_synced": c.total_records_synced,
        "consecutive_failures": c.consecutive_failures,
        "last_scheduled_run_at": _iso(c.last_scheduled_run_at),
        "last_scheduled_success_at": _iso(c.last_scheduled_success_at),
        "consecutive_scheduled_failures": c.consecutive_scheduled_failures,
        "destination": _destination_for(c.connector_id),
    }


def _destination_for(connector_id: str) -> dict:
    info = dict(destination_info())
    fact = info["fact_tables"].get(connector_id)
    tables = [fact] if fact else []
    # every source except GA4 also writes campaign-tree / object rows to ad_entities
    if connector_id != "google_analytics":
        tables.append(info["entity_table"])
    info["fact_table"] = fact
    info["tables"] = tables
    return info


def _serialize_run(r: SyncRun) -> dict:
    return {
        "id": r.id,
        "status": r.status,
        "trigger": r.trigger,
        "sync_mode": r.sync_mode,
        "phase": r.phase,
        "phase_detail": r.phase_detail,
        "started_at": _iso(r.started_at),
        "finished_at": _iso(r.finished_at),
        "duration_ms": r.duration_ms,
        "records_fetched": r.records_fetched,
        "records_inserted": r.records_inserted,
        "records_updated": r.records_updated,
        "records_skipped": r.records_skipped,
        "records_failed": r.records_failed,
        "api_calls": r.api_calls,
        "retry_count": r.retry_count,
        "rate_limit_events": r.rate_limit_events,
        "execution_id": r.execution_id,
        "worker_id": r.worker_id,
        "slices_completed": r.slices_completed,
        "slices_total": r.slices_total,
        "error_code": r.error_code,
        "error_message": r.error_message,
        "will_retry": r.will_retry,
    }


def _row_dict(row) -> dict:
    out = {}
    for col in row.__table__.columns:
        value = getattr(row, col.name)
        out[col.name] = value.isoformat() if isinstance(value, (datetime, date)) else value
    return out


def _iso(value) -> str | None:
    return value.isoformat() if value else None


__all__ = ["router"]
