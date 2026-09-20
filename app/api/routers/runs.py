"""/sync-runs — per-run detail, stream breakdown, and structured errors (§17, §31)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from app.api.deps import OrgDep, SessionDep
from app.models import Connection, SyncError, SyncRun, SyncStreamStat

router = APIRouter(prefix="/sync-runs", tags=["runs"])


@router.get("")
async def recent_runs(org: OrgDep, session: SessionDep, limit: int = Query(50, le=200)):
    rows = (
        (
            await session.execute(
                select(SyncRun)
                .where(SyncRun.organization_id == org.id)
                .order_by(SyncRun.started_at.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    connectors = await _connector_ids(session, {r.connection_id for r in rows})
    return {"runs": [_run(r, connectors.get(r.connection_id)) for r in rows]}


@router.get("/{run_id}")
async def get_run(run_id: int, org: OrgDep, session: SessionDep):
    run = await session.get(SyncRun, run_id)
    if run is None or run.organization_id != org.id:
        raise HTTPException(404, "Run not found")
    stats = (
        (await session.execute(select(SyncStreamStat).where(SyncStreamStat.sync_run_id == run_id)))
        .scalars()
        .all()
    )
    errors = (
        (
            await session.execute(
                select(SyncError).where(SyncError.sync_run_id == run_id).order_by(SyncError.occurred_at)
            )
        )
        .scalars()
        .all()
    )
    connectors = await _connector_ids(session, {run.connection_id})
    data = _run(run, connectors.get(run.connection_id))
    data["streams"] = [_stat(s) for s in stats]
    data["errors"] = [_error(e) for e in errors]
    return data


async def _connector_ids(session, connection_ids: set[int]) -> dict[int, str]:
    if not connection_ids:
        return {}
    rows = await session.execute(
        select(Connection.id, Connection.connector_id).where(Connection.id.in_(connection_ids))
    )
    return {row.id: row.connector_id for row in rows}


def _run(r: SyncRun, connector_id: str | None = None) -> dict:
    return {
        "id": r.id,
        "connector_id": connector_id,
        "connection_id": r.connection_id,
        "execution_id": r.execution_id,
        "worker_id": r.worker_id,
        "status": r.status,
        "trigger": r.trigger,
        "sync_mode": r.sync_mode,
        "phase": r.phase,
        "phase_detail": r.phase_detail,
        "attempt": r.attempt,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "duration_ms": r.duration_ms,
        "records_fetched": r.records_fetched,
        "records_inserted": r.records_inserted,
        "records_updated": r.records_updated,
        "records_skipped": r.records_skipped,
        "records_failed": r.records_failed,
        "api_calls": r.api_calls,
        "retry_count": r.retry_count,
        "rate_limit_events": r.rate_limit_events,
        "checkpoint_before": r.state_before,
        "checkpoint_after": r.state_after,
        "slices_completed": r.slices_completed,
        "slices_total": r.slices_total,
        "warnings": r.warnings,
        "error_code": r.error_code,
        "error_message": r.error_message,
        "will_retry": r.will_retry,
    }


def _stat(s: SyncStreamStat) -> dict:
    return {
        "stream": s.stream,
        "status": s.status,
        "sync_mode": s.sync_mode,
        "records_fetched": s.records_fetched,
        "records_inserted": s.records_inserted,
        "records_updated": s.records_updated,
        "records_skipped": s.records_skipped,
        "records_failed": s.records_failed,
        "api_calls": s.api_calls,
        "retry_count": s.retry_count,
        "rate_limit_events": s.rate_limit_events,
        "started_at": s.started_at.isoformat() if s.started_at else None,
        "finished_at": s.finished_at.isoformat() if s.finished_at else None,
        "duration_ms": s.duration_ms,
        "checkpoint_before": s.checkpoint_before,
        "checkpoint_after": s.checkpoint_after,
        "reconciliation": s.reconciliation,
        "slices_completed": s.slices_completed,
        "slices_total": s.slices_total,
        "cursor_value": s.cursor_value,
        "error_code": s.error_code,
        "error_message": s.error_message,
    }


def _error(e: SyncError) -> dict:
    return {
        "code": e.code,
        "message": e.message,
        "provider": e.provider,
        "connector": e.connector_id,
        "stream": e.stream,
        "retryable": e.retryable,
        "recoverable": e.recoverable,
        "user_action": e.user_action,
        "http_status": e.http_status,
        "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
    }
