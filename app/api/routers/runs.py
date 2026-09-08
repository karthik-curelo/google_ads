"""/sync-runs — per-run detail, stream breakdown, and structured errors (§17, §31)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from app.api.deps import OrgDep, SessionDep
from app.models import SyncError, SyncRun, SyncStreamStat

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
    return {"runs": [_run(r) for r in rows]}


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
    data = _run(run)
    data["streams"] = [_stat(s) for s in stats]
    data["errors"] = [_error(e) for e in errors]
    return data


def _run(r: SyncRun) -> dict:
    return {
        "id": r.id,
        "connection_id": r.connection_id,
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
        "api_calls": r.api_calls,
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
