"""One connection, one run — the Airbyte job lifecycle (§28) as an async routine.

    CHECK           credentials + resource are usable, else fail fast (§14: never
                    retry a permanent auth error)
    DISCOVER        (implicit) the resource was chosen at connect time; CHECK
                    re-proves it exists
    for each stream:
        resolve window   cursor - lookback .. today, or the initial backfill
        slice            bounded date windows so a big property can't hang (§6)
        read -> write    stream records to the destination in batches (§32)
        STATE COMMIT     advance the cursor only after the rows are committed (§12)
    COMPLETE        succeeded / partial_success / failed, and reschedule

Progress (phase, counts, "slice 34/52") is written to the SyncRun row throttled,
so the UI can stream it (§17) without a DB write per record.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from app.connectors import errors as E
from app.connectors.base import (
    AnyRecord,
    ConnectorContext,
    EntityRecord,
    HealthStatus,
    Record,
    StreamDefinition,
    SyncMode,
)
from app.connectors.registry import load_connectors
from app.connectors.slicing import resolve_sync_window
from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import (
    CONN_ERROR,
    CONN_HEALTHY,
    CONN_INVALID_CONFIG,
    CONN_NEEDS_REAUTH,
    CONN_PERMISSION_DENIED,
    CONN_PROVIDER_UNAVAILABLE,
    CONN_RATE_LIMITED,
    RUN_CANCELLED,
    RUN_FAILED,
    RUN_PARTIAL,
    RUN_RUNNING,
    RUN_SUCCEEDED,
    Connection,
    SyncError,
    SyncRun,
    SyncStreamStat,
)
from app.oauth.service import DatabaseTokenProvider
from app.sync.state import commit_state, load_state
from app.sync.writer import DestinationWriter, WriteResult

logger = get_logger(__name__)

_BATCH = 1000  # records buffered before a destination flush

_HEALTH_TO_CONN_STATUS = {
    HealthStatus.NEEDS_REAUTH: CONN_NEEDS_REAUTH,
    HealthStatus.PERMISSION_DENIED: CONN_PERMISSION_DENIED,
    HealthStatus.RATE_LIMITED: CONN_RATE_LIMITED,
    HealthStatus.INVALID_CONFIGURATION: CONN_INVALID_CONFIG,
    HealthStatus.PROVIDER_UNAVAILABLE: CONN_PROVIDER_UNAVAILABLE,
}


@dataclass(slots=True)
class SyncOutcome:
    run_id: int
    status: str
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    streams_ok: list[str] = field(default_factory=list)
    streams_failed: list[str] = field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None
    will_retry: bool = False

    @property
    def ok(self) -> bool:
        return self.status in (RUN_SUCCEEDED, RUN_PARTIAL)


class RunProgress:
    """ProgressSink that persists phase/detail to the SyncRun row, throttled."""

    def __init__(self, run_id: int, *, min_interval: float = 1.0) -> None:
        self.run_id = run_id
        self._min_interval = min_interval
        self._last_write = 0.0
        self._phase = "starting"
        self._detail: str | None = None
        self._slices_done = 0
        self._slices_total = 0

    def phase(self, phase: str, detail: str | None = None) -> None:
        self._phase = phase
        self._detail = detail
        self._flush(force=True)

    def note(self, detail: str) -> None:
        self._detail = detail
        self._flush(force=False)

    def slice_progress(self, stream: str, completed: int, total: int) -> None:
        self._slices_done, self._slices_total = completed, total
        self._detail = f"{stream}: slice {completed}/{total}"
        self._flush(force=False)

    def _flush(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force and now - self._last_write < self._min_interval:
            return
        self._last_write = now
        # Fire-and-forget: progress must never block or break a sync.
        asyncio.ensure_future(self._write())  # noqa: RUF006

    async def _write(self) -> None:
        try:
            async with SessionLocal() as session:
                run = await session.get(SyncRun, self.run_id)
                if run is None:
                    return
                run.phase = self._phase[:40]
                run.phase_detail = (self._detail or "")[:2000] or None
                run.slices_completed = self._slices_done
                run.slices_total = self._slices_total
                await session.commit()
        except Exception:  # pragma: no cover - telemetry only
            logger.debug("progress write failed", exc_info=True)


async def run_connection(
    connection_id: int,
    *,
    trigger: str = "manual",
    sync_mode: str | None = None,
    worker_id: str = "local",
) -> SyncOutcome:
    settings = get_settings()
    started = datetime.now(UTC)

    async with SessionLocal() as session:
        conn = await session.get(Connection, connection_id)
        if conn is None:
            raise E.invalid_configuration(f"Connection {connection_id} does not exist.")
        identity_id = conn.oauth_identity_id
        connector_id = conn.connector_id
        org_id = conn.organization_id
        resource_id = conn.resource_id
        config = dict(conn.config or {})
        resource_metadata = dict(conn.resource_metadata or {})
        configured_streams = list(conn.streams or [])
        backfill_start = conn.backfill_start_date
        lookback_days = conn.lookback_days

        run = SyncRun(
            connection_id=connection_id,
            organization_id=org_id,
            trigger=trigger,
            sync_mode=sync_mode or "incremental",
            status=RUN_RUNNING,
            phase="authenticating",
        )
        session.add(run)
        conn.status = "syncing"
        conn.last_run_at = started
        await session.commit()
        run_id = run.id

    entry = load_connectors().get(connector_id)
    connector_cls = entry.connector_class
    provider = connector_cls.provider

    progress = RunProgress(run_id)
    token_provider = DatabaseTokenProvider(identity_id, settings=settings)
    ctx = ConnectorContext(
        token_provider=token_provider,
        config=config,
        resource_id=resource_id,
        resource_metadata=resource_metadata,
        progress=progress,
        provider_settings=_provider_settings(settings, provider),
    )

    writer = DestinationWriter(
        organization_id=org_id,
        connection_id=connection_id,
        connector_id=connector_id,
        provider=provider,
        resource_id=resource_id,
        sync_run_id=run_id,
    )

    outcome = SyncOutcome(run_id=run_id, status=RUN_RUNNING)
    connector = connector_cls(ctx)

    try:
        async with asyncio.timeout(settings.sync_run_timeout_seconds):
            await _run_inner(
                connector=connector,
                ctx=ctx,
                progress=progress,
                writer=writer,
                run_id=run_id,
                connection_id=connection_id,
                org_id=org_id,
                configured_streams=configured_streams,
                sync_mode=sync_mode,
                backfill_start=backfill_start,
                lookback_days=lookback_days,
                default_backfill_days=settings.default_backfill_days,
                outcome=outcome,
            )
    except asyncio.CancelledError:
        outcome.status = RUN_CANCELLED
        outcome.error_code = E.ErrorCode.CANCELLED
        outcome.error_message = "The sync was cancelled."
        await _finalize(connection_id, run_id, outcome, started)
        raise
    except TimeoutError:
        outcome.status = RUN_FAILED
        outcome.error_code = E.ErrorCode.TIMEOUT
        outcome.error_message = (
            f"The sync exceeded its {settings.sync_run_timeout_seconds}s ceiling and was stopped."
        )
        outcome.will_retry = True
        await _record_error(
            run_id,
            connection_id,
            org_id,
            E.timeout_error(outcome.error_message, provider=provider, connector_id=connector_id),
        )
    except E.ConnectorError as exc:
        outcome.status = RUN_FAILED
        outcome.error_code = exc.code
        outcome.error_message = exc.message
        outcome.will_retry = exc.retryable
        await _record_error(run_id, connection_id, org_id, exc)
    except Exception as exc:  # noqa: BLE001 - nothing escapes the runner untyped
        wrapped = E.wrap_unexpected(exc, provider=provider, connector_id=connector_id)
        outcome.status = RUN_FAILED
        outcome.error_code = wrapped.code
        outcome.error_message = wrapped.message
        logger.exception("Unhandled error in sync run %s", run_id)
        await _record_error(run_id, connection_id, org_id, wrapped)
    finally:
        await connector.aclose()

    if outcome.status == RUN_RUNNING:
        if outcome.streams_failed and outcome.streams_ok:
            outcome.status = RUN_PARTIAL
        elif outcome.streams_failed:
            outcome.status = RUN_FAILED
        else:
            outcome.status = RUN_SUCCEEDED

    await _finalize(connection_id, run_id, outcome, started)
    return outcome


async def _run_inner(
    *,
    connector,
    ctx: ConnectorContext,
    progress: RunProgress,
    writer: DestinationWriter,
    run_id: int,
    connection_id: int,
    org_id: int,
    configured_streams: list[dict[str, Any]],
    sync_mode: str | None,
    backfill_start: date | None,
    lookback_days: int,
    default_backfill_days: int,
    outcome: SyncOutcome,
) -> None:
    progress.phase("checking", "Verifying credentials and access")
    health = await connector.check_connection()
    if not health.ok:
        conn_status = _HEALTH_TO_CONN_STATUS.get(health.status, CONN_ERROR)
        await _set_connection_status(connection_id, conn_status, health.message)
        err = health.error or E.ConnectorError(code=E.ErrorCode.UNKNOWN_ERROR, message=health.message)
        outcome.status = RUN_FAILED
        outcome.error_code = err.code
        outcome.error_message = health.message
        outcome.will_retry = err.retryable
        await _record_error(run_id, connection_id, org_id, err)
        return

    streams = _select_streams(connector, configured_streams)
    if not streams:
        raise E.invalid_configuration(
            "This connection has no enabled streams. Edit the connection and select at least one."
        )

    today = datetime.now(UTC).date()

    for stream_def, mode in streams:
        stat = _new_stat(run_id, stream_def.name, mode)
        try:
            written = await _sync_stream(
                connector=connector,
                progress=progress,
                writer=writer,
                stream_def=stream_def,
                mode=mode,
                connection_id=connection_id,
                today=today,
                backfill_start=backfill_start,
                lookback_days=lookback_days,
                default_backfill_days=default_backfill_days,
                stat=stat,
            )
            outcome.records_fetched += written.fetched
            outcome.records_inserted += written.inserted
            outcome.records_updated += written.updated
            outcome.records_skipped += written.skipped
            outcome.streams_ok.append(stream_def.name)
            stat["status"] = RUN_SUCCEEDED
        except asyncio.CancelledError:
            raise
        except E.ConnectorError as exc:
            outcome.streams_failed.append(stream_def.name)
            outcome.will_retry = outcome.will_retry or exc.retryable
            if outcome.error_code is None:
                outcome.error_code, outcome.error_message = exc.code, exc.message
            stat["status"] = RUN_FAILED
            stat["error_code"] = exc.code
            stat["error_message"] = exc.message
            await _record_error(run_id, connection_id, org_id, exc, stream=stream_def.name)
            if exc.code == E.ErrorCode.AUTHENTICATION_ERROR:
                # Dead credentials: the rest of the streams will only fail the
                # same way. Stop and let the connection go to needs_reauth.
                await _persist_stat(stat)
                await _set_connection_status(connection_id, CONN_NEEDS_REAUTH, exc.message)
                raise
        except Exception as exc:  # noqa: BLE001
            wrapped = E.wrap_unexpected(exc, connector_id=connector.connector_id, stream=stream_def.name)
            outcome.streams_failed.append(stream_def.name)
            stat["status"] = RUN_FAILED
            stat["error_code"] = wrapped.code
            stat["error_message"] = wrapped.message
            logger.exception("Stream %s failed in run %s", stream_def.name, run_id)
            await _record_error(run_id, connection_id, org_id, wrapped, stream=stream_def.name)
        await _persist_stat(stat)


async def _sync_stream(
    *,
    connector,
    progress: RunProgress,
    writer: DestinationWriter,
    stream_def: StreamDefinition,
    mode: SyncMode,
    connection_id: int,
    today: date,
    backfill_start: date | None,
    lookback_days: int,
    default_backfill_days: int,
    stat: dict[str, Any],
) -> WriteResult:
    state = await load_state(connection_id, stream_def.name)
    state.cursor_field = state.cursor_field or stream_def.default_cursor_field

    total = WriteResult()

    if stream_def.date_partitioned and stream_def.grain == "fact":
        cursor_value = None if mode == SyncMode.FULL_REFRESH else state.cursor_value
        window = resolve_sync_window(
            cursor_value=cursor_value,
            today=today,
            backfill_start=backfill_start,
            lookback_days=lookback_days,
            default_backfill_days=default_backfill_days,
            max_history_days=connector.__class__.max_history_days,
            provider_lag_days=connector.__class__.provider_lag_days,
        )
        if window is None:
            progress.note(f"{stream_def.name}: up to date")
            return total
        progress.phase("fetching", f"{stream_def.name}: {window.reason}")
        slices = list(connector.slices(stream_def, window.start, window.end))
        reached: date | None = None
        for i, slc in enumerate(slices, start=1):
            progress.slice_progress(stream_def.name, i - 1, len(slices))
            written = await _drain_slice(connector, writer, stream_def, slc)
            total.add(written)
            reached = slc.end_date or reached
            # Commit the cursor per slice so an interrupted backfill resumes here.
            if reached is not None:
                await commit_state(state, reached=reached, added_records=written.inserted + written.updated)
        progress.slice_progress(stream_def.name, len(slices), len(slices))
        stat["slices_total"] = len(slices)
        stat["slices_completed"] = len(slices)
        stat["cursor_value"] = state.cursor_value
    else:
        # Entity / non-partitioned stream: fetch whole each run.
        progress.phase("fetching", f"{stream_def.name}: full")
        from app.connectors.base import StreamSlice

        written = await _drain_slice(connector, writer, stream_def, StreamSlice())
        total.add(written)
        await commit_state(state, reached=today, added_records=written.inserted + written.updated)
        stat["slices_total"] = 1
        stat["slices_completed"] = 1

    stat["records_fetched"] = total.fetched
    stat["records_inserted"] = total.inserted
    stat["records_updated"] = total.updated
    stat["records_skipped"] = total.skipped
    return total


async def _drain_slice(connector, writer: DestinationWriter, stream_def, slc) -> WriteResult:
    """Iterate one slice, flushing to the destination every _BATCH records."""
    facts: list[Record] = []
    entities: list[EntityRecord] = []
    result = WriteResult()

    async def flush() -> None:
        nonlocal facts, entities
        if facts:
            result.add(await writer.write_records(stream_def, facts))
            facts = []
        if entities:
            result.add(await writer.write_entities(entities))
            entities = []

    record: AnyRecord
    async for record in connector.read_slice(stream_def, slc):
        if isinstance(record, EntityRecord):
            entities.append(record)
        else:
            facts.append(record)
        if len(facts) >= _BATCH or len(entities) >= _BATCH:
            await flush()
    await flush()
    return result


# --- helpers -------------------------------------------------------------------


def _provider_settings(settings, provider: str) -> dict[str, Any]:
    if provider == "google":
        return {
            "google_ads_developer_token": settings.google_ads_developer_token,
            "google_ads_api_version": settings.google_ads_api_version,
            "google_ads_login_customer_id": settings.google_ads_login_customer_id,
        }
    if provider == "meta":
        return {
            "meta_api_version": settings.meta_api_version,
            "meta_app_id": settings.meta_app_id,
            "meta_app_secret": settings.meta_app_secret,
        }
    return {}


def _select_streams(connector, configured: list[dict[str, Any]]) -> list[tuple[StreamDefinition, SyncMode]]:
    declared = {s.name: s for s in connector.get_streams()}
    chosen: list[tuple[StreamDefinition, SyncMode]] = []
    if not configured:
        # No explicit selection: sync every declared stream incrementally.
        return [(s, SyncMode.INCREMENTAL) for s in declared.values()]
    for item in configured:
        name = item.get("stream") or item.get("name")
        if not name or item.get("enabled") is False:
            continue
        stream_def = declared.get(name)
        if stream_def is None:
            continue
        raw_mode = (item.get("sync_mode") or "incremental").lower()
        mode = SyncMode.FULL_REFRESH if raw_mode == "full_refresh" else SyncMode.INCREMENTAL
        if mode == SyncMode.INCREMENTAL and not stream_def.supports_incremental:
            mode = SyncMode.FULL_REFRESH
        chosen.append((stream_def, mode))
    return chosen


def _new_stat(run_id: int, stream: str, mode: SyncMode) -> dict[str, Any]:
    return {
        "sync_run_id": run_id,
        "stream": stream,
        "status": RUN_RUNNING,
        "sync_mode": str(mode),
        "records_fetched": 0,
        "records_inserted": 0,
        "records_updated": 0,
        "records_skipped": 0,
        "api_calls": 0,
        "slices_completed": 0,
        "slices_total": 0,
        "cursor_value": None,
        "error_code": None,
        "error_message": None,
    }


async def _persist_stat(stat: dict[str, Any]) -> None:
    from sqlalchemy import select

    async with SessionLocal() as session:
        row = (
            await session.execute(
                select(SyncStreamStat).where(
                    SyncStreamStat.sync_run_id == stat["sync_run_id"],
                    SyncStreamStat.stream == stat["stream"],
                )
            )
        ).scalar_one_or_none()
        if row is None:
            row = SyncStreamStat(sync_run_id=stat["sync_run_id"], stream=stat["stream"])
            session.add(row)
        for key, value in stat.items():
            if key in ("sync_run_id", "stream"):
                continue
            setattr(row, key, value)
        await session.commit()


async def _record_error(
    run_id: int, connection_id: int, org_id: int, exc: E.ConnectorError, *, stream: str | None = None
) -> None:
    async with SessionLocal() as session:
        session.add(
            SyncError(
                sync_run_id=run_id,
                connection_id=connection_id,
                organization_id=org_id,
                stream=stream or exc.stream,
                code=exc.code,
                message=exc.message,
                provider=exc.provider,
                connector_id=exc.connector_id,
                retryable=exc.retryable,
                recoverable=exc.recoverable,
                user_action=exc.user_action,
                http_status=exc.http_status,
                details=exc.technical_details or None,
            )
        )
        run = await session.get(SyncRun, run_id)
        if run is not None and run.error_code is None:
            run.error_code = exc.code
            run.error_message = exc.message
            run.will_retry = exc.retryable
        await session.commit()


async def _set_connection_status(connection_id: int, status: str, detail: str | None) -> None:
    async with SessionLocal() as session:
        conn = await session.get(Connection, connection_id)
        if conn is not None:
            conn.status = status
            conn.status_detail = (detail or "")[:2000] or None
        await session.commit()


async def _finalize(connection_id: int, run_id: int, outcome: SyncOutcome, started: datetime) -> None:
    finished = datetime.now(UTC)
    async with SessionLocal() as session:
        run = await session.get(SyncRun, run_id)
        conn = await session.get(Connection, connection_id)
        if run is not None:
            run.status = outcome.status
            run.finished_at = finished
            run.duration_ms = int((finished - started).total_seconds() * 1000)
            run.records_fetched = outcome.records_fetched
            run.records_inserted = outcome.records_inserted
            run.records_updated = outcome.records_updated
            run.records_skipped = outcome.records_skipped
            run.phase = "completed" if outcome.ok else "failed"
            if outcome.error_code and run.error_code is None:
                run.error_code = outcome.error_code
                run.error_message = outcome.error_message
            run.will_retry = outcome.will_retry
        if conn is not None:
            conn.last_run_at = finished
            written = outcome.records_inserted + outcome.records_updated
            conn.total_records_synced = (conn.total_records_synced or 0) + written
            if outcome.ok:
                conn.last_success_at = finished
                conn.consecutive_failures = 0
                conn.status = CONN_HEALTHY
                conn.status_detail = None
                conn.last_error_code = None
                conn.last_error_message = None
            else:
                conn.consecutive_failures = (conn.consecutive_failures or 0) + 1
                conn.last_error_code = outcome.error_code
                conn.last_error_message = outcome.error_message
                if conn.status in ("syncing", CONN_HEALTHY):
                    conn.status = CONN_ERROR
            conn.next_run_at = _next_run_at(conn, finished, outcome)
            conn.locked_at = None
            conn.locked_by = None
        await session.commit()


def _next_run_at(conn: Connection, now: datetime, outcome: SyncOutcome) -> datetime | None:
    if not conn.enabled or conn.schedule_interval_seconds is None:
        return None
    interval = conn.schedule_interval_seconds
    if not outcome.ok and outcome.will_retry:
        # Backoff on repeated failure, capped, so a broken connection doesn't
        # hammer the provider every interval.
        backoff = min(interval * (2 ** min(conn.consecutive_failures, 6)), 6 * 3600)
        return now + timedelta(seconds=max(interval, backoff))
    # `config.daily_at` ("HH:MM", optionally with `config.daily_at_offset_minutes`
    # for a non-UTC wall clock) pins the run to a fixed time of day with no drift.
    daily_at = (conn.config or {}).get("daily_at")
    if daily_at:
        pinned = _next_daily_at(now, daily_at, (conn.config or {}).get("daily_at_offset_minutes", 0))
        if pinned is not None:
            return pinned
    return now + timedelta(seconds=interval)


def _next_daily_at(now_utc: datetime, hhmm: str, offset_minutes: int) -> datetime | None:
    """Next occurrence of wall-clock `hhmm` in the given UTC offset, as aware UTC."""
    try:
        hh, mm = (int(part) for part in hhmm.split(":", 1))
        off = timedelta(minutes=int(offset_minutes))
    except (ValueError, TypeError):
        return None
    local = (now_utc.astimezone(UTC) + off).replace(tzinfo=None)  # naive local wall time
    target = local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    return (target - off).replace(tzinfo=UTC)


__all__ = ["RunProgress", "SyncOutcome", "run_connection"]
