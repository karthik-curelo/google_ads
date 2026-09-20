"""Operator CLI for the sync engine.

    python -m app.sync.cli sync      --connection 11 [--timeout 86400]
    python -m app.sync.cli counts    --connection 11
    python -m app.sync.cli reconcile --connection 11 --stream booking_created \
                                     --from 2026-06-01 --to 2026-08-31 [--apply]
    python -m app.sync.cli preflight

`sync` is a normal manual run (it takes the connection's lease like any other caller,
so it cannot overlap the scheduler) with the run-time ceiling raised for a first
backfill. `counts` prints source vs warehouse per stream. `reconcile` is the deletion
sweep — a dry run unless `--apply`. `preflight` reports connections this process's
environment cannot run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta

from app.connectors.base import ConnectorContext, StaticTokenProvider
from app.connectors.leadsquared.connector import LeadSquaredCRMConnector
from app.connectors.leadsquared.reconcile import source_vs_warehouse, sweep_deletions
from app.connectors.registry import load_connectors
from app.core.config import get_settings
from app.core.database import SessionLocal, engine
from app.core.logging import setup_logging
from app.models import Connection
from app.sync.preflight import misconfigured_connections
from app.sync.runner import _provider_settings, run_connection


async def _lsq(connection_id: int) -> tuple[LeadSquaredCRMConnector, Connection]:
    async with SessionLocal() as session:
        conn = await session.get(Connection, connection_id)
    if conn is None or conn.connector_id != "leadsquared":
        raise SystemExit(f"connection {connection_id} is not a LeadSquared connection")
    ctx = ConnectorContext(
        token_provider=StaticTokenProvider(),
        config=dict(conn.config or {}),
        resource_id=conn.resource_id,
        provider_settings=_provider_settings(get_settings(), "leadsquared"),
    )
    connector = LeadSquaredCRMConnector(ctx)
    report = await connector.check_connection()  # also discovers any new activity types
    if not report.ok:
        raise SystemExit(f"LeadSquared check failed: {report.message}")
    return connector, conn


def _streams(connector: LeadSquaredCRMConnector, conn: Connection):
    wanted = {s.get("stream") or s.get("name") for s in (conn.streams or []) if s.get("enabled", True)}
    return [s for s in connector.get_streams() if not wanted or s.name in wanted]


async def cmd_sync(args) -> int:
    get_settings().sync_run_timeout_seconds = args.timeout
    outcome = await run_connection(args.connection, trigger="manual")
    if outcome is None:
        print("another worker already holds this connection's lease — nothing run")
        return 2
    print(
        json.dumps(
            {
                "run_id": outcome.run_id,
                "status": outcome.status,
                "fetched": outcome.records_fetched,
                "inserted": outcome.records_inserted,
                "updated": outcome.records_updated,
                "skipped": outcome.records_skipped,
                "failed": outcome.records_failed,
                "api_calls": outcome.api_calls,
                "retries": outcome.retry_count,
                "rate_limit_events": outcome.rate_limit_events,
                "streams_ok": len(outcome.streams_ok),
                "streams_failed": outcome.streams_failed,
                "error": outcome.error_message,
            },
            indent=2,
        )
    )
    return 0 if outcome.ok else 1


async def cmd_counts(args) -> int:
    connector, conn = await _lsq(args.connection)
    since = datetime.fromisoformat(args.since) if args.since else None
    until = datetime.fromisoformat(args.until) if args.until else None
    try:
        rows = await source_vs_warehouse(
            connector, _streams(connector, conn), conn.id, since=since, until=until
        )
    finally:
        await connector.aclose()
    print(
        f"{'stream':<40}{'source':>12}{'warehouse':>12}{'live':>12}{'tombstoned':>12}{'src-live':>10}  checkpoint"
    )
    for r in rows:
        if r.source_total == 0 and r.warehouse_total == 0 and not args.all:
            continue
        print(
            f"{r.stream:<40}{r.source_total:>12,}{r.warehouse_total:>12,}{r.warehouse_live:>12,}"
            f"{r.tombstoned:>12,}{r.difference:>10,}  {r.checkpoint or '-'}"
        )
    print(
        f"\nTOTAL source {sum(r.source_total for r in rows):,} | warehouse live {sum(r.warehouse_live for r in rows):,}"
    )
    return 0


async def cmd_reconcile(args) -> int:
    connector, conn = await _lsq(args.connection)
    stream = next((s for s in connector.get_streams() if s.name == args.stream), None)
    if stream is None:
        raise SystemExit(f"unknown stream {args.stream!r}")
    try:
        report = await sweep_deletions(
            connector,
            stream,
            conn.id,
            datetime.fromisoformat(args.start),
            datetime.fromisoformat(args.end)
            + timedelta(days=1),  # --to is inclusive: end at the next midnight
            apply=args.apply,
        )
    finally:
        await connector.aclose()
    print(json.dumps(report.as_dict(), indent=2))
    return 0


async def cmd_preflight(_args) -> int:
    problems = await misconfigured_connections()
    for p in problems:
        print(f"MISCONFIGURED connection {p.connection_id} ({p.connection_name}): {p.reason}")
    if not problems:
        print("ok — every enabled connection can run in this environment")
    return 1 if problems else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.sync.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("sync")
    p.add_argument("--connection", type=int, required=True)
    p.add_argument("--timeout", type=int, default=86400, help="run-time ceiling in seconds (default 24h)")
    p.set_defaults(fn=cmd_sync)
    p = sub.add_parser("counts")
    p.add_argument("--connection", type=int, required=True)
    p.add_argument("--all", action="store_true", help="also list streams that are empty on both sides")
    p.add_argument(
        "--since", help="compare only rows modified at/after this UTC time (YYYY-MM-DD[ HH:MM:SS])"
    )
    p.add_argument("--until", help="...and at/before this UTC time (default: now)")
    p.set_defaults(fn=cmd_counts)
    p = sub.add_parser("reconcile")
    p.add_argument("--connection", type=int, required=True)
    p.add_argument("--stream", required=True)
    p.add_argument("--from", dest="start", required=True)
    p.add_argument("--to", dest="end", required=True)
    p.add_argument("--apply", action="store_true", help="tombstone confirmed deletions (default: dry run)")
    p.set_defaults(fn=cmd_reconcile)
    p = sub.add_parser("preflight")
    p.set_defaults(fn=cmd_preflight)

    args = parser.parse_args(argv)
    setup_logging()
    load_connectors()

    async def _run() -> int:
        try:
            return await args.fn(args)
        finally:
            await engine.dispose()

    return asyncio.run(_run())


if __name__ == "__main__":
    sys.exit(main())
