"""Sync engine: the platform half of the connector/platform split.

A connector turns a date window into records; everything in this package turns
those records into committed rows and a durable cursor, on a schedule, with
retries and progress — the Airbyte job model (§28) without its process
isolation.

    runner.py     one connection, one run: CHECK -> DISCOVER -> read slices ->
                  write -> STATE COMMIT, emitting phase/progress to a SyncRun
    writer.py     the destination: batched upsert into <source>_performance / ad_entities,
                  skipped-record accounting
    state.py      per-stream cursor load and monotonic commit
    scheduler.py  in-process poll loop that claims due connections and runs them
"""

from app.sync.runner import SyncOutcome, run_connection
from app.sync.scheduler import SyncScheduler

__all__ = ["SyncOutcome", "SyncScheduler", "run_connection"]
