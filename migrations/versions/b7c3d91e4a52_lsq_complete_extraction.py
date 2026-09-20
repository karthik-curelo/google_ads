"""leadsquared complete extraction: typed columns, tombstones, run observability

Additive only — every new column is nullable or has a server default, and nothing is
dropped or retyped — so a service still running the previous release keeps working
against the migrated schema (deploy the migration first, then the code).

  leadsquared_leads / leadsquared_activities
      source_modified_on   the timestamp the source API actually filters on
                           (LeadLastModifiedOn / ModifiedOn) — incremental cursor and
                           upsert ordering guard
      source_created_on    CreatedOn as a real timestamp
      activity_event(+_name)  activity type as a first-class discriminator
      prospect_stage, owner_id   promoted from the (now complete) lead payload
      deleted_at           tombstone written by the deletion sweep; rows are kept

  sync_runs / sync_stream_stats / connections
      execution id, worker id, failed/retry/rate-limit counters, per-stream
      checkpoints and reconciliation evidence, and the *scheduled* run health that a
      manual run must not be able to hide.

Backfill (PostgreSQL only): existing activity rows get `activity_event`,
`source_created_on` and `source_modified_on` from their stored raw payload, whose
`ModifiedOn` IS the activity filter column. Existing lead rows keep NULL
`source_modified_on` — their stored payload predates LeadLastModifiedOn — and are
filled by the complete backfill the new sync performs (legacy cursors are ignored).

Revision ID: b7c3d91e4a52
Revises: 75e240dcb0fe
Create Date: 2026-09-19 18:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import app.models.base

revision: str = "b7c3d91e4a52"
down_revision: str | None = "75e240dcb0fe"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TS = app.models.base.UTCDateTime(timezone=True)
_JSON = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def _zero() -> sa.sql.elements.TextClause:
    return sa.text("0")


def upgrade() -> None:
    # --- connections: scheduled-path health ---------------------------------------
    op.add_column("connections", sa.Column("lease_expires_at", _TS, nullable=True))
    op.add_column("connections", sa.Column("last_scheduled_run_at", _TS, nullable=True))
    op.add_column("connections", sa.Column("last_scheduled_success_at", _TS, nullable=True))
    op.add_column(
        "connections",
        sa.Column("consecutive_scheduled_failures", sa.Integer(), nullable=False, server_default=_zero()),
    )

    # --- sync_runs -------------------------------------------------------------
    op.add_column("sync_runs", sa.Column("records_failed", sa.Integer(), nullable=False, server_default=_zero()))
    op.add_column("sync_runs", sa.Column("retry_count", sa.Integer(), nullable=False, server_default=_zero()))
    op.add_column("sync_runs", sa.Column("rate_limit_events", sa.Integer(), nullable=False, server_default=_zero()))
    op.add_column("sync_runs", sa.Column("execution_id", sa.String(length=40), nullable=True))
    op.add_column("sync_runs", sa.Column("worker_id", sa.String(length=120), nullable=True))
    op.create_index(op.f("ix_sync_runs_execution_id"), "sync_runs", ["execution_id"], unique=False)

    # --- sync_stream_stats -----------------------------------------------------------
    op.add_column("sync_stream_stats", sa.Column("started_at", _TS, nullable=True))
    op.add_column("sync_stream_stats", sa.Column("finished_at", _TS, nullable=True))
    op.add_column("sync_stream_stats", sa.Column("duration_ms", sa.Integer(), nullable=True))
    op.add_column("sync_stream_stats", sa.Column("records_failed", sa.Integer(), nullable=False, server_default=_zero()))
    op.add_column("sync_stream_stats", sa.Column("retry_count", sa.Integer(), nullable=False, server_default=_zero()))
    op.add_column(
        "sync_stream_stats", sa.Column("rate_limit_events", sa.Integer(), nullable=False, server_default=_zero())
    )
    op.add_column("sync_stream_stats", sa.Column("checkpoint_before", sa.String(length=120), nullable=True))
    op.add_column("sync_stream_stats", sa.Column("checkpoint_after", sa.String(length=120), nullable=True))
    op.add_column("sync_stream_stats", sa.Column("reconciliation", _JSON, nullable=True))

    # --- leadsquared_leads -------------------------------------------------------
    op.add_column("leadsquared_leads", sa.Column("source_modified_on", _TS, nullable=True))
    op.add_column("leadsquared_leads", sa.Column("source_created_on", _TS, nullable=True))
    op.add_column("leadsquared_leads", sa.Column("prospect_stage", sa.String(length=120), nullable=True))
    op.add_column("leadsquared_leads", sa.Column("owner_id", sa.String(length=64), nullable=True))
    op.add_column("leadsquared_leads", sa.Column("deleted_at", _TS, nullable=True))
    op.create_index(
        "ix_leadsquared_leads_conn_src_modified",
        "leadsquared_leads",
        ["connection_id", "source_modified_on"],
        unique=False,
    )

    # --- leadsquared_activities --------------------------------------------------------
    op.add_column("leadsquared_activities", sa.Column("activity_event", sa.Integer(), nullable=True))
    op.add_column("leadsquared_activities", sa.Column("activity_event_name", sa.String(length=120), nullable=True))
    op.add_column("leadsquared_activities", sa.Column("source_modified_on", _TS, nullable=True))
    op.add_column("leadsquared_activities", sa.Column("source_created_on", _TS, nullable=True))
    op.add_column("leadsquared_activities", sa.Column("deleted_at", _TS, nullable=True))
    op.create_index(
        "ix_leadsquared_activities_conn_event_modified",
        "leadsquared_activities",
        ["connection_id", "activity_event", "source_modified_on"],
        unique=False,
    )

    # --- backfill existing activity rows from their stored raw payload (PostgreSQL) -----
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            r"""
            UPDATE leadsquared_activities
               SET activity_event    = CASE WHEN raw->>'ActivityEvent' ~ '^[0-9]+$'
                                            THEN (raw->>'ActivityEvent')::int END,
                   source_created_on = CASE WHEN raw->>'CreatedOn' ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
                                            THEN (substr(raw->>'CreatedOn', 1, 19))::timestamp AT TIME ZONE 'UTC' END,
                   source_modified_on = CASE WHEN raw->>'ModifiedOn' ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
                                             THEN (substr(raw->>'ModifiedOn', 1, 19))::timestamp AT TIME ZONE 'UTC' END
             WHERE source_modified_on IS NULL
            """
        )


def downgrade() -> None:
    op.drop_index("ix_leadsquared_activities_conn_event_modified", table_name="leadsquared_activities")
    for col in ("deleted_at", "source_created_on", "source_modified_on", "activity_event_name", "activity_event"):
        op.drop_column("leadsquared_activities", col)

    op.drop_index("ix_leadsquared_leads_conn_src_modified", table_name="leadsquared_leads")
    for col in ("deleted_at", "owner_id", "prospect_stage", "source_created_on", "source_modified_on"):
        op.drop_column("leadsquared_leads", col)

    for col in (
        "reconciliation",
        "checkpoint_after",
        "checkpoint_before",
        "rate_limit_events",
        "retry_count",
        "records_failed",
        "duration_ms",
        "finished_at",
        "started_at",
    ):
        op.drop_column("sync_stream_stats", col)

    op.drop_index(op.f("ix_sync_runs_execution_id"), table_name="sync_runs")
    for col in ("worker_id", "execution_id", "rate_limit_events", "retry_count", "records_failed"):
        op.drop_column("sync_runs", col)

    for col in (
        "consecutive_scheduled_failures",
        "last_scheduled_success_at",
        "last_scheduled_run_at",
        "lease_expires_at",
    ):
        op.drop_column("connections", col)
