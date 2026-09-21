"""leadsquared leads: owner / dietician / disposition columns and a reporting view

Additive only (nullable columns, one new view) - the previous release keeps working
against the migrated schema, so deploy the migration first, then the code.

The values already sit in every lead's stored `raw` payload (the full ~206 attributes are
extracted); they were simply not addressable, so a reporting tool that only sees columns
could not answer "leads by dietician and consultation disposition". They are promoted to
typed columns, and `v_leadsquared_lead_dispositions` exposes them next to the lead's dates.

  owner_name                     OwnerIdName
  assigned_dietician             mx_Assigned_Dietician
  disposition                    mx_Disposition
  diet_consultation_disposition  mx_Diet_Consultation_Disposition
  diet_consultation_at           mx_Diet_Consultation_DateTime (UTC, like every LSQ timestamp)

These are the CURRENT values: LeadSquared keeps no per-change history on the lead, so
the columns answer "how do leads stand now", not "how did they stand on day X".

Backfill (PostgreSQL only): existing rows are filled from their stored raw payload. Rows
whose payload predates the complete extraction simply have none of these keys and stay NULL
until the sync next reads them.

Revision ID: e2a9c5b17f03
Revises: b7c3d91e4a52
Create Date: 2026-09-21 09:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import app.models.base

revision: str = "e2a9c5b17f03"
down_revision: str | None = "b7c3d91e4a52"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TS = app.models.base.UTCDateTime(timezone=True)

_VIEW = """
CREATE OR REPLACE VIEW v_leadsquared_lead_dispositions AS
SELECT
    connection_id,
    prospect_id,
    source,
    prospect_stage,
    owner_id,
    owner_name,
    assigned_dietician,
    disposition,
    diet_consultation_disposition,
    diet_consultation_at,
    source_created_on,
    source_modified_on
FROM leadsquared_leads
WHERE deleted_at IS NULL
"""

_BACKFILL = r"""
UPDATE leadsquared_leads
   SET owner_name = left(nullif(btrim(raw->>'OwnerIdName'), ''), 200),
       assigned_dietician = left(nullif(btrim(raw->>'mx_Assigned_Dietician'), ''), 200),
       disposition = left(nullif(btrim(raw->>'mx_Disposition'), ''), 120),
       diet_consultation_disposition = left(nullif(btrim(raw->>'mx_Diet_Consultation_Disposition'), ''), 120),
       diet_consultation_at = CASE
           WHEN raw->>'mx_Diet_Consultation_DateTime' ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
           THEN (substr(raw->>'mx_Diet_Consultation_DateTime', 1, 19))::timestamp AT TIME ZONE 'UTC'
       END
 WHERE raw->>'OwnerIdName' IS NOT NULL
    OR raw->>'mx_Assigned_Dietician' IS NOT NULL
    OR raw->>'mx_Disposition' IS NOT NULL
    OR raw->>'mx_Diet_Consultation_Disposition' IS NOT NULL
    OR raw->>'mx_Diet_Consultation_DateTime' IS NOT NULL
"""


def upgrade() -> None:
    op.add_column("leadsquared_leads", sa.Column("owner_name", sa.String(length=200), nullable=True))
    op.add_column("leadsquared_leads", sa.Column("assigned_dietician", sa.String(length=200), nullable=True))
    op.add_column("leadsquared_leads", sa.Column("disposition", sa.String(length=120), nullable=True))
    op.add_column(
        "leadsquared_leads", sa.Column("diet_consultation_disposition", sa.String(length=120), nullable=True)
    )
    op.add_column("leadsquared_leads", sa.Column("diet_consultation_at", _TS, nullable=True))

    if op.get_bind().dialect.name == "postgresql":
        op.execute(_BACKFILL)
        op.execute(_VIEW)


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP VIEW IF EXISTS v_leadsquared_lead_dispositions")
    for col in (
        "diet_consultation_at",
        "diet_consultation_disposition",
        "disposition",
        "assigned_dietician",
        "owner_name",
    ):
        op.drop_column("leadsquared_leads", col)
