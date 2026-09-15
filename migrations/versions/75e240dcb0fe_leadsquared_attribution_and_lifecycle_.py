"""leadsquared attribution and lifecycle views

Two read-only, computed views (Phase H / Phase I of the LSQ implementation;
see docs/coverage/LSQ_VERIFICATION_2026-09-11.md for the design baseline).
Neither declares an authoritative revenue figure or a generic "converted"
boolean — per implementation instructions §12/§13, that stays an open
business decision (§9 of the verification report). Postgres-only (LATERAL
joins, regex match) — this project's migrations already assume Postgres in
practice (the schema-split migration this one builds on uses `DROP TABLE ...
CASCADE`, which SQLite cannot parse either).

v_leadsquared_lead_attribution
    One row per lead, resolving the strongest available join to `ad_entities`
    using the confirmed hierarchy: Meta ad -> Meta adset -> Meta campaign ->
    Google campaign -> Google ad_group, each candidate gated by the
    platform's live-confirmed ID-length signature (Google 9-12 digits, Meta
    17-18) before it is even attempted, and joined on `organization_id` (a
    lead's own connection is the LeadSquared connection; the matching
    ad_entities rows live under a *different* connection — the Google
    Ads/Meta Ads one — so organization_id, not connection_id, is the correct
    scope). Never joins on GCLID or on names (§7/§8/§13).

v_leadsquared_booking_lifecycle
    One row per booking_id (sourced from `booking_created`/206, the only
    stream where every real booking is guaranteed to appear), left-joined to
    its latest `post_booking_order_status`/208 status (if any — §15: absence
    is a normal state, not an error), a count of how many 208 rows exist for
    it, and its `booking_cancelled`/223 record (if any). Surfaces raw facts
    only — latest status string, each source's own amount fields, a plain
    `is_cancelled` existence flag — deliberately not a single revenue number
    or a conversion verdict.

Revision ID: 75e240dcb0fe
Revises: 0cb508e4e171
Create Date: 2026-09-11 13:55:07.768166
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = '75e240dcb0fe'
down_revision: str | None = '0cb508e4e171'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_ATTRIBUTION_VIEW = """
CREATE OR REPLACE VIEW v_leadsquared_lead_attribution AS
SELECT
    l.id AS lead_row_id,
    l.organization_id,
    l.connection_id,
    l.prospect_id,
    l.source,
    l.date,
    l.dimensions ->> 'mx_source_campaign_id' AS mx_source_campaign_id,
    l.dimensions ->> 'mx_ad_id'              AS mx_ad_id,
    l.dimensions ->> 'mx_adset_id'           AS mx_adset_id,
    l.dimensions ->> 'mx_gclid'              AS mx_gclid,
    m.matched_platform,
    m.matched_level,
    m.matched_external_id,
    m.matched_name,
    m.match_priority
FROM leadsquared_leads l
LEFT JOIN LATERAL (
    SELECT ae.provider AS matched_platform, ae.level AS matched_level,
           ae.external_id AS matched_external_id, ae.name AS matched_name,
           cand.priority AS match_priority
    FROM (VALUES
        ('meta',   'ad',       l.dimensions ->> 'mx_ad_id',              1),
        ('meta',   'adset',    l.dimensions ->> 'mx_adset_id',           2),
        ('meta',   'campaign', l.dimensions ->> 'mx_source_campaign_id', 3),
        ('google', 'campaign', l.dimensions ->> 'mx_source_campaign_id', 4),
        ('google', 'ad_group', l.dimensions ->> 'mx_adset_id',           5)
    ) AS cand(platform, level, ext_id, priority)
    JOIN ad_entities ae
        ON ae.organization_id = l.organization_id
       AND ae.provider = cand.platform
       AND ae.level = cand.level
       AND ae.external_id = cand.ext_id
    WHERE
        (cand.platform = 'meta' AND cand.ext_id ~ '^[0-9]{15,18}$')
        OR (cand.platform = 'google' AND cand.ext_id ~ '^[0-9]{9,12}$')
    ORDER BY cand.priority
    LIMIT 1
) m ON true
"""

_LIFECYCLE_VIEW = """
CREATE OR REPLACE VIEW v_leadsquared_booking_lifecycle AS
WITH booking_created AS (
    SELECT
        a.connection_id, a.organization_id, a.related_prospect_id, a.booking_id,
        a.prospect_activity_id AS booking_created_activity_id,
        a.date AS booking_created_date,
        a.dimensions ->> 'total_paid_amount' AS total_paid_amount,
        a.dimensions ->> 'payment_status'    AS payment_status,
        a.dimensions ->> 'payment_method'    AS payment_method
    FROM leadsquared_activities a
    WHERE a.stream = 'booking_created' AND a.booking_id IS NOT NULL
),
latest_status AS (
    SELECT DISTINCT ON (a.connection_id, a.booking_id)
        a.connection_id, a.booking_id,
        a.dimensions ->> 'booking_status' AS latest_booking_status,
        a.date AS latest_status_date,
        a.dimensions ->> 'actual_amount'      AS actual_amount,
        a.dimensions ->> 'booking_amount'     AS booking_amount,
        a.dimensions ->> 'curelo_commission'  AS curelo_commission,
        a.prospect_activity_id AS latest_status_activity_id
    FROM leadsquared_activities a
    WHERE a.stream = 'post_booking_order_status' AND a.booking_id IS NOT NULL
    ORDER BY a.connection_id, a.booking_id, a.date DESC NULLS LAST, a.ingested_at DESC
),
status_count AS (
    SELECT connection_id, booking_id, count(*) AS post_booking_status_row_count
    FROM leadsquared_activities
    WHERE stream = 'post_booking_order_status' AND booking_id IS NOT NULL
    GROUP BY connection_id, booking_id
),
cancellation AS (
    SELECT
        a.connection_id, a.booking_id,
        a.dimensions ->> 'cancelled_amount' AS cancelled_amount,
        a.dimensions ->> 'activity_event_note' AS cancellation_note,
        a.date AS cancelled_date,
        a.prospect_activity_id AS cancellation_activity_id
    FROM leadsquared_activities a
    WHERE a.stream = 'booking_cancelled' AND a.booking_id IS NOT NULL
)
SELECT
    bc.connection_id,
    bc.organization_id,
    bc.related_prospect_id,
    bc.booking_id,
    bc.booking_created_activity_id,
    bc.booking_created_date,
    bc.total_paid_amount,
    bc.payment_status,
    bc.payment_method,
    ls.latest_booking_status,
    ls.latest_status_date,
    ls.actual_amount,
    ls.booking_amount,
    ls.curelo_commission,
    COALESCE(sc.post_booking_status_row_count, 0) AS post_booking_status_row_count,
    (cx.booking_id IS NOT NULL) AS is_cancelled,
    cx.cancelled_amount,
    cx.cancellation_note,
    cx.cancelled_date
FROM booking_created bc
LEFT JOIN latest_status ls ON ls.connection_id = bc.connection_id AND ls.booking_id = bc.booking_id
LEFT JOIN status_count  sc ON sc.connection_id = bc.connection_id AND sc.booking_id = bc.booking_id
LEFT JOIN cancellation  cx ON cx.connection_id = bc.connection_id AND cx.booking_id = bc.booking_id
"""


def upgrade() -> None:
    op.execute(_ATTRIBUTION_VIEW)
    op.execute(_LIFECYCLE_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_leadsquared_booking_lifecycle")
    op.execute("DROP VIEW IF EXISTS v_leadsquared_lead_attribution")
