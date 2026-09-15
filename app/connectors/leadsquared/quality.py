"""LeadSquared data-quality checks (implementation instruction §17).

Every check here is observability, not a sync gate — consistent with the rest
of this platform's "schema drift never aborts a sync" philosophy and with
instruction §15 ("missing 208 history is a valid state, not an ingestion
error"). Nothing here blocks or fails a run; each function returns a
structured finding for a caller (a future MCP tool, a dashboard, or a human)
to act on.

Deliberately dialect-portable (no raw `->>` JSONB SQL, no Postgres regex
operators): `dimensions` is treated as an opaque-to-SQL JSON blob everywhere
else in this codebase, and this module follows that same convention so it
runs against the SQLite test database exactly like production Postgres.
Checks that would otherwise need a full-table JSON scan are bounded by the
real, typed, indexed `date` column instead (`since`, default 90 days) — on
Postgres this is a normal indexed range scan; unbounded JSON-content
filtering would not be.

`KNOWN_SOURCES` is the closed set of `Source` dropdown values actually
observed live against the verified account (docs/coverage/LSQ_DISCOVERY_2026-09-11.md
Phase 2) — not the full universe LeadSquared could ever emit. A new value
showing up is exactly the "unknown Source" case this module is meant to
surface, not silently swallow; the set is refreshed by hand as new sources
appear, per the discovery report's own recommendation (§2's classification
allow-list, not a heuristic).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import LeadsquaredActivity, LeadsquaredLead

# Confirmed live, 30-day sample, docs/coverage/LSQ_DISCOVERY_2026-09-11.md Phase 2.
# Not a business classification (google/meta/organic/etc — that's a separate,
# deliberately-not-hardcoded decision per the discovery report) — just "have
# we seen this Source value before," so a genuinely new one gets surfaced
# rather than silently bucketed.
KNOWN_SOURCES: frozenset[str | None] = frozenset(
    {
        "Cold Calling",
        "Google_PMax",
        "Google-imaging-lp",
        "Manage_Portal",
        "App_Organic",
        "Meta_Form",
        "Inbound Phone call",
        "google_lp",
        "Website_Organic",
        "Outbound Phone call",
        "cfbc",
        "Google_RDX_Call",
        "Website_Call",
        "Lifecycle P1 - WA",
        "Google_lp",
        "WA Repeat",
        "Website_WA",
        "Meta_Call",
        "Web_PDP",
        "DSA_Meta",
        "meta_lp",
        "App_Call",
        "Google_Call",
        "Meta_WA",
        "Camp_Meta",
        "Direct Traffic",
        "App_WA",
        "GMB_Gurgaon_WA",
        "Lifecycle - WA",
        "Meta_Outbound",
        "GMB_Gurgaon_Call",
        "Lifecycle - SMS_Call",
        "Google_WA",
        "Web_Blog",
        "CleverTap",
        "Google_Outbound",
        "Meta_Social",
        "Google_Call_Guj",
        "google-lp",
        "Verification",
        "Lifecycle - RCS_Call",
        "Google_RCS",
        "Social Media",
        "Smart_Reports_WA",
        "Web_organic",
        "Meta_lp",
        "MDS_Media",
        "Affiliate_Call",
        "Google_DG",
        "AI Call",
        "Lifecycle - WA_Call",
        "Organic Search",
        "Google P1 - WA",
        "FMS Media",
        "365 Digital",
        "Affiliate P1_WA",
        "Referral Sites",
        "Affiliate_WA",
        "Affiliate_Form",
        "Manual Lead Create",
        "JustDial",
        "Support",
        "GMB_Vadodara_Call",
        "Inbound Email",
        "Pay per Click Ads",
        "test lead",
        None,  # LSQ returns an unset Source as null on a small fraction of leads — not "unknown", just absent.
    }
)

# Google IDs run 9-12 digits, Meta IDs 17-18 — a clean gap with zero real IDs
# observed in between (confirmed against this warehouse's own ad_entities,
# docs/coverage/LSQ_VERIFICATION_2026-09-11.md §E). Same pattern the
# attribution view uses.
_GOOGLE_ID_RE = re.compile(r"^[0-9]{9,12}$")
_META_ID_RE = re.compile(r"^[0-9]{15,18}$")

_DEFAULT_WINDOW_DAYS = 90


@dataclass(slots=True)
class DQFinding:
    check: str
    count: int
    sample: list[dict[str, Any]] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return self.count == 0


def _since(since: date | None) -> date:
    return since or (datetime.now(UTC).date() - timedelta(days=_DEFAULT_WINDOW_DAYS))


async def _recent_leads(session: AsyncSession, connection_id: int, since: date) -> list[LeadsquaredLead]:
    rows = (
        (
            await session.execute(
                select(LeadsquaredLead).where(
                    LeadsquaredLead.connection_id == connection_id, LeadsquaredLead.date >= since
                )
            )
        )
        .scalars()
        .all()
    )
    return list(rows)


async def unknown_sources(
    session: AsyncSession, connection_id: int, *, since: date | None = None, sample_size: int = 20
) -> DQFinding:
    """Leads whose `Source` is not in `KNOWN_SOURCES` — a real, live-observed
    failure mode (LSQ's own dropdown gained values between sessions), and the
    discovery report's own recommendation was to flag these rather than
    silently bucket them as "Other". `source` is a real typed column, so this
    one check does not need the `since` scan-bound the JSON-backed checks do.

    A null `Source` is itself a known, expected state (LSQ leaves it unset on
    a small fraction of leads) — excluded via `.isnot(None)` rather than left
    in the SQL `NOT IN (...)` list, since a NULL there would make the whole
    comparison evaluate to UNKNOWN for every row under standard SQL
    three-valued logic and silently flag nothing at all.
    """
    known_non_null = {s for s in KNOWN_SOURCES if s is not None}
    stmt = select(LeadsquaredLead.prospect_id, LeadsquaredLead.source).where(
        LeadsquaredLead.connection_id == connection_id,
        LeadsquaredLead.source.isnot(None),
        LeadsquaredLead.source.notin_(known_non_null),
    )
    rows = (await session.execute(stmt.limit(sample_size))).all()
    count = (await session.execute(select(func.count()).select_from(stmt.subquery()))).scalar_one()
    return DQFinding(
        "unknown_sources", count, [{"prospect_id": r.prospect_id, "source": r.source} for r in rows]
    )


async def id_format_mismatches(
    session: AsyncSession, connection_id: int, *, since: date | None = None, sample_size: int = 20
) -> DQFinding:
    """A lead whose `mx_Source_Campaign_ID` doesn't match *either* platform's
    known ID-length signature — i.e. not obviously Google-, not obviously
    Meta-shaped. Distinct from cross-platform *contamination* (a
    Google-sourced lead carrying a Meta-shaped id, which the attribution
    view's format check already filters out safely); this is for values that
    fit neither pattern at all, usually a malformed/truncated id."""
    leads = await _recent_leads(session, connection_id, _since(since))
    hits = []
    for lead in leads:
        campaign_id = (lead.dimensions or {}).get("mx_source_campaign_id")
        if not campaign_id:
            continue
        if not (_GOOGLE_ID_RE.match(str(campaign_id)) or _META_ID_RE.match(str(campaign_id))):
            hits.append({"prospect_id": lead.prospect_id, "source": lead.source, "campaign_id": campaign_id})
    return DQFinding("id_format_mismatches", len(hits), hits[:sample_size])


async def attribution_orphans(
    session: AsyncSession, connection_id: int, *, since: date | None = None, sample_size: int = 20
) -> DQFinding:
    """Leads that carry a GCLID or a campaign/adset/ad id but resolve to no
    row in `ad_entities` at all for this org — i.e. genuinely unattributable
    paid leads, per the discovery report's Phase-12 finding that ~60% of
    GCLID-bearing leads have no campaign id. Surfaced, not dropped."""
    from app.models import AdEntity

    leads = await _recent_leads(session, connection_id, _since(since))
    if not leads:
        return DQFinding("attribution_orphans", 0, [])
    org_id = leads[0].organization_id
    # `ad_entities` is small relative to a large lead volume (tens of
    # thousands vs. hundreds of thousands) — loading every external_id for
    # the org and intersecting in Python avoids an `external_id IN (...)`
    # clause sized by the candidate-id count, which at full backfill scale
    # (a lead volume in the hundreds of thousands) exceeds asyncpg's 32,767
    # bound-parameter limit.
    known_entity_ids: set[str] = set(
        (await session.execute(select(AdEntity.external_id).where(AdEntity.organization_id == org_id)))
        .scalars()
        .all()
    )

    hits = []
    for lead in leads:
        d = lead.dimensions or {}
        has_signal = d.get("mx_gclid") or d.get("mx_source_campaign_id")
        if not has_signal:
            continue
        ids = {d.get("mx_source_campaign_id"), d.get("mx_adset_id"), d.get("mx_ad_id")} - {None}
        if not (ids & known_entity_ids):
            hits.append(
                {
                    "prospect_id": lead.prospect_id,
                    "source": lead.source,
                    "gclid": d.get("mx_gclid"),
                    "campaign_id": d.get("mx_source_campaign_id"),
                }
            )
    return DQFinding("attribution_orphans", len(hits), hits[:sample_size])


async def missing_campaign_or_ad_identifiers(
    session: AsyncSession, connection_id: int, *, since: date | None = None, sample_size: int = 20
) -> DQFinding:
    """A paid-looking source (has a GCLID or a UTM source) with neither a
    campaign id nor an adset id at all — the discovery report's Phase-12
    campaign-name-without-id / gclid-without-campaign-id pattern."""
    leads = await _recent_leads(session, connection_id, _since(since))
    hits = []
    for lead in leads:
        d = lead.dimensions or {}
        paid_signal = d.get("mx_gclid") or d.get("mx_utm_source")
        if not paid_signal:
            continue
        if not d.get("mx_source_campaign_id") and not d.get("mx_adset_id"):
            hits.append(
                {"prospect_id": lead.prospect_id, "source": lead.source, "utm_source": d.get("mx_utm_source")}
            )
    return DQFinding("missing_campaign_or_ad_identifiers", len(hits), hits[:sample_size])


async def duplicate_prospect_ids(session: AsyncSession, connection_id: int) -> DQFinding:
    """Structurally prevented by `uq_leadsquared_leads_conn_prospect` — this
    check exists as a defensive confirmation, not because a duplicate can
    reach the table. A non-zero count here means the DB constraint itself was
    bypassed (e.g. a raw insert outside the writer), which is worth knowing."""
    rows = (
        await session.execute(
            select(LeadsquaredLead.prospect_id, func.count().label("n"))
            .where(LeadsquaredLead.connection_id == connection_id)
            .group_by(LeadsquaredLead.prospect_id)
            .having(func.count() > 1)
        )
    ).all()
    return DQFinding(
        "duplicate_prospect_ids", len(rows), [{"prospect_id": r.prospect_id, "count": r.n} for r in rows]
    )


async def duplicate_prospect_activity_ids(session: AsyncSession, connection_id: int) -> DQFinding:
    """Same defensive posture as `duplicate_prospect_ids` — a
    `ProspectActivityId` appearing more than once within the same stream
    would mean the upsert key collided."""
    rows = (
        await session.execute(
            select(
                LeadsquaredActivity.stream, LeadsquaredActivity.prospect_activity_id, func.count().label("n")
            )
            .where(LeadsquaredActivity.connection_id == connection_id)
            .group_by(LeadsquaredActivity.stream, LeadsquaredActivity.prospect_activity_id)
            .having(func.count() > 1)
        )
    ).all()
    return DQFinding(
        "duplicate_prospect_activity_ids",
        len(rows),
        [{"stream": r.stream, "prospect_activity_id": r.prospect_activity_id, "count": r.n} for r in rows],
    )


async def booking_id_inconsistencies(
    session: AsyncSession, connection_id: int, *, since: date | None = None, sample_size: int = 20
) -> DQFinding:
    """A `booking_id` that appears on a status/cancellation activity (208 or
    223) but never on a `booking_created` (206) activity for the same
    connection — the funnel's entry point missing for a booking the rest of
    the pipeline references. Per instruction §15, a missing *208* row for a
    given booking is normal; a booking_id with *no 206 at all* is a genuine
    inconsistency worth surfacing."""
    window_start = _since(since)
    downstream = (
        await session.execute(
            select(LeadsquaredActivity.stream, LeadsquaredActivity.booking_id)
            .where(
                LeadsquaredActivity.connection_id == connection_id,
                LeadsquaredActivity.stream.in_(("post_booking_order_status", "booking_cancelled")),
                LeadsquaredActivity.booking_id.isnot(None),
                LeadsquaredActivity.date >= window_start,
            )
            .distinct()
        )
    ).all()
    if not downstream:
        return DQFinding("booking_id_inconsistencies", 0, [])

    # Load every 206 booking_id in the same window rather than filtering by
    # `.in_(booking_ids)` — at full-backfill scale the downstream distinct
    # booking-id count can exceed asyncpg's 32,767 bound-parameter limit;
    # a plain set-difference in Python has no such ceiling.
    created = (
        (
            await session.execute(
                select(LeadsquaredActivity.booking_id).where(
                    LeadsquaredActivity.connection_id == connection_id,
                    LeadsquaredActivity.stream == "booking_created",
                    LeadsquaredActivity.booking_id.isnot(None),
                    LeadsquaredActivity.date >= window_start,
                )
            )
        )
        .scalars()
        .all()
    )
    have_206 = set(created)
    hits = [
        {"booking_id": r.booking_id, "stream": r.stream} for r in downstream if r.booking_id not in have_206
    ]
    return DQFinding("booking_id_inconsistencies", len(hits), hits[:sample_size])


CHECKS = (
    unknown_sources,
    id_format_mismatches,
    attribution_orphans,
    missing_campaign_or_ad_identifiers,
    duplicate_prospect_ids,
    duplicate_prospect_activity_ids,
    booking_id_inconsistencies,
)


async def run_all(session: AsyncSession, connection_id: int) -> list[DQFinding]:
    return [await check(session, connection_id) for check in CHECKS]


__all__ = [
    "CHECKS",
    "KNOWN_SOURCES",
    "DQFinding",
    "attribution_orphans",
    "booking_id_inconsistencies",
    "duplicate_prospect_activity_ids",
    "duplicate_prospect_ids",
    "id_format_mismatches",
    "missing_campaign_or_ad_identifiers",
    "run_all",
    "unknown_sources",
]
