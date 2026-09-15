"""LeadSquared connector — leads + booking/order-status/cancellation activity.

Design baseline: docs/coverage/LSQ_VERIFICATION_2026-09-11.md (Phase 14 /
§A-§F). The short version of what shaped the choices below, all live-verified
that session, not assumed:

  cursor          `Leads.RecentlyModified`'s FromDate/ToDate filters
                  `ModifiedOn`, not `CreatedOn` — confirmed by construing a
                  window around one lead's own CreatedOn (0 results) vs its
                  ModifiedOn (a match). `RetrieveByActivityEvent` filters
                  `CreatedOn` (activities are effectively immutable once
                  created, so that is also the semantically right cursor).
  page limits     Leads.RecentlyModified: 5000/page. RetrieveByActivityEvent:
                  1000/page (a 2000 request returned a live
                  MXInvalidInputException naming the 1000 cap).
  timestamps      UTC, confirmed by comparing the most-recently-modified
                  leads against true-UTC-now vs a falsely-IST-shifted "now".
  booking id      sits at a *different* mx_Custom_N slot per event type
                  (206→mx_Custom_2, 208→mx_Custom_4, 223→mx_Custom_3),
                  confirmed via `GetActivitySetting`, not sampled — resolved
                  once here at ingestion, not left for every downstream query
                  to get wrong.
  Opportunities   deliberately NOT a stream — a live "Won" opportunity's
                  amount/booking-id/customer-id were byte-for-byte identical
                  to the same booking's 206 record; it is a mirror, not an
                  independent revenue source (§A).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from typing import Any

from app.connectors import errors as E
from app.connectors.base import (
    HealthReport,
    HealthStatus,
    Record,
    ResourceDescriptor,
    StreamDefinition,
    StreamSlice,
)
from app.connectors.leadsquared.base import LeadSquaredConnector, health_from_error
from app.connectors.registry import RegistryEntry, registry
from app.connectors.validation import build_json_schema, clean_dimension

LEADS_STREAM = "leads"

# stream name -> LeadSquared ActivityEvent code (docs/coverage/LSQ_DISCOVERY_2026-09-11.md
# Phase 5, docs/coverage/LSQ_VERIFICATION_2026-09-11.md §B/§C). 225 (Payment
# Success) is deliberately excluded — the verification pass found it covers
# under 1% of bookings in this account and is not the funnel's real payment
# signal (§6/§C of the verification report).
ACTIVITY_EVENTS: dict[str, int] = {
    "booking_created": 206,
    "post_booking_order_status": 208,
    "booking_cancelled": 223,
    "facebook_lead_ads_submissions": 204,
}

# The fields worth resolving to a named key at ingestion time rather than
# query time — confirmed live via `GetActivitySetting` (implementation
# instruction §5/§14), NOT inferred from a sample payload (an early sample
# read of 206's mx_Custom_8 looked like a patient-type value; the metadata
# call proved it is actually "Booking Resource Type" — exactly the trap
# relying on samples would have walked into). `booking_id` is present in
# every mapping below except `facebook_lead_ads_submissions`, which fires
# before any booking exists. Every other mx_Custom_N field for these events
# stays in `raw` only — deliberately, per LSQ_VERIFICATION_2026-09-11.md's
# "keep JSONB, semantics vary by event" conclusion; only the fields the
# implementation instructions explicitly called out to preserve (§14) get a
# named column here.
_ACTIVITY_FIELD_MAP: dict[str, dict[str, str]] = {
    "booking_created": {
        "mx_Custom_2": "booking_id",
        "mx_Custom_6": "total_paid_amount",
        "mx_Custom_15": "payment_status",
        "mx_Custom_16": "payment_method",
    },
    "post_booking_order_status": {
        "mx_Custom_4": "booking_id",
        "mx_Custom_6": "booking_status",
        "mx_Custom_11": "patient_type",
        "mx_Custom_15": "actual_amount",
        "mx_Custom_20": "booking_amount",
        "mx_Custom_22": "curelo_commission",
    },
    "booking_cancelled": {
        "mx_Custom_3": "booking_id",
        "mx_Custom_5": "cancelled_amount",
    },
    "facebook_lead_ads_submissions": {},
}

LEAD_COLUMNS = [
    "ProspectID",
    "Phone",
    "EmailAddress",
    "Source",
    "SourceCampaign",
    "mx_Source_Campaign_ID",
    "mx_GCLid",
    "mx_Ad_Id",
    "mx_Ad_Name",
    "mx_Adset_Id",
    "mx_Adset_Name",
    "mx_utm_source",
    "mx_utm_medium",
    "mx_utm_term",
    "mx_utm_keyword",
    "mx_utm_keyword_id",
    "mx_Latest_Source",
    "mx_Lead_Type",
    "mx_Product_Service_Interest",
    "mx_Slug",
    "mx_google_location_id",
    "mx_Source_Referral_URL",
    "CreatedOn",
    "ModifiedOn",
]

_LEAD_DIMENSIONS = [
    "prospect_id",
    "source",
    "source_campaign",
    "mx_source_campaign_id",
    "mx_gclid",
    "mx_ad_id",
    "mx_ad_name",
    "mx_adset_id",
    "mx_adset_name",
    "mx_utm_source",
    "mx_utm_medium",
    "mx_utm_term",
    "mx_utm_keyword",
    "mx_utm_keyword_id",
    "mx_latest_source",
    "mx_lead_type",
    "mx_product_service_interest",
    "mx_slug",
    "mx_google_location_id",
    "mx_source_referral_url",
    "phone",
    "email",
    "created_on",
    "modified_on",
]
_ACTIVITY_DIMENSIONS = [
    "prospect_activity_id",
    "related_prospect_id",
    "booking_id",
    "activity_event",
    "activity_event_note",
    "status",
    "created_on",
    "modified_on",
    # Named per-event fields (§14) — populated only on the event type(s) they
    # actually belong to; see _ACTIVITY_FIELD_MAP. Listed together here since
    # every activity stream shares one JSON-schema declaration.
    "total_paid_amount",
    "payment_status",
    "payment_method",
    "booking_status",
    "patient_type",
    "actual_amount",
    "booking_amount",
    "curelo_commission",
    "cancelled_amount",
]


def _parse_lsq_dt(value: Any) -> datetime | None:
    """LeadSquared timestamps are `YYYY-MM-DD HH:MM:SS[.fff]`, confirmed live
    to be UTC (docs/coverage/LSQ_VERIFICATION_2026-09-11.md §11)."""
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:26], fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _streams() -> list[StreamDefinition]:
    out = [
        StreamDefinition(
            name=LEADS_STREAM,
            description="LeadSquared leads (prospects) — incremental on ModifiedOn.",
            json_schema=build_json_schema(_LEAD_DIMENSIONS, []),
            primary_key=["prospect_id"],
            grain="fact",
            slice_days=14,
            default_cursor_field="ModifiedOn",
            spec={},
        )
    ]
    descriptions = {
        "booking_created": (
            "Booking Created (event 206) — the primary commitment signal. NOT yet the "
            "authoritative conversion definition; see LSQ_VERIFICATION_2026-09-11.md §6/§9."
        ),
        "post_booking_order_status": (
            "Post Booking Order Status (event 208) — 0..N rows per booking as it moves "
            "through Booking Status pending -> confirmed/customer_confirmed -> completed. "
            "Most bookings never reach a terminal status within any given sync window; "
            "absence of a row here is a normal, expected state, not missing data."
        ),
        "booking_cancelled": "Booking Cancelled (event 223).",
        "facebook_lead_ads_submissions": (
            "Facebook Lead Ads Submissions (event 204) — a corroborating Meta attribution "
            "source captured at submission time, independent of the lead's own mx_* fields."
        ),
    }
    for name, event_code in ACTIVITY_EVENTS.items():
        out.append(
            StreamDefinition(
                name=name,
                description=descriptions[name],
                json_schema=build_json_schema(_ACTIVITY_DIMENSIONS, []),
                primary_key=["prospect_activity_id"],
                grain="fact",
                # 208 is by far the highest-volume stream (~5k/day in the
                # account this was verified against) — a shorter window keeps
                # each page count (1000/page, confirmed live cap) reasonable.
                slice_days=3 if name == "post_booking_order_status" else 7,
                default_cursor_field="CreatedOn",
                spec={"activity_event": event_code},
            )
        )
    return out


class LeadSquaredCRMConnector(LeadSquaredConnector):
    connector_id = "leadsquared"
    name = "LeadSquared"
    version = "1.0.0"
    documentation_url = "https://apidocs.leadsquared.com/"
    icon = "leadsquared"
    STREAMS = _streams()

    # --- CHECK -------------------------------------------------------------
    async def check_connection(self) -> HealthReport:
        try:
            payload = await self._get("/v2/LeadManagement.svc/LeadsMetaData.Get", {"excludeOptionSets": "1"})
        except E.ConnectorError as exc:
            return health_from_error(exc)
        if not isinstance(payload, list) or not payload:
            return HealthReport(
                status=HealthStatus.INVALID_CONFIGURATION,
                message="LeadSquared returned no lead fields — check the access key, secret key and host.",
            )
        return HealthReport(
            status=HealthStatus.HEALTHY,
            message=f"Connected to LeadSquared ({len(payload)} lead fields discovered).",
        )

    # --- DISCOVER ------------------------------------------------------
    async def discover_resources(self) -> list[ResourceDescriptor]:
        # LeadSquared has no multi-account/multi-property concept the way
        # GA4 or Google Ads does — one accessKey/secretKey pair is one
        # account. The metadata call proves reachability the same way
        # check_connection does; the descriptor itself is a fixed singleton.
        await self._get("/v2/LeadManagement.svc/LeadsMetaData.Get", {"excludeOptionSets": "1"})
        return [
            ResourceDescriptor(
                resource_id="account",
                name="LeadSquared Account",
                resource_type="account",
            )
        ]

    # --- READ ----------------------------------------------------------
    async def read_slice(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[Record]:
        if stream.name == LEADS_STREAM:
            async for rec in self._read_leads(slice_):
                yield rec
            return
        event_code = ACTIVITY_EVENTS.get(stream.name)
        if event_code is None:
            raise E.invalid_configuration(
                f"Unknown LeadSquared stream {stream.name!r}.",
                provider=self.provider,
                connector_id=self.connector_id,
            )
        async for rec in self._read_activities(stream, event_code, slice_):
            yield rec

    async def _read_leads(self, slice_: StreamSlice) -> AsyncIterator[Record]:
        start = slice_.start_date or date.today()
        end = slice_.end_date or start
        from_date = f"{start.isoformat()} 00:00:00"
        to_date = f"{end.isoformat()} 23:59:59"

        page = 1
        while True:
            body = {
                "Parameter": {"FromDate": from_date, "ToDate": to_date},
                "Columns": {"Include_CSV": ",".join(LEAD_COLUMNS)},
                # 5000/page, confirmed live as this endpoint's documented and
                # actual maximum (docs/coverage/LSQ_DISCOVERY_2026-09-11.md §10).
                "Paging": {"PageIndex": page, "PageSize": 5000},
                # Sorted by CreatedOn (not ModifiedOn) deliberately — CreatedOn
                # is the one column this session live-confirmed as a working
                # Sorting.ColumnName for this endpoint; sort order here only
                # affects pagination stability, not correctness, since every
                # page in the window is consumed regardless of order.
                "Sorting": {"ColumnName": "CreatedOn", "Direction": "1"},
            }
            self.ctx.progress.note(f"{LEADS_STREAM}: page {page} ({from_date}..{to_date})")
            payload = await self._post("/v2/LeadManagement.svc/Leads.RecentlyModified", body)
            leads = payload.get("Leads") or []
            for entry in leads:
                record = self._to_lead_record(entry)
                if record is not None:
                    yield record
            if len(leads) < 5000:
                break
            page += 1

    def _to_lead_record(self, entry: dict[str, Any]) -> Record | None:
        d = {p.get("Attribute"): p.get("Value") for p in entry.get("LeadPropertyList", [])}
        prospect_id = d.get("ProspectID")
        if not prospect_id:
            return None
        modified_on = _parse_lsq_dt(d.get("ModifiedOn"))
        created_on = _parse_lsq_dt(d.get("CreatedOn"))
        row_date = (modified_on or created_on or datetime.now(UTC)).date()
        dimensions = {
            "prospect_id": prospect_id,
            "source": clean_dimension(d.get("Source")),
            "source_campaign": d.get("SourceCampaign"),
            "mx_source_campaign_id": d.get("mx_Source_Campaign_ID"),
            "mx_gclid": d.get("mx_GCLid"),
            "mx_ad_id": d.get("mx_Ad_Id"),
            "mx_ad_name": d.get("mx_Ad_Name"),
            "mx_adset_id": d.get("mx_Adset_Id"),
            "mx_adset_name": d.get("mx_Adset_Name"),
            "mx_utm_source": d.get("mx_utm_source"),
            "mx_utm_medium": d.get("mx_utm_medium"),
            "mx_utm_term": d.get("mx_utm_term"),
            "mx_utm_keyword": d.get("mx_utm_keyword"),
            "mx_utm_keyword_id": d.get("mx_utm_keyword_id"),
            "mx_latest_source": d.get("mx_Latest_Source"),
            "mx_lead_type": d.get("mx_Lead_Type"),
            "mx_product_service_interest": d.get("mx_Product_Service_Interest"),
            "mx_slug": d.get("mx_Slug"),
            "mx_google_location_id": d.get("mx_google_location_id"),
            "mx_source_referral_url": d.get("mx_Source_Referral_URL"),
            "phone": d.get("Phone"),
            "email": d.get("EmailAddress"),
            "created_on": d.get("CreatedOn"),
            "modified_on": d.get("ModifiedOn"),
        }
        return Record(
            stream=LEADS_STREAM,
            key_values={"prospect_id": prospect_id},
            date=row_date,
            dimensions=dimensions,
            metrics={},
            raw=d,
            cursor_value=d.get("ModifiedOn"),
        )

    async def _read_activities(
        self, stream: StreamDefinition, event_code: int, slice_: StreamSlice
    ) -> AsyncIterator[Record]:
        start = slice_.start_date or date.today()
        end = slice_.end_date or start
        from_date = f"{start.isoformat()} 00:00:00"
        to_date = f"{end.isoformat()} 23:59:59"
        field_map = _ACTIVITY_FIELD_MAP.get(stream.name, {})

        page = 1
        while True:
            body = {
                "Parameter": {"FromDate": from_date, "ToDate": to_date, "ActivityEvent": event_code},
                # 1000/page — this endpoint's confirmed hard cap; 2000 returned
                # a live MXInvalidInputException naming it explicitly.
                "Paging": {"PageIndex": page, "PageSize": 1000},
                "Sorting": {"ColumnName": "CreatedOn", "Direction": "1"},
            }
            self.ctx.progress.note(f"{stream.name}: page {page} ({from_date}..{to_date})")
            payload = await self._post(
                "/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent", body
            )
            rows = payload.get("List") or []
            for row in rows:
                record = self._to_activity_record(stream, row, field_map)
                if record is not None:
                    yield record
            if len(rows) < 1000:
                break
            page += 1

    def _to_activity_record(
        self, stream: StreamDefinition, row: dict[str, Any], field_map: dict[str, str]
    ) -> Record | None:
        activity_id = row.get("ProspectActivityId")
        if not activity_id:
            return None
        created_on = _parse_lsq_dt(row.get("CreatedOn"))
        row_date = (created_on or datetime.now(UTC)).date()
        dimensions = {
            "prospect_activity_id": activity_id,
            "related_prospect_id": row.get("RelatedProspectId"),
            "activity_event": row.get("ActivityEvent"),
            "activity_event_note": row.get("ActivityEvent_Note"),
            "status": row.get("Status"),
            "created_on": row.get("CreatedOn"),
            "modified_on": row.get("ModifiedOn"),
        }
        # Named per-event fields (§14), resolved from the metadata-confirmed
        # slot for *this* event type — see _ACTIVITY_FIELD_MAP's docstring.
        for slot, named in field_map.items():
            dimensions[named] = row.get(slot)
        dimensions.setdefault("booking_id", None)
        return Record(
            stream=stream.name,
            key_values={"prospect_activity_id": activity_id},
            date=row_date,
            dimensions=dimensions,
            metrics={},
            raw=row,
            cursor_value=row.get("CreatedOn"),
        )


registry.register(
    RegistryEntry(
        connector_class=LeadSquaredCRMConnector,
        requires_settings=("leadsquared_access_key", "leadsquared_secret_key", "leadsquared_host"),
        prerequisites=(
            "A LeadSquared accessKey/secretKey pair with API access.",
            "The account's regional API host (e.g. api-in21.leadsquared.com) — LeadSquared "
            "shards accounts by region and there is no single global host.",
        ),
        caveats=(
            "Revenue/conversion semantics are intentionally not modelled yet. See "
            "docs/coverage/LSQ_VERIFICATION_2026-09-11.md §9 for the business decisions "
            "(authoritative revenue field, Payment Status meaning, repeat-customer policy) "
            "needed before any revenue/ROAS reporting is built on this connector's data.",
        ),
        resource_label="LeadSquared Account",
        tags=("crm", "leadsquared", "attribution"),
    )
)


__all__ = ["ACTIVITY_EVENTS", "LEADS_STREAM", "LeadSquaredCRMConnector"]
