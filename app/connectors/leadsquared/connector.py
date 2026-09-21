"""LeadSquared connector — complete lead payload + every activity type.

Design baseline: docs/coverage/LSQ_VERIFICATION_2026-09-11.md, corrected by the live
extraction audit of 2026-09-19 (memory: lsq-extraction-audit-2026-09-19). What the
API was verified to do, and what the connector does about it:

  filter columns   `Leads.RecentlyModified` filters on `LeadLastModifiedOn` (moves on
                   any new activity — NOT the `ModifiedOn` attribute);
                   `RetrieveByActivityEvent` filters on `ModifiedOn` (NOT CreatedOn).
                   Both are therefore *modification* cursors, which is what an
                   incremental sync wants — and why activity edits are picked up.
  paging           No page token exists and CreatedOn ordering is non-deterministic
                   (rows were silently lost). See window.py: windows are sized to
                   fit one page, verified against `RecordCount`, and ordered by the
                   record's unique id in the one case that must page.
  page limits      Leads.RecentlyModified 5000/page (default here 2000 — a full-payload
                   lead is ~3 KB, so 5000 would be ~15 MB per response);
                   RetrieveByActivityEvent 1000/page.
  timestamps       UTC. Stored with SUB-SECOND precision (printed `.000`); From/ToDate are
                   parsed as whole seconds, so `ToDate=04` means "up to 04.000". Adjacent
                   windows [00..04] + [05..09] therefore miss the crack (04.000, 05.000) —
                   live: 10 leads in [00..09], 9 in the halves. Windows are closed intervals
                   that SHARE their boundary second (see window.py).
  lead payload     206 attributes per lead. `Columns` is NOT sent, so every attribute
                   the API returns is preserved in `raw`; the ~24 attribution fields
                   the analytics views use are also normalised into `dimensions`.
  activity types   84 types; the API needs an explicit `ActivityEvent` (no "all
                   types" call), so each type is its own stream. All land in one raw
                   table, discriminated by `activity_event`; the full row (every
                   mx_Custom_N slot) is kept verbatim in `raw`.
  booking id       sits at a different mx_Custom_N slot per event type
                   (206->2, 208->4, 223->3), confirmed via GetActivitySetting.
  Opportunities    deliberately NOT a stream — a mirror of the 206 booking record.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from app.connectors import errors as E
from app.connectors.base import (
    HealthReport,
    HealthStatus,
    Record,
    ResourceDescriptor,
    StreamDefinition,
    StreamSlice,
    WindowBatch,
)
from app.connectors.leadsquared.activity_catalog import (
    ACTIVITY_TYPES,
    event_for_stream_name,
    stream_name_for_event,
)
from app.connectors.leadsquared.base import LeadSquaredConnector, health_from_error
from app.connectors.leadsquared.window import WINDOW_FMT, Page, WindowFetcher
from app.connectors.registry import RegistryEntry, registry
from app.connectors.validation import build_json_schema, clean_dimension

LEADS_STREAM = "leads"

LEADS_PATH = "/v2/LeadManagement.svc/Leads.RecentlyModified"
ACTIVITY_PATH = "/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent"
ACTIVITY_TYPES_PATH = "/v2/ProspectActivity.svc/ActivityTypes.Get"

# Documented API caps (live-verified: exceeding either is an MXInvalidInputException).
LEAD_PAGE_CAP = 5000
ACTIVITY_PAGE_CAP = 1000
DEFAULT_LEAD_PAGE_SIZE = 2000

# The four activity types the analytics layer builds on, under their historical
# stream names (kept: record_key hashes the stream name).
ACTIVITY_EVENTS: dict[str, int] = {
    "booking_created": 206,
    "post_booking_order_status": 208,
    "booking_cancelled": 223,
    "facebook_lead_ads_submissions": 204,
}

# EVERY activity type the account exposes, stream name -> event code. Raw ingestion
# never silently discards a source activity type; which types feed governed
# analytics is a separate, downstream choice. (Payment Success 225 was excluded from
# analytics because it covers <1% of bookings — it is now still *extracted* here.)
ALL_ACTIVITY_EVENTS: dict[str, int] = {stream_name_for_event(c): c for c in ACTIVITY_TYPES}

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


def _activity_description(name: str, code: int) -> str:
    legacy = {
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
    if name in legacy:
        return legacy[name]
    return (
        f"{ACTIVITY_TYPES.get(code, 'Unknown activity type')} (activity event {code}) — raw activity "
        "rows; the complete source payload is preserved and the type is kept in `activity_event`."
    )


def _activity_stream(name: str, code: int) -> StreamDefinition:
    return StreamDefinition(
        name=name,
        description=_activity_description(name, code),
        json_schema=build_json_schema(_ACTIVITY_DIMENSIONS, []),
        primary_key=["prospect_activity_id"],
        grain="fact",
        # `RetrieveByActivityEvent` filters on ModifiedOn (live-verified).
        default_cursor_field="ModifiedOn",
        cursor_kind="timestamp",
        spec={"activity_event": code},
    )


def _streams() -> list[StreamDefinition]:
    out = [
        StreamDefinition(
            name=LEADS_STREAM,
            description=(
                "LeadSquared leads (prospects) — complete source payload. Incremental on "
                "LeadLastModifiedOn, the column Leads.RecentlyModified actually filters on."
            ),
            json_schema=build_json_schema(_LEAD_DIMENSIONS, []),
            primary_key=["prospect_id"],
            grain="fact",
            default_cursor_field="LeadLastModifiedOn",
            cursor_kind="timestamp",
            spec={},
        )
    ]
    out += [_activity_stream(name, code) for name, code in ALL_ACTIVITY_EVENTS.items()]
    return out


# Attributes LeadSquared appends to every lead in a RESPONSE that describe the query, not
# the lead. `Total` is the total of the window being read (live-verified: the same lead
# came back with 153, then 108), so storing it made every re-read look like a change and
# rewrote every unchanged lead on each overlap.
_ENVELOPE_ATTRIBUTES = frozenset({"Total"})


def _clip(value: Any, limit: int) -> str | None:
    """A text value sized for its column: a value that is too long must never fail the
    whole window's write. Blank means unset."""
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def lead_attributes(entry: dict[str, Any]) -> dict[str, Any]:
    """`LeadPropertyList` ([{Attribute, Value}, ...]) -> {Attribute: Value}."""
    return {
        p.get("Attribute"): p.get("Value") for p in entry.get("LeadPropertyList", []) if p.get("Attribute")
    }


def _start_of_day(d: date) -> datetime:
    return datetime.combine(d, time.min)


def _end_of_day(d: date) -> datetime:
    # The next midnight (closed interval): 23:59:59 would leave the last second's sub-second
    # rows (23:59:59.001-.999) in the crack between this day and the next.
    return datetime.combine(d + timedelta(days=1), time.min)


class _LeadSource:
    """PageSource for Leads.RecentlyModified. `Columns` is deliberately NOT sent so the
    API returns every attribute (206 in this account); ordered by the unique id."""

    def __init__(self, connector: LeadSquaredCRMConnector, page_size: int) -> None:
        self.connector = connector
        self.page_size = page_size

    async def fetch(self, start: datetime, end: datetime, page_index: int, page_size: int) -> Page:
        body = {
            "Parameter": {"FromDate": start.strftime(WINDOW_FMT), "ToDate": end.strftime(WINDOW_FMT)},
            "Paging": {"PageIndex": page_index, "PageSize": page_size},
            "Sorting": {"ColumnName": "ProspectID", "Direction": "1"},
        }
        self.connector.ctx.progress.note(
            f"{LEADS_STREAM}: {start:%Y-%m-%d %H:%M:%S}..{end:%Y-%m-%d %H:%M:%S} p{page_index}"
        )
        payload = await self.connector._post(LEADS_PATH, body)
        count, rows = self.connector._read_page(payload, "Leads")
        return Page(record_count=count, rows=[lead_attributes(e) for e in rows])

    def row_id(self, row: dict[str, Any]) -> str | None:
        return row.get("ProspectID") or None


class _ActivitySource:
    """PageSource for RetrieveByActivityEvent — one activity type per instance."""

    def __init__(self, connector: LeadSquaredCRMConnector, stream_name: str, code: int) -> None:
        self.connector = connector
        self.stream_name = stream_name
        self.code = code
        self.page_size = ACTIVITY_PAGE_CAP

    async def fetch(self, start: datetime, end: datetime, page_index: int, page_size: int) -> Page:
        body = {
            "Parameter": {
                "FromDate": start.strftime(WINDOW_FMT),
                "ToDate": end.strftime(WINDOW_FMT),
                "ActivityEvent": self.code,
            },
            "Paging": {"PageIndex": page_index, "PageSize": page_size},
            "Sorting": {"ColumnName": "ProspectActivityId", "Direction": "1"},
        }
        self.connector.ctx.progress.note(
            f"{self.stream_name}: {start:%Y-%m-%d %H:%M:%S}..{end:%Y-%m-%d %H:%M:%S} p{page_index}"
        )
        payload = await self.connector._post(ACTIVITY_PATH, body)
        count, rows = self.connector._read_page(payload, "List")
        return Page(record_count=count, rows=rows)

    def row_id(self, row: dict[str, Any]) -> str | None:
        return row.get("ProspectActivityId") or None


class LeadSquaredCRMConnector(LeadSquaredConnector):
    connector_id = "leadsquared"
    name = "LeadSquared"
    version = "2.0.0"
    documentation_url = "https://apidocs.leadsquared.com/"
    icon = "leadsquared"
    STREAMS = _streams()

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        cfg = ctx.config or {}
        self._lead_page_size = max(
            1, min(int(cfg.get("lead_page_size") or DEFAULT_LEAD_PAGE_SIZE), LEAD_PAGE_CAP)
        )
        # Activity types seen live but absent from the catalog (populated by check_connection).
        self.undeclared_activity_types: dict[int, str] = {}

    # --- catalog ---------------------------------------------------------------
    def get_streams(self) -> list[StreamDefinition]:
        """Declared streams plus one for every live activity type the catalog does
        not know yet — a type added in LeadSquared after the catalog snapshot is
        extracted from its very first run instead of being silently ignored."""
        extra = [
            _activity_stream(stream_name_for_event(code), code)
            for code in sorted(self.undeclared_activity_types)
        ]
        return [*self.declared_streams(), *extra]

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
        details: dict[str, Any] = {"lead_fields": len(payload)}
        # Best effort: a failure here must not fail the health check, but a new
        # activity type must not go unnoticed.
        try:
            live = await self._get(ACTIVITY_TYPES_PATH)
            live_types = {
                int(t["ActivityEvent"]): str(t.get("ActivityEventName") or "")
                for t in live
                if isinstance(t, dict) and t.get("ActivityEvent") is not None
            }
            self.undeclared_activity_types = {c: n for c, n in live_types.items() if c not in ACTIVITY_TYPES}
            details["activity_types_live"] = len(live_types)
            details["undeclared_activity_types"] = self.undeclared_activity_types
            details["catalog_types_missing_from_live"] = sorted(set(ACTIVITY_TYPES) - set(live_types))
        except (E.ConnectorError, TypeError, ValueError, KeyError) as exc:
            details["activity_types_check_error"] = str(exc)[:200]
        message = f"Connected to LeadSquared ({len(payload)} lead fields discovered)."
        if self.undeclared_activity_types:
            message += f" {len(self.undeclared_activity_types)} new activity type(s) will be extracted."
        return HealthReport(status=HealthStatus.HEALTHY, message=message, details=details)

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

    # --- response validation -----------------------------------------------
    def _read_page(self, payload: Any, key: str) -> tuple[int, list[dict[str, Any]]]:
        """Validate one retrieval response. A payload missing the data it must carry is
        an error — never an "empty page", which would silently end a window and let
        the checkpoint advance over rows that were never read."""
        if not isinstance(payload, dict):
            raise self._malformed(f"expected a JSON object, got {type(payload).__name__}", payload)
        if payload.get("Status") == "Error" or payload.get("ExceptionType"):
            # An application error delivered with a 2xx status.
            raise E.invalid_configuration(
                f"LeadSquared returned an error body: {payload.get('ExceptionMessage') or payload.get('Message')}",
                provider=self.provider,
                connector_id=self.connector_id,
                technical_details={"exception_type": str(payload.get("ExceptionType"))},
            )
        count = payload.get("RecordCount")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise self._malformed("missing or non-integer RecordCount", payload)
        rows = payload.get(key)
        if rows is None and count == 0:
            return 0, []  # LeadSquared omits the list entirely for an empty window
        if not isinstance(rows, list):
            raise self._malformed(f"RecordCount={count} but no {key!r} list", payload)
        return count, rows

    def _malformed(self, why: str, payload: Any) -> E.ConnectorError:
        return E.api_schema_error(
            f"LeadSquared returned a malformed response: {why}.",
            provider=self.provider,
            connector_id=self.connector_id,
            technical_details={"keys": sorted(payload)[:10] if isinstance(payload, dict) else None},
        )

    # --- READ ----------------------------------------------------------
    def _source_for(self, stream: StreamDefinition) -> _LeadSource | _ActivitySource:
        if stream.name == LEADS_STREAM:
            return _LeadSource(self, self._lead_page_size)
        code = event_for_stream_name(stream.name)
        if code is None:
            raise E.invalid_configuration(
                f"Unknown LeadSquared stream {stream.name!r}.",
                provider=self.provider,
                connector_id=self.connector_id,
            )
        return _ActivitySource(self, stream.name, code)

    async def read_range(
        self,
        stream: StreamDefinition,
        start: datetime,
        end: datetime,
        *,
        initial_span: timedelta | None = None,
    ) -> AsyncIterator[WindowBatch]:
        source = self._source_for(stream)
        fetcher = WindowFetcher(source)
        async for window in fetcher.windows(start, end, initial_span=initial_span):
            if stream.name == LEADS_STREAM:
                records = [r for r in (self._to_lead_record(a) for a in window.rows) if r is not None]
            else:
                field_map = _ACTIVITY_FIELD_MAP.get(stream.name, {})
                records = [
                    r for r in (self._to_activity_record(stream, row, field_map) for row in window.rows) if r
                ]
            yield WindowBatch(
                start=window.start,
                end=window.end,
                records=records,
                source_count=window.source_count,
                fetched_rows=window.fetched_rows,
                distinct_ids=window.distinct,
                unmappable=window.unmappable,
                api_calls=window.api_calls,
                split=window.split,
                paged_fallback=window.paged_fallback,
            )

    # --- verification helpers (used by the deletion sweep and count comparisons) ---
    async def count_window(self, stream: StreamDefinition, start: datetime, end: datetime) -> int:
        """The source's own total for a window — one cheap request (page size 1)."""
        page = await self._source_for(stream).fetch(start, end, 1, 1)
        return page.record_count

    async def window_ids(self, stream: StreamDefinition, start: datetime, end: datetime) -> set[str]:
        """Every id the source holds for a window, via the verified window fetcher."""
        source = self._source_for(stream)
        ids: set[str] = set()
        async for w in WindowFetcher(source).windows(start, end):
            ids.update(i for i in (source.row_id(r) for r in w.rows) if i)
        return ids

    async def record_exists(self, stream: StreamDefinition, record_id: str) -> bool:
        """Independent by-id existence check — the second signal a tombstone needs.

        Activities: `GetActivityDetails` answers a typed MXUnknownProspectActivityException
        for a deleted/unknown id (live-verified). Leads: `Leads.GetById` returns an empty
        list for an unknown id and a one-element list for a real one (live-verified).
        Only a definite "not found" returns False; any other failure raises.
        """
        if stream.name == LEADS_STREAM:
            found = await self._get("/v2/LeadManagement.svc/Leads.GetById", {"id": record_id})
            return bool(found)
        try:
            await self._get("/v2/ProspectActivity.svc/GetActivityDetails", {"activityId": record_id})
        except E.ConnectorError as exc:
            if exc.code == E.ErrorCode.RESOURCE_NOT_FOUND:
                return False
            raise
        return True

    async def read_slice(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[Record]:
        """Whole-day compatibility wrapper over `read_range` for callers that still
        speak date slices. The sync runner uses `read_range` directly."""
        start = _start_of_day(slice_.start_date or date.today())
        end = _end_of_day(slice_.end_date or slice_.start_date or date.today())
        async for batch in self.read_range(stream, start, end):
            for record in batch.records:
                yield record

    # --- record builders ---------------------------------------------------
    def _to_lead_record(self, d: dict[str, Any]) -> Record | None:
        prospect_id = d.get("ProspectID")
        if not prospect_id:
            return None
        modified_on = _parse_lsq_dt(d.get("ModifiedOn"))
        created_on = _parse_lsq_dt(d.get("CreatedOn"))
        last_modified = _parse_lsq_dt(d.get("LeadLastModifiedOn")) or modified_on
        # `date` keeps its long-standing meaning (ModifiedOn's day) so existing
        # views/queries over leadsquared_leads.date do not shift meaning.
        row_date = (modified_on or created_on or last_modified or datetime.now(UTC)).date()
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
            # The COMPLETE source payload. LeadSquared returns unset attributes as
            # null; a missing key therefore means "unset" and only those are elided.
            raw={k: v for k, v in d.items() if v is not None and k not in _ENVELOPE_ATTRIBUTES},
            cursor_value=d.get("LeadLastModifiedOn") or d.get("ModifiedOn"),
            extra={
                "source_modified_on": last_modified,
                "source_created_on": created_on,
                "prospect_stage": d.get("ProspectStage"),
                "owner_id": d.get("OwnerId"),
                "owner_name": _clip(d.get("OwnerIdName"), 200),
                "assigned_dietician": _clip(d.get("mx_Assigned_Dietician"), 200),
                "disposition": _clip(d.get("mx_Disposition"), 120),
                "diet_consultation_disposition": _clip(d.get("mx_Diet_Consultation_Disposition"), 120),
                "diet_consultation_at": _parse_lsq_dt(d.get("mx_Diet_Consultation_DateTime")),
                "deleted_at": None,
            },
        )

    def _to_activity_record(
        self, stream: StreamDefinition, row: dict[str, Any], field_map: dict[str, str]
    ) -> Record | None:
        activity_id = row.get("ProspectActivityId")
        if not activity_id:
            return None
        created_on = _parse_lsq_dt(row.get("CreatedOn"))
        modified_on = _parse_lsq_dt(row.get("ModifiedOn")) or created_on
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
        code = event_for_stream_name(stream.name)
        try:
            event = int(row.get("ActivityEvent")) if row.get("ActivityEvent") is not None else code
        except (TypeError, ValueError):
            event = code
        return Record(
            stream=stream.name,
            key_values={"prospect_activity_id": activity_id},
            date=row_date,
            dimensions=dimensions,
            metrics={},
            raw=row,
            cursor_value=row.get("ModifiedOn") or row.get("CreatedOn"),
            extra={
                "activity_event": event,
                "activity_event_name": ACTIVITY_TYPES.get(event) if event is not None else None,
                "source_modified_on": modified_on,
                "source_created_on": created_on,
                "deleted_at": None,
            },
        )


registry.register(
    RegistryEntry(
        connector_class=LeadSquaredCRMConnector,
        requires_settings=("leadsquared_access_key", "leadsquared_secret_key", "leadsquared_host"),
        prerequisites=(
            "A LeadSquared accessKey/secretKey pair with API access.",
            "The account's regional API host (e.g. api-in21.leadsquared.com) — LeadSquared "
            "shards accounts by region and there is no single global host.",
            "LEADSQUARED_ACCESS_KEY / LEADSQUARED_SECRET_KEY / LEADSQUARED_HOST must be set in the "
            "environment of the process that runs the SCHEDULER (the production service's .env), "
            "not only on the machine an operator triggers manual syncs from.",
        ),
        caveats=(
            "Revenue/conversion semantics are intentionally not modelled yet. See "
            "docs/coverage/LSQ_VERIFICATION_2026-09-11.md §9 for the business decisions "
            "(authoritative revenue field, Payment Status meaning, repeat-customer policy) "
            "needed before any revenue/ROAS reporting is built on this connector's data.",
            "Source deletions are detected only by the deletion sweep (app.sync.cli reconcile); "
            "the warehouse is upsert history, not a mirror, until that sweep has run.",
        ),
        resource_label="LeadSquared Account",
        tags=("crm", "leadsquared", "attribution"),
    )
)


__all__ = [
    "ACTIVITY_EVENTS",
    "ALL_ACTIVITY_EVENTS",
    "LEADS_STREAM",
    "LeadSquaredCRMConnector",
    "lead_attributes",
]
