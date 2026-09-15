"""LeadSquared connector — auth, pagination, and the per-event field mapping
that LSQ_VERIFICATION_2026-09-11.md §B confirmed live (booking_id and the
other §14-preserved fields sit at a *different* mx_Custom_N slot per event
type; getting that wrong silently corrupts every downstream join)."""

from __future__ import annotations

from datetime import date

import httpx
import respx

from app.connectors import errors as E
from app.connectors.base import AuthType, HealthStatus, StreamSlice
from app.connectors.leadsquared.connector import ACTIVITY_EVENTS, LEADS_STREAM, LeadSquaredCRMConnector
from app.connectors.registry import load_connectors
from tests._fakes import make_ctx

HOST = "https://api-test.leadsquared.com"


def _ctx(**kw):
    kw.setdefault(
        "provider_settings",
        {
            "leadsquared_access_key": "ak",
            "leadsquared_secret_key": "sk",
            "leadsquared_host": HOST,
            "http_timeout_seconds": 30.0,
        },
    )
    return make_ctx(**kw)


def test_registry_has_leadsquared_with_static_api_key_auth():
    reg = load_connectors()
    entry = reg.get("leadsquared")
    assert entry.connector_class.auth_type == AuthType.API_KEY
    assert entry.connector_class.provider == "leadsquared"
    names = {s.name for s in entry.connector_class.declared_streams()}
    assert names == {LEADS_STREAM, *ACTIVITY_EVENTS}


@respx.mock
async def test_check_connection_success():
    respx.get(f"{HOST}/v2/LeadManagement.svc/LeadsMetaData.Get").mock(
        return_value=httpx.Response(200, json=[{"SchemaName": "ProspectID"}, {"SchemaName": "Phone"}])
    )
    conn = LeadSquaredCRMConnector(_ctx())
    report = await conn.check_connection()
    await conn.aclose()
    assert report.ok
    assert "2 lead fields" in report.message


@respx.mock
async def test_check_connection_classifies_bad_credentials():
    respx.get(f"{HOST}/v2/LeadManagement.svc/LeadsMetaData.Get").mock(
        return_value=httpx.Response(
            401,
            json={
                "Status": "Error",
                "ExceptionType": "MXAuthenticationFailedException",
                "ExceptionMessage": "Invalid accessKey or secretKey",
            },
        )
    )
    conn = LeadSquaredCRMConnector(_ctx())
    report = await conn.check_connection()
    await conn.aclose()
    assert report.status == HealthStatus.NEEDS_REAUTH


def test_missing_credentials_raises_invalid_configuration():
    conn = LeadSquaredCRMConnector(_ctx(provider_settings={"http_timeout_seconds": 30.0}))
    try:
        conn._require_configured()
    except E.ConnectorError as exc:
        assert exc.code == E.ErrorCode.INVALID_CONFIGURATION
    else:
        raise AssertionError("expected invalid_configuration")


@respx.mock
async def test_discover_resources_returns_the_one_account():
    respx.get(f"{HOST}/v2/LeadManagement.svc/LeadsMetaData.Get").mock(
        return_value=httpx.Response(200, json=[{"SchemaName": "ProspectID"}])
    )
    conn = LeadSquaredCRMConnector(_ctx())
    resources = await conn.discover_resources()
    await conn.aclose()
    assert len(resources) == 1
    assert resources[0].resource_id == "account"


def _lead_property_list(**fields: str | None) -> dict:
    return {"LeadPropertyList": [{"Attribute": k, "Value": v} for k, v in fields.items()]}


@respx.mock
async def test_read_slice_leads_maps_identity_and_attribution_fields():
    page = {
        "RecordCount": 1,
        "Leads": [
            _lead_property_list(
                ProspectID="p1",
                Phone="+91-9999999999",
                EmailAddress=None,
                Source="google_lp",
                SourceCampaign="Search_FBC",
                mx_Source_Campaign_ID="23226177337",
                mx_GCLid="CjwK...",
                mx_Ad_Id=None,
                mx_Adset_Id="118000000",
                mx_utm_source="google",
                mx_Lead_Type="P1 - Curelo New",
                CreatedOn="2026-09-10 05:06:15.000",
                ModifiedOn="2026-09-11 05:27:32.000",
            )
        ],
    }
    route = respx.post(f"{HOST}/v2/LeadManagement.svc/Leads.RecentlyModified")
    route.mock(return_value=httpx.Response(200, json=page))

    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream(LEADS_STREAM)
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 10), date(2026, 9, 11)))]
    await conn.aclose()

    assert len(rows) == 1
    r = rows[0]
    assert r.key_values == {"prospect_id": "p1"}
    assert r.date == date(2026, 9, 11)  # from ModifiedOn, not CreatedOn
    assert r.dimensions["source"] == "google_lp"
    assert r.dimensions["mx_source_campaign_id"] == "23226177337"
    assert r.dimensions["mx_gclid"] == "CjwK..."
    assert r.dimensions["mx_adset_id"] == "118000000"
    assert r.dimensions["mx_lead_type"] == "P1 - Curelo New"
    assert r.cursor_value == "2026-09-11 05:27:32.000"
    assert r.raw["ProspectID"] == "p1"  # raw payload preserved verbatim


@respx.mock
async def test_read_slice_leads_paginates_at_the_confirmed_5000_cap():
    full_page = {"RecordCount": 5001, "Leads": [_lead_property_list(ProspectID=f"p{i}") for i in range(5000)]}
    tail_page = {"RecordCount": 5001, "Leads": [_lead_property_list(ProspectID="p5000")]}
    route = respx.post(f"{HOST}/v2/LeadManagement.svc/Leads.RecentlyModified")
    route.side_effect = [httpx.Response(200, json=full_page), httpx.Response(200, json=tail_page)]

    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream(LEADS_STREAM)
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    await conn.aclose()
    assert len(rows) == 5001
    assert route.call_count == 2


@respx.mock
async def test_read_slice_leads_without_a_prospect_id_is_skipped():
    page = {"RecordCount": 1, "Leads": [_lead_property_list(ProspectID=None, Source="Direct Traffic")]}
    respx.post(f"{HOST}/v2/LeadManagement.svc/Leads.RecentlyModified").mock(
        return_value=httpx.Response(200, json=page)
    )
    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream(LEADS_STREAM)
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    await conn.aclose()
    assert rows == []


def _activity_row(**overrides) -> dict:
    row = {
        "ProspectActivityId": "act-1",
        "RelatedProspectId": "p1",
        "ActivityEvent": "206",
        "ActivityEvent_Note": "Booking Created Details",
        "Status": "Active",
        "CreatedOn": "2026-09-11 06:01:40",
        "ModifiedOn": "2026-09-11 06:01:42",
    }
    row.update(overrides)
    return row


@respx.mock
async def test_read_slice_booking_created_resolves_booking_id_at_its_own_slot():
    """206's Booking ID is mx_Custom_2 — confirmed via GetActivitySetting,
    not the mx_Custom_4 slot 208 uses for the same field name."""
    row = _activity_row(mx_Custom_2="BK-100", mx_Custom_6="999", mx_Custom_15="Success", mx_Custom_16="upi")
    respx.post(f"{HOST}/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent").mock(
        return_value=httpx.Response(200, json={"RecordCount": 1, "List": [row]})
    )
    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream("booking_created")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 11), date(2026, 9, 11)))]
    await conn.aclose()
    assert len(rows) == 1
    r = rows[0]
    assert r.key_values == {"prospect_activity_id": "act-1"}
    assert r.dimensions["related_prospect_id"] == "p1"
    assert r.dimensions["booking_id"] == "BK-100"
    assert r.dimensions["total_paid_amount"] == "999"
    assert r.dimensions["payment_status"] == "Success"
    assert r.dimensions["payment_method"] == "upi"
    # 208-only fields must not leak onto a 206 row
    assert r.dimensions.get("booking_status") is None
    assert r.date == date(2026, 9, 11)


@respx.mock
async def test_read_slice_post_booking_order_status_resolves_its_own_slots():
    """208's Booking ID is mx_Custom_4, not 206's mx_Custom_2 — this is the
    exact landmine the verification report flagged and metadata-confirmed."""
    row = _activity_row(
        ActivityEvent="208",
        mx_Custom_4="BK-100",
        mx_Custom_6="completed",
        mx_Custom_11="P1 - Curelo New",
        mx_Custom_15="1018",
        mx_Custom_20="1018",
        mx_Custom_22="150",
    )
    respx.post(f"{HOST}/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent").mock(
        return_value=httpx.Response(200, json={"RecordCount": 1, "List": [row]})
    )
    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream("post_booking_order_status")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 11), date(2026, 9, 11)))]
    await conn.aclose()
    r = rows[0]
    assert r.dimensions["booking_id"] == "BK-100"
    assert r.dimensions["booking_status"] == "completed"
    assert r.dimensions["patient_type"] == "P1 - Curelo New"
    assert r.dimensions["actual_amount"] == "1018"
    assert r.dimensions["booking_amount"] == "1018"
    assert r.dimensions["curelo_commission"] == "150"
    # 206-only fields must not leak onto a 208 row
    assert r.dimensions.get("total_paid_amount") is None
    assert r.dimensions.get("payment_status") is None


@respx.mock
async def test_read_slice_booking_cancelled_resolves_its_own_slots():
    row = _activity_row(
        ActivityEvent="223", mx_Custom_3="BK-100", mx_Custom_5="675.0", mx_Custom_6="Booked by mistake"
    )
    respx.post(f"{HOST}/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent").mock(
        return_value=httpx.Response(200, json={"RecordCount": 1, "List": [row]})
    )
    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream("booking_cancelled")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 11), date(2026, 9, 11)))]
    await conn.aclose()
    r = rows[0]
    assert r.dimensions["booking_id"] == "BK-100"
    assert r.dimensions["cancelled_amount"] == "675.0"


@respx.mock
async def test_read_slice_activity_paginates_at_the_confirmed_1000_cap():
    full_page = {
        "RecordCount": 1001,
        "List": [_activity_row(ProspectActivityId=f"a{i}") for i in range(1000)],
    }
    tail_page = {"RecordCount": 1001, "List": [_activity_row(ProspectActivityId="a1000")]}
    route = respx.post(f"{HOST}/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent")
    route.side_effect = [httpx.Response(200, json=full_page), httpx.Response(200, json=tail_page)]

    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream("booking_created")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    await conn.aclose()
    assert len(rows) == 1001
    assert route.call_count == 2


@respx.mock
async def test_activity_over_page_size_cap_classifies_as_invalid_configuration():
    """Live-observed shape: requesting PageSize > 1000 on this endpoint
    returns a 500 with an MXInvalidInputException body, not a 4xx."""
    respx.post(f"{HOST}/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent").mock(
        return_value=httpx.Response(
            500,
            json={
                "Status": "Error",
                "ExceptionType": "MXInvalidInputException",
                "ExceptionMessage": "Invalid Input! Parameter Name: PageSize can not be more than 1000",
            },
        )
    )
    conn = LeadSquaredCRMConnector(_ctx())
    stream = conn.get_stream("booking_created")
    try:
        _ = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    except E.ConnectorError as exc:
        assert exc.code == E.ErrorCode.INVALID_CONFIGURATION
        assert exc.retryable is False
    else:
        raise AssertionError("expected invalid_configuration")
    await conn.aclose()


@respx.mock
async def test_rate_limit_response_is_retryable():
    respx.post(f"{HOST}/v2/ProspectActivity.svc/CustomActivity/RetrieveByActivityEvent").mock(
        return_value=httpx.Response(
            429,
            json={
                "Status": "Error",
                "ExceptionType": "MXThrottleException",
                "ExceptionMessage": "Too many calls",
            },
        )
    )
    conn = LeadSquaredCRMConnector(_ctx())
    resp = httpx.Response(
        429,
        json={
            "Status": "Error",
            "ExceptionType": "MXThrottleException",
            "ExceptionMessage": "Too many calls",
        },
    )
    err = conn._classify(resp)
    await conn.aclose()
    assert err is not None
    assert err.code == E.ErrorCode.RATE_LIMIT_ERROR
    assert err.retryable is True
