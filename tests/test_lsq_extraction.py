"""LeadSquared extraction through the real connector + HTTP client, against the
faithful in-memory API (tests/_lsq_fake.py).

Covers: complete lead payload, every activity type, the verified filter columns,
the error taxonomy (HTTP 500 is a *logical* error), retries, malformed responses,
and the shared per-account rate limiter.
"""

from __future__ import annotations

from datetime import datetime

import httpx
import pytest
import respx

from app.connectors import errors as E
from app.connectors.leadsquared.activity_catalog import ACTIVITY_TYPES, stream_name_for_event
from app.connectors.leadsquared.base import classify_lsq_response
from app.connectors.leadsquared.connector import (
    ALL_ACTIVITY_EVENTS,
    LEADS_STREAM,
    LeadSquaredCRMConnector,
)
from tests._fakes import make_ctx
from tests._lsq_fake import HOST, FakeLeadSquared, install

START = datetime(2026, 9, 1, 0, 0, 0)
END = datetime(2026, 9, 1, 23, 59, 59)


def _connector(**cfg) -> LeadSquaredCRMConnector:
    return LeadSquaredCRMConnector(
        make_ctx(
            config=cfg,
            provider_settings={
                "leadsquared_access_key": "ak",
                "leadsquared_secret_key": "sk",
                "leadsquared_host": HOST,
                "leadsquared_rate_per_second": 1000,
                "leadsquared_burst": 1000,
            },
        )
    )


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    """Retries use full-jitter backoff; make it instant so retry tests are fast."""
    monkeypatch.setattr("app.connectors.http.random.uniform", lambda _a, _b: 0.0)


async def _read(conn: LeadSquaredCRMConnector, stream_name: str, start=START, end=END):
    stream = conn.get_stream(stream_name)
    return [b async for b in conn.read_range(stream, start, end)]


# --- complete lead payload ------------------------------------------------------


@respx.mock
async def test_every_attribute_the_api_returns_for_a_lead_is_preserved():
    fake = FakeLeadSquared()
    attrs = {f"mx_Field_{i}": f"value {i}" for i in range(180)}  # far more than the old 24
    attrs.update(
        ProspectStage="Opportunity",
        FirstName="Asha",
        mx_Disposition="Callback",
        OwnerId="owner-1",
        OwnerIdName="Priya Nair",
        OwnerIdEmailAddress="priya@example.com",
        LeadConversionDate="2026-09-01 07:00:00.000",
        ProspectActivityDate_Max="2026-09-01 08:00:00.000",
        mx_Patient_Tags="vip,repeat",
        mx_Outbound_Call_COunter="4",
        Source="google_lp",
        mx_Ad_Id=None,  # unset at the source: elided, not invented
    )
    fake.add_lead("p1", "2026-09-01 09:30:00", **attrs)
    install(respx.mock, fake)

    conn = _connector()
    batches = await _read(conn, LEADS_STREAM)
    await conn.aclose()

    (rec,) = [r for b in batches for r in b.records]
    for name, value in attrs.items():
        if value is not None:
            assert rec.raw[name] == value, f"{name} was dropped"
    assert "mx_Ad_Id" not in rec.raw
    assert rec.raw["LeadLastModifiedOn"] == "2026-09-01 09:30:00"  # the real filter column
    # ...and the analytics-facing normalisation is unchanged
    assert rec.dimensions["source"] == "google_lp"
    # typed promotions
    assert rec.extra["prospect_stage"] == "Opportunity"
    assert rec.extra["owner_id"] == "owner-1"
    assert rec.extra["source_modified_on"] == datetime(
        2026, 9, 1, 9, 30, tzinfo=rec.extra["source_modified_on"].tzinfo
    )


@respx.mock
async def test_the_lead_request_never_restricts_columns():
    fake = FakeLeadSquared()
    fake.add_lead("p1", "2026-09-01 09:30:00", Phone="+91-1")
    install(respx.mock, fake)
    conn = _connector()
    await _read(conn, LEADS_STREAM)
    await conn.aclose()
    for req in fake.calls_to("Leads.RecentlyModified"):
        assert "Columns" not in req["body"], "sending Columns would silently drop lead attributes"
        assert req["body"]["Sorting"]["ColumnName"] == "ProspectID"  # unique, deterministic


# --- every activity type ----------------------------------------------------------


@respx.mock
async def test_all_84_activity_types_are_extracted_with_full_payload_and_type_discriminator():
    fake = FakeLeadSquared()
    for code in ACTIVITY_TYPES:
        fake.add_activity(
            code, f"act-{code}", "2026-09-01 10:00:00", mx_Custom_1=f"payload-{code}", mx_Custom_9="x"
        )
    install(respx.mock, fake)

    conn = _connector()
    extracted = {}
    for name, code in ALL_ACTIVITY_EVENTS.items():
        batches = await _read(conn, name)
        (rec,) = [r for b in batches for r in b.records]
        extracted[code] = rec
    await conn.aclose()

    assert set(extracted) == set(ACTIVITY_TYPES) and len(extracted) == 84
    for code, rec in extracted.items():
        assert rec.stream == stream_name_for_event(code)
        assert rec.extra["activity_event"] == code  # first-class discriminator
        assert rec.extra["activity_event_name"] == ACTIVITY_TYPES[code]
        assert rec.raw["mx_Custom_1"] == f"payload-{code}"  # full source row kept verbatim
        assert rec.raw["ProspectActivityId"] == f"act-{code}"


@respx.mock
async def test_a_new_activity_type_seen_live_becomes_a_stream_instead_of_being_ignored():
    fake = FakeLeadSquared()
    fake.activity_types = {**ACTIVITY_TYPES, 9999: "Brand New Event"}
    fake.add_activity(9999, "new-1", "2026-09-01 10:00:00", mx_Custom_1="hello")
    install(respx.mock, fake)

    conn = _connector()
    assert "activity_9999" not in {s.name for s in conn.get_streams()}
    report = await conn.check_connection()
    assert report.ok and report.details["undeclared_activity_types"] == {9999: "Brand New Event"}
    assert "activity_9999" in {s.name for s in conn.get_streams()}

    batches = await _read(conn, "activity_9999")
    await conn.aclose()
    (rec,) = [r for b in batches for r in b.records]
    assert rec.extra["activity_event"] == 9999 and rec.raw["mx_Custom_1"] == "hello"


@respx.mock
async def test_check_connection_survives_a_failing_activity_type_lookup():
    fake = FakeLeadSquared()
    install(respx.mock, fake)
    fake.inject.append(
        lambda c: httpx.Response(404, json={}) if c["path"].endswith("ActivityTypes.Get") else None
    )
    conn = _connector()
    report = await conn.check_connection()
    await conn.aclose()
    assert report.ok and "activity_types_check_error" in report.details


# --- the verified filter columns -----------------------------------------------------


@respx.mock
async def test_activity_windows_filter_on_modified_on_not_created_on():
    fake = FakeLeadSquared()
    # created in August, edited on Sep 1 (the real audit case)
    fake.add_activity(206, "edited", "2026-09-01 05:10:00", created="2026-08-29 06:15:02")
    install(respx.mock, fake)
    conn = _connector()

    aug = await _read(conn, "booking_created", datetime(2026, 8, 29), datetime(2026, 8, 29, 23, 59, 59))
    sep = await _read(conn, "booking_created", START, END)
    await conn.aclose()
    assert sum(b.source_count for b in aug) == 0  # invisible by its CreatedOn
    assert [r.key_values["prospect_activity_id"] for b in sep for r in b.records] == ["edited"]


@respx.mock
async def test_lead_windows_filter_on_last_modified_on_not_the_modified_on_attribute():
    fake = FakeLeadSquared()
    row = fake.add_lead("p1", "2026-09-01 09:00:00")
    row["ModifiedOn"] = "2026-08-20 09:00:00"  # attribute is old; a new activity bumped LeadLastModifiedOn
    install(respx.mock, fake)
    conn = _connector()
    batches = await _read(conn, LEADS_STREAM)
    await conn.aclose()
    assert [r.key_values["prospect_id"] for b in batches for r in b.records] == ["p1"]


# --- errors: HTTP 500 is a logical error ---------------------------------------------


def _resp(status: int, exc_type: str, message: str) -> httpx.Response:
    return httpx.Response(
        status, json={"Status": "Error", "ExceptionType": exc_type, "ExceptionMessage": message}
    )


@pytest.mark.parametrize(
    ("exc_type", "message", "expected_code"),
    [
        ("MXInvalidInputException", "PageSize can not be more than 1000.", E.ErrorCode.INVALID_CONFIGURATION),
        (
            "MXMandatoryFieldMissingException",
            "Page size can not be larger than 5000.",
            E.ErrorCode.INVALID_CONFIGURATION,
        ),
        ("MXUnknownProspectActivityException", "Activity does not exist.", E.ErrorCode.RESOURCE_NOT_FOUND),
        ("MXSomethingNewException", "no idea", E.ErrorCode.INVALID_CONFIGURATION),
    ],
)
def test_http_500_with_an_mx_exception_is_a_non_retryable_logical_error(exc_type, message, expected_code):
    err = classify_lsq_response(_resp(500, exc_type, message))
    assert err is not None and err.code == expected_code
    assert err.retryable is False, "a deterministic API rejection must not be retried"


def test_bad_credentials_are_an_authentication_error_not_retried():
    err = classify_lsq_response(
        _resp(401, "MXInvalidAccessDetailsException", "Invalid Access Details provided.")
    )
    assert err.code == E.ErrorCode.AUTHENTICATION_ERROR and not err.retryable


def test_429_is_a_retryable_rate_limit_error():
    err = classify_lsq_response(_resp(429, "MXThrottleException", "Too many calls"))
    assert err.code == E.ErrorCode.RATE_LIMIT_ERROR and err.retryable


def test_a_500_without_an_mx_body_or_with_a_transient_message_is_still_transient():
    assert (
        classify_lsq_response(httpx.Response(500, text="<html>Bad gateway</html>")) is None
    )  # -> generic retry
    err = classify_lsq_response(_resp(500, "MXTimeoutException", "The operation timed out, try again"))
    assert err.code == E.ErrorCode.PROVIDER_UNAVAILABLE and err.retryable


@respx.mock
async def test_a_logical_500_is_sent_once_not_five_times():
    fake = FakeLeadSquared()
    install(respx.mock, fake)
    fake.inject.append(
        lambda c: _resp(500, "MXInvalidInputException", "nope") if "Retrieve" in c["path"] else None
    )
    conn = _connector()
    stats = conn.http.stats  # captured before aclose(), which discards the client
    with pytest.raises(E.ConnectorError) as exc:
        await _read(conn, "booking_created")
    await conn.aclose()
    assert exc.value.code == E.ErrorCode.INVALID_CONFIGURATION and exc.value.retryable is False
    assert stats.calls == 1 and stats.retries == 0


# --- retries: transient failures are retried safely ------------------------------------


@respx.mock
async def test_429_then_success_retries_and_counts_the_rate_limit_event():
    fake = FakeLeadSquared()
    fake.add_activity(206, "a1", "2026-09-01 10:00:00")
    install(respx.mock, fake)
    state = {"n": 0}

    def first_call_is_throttled(c):
        if "Retrieve" in c["path"]:
            state["n"] += 1
            if state["n"] == 1:
                return _resp(429, "MXThrottleException", "Too many calls")
        return None

    fake.inject.append(first_call_is_throttled)
    conn = _connector()
    stats = conn.http.stats
    batches = await _read(conn, "booking_created")
    await conn.aclose()
    assert [r.key_values["prospect_activity_id"] for b in batches for r in b.records] == ["a1"]
    assert stats.retries == 1 and stats.rate_limit_events == 1


@respx.mock
async def test_a_timeout_is_retried_and_the_window_still_completes():
    fake = FakeLeadSquared()
    fake.add_activity(206, "a1", "2026-09-01 10:00:00")
    install(respx.mock, fake)
    state = {"n": 0}

    def times_out_once(c):
        if "Retrieve" in c["path"]:
            state["n"] += 1
            if state["n"] == 1:
                return httpx.ReadTimeout("slow")
        return None

    fake.inject.append(times_out_once)
    conn = _connector()
    stats = conn.http.stats
    batches = await _read(conn, "booking_created")
    await conn.aclose()
    assert sum(len(b.records) for b in batches) == 1 and stats.retries == 1


@respx.mock
async def test_a_transient_502_is_retried():
    fake = FakeLeadSquared()
    fake.add_activity(206, "a1", "2026-09-01 10:00:00")
    install(respx.mock, fake)
    state = {"n": 0}

    def bad_gateway_once(c):
        if "Retrieve" in c["path"]:
            state["n"] += 1
            if state["n"] == 1:
                return httpx.Response(502, text="bad gateway")
        return None

    fake.inject.append(bad_gateway_once)
    conn = _connector()
    batches = await _read(conn, "booking_created")
    await conn.aclose()
    assert sum(len(b.records) for b in batches) == 1


# --- malformed responses must never look like "no more data" ----------------------------


@pytest.mark.parametrize(
    "payload",
    [
        ["not", "an", "object"],
        {},  # no RecordCount
        {"RecordCount": "12", "List": []},  # non-integer count
        {"RecordCount": 5},  # rows promised, list missing
        {"RecordCount": 5, "List": "oops"},
        {"Status": "Error", "ExceptionType": "MXInvalidInputException", "ExceptionMessage": "in a 200"},
    ],
)
@respx.mock
async def test_a_malformed_page_raises_instead_of_being_treated_as_an_empty_page(payload):
    fake = FakeLeadSquared()
    install(respx.mock, fake)
    fake.inject.append(lambda c: httpx.Response(200, json=payload) if "Retrieve" in c["path"] else None)
    conn = _connector()
    with pytest.raises(E.ConnectorError):
        await _read(conn, "booking_created")
    await conn.aclose()


@respx.mock
async def test_a_genuinely_empty_window_omits_the_list_and_is_valid():
    fake = FakeLeadSquared()
    install(respx.mock, fake)
    conn = _connector()
    batches = await _read(conn, "booking_created")
    await conn.aclose()
    assert [(b.source_count, len(b.records)) for b in batches] == [(0, 0)]


# --- rate limiting is shared per account -------------------------------------------------


def test_clients_for_one_account_share_a_single_rate_limiter():
    a, b = _connector(), _connector()
    assert a.http.limiter is b.http.limiter
    other = LeadSquaredCRMConnector(
        make_ctx(
            provider_settings={
                "leadsquared_access_key": "different",
                "leadsquared_secret_key": "sk",
                "leadsquared_host": HOST,
            }
        )
    )
    assert other.http.limiter is not a.http.limiter


@respx.mock
async def test_a_429_slows_every_worker_sharing_the_limiter():
    fake = FakeLeadSquared()
    fake.add_activity(206, "a1", "2026-09-01 10:00:00")
    install(respx.mock, fake)
    state = {"n": 0}

    def once(c):
        if "Retrieve" in c["path"]:
            state["n"] += 1
            if state["n"] == 1:
                return _resp(429, "MXThrottleException", "Too many calls")
        return None

    fake.inject.append(once)
    a, b = _connector(), _connector()
    await _read(a, "booking_created")
    # b never saw the 429, but shares the (now penalised) bucket
    assert b.http.limiter._penalty_factor == 2.0
    await a.aclose()
    await b.aclose()
