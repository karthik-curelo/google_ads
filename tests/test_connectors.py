import httpx
import respx

from app.connectors import errors as E
from app.connectors.base import HealthStatus
from app.connectors.google.analytics import GoogleAnalyticsConnector
from app.connectors.google.search_console import GoogleSearchConsoleConnector
from app.connectors.meta.ads import MetaAdsConnector
from app.connectors.registry import availability, load_connectors
from tests._fakes import make_ctx

ADMIN = "https://analyticsadmin.googleapis.com/v1beta"
DATA = "https://analyticsdata.googleapis.com/v1beta"


def test_registry_has_five_connectors_and_availability_reasons():
    reg = load_connectors()
    assert {
        "google_analytics",
        "google_search_console",
        "google_ads",
        "meta_ads",
        "instagram_insights",
        "facebook_pages",
    } <= {e.connector_id for e in reg.all()}

    class S:  # missing google creds
        google_client_id = ""
        google_client_secret = ""
        meta_app_id = "x"
        meta_app_secret = "y"

    ok, reason = availability(reg.get("google_analytics"), S())
    assert ok is False and "GOOGLE_CLIENT_ID" in reason


@respx.mock
async def test_ga4_discover_resources_parses_account_summaries():
    respx.get(f"{ADMIN}/accountSummaries").mock(
        return_value=httpx.Response(
            200,
            json={
                "accountSummaries": [
                    {
                        "account": "accounts/111",
                        "displayName": "Acme",
                        "propertySummaries": [
                            {
                                "property": "properties/999",
                                "displayName": "Acme Web",
                                "propertyType": "PROPERTY_TYPE_ORDINARY",
                            }
                        ],
                    }
                ]
            },
        )
    )
    conn = GoogleAnalyticsConnector(make_ctx())
    resources = await conn.discover_resources()
    await conn.aclose()
    assert len(resources) == 1
    assert resources[0].resource_id == "999" and resources[0].parent_id == "111"


@respx.mock
async def test_ga4_read_slice_paginates_and_maps_measures():
    prop = "999"
    page1 = {
        "dimensionHeaders": [{"name": "date"}],
        "metricHeaders": [{"name": "activeUsers"}, {"name": "sessions"}],
        "rows": [
            {"dimensionValues": [{"value": "20260801"}], "metricValues": [{"value": "12"}, {"value": "20"}]}
        ],
        "rowCount": 2,
    }
    page2 = {
        **page1,
        "rows": [
            {"dimensionValues": [{"value": "20260802"}], "metricValues": [{"value": "5"}, {"value": "9"}]}
        ],
    }
    route = respx.post(f"{DATA}/properties/{prop}:runReport")
    route.side_effect = [httpx.Response(200, json=page1), httpx.Response(200, json=page2)]
    respx.get(f"{ADMIN}/properties/{prop}").mock(
        return_value=httpx.Response(200, json={"currencyCode": "USD"})
    )

    ctx = make_ctx(resource_id=prop)
    conn = GoogleAnalyticsConnector(ctx)
    stream = conn.get_stream("daily_overview")
    from datetime import date

    from app.connectors.base import StreamSlice

    got = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 8, 1), date(2026, 8, 2)))]
    await conn.aclose()
    assert len(got) == 2
    assert got[0].measures["users"] == 12 and got[0].measures["sessions"] == 20
    assert got[0].currency == "USD"
    assert got[0].date == date(2026, 8, 1)


@respx.mock
async def test_ga4_classifies_permission_denied():
    respx.get(f"{ADMIN}/accountSummaries").mock(
        return_value=httpx.Response(
            403,
            json={"error": {"code": 403, "status": "PERMISSION_DENIED", "message": "no access"}},
        )
    )
    conn = GoogleAnalyticsConnector(make_ctx(resource_id="999"))
    report = await conn.check_connection()
    await conn.aclose()
    assert report.status == HealthStatus.PERMISSION_DENIED


@respx.mock
async def test_gsc_read_slice_paginates_by_start_row():
    site = "https://example.com/"
    from urllib.parse import quote

    url = f"https://www.googleapis.com/webmasters/v3/sites/{quote(site, safe='')}/searchAnalytics/query"
    big = {
        "rows": [
            {
                "keys": [f"2026-08-0{i % 9 + 1}", f"q{i}"],
                "clicks": i,
                "impressions": i * 10,
                "ctr": 0.1,
                "position": 3.0,
            }
            for i in range(25000)
        ]
    }
    tail = {
        "rows": [{"keys": ["2026-08-05", "last"], "clicks": 1, "impressions": 2, "ctr": 0.5, "position": 1.0}]
    }
    route = respx.post(url)
    route.side_effect = [httpx.Response(200, json=big), httpx.Response(200, json=tail)]

    conn = GoogleSearchConsoleConnector(make_ctx(resource_id=site))
    stream = conn.get_stream("search_analytics_by_query")
    from datetime import date

    from app.connectors.base import StreamSlice

    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 8, 1), date(2026, 8, 7)))]
    await conn.aclose()
    assert len(rows) == 25001
    assert rows[-1].measures["clicks"] == 1 and rows[-1].measures["average_position"] == 1.0


@respx.mock
async def test_meta_paged_follows_next_cursor_and_maps_conversions():
    base = "https://graph.facebook.com/v26.0"
    act = "act_123/insights"
    p1 = {
        "data": [
            {
                "date_start": "2026-08-01",
                "date_stop": "2026-08-01",
                "campaign_id": "c1",
                "impressions": "100",
                "clicks": "10",
                "spend": "5.50",
                "reach": "80",
                "actions": [{"action_type": "purchase", "value": "3"}],
                "action_values": [{"action_type": "purchase", "value": "45.0"}],
            }
        ],
        "paging": {"next": f"{base}/{act}?after=XYZ"},
    }
    p2 = {
        "data": [
            {
                "date_start": "2026-08-02",
                "date_stop": "2026-08-02",
                "campaign_id": "c1",
                "impressions": "200",
                "clicks": "20",
                "spend": "9.00",
                "reach": "150",
                "actions": [],
            }
        ]
    }
    route = respx.get(url__regex=rf"{base}/act_123/insights.*")
    route.side_effect = [httpx.Response(200, json=p1), httpx.Response(200, json=p2)]

    ctx = make_ctx(resource_id="act_123", resource_metadata={"currency": "USD"})
    conn = MetaAdsConnector(ctx)
    stream = conn.get_stream("campaign_insights")
    from datetime import date

    from app.connectors.base import StreamSlice

    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 8, 1), date(2026, 8, 2)))]
    await conn.aclose()
    assert len(rows) == 2
    assert rows[0].measures["cost"] == 5.5 and rows[0].measures["conversions"] == 3.0
    assert rows[0].measures["conversion_value"] == 45.0
    assert rows[0].key_values["campaign_id"] == "c1"


def test_google_base_classifies_status_strings():
    conn = GoogleAnalyticsConnector(make_ctx())
    resp = httpx.Response(429, json={"error": {"status": "RESOURCE_EXHAUSTED", "message": "slow"}})
    err = conn._classify(resp)
    assert err is not None and err.code == E.ErrorCode.RATE_LIMIT_ERROR and err.retryable
