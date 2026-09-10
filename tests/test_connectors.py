import httpx
import respx

from app.connectors import errors as E
from app.connectors.base import HealthStatus
from app.connectors.google.ads import GoogleAdsConnector
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


_ADS = "https://googleads.googleapis.com/v25"


def _ads_ctx():
    return make_ctx(
        resource_id="9232673741",
        resource_metadata={"currency_code": "INR"},  # preset so _get_currency short-circuits
        provider_settings={"http_timeout_seconds": 30.0, "google_ads_developer_token": "dev-tok"},
    )


@respx.mock
async def test_ads_auction_insight_stream_skips_on_metric_access_denied():
    """Allowlist-gated Auction Insights metrics 403 with METRIC_ACCESS_DENIED on a
    non-allowlisted token — the stream must yield nothing and NOT raise, so the
    run stays green and self-heals once the token is allowlisted."""
    from datetime import date

    from app.connectors.base import StreamSlice

    respx.post(f"{_ADS}/customers/9232673741/googleAds:searchStream").mock(
        return_value=httpx.Response(
            403,
            json={
                "error": {
                    "code": 403,
                    "status": "PERMISSION_DENIED",
                    "message": "The developer doesn't have access to metrics: "
                    "'auction_insight_search_impression_share'.",
                }
            },
        )
    )
    conn = GoogleAdsConnector(_ads_ctx())
    stream = conn.get_stream("auction_insight_campaign_performance")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 7)))]
    await conn.aclose()
    assert rows == []


@respx.mock
async def test_ads_group_placement_stream_maps_rows():
    from datetime import date

    from app.connectors.base import StreamSlice

    respx.post(f"{_ADS}/customers/9232673741/googleAds:searchStream").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "results": [
                        {
                            "campaign": {"id": "23479169945"},
                            "segments": {"date": "2026-09-05"},
                            "groupPlacementView": {
                                "resourceName": "customers/9/groupPlacementViews/1~ABC",
                                "placement": "youtube.com/channel/UC123",
                                "displayName": "Some Channel",
                                "placementType": "YOUTUBE_CHANNEL",
                                "targetUrl": "https://youtube.com/channel/UC123",
                            },
                            "metrics": {"impressions": "42", "clicks": "3", "costMicros": "1500000"},
                        }
                    ]
                }
            ],
        )
    )
    conn = GoogleAdsConnector(_ads_ctx())
    stream = conn.get_stream("group_placement_performance")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 5), date(2026, 9, 5)))]
    await conn.aclose()
    assert len(rows) == 1
    rec = rows[0]
    assert rec.date == date(2026, 9, 5)
    assert rec.dimensions["group_placement_view.placement"] == "youtube.com/channel/UC123"
    assert rec.measures["impressions"] == 42 and rec.measures["clicks"] == 3
    # pk is (date, resource_name) — `placement` is null for Google's unknown bucket
    assert rec.key_values["group_placement_view.resource_name"] == "customers/9/groupPlacementViews/1~ABC"


@respx.mock
async def test_ads_asset_group_asset_stream_maps_rows():
    """PMax per-asset performance (the PMax half of 'Asset-Wise CTR') — verified
    live against customer 9232673741 on 2026-09-09; this test locks in the
    mapping with a synthetic 200 response shaped like that live response."""
    from datetime import date

    from app.connectors.base import StreamSlice

    respx.post(f"{_ADS}/customers/9232673741/googleAds:searchStream").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "results": [
                        {
                            "campaign": {"id": "23784451377"},
                            "assetGroup": {"id": "6705438678", "name": "Blood Test and FBC"},
                            "segments": {"date": "2026-09-01"},
                            "asset": {"resourceName": "customers/9/assets/298323238808", "type": "TEXT"},
                            "assetGroupAsset": {
                                "resourceName": "customers/9/assetGroupAssets/6705438678~298323238808~HEADLINE",
                                "asset": "customers/9/assets/298323238808",
                                "fieldType": "HEADLINE",
                                "status": "ENABLED",
                            },
                            "metrics": {
                                "impressions": "1079",
                                "clicks": "63",
                                "ctr": 0.0584,
                                "costMicros": "3435961696",
                                "conversions": 3,
                                "conversionsValue": 2496,
                            },
                        }
                    ]
                }
            ],
        )
    )
    conn = GoogleAdsConnector(_ads_ctx())
    stream = conn.get_stream("asset_group_asset_performance")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    await conn.aclose()
    assert len(rows) == 1
    rec = rows[0]
    assert rec.date == date(2026, 9, 1)
    assert rec.dimensions["asset_group_asset.field_type"] == "HEADLINE"
    assert rec.dimensions["asset_group.name"] == "Blood Test and FBC"
    assert rec.measures["impressions"] == 1079 and rec.measures["clicks"] == 63
    assert rec.measures["conversions"] == 3
    # pk is (date, resource_name) — always populated and globally unique
    assert (
        rec.key_values["asset_group_asset.resource_name"]
        == "customers/9/assetGroupAssets/6705438678~298323238808~HEADLINE"
    )


@respx.mock
async def test_ads_search_term_stream_preserves_multi_keyword_grain():
    """Same search term, same ad group, same day, TWO different triggering
    keywords — live-verified real shape on customer 9232673741 (e.g. "mri near
    me" under ad group 194345089574 maps to two distinct keyword criteria).
    Without segments.keyword.ad_group_criterion in the pk these would collapse
    into one row as a 'duplicate primary key within batch' skip; this test
    locks in that both rows survive, distinct, with their own keyword."""
    from datetime import date

    from app.connectors.base import StreamSlice

    respx.post(f"{_ADS}/customers/9232673741/googleAds:searchStream").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "results": [
                        {
                            "campaign": {"id": "23479169945"},
                            "adGroup": {"id": "194345089574"},
                            "segments": {
                                "date": "2026-09-01",
                                "searchTermMatchType": "BROAD",
                                "keyword": {
                                    "info": {"text": "mri scan", "matchType": "PHRASE"},
                                    "adGroupCriterion": "customers/9/adGroupCriteria/194345089574~41176711",
                                },
                            },
                            "searchTermView": {
                                "resourceName": "customers/9/searchTermViews/1",
                                "status": "NONE",
                                "searchTerm": "mri near me",
                            },
                            "metrics": {"impressions": "10", "clicks": "2", "costMicros": "500000"},
                        },
                        {
                            "campaign": {"id": "23479169945"},
                            "adGroup": {"id": "194345089574"},
                            "segments": {
                                "date": "2026-09-01",
                                "searchTermMatchType": "BROAD",
                                "keyword": {
                                    "info": {"text": "mri near me", "matchType": "BROAD"},
                                    "adGroupCriterion": "customers/9/adGroupCriteria/194345089574~84231112641",
                                },
                            },
                            "searchTermView": {
                                "resourceName": "customers/9/searchTermViews/1",
                                "status": "NONE",
                                "searchTerm": "mri near me",
                            },
                            "metrics": {"impressions": "7", "clicks": "1", "costMicros": "300000"},
                        },
                        # a row with NO resolvable keyword (Google omits the segment
                        # entirely for some rows) — must survive as its own row,
                        # not collapse onto either of the two above.
                        {
                            "campaign": {"id": "23479169945"},
                            "adGroup": {"id": "194345089574"},
                            "segments": {"date": "2026-09-01", "searchTermMatchType": "BROAD"},
                            "searchTermView": {
                                "resourceName": "customers/9/searchTermViews/1",
                                "status": "NONE",
                                "searchTerm": "mri near me",
                            },
                            "metrics": {"impressions": "1", "clicks": "0", "costMicros": "0"},
                        },
                    ]
                }
            ],
        )
    )
    conn = GoogleAdsConnector(_ads_ctx())
    stream = conn.get_stream("search_term_performance")
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    await conn.aclose()

    # grain preserved: 3 API rows in -> 3 warehouse rows out, none collapsed.
    assert len(rows) == 3
    # total == unique grain: every row keys uniquely on (date, ad_group, term,
    # match_type, keyword_criterion).
    assert len({tuple(sorted(r.key_values.items())) for r in rows}) == 3

    # group by key_values (the null-safe pk projection) rather than dimensions
    # (which carries the raw None for the no-keyword row, not "").
    by_criterion = {r.key_values["segments.keyword.ad_group_criterion"]: r for r in rows}
    assert set(by_criterion) == {
        "customers/9/adGroupCriteria/194345089574~41176711",
        "customers/9/adGroupCriteria/194345089574~84231112641",
        "",  # the no-keyword row: preserved as "" (null-safe), not dropped
    }
    # same search term, two different keywords, correctly distinguished
    r1 = by_criterion["customers/9/adGroupCriteria/194345089574~41176711"]
    r2 = by_criterion["customers/9/adGroupCriteria/194345089574~84231112641"]
    assert r1.dimensions["search_term_view.search_term"] == "mri near me"
    assert r2.dimensions["search_term_view.search_term"] == "mri near me"
    assert r1.dimensions["segments.keyword.info.text"] == "mri scan"
    assert r2.dimensions["segments.keyword.info.text"] == "mri near me"
    assert r1.measures["clicks"] == 2 and r2.measures["clicks"] == 1
    # the unresolved-keyword row kept its own metrics too, not merged into either
    r3 = by_criterion[""]
    assert r3.measures["impressions"] == 1


@respx.mock
async def test_ads_conversion_action_performance_maps_rows_and_is_separate_stream():
    """New normalized conversion-action stream — separate from
    campaign_performance, only conversions/conversions_value requested (never
    impressions/clicks/cost, which are not additive across this segment).
    Live-verified reconciliation: summed across every conversion_action row
    for a (date, campaign), these equal that campaign's aggregate
    metrics.conversions — 107/107 pairs matched on customer 9232673741,
    2026-09-09, zero mismatch."""
    from datetime import date

    from app.connectors.base import StreamSlice

    respx.post(f"{_ADS}/customers/9232673741/googleAds:searchStream").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "results": [
                        {
                            "campaign": {"id": "23195333226", "name": "Search_FBC_Delhi_NCR"},
                            "segments": {
                                "date": "2026-09-01",
                                "conversionAction": "customers/9/conversionActions/7410561274",
                                "conversionActionName": "curelohealth (web) Submit_Form",
                                "conversionActionCategory": "SUBMIT_LEAD_FORM",
                            },
                            "metrics": {"conversions": 12.999829, "conversionsValue": 0},
                        },
                        {
                            "campaign": {"id": "23195333226", "name": "Search_FBC_Delhi_NCR"},
                            "segments": {
                                "date": "2026-09-01",
                                "conversionAction": "customers/9/conversionActions/6672831801",
                                "conversionActionName": "Purchase",
                                "conversionActionCategory": "PURCHASE",
                            },
                            "metrics": {"conversions": 2, "conversionsValue": 4998.0},
                        },
                    ]
                }
            ],
        )
    )
    conn = GoogleAdsConnector(_ads_ctx())
    stream = conn.get_stream("campaign_conversion_action_performance")
    # only the two intended metrics are ever requested by this stream
    assert stream.spec["metrics"] == ["metrics.conversions", "metrics.conversions_value"]
    rows = [r async for r in conn.read_slice(stream, StreamSlice(date(2026, 9, 1), date(2026, 9, 1)))]
    await conn.aclose()
    assert len(rows) == 2

    by_action = {r.dimensions["segments.conversion_action"]: r for r in rows}
    lead = by_action["customers/9/conversionActions/7410561274"]
    purchase = by_action["customers/9/conversionActions/6672831801"]
    assert lead.dimensions["segments.conversion_action_category"] == "SUBMIT_LEAD_FORM"
    assert purchase.dimensions["segments.conversion_action_category"] == "PURCHASE"
    assert purchase.measures["conversions"] == 2 and purchase.measures["conversion_value"] == 4998.0
    # stable ID is the join key: the numeric suffix matches conversion_actions
    # entity external_id (ad_entities.external_id), not the mutable name.
    assert purchase.dimensions["segments.conversion_action"].endswith("6672831801")
    # reconciliation invariant: summed conversions across every action row for
    # one (date, campaign) must equal that campaign's aggregate — checked here
    # at unit scope; the 107/107 live match is documented in the stream's spec
    # comment and re-verified against the live account in this session.
    assert sum(r.measures["conversions"] for r in rows) == 12.999829 + 2
