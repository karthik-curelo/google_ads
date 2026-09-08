import httpx
import pytest
import respx

from app.connectors import errors as E
from app.oauth.base import TokenSet, generate_state
from app.oauth.google import GoogleOAuthProvider
from app.oauth.meta import MetaOAuthProvider, classify_graph_error

G = {"client_id": "cid", "client_secret": "csecret", "redirect_uri": "https://app/cb"}


def test_state_is_unique_and_urlsafe():
    a, b = generate_state(), generate_state()
    assert a != b and len(a) > 30 and "/" not in a


def test_tokenset_expiry_skew():
    from datetime import UTC, datetime, timedelta

    assert TokenSet("x", expires_at=datetime.now(UTC) + timedelta(seconds=60)).expired is True
    assert TokenSet("x", expires_at=datetime.now(UTC) + timedelta(seconds=600)).expired is False
    assert TokenSet("x").expired is False


@respx.mock
async def test_google_exchange_code_happy_path():
    respx.post("https://oauth2.googleapis.com/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "ya29.new",
                "refresh_token": "1//refresh",
                "expires_in": 3599,
                "scope": "openid email https://www.googleapis.com/auth/analytics.readonly",
            },
        )
    )
    token = await GoogleOAuthProvider(**G).exchange_code("auth-code")
    assert token.access_token == "ya29.new" and token.refresh_token == "1//refresh"
    assert "openid" in token.scopes


@respx.mock
async def test_google_exchange_code_without_refresh_token_is_auth_error():
    respx.post("https://oauth2.googleapis.com/token").mock(
        return_value=httpx.Response(200, json={"access_token": "ya29.x", "expires_in": 3599})
    )
    with pytest.raises(E.ConnectorError) as exc:
        await GoogleOAuthProvider(**G).exchange_code("auth-code")
    assert exc.value.code == E.ErrorCode.AUTHENTICATION_ERROR


@respx.mock
async def test_google_invalid_grant_is_translated():
    respx.post("https://oauth2.googleapis.com/token").mock(
        return_value=httpx.Response(400, json={"error": "invalid_grant"})
    )
    with pytest.raises(E.ConnectorError) as exc:
        await GoogleOAuthProvider(**G).refresh("stale-refresh")
    assert exc.value.code == E.ErrorCode.AUTHENTICATION_ERROR
    assert exc.value.retryable is False


@respx.mock
async def test_google_refresh_keeps_old_refresh_token():
    respx.post("https://oauth2.googleapis.com/token").mock(
        return_value=httpx.Response(200, json={"access_token": "ya29.rotated", "expires_in": 3599})
    )
    token = await GoogleOAuthProvider(**G).refresh("1//keepme")
    assert token.access_token == "ya29.rotated" and token.refresh_token == "1//keepme"


@respx.mock
async def test_meta_exchange_swaps_for_long_lived_token():
    route = respx.get(url__regex=r"https://graph\.facebook\.com/.*/oauth/access_token")
    route.side_effect = [
        httpx.Response(200, json={"access_token": "EAAshort"}),
        httpx.Response(200, json={"access_token": "EAAlong", "expires_in": 5184000}),
    ]
    respx.get(url__regex=r"https://graph\.facebook\.com/.*/debug_token").mock(
        return_value=httpx.Response(200, json={"data": {"scopes": ["ads_read"]}})
    )
    token = await MetaOAuthProvider(**G, api_version="v26.0").exchange_code("code")
    assert token.access_token == "EAAlong" and token.refresh_token is None
    assert token.scopes == ["ads_read"]


def test_classify_graph_error_maps_codes():
    assert (
        classify_graph_error({"error": {"code": 190, "message": "expired"}}, 400).code
        == E.ErrorCode.AUTHENTICATION_ERROR
    )
    assert (
        classify_graph_error({"error": {"code": 4, "message": "slow down"}}, 400).code
        == E.ErrorCode.RATE_LIMIT_ERROR
    )
    assert classify_graph_error({}, 400) is None
