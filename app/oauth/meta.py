"""Meta OAuth 2.0 (Facebook Login) — Meta Ads and Instagram Insights.

Meta differs from Google in a way the platform must model rather than paper over:
**there are no refresh tokens.** The short-lived token from the code exchange is
swapped for a long-lived user token (~60 days) via the `fb_exchange_token` grant,
and when that expires a human must re-authorise. So `supports_refresh` is False
and expiry drives the connection to `needs_reauth` instead of a doomed refresh
attempt.

`appsecret_proof` is sent on every call — an HMAC of the access token keyed by the
app secret. Meta requires it for server-side calls when "Require App Secret" is
on, and it makes a stolen token useless without the secret.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode

from app.connectors import errors as E
from app.core.logging import get_logger
from app.oauth.base import IdentityInfo, OAuthProvider, TokenSet, expires_at_from

logger = get_logger(__name__)

# Read-only scopes. `ads_read` is the reporting scope; `ads_management` would also
# permit *changing* campaigns and is deliberately not requested.
SCOPE_ADS_READ = "ads_read"
SCOPE_BUSINESS_MANAGEMENT = "business_management"
SCOPE_READ_INSIGHTS = "read_insights"
SCOPE_PAGES_SHOW_LIST = "pages_show_list"
SCOPE_PAGES_READ_ENGAGEMENT = "pages_read_engagement"
SCOPE_INSTAGRAM_BASIC = "instagram_basic"
SCOPE_INSTAGRAM_MANAGE_INSIGHTS = "instagram_manage_insights"

# Meta error codes that mean "the user must act", not "try again".
_AUTH_ERROR_CODES = {102, 190, 458, 459, 460, 463, 464, 467}
_PERMISSION_ERROR_CODES = {10, 200, 272, 294, 299}
# 4/17/32/613 throttle the whole app; 80004 is per AD ACCOUNT ("There have been too many
# calls to this ad-account. Wait a bit and try again.") — live, two ad accounts syncing
# back to back (2026-09-24). Unclassified, it fell through to the generic HTTP-400
# handler as invalid_configuration (retryable=False, "correct this connection's
# configuration"), which failed the stream outright instead of retrying with backoff.
_RATE_LIMIT_CODES = {4, 17, 32, 613, 80004}
_TRANSIENT_CODES = {1, 2}


class MetaOAuthProvider(OAuthProvider):
    provider = "meta"
    base_scopes = ("public_profile",)
    supports_refresh = False

    def __init__(self, *, client_id: str, client_secret: str, redirect_uri: str, api_version: str = "v26.0"):
        super().__init__(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)
        self.api_version = api_version

    @property
    def graph_base(self) -> str:
        return f"https://graph.facebook.com/{self.api_version}"

    def appsecret_proof(self, access_token: str) -> str:
        return hmac.new(self.client_secret.encode(), access_token.encode(), hashlib.sha256).hexdigest()

    def auth_params(self, access_token: str) -> dict[str, str]:
        return {"access_token": access_token, "appsecret_proof": self.appsecret_proof(access_token)}

    def authorization_url(
        self, *, state: str, scopes: Sequence[str], force_consent: bool = False, **_: Any
    ) -> str:
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "state": state,
            "response_type": "code",
            "scope": ",".join(self.resolve_scopes(scopes)),
        }
        if force_consent:
            # Re-prompts for scopes the user previously declined, which is the
            # only way to recover from a partial grant.
            params["auth_type"] = "rerequest"
        return f"https://www.facebook.com/{self.api_version}/dialog/oauth?{urlencode(params)}"

    async def exchange_code(self, code: str) -> TokenSet:
        async with self._client() as client:
            try:
                short_lived = await client.get(
                    f"{self.graph_base}/oauth/access_token",
                    params={
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "redirect_uri": self.redirect_uri,
                        "code": code,
                    },
                )
            except E.ConnectorError as exc:
                raise self._translate(exc) from exc

            short_token = short_lived.get("access_token")
            if not short_token:
                raise E.authentication_error(
                    "Meta's token endpoint returned no access token.", provider=self.provider
                )

            # Immediately upgrade to a long-lived token — the short-lived one
            # lasts about an hour, which is useless for scheduled syncs.
            try:
                long_lived = await client.get(
                    f"{self.graph_base}/oauth/access_token",
                    params={
                        "grant_type": "fb_exchange_token",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "fb_exchange_token": short_token,
                    },
                )
            except E.ConnectorError as exc:
                logger.warning("Meta long-lived token exchange failed: %s", exc.code)
                long_lived = short_lived

            access_token = long_lived.get("access_token", short_token)
            token = TokenSet(
                access_token=access_token,
                refresh_token=None,  # Meta has none, by design.
                expires_at=expires_at_from(long_lived.get("expires_in")),
            )
            token.scopes = await self._granted_scopes(client, access_token)
            return token

    async def refresh(self, refresh_token: str) -> TokenSet:
        raise E.authentication_error(
            "Meta does not support refresh tokens. This connection must be reconnected.",
            provider=self.provider,
            user_action="Click Reconnect and sign in to Meta again.",
        )

    async def fetch_identity(self, access_token: str) -> IdentityInfo:
        async with self._client() as client:
            payload = await client.get(
                f"{self.graph_base}/me",
                params={"fields": "id,name,email", **self.auth_params(access_token)},
            )
        user_id = payload.get("id")
        if not user_id:
            raise E.api_schema_error("Meta /me response contained no user id.", provider=self.provider)
        return IdentityInfo(
            external_account_id=str(user_id),
            email=payload.get("email"),
            display_name=payload.get("name"),
            raw={"id": user_id, "name": payload.get("name")},
        )

    async def revoke(self, token_set: TokenSet) -> bool:
        if not token_set.access_token:
            return False
        try:
            async with self._client() as client:
                await client.request(
                    "DELETE",
                    f"{self.graph_base}/me/permissions",
                    params=self.auth_params(token_set.access_token),
                )
            return True
        except E.ConnectorError as exc:
            logger.info("Meta permission revocation returned %s", exc.code)
            return False

    async def _granted_scopes(self, client: Any, access_token: str) -> list[str]:
        """Read back what the user actually granted.

        Meta lets a user decline individual permissions while still completing the
        flow, so the granted set is not the requested set. Knowing the difference
        is what lets a connection say "Instagram insights unavailable: you did not
        grant instagram_manage_insights" instead of failing later.
        """
        try:
            payload = await client.get(
                f"{self.graph_base}/debug_token",
                params={
                    "input_token": access_token,
                    "access_token": f"{self.client_id}|{self.client_secret}",
                },
            )
        except E.ConnectorError as exc:
            logger.warning("Meta debug_token failed, scopes unknown: %s", exc.code)
            return []
        return list((payload.get("data") or {}).get("scopes") or [])

    async def inspect_token(self, access_token: str) -> dict[str, Any]:
        """Full debug_token payload — expiry, scopes, validity."""
        async with self._client() as client:
            payload = await client.get(
                f"{self.graph_base}/debug_token",
                params={
                    "input_token": access_token,
                    "access_token": f"{self.client_id}|{self.client_secret}",
                },
            )
        return (payload.get("data") or {}) if isinstance(payload, dict) else {}

    def _translate(self, exc: E.ConnectorError) -> E.ConnectorError:
        body = str(exc.technical_details.get("body_snippet", ""))
        if "redirect_uri" in body and "match" in body:
            return E.invalid_configuration(
                "Meta rejected the redirect URI.",
                provider=self.provider,
                user_action=(
                    "Add this exact URI to Valid OAuth Redirect URIs in your Meta app's "
                    "Facebook Login settings: " + self.redirect_uri
                ),
            )
        if "Invalid verification code" in body or "authorization code has been used" in body:
            return E.authentication_error(
                "Meta rejected the authorisation code (expired or already used).",
                provider=self.provider,
                user_action="Start the connection flow again.",
            )
        return exc


def classify_graph_error(payload: dict[str, Any], http_status: int) -> E.ConnectorError | None:
    """Map a Graph API error body to a typed failure.

    Shared by both Meta connectors, because Graph reports a token problem as
    HTTP 400 with `code: 190` rather than a 401 — generic status-code handling
    would classify an expired token as a bad request and never prompt a reconnect.
    """
    error = (payload or {}).get("error") or {}
    if not error:
        return None
    code = error.get("code")
    subcode = error.get("error_subcode")
    message = error.get("message") or "Meta Graph API error"
    details = {
        "graph_code": code,
        "graph_subcode": subcode,
        "type": error.get("type"),
        "fbtrace_id": error.get("fbtrace_id"),
    }

    if code in _AUTH_ERROR_CODES:
        return E.authentication_error(
            f"Meta rejected the access token: {message}",
            provider="meta",
            http_status=http_status,
            technical_details=details,
        )
    if code in _PERMISSION_ERROR_CODES:
        return E.permission_error(
            f"Meta denied access: {message}",
            provider="meta",
            http_status=http_status,
            technical_details=details,
        )
    if code in _RATE_LIMIT_CODES:
        return E.rate_limit_error(
            f"Meta is throttling this app: {message}",
            provider="meta",
            http_status=http_status,
            technical_details=details,
        )
    if code in _TRANSIENT_CODES:
        return E.provider_unavailable(
            f"Meta reported a temporary problem: {message}",
            provider="meta",
            http_status=http_status,
            technical_details=details,
        )
    if code == 100:
        return E.invalid_configuration(
            f"Meta rejected the request parameters: {message}",
            provider="meta",
            http_status=http_status,
            technical_details=details,
        )
    if code == 803:
        return E.resource_not_found(
            f"Meta could not find the requested object: {message}",
            provider="meta",
            http_status=http_status,
            technical_details=details,
        )
    return None
