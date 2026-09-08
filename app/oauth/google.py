"""Google OAuth 2.0 — authorization code flow with incremental authorisation.

Used by Google Analytics 4, Search Console and Google Ads from a single consent
identity. Three details matter and are easy to get wrong:

`access_type=offline` + `prompt=consent`
    Google only returns a refresh token on the *first* authorisation of a
    client/user pair. Without `prompt=consent` a returning user yields no refresh
    token, the connection works for an hour and then dies — the classic
    "it worked yesterday" OAuth bug.

`include_granted_scopes=true`
    Incremental authorisation. Connecting Search Console after Analytics asks
    only for the Search Console scope and returns a token valid for both, so the
    user is never asked to re-grant what they already gave.

`invalid_grant`
    The one error that must never be retried: the user revoked access, or the
    refresh token was expired/superseded. Retrying cannot help; the connection
    needs re-authorisation.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode

from app.connectors import errors as E
from app.core.logging import get_logger
from app.oauth.base import IdentityInfo, OAuthProvider, TokenSet, expires_at_from

logger = get_logger(__name__)

AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
REVOKE_ENDPOINT = "https://oauth2.googleapis.com/revoke"
USERINFO_ENDPOINT = "https://www.googleapis.com/oauth2/v3/userinfo"

# Per-service scopes, all read-only. Referenced by the connectors so a connector
# and its scope requirement never drift apart.
SCOPE_ANALYTICS_READONLY = "https://www.googleapis.com/auth/analytics.readonly"
SCOPE_SEARCH_CONSOLE_READONLY = "https://www.googleapis.com/auth/webmasters.readonly"
SCOPE_ADWORDS = "https://www.googleapis.com/auth/adwords"


class GoogleOAuthProvider(OAuthProvider):
    provider = "google"
    base_scopes = ("openid", "email", "profile")
    supports_refresh = True

    def authorization_url(
        self,
        *,
        state: str,
        scopes: Sequence[str],
        login_hint: str | None = None,
        force_consent: bool = True,
        **_: Any,
    ) -> str:
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.resolve_scopes(scopes)),
            "state": state,
            "access_type": "offline",
            "include_granted_scopes": "true",
        }
        if force_consent:
            params["prompt"] = "consent"
        if login_hint:
            # Pre-selects the account when reconnecting a specific identity.
            params["login_hint"] = login_hint
        return f"{AUTH_ENDPOINT}?{urlencode(params)}"

    async def exchange_code(self, code: str) -> TokenSet:
        payload = await self._token_request(
            {
                "code": code,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code",
            }
        )
        token = TokenSet(
            access_token=payload["access_token"],
            refresh_token=payload.get("refresh_token"),
            expires_at=expires_at_from(payload.get("expires_in")),
            scopes=(payload.get("scope") or "").split(),
        )
        if not token.refresh_token:
            # Recoverable, and worth saying precisely: without offline access the
            # connection cannot sync unattended.
            raise E.authentication_error(
                "Google did not return a refresh token. Offline access is required for scheduled syncs.",
                provider=self.provider,
                user_action=(
                    "Remove this app's access at myaccount.google.com/permissions, then "
                    "connect again and approve the consent screen."
                ),
            )
        return token

    async def refresh(self, refresh_token: str) -> TokenSet:
        payload = await self._token_request(
            {
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
            }
        )
        return TokenSet(
            access_token=payload["access_token"],
            # Google normally omits it on refresh; keep the old one.
            refresh_token=payload.get("refresh_token") or refresh_token,
            expires_at=expires_at_from(payload.get("expires_in")),
            scopes=(payload.get("scope") or "").split(),
        )

    async def fetch_identity(self, access_token: str) -> IdentityInfo:
        async with self._client() as client:
            payload = await client.get(USERINFO_ENDPOINT, headers={"Authorization": f"Bearer {access_token}"})
        subject = payload.get("sub") or payload.get("email")
        if not subject:
            raise E.api_schema_error(
                "Google userinfo response contained no account identifier.",
                provider=self.provider,
            )
        return IdentityInfo(
            external_account_id=str(subject),
            email=payload.get("email"),
            display_name=payload.get("name") or payload.get("email"),
            raw={k: v for k, v in payload.items() if k in {"sub", "email", "name", "picture"}},
        )

    async def revoke(self, token_set: TokenSet) -> bool:
        # Revoking either token of a pair revokes both; prefer the refresh token
        # since it is the durable grant.
        token = token_set.refresh_token or token_set.access_token
        if not token:
            return False
        try:
            async with self._client() as client:
                await client.request("POST", REVOKE_ENDPOINT, data={"token": token}, expected_json=False)
            return True
        except E.ConnectorError as exc:
            # An already-revoked grant returns 400. Disconnect must still succeed
            # locally — otherwise a user cannot remove a connection whose access
            # they already withdrew at Google.
            logger.info("Google token revocation returned %s (treating as revoked)", exc.code)
            return False

    async def _token_request(self, data: dict[str, str]) -> dict[str, Any]:
        async with self._client() as client:
            try:
                payload = await client.request(
                    "POST",
                    TOKEN_ENDPOINT,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
            except E.ConnectorError as exc:
                raise self._translate(exc) from exc
        if "access_token" not in payload:
            raise E.authentication_error(
                "Google's token endpoint returned no access token.", provider=self.provider
            )
        return payload

    def _translate(self, exc: E.ConnectorError) -> E.ConnectorError:
        """Turn Google's OAuth error bodies into the right typed failure."""
        body = str(exc.technical_details.get("body_snippet", ""))
        if "invalid_grant" in body:
            return E.authentication_error(
                "Google rejected the stored authorisation (invalid_grant). Access was "
                "revoked or the refresh token is no longer valid.",
                provider=self.provider,
            )
        if "invalid_client" in body:
            return E.invalid_configuration(
                "Google rejected the OAuth client credentials (invalid_client).",
                provider=self.provider,
                user_action="Check GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET.",
            )
        if "redirect_uri_mismatch" in body:
            return E.invalid_configuration(
                "Google rejected the redirect URI (redirect_uri_mismatch).",
                provider=self.provider,
                user_action=(
                    "Add this exact URI to the OAuth client's authorised redirect URIs in "
                    "Google Cloud Console: " + self.redirect_uri
                ),
            )
        if "invalid_scope" in body:
            return E.invalid_configuration(
                "Google rejected one of the requested scopes.",
                provider=self.provider,
                user_action="Enable the corresponding API in your Google Cloud project.",
            )
        return exc
