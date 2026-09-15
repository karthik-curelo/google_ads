"""Shared HTTP/auth layer for the LeadSquared connector.

Auth is a static `accessKey`/`secretKey` query-param pair on every call, not
OAuth (`AuthType.API_KEY`) — LeadSquared has no per-user consent screen or
refresh token. The real credential comes from `ctx.provider_settings`
(env-configured, exactly like the Google Ads developer token or Meta app
secret), never from a stored/encrypted `OAuthIdentity` token.

Rate limiting is deliberately conservative and configurable rather than
assuming a plan tier (implementation instruction §11): LeadSquared's own
rate-limit page documents two tiers (Pro: 5 bulk calls/5s; Super: 10/5s) and
this session confirmed live that no endpoint exposes which tier an account is
on (docs/coverage/LSQ_VERIFICATION_2026-09-11.md §F) — so the default sits
below the *lower* of the two, and is raised only by explicit configuration
(`LEADSQUARED_RATE_PER_SECOND`), never by guessing.
"""

from __future__ import annotations

from typing import Any

import httpx

from app.connectors import errors as E
from app.connectors.base import AuthType, BaseConnector, HealthReport, HealthStatus
from app.connectors.http import HttpClient, RateLimiter, RetryPolicy

_HEALTH_MAP = {
    E.ErrorCode.AUTHENTICATION_ERROR: HealthStatus.NEEDS_REAUTH,
    E.ErrorCode.PERMISSION_ERROR: HealthStatus.PERMISSION_DENIED,
    E.ErrorCode.RATE_LIMIT_ERROR: HealthStatus.RATE_LIMITED,
    E.ErrorCode.INVALID_CONFIGURATION: HealthStatus.INVALID_CONFIGURATION,
    E.ErrorCode.PROVIDER_UNAVAILABLE: HealthStatus.PROVIDER_UNAVAILABLE,
    E.ErrorCode.RESOURCE_NOT_FOUND: HealthStatus.INVALID_CONFIGURATION,
    E.ErrorCode.NOT_SUPPORTED: HealthStatus.NOT_SUPPORTED,
}


def health_from_error(exc: E.ConnectorError) -> HealthReport:
    return HealthReport(
        status=_HEALTH_MAP.get(exc.code, HealthStatus.UNKNOWN), message=exc.message, error=exc
    )


class LeadSquaredConnector(BaseConnector):
    provider = "leadsquared"
    auth_type = AuthType.API_KEY
    category = "crm"

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        ps = ctx.provider_settings
        self._access_key = (ps.get("leadsquared_access_key") or "").strip()
        self._secret_key = (ps.get("leadsquared_secret_key") or "").strip()
        self._host = (ps.get("leadsquared_host") or "").strip().rstrip("/")
        self._rate_per_second = float(ps.get("leadsquared_rate_per_second") or 0.8)
        self._burst = int(ps.get("leadsquared_burst") or 2)

    def _build_http_client(self) -> HttpClient:
        return HttpClient(
            timeout=float(self.ctx.provider_settings.get("http_timeout_seconds", 120.0)),
            retry=RetryPolicy(max_attempts=5, base_delay=2.0, max_delay=90.0, max_elapsed=300.0),
            rate_limiter=RateLimiter(rate_per_second=self._rate_per_second, burst=self._burst),
            max_concurrency=2,
            provider=self.provider,
            connector_id=self.connector_id,
            classify=self._classify,
        )

    def _require_configured(self) -> None:
        if not (self._access_key and self._secret_key and self._host):
            raise E.invalid_configuration(
                "LeadSquared is not configured.",
                provider=self.provider,
                connector_id=self.connector_id,
                user_action=(
                    "Set LEADSQUARED_ACCESS_KEY, LEADSQUARED_SECRET_KEY and LEADSQUARED_HOST "
                    "(the account's regional API host, e.g. api-in21.leadsquared.com), then restart."
                ),
            )

    @property
    def _auth_params(self) -> dict[str, str]:
        return {"accessKey": self._access_key, "secretKey": self._secret_key}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self._require_configured()
        merged = {**self._auth_params, **(params or {})}
        return await self.http.get(f"{self._host}{path}", params=merged)

    async def _post(self, path: str, body: dict[str, Any], params: dict[str, Any] | None = None) -> Any:
        self._require_configured()
        merged = {**self._auth_params, **(params or {})}
        return await self.http.post(f"{self._host}{path}", params=merged, json=body)

    # --- error classification ------------------------------------------
    def _classify(self, response: httpx.Response) -> E.ConnectorError | None:
        """LeadSquared's error body shape, confirmed live this session:
        `{"Status": "Error", "ExceptionType": "MXInvalidInputException", ...}`
        on a 4xx/5xx (e.g. an over-the-1000-cap `PageSize` on the activity
        endpoint, or a malformed date range). Returning None here falls
        through to `HttpClient`'s generic status-code classifier.
        """
        if response.is_success:
            return None
        try:
            body = response.json()
        except ValueError:
            return None
        if not isinstance(body, dict):
            return None
        exc_type = str(body.get("ExceptionType") or "")
        message = str(body.get("ExceptionMessage") or body.get("Message") or "LeadSquared API error.")
        status = response.status_code
        details = {"exception_type": exc_type}

        if status in (401, 403) or "accesskey" in message.lower() or "secretkey" in message.lower():
            return E.authentication_error(
                f"LeadSquared rejected the access key / secret key: {message}",
                provider=self.provider,
                connector_id=self.connector_id,
                http_status=status,
                technical_details=details,
            )
        if status == 429 or "rate" in exc_type.lower() or "throttl" in message.lower():
            return E.rate_limit_error(
                f"LeadSquared is rate limiting requests: {message}",
                provider=self.provider,
                connector_id=self.connector_id,
                http_status=status,
                technical_details=details,
            )
        if "InvalidInput" in exc_type or "MandatoryFieldMissing" in exc_type or status in (400, 422):
            return E.invalid_configuration(
                f"LeadSquared rejected the request: {message}",
                provider=self.provider,
                connector_id=self.connector_id,
                http_status=status,
                technical_details=details,
            )
        return None
