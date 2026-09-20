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

import re
from typing import Any

import httpx

from app.connectors import errors as E
from app.connectors.base import AuthType, BaseConnector, HealthReport, HealthStatus
from app.connectors.http import HttpClient, RetryPolicy, shared_rate_limiter

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
        self._rate_per_second = float(ps.get("leadsquared_rate_per_second") or 0.5)
        self._burst = int(ps.get("leadsquared_burst") or 2)

    def _build_http_client(self) -> HttpClient:
        return HttpClient(
            timeout=float(self.ctx.provider_settings.get("http_timeout_seconds", 120.0)),
            retry=RetryPolicy(max_attempts=5, base_delay=2.0, max_delay=90.0, max_elapsed=300.0),
            # One bucket per LeadSquared account (host + key), shared by every client
            # in the process: the API budget belongs to the account, so N
            # concurrent connections/streams must draw on the same allowance.
            rate_limiter=shared_rate_limiter(
                ("leadsquared", self._host, self._access_key), self._rate_per_second, self._burst
            ),
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
        return classify_lsq_response(response, provider=self.provider, connector_id=self.connector_id)


# LeadSquared reports EVERY application-level failure as HTTP 500 with a body like
#   {"Status": "Error", "ExceptionType": "MXInvalidInputException", "ExceptionMessage": "..."}
# (live-verified: bad PageSize, bad date, missing ActivityEvent, unknown activity
# id are all 500s; only bad credentials (401) and a bad path (404) use other
# codes). A 500 therefore does NOT mean "the server is having a bad moment" — an
# MX*Exception is a deterministic answer to that exact request, and repeating the
# request repeats the answer. Those are non-retryable. A 500 with no such body
# (HTML error page, empty) is still treated as transient infrastructure failure.
_AUTH_HINT = re.compile(r"access ?key|secret ?key|invalid access|access details", re.I)
_THROTTLE_HINT = re.compile(
    r"throttl|rate.?limit|too many (calls|requests)|exceeded (the )?(api )?limit", re.I
)
_TRANSIENT_HINT = re.compile(
    r"timeout|timed out|deadlock|temporar|try again|unavailable|connection (reset|closed)", re.I
)
_NOT_FOUND_TYPES = re.compile(r"^MXUnknown\w*Exception$")


def classify_lsq_response(
    response: httpx.Response, *, provider: str = "leadsquared", connector_id: str = "leadsquared"
) -> E.ConnectorError | None:
    """Map a LeadSquared error response to a typed error, or None to fall through to
    the generic status-code classifier (which retries 5xx/408/429)."""
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
    common = {
        "provider": provider,
        "connector_id": connector_id,
        "http_status": status,
        "technical_details": {"exception_type": exc_type, "message": message[:300]},
    }

    if status in (401, 403) or _AUTH_HINT.search(message) or "AccessDetails" in exc_type:
        return E.authentication_error(
            f"LeadSquared rejected the access key / secret key: {message}", **common
        )
    if status == 429 or _THROTTLE_HINT.search(f"{exc_type} {message}"):
        return E.rate_limit_error(f"LeadSquared is rate limiting requests: {message}", **common)
    if not exc_type.startswith("MX"):
        return None  # not an application exception -> generic (transient) handling
    if _TRANSIENT_HINT.search(message):
        return E.provider_unavailable(f"LeadSquared reported a transient error: {message}", **common)
    if _NOT_FOUND_TYPES.match(exc_type):
        return E.resource_not_found(f"LeadSquared: {message}", **common)
    # Any other MX*Exception is the API deterministically rejecting this request.
    return E.invalid_configuration(f"LeadSquared rejected the request ({exc_type}): {message}", **common)
