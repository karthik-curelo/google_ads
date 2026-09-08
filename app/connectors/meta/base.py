"""Shared base for the Meta connectors (§22, §24).

Meta Ads and Instagram Insights share the Graph API host, the
`access_token` + `appsecret_proof` auth params (Meta uses query params, not a
bearer header), the Graph error vocabulary, and cursor pagination — so all of it
lives here once.

The `on_response` hook reads Meta's usage headers (`X-App-Usage`,
`X-Business-Use-Case-Usage`) and slows the client down before Meta starts
returning code 4/17/32 throttles (§14).
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.connectors import errors as E
from app.connectors.base import AuthType, BaseConnector, HealthReport, HealthStatus
from app.connectors.http import HttpClient, RateLimiter, RetryPolicy
from app.oauth.meta import classify_graph_error

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


class MetaConnector(BaseConnector):
    provider = "meta"
    auth_type = AuthType.OAUTH2
    category = "marketing"
    rate_per_second: float = 3.0
    burst: int = 6
    page_size: int = 200

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        ps = ctx.provider_settings
        self._api_version = (ps.get("meta_api_version") or "v26.0").strip()
        self._app_secret = (ps.get("meta_app_secret") or "").strip()
        self._usage_warned = False

    @property
    def graph_base(self) -> str:
        return f"https://graph.facebook.com/{self._api_version}"

    def _build_http_client(self) -> HttpClient:
        return HttpClient(
            timeout=float(self.ctx.provider_settings.get("http_timeout_seconds", 120.0)),
            retry=RetryPolicy(max_attempts=5, base_delay=2.0, max_delay=90.0, max_elapsed=300.0),
            rate_limiter=RateLimiter(rate_per_second=self.rate_per_second, burst=self.burst),
            max_concurrency=3,
            provider=self.provider,
            connector_id=self.connector_id,
            classify=self._classify,
            on_response=self._on_response,
        )

    # --- auth params -----------------------------------------------------
    async def _auth_params(self) -> dict[str, str]:
        token = await self.ctx.token_provider.access_token()
        params = {"access_token": token}
        if self._app_secret:
            params["appsecret_proof"] = hmac.new(
                self._app_secret.encode(), token.encode(), hashlib.sha256
            ).hexdigest()
        return params

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = path if path.startswith("http") else f"{self.graph_base}/{path.lstrip('/')}"
        merged = {**(params or {}), **await self._auth_params()}
        return await self.http.get(url, params=merged)

    async def _paged(
        self, path: str, params: dict[str, Any] | None = None, *, max_pages: int = 1000
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield each `data[]` item across Graph cursor pages."""
        params = {**(params or {}), "limit": self.page_size}
        payload = await self._get(path, params)
        pages = 0
        while True:
            for item in payload.get("data", []):
                yield item
            pages += 1
            next_url = (payload.get("paging") or {}).get("next")
            if not next_url or pages >= max_pages:
                break
            # `next` is a full URL already carrying access_token; reuse verbatim.
            payload = await self.http.get(next_url)

    # --- error classification ------------------------------------------
    def _classify(self, response: httpx.Response) -> E.ConnectorError | None:
        if response.is_success:
            return None
        try:
            body = response.json()
        except ValueError:
            return None
        if not isinstance(body, dict):
            return None
        err = classify_graph_error(body, response.status_code)
        if err is not None:
            err.connector_id = err.connector_id or self.connector_id
        return err

    def _on_response(self, response: httpx.Response) -> None:
        worst = 0.0
        for header in ("x-app-usage", "x-business-use-case-usage", "x-ad-account-usage"):
            raw = response.headers.get(header)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except ValueError:
                continue
            worst = max(worst, _max_usage(data))
        if worst >= 90:
            self.http.limiter.penalize(factor=5.0, duration=300.0)
            if not self._usage_warned:
                self.ctx.progress.note(f"Meta API usage at {worst:.0f}% — slowing requests")
                self._usage_warned = True


def _max_usage(data: Any) -> float:
    """Largest percentage across Meta's various usage-header shapes."""
    best = 0.0
    if isinstance(data, dict):
        for value in data.values():
            best = max(best, _max_usage(value))
    elif isinstance(data, list):
        for value in data:
            best = max(best, _max_usage(value))
    elif isinstance(data, (int, float)):
        best = float(data)
    return best
