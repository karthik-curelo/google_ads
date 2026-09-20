"""Shared base for the three Google connectors (§22, §24).

GA4, Search Console and Google Ads differ only in endpoints and stream specs —
the OAuth identity, the bearer-header wiring, the error vocabulary and the HTTP
policy are identical, so they live here once. A fourth Google connector is a
subclass plus a STREAMS list.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from app.connectors import errors as E
from app.connectors.base import AuthType, BaseConnector, HealthReport, HealthStatus
from app.connectors.http import HttpClient, RetryPolicy, shared_rate_limiter

# google.rpc.Code name -> our typed error. Google returns these as the string
# `status` on the error body regardless of transport code.
_STATUS_MAP = {
    "UNAUTHENTICATED": E.authentication_error,
    "PERMISSION_DENIED": E.permission_error,
    "RESOURCE_EXHAUSTED": E.rate_limit_error,
    "NOT_FOUND": E.resource_not_found,
    "INVALID_ARGUMENT": E.invalid_configuration,
    "FAILED_PRECONDITION": E.invalid_configuration,
    "UNAVAILABLE": E.provider_unavailable,
    "INTERNAL": E.provider_unavailable,
    "DEADLINE_EXCEEDED": E.timeout_error,
}


class GoogleConnector(BaseConnector):
    provider = "google"
    auth_type = AuthType.OAUTH2
    category = "marketing"
    # Conservative default; a large GA4 property or Ads account can spike, and the
    # on_response quota hook tightens this further when the provider warns us.
    rate_per_second: float = 5.0
    burst: int = 10

    async def _auth_headers(self) -> Mapping[str, str]:
        token = await self.ctx.token_provider.access_token()
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def _build_http_client(self) -> HttpClient:
        return HttpClient(
            timeout=float(self.ctx.provider_settings.get("http_timeout_seconds", 120.0)),
            retry=RetryPolicy(max_attempts=5, base_delay=1.0, max_delay=60.0, max_elapsed=300.0),
            # One bucket per Google API in the process: connections of the same API (two GA4
            # properties syncing at once) must share its quota, not each spend a full one.
            rate_limiter=shared_rate_limiter(("google", self.connector_id), self.rate_per_second, self.burst),
            max_concurrency=4,
            provider=self.provider,
            connector_id=self.connector_id,
            classify=self._classify,
            on_response=self._on_response,
            auth_header_provider=self._auth_headers,
        )

    # --- error classification ------------------------------------------------
    def _classify(self, response: httpx.Response) -> E.ConnectorError | None:
        if response.is_success:
            return None
        try:
            body = response.json()
        except ValueError:
            return None
        error = body.get("error") if isinstance(body, dict) else None
        if not isinstance(error, dict):
            return None
        status = error.get("status")
        message = error.get("message") or f"Google API error {response.status_code}"
        details = {
            "status": status,
            "http_status": response.status_code,
            "reason": _first_reason(error),
        }
        factory = _STATUS_MAP.get(str(status)) if status else None
        if factory is None:
            return None
        return factory(
            f"{message}",
            provider=self.provider,
            connector_id=self.connector_id,
            http_status=response.status_code,
            technical_details=details,
        )

    def _on_response(self, response: httpx.Response) -> None:
        """Hook for subclasses that read provider quota headers/bodies."""


def _first_reason(error: dict[str, Any]) -> str | None:
    errs = error.get("errors")
    if isinstance(errs, list) and errs and isinstance(errs[0], dict):
        return errs[0].get("reason")
    return None


_HEALTH_MAP = {
    E.ErrorCode.AUTHENTICATION_ERROR: HealthStatus.NEEDS_REAUTH,
    E.ErrorCode.PERMISSION_ERROR: HealthStatus.PERMISSION_DENIED,
    E.ErrorCode.RATE_LIMIT_ERROR: HealthStatus.RATE_LIMITED,
    E.ErrorCode.QUOTA_EXCEEDED: HealthStatus.RATE_LIMITED,
    E.ErrorCode.INVALID_CONFIGURATION: HealthStatus.INVALID_CONFIGURATION,
    E.ErrorCode.PROVIDER_UNAVAILABLE: HealthStatus.PROVIDER_UNAVAILABLE,
    E.ErrorCode.RESOURCE_NOT_FOUND: HealthStatus.INVALID_CONFIGURATION,
    E.ErrorCode.NOT_SUPPORTED: HealthStatus.NOT_SUPPORTED,
}


def health_from_error(exc: E.ConnectorError) -> HealthReport:
    """Turn a classified ConnectorError into the §15 health vocabulary."""
    return HealthReport(
        status=_HEALTH_MAP.get(exc.code, HealthStatus.UNKNOWN),
        message=exc.message,
        error=exc,
    )
