"""Shared async HTTP layer: retry, exponential backoff with jitter, rate limiting.

Every connector goes through this, which is the point — §14's retry policy is
implemented once and five providers inherit it. Provider-specific knowledge
enters through two injection points rather than through forks of this file:

  `classify`  maps a provider's error response to a ConnectorError, so a Google
              403 "insufficient scope" and a Meta 190 "token expired" both come
              out as the same typed, non-retryable auth failure.
  `on_response`  lets a connector read quota headers and slow *itself* down.
              GA4 returns `propertyQuota`, Meta returns
              `X-Business-Use-Case-Usage` — backing off before being throttled is
              considerably cheaper than being throttled.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from app.connectors import errors as E
from app.core.logging import get_logger

logger = get_logger(__name__)

# Statuses worth trying again. 409 is absent deliberately — a conflict is a
# state problem, and retrying it just conflicts again.
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0
    # A ceiling on *total* time spent retrying one request, so a run cannot be
    # held open indefinitely by a provider that is down (§14 "maximum retry
    # duration", §37 "must not hang").
    max_elapsed: float = 300.0
    # Cap on how long we will obey a provider's Retry-After. Meta occasionally
    # asks for an hour; that belongs to the next scheduled run, not this one.
    max_retry_after: float = 120.0

    def delay_for(self, attempt: int, retry_after: float | None = None) -> float:
        if retry_after is not None:
            return min(retry_after, self.max_retry_after)
        # Full jitter (AWS's recommendation): uniform over [0, backoff]. Equal
        # jitter still synchronises retries when many streams fail at once.
        ceiling = min(self.max_delay, self.base_delay * (2**attempt))
        return random.uniform(0, ceiling)


class RateLimiter:
    """Token bucket with an adaptive penalty, shared per connector instance.

    `penalize` exists so a connector that sees "you have 8% of your hourly quota
    left" can stretch its own interval instead of sprinting into a 429.
    """

    __slots__ = ("_rate", "_burst", "_tokens", "_updated", "_lock", "_penalty_until", "_penalty_factor")

    def __init__(self, rate_per_second: float = 5.0, burst: int = 5):
        self._rate = max(rate_per_second, 0.01)
        self._burst = max(burst, 1)
        self._tokens = float(self._burst)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()
        self._penalty_until = 0.0
        self._penalty_factor = 1.0

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                rate = self._rate
                if now < self._penalty_until:
                    rate = self._rate / self._penalty_factor
                self._tokens = min(self._burst, self._tokens + (now - self._updated) * rate)
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / rate)

    def penalize(self, factor: float, duration: float) -> None:
        """Divide throughput by `factor` for `duration` seconds."""
        self._penalty_factor = max(1.0, factor)
        self._penalty_until = max(self._penalty_until, time.monotonic() + duration)


@dataclass
class HttpStats:
    calls: int = 0
    retries: int = 0
    rate_limit_waits: float = 0.0
    bytes_received: int = 0
    statuses: dict[int, int] = field(default_factory=dict)

    def record(self, status: int, size: int) -> None:
        self.calls += 1
        self.bytes_received += size
        self.statuses[status] = self.statuses.get(status, 0) + 1


ClassifyFn = Callable[[httpx.Response], E.ConnectorError | None]
OnResponseFn = Callable[[httpx.Response], None]


class HttpClient:
    """Thin, retrying, rate-limited wrapper over one httpx.AsyncClient."""

    def __init__(
        self,
        *,
        timeout: float = 120.0,
        retry: RetryPolicy | None = None,
        rate_limiter: RateLimiter | None = None,
        max_concurrency: int = 5,
        provider: str | None = None,
        connector_id: str | None = None,
        classify: ClassifyFn | None = None,
        on_response: OnResponseFn | None = None,
        auth_header_provider: Callable[[], Awaitable[Mapping[str, str]]] | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        self.retry = retry or RetryPolicy()
        self.limiter = rate_limiter or RateLimiter()
        self.stats = HttpStats()
        self.provider = provider
        self.connector_id = connector_id
        self._classify = classify
        self._on_response = on_response
        self._auth_header_provider = auth_header_provider
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=min(30.0, timeout)),
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=max_concurrency * 2, max_keepalive_connections=max_concurrency
            ),
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HttpClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    def _err(self, factory, message: str, **kw: Any) -> E.ConnectorError:
        return factory(message, provider=self.provider, connector_id=self.connector_id, **kw)

    @staticmethod
    def _retry_after(response: httpx.Response) -> float | None:
        raw = response.headers.get("retry-after")
        if not raw:
            return None
        try:
            return max(0.0, float(raw))
        except ValueError:
            # HTTP-date form. Providers rarely use it; parse rather than guess.
            from email.utils import parsedate_to_datetime

            try:
                target = parsedate_to_datetime(raw)
            except (TypeError, ValueError):
                return None
            from datetime import UTC, datetime

            return max(0.0, (target - datetime.now(UTC)).total_seconds())

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        data: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        expected_json: bool = True,
    ) -> Any:
        """Perform a request with retries. Returns parsed JSON (or raw text).

        `data` is form-encoded — OAuth token endpoints require it.
        """
        deadline = time.monotonic() + self.retry.max_elapsed
        last_error: E.ConnectorError | None = None

        for attempt in range(self.retry.max_attempts):
            request_headers: dict[str, str] = dict(headers or {})
            if self._auth_header_provider is not None:
                # Re-resolved every attempt so a token refreshed mid-retry is
                # picked up rather than replaying a stale Authorization header.
                request_headers.update(await self._auth_header_provider())

            wait_started = time.monotonic()
            await self.limiter.acquire()
            self.stats.rate_limit_waits += time.monotonic() - wait_started

            try:
                async with self._semaphore:
                    response = await self._client.request(
                        method, url, params=params, json=json, data=data, headers=request_headers
                    )
            except asyncio.CancelledError:
                # Cancellation is not a failure to retry — propagate at once so
                # pause/timeout actually stops work.
                raise
            except httpx.TimeoutException as exc:
                last_error = self._err(
                    E.timeout_error, f"Request to {url} timed out.", technical_details={"url": url}
                )
                logger.warning("Timeout on %s %s (attempt %d): %s", method, url, attempt + 1, exc)
            except httpx.HTTPError as exc:
                last_error = self._err(
                    E.network_error, f"Network error calling {url}: {exc}", technical_details={"url": url}
                )
                logger.warning("Network error on %s %s (attempt %d): %s", method, url, attempt + 1, exc)
            else:
                self.stats.record(response.status_code, len(response.content))
                if self._on_response is not None:
                    try:
                        self._on_response(response)
                    except Exception:  # pragma: no cover - never let telemetry break a sync
                        logger.debug("on_response hook failed", exc_info=True)

                if response.is_success:
                    if not expected_json:
                        return response.text
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise self._err(
                            E.api_schema_error,
                            f"{self.provider or 'Provider'} returned a non-JSON success response.",
                            technical_details={
                                "url": url,
                                "content_type": response.headers.get("content-type"),
                            },
                        ) from exc

                # Give the connector first refusal on classification.
                error = self._classify(response) if self._classify else None
                if error is None:
                    error = self._classify_generic(response, url)
                error.provider = error.provider or self.provider
                error.connector_id = error.connector_id or self.connector_id
                if error.retry_after_seconds is None:
                    error.retry_after_seconds = self._retry_after(response)
                last_error = error

                if not error.retryable:
                    raise error

            # --- retry decision ------------------------------------------------
            assert last_error is not None
            if attempt >= self.retry.max_attempts - 1:
                break
            delay = self.retry.delay_for(attempt, last_error.retry_after_seconds)
            if time.monotonic() + delay > deadline:
                last_error.technical_details["gave_up_reason"] = (
                    f"retry budget of {self.retry.max_elapsed}s exhausted"
                )
                break
            self.stats.retries += 1
            logger.info(
                "Retrying %s %s in %.1fs (attempt %d/%d, %s)",
                method,
                url,
                delay,
                attempt + 1,
                self.retry.max_attempts,
                last_error.code,
            )
            await asyncio.sleep(delay)

        raise last_error or self._err(E.unknown_error, f"Request to {url} failed.")

    def _classify_generic(self, response: httpx.Response, url: str) -> E.ConnectorError:
        status = response.status_code
        # Bodies can contain tokens echoed back; truncate and let the logging
        # filter mask the remainder.
        snippet = response.text[:500]
        details = {"url": url, "status": status, "body_snippet": snippet}

        if status in (401,):
            return self._err(
                E.authentication_error,
                "The provider rejected the stored credentials.",
                http_status=status,
                technical_details=details,
            )
        if status == 403:
            return self._err(
                E.permission_error,
                "The provider denied access to this resource.",
                http_status=status,
                technical_details=details,
            )
        if status == 404:
            return self._err(
                E.resource_not_found,
                "The requested resource was not found.",
                http_status=status,
                technical_details=details,
            )
        if status == 429:
            return self._err(
                E.rate_limit_error,
                "The provider is rate limiting requests.",
                http_status=status,
                technical_details=details,
            )
        if status in (400, 422):
            return self._err(
                E.invalid_configuration,
                "The provider rejected the request as invalid.",
                http_status=status,
                technical_details=details,
            )
        if status in RETRYABLE_STATUSES:
            return self._err(
                E.provider_unavailable,
                f"The provider returned HTTP {status}.",
                http_status=status,
                technical_details=details,
            )
        return self._err(
            E.unknown_error,
            f"Unexpected HTTP {status} from provider.",
            http_status=status,
            technical_details=details,
        )

    # Convenience wrappers -------------------------------------------------
    async def get(self, url: str, **kw: Any) -> Any:
        return await self.request("GET", url, **kw)

    async def post(self, url: str, **kw: Any) -> Any:
        return await self.request("POST", url, **kw)
