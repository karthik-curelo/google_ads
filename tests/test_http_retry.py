import time

import httpx
import pytest

from app.connectors import errors as E
from app.connectors.http import HttpClient, RateLimiter, RetryPolicy


def test_retry_policy_jitter_and_retry_after_cap():
    p = RetryPolicy(base_delay=1.0, max_delay=60.0, max_retry_after=120.0)
    for attempt in range(6):
        d = p.delay_for(attempt)
        assert 0.0 <= d <= min(60.0, 1.0 * 2**attempt)
    assert p.delay_for(0, retry_after=999.0) == 120.0
    assert p.delay_for(0, retry_after=5.0) == 5.0


@pytest.mark.asyncio
async def test_rate_limiter_penalize_slows_acquire():
    rl = RateLimiter(rate_per_second=100.0, burst=1)
    await rl.acquire()  # drain the burst token
    rl.penalize(factor=50.0, duration=1.0)
    start = time.monotonic()
    await rl.acquire()
    assert time.monotonic() - start >= 0.15  # had to wait for a penalised refill


@pytest.mark.asyncio
async def test_http_client_retries_500_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(500, json={"error": "boom"})
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = HttpClient(
        retry=RetryPolicy(max_attempts=5, base_delay=0.01, max_delay=0.05, max_elapsed=5.0),
        client=httpx.AsyncClient(transport=transport),
        provider="test",
    )
    result = await client.get("https://example.test/data")
    assert result == {"ok": True}
    assert calls["n"] == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_http_client_does_not_retry_403():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": {"message": "nope"}})

    client = HttpClient(
        retry=RetryPolicy(max_attempts=5, base_delay=0.01),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        provider="test",
    )
    with pytest.raises(E.ConnectorError) as exc:
        await client.get("https://example.test/data")
    assert exc.value.code == E.ErrorCode.PERMISSION_ERROR
    assert exc.value.retryable is False
    await client.aclose()


@pytest.mark.asyncio
async def test_http_client_gives_up_after_max_attempts():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = HttpClient(
        retry=RetryPolicy(max_attempts=3, base_delay=0.01, max_delay=0.02, max_elapsed=5.0),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        provider="test",
    )
    with pytest.raises(E.ConnectorError) as exc:
        await client.get("https://example.test/x")
    assert exc.value.retryable is True
    assert client.stats.retries == 2
    await client.aclose()
