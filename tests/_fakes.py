"""Shared test doubles."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import date

from app.connectors.base import (
    BaseConnector,
    ConnectorContext,
    HealthReport,
    HealthStatus,
    Record,
    ResourceDescriptor,
    StreamDefinition,
    StreamSlice,
)
from app.connectors.http import HttpClient
from app.connectors.validation import build_json_schema

# The warehouse splits fact rows into one table per source. The stub connector
# has no table of its own, so point it at the GA4 one (it has a `sessions`
# column, which is what the stub emits).
from app.models import GoogleAnalyticsPerformance as _StubPerf
from app.sync import writer as _writer

_writer.CONNECTOR_MODEL_MAP.setdefault("stub", _StubPerf)


class FakeTokenProvider:
    def __init__(self, token: str = "test-access-token", scopes: Sequence[str] = ()) -> None:
        self._token = token
        self._scopes = list(scopes)
        self.invalidated = 0

    async def access_token(self) -> str:
        return self._token

    async def invalidate(self) -> None:
        self.invalidated += 1

    @property
    def scopes(self) -> Sequence[str]:
        return self._scopes

    @property
    def account_label(self) -> str | None:
        return "tester@example.com"


def make_ctx(**kw) -> ConnectorContext:
    kw.setdefault("token_provider", FakeTokenProvider())
    kw.setdefault("provider_settings", {"http_timeout_seconds": 30.0})
    return ConnectorContext(**kw)


class StubConnector(BaseConnector):
    """A connector with no network — yields a fixed number of rows per day."""

    connector_id = "stub"
    name = "Stub"
    provider = "stub"
    STREAMS = [
        StreamDefinition(
            name="daily",
            description="stub daily",
            json_schema=build_json_schema(["date", "channel"], ["sessions"]),
            primary_key=["date", "channel"],
            slice_days=3,
        )
    ]

    rows_per_day = 2
    fail_with: Exception | None = None

    def _build_http_client(self) -> HttpClient:  # pragma: no cover - never used
        return HttpClient(provider="stub")

    async def check_connection(self) -> HealthReport:
        return HealthReport(status=HealthStatus.HEALTHY, message="ok")

    async def discover_resources(self) -> list[ResourceDescriptor]:
        return [ResourceDescriptor(resource_id="r1", name="Resource 1", resource_type="thing")]

    async def read_slice(self, stream: StreamDefinition, slice_: StreamSlice) -> AsyncIterator[Record]:
        if self.fail_with is not None:
            raise self.fail_with
        day = slice_.start_date or date.today()
        end = slice_.end_date or day
        while day <= end:
            for i in range(self.rows_per_day):
                ch = f"ch{i}"
                yield Record(
                    stream=stream.name,
                    key_values={"date": day.isoformat(), "channel": ch},
                    date=day,
                    dimensions={"date": day.isoformat(), "channel": ch},
                    metrics={"sessions": 10 + i},
                    measures={"sessions": 10 + i},
                )
            day = date.fromordinal(day.toordinal() + 1)
