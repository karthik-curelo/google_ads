"""P0 — deterministic pagination: no skipped records, ever.

The live audit found that ordering by CreatedOn (non-unique) made identical requests
return different rows, so multi-page pulls silently lost records. These tests pin the
fix: windows are sized to fit ONE page (an atomic snapshot), verified against the
source's own count, and only a single over-full second falls back to ordered paging.

They deliberately create timestamp ties, and prove the fake reproduces the original
loss so a passing result means something.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

import httpx
import pytest

from app.connectors import errors as E
from app.connectors.leadsquared.window import Page, WindowFetcher
from tests._lsq_fake import FakeLeadSquared

T0 = datetime(2026, 8, 1, 0, 0, 0)


class FakeSource:
    """A PageSource over (id, timestamp) rows. Ordered by id, as the protocol demands."""

    def __init__(self, rows: list[tuple[str, datetime]], page_size: int = 1000) -> None:
        self.rows = rows
        self.page_size = page_size
        self.requests: list[tuple[datetime, datetime, int, int]] = []
        self.before: object | None = None  # hook(request_no) to mutate rows mid-pull

    def row_id(self, row: dict) -> str | None:
        return row.get("id")

    async def fetch(self, start: datetime, end: datetime, page_index: int, page_size: int) -> Page:
        self.requests.append((start, end, page_index, page_size))
        if callable(self.before):
            self.before(len(self.requests))
        hits = sorted((r for r in self.rows if start <= r[1] <= end), key=lambda r: r[0])
        lo = (page_index - 1) * page_size
        page = hits[lo : lo + page_size]
        if not page and page_index > 1:
            return Page(0, [])  # live behaviour: RecordCount=0 beyond the last page
        return Page(len(hits), [{"id": i, "ts": t} for i, t in page])


async def _collect(fetcher: WindowFetcher, start: datetime, end: datetime, **kw):
    return [w async for w in fetcher.windows(start, end, **kw)]


def _rows(n: int, *, seconds: int, seed: int = 1) -> list[tuple[str, datetime]]:
    """Rows with SUB-SECOND timestamps, as LeadSquared really stores them."""
    rng = random.Random(seed)
    return [
        (f"id{i:06d}", T0 + timedelta(seconds=rng.randrange(seconds), milliseconds=rng.randrange(1000)))
        for i in range(n)
    ]


# --- the fake really reproduces the original bug ---------------------------------


def _naive_pull(fake: FakeLeadSquared, code: int, frm: str, to: str, sort_col: str) -> list[str]:
    """The pre-fix strategy: page 1..n of the whole window, ordered by `sort_col`."""
    ids, page = [], 1
    while True:
        resp: httpx.Response = fake._retrieve(
            {
                "Parameter": {"FromDate": frm, "ToDate": to, "ActivityEvent": code},
                "Paging": {"PageIndex": page, "PageSize": 1000},
                "Sorting": {"ColumnName": sort_col, "Direction": "1"},
            },
            kind="activity",
        )
        rows = resp.json().get("List") or []
        ids += [r["ProspectActivityId"] for r in rows]
        if len(rows) < 1000:
            return ids
        page += 1


def _burst_fake(n: int = 3000) -> FakeLeadSquared:
    """`n` activities stamped within a handful of seconds — heavy timestamp ties."""
    fake = FakeLeadSquared(unstable_ties=True)
    for i in range(n):
        fake.add_activity(206, f"a{i:05d}", T0 + timedelta(seconds=i % 5))
    return fake


def test_the_fake_reproduces_the_original_loss_under_createdon_paging():
    fake = _burst_fake()
    lost = 0
    for _ in range(10):
        got = _naive_pull(fake, 206, "2026-08-01 00:00:00", "2026-08-01 23:59:59", "CreatedOn")
        lost += 3000 - len(set(got))
    assert lost > 0, "CreatedOn ordering over ties must drop/duplicate rows for this test to mean anything"


def test_unique_id_ordering_is_stable_across_repeated_pulls():
    fake = _burst_fake()
    pulls = [
        _naive_pull(fake, 206, "2026-08-01 00:00:00", "2026-08-01 23:59:59", "ProspectActivityId")
        for _ in range(5)
    ]
    assert all(len(set(p)) == 3000 and p == pulls[0] for p in pulls)


# --- the fetcher: complete, single-page, contiguous -------------------------------


async def test_dense_window_is_split_until_every_leaf_fits_one_page():
    src = FakeSource(_rows(5000, seconds=24 * 3600))
    windows = await _collect(WindowFetcher(src), T0, T0 + timedelta(hours=24) - timedelta(seconds=1))
    assert sum(len(w.rows) for w in windows) == 5000
    assert all(w.source_count <= src.page_size for w in windows)
    # The heart of the fix: nothing was ever read past page 1.
    assert max(page_index for _, _, page_index, _ in src.requests) == 1


async def test_no_row_is_skipped_or_duplicated_across_windows():
    rows = _rows(4000, seconds=6 * 3600, seed=3)
    src = FakeSource(rows)
    end = T0 + timedelta(hours=6) - timedelta(seconds=1)
    windows = await _collect(WindowFetcher(src), T0, end)
    got = [r["id"] for w in windows for r in w.rows]
    assert len(got) == len(set(got)) == 4000
    assert set(got) == {i for i, _ in rows}


async def test_windows_are_contiguous_gap_free_and_exactly_cover_the_range():
    src = FakeSource(_rows(3000, seconds=3 * 3600, seed=5))
    end = T0 + timedelta(hours=3) - timedelta(seconds=1)
    windows = await _collect(WindowFetcher(src), T0, end)
    assert windows[0].start == T0 and windows[-1].end == end
    for prev, nxt in zip(windows, windows[1:], strict=False):
        assert nxt.start == prev.end  # windows SHARE their boundary second, so no sub-second crack
    for w in windows:
        assert all(w.start <= t <= w.end for _, t in [(r["id"], r["ts"]) for r in w.rows])


async def test_repeated_identical_retrieval_returns_the_identical_set():
    rows = [(f"id{i:05d}", T0 + timedelta(seconds=i % 7)) for i in range(4000)]  # massive ties
    end = T0 + timedelta(hours=1)
    sets = []
    for _ in range(5):
        windows = await _collect(WindowFetcher(FakeSource(rows)), T0, end)
        sets.append({r["id"] for w in windows for r in w.rows})
    assert all(s == sets[0] for s in sets) and len(sets[0]) == 4000


async def test_a_single_second_holding_more_than_a_page_pages_in_id_order_without_loss():
    rows = [(f"id{i:05d}", T0) for i in range(2600)]  # ONE second, 2.6 pages
    src = FakeSource(rows)
    windows = await _collect(WindowFetcher(src), T0, T0 + timedelta(minutes=1))
    burst = [w for w in windows if w.paged_fallback]
    assert len(burst) == 1 and len(burst[0].rows) == 2600
    assert {r["id"] for w in windows for r in w.rows} == {i for i, _ in rows}


async def test_empty_range_costs_a_handful_of_requests_not_one_per_day():
    src = FakeSource([])
    windows = await _collect(
        WindowFetcher(src), datetime(2000, 1, 1), datetime(2026, 9, 1), initial_span=timedelta(days=1)
    )
    assert sum(w.source_count for w in windows) == 0
    assert len(src.requests) < 40  # ~26 years of nothing


async def test_rows_without_an_id_are_counted_not_silently_dropped():
    class Src(FakeSource):
        async def fetch(self, start, end, page_index, page_size):
            return Page(3, [{"id": "a"}, {"id": None}, {"id": "b"}])

    windows = await _collect(WindowFetcher(Src([])), T0, T0 + timedelta(seconds=10))
    w = windows[0]
    assert (w.source_count, len(w.rows), w.unmappable) == (3, 2, 1)


# --- integrity: a window that cannot be verified must fail, never yield -----------


async def test_short_page_is_retried_and_a_persistent_mismatch_fails_the_window():
    class Short(FakeSource):
        async def fetch(self, start, end, page_index, page_size):
            self.requests.append((start, end, page_index, page_size))
            return Page(10, [{"id": f"x{i}"} for i in range(9)])  # claims 10, delivers 9

    src = Short([])
    with pytest.raises(E.ConnectorError) as exc:
        await _collect(WindowFetcher(src, integrity_retries=2), T0, T0 + timedelta(seconds=30))
    assert exc.value.code == E.ErrorCode.RECONCILIATION_FAILED and exc.value.retryable
    assert len(src.requests) == 3  # the original + 2 retries


async def test_a_transient_short_page_is_healed_by_the_retry():
    calls = {"n": 0}

    class Flaky(FakeSource):
        async def fetch(self, start, end, page_index, page_size):
            calls["n"] += 1
            if calls["n"] == 1:
                return Page(3, [{"id": "a"}, {"id": "b"}])  # short once
            return Page(3, [{"id": "a"}, {"id": "b"}, {"id": "c"}])

    windows = await _collect(WindowFetcher(Flaky([])), T0, T0 + timedelta(seconds=30))
    assert [len(w.rows) for w in windows] == [3]


async def test_duplicate_ids_inside_one_response_are_not_accepted():
    class Dup(FakeSource):
        async def fetch(self, start, end, page_index, page_size):
            return Page(3, [{"id": "a"}, {"id": "a"}, {"id": "b"}])

    with pytest.raises(E.ConnectorError):
        await _collect(WindowFetcher(Dup([]), integrity_retries=1), T0, T0 + timedelta(seconds=30))


async def test_paged_fallback_rejects_a_pull_whose_count_changed_midway():
    rows = [(f"id{i:05d}", T0) for i in range(2500)]
    src = FakeSource(rows)

    def mutate(request_no: int) -> None:
        if request_no == 3:  # between pages of the burst pull: a row leaves the window
            src.rows = [r for r in src.rows if r[0] != "id00010"]

    src.before = mutate
    windows = await _collect(WindowFetcher(src, integrity_retries=2), T0, T0 + timedelta(seconds=0))
    # The first pull was rejected (count moved 2500 -> 2499); a later attempt is whole.
    burst = windows[0]
    assert burst.paged_fallback and len(burst.rows) == 2499
    assert "id00010" not in {r["id"] for r in burst.rows}


async def test_a_pull_that_can_never_be_made_consistent_fails_rather_than_guessing():
    src = FakeSource([(f"id{i:05d}", T0) for i in range(2500)])
    state = {"n": 0}

    def churn(request_no: int) -> None:
        state["n"] += 1
        if state["n"] % 3 == 0 and src.rows:  # keep shrinking between pages, every attempt
            src.rows = src.rows[:-1]

    src.before = churn
    with pytest.raises(E.ConnectorError) as exc:
        await _collect(WindowFetcher(src, integrity_retries=1), T0, T0)
    assert exc.value.code == E.ErrorCode.RECONCILIATION_FAILED


async def test_sub_second_rows_in_the_crack_between_adjacent_windows_are_not_lost():
    """Live finding (2026-09-20): the 10-second slice [00..09] held 10 leads but adjacent halves
    [00..04] + [05..09] held 9 — the API parses ToDate as whole seconds, so a row stamped 04.5 is
    in neither. One lead in ten was lost at every window boundary during a bulk hour."""
    T = datetime(2026, 9, 18, 12, 12, 0)
    rows = [("crack", T + timedelta(seconds=4, milliseconds=500))]  # inside the crack (04.000, 05.000)
    rows += [(f"r{i}", T + timedelta(seconds=i, milliseconds=100)) for i in (0, 1, 2, 7, 8, 9)]
    src = FakeSource(rows, page_size=3)  # tiny page: forces the range to be split at ~its midpoint
    windows = await _collect(WindowFetcher(src, fill=1.0, max_fanout=2), T, T + timedelta(seconds=10))
    got = {r["id"] for w in windows for r in w.rows}
    assert "crack" in got and got == {i for i, _ in rows}, "a row in the sub-second crack was lost"
    assert len(windows) >= 2  # it really was split


async def test_a_bulk_burst_with_subsecond_timestamps_is_captured_completely():
    """~40 rows/second for a while (the 12:10-13:09 bulk hour), split into many leaves."""
    rows = _rows(9000, seconds=300, seed=9)  # 30 rows/s
    src = FakeSource(rows)
    windows = await _collect(WindowFetcher(src), T0, T0 + timedelta(seconds=300))
    got = {r["id"] for w in windows for r in w.rows}
    assert got == {i for i, _ in rows}, f"lost {len({i for i, _ in rows} - got)} rows to boundary cracks"
    assert len(windows) > 5  # many boundaries, each of which used to leak rows
    assert max(page_index for _, _, page_index, _ in src.requests) == 1


async def test_a_row_stamped_exactly_on_a_boundary_is_returned_by_both_windows_and_is_harmless():
    T = datetime(2026, 9, 18, 12, 0, 0)
    rows = [("on_boundary", T + timedelta(seconds=5))] + [
        (f"r{i}", T + timedelta(seconds=i, milliseconds=300)) for i in (1, 2, 8, 9)
    ]
    windows = await _collect(
        WindowFetcher(FakeSource(rows, page_size=3), fill=1.0, max_fanout=2), T, T + timedelta(seconds=10)
    )
    holders = [w for w in windows if any(r["id"] == "on_boundary" for r in w.rows)]
    assert len(holders) >= 1
    ids = [r["id"] for w in windows for r in w.rows]
    assert set(ids) == {i for i, _ in rows}  # nothing lost; the duplicate (if any) is deduped by the upsert


async def test_a_zero_width_range_and_a_one_second_range_work():
    T = datetime(2026, 9, 18, 12, 0, 0)
    src = FakeSource([("a", T), ("b", T + timedelta(milliseconds=400))])
    (w,) = await _collect(WindowFetcher(src), T, T)  # zero width: only rows exactly on .000
    assert {r["id"] for r in w.rows} == {"a"}
    (w,) = await _collect(WindowFetcher(src), T, T + timedelta(seconds=1))  # one full second
    assert {r["id"] for r in w.rows} == {"a", "b"}
