from datetime import date

from app.connectors.slicing import (
    advance_cursor,
    date_slices,
    parse_cursor,
    resolve_sync_window,
)


def test_date_slices_inclusive_and_ordered():
    s = date_slices(date(2026, 1, 1), date(2026, 1, 10), 4)
    assert [(x.start_date, x.end_date) for x in s] == [
        (date(2026, 1, 1), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 8)),
        (date(2026, 1, 9), date(2026, 1, 10)),
    ]


def test_date_slices_empty_when_reversed():
    assert date_slices(date(2026, 1, 10), date(2026, 1, 1), 7) == []


def test_parse_cursor_forms():
    assert parse_cursor("20260830") == date(2026, 8, 30)
    assert parse_cursor("2026-08-30T00:00:00Z") == date(2026, 8, 30)
    assert parse_cursor("garbage") is None
    assert parse_cursor(None) is None


def test_resolve_window_initial_backfill():
    w = resolve_sync_window(cursor_value=None, today=date(2026, 8, 30), default_backfill_days=90)
    assert w.is_backfill and w.start == date(2026, 6, 1) and w.end == date(2026, 8, 30)


def test_resolve_window_incremental_applies_lookback():
    w = resolve_sync_window(cursor_value="2026-08-20", today=date(2026, 8, 30), lookback_days=3)
    assert not w.is_backfill and w.start == date(2026, 8, 17) and w.end == date(2026, 8, 30)


def test_resolve_window_respects_provider_lag_and_retention():
    w = resolve_sync_window(
        cursor_value="2026-08-25",
        today=date(2026, 8, 30),
        lookback_days=3,
        provider_lag_days=2,
        max_history_days=5,
    )
    assert w.end == date(2026, 8, 28)  # today - lag
    assert w.start == date(2026, 8, 25)  # clamped to retention (today - 5)


def test_resolve_window_none_when_start_after_end():
    # provider lag pushes the window end before the (future) cursor start
    assert (
        resolve_sync_window(
            cursor_value="2026-08-30", today=date(2026, 8, 30), lookback_days=0, provider_lag_days=5
        )
        is None
    )
    # a same-day re-fetch is still returned (lookback keeps recent days in scope)
    w = resolve_sync_window(cursor_value="2026-08-30", today=date(2026, 8, 30), lookback_days=0)
    assert w is not None and w.start == w.end == date(2026, 8, 30)


def test_advance_cursor_is_monotonic():
    assert advance_cursor("2026-08-20", date(2026, 8, 25)) == "2026-08-25"
    assert advance_cursor("2026-08-25", date(2026, 8, 20)) == "2026-08-25"
    assert advance_cursor(None, "2026-01-01") == "2026-01-01"
    assert advance_cursor("2026-08-25", None) == "2026-08-25"
