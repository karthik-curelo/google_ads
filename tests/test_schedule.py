"""Scheduler next-run maths: `config.daily_at` pins a run to a wall-clock time."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from app.sync.runner import _next_daily_at, _next_run_at

IST = 330  # UTC+5:30


def _at(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=UTC)


def test_daily_at_later_today():
    # 09:00 UTC = 14:30 IST; next 10:00 IST is later the same day → 04:30 UTC next... no:
    now = _at("2026-09-09T03:00:00")  # 08:30 IST
    assert _next_daily_at(now, "10:00", IST) == _at("2026-09-09T04:30:00")  # 10:00 IST today


def test_daily_at_rolls_to_tomorrow_when_past():
    now = _at("2026-09-09T05:00:00")  # 10:30 IST — 10:00 already gone
    assert _next_daily_at(now, "10:00", IST) == _at("2026-09-10T04:30:00")


def test_daily_at_exact_moment_rolls_forward():
    now = _at("2026-09-09T04:30:00")  # exactly 10:00 IST
    assert _next_daily_at(now, "10:00", IST) == _at("2026-09-10T04:30:00")


def test_daily_at_midnight_wrap_across_utc_date():
    now = _at("2026-09-09T18:00:00")  # 23:30 IST
    # 00:30 IST on the 10th == 19:00 UTC on the 9th
    assert _next_daily_at(now, "00:30", IST) == _at("2026-09-09T19:00:00")


def test_daily_at_utc_offset_zero():
    now = _at("2026-09-09T09:59:00")
    assert _next_daily_at(now, "10:00", 0) == _at("2026-09-09T10:00:00")


def test_daily_at_bad_input_returns_none():
    now = _at("2026-09-09T00:00:00")
    assert _next_daily_at(now, "not-a-time", IST) is None
    assert _next_daily_at(now, "10:00", "xx") is None


# --- multiple times/day (a list of "HH:MM") --------------------------------


def test_daily_at_list_picks_soonest_of_multiple_today():
    # 08:30 IST — both 10:00 and 17:00 are still ahead today; 10:00 wins.
    now = _at("2026-09-09T03:00:00")
    assert _next_daily_at(now, ["10:00", "17:00"], IST) == _at("2026-09-09T04:30:00")


def test_daily_at_list_one_time_passed_other_still_today():
    # 11:00 IST — 10:00 has passed (rolls to tomorrow), 17:00 hasn't (stays
    # today) — the next run must be *today's* 17:00, not tomorrow's 10:00.
    now = _at("2026-09-09T05:30:00")  # 11:00 IST
    assert _next_daily_at(now, ["10:00", "17:00"], IST) == _at("2026-09-09T11:30:00")  # 17:00 IST


def test_daily_at_list_both_passed_rolls_to_tomorrows_earliest():
    # 20:00 IST — both 10:00 and 17:00 are gone for today; tomorrow's 10:00 wins.
    now = _at("2026-09-09T14:30:00")  # 20:00 IST
    assert _next_daily_at(now, ["10:00", "17:00"], IST) == _at("2026-09-10T04:30:00")


def test_daily_at_list_ignores_bad_entries_but_uses_good_ones():
    now = _at("2026-09-09T03:00:00")  # 08:30 IST
    assert _next_daily_at(now, ["not-a-time", "10:00"], IST) == _at("2026-09-09T04:30:00")


def test_daily_at_list_all_bad_returns_none():
    now = _at("2026-09-09T03:00:00")
    assert _next_daily_at(now, ["nope", "also-nope"], IST) is None


def test_next_run_at_uses_daily_at_list():
    conn = _conn(config={"daily_at": ["10:00", "17:00"], "daily_at_offset_minutes": IST})
    now = _at("2026-09-09T05:30:00")  # 11:00 IST — 10:00 passed, 17:00 hasn't
    assert _next_run_at(conn, now, _OK) == _at("2026-09-09T11:30:00")  # 17:00 IST today


def _conn(**kw):
    defaults = {"enabled": True, "schedule_interval_seconds": 86400, "config": {}, "consecutive_failures": 0}
    return SimpleNamespace(**{**defaults, **kw})


_OK = SimpleNamespace(ok=True, will_retry=False)
_RETRY = SimpleNamespace(ok=False, will_retry=True)


def test_next_run_at_uses_daily_at_when_configured():
    conn = _conn(config={"daily_at": "10:00", "daily_at_offset_minutes": IST})
    now = _at("2026-09-09T05:00:00")
    assert _next_run_at(conn, now, _OK) == _at("2026-09-10T04:30:00")


def test_next_run_at_falls_back_to_interval_without_daily_at():
    conn = _conn()
    now = _at("2026-09-09T05:00:00")
    assert _next_run_at(conn, now, _OK) == _at("2026-09-10T05:00:00")


def test_next_run_at_backs_off_on_retry_regardless_of_daily_at():
    conn = _conn(config={"daily_at": "10:00", "daily_at_offset_minutes": IST}, consecutive_failures=2)
    now = _at("2026-09-09T05:00:00")
    # backoff path ignores daily_at so a broken connection doesn't wait a full day
    assert _next_run_at(conn, now, _RETRY) > now
    assert _next_run_at(conn, now, _RETRY) != _at("2026-09-10T04:30:00")


def test_next_run_at_none_when_disabled_or_no_interval():
    assert _next_run_at(_conn(enabled=False), _at("2026-09-09T00:00:00"), _OK) is None
    assert _next_run_at(_conn(schedule_interval_seconds=None), _at("2026-09-09T00:00:00"), _OK) is None
