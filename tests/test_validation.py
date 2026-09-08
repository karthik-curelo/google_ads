from datetime import date

from app.connectors.base import Record
from app.connectors.validation import (
    build_json_schema,
    detect_drift,
    micros_to_units,
    to_date,
    to_int,
    to_number,
    validate_records,
)
from app.models import make_record_key
from app.sync.writer import coerce_measures


def test_to_number_handles_provider_strings():
    assert to_number("1,423") == 1423
    assert to_number("3.14") == 3.14
    assert to_number("(not set)") is None
    assert to_number("") is None
    assert to_number(None) is None
    assert to_number(True) == 1


def test_micros_and_int_and_date():
    assert micros_to_units("1500000") == 1.5
    assert to_int("12.7") == 13
    assert to_date("20260830") == date(2026, 8, 30)
    assert to_date("2026-08-30T10:00:00Z") == date(2026, 8, 30)
    assert to_date("-") is None


def test_validate_records_rejects_missing_key_and_date_and_dupes():
    rows = [
        Record(stream="s", key_values={"date": "2026-08-01", "q": "a"}, date=date(2026, 8, 1)),
        Record(stream="s", key_values={"date": "2026-08-01", "q": None}, date=date(2026, 8, 1)),
        Record(stream="s", key_values={"date": "2026-08-02", "q": "b"}, date=None),
        Record(stream="s", key_values={"date": "2026-08-01", "q": "a"}, date=date(2026, 8, 1)),
    ]
    result = validate_records(rows, ["date", "q"], require_date=True)
    assert len(result.valid) == 1
    reasons = [r for _, r in result.skipped]
    assert any("primary key" in r for r in reasons)
    assert any("date" in r for r in reasons)
    assert any("duplicate" in r for r in reasons)


def test_detect_drift_reports_added_and_removed():
    prev = build_json_schema(["date", "country"], ["clicks"])
    curr = build_json_schema(["date", "region"], ["clicks", "impressions"])
    drift = detect_drift(prev, curr)
    assert drift.changed
    assert "region" in drift.added_fields and "impressions" in drift.added_fields
    assert "country" in drift.removed_fields


def test_record_key_is_stable_and_separator_safe():
    a = make_record_key("s", {"a": "b|c"})
    b = make_record_key("s", {"a": "b", "x": "c"})
    assert a != b
    assert make_record_key("s", {"a": 1, "b": 2}) == make_record_key("s", {"b": 2, "a": 1})


def test_coerce_measures_filters_unknown_keys():
    assert coerce_measures({"clicks": 5, "cost": 1.0, "currency": "USD", "junk": 9}) == {
        "clicks": 5,
        "cost": 1.0,
        "currency": "USD",
    }
