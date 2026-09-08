"""Record validation, type coercion, and schema-drift detection (§19).

Report APIs return everything as strings — GA4 sends `"activeUsers": "1423"`,
Search Console sends floats for integers, Google Ads returns cost in micros. If
coercion is left to each connector it gets done five slightly different ways, so
it happens once, here.

Two rules from Airbyte are enforced:
  schema drift never aborts a sync — a new metric is recorded and ingestion
    continues, because failing a nightly sync over an additive provider change
    is worse than the drift.
  nothing is silently dropped — an unusable record produces a SkippedRecord with
    a reason, so a run's skipped count is always explainable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from app.connectors.base import Record

# Providers spell "no value" many ways; all mean the same thing.
_NULLISH = frozenset({"", "-", "(none)", "(not set)", "n/a", "null", "none"})


def to_number(value: Any) -> float | int | None:
    """Coerce a provider's stringly-typed metric to a number. None if unusable."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if text.lower() in _NULLISH:
        return None
    text = text.replace(",", "")
    try:
        if "." in text or "e" in text.lower():
            return float(text)
        return int(text)
    except ValueError:
        return None


def to_int(value: Any) -> int | None:
    number = to_number(value)
    if number is None:
        return None
    try:
        return int(round(float(number)))
    except (ValueError, OverflowError):
        return None


def to_float(value: Any) -> float | None:
    number = to_number(value)
    return None if number is None else float(number)


def micros_to_units(value: Any) -> float | None:
    """Google Ads reports money in micros (1_000_000 micros = 1 currency unit)."""
    number = to_number(value)
    return None if number is None else float(number) / 1_000_000.0


def to_date(value: Any) -> date | None:
    """Parse the date forms these APIs actually emit."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in _NULLISH:
        return None
    try:
        if len(text) == 8 and text.isdigit():  # 20260830 (GA4)
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        return date.fromisoformat(text[:10])  # 2026-08-30 (GSC, Ads, Meta)
    except ValueError:
        return None


def clean_dimension(value: Any) -> str | None:
    """Normalise dimension values without discarding provider semantics.

    GA4's "(not set)" and "(none)" are *meaningful* — they mean the dimension was
    genuinely absent for that traffic — so they are preserved as-is rather than
    nulled. Only truly empty strings become None.
    """
    if value is None:
        return None
    text = str(value)
    return None if text.strip() == "" else text


@dataclass(slots=True)
class ValidationResult:
    valid: list[Record] = field(default_factory=list)
    skipped: list[tuple[dict[str, Any], str]] = field(default_factory=list)

    @property
    def counts(self) -> tuple[int, int]:
        return len(self.valid), len(self.skipped)


def validate_records(
    records: list[Record], primary_key: list[str], *, require_date: bool = True
) -> ValidationResult:
    """Drop only records that cannot be identified; keep everything else.

    A record without its primary key cannot be deduplicated, so ingesting it
    would corrupt the upsert grain — that is the one thing worth rejecting.
    """
    result = ValidationResult()
    seen: set[str] = set()

    for record in records:
        missing = [k for k in primary_key if record.key_values.get(k) in (None, "")]
        if missing:
            result.skipped.append(
                ({"key_values": record.key_values}, f"missing primary key field(s): {','.join(missing)}")
            )
            continue
        if require_date and record.date is None:
            result.skipped.append(({"key_values": record.key_values}, "missing or unparseable date"))
            continue

        # Within-batch dedup. Providers occasionally repeat a row across pages
        # when a report is paginated while data is still settling; the upsert
        # would handle it, but executemany with duplicate conflict targets fails
        # on Postgres ("cannot affect row a second time"), so it must be caught
        # before the write.
        fingerprint = repr(sorted((k, str(v)) for k, v in record.key_values.items()))
        if fingerprint in seen:
            result.skipped.append(({"key_values": record.key_values}, "duplicate primary key within batch"))
            continue
        seen.add(fingerprint)
        result.valid.append(record)

    return result


@dataclass(slots=True)
class SchemaDrift:
    added_fields: list[str] = field(default_factory=list)
    removed_fields: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.added_fields or self.removed_fields)

    def as_dict(self) -> dict[str, Any]:
        return {"added_fields": self.added_fields, "removed_fields": self.removed_fields}


def detect_drift(previous: dict[str, Any] | None, current: dict[str, Any]) -> SchemaDrift:
    """Compare two JSON Schemas' top-level properties. Reporting only."""
    if not previous:
        return SchemaDrift()
    old = set((previous.get("properties") or {}).keys())
    new = set((current.get("properties") or {}).keys())
    return SchemaDrift(added_fields=sorted(new - old), removed_fields=sorted(old - new))


def build_json_schema(
    dimensions: list[str], metrics: list[str], *, extra: dict[str, str] | None = None
) -> dict[str, Any]:
    """Declare a report stream's schema from its dimension/metric lists."""
    properties: dict[str, Any] = {"date": {"type": ["string", "null"], "format": "date"}}
    for dimension in dimensions:
        properties[dimension] = {"type": ["string", "null"]}
    for metric in metrics:
        properties[metric] = {"type": ["number", "null"]}
    for name, json_type in (extra or {}).items():
        properties[name] = {"type": [json_type, "null"]}
    return {"type": "object", "additionalProperties": True, "properties": properties}
