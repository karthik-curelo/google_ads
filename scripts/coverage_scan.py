"""API coverage scanner (see docs/coverage/PLAN.md).

Joins the hand-maintained capability inventories in docs/coverage/_reference/*.json
(what each API *could* give) against what the connectors actually request (read
from their declarative stream specs), and writes:

  docs/coverage/<connector_id>.yaml   — per-source manifest with a status per capability
  docs/coverage/GAP_REPORT.md         — one table per source, missing high-value first

Run:  python scripts/coverage_scan.py            # print the gap report
      python scripts/coverage_scan.py --write    # also (re)write the yaml + md files

No new dependencies — the YAML we emit is shallow and hand-formatted.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
REF_DIR = ROOT / "docs" / "coverage" / "_reference"
OUT_DIR = ROOT / "docs" / "coverage"

# Reference keys that hold {name: {...}} capability maps (everything else is metadata).
_CAP_SECTIONS = (
    "resources", "dimensions", "metrics", "segments", "breakdowns", "action_breakdowns",
    "insight_fields", "account_metrics", "media_metrics", "reel_metrics", "story_metrics",
    "demographic_breakdowns", "search_types", "report_methods", "config",
    "page_insight_metrics", "post_insight_metrics",
)
_TERMINAL = ("unsupported", "platform_limited", "permission_gated", "partial", "implemented")


def _implemented_sets(connector_id: str) -> tuple[set[str], set[str]]:
    """(fields, resources) a connector actually requests, unioned across its streams."""
    from app.connectors.registry import load_connectors

    registry = load_connectors()
    if connector_id not in registry:
        return set(), set()
    cls = registry.connector_class(connector_id)

    fields: set[str] = set()
    resources: set[str] = set()
    for stream in cls.declared_streams():
        resources.add(stream.name)
        fields.update((stream.json_schema.get("properties") or {}).keys())
        spec = stream.spec or {}
        for key in ("dimensions", "metrics", "dims", "select", "fields", "breakdowns", "extra_dims"):
            val = spec.get(key)
            if isinstance(val, (list, tuple)):
                fields.update(str(v) for v in val)
        for key in ("resource", "edge", "level"):
            if spec.get(key):
                resources.add(str(spec[key]))
    return fields, resources


def _match(name: str, pool: set[str]) -> bool:
    if name in pool:
        return True
    if name.endswith("*"):
        prefix = name[:-1]
        return any(p.startswith(prefix) for p in pool)
    return False


def _classify(name: str, entry: dict[str, Any], fields: set[str], resources: set[str]) -> str:
    if _match(name, fields) or _match(name, resources):
        return "implemented"
    declared = str(entry.get("status") or "").strip()
    if declared in _TERMINAL:
        return declared
    return "missing"


def scan_source(ref_path: Path) -> dict[str, Any]:
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    connector_id = ref["connector_id"]
    fields, resources = _implemented_sets(connector_id)
    has_connector = bool(fields or resources)

    sections: dict[str, dict[str, str]] = {}
    for section, entries in ref.items():
        if section not in _CAP_SECTIONS or not isinstance(entries, dict):
            continue
        out: dict[str, str] = {}
        for name, entry in entries.items():
            entry = entry if isinstance(entry, dict) else {}
            out[name] = _classify(name, entry, fields, resources) if has_connector else (
                str(entry.get("status") or "missing")
            )
        sections[section] = out

    return {
        "source": ref["source"],
        "connector_id": connector_id,
        "generated": date.today().isoformat(),
        "has_connector": has_connector,
        "refresh": ref.get("refresh"),
        "review_by": ref.get("review_by"),
        "sections": sections,
        "notes": ref.get("notes", []),
        "_ref": ref,
    }


def _tally(sections: dict[str, dict[str, str]]) -> dict[str, int]:
    t: dict[str, int] = {}
    for entries in sections.values():
        for status in entries.values():
            t[status] = t.get(status, 0) + 1
    return t


def _coverage_pct(t: dict[str, int]) -> int:
    counted = sum(v for k, v in t.items() if k != "unsupported")
    if not counted:
        return 0
    got = t.get("implemented", 0) + t.get("platform_limited", 0) + t.get("permission_gated", 0)
    return round(100 * got / counted)


def _yaml(scan: dict[str, Any]) -> str:
    lines = [
        f"source: {scan['source']}",
        f"connector_id: {scan['connector_id']}",
        f"generated: {scan['generated']}",
        f"has_connector: {str(scan['has_connector']).lower()}",
        f"refresh: {json.dumps(scan['refresh'])}",
        f"review_by: {scan['review_by']}",
        "verified_in_db: false   # flipped by tests/test_coverage_reaches_db.py",
        "sections:",
    ]
    for section, entries in scan["sections"].items():
        lines.append(f"  {section}:")
        for name, status in entries.items():
            lines.append(f"    {json.dumps(name)}: {status}")
    t = _tally(scan["sections"])
    lines.append("tally:")
    for status in sorted(t):
        lines.append(f"  {status}: {t[status]}")
    lines.append(f"coverage_pct: {_coverage_pct(t)}")
    return "\n".join(lines) + "\n"


def _gap_md(scans: list[dict[str, Any]]) -> str:
    md = [
        "# API Coverage - Gap Report",
        "",
        f"Generated {date.today().isoformat()} by `scripts/coverage_scan.py`. "
        "See `PLAN.md` for method and the four platform ceilings.",
        "",
        "| Source | Coverage | implemented | missing | partial | perm-gated | platform-limited |",
        "|---|---|---|---|---|---|---|",
    ]
    for s in scans:
        t = _tally(s["sections"])
        md.append(
            f"| {s['source']} | {_coverage_pct(t)}% | {t.get('implemented', 0)} | "
            f"{t.get('missing', 0)} | {t.get('partial', 0)} | {t.get('permission_gated', 0)} | "
            f"{t.get('platform_limited', 0)} |"
        )
    md.append("")

    for s in scans:
        md.append(f"## {s['source']}")
        md.append("")
        if not s["has_connector"]:
            md.append("_No connector registered - every capability below is unbuilt._")
            md.append("")
        gaps: list[tuple[str, str, str, str]] = []
        for section, entries in s["sections"].items():
            ref_section = s["_ref"].get(section, {})
            for name, status in entries.items():
                if status in ("implemented", "unsupported"):
                    continue
                meta = ref_section.get(name, {}) if isinstance(ref_section, dict) else {}
                value = meta.get("value", "?") if isinstance(meta, dict) else "?"
                note = meta.get("note", "") if isinstance(meta, dict) else ""
                gaps.append((value, section, f"{name} - {status}", note))
        if not gaps:
            md.append("Nothing outstanding.")
            md.append("")
            continue
        order = {"high": 0, "medium": 1, "low": 2, "?": 3}
        gaps.sort(key=lambda g: (order.get(g[0], 9), g[1]))
        md.append("| Value | Section | Capability | Note |")
        md.append("|---|---|---|---|")
        for value, section, cap, note in gaps:
            md.append(f"| {value} | {section} | {cap} | {note} |")
        md.append("")
    return "\n".join(md) + "\n"


def main() -> int:
    write = "--write" in sys.argv
    scans = [scan_source(p) for p in sorted(REF_DIR.glob("*.json"))]
    report = _gap_md(scans)
    print(report)
    if write:
        for s in scans:
            (OUT_DIR / f"{s['connector_id']}.yaml").write_text(_yaml(s), encoding="utf-8")
        (OUT_DIR / "GAP_REPORT.md").write_text(report, encoding="utf-8")
        print(f"wrote {len(scans)} manifests + GAP_REPORT.md to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
