"""Guards the API-coverage scanner (scripts/coverage_scan.py) against bitrot.

Not Phase D (that needs live connections + Postgres) — just: the scanner still
imports, every reference file scans without error, and every registered connector
has a reference inventory to be measured against.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from app.connectors.registry import load_connectors

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("coverage_scan", ROOT / "scripts" / "coverage_scan.py")
coverage_scan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(coverage_scan)


def _ref_ids() -> set[str]:
    import json

    return {
        json.loads(p.read_text(encoding="utf-8"))["connector_id"]
        for p in coverage_scan.REF_DIR.glob("*.json")
    }


def _real_connector_ids() -> set[str]:
    # Ignore stub connectors other tests register into the global registry.
    return {
        e.connector_id
        for e in load_connectors().all()
        if e.connector_class.__module__.startswith("app.connectors.")
    }


def test_every_registered_connector_has_a_reference_inventory():
    missing = _real_connector_ids() - _ref_ids()
    assert not missing, f"no docs/coverage/_reference/*.json for: {sorted(missing)}"


def test_scan_runs_and_classifies_every_reference():
    scans = [coverage_scan.scan_source(p) for p in coverage_scan.REF_DIR.glob("*.json")]
    assert scans
    registered = _real_connector_ids()
    for scan in scans:
        # Registered connectors must be introspectable; unbuilt ones (facebook
        # pages before its module existed) are allowed to have no connector.
        if scan["connector_id"] in registered:
            assert scan["has_connector"], scan["connector_id"]
        for section in scan["sections"].values():
            assert all(
                status in coverage_scan._TERMINAL or status == "missing"
                for status in section.values()
            )


def test_gap_report_and_manifests_render():
    scans = [coverage_scan.scan_source(p) for p in coverage_scan.REF_DIR.glob("*.json")]
    assert "# API Coverage" in coverage_scan._gap_md(scans)
    for scan in scans:
        text = coverage_scan._yaml(scan)
        assert "coverage_pct:" in text and "verified_in_db:" in text
