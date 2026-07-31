from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui.server import c2_ops_cockpit_status_v2_collector_v1 as cockpit_collector
from constellation_2.phaseL.ui_api.sleeve_evaluation_read_model import build_sleeve_evaluation_view


DAY = "2026-04-16"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_bond_registry(monkeypatch, tmp_path: Path) -> tuple[Path, Path, Path]:
    repo_root = tmp_path / "repo"
    sleeve_truth = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    global_truth = tmp_path / "runtime" / "truth"
    registry_path = repo_root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json"
    _write_json(
        registry_path,
        {
            "schema_id": "c2_sleeve_registry",
            "schema_version": "v1",
            "sleeves": [
                {
                    "sleeve_id": "BOND",
                    "enabled": True,
                    "mode": "PAPER",
                    "execution_mode": "MANUAL",
                    "status": "MANUAL_PRODUCTION",
                    "asset_class": "FIXED_INCOME",
                    "sleeve_type": "BOND",
                    "display_name": "Bond Sleeve",
                    "ui_visible": True,
                    "allocator_visible": True,
                    "automated_execution_enabled": False,
                    "broker_execution_allowed": False,
                    "advisory_only": True,
                    "ib_account": "DUO847203",
                    "symbols": ["BND", "TLT"],
                    "truth_partition": "truth_sleeves/BOND/PAPER",
                }
            ],
        },
    )
    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.REPO_ROOT", repo_root)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.SLEEVE_TRUTH_ROOT", sleeve_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.GLOBAL_TRUTH_ROOT", global_truth)
    return repo_root, sleeve_truth, global_truth


def _bond_row(payload: dict) -> dict:
    for row in payload.get("sleeves") or []:
        if row.get("sleeve_id") == "BOND":
            return row
    raise AssertionError("BOND sleeve row missing")


def test_bond_sleeve_is_visible_manual_advisory_without_automated_artifacts(monkeypatch, tmp_path: Path) -> None:
    _seed_bond_registry(monkeypatch, tmp_path)

    payload = build_sleeve_evaluation_view(DAY)
    bond = _bond_row(payload)

    assert bond["display_name"] == "Bond Sleeve"
    assert bond["execution_mode"] == "MANUAL"
    assert bond["execution_label"] == "Manual execution"
    assert bond["advisory_label"] == "Advisory only"
    assert bond["manual_execution_only"] is True
    assert bond["advisory_only"] is True
    assert bond["broker_execution_allowed"] is False
    assert bond["automated_execution_allowed"] is False
    assert bond["qualification_state"] == "MANUAL_ADVISORY"
    assert bond["readiness_diagnostic_code"] == "MANUAL_ADVISORY_NO_AUTOMATED_READINESS_REQUIRED"
    assert bond["latest_evaluation"]["present"] is False
    assert "BOND_EVALUATION_NOT_AVAILABLE" in bond["degradation_codes"]


def test_bond_sleeve_latest_recommendation_is_exposed_when_artifact_exists(monkeypatch, tmp_path: Path) -> None:
    _repo_root, _sleeve_truth, global_truth = _seed_bond_registry(monkeypatch, tmp_path)
    _write_json(
        global_truth / "reports" / "bond_sleeve_recommendation_v2" / DAY / "bond_sleeve_recommendation.v2.json",
        {
            "schema_id": "bond_sleeve_recommendation",
            "schema_version": "v2",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T17:00:00Z",
            "sleeve_id": "BOND",
            "execution_mode": "MANUAL",
            "manual_execution_only": True,
            "advisory_only": True,
            "broker_execution_allowed": False,
            "recommendation_state": "manual_buy_ladder",
            "operator_next_step": "Review the ladder manually before any broker action.",
        },
    )

    payload = build_sleeve_evaluation_view(DAY)
    bond = _bond_row(payload)

    assert bond["recommendation"]["recommendation_state"] == "MANUAL_BUY_LADDER"
    assert bond["latest_evaluation"]["present"] is True
    assert bond["latest_evaluation"]["source"] == "bond_sleeve_recommendation_v2"
    assert bond["latest_evaluation"]["produced_utc"] == f"{DAY}T17:00:00Z"
    assert bond["readiness_grade_next_step"] == "Review the ladder manually before any broker action."
    assert bond["degradation_codes"] == []


def test_cockpit_sleeve_strip_keeps_bond_visible_when_automated_policy_missing(monkeypatch, tmp_path: Path) -> None:
    repo_root, _sleeve_truth, _global_truth = _seed_bond_registry(monkeypatch, tmp_path)
    monkeypatch.setattr(cockpit_collector, "SLEEVE_POLICY_REGISTRY", tmp_path / "missing_policy.json")
    monkeypatch.setattr(
        cockpit_collector,
        "SLEEVE_REGISTRY",
        repo_root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
    )

    rows, warnings = cockpit_collector._build_sleeve_strip_rows(
        truth_root=tmp_path / "truth",
        day=DAY,
        mode_from_attempt="PAPER",
        primary_account="DUO847203",
        fallback_rows=[],
    )

    bond = next(row for row in rows if row.get("sleeve_id") == "BOND")
    assert "SLEEVE_POLICY_MISSING" in warnings
    assert bond["name"] == "Bond Sleeve"
    assert bond["execution_mode"] == "MANUAL"
    assert bond["execution_label"] == "Manual execution"
    assert bond["advisory_label"] == "Advisory only"
    assert bond["entries_allowed"] is False
    assert bond["broker_execution_allowed"] is False
    assert bond["automated_execution_allowed"] is False
