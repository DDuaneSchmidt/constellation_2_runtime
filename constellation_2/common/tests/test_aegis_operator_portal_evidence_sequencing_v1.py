from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.check_aegis_operator_portal_pre_graph_evidence_v1 import build_pre_graph_evidence_report_v1
from ops.aegis.verified_runtime_graph_v1 import _load_manifest

DAY = "2026-06-02"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _operator_portal_manifest(required: list[str]) -> dict:
    return {
        "module_id": "operator_portal",
        "display_name": "Operator Portal",
        "type": "ui",
        "owner": "tests",
        "capabilities": [
            {
                "capability_id": "operator_decision_dashboard_v1",
                "display_name": "Operator Decision Dashboard V1",
                "required_evidence": required,
                "allowed_actions": ["READ_ONLY", "QUERY"],
                "consumed_runtime_context": ["aegis_verified_runtime_graph_v1"],
            },
            {
                "capability_id": "hypothesis_proposal_promotion_pipeline_v1",
                "display_name": "Hypothesis Proposal Promotion Pipeline V1",
                "required_evidence": [],
                "allowed_actions": ["READ_ONLY", "QUERY"],
            },
        ],
        "inputs": [],
        "outputs": [],
        "commands": [{"name": "unit", "command": "pytest"}],
        "tests": [{"path": "constellation_2/common/tests/test_aegis_operator_portal_evidence_sequencing_v1.py"}],
        "evidence_artifacts": [
            {
                "evidence_id": "unit_evidence_v1",
                "artifact_type": "unit",
                "path": "reports/unit_evidence_v1/{day}/unit.v1.json",
                "producer": "tests",
                "day_scoped": True,
            }
        ],
        "ui_surfaces": [],
        "policies": ["read_only"],
        "freshness_requirements": [],
    }


def test_operator_decision_dashboard_does_not_require_verified_graph_itself() -> None:
    manifest = _load_manifest(REPO_ROOT / "aegis/modules/operator_portal/aegis.module.yaml")
    dashboard = next(
        row for row in manifest["capabilities"] if row.get("capability_id") == "operator_decision_dashboard_v1"
    )

    assert "aegis_verified_runtime_graph_v1" not in dashboard.get("required_evidence", [])
    assert "aegis_verified_runtime_graph_v1" in dashboard.get("consumed_runtime_context", [])


def test_audit_chain_runs_operator_portal_evidence_before_verified_graph() -> None:
    package = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))
    audit = package["scripts"]["aegis:audit"]
    required_steps = [
        "npm run aegis:hypothesis-proposal-promotion",
        "npm run aegis:approved-hypothesis-paper-setup",
        "npm run aegis:hypothesis-workflow-state",
        "npm run aegis:operator-action-queue",
        "npm run aegis:hypothesis-workflow-replay-verification",
        "npm run aegis:generated-hypothesis-throughput",
        "npm run aegis:macro-calendar-data-readiness",
        "npm run aegis:research-daily-scorecard",
        "npm run aegis:oil-shock-candidate-flow",
        "npm run aegis:research-quality-engine",
        "npm run aegis:hypothesis-decision-policy",
        "npm run aegis:research-allocation-recommendation",
        "npm run aegis:research-follow-through-control",
        "npm run aegis:ai-research-intelligence",
        "npm run aegis:operator-portal-pre-graph-evidence",
    ]
    graph_index = audit.index("npm run aegis:verified-graph -- --strict")

    for step in required_steps:
        assert step in audit
        assert audit.index(step) < graph_index


def test_missing_operator_portal_evidence_has_clear_pre_graph_report(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    modules_root = tmp_path / "modules"
    manifest_path = modules_root / "operator_portal" / "aegis.module.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(_operator_portal_manifest(["unit_evidence_v1"]), indent=2) + "\n", encoding="utf-8")

    report = build_pre_graph_evidence_report_v1(truth_root=truth_root, day_utc=DAY, modules_root=modules_root)

    assert report["status"] == "FAIL"
    assert report["missing_count"] == 1
    missing = report["missing_evidence"][0]
    assert missing["capability"] == "operator_decision_dashboard_v1"
    assert missing["required_evidence"] == "unit_evidence_v1"
    assert missing["status"] == "MISSING"
    assert missing["path"].endswith("reports/unit_evidence_v1/2026-06-02/unit.v1.json")


def test_present_operator_portal_evidence_reports_hash_and_generated_at(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    modules_root = tmp_path / "modules"
    manifest_path = modules_root / "operator_portal" / "aegis.module.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(_operator_portal_manifest(["unit_evidence_v1"]), indent=2) + "\n", encoding="utf-8")
    _write_json(
        truth_root / "reports/unit_evidence_v1" / DAY / "unit.v1.json",
        {"schema_id": "unit", "schema_version": "v1", "day_utc": DAY, "generated_at_utc": f"{DAY}T12:00:00Z"},
    )

    report = build_pre_graph_evidence_report_v1(truth_root=truth_root, day_utc=DAY, modules_root=modules_root)

    row = report["capabilities"][0]["evidence"][0]
    assert report["status"] == "PASS"
    assert row["status"] == "PRESENT"
    assert row["generated_at"] == f"{DAY}T12:00:00Z"
    assert row["content_hash"]


def test_june_2_mark_coverage_and_lineage_remain_green_when_artifacts_exist() -> None:
    truth_root = Path("/home/node/constellation_runtime_data/truth")
    mark_path = truth_root / "reports/aegis_mark_coverage_report_v1" / DAY / "mark_coverage_report.v1.json"
    lineage_path = truth_root / "reports/aegis_evidence_lineage_integrity_v1" / DAY / "evidence_lineage_integrity.v1.json"
    if not mark_path.exists() or not lineage_path.exists():
        pytest.skip("June 2 runtime artifacts are not available in this test environment")

    mark = json.loads(mark_path.read_text(encoding="utf-8"))
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))

    assert mark["total_positions"] == 57
    assert mark["marked_positions"] == 57
    assert mark["coverage_pct"] == 100.0
    assert lineage["evidence_coverage_panel"]["current_integrity_status"] == "GREEN"
    assert lineage["evidence_coverage_panel"]["mark_coverage_pct"] == 100.0


def test_pre_graph_check_does_not_change_safety_gates(tmp_path: Path) -> None:
    modules_root = tmp_path / "modules"
    manifest_path = modules_root / "operator_portal" / "aegis.module.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(_operator_portal_manifest([]), indent=2) + "\n", encoding="utf-8")

    report = build_pre_graph_evidence_report_v1(truth_root=tmp_path / "truth", day_utc=DAY, modules_root=modules_root)

    assert report["safety"] == {
        "read_only_check": True,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "trade_advice_allowed": False,
        "real_capital_allocation_allowed": False,
        "autonomous_execution_allowed": False,
        "safety_gates_changed": False,
    }
