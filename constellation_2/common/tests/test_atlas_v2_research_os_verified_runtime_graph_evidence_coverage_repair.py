from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.verified_runtime_graph_evidence_coverage_repair import (
    build_verified_runtime_graph_evidence_coverage_repair,
    run_verified_runtime_graph_evidence_coverage_repair,
)


def test_inventory_reports_existing_and_absent_evidence(tmp_path: Path) -> None:
    truth_root = _write_graph_fixture(tmp_path)
    report = build_verified_runtime_graph_evidence_coverage_repair(
        root=tmp_path / "reports",
        truth_root=truth_root,
        day_utc="2026-06-06",
        created_at="2026-06-06T00:00:00Z",
        audit_result={"command": "npm run aegis:audit", "exit_code": 2, "status": "BLOCKED"},
    )

    assert report["summary"]["blockers_inventoried"] == 2
    assert report["summary"]["blockers_repaired"] == 1
    assert report["summary"]["remaining_blockers"] == 1
    assert report["summary"]["authority_changed"] is False
    assert report["summary"]["evidence_fabricated"] is False
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["summary"]["audit_result_recorded"] is True
    assert report["graph_blocker_inventory"][0]["repair_action"] == "REGISTER_EXISTING_EVIDENCE"
    assert report["graph_blocker_inventory"][1]["repair_action"] == "DOCUMENT_UNAVAILABLE_EVIDENCE"
    assert report["remaining_blockers"][0]["required_future_evidence"] == "missing_evidence"


def test_reports_are_written_with_required_columns(tmp_path: Path) -> None:
    truth_root = _write_graph_fixture(tmp_path)
    report = run_verified_runtime_graph_evidence_coverage_repair(
        root=tmp_path / "reports",
        truth_root=truth_root,
        day_utc="2026-06-06",
        created_at="2026-06-06T00:00:00Z",
    )
    out_dir = tmp_path / "reports" / "verified_runtime_graph_evidence_coverage_repair"

    assert (out_dir / "latest.json").exists()
    assert (out_dir / "latest_summary.md").exists()
    for filename in [
        "graph_blocker_inventory.csv",
        "repair_actions.csv",
        "remaining_blockers.csv",
        "graph_visibility_matrix.csv",
    ]:
        path = out_dir / filename
        assert path.exists()
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        assert rows or filename == "remaining_blockers.csv"
    assert json.loads((out_dir / "latest.json").read_text(encoding="utf-8"))["summary"] == report["summary"]


def _write_graph_fixture(tmp_path: Path) -> Path:
    truth_root = tmp_path / "truth"
    graph_dir = truth_root / "reports" / "aegis_verified_runtime_graph_v1" / "2026-06-06"
    graph_dir.mkdir(parents=True)
    existing_artifact = graph_dir / "existing.json"
    existing_artifact.write_text('{"ok": true}\n', encoding="utf-8")
    graph = {
        "audit_blockers": [
            "module_a:capability_a:REQUIRED_EVIDENCE_MISSING:existing_evidence",
            "module_a:capability_b:REQUIRED_EVIDENCE_MISSING:missing_evidence",
        ],
        "graph_status": "BLOCKED",
        "readiness_linkage_to_runtime_truth_kernel": {"runtime_truth_classification": "PARTIAL_CONTEXT"},
        "runtime_readiness_status": {"highest_readiness_layer": "BLOCKED"},
        "safety_invariants": {"trade_advice_allowed": False, "broker_execution_allowed": False},
        "state_transitions": [
            {"module_id": "module_a", "capability_id": "capability_a", "to_state": "BLOCKED"},
            {"module_id": "module_a", "capability_id": "capability_b", "to_state": "BLOCKED"},
        ],
    }
    ledger = {
        "entries": [
            {
                "evidence_id": "existing_evidence",
                "artifact_path": str(existing_artifact),
                "freshness_status": "CURRENT",
                "hash_verification_status": "VERIFIED",
                "validation_status": "VALID",
            },
            {
                "evidence_id": "missing_evidence",
                "artifact_path": str(graph_dir / "missing.json"),
                "freshness_status": "MISSING",
                "hash_verification_status": "MISSING",
                "validation_status": "MISSING",
            },
        ]
    }
    portal = {
        "capabilities": [
            {"module_id": "module_a", "capability_id": "capability_a", "state": "BLOCKED", "evidence_links": []},
            {"module_id": "module_a", "capability_id": "capability_b", "state": "BLOCKED", "evidence_links": []},
        ],
        "safety_invariants": {"trade_advice_allowed": False, "broker_execution_allowed": False},
    }
    (graph_dir / "verified_runtime_graph.v1.json").write_text(json.dumps(graph), encoding="utf-8")
    (graph_dir / "evidence_ledger.v1.json").write_text(json.dumps(ledger), encoding="utf-8")
    (graph_dir / "portal_runtime_model.v1.json").write_text(json.dumps(portal), encoding="utf-8")
    return truth_root
