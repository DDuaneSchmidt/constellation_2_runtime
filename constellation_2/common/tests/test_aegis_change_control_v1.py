from __future__ import annotations

import copy
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.change_control_v1 import build_report, load_register, record_controlled_decision, validate_register


REGISTER = REPO / "aegis" / "change_control" / "aegis_change_control_register_v1.json"


def _base_closed_payload() -> dict:
    return {
        "schema_id": "aegis_change_control_register_v1",
        "schema_version": "v1",
        "register_id": "test",
        "generated_at": "2026-05-30T00:00:00Z",
        "intake_register": [
            {
                "id": "ACC-20260530-101",
                "title": "Closed UI displayed truth fix",
                "description": "Test fixture",
                "change_type": "BUG",
                "previous_status": "VALIDATED",
                "status": "CLOSED",
                "severity": "P1",
                "priority": "P1",
                "owner": "test",
                "source": "test",
                "created_at": "2026-05-30T00:00:00Z",
                "updated_at": "2026-05-30T00:00:00Z",
                "decision_ids": ["ACD-20260530-101"],
                "implementation_ids": ["ACI-20260530-101"],
                "validation_ids": ["ACV-20260530-101"],
                "tags": ["ui", "displayed_truth", "truth_data", "operator_facing", "artifact_api_browser_consistency"],
            }
        ],
        "decision_records": [
            {
                "id": "ACD-20260530-101",
                "change_id": "ACC-20260530-101",
                "decision": "APPROVE",
                "rationale": "Approved for test",
                "decided_by": "test",
                "decided_at": "2026-05-30T00:00:00Z",
                "revisit_condition": None,
                "evidence_refs": [],
            }
        ],
        "implementation_records": [
            {
                "id": "ACI-20260530-101",
                "change_id": "ACC-20260530-101",
                "summary": "Implemented for test",
                "files_changed": ["x"],
                "commands_added": [],
                "behavior_changed": True,
                "implemented_by": "test",
                "implemented_at": "2026-05-30T00:00:00Z",
                "evidence_refs": [],
            }
        ],
        "validation_records": [
            {
                "id": "ACV-20260530-101",
                "change_id": "ACC-20260530-101",
                "validated_at": "2026-05-30T00:00:00Z",
                "validated_by": "test",
                "validation_summary": "Validated for test",
                "commands_run": ["pytest"],
                "test_results": ["passed"],
                "screenshot_evidence": ["docs/screenshots/test.png"],
                "browser_evidence": ["browser text proof"],
                "artifact_evidence": ["truth/reports/test.json"],
                "api_evidence": ["/api/test"],
                "safety_gate_evidence": [],
                "remaining_gaps": [],
            }
        ],
    }


def test_seed_change_control_register_is_valid() -> None:
    payload = load_register(REGISTER)
    result = validate_register(payload)
    assert result["ok"] is True, result
    assert len(payload["intake_register"]) >= 10


def test_cannot_close_without_validation_record() -> None:
    payload = _base_closed_payload()
    payload["intake_register"][0]["validation_ids"] = []
    payload["validation_records"] = []
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("closed_requires_validation_record" in failure or "status_requires_validation_record" in failure for failure in result["failures"])


def test_ui_change_cannot_close_without_screenshot_evidence() -> None:
    payload = _base_closed_payload()
    payload["validation_records"][0]["screenshot_evidence"] = []
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("ui_closed_requires_screenshot_evidence" in failure for failure in result["failures"])


def test_truth_change_cannot_close_without_artifact_api_browser_consistency_evidence() -> None:
    payload = _base_closed_payload()
    payload["validation_records"][0]["artifact_evidence"] = []
    payload["validation_records"][0]["api_evidence"] = []
    payload["validation_records"][0]["browser_evidence"] = []
    result = validate_register(payload)
    assert result["ok"] is False
    failures = " ".join(result["failures"])
    assert "truth_closed_requires_artifact_evidence" in failures
    assert "truth_closed_requires_api_evidence" in failures
    assert "operator_truth_closed_requires_browser_evidence" in failures


def test_safety_change_cannot_close_without_safety_gate_proof() -> None:
    payload = _base_closed_payload()
    payload["intake_register"][0]["change_type"] = "SAFETY_IMPROVEMENT"
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("safety_closed_requires_safety_gate_evidence" in failure for failure in result["failures"])


def test_invalid_status_transition_fails() -> None:
    payload = _base_closed_payload()
    payload["intake_register"][0]["previous_status"] = "CAPTURED"
    payload["intake_register"][0]["status"] = "CLOSED"
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("invalid_transition:CAPTURED->CLOSED" in failure for failure in result["failures"])


def test_duplicate_ids_fail() -> None:
    payload = _base_closed_payload()
    duplicate = copy.deepcopy(payload["intake_register"][0])
    payload["intake_register"].append(duplicate)
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("duplicate_id:ACC-20260530-101" in failure for failure in result["failures"])


def test_linked_decision_implementation_validation_records_resolve() -> None:
    payload = _base_closed_payload()
    result = validate_register(payload)
    assert result["ok"] is True, result

    payload["decision_records"][0]["change_id"] = "ACC-20260530-999"
    failed = validate_register(payload)
    assert failed["ok"] is False
    assert any("decision:ACD-20260530-101:missing_change" in failure for failure in failed["failures"])



def test_validating_status_is_valid_lifecycle_state() -> None:
    payload = _base_closed_payload()
    payload["intake_register"][0]["previous_status"] = "IMPLEMENTED"
    payload["intake_register"][0]["status"] = "VALIDATING"
    payload["intake_register"][0]["validation_ids"] = []
    payload["validation_records"] = []
    result = validate_register(payload)
    assert result["ok"] is True, result


def test_report_contains_lifecycle_validation_decision_dashboards() -> None:
    payload = load_register(REGISTER)
    report = build_report(payload)
    assert report["lifecycle_counts"]["CAPTURED"] >= 1
    assert "VALIDATING" in report["lifecycle_counts"]
    assert "validation_dashboard" in report
    assert "decision_dashboard" in report
    assert "closure_evidence_summary" in report
    assert report["summary"]["open_p0_p1_count"] >= 5


def test_controlled_decision_approve_creates_decision_record(tmp_path: Path) -> None:
    source = load_register(REGISTER)
    path = tmp_path / "register.json"
    path.write_text(__import__("json").dumps(source, indent=2) + "\n", encoding="utf-8")
    result = record_controlled_decision({
        "change_id": "ACC-20260530-008A",
        "action": "APPROVE",
        "notes": "Approve validation engine first.",
        "decided_by": "test",
    }, path)
    assert result["ok"] is True
    updated = load_register(path)
    row = next(item for item in updated["intake_register"] if item["id"] == "ACC-20260530-008A")
    assert row["status"] == "DECIDED"
    assert row["decision_ids"]
    decision = next(item for item in updated["decision_records"] if item["id"] == row["decision_ids"][-1])
    assert decision["decision"] == "APPROVE"
    assert decision["decision_notes"] == "Approve validation engine first."


def test_controlled_decision_prioritize_does_not_close_validate_or_implement(tmp_path: Path) -> None:
    source = load_register(REGISTER)
    path = tmp_path / "register.json"
    path.write_text(__import__("json").dumps(source, indent=2) + "\n", encoding="utf-8")
    result = record_controlled_decision({
        "change_id": "ACC-20260530-010",
        "action": "PRIORITIZE",
        "priority": "P0",
        "notes": "Escalate after validation engine decision.",
    }, path)
    assert result["ok"] is True
    updated = load_register(path)
    row = next(item for item in updated["intake_register"] if item["id"] == "ACC-20260530-010")
    assert row["priority"] == "P0"
    assert row["status"] == "CAPTURED"
    assert not row["implementation_ids"]
    assert not row["validation_ids"]


def test_controlled_decision_rejects_forbidden_close_validate_implement(tmp_path: Path) -> None:
    source = load_register(REGISTER)
    path = tmp_path / "register.json"
    path.write_text(__import__("json").dumps(source, indent=2) + "\n", encoding="utf-8")
    for action in ["CLOSE", "VALIDATE", "IMPLEMENT"]:
        result = None
        try:
            record_controlled_decision({"change_id": "ACC-20260530-008", "action": action}, path)
        except Exception as exc:  # exact exception type is validated by message below
            result = str(exc)
        assert result and "FORBIDDEN_DECISION_ACTION" in result


def test_parent_child_relationships_are_reported() -> None:
    payload = load_register(REGISTER)
    result = validate_register(payload)
    assert result["ok"] is True, result
    report = build_report(payload)
    graph = report["relationship_graph"]
    parent = next(row for row in graph["parents"] if row["id"] == "ACC-20260530-008")
    child_ids = {row["id"] for row in parent["children"]}
    assert {"ACC-20260530-008A", "ACC-20260530-008B", "ACC-20260530-008C", "ACC-20260530-008D"}.issubset(child_ids)
    assert parent["rollup"]["required_child_count"] == 4
    assert parent["rollup"]["required_children_open"] == 4


def test_parent_cannot_validate_with_open_required_children() -> None:
    payload = load_register(REGISTER)
    payload = copy.deepcopy(payload)
    parent = next(row for row in payload["intake_register"] if row["id"] == "ACC-20260530-008")
    parent["previous_status"] = "IMPLEMENTED"
    parent["status"] = "VALIDATED"
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("parent_requires_closed_children_before_VALIDATED" in failure for failure in result["failures"])


def test_child_parent_links_must_be_bidirectional() -> None:
    payload = load_register(REGISTER)
    payload = copy.deepcopy(payload)
    child = next(row for row in payload["intake_register"] if row["id"] == "ACC-20260530-008A")
    child["parent_id"] = "ACC-20260530-999"
    result = validate_register(payload)
    assert result["ok"] is False
    assert any("missing_parent:ACC-20260530-999" in failure for failure in result["failures"])
