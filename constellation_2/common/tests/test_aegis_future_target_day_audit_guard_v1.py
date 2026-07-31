from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.candidate_to_paper_self_check_v1 import build_candidate_to_paper_self_check_v1
from ops.aegis.evidence_lineage_integrity_v1 import build_evidence_lineage_integrity_v1, integrity_failures_v1
from ops.aegis.future_target_day_audit_guard_v1 import build_future_target_day_audit_guard_v1, write_future_target_day_audit_guard_v1
from ops.aegis.generated_hypothesis_outcome_eligibility_day_proof_v1 import build_generated_hypothesis_outcome_eligibility_day_proof_v1
from ops.aegis.generated_hypothesis_outcome_mark_certification_repair_v1 import build_generated_hypothesis_outcome_mark_certification_repair_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.paper_outcome_auto_closure_v1 import build_paper_outcome_auto_closure_v1
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1

CURRENT = "2026-06-02"
PAST = "2026-06-01"
FUTURE = "2026-06-03"
HID = "ehp_cdbd8fe683acb622"
CID = "candidate_contract_a3dd44d21952f8098131aa04"
PID = "paper-position:candidate_contract_a3dd44d21952f8098131aa04"
SLEEVE = "C2_OIL_SHOCK_REVERSAL_V1"


def _report(root: Path, family: str, day: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def _write(path: Path, payload: dict) -> None:
    write_json_v1(path, payload)


def _position(day: str, *, mark: bool = False) -> dict:
    row = {"position_id": PID, "candidate_id": CID, "symbol": "USO", "sleeve_id": SLEEVE, "hypothesis_id": HID, "thesis_id": "thesis-test", "entry_price": "135.3914", "entry_time": "2026-06-02T20:11:45Z", "originating_day": "2026-06-02", "quantity": "1", "side": "BUY", "current_status": "OPEN", "paper_tracking_mode": "AUTO_PROMOTED_RESEARCH_OBSERVATION", "auto_promotion_reason_codes": ["TEST_AUTO_PROMOTION"], "candidate_lineage": {"candidate_id": CID, "hypothesis_id": HID, "sleeve_id": SLEEVE, "thesis_id": "thesis-test"}}
    if mark:
        row.update({"current_certified_mark": "137.27", "mark_timestamp_utc": f"{day}T20:00:00Z", "mark_source_path": "/tmp/USO.json", "mark_source_hash": "hash-uso", "mark_certification_status": "CERTIFIED"})
    return row


def _market(day: str, *, current: bool = False) -> dict:
    return {"day_utc": day, "symbols": {"USO": {"symbol": "USO", "last_price": 137.27, "close": 137.27, "freshness_status": "CURRENT" if current else "STALE", "market_session_date": day if current else CURRENT, "data_finality": "FINAL_EOD" if current else "PROVISIONAL_INTRADAY", "data_timestamp_utc": f"{day if current else CURRENT}T20:00:00Z", "source_timestamp_utc": f"{day if current else CURRENT}T20:00:00Z", "source_url_or_path": "/tmp/USO.json", "source_hash": "hash-uso", "provider": "TEST"}}}


def _seed(root: Path, day: str, *, mark: bool = False, market_current: bool = False, guard: bool = True) -> None:
    pos = _position(day, mark=mark)
    _write(_report(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"), {"positions": [pos], "open_positions": [pos], "closed_positions": []})
    _write(_report(root, "aegis_market_data_v1", day, "market_data.v1.json"), _market(day, current=market_current))
    _write(_report(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json"), {"rows": [{"position_id": PID, "candidate_id": CID, "exit_recommendation": "HOLD", "exit_trigger": "MISSING_CURRENT_MARK", "exit_mark": None, "exit_mark_certification_status": "MISSING_MARK"}]})
    _write(_report(root, "aegis_exit_recommendations_v1", day, "exit_recommendations.v1.json"), {"recommendations": [{"position_id": PID, "candidate_id": CID, "exit_recommendation": "HOLD", "reason_codes": ["NO_EXIT_RULE_TRIGGERED"], "policy": {"sleeve_id": SLEEVE}}]})
    _write(_report(root, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", day, "generated_hypothesis_paper_observation_to_outcome.v1.json"), {"summary": {"outcome_row_created": False, "outcome_status": "OUTCOME_NOT_READY", "exit_price_source": ""}})
    _write(_report(root, "aegis_generated_hypothesis_outcome_maturity_monitor_v1", day, "generated_hypothesis_outcome_maturity_monitor.v1.json"), {"summary": {"total_open_generated_hypothesis_observations": 1}})
    _write(_report(root, "aegis_generated_hypothesis_validation_proof_v1", day, "generated_hypothesis_validation_proof.v1.json"), {"summary": {"furthest_stage_reached": "PAPER_OBSERVATION_FLOW"}})
    _write(_report(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"), {"schema_id": "aegis_candidate_generation_diagnostics", "day_utc": day})
    _write(_report(root, "aegis_runtime_truth_kernel_v1", day, "runtime_truth_kernel.v1.json"), {"trade_advice_allowed": False})
    if guard:
        payload = build_future_target_day_audit_guard_v1(truth_root=root, day_utc=day, actual_runtime_date=CURRENT)
        write_future_target_day_audit_guard_v1(truth_root=root, day_utc=day, payload=payload)


def test_current_target_day_uses_strict_final_audit_mode(tmp_path: Path) -> None:
    payload = build_future_target_day_audit_guard_v1(truth_root=tmp_path, day_utc=CURRENT, actual_runtime_date=CURRENT)
    assert payload["audit_mode"] == "STRICT_FINAL"
    assert payload["final_mark_certification_allowed"] is True
    assert payload["guard_status"] == "NOT_APPLICABLE_TARGET_DAY_CURRENT_OR_PAST"


def test_past_target_day_uses_strict_final_audit_mode(tmp_path: Path) -> None:
    payload = build_future_target_day_audit_guard_v1(truth_root=tmp_path, day_utc=PAST, actual_runtime_date=CURRENT)
    assert payload["audit_mode"] == "STRICT_FINAL"
    assert payload["evidence_lineage_final_mark_check_allowed"] is True


def test_future_target_day_reports_guarded(tmp_path: Path) -> None:
    payload = build_future_target_day_audit_guard_v1(truth_root=tmp_path, day_utc=FUTURE, actual_runtime_date=CURRENT)
    assert payload["guard_status"] == "TARGET_DAY_IN_FUTURE_GUARDED"
    assert payload["blocker_code"] == "TARGET_DAY_IN_FUTURE"
    assert payload["future_day_delta"] == 1


def test_future_target_day_does_not_certify_final_marks(tmp_path: Path) -> None:
    _seed(tmp_path, FUTURE)
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=FUTURE)
    assert payload["blocker_code"] == "TARGET_DAY_IN_FUTURE"
    assert payload["mark_certification_status"] == "TARGET_DAY_IN_FUTURE"
    assert payload["certified_mark_found_after_repair"] is False


def test_future_target_day_does_not_create_outcomes(tmp_path: Path) -> None:
    _seed(tmp_path, FUTURE)
    payload = build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=FUTURE)
    assert payload["oil_shock"]["blocker_code"] == "TARGET_DAY_IN_FUTURE"
    assert payload["oil_shock"]["outcome_row_created"] is False


def test_future_target_day_does_not_create_validation_samples(tmp_path: Path) -> None:
    _seed(tmp_path, FUTURE)
    repair = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=FUTURE)
    assert repair["validation_sample_created_by_this_artifact"] is False
    assert repair["research_quality_result_created_by_this_artifact"] is False


def test_future_target_day_blocks_generic_auto_closure_and_samples(tmp_path: Path) -> None:
    _seed(tmp_path, FUTURE, mark=True, market_current=True)
    closure = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=FUTURE)
    assert closure["summary"]["future_target_day_guarded"] is True
    assert closure["summary"]["auto_closed_count"] == 0
    assert all(row["auto_closure_state"] != "AUTO_CLOSED_PAPER_OUTCOME" for row in closure["rows"])
    assert all("TARGET_DAY_IN_FUTURE" in row["auto_closure_reason_codes"] for row in closure["rows"])
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc=FUTURE, outcome_registry={"outcomes": [{"outcome_state": "CLOSED_WIN"}]})
    assert samples["summary"]["future_target_day_guarded"] is True
    assert samples["summary"]["total_samples"] == 0
    assert samples["samples"] == []


def _seed_outcome_and_empty_samples(root: Path, day: str) -> None:
    _write(_report(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"), {"outcomes": [{"position_id": PID, "candidate_id": CID, "outcome_state": "OPEN"}]})
    _write(_report(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"), {"summary": {"total_samples": 0}, "samples": []})


def test_future_target_day_self_check_guards_missing_validation_samples(tmp_path: Path) -> None:
    _seed(tmp_path, FUTURE)
    _seed_outcome_and_empty_samples(tmp_path, FUTURE)
    check = build_candidate_to_paper_self_check_v1(truth_root=tmp_path, day_utc=FUTURE)

    assert check["ok"] is True
    assert check["future_target_day_guarded"] is True
    assert check["guarded_skip_count"] == 1
    assert check["guarded_skips"][0]["check_code"] == "AUTO_PROMOTED_POSITION_MISSING_VALIDATION_SAMPLE_ROW"
    assert not any(f.get("failure_code") == "AUTO_PROMOTED_POSITION_MISSING_VALIDATION_SAMPLE_ROW" for f in check["failures"])


def test_current_target_day_self_check_keeps_missing_validation_sample_strict(tmp_path: Path) -> None:
    _seed(tmp_path, CURRENT)
    _seed_outcome_and_empty_samples(tmp_path, CURRENT)
    check = build_candidate_to_paper_self_check_v1(truth_root=tmp_path, day_utc=CURRENT)

    assert check["ok"] is False
    assert check["future_target_day_guarded"] is False
    assert any(f.get("failure_code") == "AUTO_PROMOTED_POSITION_MISSING_VALIDATION_SAMPLE_ROW" for f in check["failures"])


def test_future_missing_marks_are_not_evidence_lineage_corruption(tmp_path: Path) -> None:
    _seed(tmp_path, FUTURE)
    payload = build_evidence_lineage_integrity_v1(truth_root=tmp_path, day_utc=FUTURE)
    failures = integrity_failures_v1(payload)
    assert payload["evidence_coverage_panel"]["future_target_day_guarded"] is True
    assert payload["evidence_coverage_panel"]["current_integrity_status"] == "GUARDED_FUTURE_TARGET_DAY"
    assert not any("mark_coverage_integrity" in item for item in failures)


def test_current_past_missing_marks_still_fail_strict_lineage(tmp_path: Path) -> None:
    _seed(tmp_path, CURRENT)
    payload = build_evidence_lineage_integrity_v1(truth_root=tmp_path, day_utc=CURRENT)
    failures = integrity_failures_v1(payload)
    assert any("mark_coverage_integrity" in item for item in failures)
    assert payload["evidence_coverage_panel"]["current_integrity_status"] == "RED"


def test_future_guarded_sleeves_now_skips_hanging_execution(tmp_path: Path) -> None:
    from ops.tools.run_aegis_future_guarded_sleeves_now_v1 import main
    import sys
    old = sys.argv
    try:
        sys.argv = ["guarded", "--truth-root", str(tmp_path), "--day", FUTURE]
        assert main() == 0
    finally:
        sys.argv = old


def test_portal_smoke_fragments_do_not_hard_code_closed_outcome_count() -> None:
    text = Path("constellation_2/phaseL/ui/tests/test_operator_rendered_truth_regression_v1.py").read_text(encoding="utf-8")
    assert "Closed paper outcomes today 5" not in text


def test_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    payload = build_future_target_day_audit_guard_v1(truth_root=tmp_path, day_utc=FUTURE, actual_runtime_date=CURRENT)
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["safety_gates_changed"] is False
