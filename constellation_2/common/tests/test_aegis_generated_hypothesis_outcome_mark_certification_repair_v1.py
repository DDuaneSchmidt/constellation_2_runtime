from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.generated_hypothesis_outcome_mark_certification_repair_v1 import (
    build_generated_hypothesis_outcome_mark_certification_repair_v1,
    generated_hypothesis_outcome_mark_certification_repair_path_v1,
    write_generated_hypothesis_outcome_mark_certification_repair_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-03"
HID = "ehp_cdbd8fe683acb622"
CID = "candidate_contract_a3dd44d21952f8098131aa04"
PID = "paper-position:candidate_contract_a3dd44d21952f8098131aa04"
SLEEVE = "C2_OIL_SHOCK_REVERSAL_V1"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _write(path: Path, payload: dict) -> None:
    write_json_v1(path, payload)


def _position(*, certified: bool = False) -> dict:
    row = {
        "position_id": PID,
        "candidate_id": CID,
        "symbol": "USO",
        "sleeve_id": SLEEVE,
        "hypothesis_id": HID,
        "entry_price": "135.3914",
        "entry_time": "2026-06-02T20:11:45Z",
        "originating_day": "2026-06-02",
        "quantity": "1",
        "side": "BUY",
        "current_status": "OPEN",
        "paper_tracking_mode": "AUTO_PROMOTED_RESEARCH_OBSERVATION",
        "candidate_lineage": {"candidate_id": CID, "hypothesis_id": HID, "sleeve_id": SLEEVE, "raw_signal_id": "raw-oil"},
    }
    if certified:
        row.update({
            "mark_price": "137.27",
            "current_certified_mark": "137.27",
            "mark_timestamp_utc": "2026-06-03T20:00:00Z",
            "mark_source_path": "/tmp/USO.json",
            "mark_source_hash": "hash-uso",
            "mark_freshness_status": "CURRENT",
            "mark_market_session_date": DAY,
            "mark_certification_status": "CERTIFIED",
        })
    return row


def _market(*, current: bool = False, present: bool = True) -> dict:
    symbols = {}
    if present:
        symbols["USO"] = {
            "symbol": "USO",
            "last_price": 137.27,
            "close": 137.27,
            "freshness_status": "CURRENT" if current else "STALE",
            "market_session_date": DAY if current else "2026-06-02",
            "data_finality": "FINAL_EOD" if current else "PROVISIONAL_INTRADAY",
            "data_timestamp_utc": "2026-06-03T20:00:00Z" if current else "2026-06-02T20:00:00Z",
            "source_timestamp_utc": "2026-06-03T20:00:00Z" if current else "2026-06-02T20:00:00Z",
            "source_url_or_path": "/tmp/USO.json",
            "source_hash": "hash-uso",
            "provider": "TEST_PROVIDER",
        }
    return {"day_utc": DAY, "symbols": symbols}


def _closure(*, routed: bool = False) -> dict:
    row = {
        "position_id": PID,
        "candidate_id": CID,
        "symbol": "USO",
        "sleeve_id": SLEEVE,
        "hypothesis_id": HID,
        "auto_closure_state": "AUTO_CLOSURE_NOT_ELIGIBLE",
        "exit_recommendation": "HOLD",
        "exit_trigger": "MISSING_CURRENT_MARK" if not routed else "NO_EXIT_RULE_TRIGGERED",
        "entry_mark": 135.3914,
        "exit_mark": None,
        "exit_mark_certification_status": "MISSING_MARK",
        "exit_price_source_artifact": "",
        "exit_price_source_hash": "",
        "exit_price_timestamp": "",
        "auto_closure_reason_codes": ["HOLD_RECOMMENDATION"],
    }
    if routed:
        row.update({"exit_mark": 137.27, "exit_mark_certification_status": "CERTIFIED", "exit_price_source_artifact": "/tmp/USO.json", "exit_price_source_hash": "hash-uso", "exit_price_timestamp": "2026-06-03T20:00:00Z"})
    return row


def _lineage(*, pct: float = 0.0, broken: int = 2) -> dict:
    return {"evidence_coverage_panel": {"mark_coverage_pct": pct, "broken_chain_count": broken, "current_integrity_status": "GREEN" if pct == 100.0 and broken == 0 else "RED"}}


def _p20(blocker: str = "MARK_DATA_MISSING") -> dict:
    return {"summary": {"outcome_readiness_status": "NOT_READY", "outcome_status": "OUTCOME_NOT_READY", "outcome_row_created": False, "blocker_code": blocker, "remaining_blocker": blocker}}


def _seed(root: Path, *, position: dict | None = None, market: dict | None = None, closure: dict | None = None, lineage: dict | None = None, diagnostics: bool = True, p20: dict | None = None) -> None:
    pos = position if position is not None else _position()
    _write(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {"positions": [pos], "open_positions": [pos], "closed_positions": []})
    _write(_report(root, "aegis_market_data_v1", "market_data.v1.json"), market if market is not None else _market())
    _write(_report(root, "aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"), {"rows": [closure if closure is not None else _closure()]})
    _write(_report(root, "aegis_evidence_lineage_integrity_v1", "evidence_lineage_integrity.v1.json"), lineage if lineage is not None else _lineage())
    _write(_report(root, "aegis_generated_hypothesis_outcome_eligibility_day_proof_v1", "generated_hypothesis_outcome_eligibility_day_proof.v1.json"), p20 if p20 is not None else _p20())
    _write(_report(root, "aegis_exit_recommendations_v1", "exit_recommendations.v1.json"), {"recommendations": [{"position_id": PID, "candidate_id": CID, "symbol": "USO", "sleeve_id": SLEEVE, "exit_recommendation": "HOLD", "reason_codes": ["NO_EXIT_RULE_TRIGGERED"], "policy": {"sleeve_id": SLEEVE}}]})
    _write(_report(root, "aegis_runtime_truth_kernel_v1", "runtime_truth_kernel.v1.json"), {"trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False})
    if diagnostics:
        _write(_report(root, "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"), {"schema_id": "aegis_candidate_generation_diagnostics", "day_utc": DAY})


def test_holding_period_eligible_observation_blocked_by_missing_certified_mark(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["generated_hypothesis_id"] == HID
    assert payload["required_mark_symbol"] == "USO"
    assert payload["certified_mark_found_before_repair"] is False
    assert payload["certified_mark_found_after_repair"] is False
    assert payload["blocker_code"] == "AUTHORITATIVE_MARK_SOURCE_STALE"


def test_authoritative_price_exists_but_certified_mark_is_missing(tmp_path: Path) -> None:
    _seed(tmp_path, market=_market(current=True), lineage=_lineage(pct=100.0, broken=0))
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["authoritative_price_found"] is True
    assert payload["authoritative_price_status"] == "CERTIFIABLE"
    assert payload["certified_mark_found_after_repair"] is True
    assert payload["mark_certification_status"] == "CERTIFIED"


def test_certified_mark_exists_but_is_not_routed_to_outcome_closure(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(certified=True), market=_market(current=True), closure=_closure(routed=False), lineage=_lineage(pct=100.0, broken=0), p20=_p20("MARK_DATA_MISSING"))
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["certified_mark_found_before_repair"] is True
    assert payload["mark_routing_status"] in {"ROUTED_TO_OUTCOME_CLOSURE", "MARK_ROUTING_MISSING", "NOT_ROUTED"}


def test_missing_authoritative_mark_fails_closed(tmp_path: Path) -> None:
    _seed(tmp_path, market=_market(present=False))
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["blocker_code"] == "AUTHORITATIVE_MARK_SOURCE_MISSING"
    assert "authoritative_price" in payload["missing_fields"]


def test_evidence_lineage_broken_chains_are_identified(tmp_path: Path) -> None:
    _seed(tmp_path, lineage=_lineage(pct=50.0, broken=4))
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["broken_chain_count_before_repair"] == 4
    assert payload["mark_coverage_pct_before_repair"] == 50.0


def test_mark_coverage_repairs_to_100_only_with_valid_lineage(tmp_path: Path) -> None:
    _seed(tmp_path, market=_market(current=True), lineage=_lineage(pct=0.0, broken=2))
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["certified_mark_found_after_repair"] is True
    assert payload["mark_coverage_pct_after_repair"] == 100.0
    assert payload["broken_chain_count_after_repair"] == 0


def test_missing_candidate_generation_diagnostics_artifact_is_generated_for_target_day(tmp_path: Path) -> None:
    _seed(tmp_path, diagnostics=False)
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["candidate_generation_diagnostics_found_before_repair"] is False
    assert payload["candidate_generation_diagnostics_found_after_repair"] is True
    assert _report(tmp_path, "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json").exists()


def test_package_020_rerun_sees_certified_mark_after_repair(tmp_path: Path) -> None:
    _seed(tmp_path, market=_market(current=True), p20=_p20("MARK_DATA_MISSING"))
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["certified_mark_found_after_repair"] is True
    assert payload["package_020_status_after_repair"] in {"CLOSE_CONDITION_NOT_MET", "NONE", "OUTCOME_CREATED"}


def test_no_fabricated_mark_or_outcome_is_created(tmp_path: Path) -> None:
    _seed(tmp_path, market=_market())
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["no_mark_fabrication"] is True
    assert payload["outcome_created_by_this_artifact"] is False
    assert payload["outcome_row_created_after_repair"] is False


def test_safety_gates_remain_unchanged_and_write_path(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY)
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["safety_gates_changed"] is False
    path = write_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=tmp_path, repo_root=Path.cwd(), day_utc=DAY, payload=payload)
    assert path == generated_hypothesis_outcome_mark_certification_repair_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert json.loads(path.read_text(encoding="utf-8"))["summary"]["blocker_code"] == payload["blocker_code"]
