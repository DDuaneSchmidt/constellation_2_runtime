from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from ops.aegis.canonical_operator_state_v1 import build_candidate_ui_projection_v1
from ops.aegis.run_history_v1 import append_candidate_diagnostics_run_history_v1, latest_run_row_v1
from ops.aegis.operator_action_command_contracts_v1 import API_COMMAND, command_registry_v1, execute_aegis_command_v1
from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import _attach_candidate_ui_projection_v1, _candidate_projection_debug_v1, _operator_cockpit_payload

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"


def _source(path: str, generated_at: str, *, found: bool = True) -> dict:
    return {"path": path, "hash": "a" * 64, "found": found, "generated_at": generated_at, "freshness_status": "CURRENT"}


def _payloads(day: str = "2026-05-26", count: int = 17) -> dict:
    contracts = [{"candidate_id": f"candidate-{idx}", "symbol": f"S{idx}"} for idx in range(count)]
    queue_rows = [
        {"candidate_id": row["candidate_id"], "symbol": row["symbol"], "status": "AWAITING_REVIEW", "paper_trade_eligible": True, "live_trade_eligible": False}
        for row in contracts
    ]
    review_rows = [{"candidate_id": row["candidate_id"], "symbol": row["symbol"], "status": "AWAITING_REVIEW"} for row in contracts]
    return {
        "candidate_generation_diagnostics": {"schema_id": "aegis_candidate_generation_diagnostics", "day_utc": day, "generated_at": "2026-05-27T00:09:53Z"},
        "candidate_contracts": {"schema_id": "aegis_candidate_contracts", "day_utc": day, "generated_at_utc": "2026-05-27T00:09:54Z", "candidate_contracts": contracts},
        "candidate_review_packet": {"schema_id": "candidate_review_packet", "day_utc": day, "generated_at_utc": "2026-05-27T00:09:55Z", "candidate_count": count, "review_candidates": review_rows},
        "paper_review_queue": {"schema_id": "paper_review_queue", "day_utc": day, "generated_at_utc": "2026-05-27T00:09:55Z", "rows": queue_rows, "status_counts": {"AWAITING_REVIEW": count}},
        "paper_trade_outcomes": {"schema_id": "paper_trade_outcomes", "day_utc": day, "generated_at_utc": "2026-05-27T00:09:55Z", "rows": []},
    }


def _sources() -> dict:
    return {
        "candidate_generation_diagnostics": _source("/truth/diag.json", "2026-05-27T00:09:53Z"),
        "candidate_contracts": _source("/truth/contracts.json", "2026-05-27T00:09:54Z"),
        "candidate_review_packet": _source("/truth/packet.json", "2026-05-27T00:09:55Z"),
        "paper_review_queue": _source("/truth/queue.json", "2026-05-27T00:09:55Z"),
        "paper_trade_outcomes": _source("/truth/outcomes.json", "2026-05-27T00:09:55Z"),
    }


def test_candidate_ui_projection_uses_governed_contracts_and_paper_queue() -> None:
    projection = build_candidate_ui_projection_v1(payloads=_payloads(), sources=_sources(), day_utc="2026-05-26", canonical_generated_at="2026-05-27T00:10:00Z")

    assert projection["projection_status"] == "AVAILABLE"
    assert projection["diagnostics_status"] == "AVAILABLE"
    assert projection["paper_review_queue_status"] == "AVAILABLE"
    assert projection["candidate_contract_count"] == 17
    assert projection["paper_review_queue_count"] == 17
    assert projection["awaiting_review_count"] == 17
    assert projection["reviewable_candidate_count"] == 17
    assert len(projection["paper_review_queue_rows"]) == 17
    assert projection["trade_advice_allowed"] is False
    assert projection["broker_submit_transmit_allowed"] is False
    assert projection["autonomous_execution_allowed"] is False


def test_candidate_ui_projection_collapses_approval_only_rows_to_paper_trade_action() -> None:
    payloads = _payloads()
    for idx in range(4):
        payloads["paper_review_queue"]["rows"][idx]["status"] = "APPROVED_FOR_PAPER"
    payloads["paper_review_queue"]["status_counts"] = {"AWAITING_REVIEW": 13, "APPROVED_FOR_PAPER": 4}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=_sources(), day_utc="2026-05-26", canonical_generated_at="2026-05-27T00:10:00Z")

    assert projection["awaiting_review_count"] == 17
    first_row = projection["paper_workflow_rows"][0]
    assert first_row["workflow_state"] == "AWAITING_REVIEW"
    assert first_row["workflow_timeline"]["reviewed"] is False
    assert first_row["workflow_timeline"]["approved"] is False


def test_candidate_ui_projection_surfaces_wrong_day_and_count_mismatch() -> None:
    payloads = _payloads(day="2026-05-25", count=17)
    payloads["paper_review_queue"]["rows"] = payloads["paper_review_queue"]["rows"][:-1]
    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=_sources(), day_utc="2026-05-26", canonical_generated_at="2026-05-27T00:10:00Z")

    assert projection["projection_status"] == "PROJECTION_MISMATCH"
    assert any(reason.startswith("WRONG_DAY:candidate_generation_diagnostics") for reason in projection["mismatch_reasons"])
    assert any(reason.startswith("CONTRACT_QUEUE_COUNT_MISMATCH") for reason in projection["mismatch_reasons"])


def test_candidate_ui_projection_marks_stale_canonical_projection() -> None:
    projection = build_candidate_ui_projection_v1(payloads=_payloads(), sources=_sources(), day_utc="2026-05-26", canonical_generated_at="2026-05-27T00:00:00Z")

    assert projection["projection_status"] == "CANONICAL_PROJECTION_STALE"
    assert any(reason.startswith("CANONICAL_OLDER_THAN_CANDIDATE_SOURCES") for reason in projection["mismatch_reasons"])
    assert "TARGET_DAY=2026-05-26 npm run aegis:candidate-diagnostics" in projection["repair_commands"]



def test_candidate_ui_projection_distinguishes_diagnostic_outputs_from_valid_contracts() -> None:
    payloads = _payloads(day="2026-05-27", count=0)
    payloads["candidate_generation_diagnostics"].update({
        "candidate_generation_status": "RAN",
        "operator_interpretation": "DATA_BLOCKED",
        "total_raw_signals": 17,
        "diagnostic_candidate_outputs": 4,
        "total_candidates_generated": 0,
        "valid_candidate_contracts": 0,
        "rejected_candidate_contracts": 17,
        "total_candidates_rejected": 21,
        "total_sleeves_expected": 7,
        "total_sleeves_run": 3,
    })
    payloads["candidate_contracts"]["candidates_created"] = 0
    payloads["candidate_contracts"]["candidates_rejected"] = 17
    payloads["candidate_contracts"]["rejection_reasons"] = [{"reason": "ENTRY_REFERENCE_PRICE_STALE", "count": 17}]
    payloads["candidate_contracts"]["rejected_raw_signals"] = [{
        "symbol": "AAPL",
        "rejection_reason": "ENTRY_REFERENCE_PRICE_STALE",
        "price_timestamp": "2026-05-26T16:00:00Z",
        "candidate_snapshot_timestamp": "2026-05-27T13:50:00Z",
        "freshness_window_seconds": 0,
        "stale_by_seconds": 86400,
        "freshness_policy_mode": "CURRENT_SESSION_CERTIFIED",
        "stale_reason": "registry_status=STALE;market_session_date=2026-05-26;required_day=2026-05-27",
    }]
    payloads["run_history"] = {
        "schema_id": "aegis_run_history",
        "day_utc": "2026-05-27",
        "runs": [{
            "run_id": "run-950",
            "started_at": "2026-05-27T13:50:00Z",
            "completed_at": "2026-05-27T14:18:02Z",
            "command": "npm run aegis:candidate-diagnostics",
            "target_day": "2026-05-27",
            "status": "SUCCESS",
            "sleeves_expected": 7,
            "sleeves_run": 3,
            "raw_signals": 17,
            "diagnostic_candidate_outputs": 4,
            "diagnostics_candidates_generated": 0,
            "valid_candidate_contracts": 0,
            "candidate_contracts_created": 0,
            "rejected_candidate_contracts": 17,
            "reviewable_current_day_candidates": 0,
            "carried_forward_candidates": 0,
            "rejected_count": 21,
            "artifact_mismatch": True,
            "artifact_mismatch_status": "DIAGNOSTIC_OUTPUTS_REJECTED_BY_CONTRACT_VALIDATION",
            "artifact_mismatch_reason": "ENTRY_REFERENCE_PRICE_STALE",
            "mismatch_reason": "ENTRY_REFERENCE_PRICE_STALE",
            "mismatch_explanation": {"operator_message": "Diagnostics found candidate-like outputs, but 0 passed candidate contract validation.", "reason": "ENTRY_REFERENCE_PRICE_STALE"},
        }],
    }
    sources = {**_sources(), "run_history": _source("/truth/run_history.json", "2026-05-27T14:18:03Z")}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:18:04Z")

    assert projection["diagnostics_status"] == "AVAILABLE"
    assert projection["raw_signal_count"] == 17
    assert projection["diagnostic_candidate_outputs"] == 4
    assert projection["diagnostics_candidates_generated"] == 0
    assert projection["valid_candidate_contracts"] == 0
    assert projection["candidate_contracts_created"] == 0
    assert projection["rejected_candidate_contracts"] == 17
    assert projection["candidate_contract_count"] == 0
    assert projection["sleeves_expected"] == 7
    assert projection["sleeves_run"] == 3
    assert projection["rejected_count"] == 21
    assert projection["run_visibility_status"] == "DIAGNOSTIC_OUTPUTS_REJECTED_BY_CONTRACT_VALIDATION"
    assert projection["run_summary"]["mismatch_reason"] == "ENTRY_REFERENCE_PRICE_STALE"
    assert projection["run_summary"]["mismatch_explanation"]["operator_message"] == "Diagnostics found candidate-like outputs, but 0 passed candidate contract validation."
    assert projection["run_summary"]["mismatch_explanation"]["price_timestamp"] == "2026-05-26T16:00:00Z"
    assert projection["run_summary"]["mismatch_explanation"]["freshness_policy_mode"] == "CURRENT_SESSION_CERTIFIED"
    assert projection["run_summary"]["mismatch_explanation"]["stale_by_seconds"] == 86400
    assert "required_day=2026-05-27" in projection["run_summary"]["mismatch_explanation"]["stale_reason"]


    

def test_candidate_ui_projection_surfaces_diagnostic_failed_producer_rejection_rows() -> None:
    payloads = _payloads(day="2026-05-27", count=0)
    payloads["candidate_generation_diagnostics"].update({
        "total_candidates_rejected": 6,
        "diagnostic_candidate_outputs": 4,
        "total_candidates_generated": 0,
        "total_sleeves_expected": 7,
        "total_sleeves_run": 1,
        "failed_producers": [
            {
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "run_status": "BLOCKED",
                "canonical_blocker": "ALLOWED_SYMBOL_MISMATCH",
                "reason_codes": ["ALLOWED_SYMBOL_MISMATCH", "MARKET_DATA_SHA_MISMATCH", "STALE_MISMATCHED_INTENT_REJECTED"],
                "next_repair_action": "npm run aegis:repair-candidate-readiness",
                "producer_command": "TARGET_DAY=2026-05-27 npm run aegis:candidate-diagnostics",
            },
            {
                "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                "run_status": "BLOCKED",
                "canonical_blocker": "SLEEVE_INPUT_REQUIREMENT_BLOCKED",
                "reason_codes": ["SLEEVE_INPUT_REQUIREMENT_BLOCKED", "market.volatility.VIX"],
                "next_repair_action": "npm run aegis:repair-candidate-readiness",
            },
        ],
    })
    sources = {**_sources(), "run_history": _source("/truth/run_history.json", "2026-05-27T14:18:03Z")}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:18:04Z")

    rows = projection["diagnostic_rejection_rows"]
    assert projection["diagnostic_rejection_count"] == 2
    assert rows[0]["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert rows[0]["rejection_reason"] == "ALLOWED_SYMBOL_MISMATCH"
    assert "STALE_MISMATCHED_INTENT_REJECTED" in rows[0]["reason_codes"]
    assert rows[0]["source_collection"] == "failed_producers"
    assert rows[0]["source_artifact_path"] == "/truth/diag.json"
    assert projection["run_summary"]["diagnostic_rejection_rows"] == rows
    assert projection["run_summary"]["mismatch_explanation"]["diagnostic_rejection_rows"] == rows


def test_candidate_ui_projection_distinguishes_missing_diagnostics_from_zero_candidates() -> None:
    payloads = _payloads(day="2026-05-27", count=0)
    payloads["candidate_generation_diagnostics"] = {}
    sources = {**_sources(), "candidate_generation_diagnostics": _source("", "", found=False)}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:18:04Z")

    assert projection["diagnostics_status"] == "MISSING"
    assert projection["run_visibility_status"] == "DIAGNOSTICS_MISSING"
    assert projection["raw_signal_count"] == 0
    assert projection["diagnostics_candidates_generated"] == 0



def test_run_history_does_not_backfill_generated_at_as_run_start() -> None:
    payloads = _payloads(day="2026-05-27", count=0)
    payloads["candidate_generation_diagnostics"].update({
        "generated_at": "2026-05-27T14:18:02Z",
        "total_raw_signals": 17,
        "diagnostic_candidate_outputs": 4,
        "total_candidates_generated": 0,
        "valid_candidate_contracts": 0,
        "rejected_candidate_contracts": 17,
        "total_candidates_rejected": 21,
        "total_sleeves_expected": 7,
        "total_sleeves_run": 3,
    })
    payloads["run_history"] = {
        "schema_id": "aegis_run_history",
        "day_utc": "2026-05-27",
        "runs": [{
            "run_id": "run-950",
            "started_at": "",
            "run_start": "NOT_RECORDED",
            "completed_at": "2026-05-27T14:18:02Z",
            "market_snapshot_time": "2026-05-27T13:35:05Z",
            "candidate_snapshot_time": "2026-05-27T13:50:00Z",
            "diagnostics_started_at": "NOT_RECORDED",
            "diagnostics_completed_at": "2026-05-27T14:18:02Z",
            "projection_generated_at": "2026-05-27T14:19:00Z",
            "status": "PARTIAL",
            "raw_signals": 17,
            "diagnostic_candidate_outputs": 4,
            "diagnostics_candidates_generated": 0,
            "valid_candidate_contracts": 0,
            "candidate_contracts_created": 0,
            "rejected_candidate_contracts": 17,
            "rejected_count": 21,
        }],
    }
    sources = {**_sources(), "run_history": _source("/truth/run_history.json", "2026-05-27T14:19:00Z")}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:19:00Z")

    assert projection["run_summary"]["run_start"] == "NOT_RECORDED"
    assert projection["run_summary"]["started_at"] == ""
    assert projection["run_summary"]["candidate_snapshot_time"] == "2026-05-27T13:50:00Z"
    assert projection["run_summary"]["diagnostics_completed_at"] == "2026-05-27T14:18:02Z"
    assert projection["run_summary"]["projection_generated_at"] == "2026-05-27T14:19:00Z"


def test_run_history_records_contract_validation_counts(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    day = "2026-05-27"
    contracts_path = truth / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json"
    contracts_path.parent.mkdir(parents=True, exist_ok=True)
    contracts_path.write_text(
        '{"candidates_created":0,"candidates_rejected":17,"candidate_contracts":[],"rejection_reasons":[{"reason":"ENTRY_REFERENCE_PRICE_STALE","count":17}],"rejected_raw_signals":[{"symbol":"AAPL","rejection_reason":"ENTRY_REFERENCE_PRICE_STALE"}]}',
        encoding="utf-8",
    )
    queue_path = truth / "reports" / "aegis_paper_review_queue_v1" / day / "paper_review_queue.v1.json"
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue_path.write_text(
        '{"rows":[{"candidate_id":"prior","originating_day":"2026-05-26","status":"AWAITING_REVIEW"}]}',
        encoding="utf-8",
    )

    history = append_candidate_diagnostics_run_history_v1(
        truth_root=truth,
        day_utc=day,
        command="npm run aegis:candidate-diagnostics",
        diagnostics={"generated_at": "2026-05-27T14:18:02Z", "diagnostic_candidate_outputs": 4, "total_candidates_generated": 0},
        candidate_contracts_path=str(contracts_path),
        paper_review_queue_path=str(queue_path),
    )

    row = history["runs"][0]
    assert row["diagnostic_candidate_outputs"] == 4
    assert row["valid_candidate_contracts"] == 0
    assert row["rejected_candidate_contracts"] == 17
    assert row["reviewable_current_day_candidates"] == 0
    assert row["carried_forward_candidates"] == 1
    assert row["artifact_mismatch_status"] == "DIAGNOSTIC_OUTPUTS_REJECTED_BY_CONTRACT_VALIDATION"
    assert row["artifact_mismatch_reason"] == "ENTRY_REFERENCE_PRICE_STALE"
    assert row["mismatch_explanation"]["operator_message"] == "Diagnostics found candidate-like outputs, but 0 passed candidate contract validation."


def test_latest_run_history_uses_explicit_run_id_before_timestamps() -> None:
    history = {
        "latest_run_id": "run-new",
        "runs": [
            {"run_id": "run-old", "candidate_snapshot_time": "2026-05-27T14:42:50Z", "completed_at": "2026-05-27T14:42:52Z"},
            {"run_id": "run-new", "candidate_snapshot_time": "2026-05-27T13:50:00Z", "completed_at": "2026-05-27T14:44:34Z"},
        ],
    }

    assert latest_run_row_v1(history)["run_id"] == "run-new"


def test_run_history_prefers_candidate_snapshot_time_over_generated_at(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    day = "2026-05-27"
    snapshot_path = truth / "reports" / "operator_state_snapshot_v1" / day / "operator_state_snapshot.v1.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        '{"generated_at_utc":"2026-05-27T14:18:02Z","operator_today_projection":{"candidate_snapshot_generated_at":"2026-05-27T13:50:00Z"}}',
        encoding="utf-8",
    )
    history = append_candidate_diagnostics_run_history_v1(
        truth_root=truth,
        day_utc=day,
        command="npm run aegis:candidate-diagnostics",
        diagnostics={
            "generated_at": "2026-05-27T14:18:02Z",
            "total_raw_signals": 17,
            "total_candidates_generated": 4,
        },
    )

    row = history["runs"][0]
    assert row["run_start"] == "NOT_RECORDED"
    assert row["started_at"] == ""
    assert row["candidate_snapshot_time"] == "2026-05-27T13:50:00Z"
    assert row["diagnostics_completed_at"] == "2026-05-27T14:18:02Z"


def test_candidate_page_renders_governed_projection_and_refresh_action() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert "candidate_ui_projection" in source
    assert "Real candidate contracts" in source
    assert "Awaiting paper review" in source
    assert "CANDIDATE_PROJECTION_AVAILABLE" in source
    assert "PROJECTION_MISMATCH" in source
    assert "CANONICAL_PROJECTION_STALE" in source
    assert "REFRESH_CANDIDATE_PROJECTION" in source
    assert "REFRESH_CANDIDATE_CONTRACTS" in source
    assert "Price timestamp" in source
    assert "Stale by seconds" in source
    assert "paper_review_queue_rows" in source


def test_refresh_candidate_projection_command_uses_argv_sequence_and_keeps_policy_false(tmp_path: Path) -> None:
    commands = command_registry_v1()["commands_by_id"]
    command = commands["REFRESH_CANDIDATE_PROJECTION"]
    assert command["label"] == "Refresh Candidate Projection"
    assert command["action_type"] == API_COMMAND
    assert command["endpoint"] == "/api/aegis/commands/execute"
    assert command["safety_classification"]["trade_advice_allowed"] is False
    assert command["safety_classification"]["broker_submit_transmit_allowed"] is False
    assert command["safety_classification"]["autonomous_execution_allowed"] is False

    captured = []

    def runner(argv, **kwargs):
        captured.append({"argv": argv, "kwargs": kwargs})
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    result = execute_aegis_command_v1(
        {"command_id": "REFRESH_CANDIDATE_PROJECTION", "target_type": "candidate_projection", "target_id": "2026-05-26", "payload": {"day_utc": "2026-05-26"}},
        truth_root=tmp_path,
        day_utc="2026-05-26",
        command_runner=runner,
    )

    assert result["ok"] is True
    assert result["portal_action_id"] == "refresh_candidate_projection"
    assert result["shell"] is False
    assert result["command_argv_sequence"] == [
        ["npm", "run", "aegis:signal-evidence-graph"],
        ["npm", "run", "aegis:candidate-contracts"],
        ["npm", "run", "aegis:signal-death-report"],
        ["npm", "run", "aegis:candidate-diagnostics"],
        ["npm", "run", "aegis:paper:review-queue"],
        ["npm", "run", "aegis:roll-candidate-state"],
        ["npm", "run", "aegis:run-history"],
        ["npm", "run", "aegis:canonical-operator-state"],
        ["npm", "run", "aegis:audit"],
        ["npm", "run", "aegis:portal-smoke"],
    ]
    assert [row["argv"] for row in captured] == result["command_argv_sequence"]
    assert all(row["kwargs"]["shell"] is False for row in captured)
    assert result["safety_policy_summary"]["trade_advice_allowed"] is False
    assert result["safety_policy_summary"]["broker_execution_allowed"] is False
    assert result["safety_policy_summary"]["autonomous_execution_allowed"] is False


def test_candidate_projection_debug_reports_exact_runtime_paths(tmp_path: Path) -> None:
    day = "2026-05-26"
    canonical_path = tmp_path / "reports" / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"
    canonical_path.parent.mkdir(parents=True)
    canonical_path.write_text("{}", encoding="utf-8")

    debug = _candidate_projection_debug_v1(
        truth_root=tmp_path,
        day_utc=day,
        canonical_path=canonical_path,
        canonical={},
        projection={},
        operational_day_source="test",
    )

    assert debug["truth_root"] == str(tmp_path.resolve())
    assert debug["canonical_operator_state_path"] == str(canonical_path)
    assert debug["canonical_operator_state_exists"] is True
    assert debug["has_candidate_ui_projection"] is False
    assert debug["candidate_diagnostics_path"].endswith("candidate_generation_diagnostics.v1.json")
    assert debug["candidate_diagnostics_exists"] is False
    assert debug["paper_review_queue_path"].endswith("paper_review_queue.v1.json")
    assert debug["paper_review_queue_exists"] is False
    assert debug["api_server_cwd"]
    assert debug["static_bundle_path"].endswith("operator_shell/pages/index.js")
    assert debug["broker_execution_allowed"] is False
    assert debug["autonomous_execution_allowed"] is False
    assert debug["trade_advice_allowed"] is False


def test_candidate_projection_debug_reports_counts_after_projection(tmp_path: Path) -> None:
    day = "2026-05-26"
    canonical_path = tmp_path / "reports" / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"
    canonical_path.parent.mkdir(parents=True)
    canonical_path.write_text("{}", encoding="utf-8")
    projection = build_candidate_ui_projection_v1(payloads=_payloads(), sources=_sources(), day_utc=day, canonical_generated_at="2026-05-27T00:10:00Z")

    debug = _candidate_projection_debug_v1(
        truth_root=tmp_path,
        day_utc=day,
        canonical_path=canonical_path,
        canonical={"candidate_ui_projection": projection, "generated_at_utc": "2026-05-27T00:10:00Z"},
        projection=projection,
        operational_day_source="test",
    )

    assert debug["has_candidate_ui_projection"] is True
    assert debug["canonical_has_candidate_ui_projection"] is True
    assert debug["candidate_contract_count"] == 17
    assert debug["awaiting_review_count"] == 0
    assert debug["reviewable_candidate_count"] == 0
    assert debug["raw_projection_awaiting_review_count"] == 17
    assert debug["raw_projection_reviewable_candidate_count"] == 17
    assert debug["diagnostics_status"] == "AVAILABLE"
    assert debug["paper_review_queue_status"] == "AVAILABLE"


def test_operator_state_snapshot_overlay_attaches_canonical_candidate_projection(tmp_path: Path) -> None:
    day = "2026-05-26"
    projection = build_candidate_ui_projection_v1(payloads=_payloads(), sources=_sources(), day_utc=day, canonical_generated_at="2026-05-27T00:10:00Z")
    canonical_path = tmp_path / "reports" / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"
    canonical_path.parent.mkdir(parents=True)
    canonical_path.write_text(
        '{"schema_id":"aegis_canonical_operator_state","day_utc":"2026-05-26","generated_at_utc":"2026-05-27T00:10:00Z","candidate_ui_projection":'
        + __import__("json").dumps(projection)
        + "}",
        encoding="utf-8",
    )

    enriched = _attach_candidate_ui_projection_v1(
        {"schema_id": "operator_state_snapshot", "day_utc": day, "candidate_ui_projection": {}},
        truth_root=tmp_path,
        day_utc=day,
        operational_day_source="test-state-snapshot",
    )

    assert enriched["has_candidate_ui_projection"] is True
    assert enriched["candidate_ui_projection"]["candidate_contract_count"] == 17
    assert enriched["candidate_ui_projection"]["awaiting_review_count"] == 17
    assert enriched["candidate_projection_debug"]["operational_day_source"] == "test-state-snapshot"
    assert enriched["candidate_projection_debug"]["canonical_operator_state_path"] == str(canonical_path)
    assert enriched["candidate_projection_debug"]["day_path_invariant_ok"] is True
    assert enriched["candidate_ui_projection"]["trade_advice_allowed"] is False
    assert enriched["candidate_ui_projection"]["broker_submit_transmit_allowed"] is False
    assert enriched["candidate_ui_projection"]["autonomous_execution_allowed"] is False


def test_candidate_page_exposes_human_reviewed_paper_workflow_controls() -> None:
    source = PAGES.read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    for text in ["Confirm Captured", "Mark Not Captured", "Defer", "View / Correct Capture", "Record Exit"]:
        assert text in source
    for command_id in ["CONFIRM_CANDIDATE_CAPTURED", "MARK_CANDIDATE_NOT_CAPTURED", "DEFER_CANDIDATE", "CORRECT_CANDIDATE_CAPTURE", "RECORD_PAPER_ENTRY", "RECORD_PAPER_EXIT"]:
        assert command_id in source
    for warning in ["NO LIVE EXECUTION", "NO BROKER ORDER", "PAPER ONLY", "HUMAN REVIEW REQUIRED", "LIVE TRADING DISABLED"]:
        assert warning in source
    assert "paperWorkflowState" in source
    assert "renderPaperWorkflowTimeline" in source
    assert "paper-candidate-action-form" in source
    assert "Origin / expiry" in source
    assert "originating_day" in source
    assert "rollover_status" in source
    assert "Persistent candidates" in source
    assert "submitOperatorWorkflowCommand" in main
    assert "updatePaperCandidateRowOptimistically" not in main


def test_record_entry_confirmation_modal_has_clear_escape_paths() -> None:
    source = PAGES.read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "data-paper-entry-dialog" in source
    assert "paper-entry-modal-footer" in source
    assert "data-paper-entry-cancel" in source
    assert 'type="button" data-paper-entry-cancel' in source
    assert source.index("data-paper-entry-cancel") < source.index("data-paper-entry-submit")
    assert "closePaperTradeDialog" in main
    assert "resetPaperTradeDialog" in main
    assert "dialog[data-paper-entry-dialog]" in main
    assert 'event.target?.matches?.("dialog[data-paper-entry-dialog]")' in main
    assert "paperEntryCancel" in main
    assert "executeAegisCommand" not in main[main.index("const paperEntryCancel"):main.index("const manualCaptureOpen")]
    assert 'panel.addEventListener("cancel"' in main
    assert "resetPaperTradeDialog(panel);" in main
    assert ".candidate-review-dialog.paper-entry-dialog" in css
    assert ".candidate-review-dialog.paper-entry-dialog::backdrop" in css
    assert ".paper-entry-close-button" in css
    assert "z-index: 1000" in css


def test_manual_workflow_submit_loading_failure_and_success_paths_remain_escapable() -> None:
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    paper_submit_block = main[main.index('const paperCandidateForm = event.target.closest(".paper-candidate-action-form")'):main.index('const candidateActionForm = event.target.closest(".candidate-action-form")')]

    assert 'submitter.disabled = true' in paper_submit_block
    assert 'submitter.setAttribute("aria-busy", "true")' in paper_submit_block
    assert 'button-spinner' in paper_submit_block
    assert 'paperCandidateForm.dataset.paperSubmitting = "true"' in paper_submit_block
    assert 'dialog.dataset.paperSubmitting = "true"' in paper_submit_block
    assert '["APPROVE_CANDIDATE", "REJECT_CANDIDATE", "CONFIRM_CANDIDATE_CAPTURED", "MARK_CANDIDATE_NOT_CAPTURED", "DEFER_CANDIDATE", "CORRECT_CANDIDATE_CAPTURE", "RECORD_PAPER_ENTRY", "RECORD_PAPER_EXIT", "REVOKE_APPROVAL"].includes(commandId)' in paper_submit_block
    assert 'submitOperatorWorkflowCommand' in paper_submit_block
    assert 'updatePaperCandidateRowOptimistically' not in paper_submit_block
    assert 'if (result?.ok && dialog?.close) dialog.close();' in paper_submit_block
    assert 'statusNode.dataset.tone = "error"' in paper_submit_block
    assert 'paperCandidateForm.dataset.paperSubmitting = "false"' in paper_submit_block
    assert 'submitter.disabled = false' in paper_submit_block
    assert 'submitter.removeAttribute("aria-busy")' in paper_submit_block


def test_refresh_candidate_contracts_command_uses_market_registry_contract_argv_only(tmp_path: Path) -> None:
    commands = command_registry_v1()["commands_by_id"]
    command = commands["REFRESH_CANDIDATE_CONTRACTS"]
    assert command["label"] == "Refresh Candidate Contracts"
    assert command["action_type"] == API_COMMAND
    assert command["safety_classification"]["trade_advice_allowed"] is False
    assert command["safety_classification"]["broker_submit_transmit_allowed"] is False
    assert command["safety_classification"]["autonomous_execution_allowed"] is False

    captured = []

    def runner(argv, **kwargs):
        captured.append({"argv": argv, "kwargs": kwargs})
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    result = execute_aegis_command_v1(
        {"command_id": "REFRESH_CANDIDATE_CONTRACTS", "target_type": "candidate_contracts", "target_id": "2026-05-27", "payload": {"day_utc": "2026-05-27"}},
        truth_root=tmp_path,
        day_utc="2026-05-27",
        command_runner=runner,
    )

    assert result["ok"] is True
    assert result["portal_action_id"] == "refresh_candidate_contracts"
    assert result["shell"] is False
    assert result["command_argv_sequence"] == [
        ["npm", "run", "aegis:refresh-market-data"],
        ["npm", "run", "aegis:data-registry"],
        ["npm", "run", "aegis:candidate-contracts"],
    ]
    assert [row["argv"] for row in captured] == result["command_argv_sequence"]
    assert all(row["kwargs"]["shell"] is False for row in captured)
    assert result["safety_policy_summary"]["trade_advice_allowed"] is False
    assert result["safety_policy_summary"]["broker_execution_allowed"] is False
    assert result["safety_policy_summary"]["autonomous_execution_allowed"] is False

def test_operator_cockpit_builds_artifact_projection_when_canonical_projection_missing(tmp_path: Path) -> None:
    day = "2026-05-27"
    canonical_path = tmp_path / "reports" / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    canonical_path.write_text(
        json.dumps({"schema_id": "aegis_canonical_operator_state", "day_utc": day, "generated_at_utc": "2026-05-27T14:18:04Z", "safety": {}}),
        encoding="utf-8",
    )
    (tmp_path / "reports" / "aegis_operator_brief_v1" / day).mkdir(parents=True, exist_ok=True)
    (tmp_path / "reports" / "aegis_operator_brief_v1" / day / "operator_brief.v1.json").write_text('{"day_utc":"2026-05-27","generated_at_utc":"2026-05-27T14:18:04Z"}', encoding="utf-8")
    payloads = _payloads(day=day, count=3)
    paths = {
        "candidate_generation_diagnostics": tmp_path / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        "candidate_contracts": tmp_path / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json",
        "candidate_review_packet": tmp_path / "reports" / "aegis_candidate_review_packet_v1" / day / "candidate_review_packet.v1.json",
        "paper_review_queue": tmp_path / "reports" / "aegis_paper_review_queue_v1" / day / "paper_review_queue.v1.json",
        "paper_trade_outcomes": tmp_path / "reports" / "aegis_paper_trade_outcomes_v1" / day / "paper_trade_outcomes.v1.json",
    }
    for key, target in paths.items():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payloads[key]), encoding="utf-8")

    payload = _operator_cockpit_payload(tmp_path, day)

    assert payload["displayed_artifact_day"] == day
    assert payload["has_candidate_ui_projection"] is True
    assert payload["candidate_ui_projection"]["diagnostics_status"] == "AVAILABLE"
    assert payload["candidate_ui_projection"]["candidate_contract_count"] == 3
    assert payload["candidate_projection_debug"]["candidate_diagnostics_exists"] is True
    assert payload["candidate_projection_debug"]["day_path_invariant_ok"] is True
    assert payload["safety"]["broker_submit_transmit_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False


def test_candidates_route_has_no_implicit_payload_reference() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderNoOpportunityExplanation", 1)[1].split("function renderMissingOpportunityDiagnostics", 1)[0]

    assert "function renderNoOpportunityExplanation(opportunities = {}, payload = {})" in source
    assert "payload.verified_runtime_graph" in block
    assert "renderNoOpportunityExplanation(opportunities, payload)" in source





def test_candidate_ui_projection_carries_market_data_coverage_summary() -> None:
    payloads = _payloads(day="2026-05-27", count=0)
    payloads["market_data_coverage"] = {
        "schema_id": "aegis_market_data_coverage",
        "day_utc": "2026-05-27",
        "status": "BLOCKED",
        "required_symbol_count": 100,
        "certified_symbol_count": 22,
        "missing_symbol_count": 0,
        "stale_symbol_count": 78,
        "coverage_pct": 22.0,
        "stale_symbols": ["AAPL", "MSFT"],
        "missing_symbols": [],
        "blocking_consumers": ["C2_TREND_EQ_PRIMARY_V1"],
        "consumer_sleeves": ["C2_TREND_EQ_PRIMARY_V1"],
        "repair_action": "TARGET_DAY=2026-05-27 npm run aegis:repair-input-contracts",
    }
    sources = {**_sources(), "market_data_coverage": _source("/truth/market_data_coverage.v1.json", "2026-05-27T14:20:00Z")}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:21:00Z")

    summary = projection["market_data_coverage"]
    assert summary["status"] == "BLOCKED"
    assert summary["required_symbol_count"] == 100
    assert summary["certified_symbol_count"] == 22
    assert summary["stale_symbol_count"] == 78
    assert summary["blocking_consumers"] == ["C2_TREND_EQ_PRIMARY_V1"]
    assert projection["run_summary"]["market_data_coverage"]["source_path"] == "/truth/market_data_coverage.v1.json"
    source = PAGES.read_text(encoding="utf-8")
    assert "renderMarketDataCoveragePanel" in source
    assert "MARKET_DATA_COVERAGE_" in source



def test_dashboard_explains_coverage_ready_hash_mismatch_distinction() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert "Coverage is ready, but downstream sleeve contracts were generated against an older market-data hash. Run repair-input-contracts." in source
    assert "DOWNSTREAM_CONTRACT_HASH_STALE" in source
    assert "market_data_hash_status" in source


def test_candidate_ui_projection_carries_input_contract_reconciliation_rows() -> None:
    payloads = _payloads(day="2026-05-27", count=0)
    payloads["input_contract_reconciliation"] = {
        "schema_id": "aegis_input_contract_reconciliation",
        "day_utc": "2026-05-27",
        "rows": [{
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "contract_status": "OK",
            "allowed_symbol_status": "MISMATCH",
            "market_data_hash_status": "MISMATCH",
            "expected_allowed_symbols": ["AAPL", "MSFT"],
            "actual_symbols": ["SPY"],
            "exact_mismatch_cause": "MARKET_DATA_SHA_MISMATCH expected=x actual=y",
            "repair_action": "TARGET_DAY=2026-05-27 npm run aegis:repair-input-contracts",
            "blocking_current_day_valid_candidates": True,
        }],
    }
    sources = {**_sources(), "input_contract_reconciliation": _source("/truth/input_contract_reconciliation.v1.json", "2026-05-27T14:20:00Z")}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:21:00Z")

    assert projection["input_contract_reconciliation_count"] == 1
    assert projection["input_contract_reconciliation_rows"][0]["allowed_symbol_status"] == "MISMATCH"
    assert projection["run_summary"]["input_contract_reconciliation_rows"][0]["repair_action"].endswith("aegis:repair-input-contracts")
    bundle = PAGES.read_text(encoding="utf-8")
    assert "Producer contract mismatch table" in bundle
    assert "input_contract_reconciliation_rows" in bundle


def test_candidate_ui_projection_uses_paper_position_ledger_for_open_closed_legacy_counts() -> None:
    payloads = _payloads()
    payloads["paper_position_ledger"] = {
        "schema_id": "aegis_paper_position_ledger",
        "day_utc": "2026-05-27",
        "open_position_count": 1,
        "closed_position_count": 1,
        "legacy_capture_count": 1,
        "open_positions": [{"position_id": "paper-position:candidate-1", "candidate_id": "candidate-1", "symbol": "SPY", "current_status": "OPEN"}],
        "closed_positions": [{"position_id": "paper-position:candidate-2", "candidate_id": "candidate-2", "symbol": "QQQ", "current_status": "CLOSED"}],
        "legacy_captures": [{"position_id": "legacy:DOW", "symbol": "DOW", "current_status": "LEGACY", "legacy_classification": "LEGACY_CAPTURE"}],
    }
    sources = _sources()
    sources["paper_position_ledger"] = _source("/truth/paper_position_ledger.v1.json", "2026-05-27T14:21:00Z")

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:21:00Z")

    assert projection["paper_position_ledger_status"] == "AVAILABLE"
    assert projection["paper_position_open_count"] == 1
    assert projection["paper_position_closed_count"] == 1
    assert projection["legacy_capture_count"] == 1
    assert projection["open_paper_positions"][0]["symbol"] == "SPY"
    assert projection["historical_paper_positions"][0]["current_status"] == "CLOSED"
    assert projection["legacy_captures"][0]["legacy_classification"] == "LEGACY_CAPTURE"


def test_candidate_ui_projection_surfaces_exit_recommendations_for_open_paper_positions() -> None:
    payloads = _payloads()
    payloads["paper_position_ledger"] = {
        "schema_id": "aegis_paper_position_ledger",
        "day_utc": "2026-05-27",
        "open_position_count": 1,
        "closed_position_count": 0,
        "legacy_capture_count": 0,
        "open_positions": [{"position_id": "paper-position:candidate-1", "candidate_id": "candidate-1", "symbol": "SPY", "current_status": "OPEN", "unrealized_pnl": "42"}],
        "closed_positions": [],
        "legacy_captures": [],
    }
    payloads["exit_recommendations"] = {
        "schema_id": "aegis_exit_recommendations",
        "day_utc": "2026-05-27",
        "recommendation_count": 1,
        "recommendations": [{
            "position_id": "paper-position:candidate-1",
            "candidate_id": "candidate-1",
            "symbol": "SPY",
            "exit_recommendation": "EXIT_TAKE_PROFIT",
            "reason_codes": ["TAKE_PROFIT_THRESHOLD_REACHED"],
            "operator_action_required": True,
            "automatic_exit_allowed": False,
        }],
        "pnl_report_hooks": {"recommendation_counts": {"EXIT_TAKE_PROFIT": 1}},
    }
    sources = _sources()
    sources["paper_position_ledger"] = _source("/truth/paper_position_ledger.v1.json", "2026-05-27T14:21:00Z")
    sources["exit_recommendations"] = _source("/truth/exit_recommendations.v1.json", "2026-05-27T14:22:00Z")

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:23:00Z")

    assert projection["exit_recommendations_status"] == "AVAILABLE"
    assert projection["exit_recommendation_count"] == 1
    assert projection["exit_recommendation_counts"] == {"EXIT_TAKE_PROFIT": 1}
    row = next(item for item in projection["paper_workflow_rows"] if item["candidate_id"] == "candidate-1")
    assert row["exit_recommendation"]["exit_recommendation"] == "EXIT_TAKE_PROFIT"
    bundle = PAGES.read_text(encoding="utf-8")
    assert "renderOpenPaperPositionExitModals" in bundle
    assert "exit_reason_selected_by_operator" in bundle



def test_candidate_ui_projection_surfaces_paper_lifecycle_reconciliation() -> None:
    payloads = _payloads()
    payloads["paper_lifecycle_reconciliation"] = {
        "schema_id": "aegis_paper_lifecycle_reconciliation",
        "day_utc": "2026-05-27",
        "counts": {"awaiting_review": 30, "open_ledger_positions": 4, "failed_actions": 5},
        "operator_summary_messages": ["30 candidates still awaiting Record Entry", "PAPER_ENTRY_ACTION_FAILED"],
        "failed_actions": [{"status": "PAPER_ENTRY_ACTION_FAILED", "candidate_id": "candidate-1", "symbol": "BMY", "error": "paper receipt requires a candidate with APPROVED_FOR_PAPER status"}],
        "expected_open_but_missing": [{"candidate_id": "candidate-1", "symbol": "BMY", "current_lifecycle_state": "AWAITING_REVIEW"}],
        "receipt_without_ledger_event": [{"status": "PAPER_LEDGER_EVENT_MISSING", "candidate_id": "candidate-2", "symbol": "KRE"}],
    }
    sources = _sources()
    sources["paper_lifecycle_reconciliation"] = _source("/truth/paper_lifecycle_reconciliation.v1.json", "2026-05-27T14:21:00Z")

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T14:21:00Z")

    assert projection["paper_lifecycle_reconciliation_status"] == "AVAILABLE"
    assert projection["paper_lifecycle_counts"]["awaiting_review"] == 30
    assert "30 candidates still awaiting Record Entry" in projection["paper_lifecycle_operator_summary_messages"]
    assert projection["paper_lifecycle_failed_actions"][0]["status"] == "PAPER_ENTRY_ACTION_FAILED"
    bundle = PAGES.read_text(encoding="utf-8")
    assert "renderPaperLifecycleQueueWarning" in bundle
    assert "PAPER_ENTRY_ACTION_FAILED" in bundle
    assert "PAPER_LEDGER_EVENT_MISSING" in bundle



def test_candidate_ui_projection_surfaces_trading_lifecycle_state() -> None:
    payloads = _payloads(day="2026-05-27", count=1)
    payloads["trading_lifecycle_state"] = {
        "schema_id": "aegis_trading_lifecycle_state",
        "day_utc": "2026-05-27",
        "counts": {"PAPER_POSITION_OPEN": 1, "LEGACY_PARTIAL_OPEN": 1, "PAPER_TRADE_FAILED": 1, "AWAITING_REVIEW": 1, "PAPER_POSITION_CLOSED": 1},
        "lifecycle_items": [{"candidate_id": "candidate-open", "symbol": "AAPL", "current_state": "PAPER_POSITION_OPEN"}],
        "open_governed_positions": [{"candidate_id": "candidate-open", "symbol": "AAPL", "current_state": "PAPER_POSITION_OPEN", "current_status": "OPEN"}],
        "legacy_partial_open_positions": [{"candidate_id": "legacy-dow", "symbol": "DOW", "current_state": "LEGACY_PARTIAL_OPEN", "lineage_quality": "LEGACY_PARTIAL"}],
        "failed_paper_trade_attempts": [{"candidate_id": "candidate-failed", "symbol": "BMY", "current_state": "PAPER_TRADE_FAILED", "reason": "Record Entry failed"}],
        "awaiting_paper_trade": [{"candidate_id": "candidate-wait", "symbol": "KRE", "current_state": "AWAITING_REVIEW"}],
        "closed_positions": [{"candidate_id": "candidate-closed", "symbol": "BAC", "current_state": "PAPER_POSITION_CLOSED"}],
        "unclassified_lifecycle_items": [],
    }
    sources = {**_sources(), "trading_lifecycle_state": _source("/truth/trading_lifecycle_state.json", "2026-05-27T15:00:00Z")}

    projection = build_candidate_ui_projection_v1(payloads=payloads, sources=sources, day_utc="2026-05-27", canonical_generated_at="2026-05-27T15:01:00Z")

    assert projection["trading_lifecycle_state_status"] == "AVAILABLE"
    assert projection["trading_lifecycle_counts"]["LEGACY_PARTIAL_OPEN"] == 1
    assert projection["trading_lifecycle_open_governed_positions"][0]["symbol"] == "AAPL"
    assert projection["trading_lifecycle_legacy_partial_open_positions"][0]["symbol"] == "DOW"
    assert projection["trading_lifecycle_failed_paper_trade_attempts"][0]["current_state"] == "PAPER_TRADE_FAILED"



def test_positions_workspace_excludes_candidate_workflow_primary_content() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("async function renderPositionsWorkspace", 1)[1].split("function positionNumber", 1)[0]
    diagnostics = source.split("async function renderPositionsDiagnosticsWorkspace", 1)[1].split("function renderPositionsLifecycleSummary", 1)[0]

    assert 'data-source-field="signal_evidence_present_candidates"' not in block
    assert "Only final output-intent candidates that passed signal evidence boundary appear here" not in block
    assert "Rejected-intent lineage is diagnostics/history only" not in block
    assert "renderCandidateCaptureConfirmationPanel" not in block
    assert "positionsTodayCandidateColumns" not in block
    assert "Today's Candidates" not in block
    assert 'signalEvidenceBoundaryRows(payload, "SIGNAL_EVIDENCE_REJECTED_INTENT")' in diagnostics
    assert "Rejected-Intent Lineage" in diagnostics
    assert "signalEvidenceBoundaryColumns()" in diagnostics
    assert "candidateLifecycleColumns(payload, { actions: false })" in diagnostics

def test_diagnostics_rejected_intent_lineage_has_no_operator_capture_actions() -> None:
    source = PAGES.read_text(encoding="utf-8")
    signal_columns = source.split("function signalEvidenceBoundaryColumns", 1)[1].split("async function renderPositionsDiagnosticsWorkspace", 1)[0]
    rejected_section = source.split('data-testid="positions-rejected-intent-lineage"', 1)[1].split('`<section class="operator-section lifecycle-context"', 1)[0]

    assert "Confirm Captured" not in signal_columns
    assert "Mark Not Captured" not in signal_columns
    assert "DEFER_CANDIDATE" not in signal_columns
    assert "Confirm Captured" in rejected_section  # explanatory copy only
    assert "signalEvidenceBoundaryColumns()" in rejected_section
    assert "renderPositionsTodayCandidateActions" not in rejected_section


def test_positions_workspace_uses_ownership_only_sections() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("async function renderPositionsWorkspace", 1)[1].split("function positionNumber", 1)[0]

    assert "Current Holdings" in block
    assert "Holdings Summary" in block
    assert "Price data incomplete" in block
    assert "fetchAegisPositions(routeParams)" in block
    assert "fetchAegisOperatorCockpit(routeParams)" not in block
    assert "renderPositionsOwnershipTable" in block
    assert "renderClosedPositionsTable" in block

    assert "Today's Candidates" not in block
    assert "positionsTodayCandidateColumns" not in block
    assert "renderCandidateCaptureConfirmationPanel" not in block
    assert "renderPositionsPrimarySummary" not in source
    assert "Legacy / Partial Open Positions" not in block
    assert "Failed Record Entry Attempts" not in block
    assert "renderAvailableCandidatesDisclosure" not in block
    assert "renderLastPaperTradeActionDiagnostic" not in block
    assert "renderNonActiveCandidatesSection" not in block
    assert "UNCLASSIFIED_LIFECYCLE_ITEM" not in block
    assert "Command Status" not in block
    assert "source_artifacts" not in block


def test_positions_diagnostics_owns_lifecycle_and_command_detail() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("async function renderPositionsDiagnosticsWorkspace", 1)[1].split("function renderPositionsLifecycleSummary", 1)[0]

    assert "Positions Diagnostics" in block
    assert "Command Status" in block
    assert "Carry-forward / Lifecycle Context" in block
    assert "Legacy / Partial Open Positions" in block
    assert "Failed Record Entry Attempts" in block
    assert "renderAvailableCandidatesDisclosure" in block
    assert "renderLastPaperTradeActionDiagnostic" in block
    assert "Canonical Source Paths" in block
    assert "candidate_lifecycle_projection" in block


def test_positions_actionable_candidates_are_manual_entry_only() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert 'data-testid="available-candidates-collapsed"' in source
    assert '<details class="lifecycle-disclosure">' in source
    assert "governed candidate" in source
    assert "available for optional Record Entry" in source
    assert "Top symbols" in source
    assert "Record Entry" in source
    assert "Details" in source
    assert "renderFailedPaperTradeCards" in source
    assert "Retry" in source
    assert "Paper Trade" not in source


def test_candidates_ui_renders_construction_columns_from_projection() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert "function candidateConstructionColumns" in source
    for label in ("Entry", "Stop", "Target", "Qty", "Risk $", "R:R", "Session", "Actions"):
        assert f'label: "{label}"' in source
    assert "candidateEntryPrice" in source
    assert "candidateStopPrice" in source
    assert "renderCandidateConstructionDetails" in source


def test_paper_trade_button_disables_when_construction_prices_missing() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert "paperTradeMissingConstructionFields" in source
    assert "paperTradeDisabledReason" in source
    assert "Missing construction prices" in source
    assert 'disabled aria-disabled="true"' in source
    assert "renderConstructionDiagnosticsLink" in source


def test_positions_available_candidates_default_to_paper_operator_projection_actionable_bucket() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function paperLedgerProjection", 1)[1].split("function renderOpenPaperPositionsWorkflow", 1)[0]

    assert "paperOperatorProjectionCurrentSessionCandidates(payload)" in block
    assert "actionable_current_candidates" in source
    assert "paper_operator_projection.actionable_current_candidates" in source
    assert "awaiting: activeAwaiting" in block
    assert "awaiting: activeAwaiting.length ? activeAwaiting : lifecycleAwaiting" not in block
    assert "projectionOpenRows" in block
    assert "carry_forward_context" in block


def test_non_active_candidate_rows_hide_active_review_actions() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert "Not in active review state" in source
    assert "candidateActiveReviewPresent" in source
    assert "candidateActionDisabledReason" in source
    assert "action_endpoint" in source
    assert "active_review_present" in source
    assert "displayed_from" in source
    assert "candidateConstructionColumns(payload, { actions: false })" in source
    assert "Active review actions are not shown" in source


def test_actionable_candidate_rows_preserve_backend_candidate_id() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert 'name="candidate_id" value="${escapeHtml(candidateId)}"' in source
    assert 'target_id: candidateId' in (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    assert "candidate_contract_id" in source
    assert "source_candidate_contract_id" in source


def test_constructed_candidate_prices_do_not_render_missing_for_optional_target() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert 'candidatePriceCell(candidateEntryPrice(row))' in source
    assert 'candidatePriceCell(candidateStopPrice(row))' in source
    assert 'candidatePriceCell(candidateConstructionValue(row, "target_price"), { required: false })' in source
    assert '<span class="muted-mini">n/a</span>' in source


def test_record_entry_button_click_calls_command_endpoint_with_payload() -> None:
    source = PAGES.read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    client = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")

    assert "function paperWorkflowCommandPayload" in source
    assert 'data-aegis-command-action-type="API_COMMAND"' in source
    assert 'data-paper-entry-direct="true"' in source
    assert 'data-aegis-command-payload="${escapeHtml(JSON.stringify(commandPayload))}"' in source
    for field in ("candidate_id", "candidate_contract_id", "paper_session_id", "day_utc", "action", "actual_entry", "actual_stop", "quantity", "timestamp_utc"):
        assert field in source
    assert 'postJson("/api/aegis/commands"' in client
    assert 'recordAegisCommand' in main
    assert 'fetchAegisCommandStatus' in main
    assert 'const endpoint = "/api/aegis/commands"' in main


def test_manual_workflow_click_path_has_visible_diagnostics_success_and_error() -> None:
    source = PAGES.read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    handler = main[main.index("async function runAegisCommandElement"):main.index("async function handleClick")]

    assert "Last Record Entry Action" in source
    assert "data-last-paper-entry-action" in source
    assert "data-operator-shell-build-marker" in source
    assert "operator_shell build: 2026-05-28T18:26Z paper-entry-runtime-proof" in source
    assert "lastPaperTradeDiagnosticText" in source
    assert "__AEGIS_LAST_PAPER_ENTRY_ACTION" in main
    assert "updateLastPaperTradeActionDiagnostic" in main
    assert 'phase: "click received"' in main
    assert 'phase: "command received"' in main
    assert '"terminal status received"' in main
    assert 'phase: "command recording error"' in handler
    assert "command submitted" in main
    assert "PAPER_POSITION_OPEN" not in handler
    assert "response_body" in handler
    assert "window.__AEGIS_LAST_PAPER_ENTRY_ACTION" in main
    assert 'candidate_lifecycle_state' in source



def test_paper_trade_smoke_and_processor_commands_are_wired_to_backend_contracts() -> None:
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    smoke_script = (ROOT / "ops/tools/run_aegis_paper_trade_smoke_v1.py").read_text(encoding="utf-8")
    processor_script = (ROOT / "ops/tools/process_aegis_commands_v1.py").read_text(encoding="utf-8")

    assert "aegis:paper-trade-smoke" in package["scripts"]
    assert "aegis:process-commands" in package["scripts"]
    assert "execute_aegis_command_v1" in smoke_script
    assert "process_command_inbox_v1" in processor_script

def test_manual_workflow_ui_records_command_and_does_not_mutate_row_open_directly() -> None:
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    handler = main[main.index("async function runAegisCommandElement"):main.index("async function handleClick")]

    assert "submitOperatorWorkflowCommand" in handler
    assert "recordAegisCommand" in main
    assert "pollPaperTradeCommandStatus" in main
    assert "updatePaperCandidateRowOptimistically" not in main
    assert "PAPER_POSITION_OPEN" not in handler
    assert "showCommandResultPanel(commandElement" in main


def test_candidates_workflow_uses_lifecycle_counts_and_collapsed_carry_forward_context() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderRefreshCandidateProjectionButton", 1)[0]

    assert "candidate_lifecycle_projection_v1" in block
    assert "current_session_candidates" in block
    assert 'label: "Current Session Candidates", value: String(summary.current_session_total ?? currentRows.length)' in block
    assert 'data-paper-section="current-session"' in block
    assert "No current-session candidates in the lifecycle projection" in block
    assert "Carry-forward / Lifecycle Context" in block
    assert '<details class="lifecycle-disclosure">' in block
    assert "candidateLifecycleColumns(payload, { actions: false })" in source
    assert 'data-paper-section="open-paper-positions"' in block


def test_carry_forward_and_blocked_sections_do_not_render_paper_trade_or_ignore_buttons() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderRefreshCandidateProjectionButton", 1)[0]
    carry_block = block.split('data-paper-section="carry-forward"', 1)[1].split("${renderPaperTradeGoldenPathSection", 1)[0]

    assert "contextColumns" in carry_block
    assert "candidateColumns" not in carry_block
    assert "renderLifecycleCandidateActions" not in carry_block
    assert "Paper Trade" not in carry_block
    assert "Ignore" not in carry_block
    assert "candidateLifecycleColumns(payload, { actions: false })" in source


def test_candidates_ui_reads_candidate_lifecycle_projection_as_primary_truth() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderRefreshCandidateProjectionButton", 1)[0]

    assert "dashboardCandidateLifecycleProjection" in source
    assert "candidate_lifecycle_projection_v1" in block
    assert "current_session_candidates" in block
    assert "Current Session Candidates" in block
    assert "Actionable Candidates" in block
    assert "candidateLifecycleColumns" in source
    assert "candidate_lifecycle_state" in source
    assert "allowed_actions" in source


def test_candidates_ui_lifecycle_actions_are_state_derived() -> None:
    source = PAGES.read_text(encoding="utf-8")
    actions = source.split("function renderCandidateLifecycleActions", 1)[1].split("function candidateLifecycleColumns", 1)[0]

    assert "RECORD_ENTRY" in actions
    assert "RECORD_EXIT" in actions
    assert "VIEW_ENTRY_RECEIPT" in actions
    assert "VIEW_EXIT_RECEIPT" in actions
    assert "VIEW_POSITION" in actions
    assert "DETAILS" in actions
    assert "renderPaperTradeButton" in actions


def test_candidates_ui_current_session_does_not_hide_when_actionable_zero() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderRefreshCandidateProjectionButton", 1)[0]

    assert 'data-paper-section="actionable"' in block
    assert 'data-paper-section="current-session"' in block
    assert "No candidates are currently approved for Record Entry." in block
    assert "No current-session candidates in the lifecycle projection." in block
    assert "summary.current_session_total" in block
    assert "summary.actionable" in block
    assert "candidate-filter-tabs" in block


def test_lifecycle_context_is_collapsed_and_details_only() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderRefreshCandidateProjectionButton", 1)[0]
    carry_block = block.split('data-paper-section="carry-forward"', 1)[1].split("${renderPaperTradeGoldenPathSection", 1)[0]

    assert "Carry-forward / Lifecycle Context" in carry_block
    assert '<details class="lifecycle-disclosure">' in carry_block
    assert "contextColumns" in carry_block
    assert "Details only" in carry_block


def test_positions_today_candidates_render_incomplete_candidate_readiness_gate() -> None:
    source = PAGES.read_text(encoding="utf-8")
    actions = source.split("function renderPositionsTodayCandidateActions", 1)[1].split("function positionsTodayCandidateColumns", 1)[0]

    assert "candidateMissingRequiredFields" in source
    assert "renderCandidateReadinessWarning" in source
    assert "INCOMPLETE_CANDIDATE" in source
    assert "Missing required" in source
    assert "Confirm Captured" in actions
    assert "Mark Not Captured" in actions
    assert "Defer" in actions
    incomplete_branch = actions.split('state === "INCOMPLETE_CANDIDATE"', 1)[1].split('} else if', 1)[0]
    assert "renderCandidateReadinessWarning(row)" in incomplete_branch
    assert "renderDisabledCandidateCaptureButton" not in incomplete_branch
    assert "Missing required fields" in source


def test_candidate_capture_confirmation_panel_shows_ready_and_incomplete_counts() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function renderCandidateCaptureConfirmationPanel", 1)[1].split("async function renderPositionsWorkspace", 1)[0]

    assert "Ready for Operator Review" in block
    assert "Incomplete Count" in block
    assert "ready for operator review" in block
    assert "Classification" in block
    assert "Affected Count" in block
    assert "Next Action" in block
    assert "diagnosticsLink" in block
    assert "next_action" in block


def test_candidate_capture_confirmation_panel_does_not_derive_bare_failed_status() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source.split("function positionsCaptureSummary", 1)[1].split("function renderCandidateCaptureConfirmationPanel", 1)[0]

    assert '"FAILED"' not in block
    assert "candidate_capture_status" in block
    assert "UNKNOWN_REQUIRES_DIAGNOSTICS" in block
    assert "STALE_READ_MODEL" in source
    assert "incomplete" in block
    assert "readyForReview" in block
    assert "summary.incomplete" in source


def test_rebuilt_candidates_workspace_is_candidate_only() -> None:
    source = PAGES.read_text()
    block = source.split("async function renderCandidatesWorkspace", 1)[1].split("async function renderPositionsWorkspace", 1)[0]
    assert 'title: "Candidates"' in block
    assert "What was evaluated?" in block
    assert "No current candidates require operator review." in block
    assert "Candidate evaluation blocked" in block
    assert "Candidate generation blocked" in block
    assert "Last candidate evaluation" in block
    assert "Next candidate evaluation" in block
    assert "older or carry-forward rows were excluded" in block
    assert "Why not actionable?" in block
    assert "What happens next?" in block
    assert "Confirm Captured" in block
    assert "Mark Not Captured" in block
    assert "Defer" in block
    assert "Open Paper Positions" not in block
    assert "Holdings Summary" not in block
    assert "Position Review" not in block
    assert "portfolio exposure" not in block.lower()


def test_candidates_route_uses_rebuilt_workspace() -> None:
    source = PAGES.read_text()
    route_block = source.split('case "aegis_candidates":', 1)[1].split('case "aegis_open_paper_positions":', 1)[0]
    assert "return renderCandidatesWorkspace();" in route_block
    assert 'renderAegisWorkflowPage("candidates")' not in route_block
