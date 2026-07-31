from __future__ import annotations

from ops.aegis.candidate_generation_visibility_v1 import build_candidate_generation_visibility_v1


def _diagnostics_fixture() -> dict:
    return {
        "schema_id": "aegis_candidate_generation_diagnostics",
        "candidate_generation_status": "PARTIAL",
        "operator_interpretation": "PARTIAL_RUN",
        "total_sleeves_expected": 5,
        "total_sleeves_run": 2,
        "total_candidates_generated": 0,
        "total_candidates_rejected": 2,
        "signal_evidence_graph": {"signals": [{"raw_signal_id": "raw-qqq-1", "sleeve_id": "SLEEVE_REJECTED", "symbol": "QQQ", "candidate_contract_status": "REJECTED", "next_repair_action": "Repair evidence", "required_evidence": [{"purpose": "ENTRY_REFERENCE_PRICE", "data_item_id": "market.price.QQQ", "demanded": True, "fetched": False, "certified": False, "consumed": False, "failure_reason": "EVIDENCE_NOT_FETCHED", "source_artifact_path": "/truth/reports/market_data_inputs.v1.json"}]}]},
        "input_artifacts": {
            "candidate_generation_manifest": "/truth/reports/candidate_generation_manifest.v1.json",
            "intent_arbitration": "/truth/reports/intent_arbitration.v1.json",
            "runtime_truth": "/truth/reports/runtime_truth_kernel.v1.json",
        },
        "missing_input_artifacts": [{"artifact_id": "market.volatility.VIX"}],
        "stale_input_artifacts": [{"artifact_id": "event_market_snapshot", "status": "INVALID"}],
        "sleeves": [
            {
                "sleeve_id": "SLEEVE_NOT_RUN",
                "run_status": "NOT_RUN",
                "candidate_count": 0,
                "rejected_count": 0,
                "raw_signal_count": 0,
                "allowed_symbols": ["SPY"],
                "evaluation_artifact_path": "/truth/reports/sleeve_not_run.json",
            },
            {
                "sleeve_id": "SLEEVE_BLOCKED_VIX",
                "run_status": "BLOCKED",
                "evaluation_status": "BLOCKED",
                "candidate_count": 0,
                "rejected_count": 1,
                "raw_signal_count": 0,
                "canonical_blocker": "SLEEVE_INPUT_REQUIREMENT_BLOCKED",
                "reason_no_candidate": "Blocked by sleeve input contract: market.volatility.VIX.",
                "missing_inputs": ["market.volatility.VIX"],
                "allowed_symbols": ["IWM"],
                "reason_codes": ["SLEEVE_INPUT_REQUIREMENT_BLOCKED", "market.volatility.VIX"],
                "evaluation_artifact_path": "/truth/reports/sleeve_blocked_vix.json",
            },
            {
                "sleeve_id": "SLEEVE_NO_SIGNALS",
                "run_status": "RAN",
                "evaluation_status": "NO_INTENT",
                "candidate_count": 0,
                "rejected_count": 0,
                "raw_signal_count": 0,
                "symbols_evaluated": ["GLD", "SLV"],
                "reason_no_candidate": "Sleeve ran and produced no qualifying setup.",
                "evaluation_artifact_path": "/truth/reports/sleeve_no_signals.json",
            },
            {
                "sleeve_id": "SLEEVE_REJECTED",
                "run_status": "RAN",
                "evaluation_status": "RAN",
                "candidate_count": 0,
                "rejected_count": 1,
                "raw_signal_count": 2,
                "allowed_symbols": ["QQQ"],
                "reason_codes": ["SHA256_MISMATCH"],
                "canonical_blocker": "SHA256_MISMATCH",
                "reason_no_candidate": "SHA256_MISMATCH",
                "evaluation_artifact_path": "/truth/reports/sleeve_rejected.json",
            },
            {
                "sleeve_id": "SLEEVE_WITH_CANDIDATES",
                "run_status": "RAN",
                "evaluation_status": "RAN",
                "candidate_count": 2,
                "rejected_count": 0,
                "raw_signal_count": 2,
                "allowed_symbols": ["SPY", "QQQ"],
                "evaluation_artifact_path": "/truth/reports/sleeve_with_candidates.json",
            },
        ],
        "raw_signal_rejections": [
            {
                "raw_signal_id": "raw-qqq-1",
                "candidate_id": "cand-qqq-1",
                "sleeve_id": "SLEEVE_REJECTED",
                "symbol": "QQQ",
                "rejection_stage": "PORTFOLIO_SCORING",
                "rejection_reason": "SCORE_BELOW_THRESHOLD",
                "human_readable_explanation": "Thresholds not met for ranking.",
                "source_artifact_paths": ["/truth/reports/raw_signal_qqq.json"],
            }
        ],
    }


def test_visibility_classifies_all_execution_states() -> None:
    payload = build_candidate_generation_visibility_v1(_diagnostics_fixture())
    statuses = {row["sleeve_id"]: row["execution_status"] for row in payload["sleeve_execution_summary"]}

    assert statuses == {
        "SLEEVE_NOT_RUN": "NOT_RUN",
        "SLEEVE_BLOCKED_VIX": "BLOCKED",
        "SLEEVE_NO_SIGNALS": "EXECUTED_NO_SIGNALS",
        "SLEEVE_REJECTED": "EXECUTED_REJECTED",
        "SLEEVE_WITH_CANDIDATES": "EXECUTED_WITH_CANDIDATES",
    }


def test_visibility_reports_blocked_sleeve_and_missing_vix() -> None:
    payload = build_candidate_generation_visibility_v1(_diagnostics_fixture())
    blocked = next(row for row in payload["sleeve_execution_summary"] if row["sleeve_id"] == "SLEEVE_BLOCKED_VIX")

    assert blocked["primary_blocker_reason"] == "SLEEVE_INPUT_REQUIREMENT_BLOCKED"
    assert blocked["evidence_path"] == "/truth/reports/sleeve_blocked_vix.json"
    assert blocked["graph_linkage"] == "aegis_candidate_generation_diagnostics_v1.sleeves[SLEEVE_BLOCKED_VIX]"
    explanations = {row["code"] for row in payload["no_candidate_explanations"]}
    assert "VOLATILITY_FILTER_FAILED" in explanations
    assert "CONTRACTS_FAILED" in explanations


def test_visibility_reports_rejected_candidate_rows_with_evidence() -> None:
    payload = build_candidate_generation_visibility_v1(_diagnostics_fixture())
    row = payload["rejected_candidate_visibility"][0]

    assert row["symbol"] == "QQQ"
    assert row["rejection_reason"] == "SCORE_BELOW_THRESHOLD"
    assert row["rejection_stage"] == "PORTFOLIO_SCORING"
    assert row["sleeve_id"] == "SLEEVE_REJECTED"
    assert row["evidence_path"] == "/truth/reports/raw_signal_qqq.json"
    assert row["graph_linkage"] == "aegis_candidate_generation_diagnostics_v1.raw_signal_rejections[SLEEVE_REJECTED:QQQ]"
    assert row["candidate_id"] == "cand-qqq-1"
    assert row["human_readable_explanation"] == "Thresholds not met for ranking."


def test_visibility_reports_execution_coverage_and_no_signal_scan_counts() -> None:
    payload = build_candidate_generation_visibility_v1(_diagnostics_fixture())
    coverage = payload["execution_coverage"]
    no_signal = next(row for row in payload["sleeve_execution_summary"] if row["sleeve_id"] == "SLEEVE_NO_SIGNALS")

    assert coverage == {
        "expected_sleeves": 5,
        "sleeves_attempted": 4,
        "sleeves_successfully_executed": 3,
        "sleeves_blocked": 1,
        "sleeves_producing_signals": 2,
    }
    assert no_signal["symbols_evaluated_count"] == 2


def test_visibility_reports_contract_mismatch_explanation() -> None:
    payload = build_candidate_generation_visibility_v1(_diagnostics_fixture())
    rejected = next(row for row in payload["sleeve_execution_summary"] if row["sleeve_id"] == "SLEEVE_REJECTED")
    explanations = {row["code"] for row in payload["no_candidate_explanations"]}

    assert rejected["primary_blocker_reason"] == "SHA256_MISMATCH"
    assert "EVIDENCE_MISSING" in explanations


def test_visibility_exposes_signal_evidence_rows() -> None:
    payload = build_candidate_generation_visibility_v1(_diagnostics_fixture())
    row = payload["signal_evidence_rows"][0]
    assert row["raw_signal_id"] == "raw-qqq-1"
    assert row["required_evidence"][0]["purpose"] == "ENTRY_REFERENCE_PRICE"
