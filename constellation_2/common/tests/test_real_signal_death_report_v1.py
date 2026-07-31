from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.candidate_generation_diagnostics_v1 import build_candidate_generation_diagnostics_v1, write_candidate_generation_diagnostics_v1
from ops.aegis.real_signal_death_report_v1 import build_real_signal_death_report_v1

DAY = "2026-05-26"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _write_json(repo / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json", {"sleeves": [{"sleeve_id": "PRIMARY", "enabled": True}]})
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {
            "engines": [
                {
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "activation_status": "ACTIVE",
                    "engine_runner_path": "constellation_2/phaseI/trend_eq_primary/run.py",
                    "allowed_symbols": ["AAPL", "AMT"],
                },
                {
                    "engine_id": "C2_EVENT_DISLOCATION_V1",
                    "activation_status": "ACTIVE",
                    "engine_runner_path": "constellation_2/phaseI/event_dislocation/run.py",
                    "allowed_symbols": ["SPY"],
                },
            ]
        },
    )
    return repo


def _seed_runtime(root: Path) -> None:
    _write_json(
        root / "reports/aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json",
        {
            "day_utc": DAY,
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "trade_advice_allowed": False,
            "autonomous_execution_allowed": False,
            "manual_trade_capture_allowed": False,
            "missing_or_stale_source_count": 1,
        },
    )
    _write_json(root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json", {"day_utc": DAY, "status": "PARTIAL", "stale_symbols": ["VIX"]})
    _write_json(root / "reports/aegis_data_registry_v1" / DAY / "data_registry.v1.json", {"day_utc": DAY, "missing_items": [], "stale_items": ["market.volatility.VIX"]})
    _write_json(root / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {"day_utc": DAY, "sleeves": []})
    _write_json(
        root / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json",
        {
            "day_utc": DAY,
            "ready_count": 1,
            "ready_with_warnings_count": 1,
            "sleeves": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "readiness": "READY_WITH_WARNINGS", "can_run_candidate_generation": True, "required_inputs_status": [], "optional_inputs_status": [], "blocking_inputs": [], "warning_inputs": []},
                {"sleeve_id": "C2_EVENT_DISLOCATION_V1", "readiness": "READY", "can_run_candidate_generation": True, "required_inputs_status": [], "optional_inputs_status": [], "blocking_inputs": [], "warning_inputs": []},
            ],
        },
    )
    _write_json(root / "reports/aegis_event_regime_trigger_evaluator_v1" / DAY / "trigger_evaluation.v1.json", {"day_utc": DAY, "decisions": [], "confirmed_sleeve_ids": ["C2_TREND_EQ_PRIMARY_V1", "C2_EVENT_DISLOCATION_V1"]})
    _write_json(root / "reports/aegis_triggered_sleeve_runs_v1" / DAY / "triggered_sleeve_runs.v1.json", {"day_utc": DAY, "runs": []})


def _seed_sleeve_eval(root: Path) -> None:
    trend_path = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_TREND_EQ_PRIMARY_V1" / "sleeve_evaluation.v1.json"
    trend_payload = {
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "status": "BLOCKED",
        "current_status": "BLOCKED",
        "output_count": 2,
        "artifact_path": str(trend_path),
        "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED", "POSITION_STATE_STALE", "UNCHANGED_SIGNAL"],
        "lifecycle_reason_codes": ["POSITION_STATE_STALE", "UNCHANGED_SIGNAL"],
        "signal_state": {"state": "ACTIVE"},
        "exposure_intent_batch": {
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "output_count": 2,
            "output_intents": [
                {"intent_id": "c2_trend_eq_aapl_2026-05-26_v1", "intent_hash": "hash-aapl", "intent_path": "/truth/intents/aapl.exposure_intent.v1.json", "schema_id": "exposure_intent", "symbol": "AAPL"},
                {"intent_id": "c2_trend_eq_amt_2026-05-26_v1", "intent_hash": "hash-amt", "intent_path": "/truth/intents/amt.exposure_intent.v1.json", "schema_id": "exposure_intent", "symbol": "AMT"},
            ],
        },
    }
    event_path = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_EVENT_DISLOCATION_V1" / "sleeve_evaluation.v1.json"
    event_payload = {
        "sleeve_id": "C2_EVENT_DISLOCATION_V1",
        "engine_id": "C2_EVENT_DISLOCATION_V1",
        "status": "NO_INTENT",
        "current_status": "NO_INTENT",
        "output_count": 0,
        "artifact_path": str(event_path),
        "reason_codes": ["NO_INTENT_DECLARED"],
        "lifecycle_reason_codes": ["SIGNAL_INACTIVE"],
        "signal_state": {"state": "INACTIVE"},
    }
    _write_json(trend_path, trend_payload)
    _write_json(event_path, event_payload)
    _write_json(root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"day_utc": DAY, "status": "PARTIAL", "outcomes": [trend_payload, event_payload]})


def _seed_candidate_manifest(root: Path) -> None:
    _write_json(
        root / "reports/candidate_generation_manifest_v1" / DAY / f"sleeve_evaluation_kernel_v1:{DAY}" / "candidate_generation_manifest.v1.json",
        {
            "day_utc": DAY,
            "candidate_rows": [
                {
                    "candidate_id": "cand-aapl",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "symbol_or_pair": "AAPL",
                    "status": "BLOCKED",
                    "rejection_reason": "POSITION_STATE_STALE",
                    "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED", "POSITION_STATE_STALE", "UNCHANGED_SIGNAL"],
                    "output_artifact_paths": ["/truth/reports/sleeve_evaluation_kernel_v1/2026-05-26/C2_TREND_EQ_PRIMARY_V1/sleeve_evaluation.v1.json"],
                    "raw_intent_id": "",
                    "raw_intent_path": "",
                },
                {
                    "candidate_id": "cand-amt",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "symbol_or_pair": "AMT",
                    "status": "BLOCKED",
                    "rejection_reason": "POSITION_STATE_STALE",
                    "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED", "POSITION_STATE_STALE", "UNCHANGED_SIGNAL"],
                    "output_artifact_paths": ["/truth/reports/sleeve_evaluation_kernel_v1/2026-05-26/C2_TREND_EQ_PRIMARY_V1/sleeve_evaluation.v1.json"],
                    "raw_intent_id": "",
                    "raw_intent_path": "",
                },
            ]
        },
    )
    _write_json(root / "reports/candidate_lineage_v1" / DAY / "lineage" / "candidate_lineage.v1.json", {"lineage_rows": []})


def _seed_arbitration_and_promotion(root: Path) -> None:
    _write_json(root / "reports/intent_arbitration_v1" / DAY / "intent_arbitration.v1.json", {"day_utc": DAY, "status": "BLOCKED", "candidate_intents": [], "rejected_or_filtered_intents": [], "selected_intent": {}, "portfolio_scoring_path": "/truth/reports/portfolio_scoring.v1.json"})
    _write_json(root / "reports/aegis_selected_intent_promotion_v1" / DAY / "selected_intent_promotion.v1.json", {"day_utc": DAY, "status": "CONTRACT_FAILED", "promotion_status": "blocked", "missing_contract_fields": ["selected_intent.intent_id", "portfolio_scoring.executable_eligible"], "promoted_candidate_count": 0})


def _seed_lifecycle_and_paper(root: Path) -> None:
    _write_json(root / "reports/aegis_candidate_lifecycle_v1" / DAY / "candidate_lifecycle.v1.json", {"day_utc": DAY, "candidates": [{"candidate_id": "paper_rehearsal_candidate:paper_rehearsal_fixture:SPY", "raw_signal_id": "raw_signal:paper_rehearsal_fixture:SPY", "promotion_contract": {"mode": "PAPER_REHEARSAL"}}]})
    _write_json(root / "reports/aegis_paper_trade_golden_path_v1" / DAY / "paper_rehearsal_golden_path_v1_fixture" / "paper_trade_golden_path.v1.json", {"day_utc": DAY, "paper_rehearsal_lifecycle_proven": True, "raw_signal_count": 1, "candidate_count": 1, "mode": "PAPER_REHEARSAL", "receipt_type": "SIMULATED_PAPER"})


def _seed(root: Path, repo: Path) -> None:
    _seed_runtime(root)
    _seed_sleeve_eval(root)
    _seed_candidate_manifest(root)
    _seed_arbitration_and_promotion(root)
    _seed_lifecycle_and_paper(root)


def test_death_report_emits_rejection_stages_and_compares_to_golden_path(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed(root, repo)

    report = build_real_signal_death_report_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert report["total_raw_signals"] == 2
    assert report["total_candidates_created"] == 0
    assert report["signals_by_sleeve"] == {"C2_TREND_EQ_PRIMARY_V1": 2}
    assert report["signals_by_symbol"] == {"AAPL": 1, "AMT": 1}
    assert report["rejection_stage_counts"]["CANDIDATE_CONVERSION"] == 2
    assert report["comparison_to_paper_golden_path"]["paper_rehearsal_lifecycle_proven"] is True
    assert report["comparison_to_paper_golden_path"]["paper_rehearsal_excluded_from_real_totals"] is True
    assert report["comparison_to_paper_golden_path"]["real_candidate_count"] == 0
    assert report["signals"][0]["failed_candidate_contract_fields"]
    assert report["signals"][0]["failed_promotion_fields"] == ["selected_intent.intent_id", "portfolio_scoring.executable_eligible"]
    assert report["signals"][0]["candidate_contract_status"] == "REJECTED"
    assert report["signals"][0]["rejection_reason"] == "ENTRY_REFERENCE_PRICE_MISSING"
    assert report["signals"][0]["entry_reference_price_missing_symbol"] == "AAPL"
    assert report["signals"][0]["required_evidence"]
    assert report["signals"][0]["signal_evidence_graph_path"].endswith("signal_evidence_graph.v1.json")


def test_candidate_diagnostics_references_death_report_and_preserves_safety_policy(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed(root, repo)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    paths = write_candidate_generation_diagnostics_v1(truth_root=root, day_utc=DAY, payload=payload)

    assert payload["total_raw_signals"] == 2
    assert payload["paper_rehearsal_candidates_excluded_from_generation_totals"] is True
    assert payload["real_signal_death_report_path"].endswith("real_signal_death_report.v1.json")
    assert paths["signal_evidence_graph"].endswith("signal_evidence_graph.v1.json")
    assert paths["candidate_contracts"].endswith("candidate_contracts.v1.json")
    assert paths["real_signal_death_report"].endswith("real_signal_death_report.v1.json")
    assert payload["signal_evidence_graph_path"].endswith("signal_evidence_graph.v1.json")
    assert payload["candidate_contracts_path"].endswith("candidate_contracts.v1.json")
    assert payload["rejection_stage_counts"]["CANDIDATE_CONVERSION"] == 2
    assert "summary" in payload["paper_golden_path_comparison_summary"]
    assert payload["selected_intent_promotion"]["status"] == "CONTRACT_FAILED"
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False
    assert payload["safety"]["automatic_sleeve_mutation_allowed"] is False
    report = payload["real_signal_death_report"]
    assert report["runtime_truth"]["trade_advice_allowed"] is False
    assert report["safety"]["trade_advice_allowed"] is False
    assert report["safety"]["broker_execution_allowed"] is False
    assert report["safety"]["autonomous_execution_allowed"] is False
