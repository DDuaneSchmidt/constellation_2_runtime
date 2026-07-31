from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.candidate_generation_diagnostics_v1 import (
    build_candidate_generation_diagnostics_v1,
    write_candidate_generation_diagnostics_v1,
)
from ops.aegis.candidate_generation_visibility_v1 import build_candidate_generation_visibility_v1


DAY = "2026-05-18"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _repo(tmp_path: Path, sleeves: list[str]) -> Path:
    repo = tmp_path / "repo"
    _write_json(
        repo / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        {"sleeves": [{"sleeve_id": "PRIMARY", "enabled": True}]},
    )
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {
            "engines": [
                {
                    "engine_id": sleeve,
                    "activation_status": "ACTIVE",
                    "engine_runner_path": f"constellation_2/phaseI/{sleeve.lower()}/run.py",
                    "allowed_symbols": ["SPY"],
                }
                for sleeve in sleeves
            ]
        },
    )
    return repo


def _seven_sleeves() -> list[str]:
    return [
        "C2_CROSS_ASSET_TREND_V1",
        "C2_DEFENSIVE_TAIL_V1",
        "C2_EVENT_DISLOCATION_V1",
        "C2_MARKET_NEUTRAL_SPREAD_V1",
        "C2_MEAN_REVERSION_EQ_V1",
        "C2_TREND_EQ_PRIMARY_V1",
        "C2_VOL_INCOME_DEFINED_RISK_V1",
    ]


def _trigger_eval(root: Path, *, sleeves: list[str], decisions: list[dict]) -> None:
    _write_json(
        root / "reports/aegis_event_regime_trigger_evaluator_v1" / DAY / "trigger_evaluation.v1.json",
        {
            "day_utc": DAY,
            "confirmed_sleeve_ids": sleeves,
            "decisions": decisions,
        },
    )


def _runs(root: Path, runs: list[dict]) -> None:
    _write_json(
        root / "reports/aegis_triggered_sleeve_runs_v1" / DAY / "triggered_sleeve_runs.v1.json",
        {"day_utc": DAY, "runs": runs, "candidate_count": sum(int(row.get("candidate_count") or 0) for row in runs)},
    )


def _runtime(root: Path, *, missing_count: int = 0) -> None:
    _write_json(
        root / "reports/aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json",
        {
            "day_utc": DAY,
            "runtime_truth_classification": "REAL_RUNTIME",
            "human_approved_advisory_runtime_ready": True,
            "missing_or_stale_source_count": missing_count,
        },
    )


def _market_report(root: Path) -> None:
    _write_json(
        root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "day_utc": DAY,
            "status": "FAILED",
            "provider_config": {"configured": False, "primary": "", "fallback": ""},
            "requested_symbols": ["SPY"],
            "runtime_universe_mode": "production_scan_dataset",
            "production_scan_dataset_id": "ds_ohlcv_1d_liquid_etf_core_fixture",
            "dataset_snapshot_id": "ds_ohlcv_1d_liquid_etf_core_fixture",
            "production_scan_universe_count": 3,
            "sleeve_required_symbol_count": 2,
            "total_requested_symbol_count": 3,
            "requested_symbols_source": "production_scan_dataset+sleeve_required_symbols",
            "minimum_viable_runtime_reference_removed": True,
            "fetched_symbols": [],
            "missing_symbols": ["SPY"],
            "stale_symbols": [],
            "mapping_missing_symbols": [],
            "provider_failed_symbols": ["SPY"],
            "missing_fields": ["market.price.SPY"],
            "stale_fields": [],
            "provider_results": [],
            "usable_for_candidate_generation": False,
            "failure_reason": "MARKET_DATA_PROVIDER_NOT_CONFIGURED",
        },
    )


def _sleeve_eval(root: Path, sleeves: list[str], *, blocked: str = "") -> None:
    outcomes = []
    for sleeve in sleeves:
        status = "BLOCKED" if sleeve == blocked else "NO_INTENT"
        outcomes.append(
            {
                "sleeve_id": sleeve,
                "engine_id": sleeve,
                "enabled": True,
                "activation_status": "ACTIVE",
                "status": status,
                "current_status": status,
                "canonical_blocker": "PRODUCER_NONZERO_RC" if status == "BLOCKED" else "",
                "reason_codes": ["PRODUCER_NONZERO_RC"] if status == "BLOCKED" else ["NO_INTENT_DECLARED"],
                "exit_code": 1 if status == "BLOCKED" else 0,
                "producer_command": f"python3 run_{sleeve}.py",
                "market_data_manifest_check": {"status": "PASS"},
                "output_intents": [],
                "artifact_path": str(root / "unused.json"),
            }
        )
    _write_json(
        root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {"day_utc": DAY, "status": "BLOCKED" if blocked else "PASS", "outcomes": outcomes},
    )


def _readiness(root: Path, sleeves: list[str], *, blocked: str = "") -> None:
    _write_json(
        root / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json",
        {
            "day_utc": DAY,
            "total_sleeves_enabled": len(sleeves),
            "ready_count": len([sleeve for sleeve in sleeves if sleeve != blocked]),
            "ready_with_warnings_count": 0,
            "blocked_count": 1 if blocked else 0,
            "unknown_count": 0,
            "sleeves": [
                {
                    "sleeve_id": sleeve,
                    "readiness": "BLOCKED" if sleeve == blocked else "READY",
                    "required_inputs_status": [],
                    "optional_inputs_status": [],
                    "blocking_inputs": ["market.volatility.VIX"] if sleeve == blocked else [],
                    "warning_inputs": [],
                    "contract_status": "OK",
                    "can_run_candidate_generation": sleeve != blocked,
                    "reason": "fixture",
                    "source_artifacts": [],
                    "source_hashes": {},
                }
                for sleeve in sleeves
            ],
        },
    )


def _candidate_manifest_with_cross_asset_raw_signal(root: Path) -> None:
    _write_json(
        root / "reports/candidate_generation_manifest_v1" / DAY / f"sleeve_evaluation_kernel_v1:{DAY}" / "candidate_generation_manifest.v1.json",
        {
            "schema_id": "candidate_generation_manifest",
            "day_utc": DAY,
            "run_id": f"sleeve_evaluation_kernel_v1:{DAY}",
            "candidate_rows": [
                {
                    "candidate_id": "raw-candidate-1",
                    "engine_id": "C2_CROSS_ASSET_TREND_V1",
                    "status": "CANDIDATE_CREATED",
                    "symbol_or_pair": "QQQ",
                    "raw_intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1",
                    "raw_intent_hash": "rawhash",
                    "raw_intent_path": "/truth/intents/rawhash.exposure_intent.v1.json",
                    "lifecycle_decision": "INTENT_CREATED",
                    "lifecycle_reason_codes": ["PERSISTENT_SIGNAL_NO_POSITION", "UNCHANGED_SIGNAL"],
                    "allowed_by_portfolio_gate": False,
                    "portfolio_gate_decision": "",
                    "rejection_reason": "",
                    "output_artifact_paths": ["/truth/reports/sleeve_evaluation.v1.json"],
                },
                {
                    "candidate_id": "blocked-vix",
                    "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "status": "BLOCKED",
                    "symbol_or_pair": "IWM",
                    "raw_intent_id": "",
                    "rejection_reason": "SLEEVE_INPUT_REQUIREMENT_BLOCKED",
                    "reason_codes": ["SLEEVE_INPUT_REQUIREMENT_BLOCKED", "market.volatility.VIX"],
                },
            ],
        },
    )


def _candidate_lineage_with_cross_asset_not_consumed(root: Path) -> None:
    _write_json(
        root / "reports/candidate_lineage_v1" / DAY / "aegis_lite_eod_v1_2026-05-18" / "candidate_lineage.v1.json",
        {
            "schema_id": "candidate_lineage",
            "lineage_rows": [
                {
                    "candidate_id": "raw-candidate-1",
                    "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
                    "symbol": "QQQ",
                    "raw_created": True,
                    "consumed_by_eod": False,
                    "promotion_status": "not_consumed",
                    "final_state": "NOT_CONSUMED_BY_EOD",
                    "block_reasons": ["RAW_CANDIDATES_EXISTED_NOT_CONSUMED", "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE"],
                }
            ],
        },
    )


def _intent_arbitration_with_cross_asset_missing_score(root: Path) -> None:
    _write_json(
        root / "reports/intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {
            "schema_id": "intent_arbitration",
            "rejected_or_filtered_intents": [
                {
                    "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
                    "engine_id": "C2_CROSS_ASSET_TREND_V1",
                    "symbol": "QQQ",
                    "intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1",
                    "intent_hash": "rawhash",
                    "intent_path": "/truth/intents/rawhash.exposure_intent.v1.json",
                    "rejection_reason": "PORTFOLIO_SCORING_MISSING_INTENT_SCORE",
                    "portfolio_scoring_status": "MISSING_INTENT_SCORE",
                    "lifecycle_decision": "INTENT_CREATED",
                    "lifecycle_reason_codes": ["PERSISTENT_SIGNAL_NO_POSITION", "UNCHANGED_SIGNAL"],
                }
            ],
        },
    )


def _intent_arbitration_with_cross_asset_selected(root: Path) -> None:
    _write_json(
        root / "reports/intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {
            "schema_id": "intent_arbitration",
            "status": "SELECTED",
            "portfolio_scoring_path": "/truth/reports/portfolio_scoring_v1/2026-05-18/portfolio_scoring.v1.json",
            "selected_intent": {
                "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
                "engine_id": "C2_CROSS_ASSET_TREND_V1",
                "symbol": "QQQ",
                "intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1",
                "intent_hash": "rawhash",
                "intent_path": "/truth/intents/rawhash.exposure_intent.v1.json",
                "portfolio_scoring_status": "SCORED",
                "portfolio_score_rank": 1,
                "portfolio_score_total": 36.9,
                "portfolio_gate_decision": "ALLOW",
                "lifecycle_decision": "INTENT_CREATED",
                "lifecycle_reason_codes": ["PERSISTENT_SIGNAL_NO_POSITION", "UNCHANGED_SIGNAL"],
            },
            "rejected_or_filtered_intents": [],
        },
    )


def _selected_intent_promotion_passed(root: Path) -> None:
    _write_json(
        root / "reports/aegis_selected_intent_promotion_v1" / DAY / "selected_intent_promotion.v1.json",
        {
            "schema_id": "aegis_selected_intent_promotion",
            "day_utc": DAY,
            "status": "PROMOTED_TO_OPERATOR_REVIEW",
            "promotion_status": "operator_reviewable",
            "selected_intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1",
            "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
            "symbol": "QQQ",
            "promoted_candidate_count": 1,
            "missing_contract_fields": [],
            "promoted_candidate_set_path": "/truth/reports/promoted_candidate_set_v1/2026-05-18/selected/promoted_candidate_set.v1.json",
            "operator_review_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
        },
    )


def test_diagnostics_report_generates(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    _runtime(root)
    _trigger_eval(
        root,
        sleeves=["PRIMARY"],
        decisions=[{"decision": "RUN_SLEEVES", "selected_sleeve_ids": ["PRIMARY"], "trigger_id": "event:VOLATILITY_SPIKE"}],
    )
    _runs(root, [{"status": "SUCCESS", "selected_sleeve_ids": ["PRIMARY"], "candidate_count": 0, "warnings": []}])

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    paths = write_candidate_generation_diagnostics_v1(truth_root=root, day_utc=DAY, payload=payload)

    assert Path(paths["json"]).exists()
    assert Path(paths["summary"]).exists()
    assert Path(paths["matrix"]).exists()
    assert payload["schema_id"] == "aegis_candidate_generation_diagnostics"


def test_zero_candidates_with_successful_sleeve_runs_is_normal_no_signal(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    _runtime(root)
    _trigger_eval(root, sleeves=["PRIMARY"], decisions=[{"decision": "RUN_SLEEVES", "selected_sleeve_ids": ["PRIMARY"]}])
    _runs(root, [{"status": "SUCCESS", "selected_sleeve_ids": ["PRIMARY"], "candidate_count": 0, "warnings": []}])

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["candidate_generation_status"] == "RAN"
    assert payload["operator_interpretation"] == "NORMAL_NO_SIGNAL"
    assert payload["zero_candidate_explanation"] == "Candidate generation ran. No sleeves produced qualifying candidates."


def test_zero_candidates_with_no_sleeve_runs_is_engine_not_run(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    _runtime(root)
    _trigger_eval(root, sleeves=["PRIMARY"], decisions=[{"decision": "SKIP", "selected_sleeve_ids": [], "reason_skipped": "CONDITION_NOT_DETECTED"}])
    _runs(root, [{"status": "SKIPPED", "selected_sleeve_ids": [], "candidate_count": 0, "warnings": ["NO_RUN_SLEEVES_DECISIONS"]}])

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["candidate_generation_status"] == "NOT_RUN"
    assert payload["operator_interpretation"] == "ENGINE_NOT_RUN"


def test_missing_data_is_data_blocked(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    _runtime(root, missing_count=2)
    _trigger_eval(
        root,
        sleeves=["PRIMARY"],
        decisions=[{"decision": "INSUFFICIENT_EVIDENCE", "selected_sleeve_ids": [], "reason_skipped": "EVENT_OR_REGIME_EVIDENCE_MISSING_OR_STALE"}],
    )
    _runs(root, [{"status": "SKIPPED", "selected_sleeve_ids": [], "candidate_count": 0, "warnings": []}])

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["operator_interpretation"] == "DATA_BLOCKED"
    assert payload["zero_candidate_explanation"] == "No candidates found because required data was missing/stale."
    assert payload["sleeves"][0]["data_status"] == "MISSING"


def test_candidate_diagnostics_uses_exact_market_data_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    _runtime(root, missing_count=2)
    _market_report(root)
    _trigger_eval(
        root,
        sleeves=["PRIMARY"],
        decisions=[{"decision": "INSUFFICIENT_EVIDENCE", "selected_sleeve_ids": [], "reason_skipped": "EVENT_OR_REGIME_EVIDENCE_MISSING_OR_STALE"}],
    )
    _runs(root, [{"status": "SKIPPED", "selected_sleeve_ids": [], "candidate_count": 0, "warnings": []}])

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["market_data_summary"]["failure_reason"] == "MARKET_DATA_PROVIDER_NOT_CONFIGURED"
    assert payload["market_data_summary"]["missing_symbols"] == ["SPY"]
    assert payload["market_data_summary"]["provider_config"]["configured"] is False
    assert payload["market_data_summary"]["runtime_universe_mode"] == "production_scan_dataset"
    assert payload["market_data_summary"]["production_scan_dataset_id"] == "ds_ohlcv_1d_liquid_etf_core_fixture"
    assert payload["market_data_summary"]["production_scan_universe_count"] == 3
    assert payload["market_data_summary"]["sleeve_required_symbol_count"] == 2
    assert payload["market_data_summary"]["total_requested_symbol_count"] == 3
    assert payload["market_data_summary"]["requested_symbols_source"] == "production_scan_dataset+sleeve_required_symbols"


def test_candidate_diagnostics_reports_canonical_vix_missing_explanation(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["C2_VOL_INCOME_DEFINED_RISK_V1"])
    _runtime(root, missing_count=1)
    _write_json(
        root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "day_utc": DAY,
            "status": "PARTIAL",
            "provider_config": {"configured": True, "primary": "LOCAL_CACHE", "fallback": "STOOQ"},
            "requested_symbols": ["VIX"],
            "fetched_symbols": [],
            "missing_symbols": ["VIX"],
            "stale_symbols": [],
            "mapping_missing_symbols": [],
            "provider_failed_symbols": ["VIX"],
            "missing_fields": ["market.volatility.VIX"],
            "stale_fields": [],
            "symbol_resolution": {"VIX": {"aliases": ["VIX", "^VIX", "$VIX", "vix", "VIXCLS"], "provider_mappings": {"LOCAL_CACHE": ["VIX", "^VIX"], "STOOQ": ["^VIX", "vix"]}}},
            "missing_symbol_explanations": {
                "VIX": {
                    "canonical_symbol": "VIX",
                    "operator_message": "VIX is missing from LOCAL_CACHE canonical/alias paths and fallback provider lookup failed; C2_VOL_INCOME_DEFINED_RISK_V1 remains blocked and the run remains READY_PARTIAL. Other ready sleeves may proceed to operator review. No execution is authorized.",
                    "synthetic_data_allowed": False,
                }
            },
            "provider_results": [],
            "usable_for_candidate_generation": False,
            "failure_reason": None,
        },
    )
    _sleeve_eval(root, ["C2_VOL_INCOME_DEFINED_RISK_V1"], blocked="C2_VOL_INCOME_DEFINED_RISK_V1")
    _readiness(root, ["C2_VOL_INCOME_DEFINED_RISK_V1"], blocked="C2_VOL_INCOME_DEFINED_RISK_V1")

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    summary = payload["market_data_summary"]

    assert summary["missing_symbols"] == ["VIX"]
    assert "^VIX" not in summary["missing_symbols"]
    assert summary["missing_symbol_explanations"]["VIX"]["canonical_symbol"] == "VIX"
    assert summary["missing_symbol_explanations"]["VIX"]["synthetic_data_allowed"] is False


def test_rejected_raw_signals_appear_with_reasons(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    _runtime(root)
    _trigger_eval(root, sleeves=["PRIMARY"], decisions=[{"decision": "RUN_SLEEVES", "selected_sleeve_ids": ["PRIMARY"]}])
    _runs(
        root,
        [
            {
                "status": "SUCCESS",
                "selected_sleeve_ids": ["PRIMARY"],
                "candidate_count": 0,
                "warnings": ["PRIMARY:volatility filter", "PRIMARY:confidence threshold"],
            }
        ],
    )

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["total_candidates_rejected"] == 2
    assert payload["sleeves"][0]["rejection_reasons"] == ["volatility filter", "confidence threshold"]


def test_diagnostics_add_no_broker_or_autonomous_execution(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, ["PRIMARY"])
    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    safety = payload["safety"]
    assert safety["broker_execution_allowed"] is False
    assert safety["autonomous_execution_allowed"] is False
    assert safety["automatic_sleeve_mutation_allowed"] is False


def test_all_seven_advisory_sleeves_are_discovered_from_engine_registry(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = _seven_sleeves()
    repo = _repo(tmp_path, sleeves)
    _runtime(root)
    _sleeve_eval(root, sleeves)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["authoritative_sleeve_registry_path"].endswith("ENGINE_MODEL_REGISTRY_V1.json")
    assert payload["total_sleeves_registered"] == 7
    assert payload["total_sleeves_enabled"] == 7
    assert payload["total_sleeves_expected_today"] == 7
    assert payload["total_sleeves_expected"] == 7
    assert sorted(row["sleeve_id"] for row in payload["sleeves"]) == sorted(sleeves)
    assert all(row["run_status"] == "RAN" for row in payload["sleeves"])


def test_intent_simulator_is_diagnostic_only_not_candidate_expected(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    production_sleeves = _seven_sleeves()
    simulator = "C2_INTENT_SIMULATOR_V1"
    repo = _repo(tmp_path, [*production_sleeves, simulator])
    _runtime(root)
    outcomes = [
        {
            "sleeve_id": sleeve,
            "engine_id": sleeve,
            "enabled": True,
            "activation_status": "ACTIVE",
            "status": "NO_INTENT",
            "current_status": "NO_INTENT",
            "canonical_blocker": "",
            "reason_codes": ["NO_INTENT_DECLARED"],
            "exit_code": 0,
            "producer_command": f"python3 run_{sleeve}.py",
            "market_data_manifest_check": {"status": "PASS"},
            "output_intents": [],
            "artifact_path": str(root / "unused.json"),
        }
        for sleeve in production_sleeves
    ]
    outcomes.append(
        {
            "sleeve_id": simulator,
            "engine_id": simulator,
            "enabled": True,
            "activation_status": "ACTIVE",
            "status": "FILTERED_OUT",
            "current_status": "FILTERED_OUT",
            "canonical_blocker": "",
            "reason_codes": ["ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED", "OPTIONAL_SIMULATION"],
            "exit_code": 0,
            "producer_command": "",
            "market_data_manifest_check": {"status": "PASS"},
            "output_intents": [],
            "artifact_path": str(root / "simulator.json"),
        }
    )
    _write_json(
        root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {"day_utc": DAY, "status": "PASS", "outcomes": outcomes},
    )

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    by_id = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert payload["total_sleeves_registered"] == 8
    assert payload["total_sleeves_enabled"] == 7
    assert payload["total_sleeves_expected_today"] == 7
    assert payload["total_sleeves_expected"] == 7
    assert payload["total_sleeves_run"] == 7
    assert simulator not in payload["expected_sleeves"]
    assert simulator not in payload["enabled_sleeve_ids"]
    assert simulator in by_id
    assert by_id[simulator]["diagnostic_only"] is True
    assert by_id[simulator]["excluded_from_candidate_generation_totals"] is True
    assert by_id[simulator]["expected_today"] is False
    assert by_id[simulator]["evaluation_status"] == "FILTERED_OUT"
    assert by_id[simulator]["reason_codes"] == ["ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED", "OPTIONAL_SIMULATION"]


def test_legacy_execution_registry_mismatch_is_flagged_not_used(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = _seven_sleeves()
    repo = _repo(tmp_path, sleeves)
    _runtime(root)
    _sleeve_eval(root, sleeves)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["total_sleeves_expected"] == 7
    assert payload["registry_mismatches"]
    assert payload["registry_mismatches"][0]["registered_sleeve_ids"] == ["PRIMARY"]


def test_per_sleeve_blocked_status_is_visible_for_every_enabled_sleeve(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = _seven_sleeves()
    repo = _repo(tmp_path, sleeves)
    _runtime(root)
    _sleeve_eval(root, sleeves, blocked="C2_MARKET_NEUTRAL_SPREAD_V1")

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    by_id = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert len(by_id) == 7
    assert by_id["C2_MARKET_NEUTRAL_SPREAD_V1"]["run_status"] == "BLOCKED"
    assert by_id["C2_MARKET_NEUTRAL_SPREAD_V1"]["canonical_blocker"] == "PRODUCER_NONZERO_RC"
    assert "next_repair_action" in by_id["C2_MARKET_NEUTRAL_SPREAD_V1"]


def test_partial_readiness_run_is_not_global_data_blocked(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = ["C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"]
    repo = _repo(tmp_path, sleeves)
    _runtime(root, missing_count=1)
    _sleeve_eval(root, sleeves, blocked="C2_VOL_INCOME_DEFINED_RISK_V1")
    _readiness(root, sleeves, blocked="C2_VOL_INCOME_DEFINED_RISK_V1")

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["operator_interpretation"] == "PARTIAL_RUN"
    assert payload["exact_blocker"] == "SLEEVE_INPUT_REQUIREMENT_BLOCKED"
    assert payload["blocking_policy"] == "PER_SLEEVE_INPUT_CONTRACTS"
    assert payload["total_sleeves_ready"] == 1
    assert payload["total_sleeves_blocked"] == 1


def test_partial_run_zero_valid_opportunities_explains_raw_signal_rejection(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = ["C2_CROSS_ASSET_TREND_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"]
    repo = _repo(tmp_path, sleeves)
    _runtime(root, missing_count=1)
    cross_eval = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_CROSS_ASSET_TREND_V1" / "sleeve_evaluation.v1.json"
    cross_payload = {
            "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
            "engine_id": "C2_CROSS_ASSET_TREND_V1",
            "status": "INTENT_CREATED",
            "current_status": "INTENT_CREATED",
            "output_intents": [{"intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1", "symbol": "QQQ"}],
            "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED", "PERSISTENT_SIGNAL_NO_POSITION", "UNCHANGED_SIGNAL"],
            "market_data_manifest_check": {"status": "PASS"},
            "artifact_path": str(cross_eval),
        }
    vol_payload = {
        "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
        "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
        "enabled": True,
        "activation_status": "ACTIVE",
        "status": "BLOCKED",
        "current_status": "BLOCKED",
        "canonical_blocker": "SLEEVE_INPUT_REQUIREMENT_BLOCKED",
        "reason_codes": ["SLEEVE_INPUT_REQUIREMENT_BLOCKED", "market.volatility.VIX"],
        "exit_code": 0,
        "producer_command": "python3 run_vix.py",
        "market_data_manifest_check": {"status": "PASS"},
        "output_intents": [],
        "artifact_path": str(root / "unused.json"),
    }
    _write_json(cross_eval, cross_payload)
    _write_json(
        root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {"day_utc": DAY, "status": "PARTIAL", "outcomes": [cross_payload, vol_payload]},
    )
    _readiness(root, sleeves, blocked="C2_VOL_INCOME_DEFINED_RISK_V1")
    _candidate_manifest_with_cross_asset_raw_signal(root)
    _candidate_lineage_with_cross_asset_not_consumed(root)
    _intent_arbitration_with_cross_asset_missing_score(root)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    by_id = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert payload["operator_interpretation"] == "PARTIAL_RUN"
    assert payload["total_candidates_generated"] == 0
    assert payload["raw_signal_rejections"][0]["raw_signal_id"] == "c2_cross_asset_trend_qqq_2026-05-18_v1"
    assert payload["raw_signal_rejections"][0]["sleeve_id"] == "C2_CROSS_ASSET_TREND_V1"
    assert payload["raw_signal_rejections"][0]["rejection_stage"] == "PORTFOLIO_SCORING"
    assert payload["raw_signal_rejections"][0]["rejection_reason"] == "PORTFOLIO_SCORING_MISSING_INTENT_SCORE"
    assert payload["raw_signal_rejections"][0]["safety_related"] is True
    assert "portfolio scoring did not produce a score" in payload["raw_signal_rejections"][0]["human_readable_explanation"]
    assert "Partial run completed" in payload["zero_candidate_explanation"]
    assert by_id["C2_CROSS_ASSET_TREND_V1"]["rejection_reasons"] == ["PORTFOLIO_SCORING_MISSING_INTENT_SCORE"]
    assert by_id["C2_CROSS_ASSET_TREND_V1"]["raw_signal_rejections"][0]["required_next_action"].startswith("Regenerate portfolio scoring")
    assert by_id["C2_VOL_INCOME_DEFINED_RISK_V1"]["run_status"] == "BLOCKED"
    assert by_id["C2_VOL_INCOME_DEFINED_RISK_V1"]["blocking_inputs"] == ["market.volatility.VIX"]


def test_selected_scored_raw_signal_without_promotion_is_precise_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = ["C2_CROSS_ASSET_TREND_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"]
    repo = _repo(tmp_path, sleeves)
    _runtime(root, missing_count=1)
    cross_eval = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_CROSS_ASSET_TREND_V1" / "sleeve_evaluation.v1.json"
    cross_payload = {
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "engine_id": "C2_CROSS_ASSET_TREND_V1",
        "status": "INTENT_CREATED",
        "current_status": "INTENT_CREATED",
        "output_intents": [{"intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1", "symbol": "QQQ"}],
        "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED", "PERSISTENT_SIGNAL_NO_POSITION", "UNCHANGED_SIGNAL"],
        "market_data_manifest_check": {"status": "PASS"},
        "artifact_path": str(cross_eval),
    }
    vol_payload = {
        "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
        "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
        "status": "BLOCKED",
        "current_status": "BLOCKED",
        "canonical_blocker": "SLEEVE_INPUT_REQUIREMENT_BLOCKED",
        "reason_codes": ["SLEEVE_INPUT_REQUIREMENT_BLOCKED", "market.volatility.VIX"],
    }
    _write_json(cross_eval, cross_payload)
    _write_json(
        root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {"day_utc": DAY, "status": "PARTIAL", "outcomes": [cross_payload, vol_payload]},
    )
    _readiness(root, sleeves, blocked="C2_VOL_INCOME_DEFINED_RISK_V1")
    _candidate_manifest_with_cross_asset_raw_signal(root)
    _candidate_lineage_with_cross_asset_not_consumed(root)
    _intent_arbitration_with_cross_asset_selected(root)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    rejection = payload["raw_signal_rejections"][0]

    assert rejection["raw_signal_id"] == "c2_cross_asset_trend_qqq_2026-05-18_v1"
    assert rejection["portfolio_scoring_status"] == "SCORED"
    assert rejection["portfolio_score_rank"] == 1
    assert rejection["portfolio_score_total"] == "36.9"
    assert rejection["selected_by_intent_arbitration"] is True
    assert rejection["rejection_stage"] == "PROMOTION"
    assert rejection["rejection_reason"] == "SELECTED_INTENT_NOT_PROMOTED_TO_OPPORTUNITY"
    assert rejection["safety_related"] is True
    assert "failed closed instead of fabricating an opportunity" in rejection["human_readable_explanation"]
    assert payload["total_candidates_generated"] == 0



def _candidate_manifest_with_trend_raw_signals(root: Path) -> None:
    _write_json(
        root / "reports/candidate_generation_manifest_v1" / DAY / f"sleeve_evaluation_kernel_v1:{DAY}" / "candidate_generation_manifest.v1.json",
        {
            "schema_id": "candidate_generation_manifest",
            "day_utc": DAY,
            "run_id": f"sleeve_evaluation_kernel_v1:{DAY}",
            "candidate_rows": [
                {
                    "candidate_id": "trend-amt",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "status": "CANDIDATE_CREATED",
                    "symbol_or_pair": "AMT",
                    "raw_intent_id": "c2_trend_eq_amt_2026-05-18_v1",
                    "raw_intent_hash": "amt-hash",
                    "raw_intent_path": "/truth/intents/amt.exposure_intent.v1.json",
                    "lifecycle_decision": "INTENT_CREATED",
                    "lifecycle_reason_codes": ["SIGNAL_CHANGED"],
                    "allowed_by_portfolio_gate": True,
                    "portfolio_gate_decision": "ALLOW",
                    "output_artifact_paths": ["/truth/reports/sleeve_evaluation.v1.json"],
                },
                {
                    "candidate_id": "trend-bac",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "status": "SUPPRESSED",
                    "symbol_or_pair": "BAC",
                    "raw_intent_id": "c2_trend_eq_bac_2026-05-18_v1",
                    "raw_intent_hash": "bac-hash",
                    "raw_intent_path": "/truth/intents/bac.exposure_intent.v1.json",
                    "lifecycle_decision": "INTENT_CREATED",
                    "lifecycle_reason_codes": ["SIGNAL_CHANGED"],
                    "allowed_by_portfolio_gate": False,
                    "portfolio_gate_decision": "SUPPRESS",
                    "rejection_reason": "PORTFOLIO_GATE_SUPPRESSED",
                    "output_artifact_paths": ["/truth/reports/sleeve_evaluation.v1.json"],
                },
            ],
        },
    )


def _intent_arbitration_with_trend_rejections(root: Path) -> None:
    _write_json(
        root / "reports/intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {
            "schema_id": "intent_arbitration",
            "status": "BLOCKED",
            "portfolio_scoring_path": "/truth/reports/portfolio_scoring_v1/2026-05-18/portfolio_scoring.v1.json",
            "selected_intent": {},
            "raw_candidate_intents": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "symbol": "AMT", "intent_id": "c2_trend_eq_amt_2026-05-18_v1", "intent_path": "/truth/intents/amt.exposure_intent.v1.json"},
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "symbol": "BAC", "intent_id": "c2_trend_eq_bac_2026-05-18_v1", "intent_path": "/truth/intents/bac.exposure_intent.v1.json"},
            ],
            "rejected_or_filtered_intents": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "symbol": "BAC",
                    "intent_id": "c2_trend_eq_bac_2026-05-18_v1",
                    "intent_path": "/truth/intents/bac.exposure_intent.v1.json",
                    "rejection_reason": "PORTFOLIO_GATE_SUPPRESSED",
                    "portfolio_gate_decision": "SUPPRESS",
                    "portfolio_gate_reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"],
                },
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "symbol": "AMT",
                    "intent_id": "c2_trend_eq_amt_2026-05-18_v1",
                    "intent_path": "/truth/intents/amt.exposure_intent.v1.json",
                    "rejection_reason": "NON_CERTIFIED_CANDIDATE_SNAPSHOT",
                    "portfolio_scoring_status": "SCORED",
                    "portfolio_score_rank": 999999,
                    "portfolio_score_total": 0.0,
                    "portfolio_gate_decision": "ALLOW",
                    "executable_eligible": False,
                    "score_unavailable_reason": "NON_CERTIFIED_CANDIDATE_SNAPSHOT",
                    "scoring_reason_codes": ["SCORING_NOT_EXECUTABLE_NON_CERTIFIED_INPUT"],
                },
            ],
        },
    )


def _selected_intent_promotion_contract_failed(root: Path) -> None:
    _write_json(
        root / "reports/aegis_selected_intent_promotion_v1" / DAY / "selected_intent_promotion.v1.json",
        {
            "schema_id": "aegis_selected_intent_promotion",
            "day_utc": DAY,
            "status": "CONTRACT_FAILED",
            "promotion_status": "blocked",
            "selected_intent_id": "",
            "sleeve_id": "",
            "symbol": "",
            "promoted_candidate_count": 0,
            "missing_contract_fields": ["selected_intent.intent_id", "portfolio_scoring.executable_eligible"],
            "operator_review_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
        },
    )


def test_raw_signals_zero_candidates_expose_rejection_and_contract_visibility(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = ["C2_TREND_EQ_PRIMARY_V1"]
    repo = _repo(tmp_path, sleeves)
    _runtime(root)
    eval_path = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_TREND_EQ_PRIMARY_V1" / "sleeve_evaluation.v1.json"
    eval_payload = {
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "status": "INTENT_CREATED",
        "current_status": "INTENT_CREATED",
        "output_intents": [
            {"intent_id": "c2_trend_eq_amt_2026-05-18_v1", "symbol": "AMT"},
            {"intent_id": "c2_trend_eq_bac_2026-05-18_v1", "symbol": "BAC"},
        ],
        "reason_codes": ["SIGNAL_CHANGED"],
        "market_data_manifest_check": {"status": "PASS"},
        "artifact_path": str(eval_path),
    }
    _write_json(eval_path, eval_payload)
    _write_json(root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"day_utc": DAY, "status": "PARTIAL", "outcomes": [eval_payload]})
    _readiness(root, sleeves)
    _candidate_manifest_with_trend_raw_signals(root)
    _intent_arbitration_with_trend_rejections(root)
    _selected_intent_promotion_contract_failed(root)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    visibility = build_candidate_generation_visibility_v1(payload)
    by_symbol = {row["symbol"]: row for row in payload["raw_signal_rejections"]}

    assert payload["total_raw_signals"] == 2
    assert payload["total_candidates_generated"] == 0
    assert payload["symbols_evaluated"] == ["AMT", "BAC"]
    assert payload["raw_signals_by_sleeve"] == {"C2_TREND_EQ_PRIMARY_V1": 2}
    assert payload["raw_signals_by_symbol"] == {"AMT": 1, "BAC": 1}
    assert by_symbol["BAC"]["rejection_stage"] == "PORTFOLIO_GATE"
    assert by_symbol["BAC"]["rejection_reason"] == "PORTFOLIO_GATE_SUPPRESSED"
    assert by_symbol["BAC"]["candidate_gate_failed"] is True
    assert by_symbol["BAC"]["promotion_gate_failed"] is False
    assert by_symbol["AMT"]["rejection_stage"] == "PORTFOLIO_SCORING"
    assert by_symbol["AMT"]["rejection_reason"] == "NON_CERTIFIED_CANDIDATE_SNAPSHOT"
    assert by_symbol["AMT"]["candidate_gate_failed"] is True
    assert by_symbol["AMT"]["promotion_gate_failed"] is True
    assert by_symbol["AMT"]["selected_intent_promotion_status"] == "CONTRACT_FAILED"
    assert payload["selected_intent_promotion"]["status"] == "CONTRACT_FAILED"
    assert payload["safety"] == {
        "advisory_only": True,
        "read_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "broker_submit_transmit_called": False,
    }
    visible = {row["symbol"]: row for row in visibility["rejected_candidate_visibility"]}
    assert visible["AMT"]["promotion_gate_failed"] is True
    assert visible["AMT"]["candidate_gate_failed"] is True
    assert visibility["safety"]["trade_advice_allowed"] is False
    assert visibility["safety"]["broker_execution_allowed"] is False
    assert visibility["safety"]["autonomous_execution_allowed"] is False

def test_promoted_selected_intent_is_not_reported_as_raw_signal_rejection(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    sleeves = ["C2_CROSS_ASSET_TREND_V1"]
    repo = _repo(tmp_path, sleeves)
    _runtime(root)
    cross_eval = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_CROSS_ASSET_TREND_V1" / "sleeve_evaluation.v1.json"
    cross_payload = {
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "engine_id": "C2_CROSS_ASSET_TREND_V1",
        "status": "INTENT_CREATED",
        "current_status": "INTENT_CREATED",
        "output_intents": [{"intent_id": "c2_cross_asset_trend_qqq_2026-05-18_v1", "symbol": "QQQ"}],
        "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED"],
        "market_data_manifest_check": {"status": "PASS"},
        "artifact_path": str(cross_eval),
    }
    _write_json(cross_eval, cross_payload)
    _write_json(
        root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {"day_utc": DAY, "status": "PASS", "outcomes": [cross_payload]},
    )
    _readiness(root, sleeves)
    _candidate_manifest_with_cross_asset_raw_signal(root)
    _candidate_lineage_with_cross_asset_not_consumed(root)
    _intent_arbitration_with_cross_asset_selected(root)
    _selected_intent_promotion_passed(root)

    payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    assert payload["raw_signal_rejections"] == []
    assert payload["selected_intent_promotion"]["status"] == "PROMOTED_TO_OPERATOR_REVIEW"
    assert payload["selected_intent_promotion"]["promoted_candidate_count"] == 1
