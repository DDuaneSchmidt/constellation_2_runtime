from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.operator_state.canonical_operator_state_builder_v1 import (
    build_operator_state_snapshot_v1,
    load_or_build_operator_state_snapshot_response_v1,
    operator_state_snapshot_path_v1,
    write_operator_state_snapshot_v1,
)
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1

DAY = "2026-05-20"
INTENT_ID = "c2_trend_eq_amt_2026-05-20_v1"
INTENT_HASH = "a" * 64


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed(root: Path, *, malformed_conversion: bool = False, omit_selected_intent: bool = False, omit_diagnostics: bool = False, seed_gate_report: bool = False) -> None:
    intent_path = root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json"
    if not omit_selected_intent:
        _write(intent_path, {"schema_id": "exposure_intent", "schema_version": "v1", "intent_id": INTENT_ID, "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1"}, "exposure_type": "LONG_EQUITY", "underlying": {"symbol": "AMT", "currency": "USD"}, "target_notional_pct": "0.01", "constraints": {"max_risk_pct": "0.01"}})
    _write(root / "pointers" / "selected_intent_pointer.v1.json", {"schema_id": "selected_intent_pointer", "schema_version": "v1", "status": "SELECTED", "day_utc": DAY, "selected_intent": {"intent_id": INTENT_ID, "intent_hash": INTENT_HASH, "intent_path": str(intent_path.resolve()), "engine_id": "C2_TREND_EQ_PRIMARY_V1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "symbol": "AMT", "selection_reason": "HIGHEST_PORTFOLIO_SCORE_V1"}})
    if not omit_diagnostics:
        rows = [{"candidate_id": INTENT_ID, "symbol": "AMT", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "engine_id": "C2_TREND_EQ_PRIMARY_V1", "proposed_direction": "LONG", "score": 31.3333, "confidence": 23.0, "blocker_code": "EXPOSURE_INTENT_NOT_CONVERTED_TO_EXECUTION_PACKAGE", "blocker_message": "not converted", "final_disposition": "SELECTED_BY_ARBITRATION_NO_PAPER_TRADE_INTENT_CREATED"}]
        for idx in range(41):
            rows.append({"candidate_id": f"suppressed-{idx}", "symbol": f"S{idx}", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "engine_id": "C2_TREND_EQ_PRIMARY_V1", "proposed_direction": "LONG", "score": 0.0, "confidence": 0.0, "blocker_code": "PORTFOLIO_GATE_SUPPRESSED", "blocker_message": "Portfolio activation gate suppressed this candidate; one primary per regime bucket is enforced.", "portfolio_gate_decision": "SUPPRESS", "portfolio_gate_reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"]})
        _write(root / "reports" / "paper_intent_candidate_diagnostics_v1" / DAY / "paper_intent_candidate_diagnostics.v1.json", {"schema_id": "paper_intent_candidate_diagnostics", "schema_version": "v1", "day_utc": DAY, "selected_exposure_intent_id": INTENT_ID, "candidates_found": 42, "candidate_rows": rows, "paper_trade_intent_created": False})
    if seed_gate_report:
        rows = [{"candidate_id": INTENT_ID, "symbol": "AMT", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "engine_id": "C2_TREND_EQ_PRIMARY_V1", "direction": "LONG", "score": 31.3333, "confidence": "UNKNOWN", "rank_before_arbitration": 1, "selected_by_gate": "YES", "suppression_code": "SELECTED", "suppression_reason": "Selected by portfolio activation gate.", "portfolio_gate_decision": "ALLOW", "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"], "evidence_paths": [str(intent_path.resolve())]}]
        for idx in range(41):
            rows.append({"candidate_id": f"suppressed-{idx}", "symbol": f"S{idx}", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "engine_id": "C2_TREND_EQ_PRIMARY_V1", "direction": "LONG", "score": 0.0, "confidence": "UNKNOWN", "rank_before_arbitration": idx + 2, "selected_by_gate": "NO", "suppression_code": "one_primary_per_regime_bucket_suppressed", "suppression_reason": "Suppressed by ONE_PRIMARY_PER_REGIME_BUCKET after another candidate in the same regime bucket was allowed.", "portfolio_gate_decision": "SUPPRESS", "reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"], "competing_selected_candidate_id": INTENT_ID, "evidence_paths": []})
        _write(root / "reports" / "portfolio_gate_candidate_report_v1" / DAY / "portfolio_gate_candidate_report.v1.json", {"schema_id": "portfolio_gate_candidate_report", "schema_version": "portfolio_gate_candidate_report.v1", "day_utc": DAY, "selected_candidate_id": INTENT_ID, "candidate_count": 42, "selected_count": 1, "suppressed_count": 41, "suppression_code_counts": {"one_primary_per_regime_bucket_suppressed": 41}, "candidate_rows": rows, "diagnosis": "expected_governance_behavior"})
    conv_path = root / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json"
    if malformed_conversion:
        conv_path.parent.mkdir(parents=True, exist_ok=True)
        conv_path.write_text("{not json", encoding="utf-8")
    else:
        _write(conv_path, {"schema_id": "exposure_intent_paper_submission_package", "schema_version": "v1", "day_utc": DAY, "status": "BLOCKED", "exposure_intent_id": INTENT_ID, "symbol": "AMT", "paper_trade_intent_created": False, "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION", "blocker_message": "AMT market data is not current for 2026-05-20; observed_session=2026-04-02. No stale data accepted.", "market_data_status": {"expected_session": DAY, "observed_session": "2026-04-02", "status": "STALE_OR_MISSING", "path": "/tmp/AMT/2026.jsonl", "close_present": True}})
    _write(root / "market_data_snapshot_v1" / "dataset_manifest.json", {"symbols": ["AMT", "SPY"]})


def test_builder_includes_selected_amt_stale_blocker_and_41_suppressed(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, seed_gate_report=True)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY, generated_at_utc="2026-05-20T00:00:00Z")

    assert payload["manual_capture_candidate"]["candidate_available"] is True
    assert payload["manual_capture_candidate"]["selected_exposure_intent_id"] == INTENT_ID
    assert payload["manual_capture_candidate"]["symbol"] == "AMT"
    assert payload["manual_capture_candidate"]["blocker_code"] == "STALE_MARKET_DATA_BLOCKS_CONVERSION"
    assert payload["market_data_freshness"]["latest_market_session"] == "2026-04-02"
    assert payload["suppressed_candidate_watchlist"]["suppressed_count"] == 41
    assert payload["suppressed_candidate_watchlist"]["suppression_code_counts"] == {"one_primary_per_regime_bucket_suppressed": 41}
    assert payload["source_artifacts"]


def test_builder_degrades_when_selected_exposure_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_selected_intent=True, seed_gate_report=True)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    assert payload["manual_capture_candidate"]["candidate_available"] is True
    assert any(err["code"] == "SELECTED_EXPOSURE_INTENT_MISSING" for err in payload["errors"])
    assert "source_artifacts" in payload["degraded_sections"]


def test_builder_degrades_when_diagnostics_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    assert payload["current_truth_status"] == "HISTORICAL_ONLY"
    assert payload["manual_capture_candidate"]["candidate_available"] is False
    assert payload["suppressed_candidate_watchlist"]["suppressed_count"] == 0
    assert any(err["code"] == "CURRENT_OPERATOR_TRUTH_UNAVAILABLE" for err in payload["errors"])


def test_builder_uses_portfolio_gate_report_when_legacy_candidate_diagnostics_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert payload["manual_capture_candidate"]["selected_exposure_intent_id"] == INTENT_ID
    assert payload["manual_capture_candidate"]["symbol"] == "AMT"
    assert payload["latest_run_summary"]["candidates_found"] == 42
    assert payload["latest_run_summary"]["selected_candidate_count"] == 1
    assert payload["latest_run_summary"]["suppressed_count"] == 41
    assert payload["latest_run_summary"]["blocked_conversion_count"] == 1
    assert payload["suppressed_candidate_watchlist"]["suppressed_count"] == 41
    assert payload["suppressed_candidate_watchlist"]["candidates"][0]["competing_selected_candidate_id"] == INTENT_ID
    assert not any(err["code"] == "PAPER_INTENT_CANDIDATE_DIAGNOSTICS_MISSING" for err in payload["errors"])
    assert payload["eod_outcome_projection"]["selected_candidate_count"] == 1
    assert payload["eod_outcome_projection"]["suppressed_candidate_count"] == 41
    assert payload["eod_outcome_projection"]["blocked_conversion_count"] == 1


def test_current_gate_selected_amt_beats_older_selected_pointer_qqq(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    qqq_path = root / "intents_v1" / "snapshots" / "2026-05-19" / f"{'q' * 64}.exposure_intent.v1.json"
    _write(qqq_path, {"schema_id": "exposure_intent", "schema_version": "v1", "intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "engine": {"engine_id": "C2_CROSS_ASSET_TREND_V1"}, "exposure_type": "LONG_EQUITY", "underlying": {"symbol": "QQQ"}})
    _write(root / "pointers" / "selected_intent_pointer.v1.json", {"schema_id": "selected_intent_pointer", "schema_version": "v1", "status": "SELECTED", "day_utc": "2026-05-19", "selected_intent": {"intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "intent_path": str(qqq_path.resolve()), "engine_id": "C2_CROSS_ASSET_TREND_V1", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "symbol": "QQQ", "selection_reason": "OLDER_POINTER"}})

    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert payload["manual_capture_candidate"]["selected_exposure_intent_id"] == INTENT_ID
    assert payload["manual_capture_candidate"]["symbol"] == "AMT"
    assert payload["manual_capture_candidate"]["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert payload["manual_capture_candidate"]["source_day"] == DAY
    assert payload["manual_capture_candidate"]["stale_source"] is False


def test_current_truth_resolver_current_gate_beats_stale_pointer(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    qqq_path = root / "intents_v1" / "snapshots" / "2026-05-19" / f"{'q' * 64}.exposure_intent.v1.json"
    _write(qqq_path, {"schema_id": "exposure_intent", "schema_version": "v1", "intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "engine": {"engine_id": "C2_CROSS_ASSET_TREND_V1"}, "exposure_type": "LONG_EQUITY", "underlying": {"symbol": "QQQ"}})
    _write(root / "pointers" / "selected_intent_pointer.v1.json", {"schema_id": "selected_intent_pointer", "schema_version": "v1", "status": "SELECTED", "day_utc": "2026-05-19", "selected_intent": {"intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "intent_path": str(qqq_path.resolve()), "engine_id": "C2_CROSS_ASSET_TREND_V1", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "symbol": "QQQ", "selection_reason": "OLDER_POINTER"}})

    truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=DAY)

    assert truth["current_truth_status"] == "CURRENT"
    assert truth["source_day"] == DAY
    assert truth["selected_exposure"]["candidate_id"] == INTENT_ID
    assert truth["selected_exposure"]["symbol"] == "AMT"
    assert truth["suppressed_count"] == 41


def test_converter_mismatch_does_not_mix_blocker_with_selected_candidate(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    conv_path = root / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json"
    _write(conv_path, {"schema_id": "exposure_intent_paper_submission_package", "schema_version": "v1", "day_utc": DAY, "status": "BLOCKED", "exposure_intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "symbol": "QQQ", "paper_trade_intent_created": False, "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION", "blocker_message": "QQQ stale data should not be applied to AMT."})

    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert payload["manual_capture_candidate"]["selected_exposure_intent_id"] == INTENT_ID
    assert payload["manual_capture_candidate"]["symbol"] == "AMT"
    assert payload["manual_capture_candidate"]["blocker_code"] == ""
    assert payload["manual_capture_candidate"]["source_mismatch_warning"]
    assert any(err["code"] == "CURRENT_TRUTH_CONVERSION_MISMATCH" for err in payload["errors"])


def test_current_truth_resolver_converter_mismatch_does_not_merge_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    conv_path = root / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json"
    _write(conv_path, {"schema_id": "exposure_intent_paper_submission_package", "schema_version": "v1", "day_utc": DAY, "status": "BLOCKED", "exposure_intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "symbol": "QQQ", "paper_trade_intent_created": False, "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION", "blocker_message": "QQQ stale data should not be applied to AMT."})

    truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=DAY)

    assert truth["current_truth_status"] == "CURRENT"
    assert truth["selected_exposure"]["candidate_id"] == INTENT_ID
    assert truth["selected_exposure"]["symbol"] == "AMT"
    assert truth["conversion"] == {}
    assert truth["conversion_blocker"] == ""
    assert truth["source_mismatch_warning"]
    assert any(err["code"] == "CURRENT_TRUTH_CONVERSION_MISMATCH" for err in truth["errors"])


def test_older_selected_pointer_is_not_displayed_as_current_when_no_current_gate_report(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True)
    qqq_path = root / "intents_v1" / "snapshots" / "2026-05-19" / f"{'q' * 64}.exposure_intent.v1.json"
    _write(qqq_path, {"schema_id": "exposure_intent", "schema_version": "v1", "intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "engine": {"engine_id": "C2_CROSS_ASSET_TREND_V1"}, "exposure_type": "LONG_EQUITY", "underlying": {"symbol": "QQQ"}})
    _write(root / "pointers" / "selected_intent_pointer.v1.json", {"schema_id": "selected_intent_pointer", "schema_version": "v1", "status": "SELECTED", "day_utc": "2026-05-19", "selected_intent": {"intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "intent_path": str(qqq_path.resolve()), "engine_id": "C2_CROSS_ASSET_TREND_V1", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "symbol": "QQQ", "selection_reason": "OLDER_POINTER"}})

    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert payload["current_truth_status"] == "HISTORICAL_ONLY"
    assert payload["manual_capture_candidate"]["candidate_available"] is False
    assert payload["manual_capture_candidate"]["symbol"] == ""
    assert payload["manual_capture_candidate"]["selected_exposure_intent_id"] == ""
    assert any(err["code"] == "CURRENT_OPERATOR_TRUTH_UNAVAILABLE" for err in payload["errors"])


def test_builder_handles_malformed_converter_artifact(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, malformed_conversion=True, seed_gate_report=True)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    assert any(err["code"] == "EXPOSURE_INTENT_CONVERSION_MALFORMED" for err in payload["errors"])
    assert payload["manual_capture_candidate"]["paper_trade_intent_created"] is False


def test_load_or_build_response_returns_degraded_200_contract(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, seed_gate_report=True)
    response = load_or_build_operator_state_snapshot_response_v1(truth_root=root, day_utc=DAY)
    assert response["ok"] is True
    assert "data" in response
    artifact_path = Path(response["data"]["artifact_path"])
    assert artifact_path.exists()
    assert str(artifact_path.resolve()).startswith(str(root.resolve()))
    assert response["data"]["manual_capture_candidate"]["symbol"] == "AMT"


def test_written_snapshot_path_is_stable(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, seed_gate_report=True)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    path = write_operator_state_snapshot_v1(truth_root=root, day_utc=DAY, payload=payload)
    assert path == operator_state_snapshot_path_v1(truth_root=root, day_utc=DAY)
    assert path.exists()


def _seed_current_day_market_failure(root: Path, day: str = "2026-05-21") -> None:
    _write(
        root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json",
        {
            "schema_id": "aegis_market_data",
            "schema_version": "v1",
            "day_utc": day,
            "status": "FAILED",
            "failure_reason": "MARKET_DATA_FETCH_FAILED",
            "generated_at_utc": "2026-05-21T18:55:00Z",
            "provider_failed_symbols": ["DBC", "GLD", "HYG", "IEF", "IWM", "LQD", "QQQ", "SPY", "TLT", "UUP", "VIX"],
            "provider_results": [{"provider": "STOOQ", "status": "FAILED", "failure_reason": "MARKET_DATA_FETCH_FAILED"}],
            "usable_for_candidate_generation": False,
        },
    )
    _write(
        root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json",
        {
            "schema_id": "market_data_inputs",
            "schema_version": "v1",
            "day_utc": day,
            "status": "STALE",
            "validation_status": "BLOCKED",
            "stale_input_ids": ["market.price.SPY", "market.price.QQQ", "market.volatility.VIX"],
        },
    )


def _seed_blocked_candidate_manifest(root: Path, day: str = "2026-05-21") -> None:
    _write(
        root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json",
        {"schema_id": "aegis_market_data", "schema_version": "v1", "day_utc": day, "status": "CURRENT", "generated_at_utc": "2026-05-21T18:55:00Z"},
    )
    _write(
        root / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json",
        {
            "schema_id": "candidate_generation_manifest",
            "schema_version": "v1",
            "day_utc": day,
            "produced_at_utc": "2026-05-21T18:56:00Z",
            "summary": {"candidate_count": 2, "status_counts": {"BLOCKED": 2}},
            "candidate_rows": [
                {"candidate_id": "blocked-spy", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "symbol": "SPY", "status": "BLOCKED"},
                {"candidate_id": "blocked-qqq", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "symbol": "QQQ", "status": "BLOCKED"},
            ],
        },
    )
    _write(
        root / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json",
        {"schema_id": "intent_arbitration", "schema_version": "v1", "day_utc": day, "status": "BLOCKED", "canonical_blocker": "SLEEVE_INPUT_REQUIREMENT_BLOCKED"},
    )


def test_current_day_market_failure_is_not_silently_replaced_by_prior_gate(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    failed_day = "2026-05-21"
    _seed_current_day_market_failure(root, failed_day)

    truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=failed_day)

    assert truth["current_truth_status"] == "PENDING_VENDOR_DATA"
    assert truth["source_day"] == failed_day
    assert truth["displayed_artifact_day"] == DAY
    assert truth["historical_fallback"]["current_truth_status"] == "STALE_FALLBACK"
    assert truth["current_day_status"]["failed_step"] == "market_data_vendor_pending"
    assert "SPY" in truth["current_day_status"]["missing_symbols"]
    assert truth["selected_exposure"] == {}


def test_prior_day_capture_is_not_counted_as_current_day_capture(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, omit_diagnostics=True, seed_gate_report=True)
    failed_day = "2026-05-21"
    _seed_current_day_market_failure(root, failed_day)

    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=failed_day)

    strip = payload["operator_today_projection"]
    assert strip["current_runtime_day"] == failed_day
    assert strip["displayed_artifact_day"] == DAY
    assert strip["current_day_run_status"] == "PENDING_VENDOR_DATA"
    assert strip["completed_capture_count"] == 0
    assert strip["historical_completed_capture_count"] == 1


def test_current_day_blocked_candidates_are_visible_without_selected_gate(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    day = "2026-05-21"
    _seed_blocked_candidate_manifest(root, day)

    truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=day)

    assert truth["current_truth_status"] == "CURRENT_DAY_BLOCKED"
    assert truth["current_day_status"]["failed_step"] == "sleeve_evaluation"
    assert truth["current_day_status"]["candidate_count"] == 2
    assert truth["current_day_status"]["blocked_candidate_count"] == 2
    assert truth["selected_exposure"] == {}
