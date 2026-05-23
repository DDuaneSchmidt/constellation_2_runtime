from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.portfolio_gate_candidate_report_v1 import build_portfolio_gate_candidate_report_v1, write_portfolio_gate_candidate_report_v1


DAY = "2026-05-20"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _intent(path: Path, intent_id: str, symbol: str, engine_id: str) -> str:
    _write(path, {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "exposure_type": "LONG_EQUITY",
        "underlying": {"symbol": symbol},
        "engine": {"engine_id": engine_id},
    })
    return str(path)


def _fixture(tmp_path: Path) -> Path:
    truth = tmp_path / "truth"
    selected_intent = _intent(truth / "intents_v1/snapshots" / DAY / "sel.exposure_intent.v1.json", "selected", "AMT", "C2_TREND_EQ_PRIMARY_V1")
    suppressed_intent = _intent(truth / "intents_v1/snapshots" / DAY / "sup.exposure_intent.v1.json", "suppressed", "AAPL", "C2_TREND_EQ_PRIMARY_V1")
    gate = {
        "decisions": [
            {"raw_intent_id": "selected", "portfolio_gate_decision": "ALLOW", "regime_bucket": "TREND", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1"},
            {"raw_intent_id": "suppressed", "portfolio_gate_decision": "SUPPRESS", "regime_bucket": "TREND", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"]},
        ],
        "approved_executable_intents": [{"raw_intent_id": "selected"}],
        "suppressed_or_signal_only_intents": [{"raw_intent_id": "suppressed"}],
        "policy": {"rules": ["ONE_PRIMARY_PER_REGIME_BUCKET"]},
    }
    scoring = {
        "rankings": [
            {
                "intent_id": "selected",
                "symbol": "AMT",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "rank": 1,
                "score_total": 31.3,
                "regime_confidence_level": "UNKNOWN",
                "portfolio_gate_decision": "ALLOW",
                "allowed_by_portfolio_gate": True,
                "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
                "regime_bucket": "TREND",
                "evidence_paths": [selected_intent],
            },
            {
                "intent_id": "suppressed",
                "symbol": "AAPL",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "rank": 2,
                "score_total": 0,
                "regime_confidence_level": "UNKNOWN",
                "portfolio_gate_decision": "SUPPRESS",
                "allowed_by_portfolio_gate": False,
                "reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED", "SCORING_NOT_EXECUTABLE_SUPPRESS"],
                "regime_bucket": "TREND",
                "evidence_paths": [suppressed_intent],
                "position_match_status": "POSITION_STATE_STALE",
            },
        ],
        "selected_candidate_intent_id": "selected",
    }
    _write(truth / "reports/portfolio_activation_gate_v1" / DAY / "portfolio_activation_gate.v1.json", gate)
    _write(truth / "reports/portfolio_scoring_v1" / DAY / "portfolio_scoring.v1.json", scoring)
    return truth


def test_portfolio_gate_candidate_report_explains_suppression_without_execution(tmp_path: Path) -> None:
    truth = _fixture(tmp_path)
    report = build_portfolio_gate_candidate_report_v1(day_utc=DAY, truth_root=truth)

    assert report["candidate_count"] == 2
    assert report["selected_candidate_id"] == "selected"
    assert report["suppressed_count"] == 1
    assert report["suppression_code_counts"] == {"one_primary_per_regime_bucket_suppressed": 1}
    suppressed = report["top_10_suppressed"][0]
    assert suppressed["candidate_id"] == "suppressed"
    assert suppressed["competing_selected_candidate_id"] == "selected"
    assert suppressed["max_one_intent_per_cycle"] is True
    assert suppressed["portfolio_concentration"] is True
    assert suppressed["submit_boundary_precheck"] is True
    assert report["gate_policy_answers"]["designed_to_select_one_per_regime_bucket"] is True
    assert report["fix_implemented"] == "diagnostics_only"
    assert report["safety"]["trades_created"] is False
    assert report["safety"]["paper_submit_created"] is False


def test_portfolio_gate_candidate_report_persistence(tmp_path: Path) -> None:
    truth = _fixture(tmp_path)
    report = build_portfolio_gate_candidate_report_v1(day_utc=DAY, truth_root=truth)
    written = write_portfolio_gate_candidate_report_v1(truth_root=truth, report=report)

    assert Path(written["artifact_path"]).exists()
    assert written["artifact_hash"]
