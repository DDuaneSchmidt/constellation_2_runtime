from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.regime_bucket_candidate_ranking_v1 import build_regime_bucket_candidate_ranking_report_v1, regime_bucket_candidate_ranking_report_path

DAY = "2026-05-20"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _intent(root: Path, intent_id: str, symbol: str, score: float) -> str:
    path = root / "intents_v1" / "snapshots" / DAY / f"{intent_id}.exposure_intent.v1.json"
    _write(path, {"schema_id": "exposure_intent", "intent_id": intent_id, "underlying": {"symbol": symbol}, "exposure_type": "LONG_EQUITY", "score": score, "target_notional_pct": "0.01", "constraints": {"max_risk_pct": "0.01"}})
    return str(path)


def test_regime_bucket_ranking_diagnostic_detects_non_top_selected_without_changing_gate(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    first_path = _intent(truth, "first", "AAA", 0.1)
    better_path = _intent(truth, "better", "BBB", 0.9)
    gate_path = truth / "reports" / "portfolio_activation_gate_v1" / DAY / "portfolio_activation_gate.v1.json"
    rollup_path = truth / "reports" / "sleeve_evaluation_rollup_v1" / DAY / "scan_rollup.v1.json"
    _write(truth / "reports" / "portfolio_state_v1" / DAY / "portfolio_state.v1.json", {"status": "PASS", "regime": "TREND", "trend_strength": "HIGH", "volatility_regime": "NORMAL", "dispersion_regime": "LOW", "equity_beta_state": "LOW"})
    _write(rollup_path, {"outcomes": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "output_intents": [{"intent_id": "first"}, {"intent_id": "better"}]}]})
    _write(gate_path, {"schema_id": "portfolio_activation_gate", "day_utc": DAY, "status": "PASS", "source_rollup_path": str(rollup_path), "decisions": [
        {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "raw_signal_status": "ACTIVE", "raw_intent_id": "first", "raw_intent_symbol": "AAA", "raw_intent_path": first_path, "portfolio_gate_decision": "ALLOW", "allowed_by_portfolio_gate": True, "reason_codes": ["TREND_SPLIT_PRIMARY_ALLOWED"], "regime_bucket": "TREND", "overlap_group": "trend"},
        {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "raw_signal_status": "ACTIVE", "raw_intent_id": "better", "raw_intent_symbol": "BBB", "raw_intent_path": better_path, "portfolio_gate_decision": "SUPPRESS", "allowed_by_portfolio_gate": False, "reason_codes": ["TREND_SPLIT_PRIMARY_ALLOWED", "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"], "regime_bucket": "TREND", "overlap_group": "trend"},
    ]})

    report = build_regime_bucket_candidate_ranking_report_v1(day_utc=DAY, truth_root=truth)

    assert report["selection_behavior_changed"] is False
    assert report["broker_execution_allowed"] is False
    assert report["order_submission_attempted"] is False
    assert report["selected_candidate_id"] == "first"
    assert report["top_ranked_candidate_id"] == "better"
    assert report["selected_candidate_rank"] == 2
    assert report["order_dependency_detected"] is True
    bucket = report["buckets"][0]
    assert [row["candidate_id"] for row in bucket["candidates"]] == ["better", "first"]
    persisted = regime_bucket_candidate_ranking_report_path(truth_root=truth, day_utc=DAY)
    assert persisted.is_file()
