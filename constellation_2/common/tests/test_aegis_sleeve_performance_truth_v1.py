from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.sleeve_performance_truth_v1 import (  # noqa: E402
    build_sleeve_performance_truth_v1,
    write_sleeve_performance_truth_v1,
)

DAY = "2026-05-27"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, *, certified_marks: bool = False) -> None:
    _write(
        root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "open_positions": [
                {
                    "position_id": "pos-open",
                    "candidate_id": "cand-open",
                    "sleeve_id": "SLEEVE_A",
                    "symbol": "AAA",
                    "current_status": "OPEN",
                    "entry_price": "10",
                    "quantity": "10",
                    "entry_time": f"{DAY}T14:00:00Z",
                    "unrealized_pnl": "15",
                }
            ],
            "closed_positions": [
                {
                    "position_id": "pos-win",
                    "candidate_id": "cand-win",
                    "sleeve_id": "SLEEVE_A",
                    "symbol": "AAA",
                    "current_status": "CLOSED",
                    "entry_price": "10",
                    "exit_price": "12",
                    "quantity": "10",
                    "entry_time": f"{DAY}T14:00:00Z",
                    "exit_time": f"{DAY}T20:00:00Z",
                    "realized_pnl": "20",
                    "exit_source_receipt": {"exit_reason_selected_by_operator": "EXIT_STOP_LOSS"},
                },
                {
                    "position_id": "pos-loss",
                    "candidate_id": "cand-loss",
                    "sleeve_id": "SLEEVE_A",
                    "symbol": "BBB",
                    "current_status": "CLOSED",
                    "entry_price": "20",
                    "exit_price": "19",
                    "quantity": "5",
                    "entry_time": f"{DAY}T15:00:00Z",
                    "exit_time": f"{DAY}T21:00:00Z",
                    "realized_pnl": "-5",
                    "exit_source_receipt": {"exit_reason_selected_by_operator": "MANUAL_EXIT"},
                },
            ],
            "legacy_captures": [
                {
                    "position_id": "legacy-1",
                    "candidate_id": "",
                    "sleeve_id": "SLEEVE_A",
                    "current_status": "CLOSED",
                    "legacy_classification": "LEGACY_CAPTURE",
                    "realized_pnl": "999",
                }
            ],
        },
    )
    events = root / "reports" / "aegis_paper_position_events_v1" / DAY / "paper_position_events.v1.jsonl"
    events.parent.mkdir(parents=True, exist_ok=True)
    events.write_text('{"event_type":"PAPER_POSITION_OPENED"}\n', encoding="utf-8")
    _write(
        root / "reports" / "aegis_exit_recommendations_v1" / DAY / "exit_recommendations.v1.json",
        {
            "schema_id": "aegis_exit_recommendations",
            "day_utc": DAY,
            "recommendations": [
                {"candidate_id": "cand-win", "sleeve_id": "SLEEVE_A", "exit_recommendation": "EXIT_STOP_LOSS"},
                {"candidate_id": "cand-loss", "sleeve_id": "SLEEVE_A", "exit_recommendation": "EXIT_TAKE_PROFIT"},
                {"candidate_id": "cand-open", "sleeve_id": "SLEEVE_A", "exit_recommendation": "HOLD"},
            ],
        },
    )
    _write(
        root / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json",
        {
            "schema_id": "aegis_candidate_review_packet",
            "day_utc": DAY,
            "review_candidates": [
                {"candidate_id": "cand-win", "sleeve_id": "SLEEVE_A", "operator_decision": "APPROVED"},
                {"candidate_id": "cand-loss", "sleeve_id": "SLEEVE_A", "operator_decision": "REJECTED"},
                {"candidate_id": "cand-open", "sleeve_id": "SLEEVE_A", "operator_decision": "APPROVED"},
                {"candidate_id": "cand-other", "sleeve_id": "SLEEVE_B", "operator_decision": "REJECTED"},
            ],
        },
    )
    _write(
        root / "reports" / "sleeve_scorecard_daily_v1" / DAY / "sleeve_scorecard_daily.v1.json",
        {"status": "PASS", "sleeves": [{"sleeve_id": "SLEEVE_A", "rank": 2, "score": 71}]},
    )
    if certified_marks:
        _write(
            root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
            {"status": "CERTIFIED", "validation_status": "FINAL_EOD_CERTIFIED", "normalized_records": [{"symbol": "AAA", "finalization_status": "FINAL_EOD_CERTIFIED"}]},
        )
    else:
        _write(
            root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
            {"status": "CURRENT", "validation_status": "VALIDATED_CURRENT_DAY", "normalized_records": [{"symbol": "AAA", "finalization_status": "NOT_FINAL_YET"}]},
        )


def test_computes_realized_pnl_from_closed_paper_positions_and_excludes_open(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    row = {item["sleeve_id"]: item for item in payload["sleeves"]}["SLEEVE_A"]

    assert row["open_paper_position_count"] == 1
    assert row["closed_paper_position_count"] == 2
    assert row["realized_pnl"] == "15"
    assert row["paper_trade_count"] == 3
    assert payload["totals"]["realized_pnl"] == "15"


def test_marks_unrealized_not_canonical_without_certified_marks_and_available_with_certified_marks(tmp_path: Path) -> None:
    _seed(tmp_path, certified_marks=False)
    row = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)["sleeves"][0]
    assert row["unrealized_pnl_status"] == "NOT_CANONICAL"
    assert row["unrealized_pnl"] == ""

    certified_root = tmp_path / "certified"
    _seed(certified_root, certified_marks=True)
    row = build_sleeve_performance_truth_v1(truth_root=certified_root, day_utc=DAY)["sleeves"][0]
    assert row["unrealized_pnl_status"] == "AVAILABLE"
    assert row["unrealized_pnl"] == "15"


def test_computes_win_loss_hold_time_exit_reasons_and_recommendation_followed(tmp_path: Path) -> None:
    _seed(tmp_path)
    row = {item["sleeve_id"]: item for item in build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)["sleeves"]}["SLEEVE_A"]

    assert row["win_count"] == 1
    assert row["loss_count"] == 1
    assert row["win_rate"] == 0.5
    assert row["average_hold_time"] == 0.25
    assert row["exit_reason_counts"] == {"EXIT_STOP_LOSS": 1, "MANUAL_EXIT": 1}
    assert row["recommendation_followed_count"] == 1
    assert row["recommendation_followed_rate"] == 0.5


def test_separates_legacy_captures_and_preserves_policy_flags(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    row = {item["sleeve_id"]: item for item in payload["sleeves"]}["SLEEVE_A"]

    assert row["legacy_capture_count_excluded"] == 1
    assert row["realized_pnl"] == "15"
    assert payload["legacy_captures"]["excluded_from_canonical_performance"] is True
    assert payload["legacy_captures"]["legacy_capture_count"] == 1
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False
    assert payload["runtime_policy_changed"] is False
    assert row["scorecard_ref"]["source"] == "sleeve_scorecard_daily_v1"


def test_writes_requested_artifact_path(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = write_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    assert path == tmp_path / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_id"] == "aegis_sleeve_performance_truth"
