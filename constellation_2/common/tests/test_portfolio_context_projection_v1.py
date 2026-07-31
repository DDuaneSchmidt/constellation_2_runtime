from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trade_lifecycle.portfolio_context_projection_v1 import build_portfolio_context_projection_v1


DAY = "2026-05-22"


def _trade(trade_id: str, sleeve_id: str, symbol: str = "DOW", **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "trade_id": trade_id,
        "position_id": trade_id.replace(":", "_"),
        "symbol": symbol,
        "side": "BUY",
        "quantity": 10,
        "current_mark": 100,
        "entry_price": 98,
        "unrealized_pnl": 20,
        "sleeve_id": sleeve_id,
        "hypothesis_id": f"hyp:{trade_id}",
        "sector": "MATERIALS",
        "setup_type": "MEAN_REVERSION",
        "asset_class": "EQUITY",
        "factor_tags": ["value", "cyclicals"],
        "regime_dependency": ["mean_reversion"],
        "source_artifacts": [f"/tmp/{trade_id.replace(':', '_')}.json"],
    }
    row.update(overrides)
    return row


def _projection(trades: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_id": "paper_trade_evaluation_projection",
        "schema_version": "v1",
        "content_hash": "trade-projection-hash",
        "trade_lifecycle_ledger": {"content_hash": "ledger-hash"},
        "open_trades": trades,
        "all_trades": trades,
    }


def _exit_projection() -> dict[str, object]:
    return {
        "schema_id": "exit_review_projection",
        "schema_version": "v1",
        "content_hash": "exit-review-hash",
        "rows": [],
        "thesis_state_projection_v1": {"schema_id": "thesis_state_projection", "content_hash": "thesis-hash", "rows": []},
    }


def test_dow_appears_in_portfolio_exposure_and_sleeve_rollup(tmp_path: Path) -> None:
    payload = build_portfolio_context_projection_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        paper_trade_evaluation_projection=_projection([_trade("trade:dow:1", "SLEEVE_A")]),
        exit_review_projection=_exit_projection(),
    )

    by_symbol = payload["exposure_summary"]["by_symbol"]
    by_sleeve = payload["exposure_summary"]["by_sleeve"]

    assert by_symbol[0]["symbol"] == "DOW"
    assert by_symbol[0]["open_trade_count"] == 1
    assert by_sleeve[0]["sleeve_id"] == "SLEEVE_A"
    assert by_sleeve[0]["unrealized_pnl_contribution"] == 20


def test_concentration_warning_appears_when_threshold_exceeded(tmp_path: Path) -> None:
    payload = build_portfolio_context_projection_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        paper_trade_evaluation_projection=_projection([
            _trade("trade:dow:1", "SLEEVE_A"),
            _trade("trade:dow:2", "SLEEVE_A", current_mark=101),
        ]),
        exit_review_projection=_exit_projection(),
    )

    warnings = payload["concentration_warnings"]
    assert any(row["warning_type"] == "EXCESSIVE_SYMBOL_EXPOSURE" and row["value"] == "DOW" for row in warnings)
    assert all(row["advisory_only"] is True for row in warnings)


def test_overlapping_sleeve_exposure_is_detected(tmp_path: Path) -> None:
    payload = build_portfolio_context_projection_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        paper_trade_evaluation_projection=_projection([
            _trade("trade:dow:1", "SLEEVE_A"),
            _trade("trade:dow:2", "SLEEVE_B", setup_type="MOMENTUM_TREND", factor_tags=["momentum"], regime_dependency=["trend"]),
        ]),
        exit_review_projection=_exit_projection(),
    )

    overlaps = payload["sleeve_overlap"]
    assert any(row["dimension"] == "symbol" and row["value"] == "DOW" for row in overlaps)
    assert any(row["overlap_type"] == "duplicate-risk_overlap" for row in overlaps)


def test_missing_sector_factor_data_is_partial_not_failure(tmp_path: Path) -> None:
    payload = build_portfolio_context_projection_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        paper_trade_evaluation_projection=_projection([
            _trade("trade:unknown:1", "SLEEVE_A", symbol="XYZ", sector="", factor_tags=[], regime_dependency=[], setup_type=""),
        ]),
        exit_review_projection=_exit_projection(),
    )

    assert payload["confidence"] == "PARTIAL"
    assert payload["exposure_summary"]["by_symbol"][0]["symbol"] == "XYZ"
    assert {row["field"] for row in payload["missing_data"]} >= {"sector", "factor", "regime_dependency"}


def test_replay_deterministic_and_no_broker_or_trade_advice_behavior(tmp_path: Path) -> None:
    trade_projection = _projection([_trade("trade:dow:1", "SLEEVE_A"), _trade("trade:dow:2", "SLEEVE_B")])
    first = build_portfolio_context_projection_v1(truth_root=tmp_path, day_utc=DAY, paper_trade_evaluation_projection=trade_projection, exit_review_projection=_exit_projection())
    second = build_portfolio_context_projection_v1(truth_root=tmp_path, day_utc=DAY, paper_trade_evaluation_projection=trade_projection, exit_review_projection=_exit_projection())

    assert first["content_hash"] == second["content_hash"]
    assert first["broker_submit_transmit_allowed"] is False
    assert first["autonomous_execution_allowed"] is False
    assert first["order_routing_allowed"] is False
    assert first["trade_advice_allowed"] is False
    assert first["sleeve_score_mutation_allowed"] is False
    assert first["candidate_promotion_allowed"] is False
    assert first["position_sizing_allowed"] is False
    assert "exit_decision" not in first
    assert "score" not in first
