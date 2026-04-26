from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from constellation_2.research_lab.ai_hypothesis_batch_v1 import (
    edge_hypothesis_rejected_path,
    edge_hypothesis_validated_path,
    evaluate_ai_hypothesis_batch_v1,
    intake_ai_hypothesis_batch_v1,
    runtime_tests_queued_root,
)
from constellation_2.research_lab.research_event_bus_v1 import resolve_runtime_root


@pytest.fixture()
def runtime_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = (tmp_path / "research_lab_runtime").resolve()
    monkeypatch.setenv("CONSTELLATION_RESEARCH_LAB_RUNTIME_ROOT", str(root))
    return root


def _valid_batch() -> dict:
    return {
        "schema_version": "ai_hypothesis_batch.v1",
        "source_packet_path": "/home/node/constellation_runtime_data/research_lab/reviews/ai_edge_reviews/2026-04-24/research_ai_packet.v1.json",
        "generated_utc": "2026-04-26T00:00:00Z",
        "hypotheses": [
            {
                "idea_id": "EDGE-2026-0001",
                "hypothesis": "Mean-reversion edge appears after volatility regime spikes in index constituents.",
                "expected_edge_mechanism": "Behavioral overshoot and liquidity mean reversion.",
                "market": "US_EQUITIES",
                "edge_type": "MEAN_REVERSION",
                "instruments": ["SPY", "QQQ"],
                "features_required": ["returns_1d", "vix_level", "spread_proxy"],
                "data_required": ["ohlcv_daily", "bid_ask_spread_proxy", "fees"],
                "test_design": {
                    "test_type": "WALK_FORWARD",
                    "in_sample_period": "2018-01-01/2022-12-31",
                    "out_of_sample_period": "2023-01-01/2025-12-31",
                    "cost_model_required": True,
                    "slippage_model_required": True,
                    "walk_forward_required": True,
                },
                "success_criteria": {
                    "min_trade_count": 100,
                    "min_expectancy_after_costs": 0.02,
                    "max_drawdown_limit": 0.15,
                    "min_generalization_ratio": 0.6,
                },
                "rejection_criteria": ["expectancy_after_costs_below_0", "generalization_ratio_below_0_5"],
                "risk_notes": ["must remain diversified", "do not use leverage"],
                "forbidden_if": ["requires MNPI", "requires unavailable data"],
            }
        ],
    }


def test_valid_batch_accepted(runtime_root: Path) -> None:
    result = evaluate_ai_hypothesis_batch_v1(_valid_batch())
    assert len(result["accepted"]) == 1
    assert len(result["rejected"]) == 0


def test_missing_rejection_criteria_rejected(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["rejection_criteria"] = []
    result = evaluate_ai_hypothesis_batch_v1(payload)
    assert len(result["rejected"]) == 1
    assert "REJECTION_CRITERIA_MISSING" in result["rejected"][0]["reasons"]


def test_missing_success_criteria_rejected(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["success_criteria"] = {}
    result = evaluate_ai_hypothesis_batch_v1(payload)
    assert len(result["rejected"]) == 1
    assert "SUCCESS_CRITERIA_MISSING" in result["rejected"][0]["reasons"]


def test_no_out_of_sample_rejected(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["test_design"]["out_of_sample_period"] = ""
    result = evaluate_ai_hypothesis_batch_v1(payload)
    assert len(result["rejected"]) == 1
    assert "OUT_OF_SAMPLE_PERIOD_MISSING" in result["rejected"][0]["reasons"]


def test_cost_slippage_disabled_rejected(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["test_design"]["cost_model_required"] = False
    payload["hypotheses"][0]["test_design"]["slippage_model_required"] = False
    result = evaluate_ai_hypothesis_batch_v1(payload)
    assert len(result["rejected"]) == 1
    reasons = set(result["rejected"][0]["reasons"])
    assert "COST_MODEL_NOT_REQUIRED" in reasons
    assert "SLIPPAGE_MODEL_NOT_REQUIRED" in reasons


def test_direct_trade_instruction_rejected(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["hypothesis"] = "Buy SPY at market open and sell at close."
    result = evaluate_ai_hypothesis_batch_v1(payload)
    assert len(result["rejected"]) == 1
    assert "DIRECT_TRADE_INSTRUCTION_FORBIDDEN" in result["rejected"][0]["reasons"]


def test_risk_gate_override_rejected(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["risk_notes"] = ["Override gate and change risk limits to increase returns"]
    result = evaluate_ai_hypothesis_batch_v1(payload)
    assert len(result["rejected"]) == 1
    assert "GATE_RISK_CORE_OVERRIDE_FORBIDDEN" in result["rejected"][0]["reasons"]


def test_valid_hypothesis_converted_to_edge_hypothesis(runtime_root: Path) -> None:
    intake = intake_ai_hypothesis_batch_v1(_valid_batch(), queue_tests=False)
    assert intake["accepted_count"] == 1
    idea_path = edge_hypothesis_validated_path("EDGE-2026-0001")
    assert idea_path.exists()
    payload = json.loads(idea_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "edge_hypothesis.v1"
    assert payload["status"] == "VALIDATED"


def test_rejected_idea_retained_with_reason(runtime_root: Path) -> None:
    payload = _valid_batch()
    payload["hypotheses"][0]["hypothesis"] = "Buy SPY now and place order immediately."
    intake = intake_ai_hypothesis_batch_v1(payload, queue_tests=False)
    assert intake["rejected_count"] == 1
    rejected_path = edge_hypothesis_rejected_path("EDGE-2026-0001")
    assert rejected_path.exists()
    rejected_payload = json.loads(rejected_path.read_text(encoding="utf-8"))
    reasons = rejected_payload["risk_notes"]["rejected_reasons"]
    assert "DIRECT_TRADE_INSTRUCTION_FORBIDDEN" in reasons


def test_queue_tool_creates_sandbox_test_plan(runtime_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    intake_ai_hypothesis_batch_v1(_valid_batch(), queue_tests=False)

    from ops.tools import run_research_test_queue_v1 as queue_tool

    monkeypatch.setattr(sys, "argv", ["run_research_test_queue_v1.py", "--all_validated_without_test"])
    rc = queue_tool.main()
    assert rc == 0

    queued_dir = runtime_tests_queued_root()
    queued = list(queued_dir.glob("*.sandbox_test_plan.v1.json"))
    assert queued


def test_no_repo_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_runtime = Path(__file__).resolve().parents[3] / "runtime" / "research_lab_bad_root"
    monkeypatch.setenv("CONSTELLATION_RESEARCH_LAB_RUNTIME_ROOT", str(repo_runtime))
    with pytest.raises(ValueError):
        resolve_runtime_root()
