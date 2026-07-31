from __future__ import annotations

import json
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.research_lab.research_validation_engine_v1 import (
    build_research_hypothesis_registry_v1,
    build_research_promotion_gate_v1,
    build_research_validation_protocol_v1,
    build_research_validation_result_v1,
    build_research_validation_run_v1,
    build_research_outcome_feedback_v1,
    research_validation_engine_self_check_v1,
    write_research_validation_engine_v1,
)

DAY = "2026-05-30"
HYPOTHESIS = "rh-process-test-etf-drop-mean-reversion-v1"


def _doctor(root: Path) -> None:
    path = root / "reports" / "aegis_research_doctor_v1" / DAY / "research_doctor.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "hypotheses": [
            {
                "hypothesis_id": HYPOTHESIS,
                "title": "ETF mean-reversion after sharp 1-day drop",
                "current_autonomous_state": "Queued",
                "created_by": "SYSTEM",
                "last_attempted_run": "2026-05-30T14:00:00Z",
                "symbols": ["SPY"],
            }
        ]
    }), encoding="utf-8")


def _result(root: Path, event_day: str = "2026-05-29", close: float = 100.0, forward_close: float = 103.0) -> None:
    result = root / "reports" / "aegis_research_test_results_v1" / event_day / HYPOTHESIS / "research_test_result.v1.json"
    result.parent.mkdir(parents=True, exist_ok=True)
    result.write_text(json.dumps({
        "minimum_sample_size": 20,
        "event_data_availability": {
            "event_rows": [{"symbol": "SPY", "close": close, "daily_return": -0.02, "vix_filter_pass": True}],
            "observed_rows": [],
        },
    }), encoding="utf-8")
    market = root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json"
    market.parent.mkdir(parents=True, exist_ok=True)
    market.write_text(json.dumps({"normalized_records": [{"symbol": "SPY", "close": forward_close}]}), encoding="utf-8")


def test_hypotheses_and_protocols_are_versioned(tmp_path: Path) -> None:
    _doctor(tmp_path)
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)

    assert registry["hypotheses"][0]["hypothesis_version"] == "v1"
    assert {row["protocol_id"] for row in protocols["protocols"]} == {
        "EVENT_WINDOW_RETURN_V1",
        "MEAN_REVERSION_FORWARD_RETURN_V1",
        "POST_SIGNAL_SURVIVAL_V1",
    }
    assert all(row["protocol_version"] == "v1" for row in protocols["protocols"])


def test_validation_run_references_exact_hypothesis_and_protocol_versions(tmp_path: Path) -> None:
    _doctor(tmp_path)
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)
    runs = build_research_validation_run_v1(truth_root=tmp_path, day_utc=DAY, registry_payload=registry, protocol_payload=protocols)
    run = runs["runs"][0]

    assert run["hypothesis_id"] == HYPOTHESIS
    assert run["hypothesis_version"] == registry["hypotheses"][0]["hypothesis_version"]
    assert run["protocol_id"] == "MEAN_REVERSION_FORWARD_RETURN_V1"
    assert run["protocol_version"] == "v1"
    assert run["source_artifacts"]


def test_under_sampled_inconclusive_and_disproven_cannot_promote(tmp_path: Path) -> None:
    payload = {
        "results": [
            {"result_id": "r1", "hypothesis_id": "h1", "hypothesis_version": "v1", "validation_status": "UNDER_SAMPLED", "source_artifacts": ["a"]},
            {"result_id": "r2", "hypothesis_id": "h2", "hypothesis_version": "v1", "validation_status": "INCONCLUSIVE", "source_artifacts": ["a"]},
            {"result_id": "r3", "hypothesis_id": "h3", "hypothesis_version": "v1", "validation_status": "DISPROVEN", "source_artifacts": ["a"]},
            {"result_id": "r4", "hypothesis_id": "h4", "hypothesis_version": "v1", "validation_status": "NOT_READY", "source_artifacts": ["a"]},
        ]
    }
    gate = build_research_promotion_gate_v1(day_utc=DAY, result_payload=payload)

    assert all(row["eligible_for_candidate_review"] is False for row in gate["promotion_gates"])
    assert all(row["tradeable"] is False for row in gate["promotion_gates"])
    assert all(row["trade_advice_allowed"] is False for row in gate["promotion_gates"])


def test_supported_can_only_become_candidate_review_eligible_not_tradeable() -> None:
    gate = build_research_promotion_gate_v1(day_utc=DAY, result_payload={
        "results": [{"result_id": "r1", "hypothesis_id": "h1", "hypothesis_version": "v1", "validation_status": "SUPPORTED", "source_artifacts": ["a"]}]
    })
    row = gate["promotion_gates"][0]

    assert row["eligible_for_candidate_review"] is True
    assert row["candidate_review_only"] is True
    assert row["tradeable"] is False
    assert row["trade_advice_allowed"] is False


def test_ai_summary_cannot_override_deterministic_under_sampled_status(tmp_path: Path) -> None:
    _doctor(tmp_path)
    _result(tmp_path)
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)
    runs = build_research_validation_run_v1(truth_root=tmp_path, day_utc=DAY, registry_payload=registry, protocol_payload=protocols)
    results = build_research_validation_result_v1(truth_root=tmp_path, day_utc=DAY, run_payload=runs, protocol_payload=protocols)
    row = results["results"][0]

    assert row["sample_count"] == 1
    assert row["validation_status"] != "SUPPORTED"
    assert row["ai_summary_can_override_status"] is False
    assert row["source_artifacts"]


def test_outcome_feedback_does_not_rewrite_original_validation_result(tmp_path: Path) -> None:
    gate = build_research_promotion_gate_v1(day_utc=DAY, result_payload={
        "results": [{"result_id": "r1", "hypothesis_id": "h1", "hypothesis_version": "v1", "validation_status": "SUPPORTED", "source_artifacts": ["a"]}]
    })
    feedback = build_research_outcome_feedback_v1(truth_root=tmp_path, day_utc=DAY, gate_payload=gate)

    assert feedback["summary"]["historical_validation_rewritten"] is False
    assert feedback["outcome_feedback"] == []


def test_self_check_enforces_safety_and_promotion_rules(tmp_path: Path) -> None:
    _doctor(tmp_path)
    _result(tmp_path)
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)
    runs = build_research_validation_run_v1(truth_root=tmp_path, day_utc=DAY, registry_payload=registry, protocol_payload=protocols)
    results = build_research_validation_result_v1(truth_root=tmp_path, day_utc=DAY, run_payload=runs, protocol_payload=protocols)
    gate = build_research_promotion_gate_v1(day_utc=DAY, result_payload=results)
    feedback = build_research_outcome_feedback_v1(truth_root=tmp_path, day_utc=DAY, gate_payload=gate)
    write_research_validation_engine_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        registry_payload=registry,
        protocol_payload=protocols,
        run_payload=runs,
        result_payload=results,
        promotion_gate_payload=gate,
        outcome_feedback_payload=feedback,
    )

    report = research_validation_engine_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert report["ok"] is True
    assert report["summary"]["eligible_for_candidate_review"] == 0
    assert all(check["ok"] for check in report["checks"])


def _write_sample_artifact(root: Path, *, hypothesis_id: str, sample_count: int = 20, forward_return: float = 0.01) -> None:
    path = root / "reports" / "aegis_research_validation_samples_v1" / DAY / "research_validation_samples.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    samples = []
    for idx in range(sample_count):
        day_num = idx + 1
        samples.append({
            "sample_id": f"s{idx}",
            "hypothesis_id": hypothesis_id,
            "symbol": "SPY",
            "event_day": f"2026-04-{day_num:02d}",
            "forward_close_day": f"2026-04-{day_num + 1:02d}",
            "sample_timestamp": f"2026-04-{day_num + 1:02d}T20:00:00Z",
            "event_close": 100.0,
            "forward_close": 101.0,
            "trigger_return": -0.02,
            "forward_return_1d": forward_return,
            "sample_status": "VALID",
        })
    path.write_text(json.dumps({
        "schema_id": "aegis_research_validation_samples",
        "schema_version": "v1",
        "day_utc": DAY,
        "hypothesis_id": hypothesis_id,
        "required_samples": 20,
        "current_samples": sample_count,
        "missing_samples": max(20 - sample_count, 0),
        "excluded_samples": [],
        "samples": samples,
    }), encoding="utf-8")


def test_supported_result_requires_protocol_metrics_binding_bias_and_sample_policy(tmp_path: Path) -> None:
    _doctor(tmp_path)
    _write_sample_artifact(tmp_path, hypothesis_id=HYPOTHESIS, sample_count=20, forward_return=0.01)
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)
    runs = build_research_validation_run_v1(truth_root=tmp_path, day_utc=DAY, registry_payload=registry, protocol_payload=protocols)
    results = build_research_validation_result_v1(truth_root=tmp_path, day_utc=DAY, run_payload=runs, protocol_payload=protocols)
    row = results["results"][0]

    assert row["validation_status"] == "SUPPORTED"
    assert row["required_metric_status"]["status"] == "PASS"
    assert row["artifact_binding_status"] == "PASS"
    assert row["bias_control_status"] == "PASS"
    assert row["sample_size_status"] == "PASS"
    assert row["code_commit"]
    assert row["protocol_hash"]
    assert row["evaluator_hash"]
    assert row["input_context_hash"]


def test_wrong_hypothesis_sample_artifact_blocks_support(tmp_path: Path) -> None:
    _doctor(tmp_path)
    _write_sample_artifact(tmp_path, hypothesis_id="different-hypothesis", sample_count=20, forward_return=0.01)
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)
    runs = build_research_validation_run_v1(truth_root=tmp_path, day_utc=DAY, registry_payload=registry, protocol_payload=protocols)
    results = build_research_validation_result_v1(truth_root=tmp_path, day_utc=DAY, run_payload=runs, protocol_payload=protocols)
    row = results["results"][0]

    assert row["validation_status"] != "SUPPORTED"
    assert row["artifact_binding_status"] == "FAIL"
    assert "HYPOTHESIS_SAMPLE_BINDING_MISMATCH" in row["artifact_binding_failures"]


def test_event_window_cannot_support_without_required_benchmark_metric(tmp_path: Path) -> None:
    path = tmp_path / "reports" / "aegis_research_doctor_v1" / DAY / "research_doctor.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"hypotheses": [{"hypothesis_id": "rh-event-v1", "title": "Earnings event effect", "symbols": ["NVDA"]}]}), encoding="utf-8")
    registry = build_research_hypothesis_registry_v1(truth_root=tmp_path, day_utc=DAY)
    protocols = build_research_validation_protocol_v1(day_utc=DAY)
    runs = build_research_validation_run_v1(truth_root=tmp_path, day_utc=DAY, registry_payload=registry, protocol_payload=protocols)
    results = build_research_validation_result_v1(truth_root=tmp_path, day_utc=DAY, run_payload=runs, protocol_payload=protocols)
    row = results["results"][0]

    assert row["protocol_id"] == "EVENT_WINDOW_RETURN_V1"
    assert row["validation_status"] != "SUPPORTED"
    assert "benchmark_relative_return" in row["required_metric_status"]["missing_required_metrics"]
