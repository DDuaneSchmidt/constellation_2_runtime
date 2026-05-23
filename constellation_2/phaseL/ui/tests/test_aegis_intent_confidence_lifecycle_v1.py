from __future__ import annotations

import json
from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.candidate_intent_plane_v1 import build_and_write_candidate_intent_plane_v1

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
TIMELINE = ROOT / "ops/aegis/operator_state/runtime_timeline_projection_v1.py"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_candidate_day(tmp_path: Path, *, certified: bool = False, blocked: bool = False, selected: bool = True) -> None:
    day = "2026-05-21"
    manifest = {
        "schema_id": "candidate_generation_manifest",
        "day_utc": day,
        "artifact_id": "manifest-1",
        "candidate_lane": "CERTIFIED" if certified else "PROVISIONAL",
        "certification_state": "CERTIFIED" if certified else "CERTIFICATION_PENDING",
        "input_market_data_snapshot_ids": ["md-1"],
        "candidate_rows": [
            {
                "candidate_id": "cand-aapl",
                "raw_intent_id": "intent-aapl",
                "symbol": "AAPL",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "status": "INTENT_CREATED",
                "source_data_mode": "INTRADAY_OPERATIONAL",
            }
        ],
    }
    _write(tmp_path / "reports/candidate_generation_manifest_v1" / day / "run-1/candidate_generation_manifest.v1.json", manifest)
    score_status = "SCORED"
    _write(
        tmp_path / "reports/portfolio_scoring_v1" / day / "portfolio_scoring.v1.json",
        {
            "schema_id": "portfolio_scoring",
            "day_utc": day,
            "rankings": [
                {
                    "intent_id": "intent-aapl",
                    "rank": 1,
                    "score_total": 92.0,
                    "score_status": score_status,
                    "market_data_mode": "INTRADAY_OPERATIONAL",
                    "certification_state": "CERTIFIED" if certified else "CERTIFICATION_PENDING",
                    "final_eod_certification_status": "VALID" if certified else "PENDING",
                    "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
                }
            ],
        },
    )
    selected_intent = {"intent_id": "intent-aapl", "symbol": "AAPL", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1"} if selected else {}
    _write(
        tmp_path / "reports/intent_arbitration_v1" / day / "intent_arbitration.v1.json",
        {
            "schema_id": "intent_arbitration",
            "day_utc": day,
            "status": "SELECTED" if selected else "NO_EXECUTABLE_INTENT",
            "selected_intent": selected_intent,
            "candidate_intents": [selected_intent] if selected else [],
            "rejected_or_filtered_intents": [] if selected else [{"intent_id": "intent-aapl", "rejection_reason": "NOT_SELECTED_BY_PORTFOLIO_SCORE_V1"}],
        },
    )
    _write(
        tmp_path / "reports/candidate_promotion_map_v1" / day / "candidate_promotion_map.v1.json",
        {
            "schema_id": "candidate_promotion_map",
            "day_utc": day,
            "candidate_rows": [
                {
                    "candidate_id": "cand-aapl",
                    "raw_intent_id": "intent-aapl",
                    "rank": 1,
                    "score": 92.0,
                    "selected_status": "SELECTED" if selected else "NOT_SELECTED",
                    "certification_state": "CERTIFIED_CONFIRMED" if certified else "CERTIFICATION_PENDING",
                    "exact_blocker": "CONVERSION_MISSING" if blocked else "",
                }
            ],
        },
    )
    _write(tmp_path / "reports/market_data_inputs_v1" / day / "market_data_inputs.v1.json", {"input_market_data_snapshot_ids": ["md-1"]})


def test_selected_candidate_accumulates_confidence_before_recommendation(tmp_path: Path) -> None:
    _seed_candidate_day(tmp_path)
    payload, _ = build_and_write_candidate_intent_plane_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        now_utc="2026-05-21T20:20:00Z",
        write_histories=True,
    )
    row = payload["intent_snapshots"][0]
    assert row["intent_state"] == "CONFIDENCE_ACCUMULATING"
    assert row["capture_guidance"] == "AWAIT_CERTIFICATION"
    assert row["stability_score"] < 0.70
    assert payload["safety"]["broker_submit_transmit_allowed"] is False


def test_high_confidence_stable_preliminary_intent_can_be_recommended_when_enabled(tmp_path: Path) -> None:
    _seed_candidate_day(tmp_path)
    # First snapshot establishes prior state; second snapshot can satisfy stability.
    build_and_write_candidate_intent_plane_v1(truth_root=tmp_path, day_utc="2026-05-21", now_utc="2026-05-21T20:16:00Z", write_histories=True)
    payload, _ = build_and_write_candidate_intent_plane_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        now_utc="2026-05-21T20:20:00Z",
        policy_overrides={
            "preliminary_capture_enabled": True,
            "confidence_threshold": 0.50,
            "stability_threshold": 0.50,
            "convergence_threshold": 0.50,
            "minimum_snapshot_observations": 2,
        },
        write_histories=True,
    )
    row = payload["intent_snapshots"][0]
    assert row["capture_guidance"] == "MANUAL_IB_CAPTURE_RECOMMENDED"
    assert row["recommendation_mode"] == "PRELIMINARY"
    assert row["execution_eligibility_state"] == "BROKER_AUTOMATION_DISABLED"


def test_preliminary_recommendation_respects_earliest_time(tmp_path: Path) -> None:
    _seed_candidate_day(tmp_path)
    build_and_write_candidate_intent_plane_v1(truth_root=tmp_path, day_utc="2026-05-21", now_utc="2026-05-21T19:00:00Z", write_histories=True)
    payload, _ = build_and_write_candidate_intent_plane_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        now_utc="2026-05-21T19:30:00Z",
        policy_overrides={
            "preliminary_capture_enabled": True,
            "confidence_threshold": 0.50,
            "stability_threshold": 0.50,
            "convergence_threshold": 0.50,
            "minimum_snapshot_observations": 2,
        },
        write_histories=True,
    )
    row = payload["intent_snapshots"][0]
    assert row["capture_guidance"] == "AWAIT_CERTIFICATION"
    assert row["recommendation_decision"] == "TOO_EARLY_FOR_PRELIMINARY"


def test_final_certified_intent_becomes_final_recommendation(tmp_path: Path) -> None:
    _seed_candidate_day(tmp_path, certified=True)
    build_and_write_candidate_intent_plane_v1(truth_root=tmp_path, day_utc="2026-05-21", now_utc="2026-05-21T21:00:00Z", write_histories=True)
    payload, _ = build_and_write_candidate_intent_plane_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        now_utc="2026-05-21T21:10:00Z",
        policy_overrides={"confidence_threshold": 0.50, "stability_threshold": 0.50, "convergence_threshold": 0.50, "minimum_snapshot_observations": 2},
        write_histories=True,
    )
    row = payload["intent_snapshots"][0]
    assert row["capture_guidance"] == "MANUAL_IB_CAPTURE_RECOMMENDED"
    assert row["recommendation_mode"] == "FINAL"


def test_blocked_or_suppressed_intent_never_recommends_capture(tmp_path: Path) -> None:
    _seed_candidate_day(tmp_path, certified=True, blocked=True)
    payload, _ = build_and_write_candidate_intent_plane_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        now_utc="2026-05-21T21:10:00Z",
        policy_overrides={"preliminary_capture_enabled": True, "confidence_threshold": 0.0, "stability_threshold": 0.0, "convergence_threshold": 0.0, "minimum_snapshot_observations": 1},
        write_histories=True,
    )
    row = payload["intent_snapshots"][0]
    assert row["capture_guidance"] == "SYSTEM_REPAIR_REQUIRED"
    assert row["recommendation_decision"] == "BLOCKER_ACTIVE"


def test_ui_and_runtime_timeline_use_intent_lifecycle_language() -> None:
    pages = pages_source_v1(ROOT)
    timeline = TIMELINE.read_text(encoding="utf-8")
    candidate_block = pages.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderAegisReviewWorkflow", 1)[0]
    attention_block = pages.split("function renderDashboardAttentionRequired", 1)[1].split("function candidatePipelineObservability", 1)[0]
    assert "Intent Pipeline" in candidate_block
    assert "Confidence" in candidate_block
    assert "Stability" in candidate_block
    assert "Convergence" in candidate_block
    assert "Capture guidance" in candidate_block
    assert "IB capture tickets: 0" in attention_block
    assert "No action required" in attention_block
    assert "Manual IB capture recommended" in attention_block
    assert "preliminary_recommendation_window" in timeline
    assert "final_recommendation_window" in timeline
