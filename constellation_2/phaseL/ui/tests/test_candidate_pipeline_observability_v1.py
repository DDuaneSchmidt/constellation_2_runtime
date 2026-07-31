from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _manifest_path(root: Path, day: str) -> Path:
    return root / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"


def _write_manifest(root: Path, day: str, rows: list[dict], *, certification_state: str = "CERTIFICATION_PENDING", input_ids: list[str] | None = None) -> None:
    input_ids = [f"md-{day}"] if input_ids is None else input_ids
    normalized = []
    for idx, row in enumerate(rows):
        status = row.get("status", "CANDIDATE_CREATED")
        normalized.append(
            {
                "candidate_id": row.get("candidate_id", f"candidate-{day}-{idx}"),
                "engine_id": row.get("engine_id", "C2_TREND_EQ_PRIMARY_V1"),
                "symbol_or_pair": row.get("symbol_or_pair", "SPY"),
                "status": status,
                "reason_codes": row.get("reason_codes", []),
                "rejection_reason": row.get("rejection_reason", ""),
                "raw_intent_id": row.get("raw_intent_id", f"intent-{day}-{idx}" if status == "CANDIDATE_CREATED" else ""),
                "portfolio_gate_decision": row.get("portfolio_gate_decision", "ALLOW" if status == "CANDIDATE_CREATED" else "SUPPRESS"),
                "allowed_by_portfolio_gate": row.get("allowed_by_portfolio_gate", status == "CANDIDATE_CREATED"),
                "candidate_lane": row.get("candidate_lane", "PROVISIONAL"),
                "certification_state": row.get("certification_state", certification_state),
                "certification_label": row.get("certification_label", "NON_CERTIFIED"),
                "execution_eligible": row.get("execution_eligible", False),
                "manual_capture_eligible": row.get("manual_capture_eligible", False),
                "final_eod_certification_status": row.get("final_eod_certification_status", "PENDING"),
                "input_market_data_snapshot_ids": row.get("input_market_data_snapshot_ids", input_ids),
            }
        )
    status_counts: dict[str, int] = {}
    for row in normalized:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    _write_json(
        _manifest_path(root, day),
        {
            "schema_id": "candidate_generation_manifest",
            "schema_version": "v1",
            "run_id": f"sleeve_evaluation_kernel_v1:{day}",
            "day_utc": day,
            "candidate_lane": "PROVISIONAL",
            "certification_state": certification_state,
            "input_market_data_snapshot_ids": input_ids,
            "candidate_rows": normalized,
            "summary": {"candidate_count": len(normalized), "status_counts": status_counts},
            "produced_at_utc": f"{day}T21:00:00Z",
        },
    )


def _observability(root: Path, day: str = "2026-05-21") -> dict:
    payload = resolve_current_operator_truth_v1(truth_root=root, day_utc=day, generated_at_utc=f"{day}T22:00:00Z")
    return payload["current_day_status"]["candidate_pipeline_observability"]


def test_healthy_low_opportunity_regime_does_not_alert_for_no_capture_ready(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    for day in ["2026-05-17", "2026-05-18", "2026-05-19", "2026-05-20", "2026-05-21"]:
        _write_manifest(
            root,
            day,
            [
                {"engine_id": "C2_MEAN_REVERSION_EQ_V1", "status": "NO_SIGNAL", "reason_codes": ["NO_RAW_SIGNAL"], "raw_intent_id": ""},
                {"engine_id": "C2_EVENT_DISLOCATION_V1", "status": "NO_SIGNAL", "reason_codes": ["NO_EVENT_PACKET"], "raw_intent_id": ""},
            ],
        )

    obs = _observability(root)

    assert obs["metrics"]["manual_ib_capture_ready_count"] == 0
    assert obs["metrics"]["qualified_count"] == 0
    assert obs["regime_activity_level"] == "LOW_OPPORTUNITY_HEALTHY"
    assert obs["alerts"] == []


def test_broken_sleeve_zero_rows_alerts_when_invocation_expected_candidates(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    day = "2026-05-21"
    _write_manifest(root, day, [])
    _write_json(
        root / "reports" / "sleeve_invocation_ledger_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "sleeve_invocation_ledger.v1.json",
        {
            "schema_id": "sleeve_invocation_ledger",
            "day_utc": day,
            "invocations": [{"engine_id": "C2_TREND_EQ_PRIMARY_V1", "status": "INTENT_CREATED", "symbol_or_pair": "SPY"}],
        },
    )

    obs = _observability(root, day)

    assert any(alert["code"] == "SLEEVE_ZERO_CANDIDATES_UNEXPECTED" for alert in obs["alerts"])


def test_governance_rule_suppression_spike_is_visible(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    for day in ["2026-05-16", "2026-05-17", "2026-05-18", "2026-05-19", "2026-05-20"]:
        _write_manifest(root, day, [{"status": "CANDIDATE_CREATED", "raw_intent_id": f"intent-{day}-{idx}"} for idx in range(3)])
    _write_manifest(
        root,
        "2026-05-21",
        [
            {
                "status": "SUPPRESSED",
                "raw_intent_id": "",
                "portfolio_gate_decision": "SUPPRESS",
                "allowed_by_portfolio_gate": False,
                "rejection_reason": "MEAN_REVERSION_SUPPRESSED_BY_STRONG_TREND",
                "reason_codes": ["MEAN_REVERSION_SUPPRESSED_BY_STRONG_TREND"],
            }
            for _ in range(5)
        ],
    )

    obs = _observability(root)

    assert obs["metrics"]["suppressed_rate"] == 1.0
    assert any(alert["code"] == "SUPPRESSION_SPIKE" for alert in obs["alerts"])
    assert obs["suppression_diagnostics"]["top_suppression_reasons"][0]["reason"] == "MEAN_REVERSION_SUPPRESSED_BY_STRONG_TREND"


def test_scoring_bottleneck_visibility_reports_unavailable_reasons(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    day = "2026-05-21"
    _write_manifest(root, day, [{"status": "CANDIDATE_CREATED", "raw_intent_id": "intent-score-1"}])
    _write_json(
        root / "reports" / "portfolio_scoring_v1" / day / "portfolio_scoring.v1.json",
        {
            "schema_id": "portfolio_scoring",
            "day_utc": day,
            "scoring_policy_id": "portfolio_scoring_v1",
            "scoring_policy_version": "test",
            "intents_scored_count": 0,
            "rankings": [
                {
                    "intent_id": "intent-score-1",
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "score_total": 0.0,
                    "score_unavailable_reason": "NON_CERTIFIED_CANDIDATE_SNAPSHOT",
                }
            ],
        },
    )

    obs = _observability(root, day)
    cutoffs = obs["suppression_diagnostics"]["scoring_cutoffs"]

    assert cutoffs["policy_id"] == "portfolio_scoring_v1"
    assert cutoffs["score_unavailable_reasons"]["NON_CERTIFIED_CANDIDATE_SNAPSHOT"] == 1


def test_certification_bottleneck_visibility_distinguishes_pending_from_failure(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    day = "2026-05-21"
    _write_manifest(root, day, [{"status": "CANDIDATE_CREATED", "raw_intent_id": "intent-cert-1"}], certification_state="CERTIFICATION_PENDING")

    pending = _observability(root, day)
    assert pending["metrics"]["qualified_count"] == 1
    assert pending["metrics"]["certified_count"] == 0
    assert not any(alert["code"] == "CERTIFICATION_FAILURE" for alert in pending["alerts"])

    _write_manifest(
        root,
        day,
        [{"status": "CANDIDATE_CREATED", "raw_intent_id": "intent-cert-1", "certification_state": "FAILED", "final_eod_certification_status": "FAILED"}],
        certification_state="FAILED",
    )
    failed = _observability(root, day)
    assert any(alert["code"] == "CERTIFICATION_FAILURE" for alert in failed["alerts"])


def test_candidate_funnel_ui_projection_shows_crwd_dynamic_certification_before_after(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    day = "2026-05-22"
    prior_day = "2026-05-21"
    final_path = root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json"
    _write_json(final_path, {"schema_id": "final_eod_market_data_v1", "day_utc": day, "final_eod_symbols": ["SPY", "QQQ", "CRWD"]})
    _write_json(
        root / "reports" / "canonical_universe_authority_v1" / day / "canonical_universe_authority.v1.json",
        {
            "schema_id": "canonical_universe_authority",
            "day_utc": day,
            "source_day": day,
            "authority_status": "PASS",
            "canonical_universe_authority_id": "cua-crwd-test",
            "universe_symbol_count": 5,
            "universe_symbols": ["SPY", "QQQ", "CRWD", "WIDE", "DIA"],
            "writer_process": "ranked_symbol_universe_v1",
            "generation_pipeline": "ranked_symbol_universe_v1",
            "immutable_hash": "abc123",
            "generated_at": f"{day}T21:00:00Z",
        },
    )
    _write_json(
        root / "reports" / "candidate_consumption_audit_v1" / prior_day / "before" / "candidate_consumption_audit.v1.json",
        {
            "schema_id": "candidate_consumption_audit",
            "trading_date": prior_day,
            "raw_candidate_count": 319,
            "promoted_candidate_count": 0,
            "excluded_candidate_count": 319,
            "consumption_counts": {"EXCLUDED_UNCOVERED_SYMBOL": 302, "EXCLUDED_LOW_SCORE": 16, "EXCLUDED_POLICY": 1},
            "certified_universe_symbol_count": 2,
            "certified_universe_symbols": ["SPY", "QQQ"],
            "candidate_rows": [{"candidate_id": "crwd-before", "symbol": "CRWD", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "source_status": "CANDIDATE_CREATED", "consumption_category": "EXCLUDED_UNCOVERED_SYMBOL", "consumption_reason": "CRWD is outside certified universe", "covered_by_certified_eod": False}],
        },
    )
    _write_json(
        root / "reports" / "candidate_consumption_audit_v1" / day / "after" / "candidate_consumption_audit.v1.json",
        {
            "schema_id": "candidate_consumption_audit",
            "trading_date": day,
            "raw_candidate_count": 319,
            "promoted_candidate_count": 0,
            "excluded_candidate_count": 319,
            "consumption_counts": {"EXCLUDED_UNCOVERED_SYMBOL": 299, "EXCLUDED_LOW_SCORE": 18, "EXCLUDED_POLICY": 2},
            "certified_artifact_path": str(final_path),
            "certified_universe_symbol_count": 3,
            "certified_universe_symbols": ["SPY", "QQQ", "CRWD"],
            "candidate_rows": [
                {"candidate_id": "crwd-event", "raw_intent_id": "intent-crwd-event", "symbol": "CRWD", "sleeve_id": "C2_EVENT_DISLOCATION_V1", "score": 0.0, "source_status": "NO_SIGNAL", "consumption_category": "EXCLUDED_LOW_SCORE", "consumption_reason": "Raw candidate did not present a promotable signal for the EOD report.", "covered_by_certified_eod": True, "source_artifact_path": "artifact-crwd-event"},
                {"candidate_id": "crwd-trend", "raw_intent_id": "intent-crwd-trend", "symbol": "CRWD", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "score": 0.0, "source_status": "CANDIDATE_CREATED", "consumption_category": "EXCLUDED_POLICY", "consumption_reason": "No effective promoted sleeve manifest exists for EOD report promotion.", "covered_by_certified_eod": True, "source_artifact_path": "artifact-crwd-trend"},
                {"candidate_id": "wide", "symbol": "WIDE", "sleeve_id": "C2_EVENT_DISLOCATION_V1", "consumption_category": "EXCLUDED_UNCOVERED_SYMBOL", "consumption_reason": "WIDE is outside certified universe", "covered_by_certified_eod": False, "source_artifact_path": "artifact-wide"},
            ],
        },
    )
    _write_json(
        root / "reports" / "dynamic_certification_queue_v1" / day / "dynamic_certification_queue.v1.json",
        {
            "schema_id": "dynamic_certification_queue",
            "schema_version": "v1",
            "artifact_id": "dynamic_certification_queue_v1",
            "operational_day": day,
            "day_utc": day,
            "generated_at_utc": f"{day}T22:00:00Z",
            "requested_symbols": ["CRWD"],
            "requested_symbol_count": 1,
            "estimated_provider_load": 1,
            "certification_status": "CERTIFIED",
            "certified_symbols": ["CRWD"],
            "failed_symbols": [],
            "promoted_after_certification_count": 0,
            "queued_symbols": [{"symbol": "CRWD", "priority_score": 77.0, "reason": "recurring uncovered exclusions; active candidate/intent pressure", "expected_provider_coverage": {"estimated_provider_requests": 1}, "certification_status": "CERTIFIED", "certification_result": {"provider": "TIINGO", "certification_status": "CERTIFIED", "reason": "VALID"}}],
            "certification_results": [{"symbol": "CRWD", "provider": "TIINGO", "certification_status": "CERTIFIED", "reason": "VALID"}],
            "content_hash": "crwd-queue-hash",
        },
    )
    _write_json(
        root / "reports" / "aegis_operational_maturity_hardening_v1" / day / "operational_maturity_hardening.v1.json",
        {"capture_ticket_count": 0, "capture_ticket_status": "NONE_AVAILABLE", "capture_ticket_projection": {"capture_ticket_count": 0, "capture_ticket_status": "NONE_AVAILABLE"}},
    )
    _write_json(
        root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        {"total_sleeves_expected": 3, "total_sleeves_run": 3, "candidate_generation_status": "COMPLETE", "expected_sleeve_ids": ["C2_EVENT_DISLOCATION_V1", "C2_MEAN_REVERSION_EQ_V1", "C2_TREND_EQ_PRIMARY_V1"]},
    )

    payload = resolve_current_operator_truth_v1(truth_root=root, day_utc=day, generated_at_utc=f"{day}T22:30:00Z")
    funnel = payload["current_day_status"]["candidate_funnel_projection"]

    assert funnel["raw_candidate_count"] == 319
    assert funnel["excluded_uncovered_symbol_count"] == 299
    assert funnel["excluded_low_score_count"] == 18
    assert funnel["excluded_policy_count"] == 2
    assert funnel["promoted_candidate_count"] == 0
    assert funnel["capture_ticket_count"] == 0
    assert funnel["dynamic_certification_queue"]["requested_symbols"] == ["CRWD"]
    assert funnel["dynamic_certification_queue"]["certification_status"] == "CERTIFIED"
    assert funnel["dynamic_certification_queue"]["top_recommendations"][0]["symbol"] == "CRWD"
    crwd_rows = [row for row in funnel["drilldown_rows"] if row["symbol"] == "CRWD"]
    assert {row["exclusion_category"] for row in crwd_rows} == {"EXCLUDED_LOW_SCORE", "EXCLUDED_POLICY"}
    assert all(row["covered_by_certified_universe"] is True for row in crwd_rows)
    assert any(row["trading_day"] == prior_day and row["excluded_uncovered_symbol_count"] == 302 for row in funnel["trend_5d"])
    assert any(row["trading_day"] == day and row["excluded_uncovered_symbol_count"] == 299 for row in funnel["trend_5d"])


def test_candidate_funnel_projection_exposes_consumption_audit_counts(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    day = "2026-05-22"
    final_path = root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json"
    _write_json(final_path, {"schema_id": "final_eod_market_data_v1", "day_utc": day, "final_eod_symbols": ["SPY", "QQQ"]})
    _write_json(
        root / "reports" / "canonical_universe_authority_v1" / day / "canonical_universe_authority.v1.json",
        {
            "schema_id": "canonical_universe_authority",
            "day_utc": day,
            "source_day": day,
            "authority_status": "PASS",
            "canonical_universe_authority_id": "cua-test",
            "universe_symbol_count": 4,
            "universe_symbols": ["SPY", "QQQ", "IWM", "DIA"],
            "writer_process": "ranked_symbol_universe_v1",
            "generation_pipeline": "ranked_symbol_universe_v1",
            "immutable_hash": "abc123",
            "generated_at": f"{day}T21:00:00Z",
        },
    )
    _write_json(
        root / "reports" / "candidate_consumption_audit_v1" / day / "run-1" / "candidate_consumption_audit.v1.json",
        {
            "schema_id": "candidate_consumption_audit",
            "trading_date": day,
            "raw_candidate_count": 319,
            "promoted_candidate_count": 0,
            "excluded_candidate_count": 319,
            "consumption_counts": {"EXCLUDED_UNCOVERED_SYMBOL": 302, "EXCLUDED_LOW_SCORE": 16, "EXCLUDED_POLICY": 1},
            "certified_artifact_path": str(final_path),
            "certified_universe_symbol_count": 2,
            "certified_universe_symbols": ["SPY", "QQQ"],
            "candidate_rows": [
                {"candidate_id": "outside", "raw_intent_id": "intent-outside", "symbol": "IWM", "sleeve_id": "TREND", "score": 0.91, "near_promotion": True, "source_status": "CANDIDATE_CREATED", "consumption_category": "EXCLUDED_UNCOVERED_SYMBOL", "consumption_reason": "outside certified universe", "covered_by_certified_eod": False, "source_artifact_path": "artifact-a"},
                {"candidate_id": "low", "symbol": "SPY", "sleeve_id": "TREND", "consumption_category": "EXCLUDED_LOW_SCORE", "consumption_reason": "below score threshold", "covered_by_certified_eod": True, "source_artifact_path": "artifact-b"},
                {"candidate_id": "policy", "symbol": "QQQ", "sleeve_id": "EVENT", "consumption_category": "EXCLUDED_POLICY", "consumption_reason": "policy excluded", "covered_by_certified_eod": True, "source_artifact_path": "artifact-c"},
            ],
        },
    )
    _write_json(
        root / "reports" / "aegis_operational_maturity_hardening_v1" / day / "operational_maturity_hardening.v1.json",
        {"capture_ticket_count": 0, "capture_ticket_status": "NONE_AVAILABLE", "capture_ticket_projection": {"capture_ticket_count": 0, "capture_ticket_status": "NONE_AVAILABLE"}},
    )
    _write_json(
        root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        {"total_sleeves_expected": 2, "total_sleeves_run": 2, "candidate_generation_status": "COMPLETE", "expected_sleeve_ids": ["TREND", "EVENT"]},
    )

    payload = resolve_current_operator_truth_v1(truth_root=root, day_utc=day, generated_at_utc=f"{day}T22:00:00Z")
    funnel = payload["current_day_status"]["candidate_funnel_projection"]

    assert funnel["raw_candidate_count"] == 319
    assert funnel["promoted_candidate_count"] == 0
    assert funnel["capture_ticket_count"] == 0
    assert funnel["excluded_uncovered_symbol_count"] == 302
    assert funnel["excluded_low_score_count"] == 16
    assert funnel["excluded_policy_count"] == 1
    assert funnel["plain_english_summary"] == "No capture tickets were created because 302 candidates were outside the certified universe, 16 were below score threshold, and 1 was excluded by policy."
    assert funnel["drilldown_rows"][0]["exclusion_category"] == "EXCLUDED_UNCOVERED_SYMBOL"
    assert funnel["universe_diagnostics"]["certified_universe_too_narrow"] is True
    assert funnel["canonical_universe_authority"]["symbol_count"] == 4
    assert funnel["certified_universe"]["symbol_count"] == 2
    assert funnel["diagnostics"]["did_sleeves_run"] is True
    queue = funnel["dynamic_certification_queue"]
    assert queue["requested_symbols"] == ["IWM"]
    assert queue["top_recommendations"][0]["symbol"] == "IWM"
    command_ids = {row["command_id"] for row in queue["commands"]}
    assert {"QUEUE_CERTIFICATION", "CERTIFY_SELECTED_SYMBOLS", "VIEW_CERTIFICATION_RESULT"} <= command_ids
