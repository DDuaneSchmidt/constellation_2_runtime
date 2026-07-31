from __future__ import annotations

import json
from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import OpsHandler, _operator_cockpit_payload


DAY = "2026-05-17"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_canonical_state(root: Path) -> None:
    canonical = {
        "schema_id": "aegis_canonical_operator_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-05-17T13:00:00Z",
        "source_hashes": {"runtime_truth": "hash-runtime", "candidate_lifecycle": "hash-candidate"},
        "runtime": {
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "highest_readiness_layer": "ADVISORY_ONLY",
            "manual_trade_capture_allowed": True,
            "advisory_status": "NO_ACTIONABLE_CANDIDATES",
            "disabled_by_policy": {
                "live_broker_trading": "DISABLED_BY_DESIGN",
                "autonomous_execution": "DISABLED_BY_DESIGN",
                "broker_submit_transmit": "DISABLED_BY_DESIGN",
            },
        },
        "actions_required": [
            {
                "action_id": "candidate:demo-candidate",
                "type": "CANDIDATE_DECISION",
                "priority": "HIGH",
                "title": "Review demo-candidate",
                "reason": "Candidate is awaiting operator decision.",
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            }
        ],
        "candidates": {
            "awaiting_decision": [
                {
                    "candidate_id": "demo-candidate",
                    "symbol": "SPY",
                    "direction": "LONG",
                    "current_operator_decision": "AWAITING_DECISION",
                    "current_intended_shares": 25,
                    "correction_count": 1,
                    "outcome_status": "OUTCOME_PENDING",
                }
            ],
            "approved_or_traded": [],
            "ignored": [],
            "deferred": [],
            "awaiting_outcome": [],
            "corrected": [
                {
                    "candidate_id": "demo-candidate",
                    "symbol": "SPY",
                    "current_operator_decision": "AWAITING_DECISION",
                    "current_intended_shares": 25,
                    "correction_count": 1,
                }
            ],
        },
        "top_candidates": [
            {
                "candidate_id": "demo-candidate",
                "rank": 1,
                "symbol": "SPY",
                "direction": "LONG",
                "sleeve_id": "event-risk",
                "priority": "HIGH",
                "why_this_trade": "Fixture candidate explanation.",
                "why_now": "Fixture event context.",
                "why_not": "Fixture risk caveat.",
                "current_operator_decision": "AWAITING_DECISION",
                "current_intended_shares": 25,
                "correction_count": 1,
            }
        ],
        "sleeves": {
            "healthy": [],
            "watch": [{"sleeve_id": "event-risk", "recommendation": "WATCH"}],
            "challenged": [],
            "insufficient_data": [],
        },
        "research": {
            "captured_hypotheses": [
                {
                    "hypothesis_id": "rh-edge-2026-0101",
                    "title": "Volatility-regime overshoot",
                    "status": "IDEA",
                    "source": "CHATGPT_SEED",
                    "classification": "SYSTEM_GENERATED_HYPOTHESIS",
                    "created_at_utc": "2026-05-15T02:20:43Z",
                }
            ],
            "active_hypotheses": [
                {
                    "hypothesis_id": "rh-edge-2026-0101",
                    "title": "Volatility-regime overshoot",
                    "status": "IDEA",
                    "source": "CHATGPT_SEED",
                    "classification": "SYSTEM_GENERATED_HYPOTHESIS",
                    "created_at_utc": "2026-05-15T02:20:43Z",
                }
            ],
            "validated_hypotheses": [],
            "rejected_hypotheses": [
                {
                    "hypothesis_id": "rh-edge-2026-0102",
                    "title": "Rejected always-buy hypothesis",
                    "status": "REJECTED",
                    "source": "CHATGPT_SEED",
                    "classification": "REJECTED",
                    "created_at_utc": "2026-05-15T02:20:43Z",
                }
            ],
            "archived_hypotheses": [],
            "needs_classification": [],
            "test_fixtures_excluded": [{"hypothesis_id": "fixture_idea_v1", "classification": "TEST_FIXTURE"}],
            "duplicates": [{"hypothesis_id": "EDGE-2026-0101", "duplicate_of": "rh-edge-2026-0101", "classification": "LEGACY_DUPLICATE"}],
            "priority_tasks": [{"task_id": "research-1", "title": "Review event-risk", "status": "REVIEW_REQUIRED"}],
            "new_tasks": [],
            "review_required": [],
        },
        "governance": {
            "awaiting_approval": [{"recommendation_id": "rec-1", "type": "WATCH_SLEEVE", "target": "event-risk", "approval_status": "PROPOSED"}],
            "approved": [],
            "rejected": [],
            "deferred": [],
        },
        "regime": {"status": "PARTIAL", "volatility_regime": "UNKNOWN"},
        "event_triggers": [{"trigger_id": "trigger-1", "detected_condition": "VOLATILITY_SPIKE", "selected_sleeve_ids": ["event-risk"], "candidate_count": 1, "status": "SUCCESS"}],
        "opportunities": {
            "open": [
                {
                    "candidate_id": "demo-candidate",
                    "symbol": "SPY",
                    "direction": "LONG",
                    "sleeve_id": "event-risk",
                    "rank": 1,
                    "priority": "HIGH",
                }
            ],
            "triggered": [{"trigger_id": "trigger-1", "candidate_count": 1}],
            "awaiting_decision": [{"candidate_id": "demo-candidate"}],
            "deferred": [],
            "awaiting_outcome": [],
        },
        "edge_lab": {
            "active_hypotheses": [
                {
                    "hypothesis_id": "rh-edge-2026-0101",
                    "title": "Volatility-regime overshoot",
                    "lifecycle_stage": "IDEA",
                    "next_step": "queue research",
                }
            ],
            "research_queue": [{"task_id": "research-1", "title": "Review event-risk"}],
            "challenger_findings": [{"sleeve_id": "event-risk", "recommendation": "WATCH"}],
            "review_required": [],
            "completed": [],
            "rejected": [{"hypothesis_id": "rh-edge-2026-0102"}],
            "archived": [],
            "duplicates": [{"hypothesis_id": "EDGE-2026-0101"}],
            "test_fixtures_excluded": [{"hypothesis_id": "fixture_idea_v1"}],
        },
        "journal": {
            "candidate_history": [{"candidate_id": "demo-candidate"}],
            "edge_history": [{"hypothesis_id": "rh-edge-2026-0101"}],
            "system_artifacts": [{"id": "runtime_truth", "path": "/tmp/runtime_truth.json"}],
            "audit_links": [{"id": "runtime_truth", "path": "/tmp/runtime_truth.json"}],
        },
        "warnings": [],
        "no_action_now": ["No broker action is available."],
        "drilldown_index": [{"id": "runtime_truth", "path": "/tmp/runtime_truth.json", "hash": "hash-runtime"}],
        "missing_inputs": [],
        "conflicts": [],
        "freshness": {},
        "safety": {
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }
    brief = {
        "schema_id": "aegis_operator_brief",
        "schema_version": "v1",
        "day_utc": DAY,
        "reads_only_canonical_operator_state": True,
        "sections": {"today": {"runtime_truth_classification": "PARTIAL_CONTEXT"}},
    }
    _write_json(
        root / "reports" / "aegis_canonical_operator_state_v1" / DAY / "canonical_operator_state.v1.json",
        canonical,
    )
    _write_json(root / "reports" / "aegis_operator_brief_v1" / DAY / "operator_brief.v1.json", brief)


def test_operator_cockpit_route_and_api_payload_use_canonical_state(tmp_path: Path) -> None:
    _seed_canonical_state(tmp_path)

    payload = _operator_cockpit_payload(tmp_path, DAY)

    assert "/aegis-operator-cockpit" in OpsHandler.SHELL_ROUTES
    assert payload["ok"] is True
    assert payload["status"] == "AVAILABLE"
    assert payload["source_read_model"] == "canonical_operator_state.v1.json"
    assert payload["canonical_operator_state"]["runtime"]["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert payload["runtime"]["highest_readiness_layer"] == "ADVISORY_ONLY"
    assert payload["actions_required"][0]["type"] == "CANDIDATE_DECISION"
    assert payload["top_candidates"][0]["candidate_id"] == "demo-candidate"
    assert payload["candidate_decisions_corrections"]["corrected"][0]["correction_count"] == 1
    assert payload["canonical_operator_state"]["opportunities"]["open"][0]["candidate_id"] == "demo-candidate"
    assert payload["canonical_operator_state"]["edge_lab"]["active_hypotheses"][0]["hypothesis_id"] == "rh-edge-2026-0101"
    assert payload["canonical_operator_state"]["journal"]["candidate_history"][0]["candidate_id"] == "demo-candidate"
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["broker_submit_transmit_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False
    assert payload["command_center_queue_audit_v1"]["schema_id"] == "aegis_command_center_queue_audit"
    assert payload["command_center_queue_audit_v1"]["data_quality_status"] == "MISSING_ARTIFACT"
    assert payload["source_paths"]["command_center_queue_audit_v1"].endswith("command_center_queue_audit.v1.json")
    assert payload["drilldown_links"][0]["path"] == "/tmp/runtime_truth.json"


def test_operator_cockpit_merges_operator_state_selected_exposure_and_watchlist(tmp_path: Path) -> None:
    _seed_canonical_state(tmp_path)
    gate_rows = [
        {
            "candidate_id": "c2_trend_eq_amt_2026-05-20_v1",
            "symbol": "AMT",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "direction": "LONG",
            "score": 31.3333,
            "confidence": "UNKNOWN",
            "rank_before_arbitration": 1,
            "selected_by_gate": "YES",
            "portfolio_gate_decision": "ALLOW",
            "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
        },
        {
            "candidate_id": "c2_cross_asset_trend_qqq_2026-05-20_v1",
            "symbol": "QQQ",
            "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
            "engine_id": "C2_CROSS_ASSET_TREND_V1",
            "direction": "LONG",
            "score": 6.3333,
            "confidence": "UNKNOWN",
            "rank_before_arbitration": 2,
            "selected_by_gate": "NO",
            "portfolio_gate_decision": "SUPPRESS",
            "suppression_code": "one_primary_per_regime_bucket_suppressed",
            "suppression_reason": "Suppressed by ONE_PRIMARY_PER_REGIME_BUCKET after another candidate in the same regime bucket was allowed.",
            "reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"],
            "competing_selected_candidate_id": "c2_trend_eq_amt_2026-05-20_v1",
        },
    ]
    for idx in range(3, 43):
        gate_rows.append(
            {
                "candidate_id": f"suppressed-{idx}",
                "symbol": f"S{idx}",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "direction": "LONG",
                "score": 1.0,
                "confidence": "UNKNOWN",
                "rank_before_arbitration": idx,
                "selected_by_gate": "NO",
                "portfolio_gate_decision": "SUPPRESS",
                "suppression_code": "one_primary_per_regime_bucket_suppressed",
                "suppression_reason": "Suppressed by ONE_PRIMARY_PER_REGIME_BUCKET after another candidate in the same regime bucket was allowed.",
                "reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"],
                "competing_selected_candidate_id": "c2_trend_eq_amt_2026-05-20_v1",
            }
        )
    _write_json(
        tmp_path / "reports" / "portfolio_gate_candidate_report_v1" / DAY / "portfolio_gate_candidate_report.v1.json",
        {
            "schema_id": "portfolio_gate_candidate_report",
            "schema_version": "portfolio_gate_candidate_report.v1",
            "day_utc": DAY,
            "determinism_fingerprint": "gate-20260520",
            "selected_candidate_id": "c2_trend_eq_amt_2026-05-20_v1",
            "candidate_count": 42,
            "selected_count": 1,
            "suppressed_count": 41,
            "suppression_code_counts": {"one_primary_per_regime_bucket_suppressed": 41},
            "candidate_rows": gate_rows,
        },
    )
    _write_json(
        tmp_path / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json",
        {
            "schema_id": "exposure_intent_paper_submission_package",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "BLOCKED",
            "exposure_intent_id": "c2_trend_eq_amt_2026-05-20_v1",
            "symbol": "AMT",
            "paper_trade_intent_created": False,
            "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION",
            "blocker_message": "AMT market data is stale.",
            "market_data_status": {"expected_session": DAY, "observed_session": "2026-04-02", "status": "STALE_OR_MISSING"},
        },
    )
    _write_json(
        tmp_path / "reports" / "operator_state_snapshot_v1" / DAY / "operator_state_snapshot.v1.json",
        {
            "schema_id": "operator_state_snapshot",
            "schema_version": "v1",
            "snapshot_id": "operator-snapshot-amt",
            "day_utc": DAY,
            "artifact_path": str(tmp_path / "reports" / "operator_state_snapshot_v1" / DAY / "operator_state_snapshot.v1.json"),
            "manual_capture_candidate": {
                "candidate_available": True,
                "selected_exposure_intent_id": "c2_trend_eq_amt_2026-05-20_v1",
                "symbol": "AMT",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "direction": "LONG",
                "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION",
                "blocker_message": "AMT market data is stale.",
                "stale_market_data": True,
                "paper_trade_intent_created": False,
            },
            "suppressed_candidate_watchlist": {
                "suppressed_count": 41,
                "suppression_code_counts": {"one_primary_per_regime_bucket_suppressed": 41},
                "candidates": [
                    {
                        "candidate_id": "c2_cross_asset_trend_qqq_2026-05-20_v1",
                        "symbol": "QQQ",
                        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
                        "suppression_code": "one_primary_per_regime_bucket_suppressed",
                        "competing_selected_candidate_id": "c2_trend_eq_amt_2026-05-20_v1",
                        "watchlist_only": True,
                    }
                ],
            },
            "latest_run_summary": {
                "selected_candidate_count": 1,
                "suppressed_count": 41,
                "blocked_conversion_count": 1,
                "sleeves_ran": 2,
            },
            "errors": [],
            "degraded_sections": [],
            "blockers": [],
        },
    )

    payload = _operator_cockpit_payload(tmp_path, DAY)

    assert payload["manual_capture_candidate"]["symbol"] == "AMT"
    assert payload["manual_capture_candidate"]["selected_exposure_intent_id"] == "c2_trend_eq_amt_2026-05-20_v1"
    assert payload["manual_capture_candidate"]["blocker_code"] == "STALE_MARKET_DATA_BLOCKS_CONVERSION"
    assert payload["suppressed_candidate_watchlist"]["suppressed_count"] == 41
    assert payload["suppressed_candidate_watchlist"]["candidates"][0]["symbol"] == "QQQ"
    assert payload["active_opportunity_projection"]["selected_candidate_count"] == 1
    assert payload["active_opportunity_projection"]["suppressed_count"] == 41
    assert payload["operator_today_projection"]["selected_candidate_count"] == 1
    assert payload["operator_today_projection"]["suppressed_count"] == 41
    assert payload["eod_opportunity_outcome_report"]["selected_candidate_count"] == 1
    assert payload["eod_opportunity_outcome_report"]["suppressed_candidate_count"] == 41
    assert payload["eod_opportunity_outcome_report"]["blocked_conversion_count"] == 1
    assert payload["eod_opportunity_outcome_report"]["aggregate_summary"]["sleeves_ran"] == 2


def test_operator_cockpit_api_exposes_research_classification_buckets(tmp_path: Path) -> None:
    _seed_canonical_state(tmp_path)

    payload = _operator_cockpit_payload(tmp_path, DAY)
    research = payload["research_priorities"]

    assert len(research["captured_hypotheses"]) == 1
    assert len(research["active_hypotheses"]) == 1
    assert len(research["validated_hypotheses"]) == 0
    assert len(research["rejected_hypotheses"]) == 1
    assert len(research["archived_hypotheses"]) == 0
    assert len(research["needs_classification"]) == 0
    assert len(research["test_fixtures_excluded"]) == 1
    assert len(research["duplicates"]) == 1
    assert research["test_fixtures_excluded"][0]["classification"] == "TEST_FIXTURE"
    assert research["duplicates"][0]["duplicate_of"] == "rh-edge-2026-0101"


def test_operator_cockpit_default_day_uses_latest_canonical_state(monkeypatch, tmp_path: Path) -> None:
    old_day = "2026-05-02"
    latest_day = DAY
    monkeypatch.setattr(server, "GLOBAL_TRUTH_ROOT", tmp_path)
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: tmp_path)
    (tmp_path / "reports" / "aegis_day_run_v1" / old_day).mkdir(parents=True, exist_ok=True)
    _seed_canonical_state(tmp_path)

    assert server._projection_day(None) == old_day
    assert server._projection_day_for_report(None, "aegis_canonical_operator_state_v1") == latest_day


def test_operator_api_explicit_day_does_not_fallback_to_current_truth_source_day(monkeypatch, tmp_path: Path) -> None:
    requested_day = "2026-05-29"
    stale_day = "2026-05-26"
    monkeypatch.setattr(server, "GLOBAL_TRUTH_ROOT", tmp_path)
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: tmp_path)
    monkeypatch.setattr(server, "resolve_current_operator_truth_v1", lambda **_: {"source_day": stale_day})

    assert server._resolved_operator_api_day_v1(requested_day, "aegis_canonical_operator_state_v1") == requested_day
    assert server._resolved_operator_api_day_v1(requested_day, "operator_state_snapshot_v1") == requested_day


def test_operator_cockpit_missing_canonical_state_is_explicit(tmp_path: Path) -> None:
    payload = _operator_cockpit_payload(tmp_path, DAY)

    assert payload["ok"] is True
    assert payload["status"] == "MISSING"
    assert payload["message"] == "Today's operator state has not been generated yet."
    assert payload["source_read_model"] == "canonical_operator_state.v1.json"
    assert payload["source_status"]["canonical_state"] == "missing"
    assert payload["source_status"]["operator_brief"] == "missing"
    assert payload["source_status"]["runtime_truth"] == "unavailable"
    assert payload["actions_required"][0]["type"] == "MISSING_CANONICAL_STATE"
    assert payload["actions_required"][0]["suggested_command"] == "npm run aegis:canonical-operator-state && npm run aegis:operator-brief"
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False


def test_operator_cockpit_missing_brief_is_non_blocking_warning(tmp_path: Path) -> None:
    _seed_canonical_state(tmp_path)
    brief_path = tmp_path / "reports" / "aegis_operator_brief_v1" / DAY / "operator_brief.v1.json"
    brief_path.unlink()

    payload = _operator_cockpit_payload(tmp_path, DAY)

    assert payload["ok"] is True
    assert payload["status"] == "PARTIAL"
    assert payload["source_status"]["canonical_state"] == "available"
    assert payload["source_status"]["operator_brief"] == "missing"
    assert payload["warnings"][0]["warning_id"] == "operator_brief_missing"
    assert payload["warnings"][0]["suggested_command"] == "npm run aegis:operator-brief"


def test_operator_cockpit_ui_is_read_only_and_uses_canonical_api_only() -> None:
    server_source = Path("constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    domain_source = Path("constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")
    pages_source = pages_source_v1(Path("."))

    assert 'path == "/api/aegis/operator-cockpit"' in server_source
    assert 'path == "/api/aegis/candidate-review-ledger"' in server_source
    assert 'path: "/aegis-operator-cockpit"' in pages_source
    assert 'query("/api/aegis/operator-cockpit", params)' in domain_source
    assert "fetchAegisOperatorCockpit" in pages_source

    page_block = pages_source.split("async function renderAegisOperatorCockpitPage", 1)[1].split("async function renderAegisAdaptiveIntelligencePage", 1)[0]
    assert "fetchAegisOperatorCockpit" in page_block
    assert "fetchAegisRuntimeTruth" not in page_block
    assert "fetchAegisLiteExecutionQueue" not in page_block
    assert "PARTIAL_CONTEXT" in page_block
    assert "DISABLED_BY_DESIGN" in page_block
    assert "Review commands" in page_block
    assert "Candidate Review Ledger" in page_block
    assert "Review ledger is audit-only" in pages_source
    assert "candidate-review-ledger" in pages_source
    assert "record-candidate-review" in page_block
    assert "TRADED_MANUALLY" not in page_block
    assert "Approved for manual IB execution" not in page_block
    assert "Drilldown" in page_block

    post_block = server_source.split("def do_POST", 1)[1].split("def do_PATCH", 1)[0]
    patch_block = server_source.split("def do_PATCH", 1)[1]
    assert "/api/aegis/operator-cockpit" not in post_block
    assert "/api/aegis/operator-cockpit" not in patch_block
    assert 'postJson("/api/aegis/operator-cockpit"' not in pages_source
    assert 'patchJson("/api/aegis/operator-cockpit"' not in pages_source
    assert "autonomous_execution_allowed === true" in page_block
