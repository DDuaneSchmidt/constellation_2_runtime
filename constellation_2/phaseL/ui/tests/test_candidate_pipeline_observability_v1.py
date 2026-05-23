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
