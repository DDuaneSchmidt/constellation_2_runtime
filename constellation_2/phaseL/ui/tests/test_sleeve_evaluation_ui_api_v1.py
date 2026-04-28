from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui_api.sleeve_evaluation_read_model import build_sleeve_evaluation_view


DAY = "2026-04-16"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _seed_single_sleeve_projection_fixture(
    monkeypatch,
    tmp_path: Path,
    *,
    readiness_doc: dict | None,
    execution_sleeve_id: str = "PRIMARY",
    measurement_qualification_state: str = "QUALIFIED",
    include_evaluation_manifest: bool = True,
    include_measurement_snapshot: bool = True,
    include_governance_snapshot: bool = True,
    measurement_snapshot_day: str = DAY,
    measurement_reason_codes: list[str] | None = None,
    measurement_attribution_diagnostics: dict | None = None,
) -> None:
    sleeve_truth = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    repo_root = tmp_path / "repo"

    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.SLEEVE_TRUTH_ROOT", sleeve_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.REPO_ROOT", repo_root)

    _write_json(
        repo_root / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json",
        {
            "schema_id": "C2_CAPITAL_AUTHORITY_POLICY_V1",
            "sleeves": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "display_name": "Trend Sleeve",
                    "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "priority_rank": 10,
                    "limits": {
                        "max_capital_at_risk_cents": 120000,
                        "max_symbols": 25,
                        "max_single_name_notional_pct": "0.10",
                        "max_sector_concentration_pct": "0.25",
                    },
                }
            ],
        },
    )
    _write_json(
        sleeve_truth / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {
            "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
            "produced_utc": f"{DAY}T12:00:00Z",
            "per_sleeve": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "actual_notional_pct": "0.020000",
                    "actual_value_cents": 250000,
                    "allowed_capital_at_risk_cents": 120000,
                    "used_capital_at_risk_cents": 10000,
                    "headroom_cents": 110000,
                    "drift_band": "STABLE",
                    "edge_band": "NEGATIVE",
                    "qualification_state": measurement_qualification_state,
                    "sample_sufficiency_band": "INSUFFICIENT",
                    "execution_health_band": "UNKNOWN",
                }
            ],
        },
    )
    if include_evaluation_manifest:
        _write_json(
            sleeve_truth / "reports" / "evaluation_input_manifest_v1" / DAY / "C2_TREND_EQ_PRIMARY" / "evaluation_input_manifest.v1.json",
            {
                "schema_id": "C2_EVALUATION_INPUT_MANIFEST_V1",
                "produced_utc": f"{DAY}T13:00:00Z",
                "execution_sleeve_id": execution_sleeve_id,
                "mode": "PAPER",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "measurement_window": {
                    "window_kind": "WEEKLY",
                    "sample_basis": "CLOSED_TRADES_ONLY",
                    "source_as_of_ts": f"{DAY}T00:00:00Z",
                },
                "evidence_summary": {
                    "included_trade_count": 1,
                    "excluded_trade_count": 0,
                    "sample_trade_count": 1,
                    "source_artifact_count": 5,
                },
                "closure_state": "COMPLETE",
                "first_blocker_code": "",
            },
        )
    if include_measurement_snapshot:
        _write_json(
            sleeve_truth
            / "reports"
            / "sleeve_edge_measurement_snapshot_v1"
            / measurement_snapshot_day
            / "C2_TREND_EQ_PRIMARY"
            / "sleeve_edge_measurement_snapshot.v1.json",
            {
                "schema_id": "C2_SLEEVE_EDGE_MEASUREMENT_SNAPSHOT_V1",
                "produced_utc": f"{DAY}T14:00:00Z",
                "sample_trade_count": 1,
                "edge_measurement": {
                    "native_net_expectancy": "0.01",
                    "adopted_management_expectancy": "0",
                    "qualification_state": measurement_qualification_state,
                    "edge_band": "QUALIFIED_POSITIVE",
                },
                "stability_measurement": {"drift_band": "STABLE", "stability_state": "stable"},
                "confidence_measurement": {"confidence_state": "MEDIUM", "sample_sufficiency_band": "LIMITED", "execution_health_band": "ACCEPTABLE"},
                "reason_codes": measurement_reason_codes or [],
                "attribution_diagnostics": measurement_attribution_diagnostics or {},
                "closure_state": "COMPLETE",
                "first_blocker_code": "",
            },
        )
    if include_governance_snapshot:
        _write_json(
            sleeve_truth / "reports" / "allocation_governance_snapshot_v1" / DAY / "C2_TREND_EQ_PRIMARY" / "allocation_governance_snapshot.v1.json",
            {
                "schema_id": "C2_ALLOCATION_GOVERNANCE_SNAPSHOT_V1",
                "produced_utc": f"{DAY}T15:00:00Z",
                "recommended_action_state": "reduce",
                "recommended_state_class": "RESTRICTED",
                "allocation_alignment_state": "REVIEW_REQUIRED",
                "recommendation_reason_codes": ["ALLOCATION_REVIEW_REQUIRED"],
                "current_control_context": {
                    "adoption_state": "AVAILABLE",
                    "action_state": "reduce",
                    "control_state": "review_required",
                    "headroom_multiplier_bp": 5000,
                },
                "closure_state": "COMPLETE",
                "first_blocker_code": "",
            },
        )
    if readiness_doc is not None:
        _write_json(
            sleeve_truth / "readiness_v1" / "sleeve_live_readiness_v1" / DAY / "sleeve_live_readiness.v1.json",
            readiness_doc,
        )


def test_sleeve_evaluation_view_uses_backend_policy_and_evaluation_artifacts(monkeypatch, tmp_path: Path) -> None:
    sleeve_truth = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    repo_root = tmp_path / "repo"

    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.SLEEVE_TRUTH_ROOT", sleeve_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.sleeve_evaluation_read_model.REPO_ROOT", repo_root)

    _write_json(
        repo_root / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json",
        {
            "schema_id": "C2_CAPITAL_AUTHORITY_POLICY_V1",
            "sleeves": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "display_name": "Trend Sleeve",
                    "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "priority_rank": 10,
                    "limits": {
                        "max_capital_at_risk_cents": 120000,
                        "max_symbols": 25,
                        "max_single_name_notional_pct": "0.10",
                        "max_sector_concentration_pct": "0.25",
                    },
                }
            ],
        },
    )
    _write_json(
        sleeve_truth / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {
            "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
            "produced_utc": f"{DAY}T12:00:00Z",
            "per_sleeve": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "actual_notional_pct": "0.020000",
                    "actual_value_cents": 250000,
                    "allowed_capital_at_risk_cents": 120000,
                    "used_capital_at_risk_cents": 10000,
                    "headroom_cents": 110000,
                    "drift_band": "STABLE",
                    "edge_band": "NEGATIVE",
                    "qualification_state": "MEASUREMENT_INVALID",
                    "sample_sufficiency_band": "INSUFFICIENT",
                    "execution_health_band": "UNKNOWN",
                    "qualification_snapshot_path": str(
                        sleeve_truth
                        / "reports"
                        / "sleeve_edge_snapshot_v1"
                        / DAY
                        / "C2_TREND_EQ_PRIMARY"
                        / "snapshot-id"
                        / "sleeve_edge_snapshot.v1.json"
                    ),
                }
            ],
        },
    )
    _write_json(
        sleeve_truth / "reports" / "evaluation_input_manifest_v1" / DAY / "C2_TREND_EQ_PRIMARY" / "evaluation_input_manifest.v1.json",
        {
            "schema_id": "C2_EVALUATION_INPUT_MANIFEST_V1",
            "produced_utc": f"{DAY}T13:00:00Z",
            "execution_sleeve_id": "PRIMARY",
            "mode": "PAPER",
            "sleeve_id": "C2_TREND_EQ_PRIMARY",
            "measurement_window": {"window_kind": "WEEKLY", "sample_basis": "CLOSED_TRADES_ONLY", "source_as_of_ts": f"{DAY}T00:00:00Z"},
            "evidence_summary": {"included_trade_count": 1, "excluded_trade_count": 0, "sample_trade_count": 1, "source_artifact_count": 5},
            "closure_state": "COMPLETE",
            "first_blocker_code": "",
        },
    )
    _write_json(
        sleeve_truth / "reports" / "sleeve_edge_measurement_snapshot_v1" / DAY / "C2_TREND_EQ_PRIMARY" / "sleeve_edge_measurement_snapshot.v1.json",
        {
            "schema_id": "C2_SLEEVE_EDGE_MEASUREMENT_SNAPSHOT_V1",
            "produced_utc": f"{DAY}T14:00:00Z",
            "sample_trade_count": 1,
            "edge_measurement": {"native_net_expectancy": "0.01", "adopted_management_expectancy": "0", "qualification_state": "QUALIFIED", "edge_band": "QUALIFIED_POSITIVE"},
            "stability_measurement": {"drift_band": "STABLE", "stability_state": "stable"},
            "confidence_measurement": {"confidence_state": "MEDIUM", "sample_sufficiency_band": "LIMITED", "execution_health_band": "ACCEPTABLE"},
            "closure_state": "COMPLETE",
            "first_blocker_code": "",
        },
    )
    _write_json(
        sleeve_truth / "reports" / "allocation_governance_snapshot_v1" / DAY / "C2_TREND_EQ_PRIMARY" / "allocation_governance_snapshot.v1.json",
        {
            "schema_id": "C2_ALLOCATION_GOVERNANCE_SNAPSHOT_V1",
            "produced_utc": f"{DAY}T15:00:00Z",
            "recommended_action_state": "reduce",
            "recommended_state_class": "RESTRICTED",
            "allocation_alignment_state": "REVIEW_REQUIRED",
            "recommendation_reason_codes": ["ALLOCATION_REVIEW_REQUIRED"],
            "current_control_context": {
                "adoption_state": "AVAILABLE",
                "action_state": "reduce",
                "control_state": "review_required",
                "headroom_multiplier_bp": 5000,
            },
            "closure_state": "COMPLETE",
            "first_blocker_code": "",
        },
    )
    _write_json(
        sleeve_truth / "readiness_v1" / "sleeve_live_readiness_v1" / DAY / "sleeve_live_readiness.v1.json",
        {
            "schema_id": "C2_SLEEVE_LIVE_READINESS_V1",
            "schema_version": 1,
            "sleeve_id": "PRIMARY",
            "mode": "PAPER",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T16:00:00Z",
            "authority_classification": "NON_CANONICAL_ADVISORY_ONLY",
            "control_decision_warning": "DO_NOT_USE_FOR_CONTROL_DECISIONS",
            "readiness_state": "NOT_READY",
            "readiness_summary": "Not ready",
            "promotion_decision_basis": "score below threshold",
            "readiness_score": 82,
            "readiness_grade": "B",
            "readiness_grade_scale": "1_to_7",
            "readiness_grade_1_to_7": 5,
            "score_threshold_grade_1_to_7": 6,
            "grading_thresholds_1_to_7": [
                {"grade": 7, "min_score": 95},
                {"grade": 6, "min_score": 85},
                {"grade": 5, "min_score": 75},
                {"grade": 4, "min_score": 65},
                {"grade": 3, "min_score": 50},
                {"grade": 2, "min_score": 30},
                {"grade": 1, "min_score": 0},
            ],
            "score_threshold": 85,
            "promotion_candidate": False,
            "promotion_blockers": [],
            "root_blockers": [],
            "derived_blockers": [],
            "aggregate_blocker_summary": {},
            "promotion_blockers_detail": [],
            "minimum_conditions_summary": [],
            "current_vs_required": {},
            "smallest_clearance_set": [],
            "blocker_dependency_order": [],
            "estimated_promotion_gate_sequence": [],
            "top_blockers_ordered": [],
            "pass_conditions_remaining": [],
            "recommended_next_actions": [],
            "calibration_support": {
                "score_contribution": [
                    {"check_id": "history_window", "weight": 30, "score_awarded": 25},
                    {"check_id": "hard_gates", "weight": 30, "score_awarded": 30},
                ]
            },
            "promotion_checklist": {
                "must_be_true": [],
                "currently_false": [],
                "gating_conditions": [],
                "informational_conditions": [],
            },
            "policy_ref": {"path": "governance/02_REGISTRIES/C2_SLEEVE_LIVE_READINESS_POLICY_V1.json", "sha256": "a" * 64},
            "checks": [],
            "reason_codes": [],
            "evidence_paths": ["/tmp/sleeve-live-readiness"],
            "evidence": [{"path": "/tmp/sleeve-live-readiness"}],
        },
    )

    payload = build_sleeve_evaluation_view(DAY)

    assert payload["view_name"] == "sleeve_evaluation_state"
    assert payload["contract_id"] == "sleeve_evaluation_projection"
    assert payload["sleeve_registry_summary"]["total_sleeves"] == 1
    assert payload["sleeves"][0]["sleeve_id"] == "C2_TREND_EQ_PRIMARY"
    assert payload["sleeves"][0]["actual_allocation_pct"] == 0.02
    assert payload["sleeves"][0]["effective_risk_budget_usd"] == 1200.0
    assert payload["sleeves"][0]["stability_metrics"]["confidence_state"] == "MEDIUM"
    assert payload["sleeves"][0]["recommendation"]["recommendation_state"] == "reduce"
    assert payload["sleeves"][0]["readiness_grade_1_to_7"] == 5
    assert payload["sleeves"][0]["score_threshold_grade_1_to_7"] == 6
    assert payload["sleeves"][0]["readiness_threshold_met"] is False
    assert payload["sleeve_live_readiness_summary"]["readiness_grade_1_to_7"] == 5
    assert payload["sleeve_live_readiness_summary"]["score_threshold_grade_1_to_7"] == 6
    assert payload["_metadata_validation"]["ok"] is True


def test_sleeve_evaluation_view_reports_missing_readiness_artifact(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(monkeypatch, tmp_path, readiness_doc=None, measurement_qualification_state="QUALIFIED")

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["score_threshold_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "NO_READINESS_ARTIFACT"
    assert "No readiness artifact found" in row["readiness_grade_reason"]
    assert payload["sleeve_live_readiness_summary"]["diagnostic_code"] == "NO_READINESS_ARTIFACT"


def test_sleeve_evaluation_view_reports_sleeve_id_mismatch(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(
        monkeypatch,
        tmp_path,
        readiness_doc={
            "sleeve_id": "C2_NOT_MATCHED",
            "readiness_score": 88,
            "score_threshold": 85,
            "readiness_grade_1_to_7": 6,
            "score_threshold_grade_1_to_7": 6,
            "readiness_grade_scale": "1_to_7",
            "grading_thresholds_1_to_7": [{"grade": 7, "min_score": 95}, {"grade": 6, "min_score": 85}, {"grade": 1, "min_score": 0}],
            "promotion_candidate": True,
            "reason_codes": ["SLEEVE_ID_MISMATCH_TEST"],
            "recommended_next_actions": ["align ids"],
            "calibration_support": {"score_contribution": [{"check_id": "history_window", "weight": 30, "score_awarded": 30}]},
        },
        measurement_qualification_state="QUALIFIED",
    )

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "READINESS_SLEEVE_ID_MISMATCH"
    assert "did not match readiness artifact" in row["readiness_grade_reason"]
    assert "artifact sleeve_id" in row["readiness_grade_next_step"].lower()


def test_sleeve_evaluation_view_reports_artifact_missing_grade_fields(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(
        monkeypatch,
        tmp_path,
        readiness_doc={
            "sleeve_id": "PRIMARY",
            "readiness_score": 88,
            "score_threshold": 85,
            "promotion_candidate": True,
            "reason_codes": ["MISSING_GRADE_FIELDS_TEST"],
            "recommended_next_actions": ["regenerate artifact"],
            "calibration_support": {"score_contribution": [{"check_id": "history_window", "weight": 30, "score_awarded": 30}]},
        },
        measurement_qualification_state="QUALIFIED",
    )

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["score_threshold_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "ARTIFACT_MISSING_GRADE_FIELDS"
    assert "Artifact missing grade fields" in row["readiness_grade_reason"]


def test_sleeve_evaluation_view_reports_measurement_invalid_grade_unavailable(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(
        monkeypatch,
        tmp_path,
        readiness_doc={
            "sleeve_id": "PRIMARY",
            "readiness_score": 88,
            "score_threshold": 85,
            "readiness_grade_1_to_7": 6,
            "score_threshold_grade_1_to_7": 6,
            "readiness_grade_scale": "1_to_7",
            "grading_thresholds_1_to_7": [{"grade": 7, "min_score": 95}, {"grade": 6, "min_score": 85}, {"grade": 1, "min_score": 0}],
            "promotion_candidate": True,
            "reason_codes": ["MEASUREMENT_INVALID_TEST"],
            "recommended_next_actions": ["fix measurement"],
            "calibration_support": {"score_contribution": [{"check_id": "history_window", "weight": 30, "score_awarded": 30}]},
        },
        measurement_qualification_state="MEASUREMENT_INVALID",
        measurement_reason_codes=["SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE", "SLEEVE_EDGE_MEASUREMENT_INVALID"],
        measurement_attribution_diagnostics={
            "source_fact_count": 3,
            "attributed_fact_count": 0,
            "unattributed_fact_count": 3,
            "expected_attribution_field": "engine_id",
            "fields_actually_present": ["lineage_attachment_refs.fact_record_ids", "trade_identity.sleeve_id"],
            "sample_records": [],
            "upstream_artifact_path": "/tmp/reconciled_trade_state_summary.v1.json",
            "lineage_requirement_diagnostic": (
                "Position facts have no order/engine lineage; sleeve grading requires order/fill attributed facts."
            ),
        },
    )

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["score_threshold_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "MEASUREMENT_INVALID"
    assert "position facts have no order/engine lineage" in row["readiness_grade_reason"].lower()
    assert "order/fill attributed facts" in row["readiness_grade_next_step"].lower()


def test_sleeve_evaluation_view_reports_broker_callback_absence_for_day(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(
        monkeypatch,
        tmp_path,
        readiness_doc={
            "sleeve_id": "PRIMARY",
            "readiness_score": 88,
            "score_threshold": 85,
            "readiness_grade_1_to_7": 6,
            "score_threshold_grade_1_to_7": 6,
            "readiness_grade_scale": "1_to_7",
            "grading_thresholds_1_to_7": [{"grade": 7, "min_score": 95}, {"grade": 6, "min_score": 85}, {"grade": 1, "min_score": 0}],
            "promotion_candidate": True,
            "reason_codes": ["MEASUREMENT_INVALID_TEST"],
            "recommended_next_actions": ["fix measurement"],
            "calibration_support": {"score_contribution": [{"check_id": "history_window", "weight": 30, "score_awarded": 30}]},
        },
        measurement_qualification_state="MEASUREMENT_INVALID",
        measurement_reason_codes=["SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE", "SLEEVE_EDGE_MEASUREMENT_INVALID"],
        measurement_attribution_diagnostics={
            "source_fact_count": 3,
            "attributed_fact_count": 0,
            "unattributed_fact_count": 3,
            "expected_attribution_field": "engine_id",
            "fields_actually_present": ["lineage_attachment_refs.fact_record_ids", "trade_identity.sleeve_id"],
            "sample_records": [],
            "upstream_artifact_path": "/tmp/reconciled_trade_state_summary.v1.json",
            "lineage_requirement_diagnostic": (
                "Position facts have no order/engine lineage; sleeve grading requires order/fill attributed facts."
            ),
        },
    )

    sleeve_truth = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_jsonl(
        sleeve_truth / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl",
        [
            {"event_type": "starting"},
            {"event_type": "openOrderEnd"},
            {"event_type": "execDetailsEnd"},
            {"event_type": "position"},
            {"event_type": "error", "ib_fields": {"args": [{"value": "errorCode=502"}]}},
            {"event_type": "error", "ib_fields": {"args": [{"value": "errorCode=504"}]}},
            {"event_type": "error", "ib_fields": {"args": [{"value": "errorCode=326"}]}},
        ],
    )
    _write_json(
        sleeve_truth / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_day_manifest.v1.json",
        {
            "schema_id": "broker_event_day_manifest",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "OK",
            "log": {
                "log_path": str(
                    sleeve_truth
                    / "execution_evidence_v1"
                    / "broker_events"
                    / DAY
                    / "broker_event_log.v1.jsonl"
                ),
                "event_type_counts": {
                    "starting": 1,
                    "openOrderEnd": 1,
                    "execDetailsEnd": 1,
                    "position": 1,
                    "error": 3,
                },
            },
        },
    )
    _write_json(
        sleeve_truth / "reports" / "broker_fact_spine_audit_v1" / "2026-04-08" / "broker_fact_spine_audit.v1.json",
        {
            "schema_id": "broker_fact_spine_audit",
            "schema_version": "v1",
            "counts": {
                "observed_order_fact_count": 1,
                "observed_fill_fact_count": 2,
            },
            "source_input_ref": {
                "artifact_path": "/home/node/constellation/ops/fixtures/broker_fact_spine_sample_events_v1.jsonl",
            },
        },
    )
    _write_jsonl(
        sleeve_truth / "execution_evidence_v1" / "broker_events" / "2026-04-13" / "broker_event_log.v1.jsonl",
        [{"event_type": "execDetails"}, {"event_type": "commissionReport"}],
    )
    _write_json(
        sleeve_truth / "execution_evidence_v1" / "broker_events" / "2026-04-13" / "broker_event_day_manifest.v1.json",
        {
            "schema_id": "broker_event_day_manifest",
            "schema_version": "v1",
            "day_utc": "2026-04-13",
            "status": "OK",
            "log": {"event_type_counts": {"execDetails": 1, "commissionReport": 1}},
        },
    )
    _write_jsonl(
        sleeve_truth / "execution_evidence_v1" / "broker_events" / "2026-04-15" / "broker_event_log.v1.jsonl",
        [{"event_type": "openOrder"}, {"event_type": "orderStatus"}],
    )
    _write_json(
        sleeve_truth / "execution_evidence_v1" / "broker_events" / "2026-04-15" / "broker_event_day_manifest.v1.json",
        {
            "schema_id": "broker_event_day_manifest",
            "schema_version": "v1",
            "day_utc": "2026-04-15",
            "status": "OK",
            "log": {"event_type_counts": {"openOrder": 1, "orderStatus": 1}},
        },
    )

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "MEASUREMENT_INVALID"
    assert "No grade available" in row["readiness_grade_reason"]
    assert "no order/fill broker events for this day" in row["readiness_grade_reason"]
    assert "502, 504, 326" in row["readiness_grade_reason"]
    assert "captured openOrder/orderStatus/execDetails callbacks" in row["readiness_grade_next_step"]
    assert "First fixture day with usable order/fill facts: 2026-04-08" in row["readiness_grade_next_step"]
    assert (
        "Live stream has partial data only: fills on 2026-04-13, orders on 2026-04-15, no day with both."
        in row["readiness_grade_next_step"]
    )
    assert payload["sleeve_live_readiness_summary"]["diagnostic_reason"] == row["readiness_grade_reason"]
    assert payload["sleeve_live_readiness_summary"]["diagnostic_next_step"] == row["readiness_grade_next_step"]


def test_sleeve_evaluation_view_reports_missing_edge_measurement_snapshot(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(
        monkeypatch,
        tmp_path,
        readiness_doc=None,
        include_measurement_snapshot=False,
        measurement_qualification_state="MEASUREMENT_INVALID",
    )

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "NO_SLEEVE_EDGE_MEASUREMENT_SNAPSHOT"
    assert "No sleeve edge measurement snapshot found" in row["readiness_grade_reason"]
    assert "Generate sleeve edge measurement snapshot" in row["readiness_grade_next_step"]


def test_sleeve_evaluation_view_reports_measurement_snapshot_day_mismatch(monkeypatch, tmp_path: Path) -> None:
    _seed_single_sleeve_projection_fixture(
        monkeypatch,
        tmp_path,
        readiness_doc=None,
        include_measurement_snapshot=True,
        measurement_snapshot_day="2026-04-15",
        measurement_qualification_state="MEASUREMENT_INVALID",
    )
    # Ensure the resolved day remains DAY and does not fallback to 2026-04-15.
    _write_json(
        tmp_path
        / "runtime"
        / "truth_sleeves"
        / "PRIMARY"
        / "PAPER"
        / "reports"
        / "evaluation_input_manifest_v1"
        / DAY
        / "C2_TREND_EQ_PRIMARY"
        / "evaluation_input_manifest.v1.json",
        {
            "schema_id": "C2_EVALUATION_INPUT_MANIFEST_V1",
            "produced_utc": f"{DAY}T13:00:00Z",
            "execution_sleeve_id": "PRIMARY",
            "mode": "PAPER",
            "sleeve_id": "C2_TREND_EQ_PRIMARY",
            "measurement_window": {"window_kind": "WEEKLY", "sample_basis": "CLOSED_TRADES_ONLY", "source_as_of_ts": f"{DAY}T00:00:00Z"},
            "evidence_summary": {"included_trade_count": 1, "excluded_trade_count": 0, "sample_trade_count": 1, "source_artifact_count": 5},
            "closure_state": "COMPLETE",
            "first_blocker_code": "",
        },
    )

    payload = build_sleeve_evaluation_view(DAY)
    row = payload["sleeves"][0]

    assert row["readiness_grade_1_to_7"] is None
    assert row["readiness_diagnostic_code"] == "SLEEVE_EDGE_MEASUREMENT_DAY_MISMATCH"
    assert "day mismatch" in row["readiness_grade_reason"].lower()
    assert "latest available day is 2026-04-15" in row["readiness_grade_next_step"]


def test_sleeves_page_reads_sleeve_projection_not_blocked_domain() -> None:
    root = Path(__file__).resolve().parents[4]
    domain_client = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    ).read_text(encoding="utf-8")
    pages = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ).read_text(encoding="utf-8")

    assert 'query("/api/sleeves")' in domain_client
    assert "async function renderSleevesPage" in pages
    assert "Readiness Grade" in pages
    assert "Sleeve Live Readiness Summary" in pages
    assert "Why" in pages
    switch_section = pages.split("export async function loadRouteView", 1)[1]
    assert 'case "sleeves":' in switch_section
    assert "return renderSleevesPage(state);" in switch_section
