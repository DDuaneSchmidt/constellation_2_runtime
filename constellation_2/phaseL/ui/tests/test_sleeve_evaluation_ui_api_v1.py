from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui_api.sleeve_evaluation_read_model import build_sleeve_evaluation_view


DAY = "2026-04-16"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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

    payload = build_sleeve_evaluation_view(DAY)

    assert payload["view_name"] == "sleeve_evaluation_state"
    assert payload["contract_id"] == "sleeve_evaluation_projection"
    assert payload["sleeve_registry_summary"]["total_sleeves"] == 1
    assert payload["sleeves"][0]["sleeve_id"] == "C2_TREND_EQ_PRIMARY"
    assert payload["sleeves"][0]["actual_allocation_pct"] == 0.02
    assert payload["sleeves"][0]["effective_risk_budget_usd"] == 1200.0
    assert payload["sleeves"][0]["stability_metrics"]["confidence_state"] == "MEDIUM"
    assert payload["sleeves"][0]["recommendation"]["recommendation_state"] == "reduce"
    assert payload["_metadata_validation"]["ok"] is True


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
    switch_section = pages.split("export async function loadRouteView", 1)[1]
    assert 'case "sleeves":' in switch_section
    assert "return renderSleevesPage(state);" in switch_section
