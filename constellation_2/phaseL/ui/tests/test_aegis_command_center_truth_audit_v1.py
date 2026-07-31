from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server

TRUTH = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-06-01"

EXPECTED_FIELDS = {
    "aegis_mode_status": ("Aegis mode/status", "PAPER MODE"),
    "raw_signals": ("Raw signals", "21"),
    "valid_candidates": ("Valid candidates", "19"),
    "auto_promoted": ("Auto-promoted", "19"),
    "open_observations": ("Open observations", "53"),
    "usable_observations": ("Usable observations", "53"),
    "blocked_observations": ("Blocked observations", "0"),
    "missing_entry_marks": ("Missing entry marks", "0"),
    "closed_outcomes": ("Closed outcomes", "2"),
    "closed_today": ("Closed today", "2"),
    "included_samples": ("Included samples", "2"),
    "manual_review_queue": ("Manual review queue", "2"),
    "david_action": ("David action", "None"),
    "research_allocation_decisions": ("Research allocation decisions", "7"),
    "allocation_state": ("Allocation state", "UPDATED"),
    "current_bottleneck": ("Current bottleneck", "Validation sample sufficiency / underpowered hypotheses"),
    "last_successful_run": ("Last successful run", None),
    "next_scheduled_run": ("Next scheduled run", "No future run confirmed"),
    "latest_material_change": ("Latest material change", None),
}

REQUIRED_SOURCES = {
    "raw_signals": "aegis_candidate_generation_diagnostics_v1",
    "valid_candidates": "aegis_candidate_contracts_v1",
    "auto_promoted": "aegis_candidate_to_paper_lifecycle_v1",
    "open_observations": "aegis_outcome_registry_v1",
    "usable_observations": "aegis_outcome_registry_v1",
    "blocked_observations": "aegis_candidate_to_paper_lifecycle_v1",
    "missing_entry_marks": "aegis_entry_reference_price_certification_v1",
    "closed_outcomes": "aegis_outcome_registry_v1",
    "closed_today": "aegis_paper_outcome_auto_closure_v1",
    "included_samples": "aegis_validation_samples_v1",
    "manual_review_queue": "aegis_paper_outcome_auto_closure_v1",
    "david_action": "aegis_command_center_queue_audit_v1",
    "research_allocation_decisions": "aegis_research_capital_allocation_v1",
    "allocation_state": "aegis_research_capital_allocation_v1",
    "current_bottleneck": "aegis_statistical_sufficiency_v1",
    "last_successful_run": "aegis_paper_session_ledger_v1",
    "next_scheduled_run": "aegis_paper_session_ledger_v1",
}


def _payload() -> dict:
    return server._today_build_operator_envelope_v1(TRUTH, DAY, DAY)


def test_command_center_truth_audit_covers_every_visible_primary_field() -> None:
    payload = _payload()
    rows = {row["field_id"]: row for row in payload.get("command_center_truth_audit", [])}
    assert set(EXPECTED_FIELDS).issubset(rows), sorted(set(EXPECTED_FIELDS) - set(rows))
    for field_id, (label, expected_value) in EXPECTED_FIELDS.items():
        row = rows[field_id]
        assert row["label"] == label
        assert row["displayed_value"] == row["source_value"], row
        assert row["match"] is True, row
        assert row["source_path"], row
        if expected_value is not None:
            assert row["displayed_value"] == expected_value, row
        required_source = REQUIRED_SOURCES.get(field_id)
        if required_source:
            assert required_source in row["authoritative_report_source"], row


def test_command_center_primary_payload_uses_authoritative_current_day_counts() -> None:
    payload = _payload()
    activity = payload["activity"]["summary"]
    validation = payload["validation"]
    diagnostics = payload["candidate_generation_diagnostics"]
    lifecycle = diagnostics["candidate_lifecycle_counts"]
    assert payload["safety"]["mode_label"] == "PAPER MODE"
    assert activity["raw_signals"] == 21
    assert activity["valid_candidates"] == 19
    assert activity["paper_positions_opened"] == 19
    assert diagnostics["raw_signal_count"] == 21
    assert diagnostics["valid_candidate_contract_count"] == 21
    assert lifecycle["auto_promoted_to_paper_tracking_count"] == 21
    assert payload["open_positions"]["count"] == 53
    assert validation["open_outcome_count"] == 53
    assert validation["closed_outcome_count"] == 2
    assert validation["paper_outcomes_closed_today"] == 2
    assert validation["validation_sample_count"] == 2
    assert validation["manual_review_queue_count"] == 2
    assert validation["current_bottleneck"] == "Validation sample sufficiency / underpowered hypotheses"
    assert payload["next"]["label"] == "No future run confirmed"
    assert payload["next"]["scheduled_at"] == ""


def test_command_center_primary_payload_does_not_expose_stale_forbidden_values() -> None:
    payload_text = str(_payload())
    forbidden = [
        "Outcome closure / validation throughput",
        "0 raw signals",
        "0 valid candidates",
        "0 auto-promoted",
        "55 open observations",
        "0 closed outcomes",
        "0 included samples",
        "0 manual review queue",
    ]
    for fragment in forbidden:
        assert fragment not in payload_text
