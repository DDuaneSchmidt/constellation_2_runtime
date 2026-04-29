from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui_api.readiness_kernel_v1 import build_readiness_kernel_v1


DAY = "2026-04-29"


def _write_json(root: Path, relative: str, payload: dict) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _seed_current_sources(truth_root: Path, sleeve_truth_root: Path) -> None:
    _write_json(truth_root, f"target_day_build_v1/{DAY}.json", {"day_utc": DAY, "status": "PASS"})
    _write_json(
        truth_root,
        f"target_day_admission_v1/{DAY}.json",
        {"day_utc": DAY, "target_day_admission_status": "ADMIT"},
    )
    _write_json(
        truth_root,
        f"reports/market_open_data_gate_v1/{DAY}/market_open_data_gate.v1.json",
        {"day_utc": DAY, "status": "PASS"},
    )
    _write_json(
        truth_root,
        f"reports/structure_decision_supply_v1/{DAY}/structure_decision_supply.v1.json",
        {"day_utc": DAY, "status": "PASS"},
    )
    _write_json(
        sleeve_truth_root,
        f"phaseC_preflight_v1/{DAY}/attempt_A0001/identity/execution_identity_record.v1.json",
        {"day_utc": DAY, "status": "PASS"},
    )
    _write_json(
        truth_root,
        f"reports/authorization_supply_v1/{DAY}/authorization_supply.v1.json",
        {"day_utc": DAY, "status": "PASS"},
    )
    _write_json(
        truth_root,
        f"reports/risk_sizing_authority_v1/{DAY}/risk_sizing_authority.v1.json",
        {"day_utc": DAY, "status": "PASS", "risk_sizing_state": "SIZED"},
    )
    _write_json(
        truth_root,
        f"reports/submit_boundary_status_v1/{DAY}/submit_boundary_status.v1.json",
        {"day_utc": DAY, "status": "READY", "boundary_status": "AUTHORIZED"},
    )
    _write_json(
        truth_root,
        f"reports/aegis_day_run_v1/{DAY}/day_run.v1.json",
        {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""},
    )
    _write_json(
        truth_root,
        f"reports/execution_mode_authority_v1/{DAY}/execution_mode_authority.v1.json",
        {"day_utc": DAY, "status": "PASS", "mode_state": "PAPER_TRANSMIT_ENABLED"},
    )


def _layer(payload: dict, layer_id: str) -> dict:
    return next(layer for layer in payload["layers"] if layer["layer_id"] == layer_id)


def test_boundary_reads_submit_boundary_status_v1(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    boundary = _layer(payload, "SUBMIT_BOUNDARY")
    assert boundary["status"] == "AUTHORIZED"
    assert boundary["classification"] == "CURRENT"
    assert boundary["source_path"].endswith("submit_boundary_status.v1.json")


def test_ledger_reads_day_run_and_market_closed_is_out_of_session(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)
    _write_json(
        truth_root,
        f"reports/aegis_day_run_v1/{DAY}/day_run.v1.json",
        {"day_utc": DAY, "final_status": "NOT_READY", "canonical_blocker": "MARKET_CLOSED"},
    )

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    ledger = _layer(payload, "LEDGER")
    assert ledger["classification"] == "OUT_OF_SESSION"
    assert payload["overall_status"] == "OUT_OF_SESSION"
    assert payload["canonical_blocker"] == "MARKET_CLOSED"


def test_control_reads_execution_mode_authority_v1(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    control = _layer(payload, "CONTROL")
    assert control["status"] == "PAPER_TRANSMIT_ENABLED"
    assert control["classification"] == "CURRENT"


def test_missing_direct_source_is_missing_evidence_unknown(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)
    (truth_root / f"reports/submit_boundary_status_v1/{DAY}/submit_boundary_status.v1.json").unlink()

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    boundary = _layer(payload, "SUBMIT_BOUNDARY")
    assert boundary["classification"] == "MISSING_EVIDENCE"
    assert boundary["status"] == "UNKNOWN"


def test_stale_direct_source_is_stale_artifact(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)
    _write_json(
        truth_root,
        f"reports/submit_boundary_status_v1/{DAY}/submit_boundary_status.v1.json",
        {"day_utc": DAY, "status": "READY", "boundary_status": "AUTHORIZED", "freshness_verdict": "STALE"},
    )

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    boundary = _layer(payload, "SUBMIT_BOUNDARY")
    assert boundary["classification"] == "STALE_ARTIFACT"
    assert payload["overall_status"] == "BLOCKED"


def test_incomplete_session_authority_status_does_not_drive_ladder_unknown(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)
    _write_json(truth_root, "session_authority_status_v1/current.json", {"canonical_readiness_authority": {}})

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    assert _layer(payload, "SUBMIT_BOUNDARY")["status"] == "AUTHORIZED"
    assert _layer(payload, "LEDGER")["status"] == "PAPER_READY"
    assert _layer(payload, "CONTROL")["status"] == "PAPER_TRANSMIT_ENABLED"
    assert {warning["code"] for warning in payload["warnings"]} == {"LEGACY_SESSION_STATUS_INCOMPLETE"}


def test_aggregation_mismatch_is_warning_not_ladder_truth(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_current_sources(truth_root, sleeve_root)
    _write_json(
        truth_root,
        "session_authority_status_v1/current.json",
        {
            "canonical_readiness_authority": {
                "details": {
                    "canonical_surfaces": [
                        {"artifact_type": "submit_boundary_status_v1", "status_value": "UNKNOWN"},
                    ]
                }
            }
        },
    )

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    assert _layer(payload, "SUBMIT_BOUNDARY")["status"] == "AUTHORIZED"
    assert any(warning["code"] == "AGGREGATION_MISMATCH" for warning in payload["warnings"])


def test_server_and_ui_are_wired_to_readiness_kernel() -> None:
    repo = Path(__file__).resolve().parents[4]
    server = (repo / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    client = (
        repo / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
    ).read_text(encoding="utf-8")
    pages = (repo / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")

    assert 'path == "/api/readiness-kernel"' in server
    assert "build_readiness_kernel_v1" in server
    assert "fetchReadinessKernel" in client
    assert 'query("/api/readiness-kernel"' in client
    assert "fetchReadinessKernel()" in pages
    assert "renderReadinessKernelLadder(readinessKernel, state)" in pages
    assert "safeList(operations.readiness_ladder)" not in pages

