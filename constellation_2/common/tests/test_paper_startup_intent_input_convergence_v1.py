from __future__ import annotations

from pathlib import Path

from constellation_2.common.paper_startup_intent_input_convergence_v1 import (
    derive_paper_startup_intent_input_convergence_payload_v1,
)


DAY = "2026-04-14"


def _row(artifact_id: str, *, ready: bool, observed_status: str, blocker_code: str = "") -> dict:
    return {
        "artifact_id": artifact_id,
        "required": True,
        "artifact_path": f"/tmp/{artifact_id}.json",
        "schema_id": artifact_id,
        "target_day_expected": DAY,
        "target_day_observed": DAY if ready or blocker_code else "",
        "observed_status": observed_status,
        "ready": ready,
        "reason_codes": [blocker_code] if blocker_code else [],
        "blocker_code": blocker_code,
        "summary": blocker_code or observed_status,
    }


def test_intent_input_convergence_blocks_when_required_input_missing(tmp_path: Path) -> None:
    payload = derive_paper_startup_intent_input_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        sleeve_id="PRIMARY",
        environment="PAPER",
        ib_account="DU1234567",
        sleeve_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        required_inputs=["positions_snapshot_v1", "regime_snapshot_v2"],
        artifact_results=[
            _row("positions_snapshot_v1", ready=True, observed_status="OK"),
            _row(
                "regime_snapshot_v2",
                ready=False,
                observed_status="MISSING",
                blocker_code="REGIME_SNAPSHOT_V2_MISSING",
            ),
        ],
        source_refs=[],
    )
    assert payload["convergence_status"] == "BLOCKED"
    assert payload["blocker_chain"][0]["artifact_id"] == "regime_snapshot_v2"


def test_intent_input_convergence_succeeds_when_required_inputs_present(tmp_path: Path) -> None:
    payload = derive_paper_startup_intent_input_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        sleeve_id="PRIMARY",
        environment="PAPER",
        ib_account="DU1234567",
        sleeve_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        required_inputs=[
            "positions_snapshot_v1",
            "regime_snapshot_v2",
            "positions_snapshot_v2",
            "accounting_nav_snapshot_v1",
            "market_data_snapshot_v1",
        ],
        artifact_results=[
            _row("positions_snapshot_v1", ready=True, observed_status="OK"),
            _row("regime_snapshot_v2", ready=True, observed_status="OK"),
            _row("positions_snapshot_v2", ready=True, observed_status="OK"),
            _row("accounting_nav_snapshot_v1", ready=True, observed_status="OK"),
            _row("market_data_snapshot_v1", ready=True, observed_status="OK"),
        ],
        source_refs=[],
    )
    assert payload["convergence_status"] == "SUCCESS"
    assert payload["blocker_chain"] == []
