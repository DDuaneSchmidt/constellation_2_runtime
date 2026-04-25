from __future__ import annotations

from pathlib import Path

from constellation_2.common.operator_summary_v1 import build_operator_summary
from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1


def test_operator_summary_is_explicitly_legacy_derived_only(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-14"
    state_machine_path = tmp_path / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json"
    payload = {
        "state_machine_id": f"tdsm:{day_utc}",
        "final_start_decision": "READY_NOW",
        "first_true_blocker": {"first_true_blocker_code": "", "first_true_blocker_artifact_path": ""},
        "supporting_session_authority": {
            "paper_session_ledger_path": str(tmp_path / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
            "ledger_id": f"ledger:{day_utc}",
            "ledger_authority_status": "GRANTED",
            "submission_authorized": True,
        },
        "blocking_codes": [],
        "supporting_regeneration_results": [
            {
                "logical_name": "startup_proof_validation_v1",
                "status": "STARTUP_READY",
                "path": str(tmp_path / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"),
            }
        ],
    }

    monkeypatch.setattr(
        "constellation_2.common.operator_summary_v1.read_trading_day_state_machine_ref_v1",
        lambda **kwargs: SurfaceRefV1(path=state_machine_path, payload=payload, sha256="a" * 64),
    )

    summary = build_operator_summary(summary_kind="preopen", day_utc=day_utc, truth_root=tmp_path)

    assert summary["binding_classification"] == "LEGACY_DERIVED_ONLY"
    assert summary["authority_scope"] == "DERIVED_ONLY_VIEW"
