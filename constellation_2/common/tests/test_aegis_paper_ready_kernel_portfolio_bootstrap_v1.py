from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_paper_ready_kernel_v1 as kernel


DAY = "2026-05-12"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _portfolio_stage() -> kernel.KernelStage:
    return kernel._stage(
        "portfolio_activation_gate",
        "portfolio",
        ["python3", "ops/tools/run_portfolio_activation_gate_v1.py"],
        "PAPER_SLEEVE",
        Path("portfolio_activation_gate.v1.json"),
        ("PASS", "ALLOW", "OK", "READY", "DEGRADED"),
        "python3 ops/tools/run_portfolio_activation_gate_v1.py",
        "Refresh portfolio activation gate.",
    )


def _bootstrap_payload(**overrides: object) -> dict:
    payload = {
        "day_utc": DAY,
        "environment": "PAPER",
        "status": "BOOTSTRAP_ACCEPTED_FOR_PAPER",
        "canonical_blocker": "",
        "approved_executable_intents": [
            {
                "intent_id": "c2_trend_eq_spy_2026-05-12_v1",
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "symbol": "SPY",
                "allowed_by_portfolio_gate": True,
                "portfolio_gate_decision": "ALLOW",
                "reason_codes": ["PORTFOLIO_STATE_BOOTSTRAP_ACCEPTED_FOR_PAPER_NO_SUPPRESSION_APPLIED"],
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_paper_kernel_accepts_portfolio_bootstrap_status_only_with_paper_evidence(tmp_path: Path) -> None:
    artifact = tmp_path / "portfolio_activation_gate.v1.json"
    _write_json(artifact, _bootstrap_payload())

    result = kernel._validate_stage_artifact(stage=_portfolio_stage(), artifact_path=artifact, target_day=DAY)

    assert result["status"] == "PASS"
    assert result["artifact_status"] == "BOOTSTRAP_ACCEPTED_FOR_PAPER"
    assert result["paper_bootstrap_accepted"] is True


def test_portfolio_bootstrap_status_is_not_accepted_for_live_or_realtime_artifacts(tmp_path: Path) -> None:
    artifact = tmp_path / "portfolio_activation_gate.v1.json"
    _write_json(artifact, _bootstrap_payload(environment="LIVE"))

    result = kernel._validate_stage_artifact(stage=_portfolio_stage(), artifact_path=artifact, target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_NOT_PAPER"
    assert result["failed_field"] == "environment"


def test_portfolio_bootstrap_status_requires_empty_canonical_blocker(tmp_path: Path) -> None:
    artifact = tmp_path / "portfolio_activation_gate.v1.json"
    _write_json(artifact, _bootstrap_payload(canonical_blocker="REGIME_BUCKET_SUPPRESSED"))

    result = kernel._validate_stage_artifact(stage=_portfolio_stage(), artifact_path=artifact, target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_HAS_BLOCKER"


def test_portfolio_bootstrap_status_requires_allow_evidence(tmp_path: Path) -> None:
    artifact = tmp_path / "portfolio_activation_gate.v1.json"
    _write_json(
        artifact,
        _bootstrap_payload(
            approved_executable_intents=[
                {
                    "intent_id": "c2_trend_eq_spy_2026-05-12_v1",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "symbol": "SPY",
                    "allowed_by_portfolio_gate": False,
                    "portfolio_gate_decision": "BLOCK",
                    "reason_codes": ["PORTFOLIO_STATE_BOOTSTRAP_ACCEPTED_FOR_PAPER_NO_SUPPRESSION_APPLIED"],
                }
            ]
        ),
    )

    result = kernel._validate_stage_artifact(stage=_portfolio_stage(), artifact_path=artifact, target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_INVALID"
    assert result["failed_field"] == "approved_executable_intents[0].allowed_by_portfolio_gate"


def test_portfolio_bootstrap_status_requires_bootstrap_reason(tmp_path: Path) -> None:
    artifact = tmp_path / "portfolio_activation_gate.v1.json"
    _write_json(
        artifact,
        _bootstrap_payload(
            approved_executable_intents=[
                {
                    "intent_id": "c2_trend_eq_spy_2026-05-12_v1",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "symbol": "SPY",
                    "allowed_by_portfolio_gate": True,
                    "portfolio_gate_decision": "ALLOW",
                    "reason_codes": ["OTHER_REASON"],
                }
            ]
        ),
    )

    result = kernel._validate_stage_artifact(stage=_portfolio_stage(), artifact_path=artifact, target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_INVALID"
    assert result["failed_field"] == "approved_executable_intents[0].reason_codes"
