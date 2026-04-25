from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from constellation_2.common.capability_state_v1 import write_capability_state_v1
from constellation_2.common.paper_policy_verdict_v1 import (
    derive_paper_policy_verdict_payload,
    read_capability_state_ref,
)
from constellation_2.common.paper_startup_authorization_convergence_v1 import (
    derive_paper_startup_authorization_convergence_payload_v1,
)
import ops.tools.run_paper_startup_authorization_convergence_v1 as convergence_module
from ops.tools.run_paper_startup_authorization_convergence_v1 import _artifact_result


DAY = "2026-04-10"
REPO_ROOT = Path("/home/node/constellation")


def _convergence_row(
    artifact_id: str,
    *,
    required: bool,
    ready: bool,
    observed_status: str,
    blocker_code: str = "",
) -> dict:
    return {
        "artifact_id": artifact_id,
        "required": required,
        "artifact_path": f"/tmp/{artifact_id}.json",
        "schema_id": artifact_id,
        "target_day_expected": DAY,
        "target_day_observed": DAY if ready or blocker_code else DAY,
        "observed_status": observed_status,
        "ready": ready,
        "reason_codes": [] if ready else ([blocker_code] if blocker_code else []),
        "blocker_code": blocker_code,
        "summary": blocker_code or observed_status,
    }


def _capability_row(capability_id: str, status: str, reason_codes: list[str] | None = None) -> dict:
    return {
        "capability_id": capability_id,
        "status": status,
        "kind": "SYNTHESIZED_SUMMARY",
        "reason_codes": list(reason_codes or []),
        "source_artifacts": [],
        "details": {},
    }


def _capability_payload() -> dict:
    return {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "ib_account": "DU1234567",
        "sleeve_id": "PRIMARY",
        "overall_status": "FAIL",
        "capabilities": [
            _capability_row("account_binding_valid", "PASS"),
            _capability_row("startup_materialization_ready", "PASS"),
            _capability_row("paper_trading_posture_ready", "PASS"),
            _capability_row("broker_connectivity_available", "PASS"),
            _capability_row("startup_authorization_gate_set_ready", "PASS"),
            _capability_row(
                "core_sleeve_gate_set_ready",
                "FAIL",
                ["operator_daily_gate_v3:FAIL"],
            ),
            _capability_row(
                "economic_health_gate_set_complete",
                "FAIL",
                ["economic_health_gate_verdict_v1:FAIL"],
            ),
            _capability_row(
                "production_certification_gate_set_complete",
                "FAIL",
                ["heartbeat_gate_v1:FAIL"],
            ),
        ],
        "source_artifacts": [],
        "registry_ref": {
            "artifact_path": str((REPO_ROOT / "governance/02_REGISTRIES/CAPABILITY_POLICY_REGISTRY_V1.json").resolve()),
            "artifact_sha256": "a" * 64,
            "schema_id": "capability_policy_registry",
            "schema_version": "v1",
        },
        "release_id": "TEST_RELEASE",
        "git_sha": "b" * 40,
        "generated_at_utc": f"{DAY}T00:00:00Z",
    }


def test_convergence_blocks_when_required_authorization_artifact_missing(tmp_path: Path) -> None:
    payload = derive_paper_startup_authorization_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        environment="PAPER",
        ib_account="DU1234567",
        sleeve_id="PRIMARY",
        sleeve_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        artifact_results=[
            _convergence_row("engine_daily_returns_v1", required=True, ready=True, observed_status="NOT_AVAILABLE"),
            _convergence_row(
                "engine_correlation_matrix_v1",
                required=True,
                ready=False,
                observed_status="MISSING",
                blocker_code="ENGINE_CORRELATION_MATRIX_V1_MISSING",
            ),
            _convergence_row(
                "authorization_gate_verdict_v1",
                required=True,
                ready=False,
                observed_status="FAIL",
                blocker_code="AUTHORIZATION_GATE_VERDICT_V1_NOT_READY",
            ),
        ],
        source_refs=[],
    )
    assert payload["binding_classification"] == "SUBSET_PROOF_ONLY"
    assert payload["convergence_status"] == "BLOCKED"
    assert payload["authorization_verdict_ready"] is False
    assert payload["blocker_chain"][0]["artifact_id"] == "engine_correlation_matrix_v1"


def test_convergence_blocks_when_bod_execution_substrate_proof_is_not_ready(tmp_path: Path) -> None:
    payload = derive_paper_startup_authorization_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        environment="PAPER",
        ib_account="DU1234567",
        sleeve_id="PRIMARY",
        sleeve_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        artifact_results=[
            _convergence_row(
                "bod_execution_environment_proof_v1",
                required=True,
                ready=False,
                observed_status="BLOCKED_BY_DEFECT",
                blocker_code="BOD_EXECUTION_ENVIRONMENT_PROOF_V1_NOT_READY",
            ),
            _convergence_row("authorization_gate_verdict_v1", required=True, ready=True, observed_status="PASS"),
        ],
        source_refs=[],
    )
    assert payload["convergence_status"] == "BLOCKED"
    assert payload["blocker_chain"][0]["artifact_id"] == "bod_execution_environment_proof_v1"


def test_convergence_ignores_optional_economic_failures_when_authorization_ready(tmp_path: Path) -> None:
    payload = derive_paper_startup_authorization_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        environment="PAPER",
        ib_account="DU1234567",
        sleeve_id="PRIMARY",
        sleeve_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        artifact_results=[
            _convergence_row("engine_daily_returns_v1", required=True, ready=True, observed_status="NOT_AVAILABLE"),
            _convergence_row(
                "engine_correlation_matrix_v1",
                required=True,
                ready=True,
                observed_status="DEGRADED_INSUFFICIENT_HISTORY",
            ),
            _convergence_row("correlation_envelope_gate_v1", required=True, ready=True, observed_status="PASS"),
            _convergence_row("feed_attestation_gate_v1", required=True, ready=True, observed_status="PASS"),
            _convergence_row("heartbeat_gate_v1", required=True, ready=True, observed_status="PASS"),
            _convergence_row("replay_certification_gate_v1", required=True, ready=True, observed_status="PASS"),
            _convergence_row("authorization_gate_verdict_v1", required=True, ready=True, observed_status="BOOTSTRAP_PASS"),
            _convergence_row(
                "economic_health_gate_verdict_v1",
                required=False,
                ready=False,
                observed_status="FAIL",
                blocker_code="ECONOMIC_HEALTH_GATE_VERDICT_V1_NOT_READY",
            ),
            _convergence_row(
                "gate_stack_verdict_v1",
                required=False,
                ready=False,
                observed_status="FAIL",
                blocker_code="GATE_STACK_VERDICT_V1_NOT_READY",
            ),
        ],
        source_refs=[],
    )
    assert payload["convergence_status"] == "SUCCESS"
    assert payload["authorization_verdict_ready"] is True
    assert payload["blocker_chain"] == []


def test_paper_policy_keeps_operator_daily_and_economic_failures_advisory_for_paper(tmp_path: Path) -> None:
    capability_ref = write_capability_state_v1(truth_root=tmp_path, payload=_capability_payload())
    reread_ref = read_capability_state_ref(truth_root=tmp_path, day_utc=DAY)
    payload = derive_paper_policy_verdict_payload(
        repo_root=REPO_ROOT,
        truth_root=tmp_path,
        capability_ref=reread_ref,
    )
    assert capability_ref.path == reread_ref.path
    assert payload["overall_status"] == "PASS"
    assert payload["paper_allowed"] is True
    advisory_ids = {item["capability_id"] for item in payload["advisory_items"]}
    assert "core_sleeve_gate_set_ready" in advisory_ids
    assert "economic_health_gate_set_complete" in advisory_ids
    assert "production_certification_gate_set_complete" in advisory_ids
    assert payload["blocking_items"] == []


def test_artifact_result_accepts_final_status_from_trading_day_intent_generation(tmp_path: Path) -> None:
    artifact_path = (
        tmp_path
        / "reports"
        / "trading_day_intent_generation_v1"
        / DAY
        / "trading_day_intent_generation.v1.json"
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        """{"schema_id":"trading_day_intent_generation","day_utc":"2026-04-10","final_status":"VALID_ZERO"}""",
        encoding="utf-8",
    )
    row = _artifact_result(
        artifact_id="trading_day_intent_generation_v1",
        artifact_path=artifact_path,
        target_day=DAY,
        required=True,
        acceptable_statuses=("INTENTS_PRESENT", "VALID_ZERO"),
    )
    assert row["observed_status"] == "VALID_ZERO"
    assert row["ready"] is True
    assert row["blocker_code"] == ""


def test_artifact_result_accepts_convergence_status_from_input_convergence(tmp_path: Path) -> None:
    artifact_path = (
        tmp_path
        / "reports"
        / "paper_startup_intent_input_convergence_v1"
        / DAY
        / "paper_startup_intent_input_convergence.v1.json"
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        """{"schema_id":"paper_startup_intent_input_convergence_v1","target_day":"2026-04-10","convergence_status":"SUCCESS"}""",
        encoding="utf-8",
    )
    row = _artifact_result(
        artifact_id="paper_startup_intent_input_convergence_v1",
        artifact_path=artifact_path,
        target_day=DAY,
        required=True,
        acceptable_statuses=("SUCCESS",),
    )
    assert row["observed_status"] == "SUCCESS"
    assert row["ready"] is True
    assert row["blocker_code"] == ""


def test_materialize_authorization_verdict_uses_canonical_writer(monkeypatch, tmp_path: Path) -> None:
    calls: dict[str, object] = {}

    def _fake_writer(*, repo_root: Path, truth_root: Path, day_utc: str, produced_utc: str, mode: str):
        calls["repo_root"] = repo_root
        calls["truth_root"] = truth_root
        calls["day_utc"] = day_utc
        calls["produced_utc"] = produced_utc
        calls["mode"] = mode
        return {
            "lifecycle_state": SimpleNamespace(action="WROTE"),
            "authorization_verdict": SimpleNamespace(action="WROTE"),
            "economic_verdict": SimpleNamespace(action="WROTE"),
            "ledger": SimpleNamespace(action="WROTE"),
        }

    monkeypatch.setattr(convergence_module, "write_gate_authority_plane", _fake_writer)
    result = convergence_module._materialize_authorization_verdict(
        day_utc=DAY,
        produced_utc=f"{DAY}T00:00:00Z",
        mode="PAPER",
        sleeve_truth_root=(tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve(),
    )
    assert result["return_code"] == 0
    assert "authorization_verdict_action" in result["stdout"]
    assert calls["day_utc"] == DAY
    assert calls["mode"] == "PAPER"
