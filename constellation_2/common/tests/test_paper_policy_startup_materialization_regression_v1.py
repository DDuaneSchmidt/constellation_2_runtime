from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.paper_policy_verdict_v1 import derive_paper_policy_verdict_payload
from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1


REPO_ROOT = Path("/home/node/constellation")
DAY = "2026-04-14"


def test_paper_policy_fails_closed_when_startup_materialization_capability_fails(tmp_path: Path) -> None:
    capability_path = tmp_path / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
    capability_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "ib_account": "DUO847203",
        "sleeve_id": "PRIMARY",
        "overall_status": "FAIL",
        "capabilities": [
            {
                "capability_id": "account_binding_valid",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_materialization_ready",
                "status": "FAIL",
                "reason_codes": [
                    "STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"
                ],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "paper_trading_posture_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "broker_connectivity_available",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_authorization_gate_set_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            }
        ],
        "source_artifacts": [],
        "registry_ref": {"artifact_sha256": "a" * 64},
        "release_id": "TEST_RELEASE",
        "git_sha": "b" * 40,
        "generated_at_utc": f"{DAY}T00:00:00Z",
    }
    capability_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    capability_ref = SurfaceRefV1(path=capability_path, payload=payload, sha256="c" * 64)

    verdict = derive_paper_policy_verdict_payload(repo_root=REPO_ROOT, truth_root=tmp_path, capability_ref=capability_ref)

    assert verdict["overall_status"] == "FAIL"
    assert verdict["paper_allowed"] is False
    assert verdict["blocking_items"][0]["capability_id"] == "startup_materialization_ready"


def test_paper_policy_tolerates_bootstrap_startup_materialization_gap_for_paper(tmp_path: Path) -> None:
    capability_path = tmp_path / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
    capability_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "ib_account": "DUO847203",
        "sleeve_id": "PRIMARY",
        "overall_status": "FAIL",
        "capabilities": [
            {
                "capability_id": "account_binding_valid",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_materialization_ready",
                "status": "FAIL",
                "reason_codes": [
                    "STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS",
                    "STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING",
                ],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "paper_trading_posture_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "broker_connectivity_available",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_authorization_gate_set_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
        ],
        "source_artifacts": [],
        "registry_ref": {"artifact_sha256": "a" * 64},
        "release_id": "TEST_RELEASE",
        "git_sha": "b" * 40,
        "generated_at_utc": f"{DAY}T00:00:00Z",
    }
    capability_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    capability_ref = SurfaceRefV1(path=capability_path, payload=payload, sha256="c" * 64)

    verdict = derive_paper_policy_verdict_payload(repo_root=REPO_ROOT, truth_root=tmp_path, capability_ref=capability_ref)

    assert verdict["overall_status"] == "PASS"
    assert verdict["paper_allowed"] is True
    assert not verdict["blocking_items"]
    assert verdict["advisory_items"][0]["capability_id"] == "startup_materialization_ready"
    assert "PAPER_BOOTSTRAP_TOLERATED:STARTUP_MATERIALIZATION_NO_ACTIVE_ATTEMPT" in verdict["advisory_items"][0]["reason_codes"]
    assert verdict["production_only_open_items"][0]["capability_id"] == "startup_materialization_ready"


def test_live_policy_does_not_tolerate_bootstrap_startup_materialization_gap(tmp_path: Path) -> None:
    capability_path = tmp_path / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
    capability_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "LIVE",
        "ib_account": "DUO847203",
        "sleeve_id": "PRIMARY",
        "overall_status": "FAIL",
        "capabilities": [
            {
                "capability_id": "account_binding_valid",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_materialization_ready",
                "status": "FAIL",
                "reason_codes": [
                    "STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS",
                    "STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING",
                ],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "paper_trading_posture_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "broker_connectivity_available",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_authorization_gate_set_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
        ],
        "source_artifacts": [],
        "registry_ref": {"artifact_sha256": "a" * 64},
        "release_id": "TEST_RELEASE",
        "git_sha": "b" * 40,
        "generated_at_utc": f"{DAY}T00:00:00Z",
    }
    capability_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    capability_ref = SurfaceRefV1(path=capability_path, payload=payload, sha256="c" * 64)

    verdict = derive_paper_policy_verdict_payload(repo_root=REPO_ROOT, truth_root=tmp_path, capability_ref=capability_ref)

    assert verdict["overall_status"] == "FAIL"
    assert verdict["paper_allowed"] is False
    assert verdict["blocking_items"][0]["capability_id"] == "startup_materialization_ready"


def test_paper_policy_tolerates_options_snapshot_pending_startup_materialization_veto(tmp_path: Path) -> None:
    day_utc = DAY
    startup_path = tmp_path / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"
    attempt_dir = tmp_path / "phaseC_preflight_v1" / day_utc / "attempt_A0001"
    pointer_path = tmp_path / "phaseC_preflight_v1" / day_utc / "latest_active_attempt.v1.json"
    veto_path = attempt_dir / "intent.veto_record.v1.json"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    startup_path.parent.mkdir(parents=True, exist_ok=True)
    pointer_path.write_text(
        json.dumps(
            {
                "schema_id": "phasec_latest_active_attempt",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_dir": str(attempt_dir.resolve()),
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    veto_path.write_text(
        json.dumps(
            {
                "schema_id": "veto_record",
                "schema_version": "v1",
                "reason_code": "C2_SUBMIT_FAIL_CLOSED_REQUIRED",
                "reason_detail": f"OPTIONS_SNAPSHOT_ROOT_MISSING: {tmp_path / 'options_chain_snapshot_v1' / day_utc}",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    startup_path.write_text(
        json.dumps(
            {
                "schema_id": "startup_materialization",
                "schema_version": "v1",
                "day_utc": day_utc,
                "status": "FAIL",
                "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
                "path_resolution_evidence": {
                    "latest_active_attempt_path": str(pointer_path.resolve()),
                },
                "phasec_materializer_result": {
                    "stdout": f"BLOCKED: intent_hash=test path={veto_path.resolve()}",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    capability_path = tmp_path / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
    capability_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "ib_account": "DUO847203",
        "sleeve_id": "PRIMARY",
        "overall_status": "FAIL",
        "capabilities": [
            {
                "capability_id": "account_binding_valid",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_materialization_ready",
                "status": "FAIL",
                "reason_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [
                    {
                        "artifact_family": "startup_materialization_v1",
                        "artifact_path": str(startup_path.resolve()),
                        "artifact_sha256": "d" * 64,
                    }
                ],
            },
            {
                "capability_id": "paper_trading_posture_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "broker_connectivity_available",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_authorization_gate_set_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
        ],
        "source_artifacts": [],
        "registry_ref": {"artifact_sha256": "a" * 64},
        "release_id": "TEST_RELEASE",
        "git_sha": "b" * 40,
        "generated_at_utc": f"{DAY}T00:00:00Z",
    }
    capability_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    capability_ref = SurfaceRefV1(path=capability_path, payload=payload, sha256="c" * 64)

    verdict = derive_paper_policy_verdict_payload(repo_root=REPO_ROOT, truth_root=tmp_path, capability_ref=capability_ref)

    assert verdict["overall_status"] == "PASS"
    assert verdict["paper_allowed"] is True
    assert not verdict["blocking_items"]
    assert verdict["advisory_items"][0]["capability_id"] == "startup_materialization_ready"
    assert "PAPER_START_ALLOWED_OPTIONS_SNAPSHOT_PENDING" in verdict["advisory_items"][0]["reason_codes"]


def test_paper_policy_does_not_tolerate_non_options_phasec_veto(tmp_path: Path) -> None:
    day_utc = DAY
    startup_path = tmp_path / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"
    attempt_dir = tmp_path / "phaseC_preflight_v1" / day_utc / "attempt_A0001"
    pointer_path = tmp_path / "phaseC_preflight_v1" / day_utc / "latest_active_attempt.v1.json"
    veto_path = attempt_dir / "intent.veto_record.v1.json"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    startup_path.parent.mkdir(parents=True, exist_ok=True)
    pointer_path.write_text(
        json.dumps(
            {
                "schema_id": "phasec_latest_active_attempt",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_dir": str(attempt_dir.resolve()),
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    veto_path.write_text(
        json.dumps(
            {
                "schema_id": "veto_record",
                "schema_version": "v1",
                "reason_code": "C2_SUBMIT_FAIL_CLOSED_REQUIRED",
                "reason_detail": "MISSING_STRATEGY_INPUT_ROOT",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    startup_path.write_text(
        json.dumps(
            {
                "schema_id": "startup_materialization",
                "schema_version": "v1",
                "day_utc": day_utc,
                "status": "FAIL",
                "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
                "path_resolution_evidence": {
                    "latest_active_attempt_path": str(pointer_path.resolve()),
                },
                "phasec_materializer_result": {
                    "stdout": f"BLOCKED: intent_hash=test path={veto_path.resolve()}",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    capability_path = tmp_path / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
    capability_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "ib_account": "DUO847203",
        "sleeve_id": "PRIMARY",
        "overall_status": "FAIL",
        "capabilities": [
            {
                "capability_id": "account_binding_valid",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_materialization_ready",
                "status": "FAIL",
                "reason_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [
                    {
                        "artifact_family": "startup_materialization_v1",
                        "artifact_path": str(startup_path.resolve()),
                        "artifact_sha256": "d" * 64,
                    }
                ],
            },
            {
                "capability_id": "paper_trading_posture_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "broker_connectivity_available",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
            {
                "capability_id": "startup_authorization_gate_set_ready",
                "status": "PASS",
                "reason_codes": [],
                "details": {},
                "kind": "SYNTHESIZED_SUMMARY",
                "source_artifacts": [],
            },
        ],
        "source_artifacts": [],
        "registry_ref": {"artifact_sha256": "a" * 64},
        "release_id": "TEST_RELEASE",
        "git_sha": "b" * 40,
        "generated_at_utc": f"{DAY}T00:00:00Z",
    }
    capability_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    capability_ref = SurfaceRefV1(path=capability_path, payload=payload, sha256="c" * 64)

    verdict = derive_paper_policy_verdict_payload(repo_root=REPO_ROOT, truth_root=tmp_path, capability_ref=capability_ref)

    assert verdict["overall_status"] == "FAIL"
    assert verdict["paper_allowed"] is False
    assert verdict["blocking_items"][0]["capability_id"] == "startup_materialization_ready"
