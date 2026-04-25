from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import constellation_2.common.day_activation_authority_v1 as day_module
import constellation_2.common.economic_state_authority_v1 as economic_module
import constellation_2.common.execution_build_authority_v1 as execution_build_module
import constellation_2.common.global_context_authority_v1 as global_module
import constellation_2.common.control_plane_validation_kernel_v1 as kernel_module
from constellation_2.common.configuration_activation_authority_v1 import (
    run_configuration_activation_authority_v1,
)
from constellation_2.common.configuration_activation_family_validator_v1 import (
    validate_configuration_activation_family_v1,
)
from constellation_2.common.control_plane_stage_admission_v1 import (
    CONTROL_STAGE_CONTEXT_ADMITTED,
    CONTROL_STAGE_DAY_ADMITTED,
    ControlPlaneStageAdmissionError,
    build_control_stage_admitted_payload_v1,
    certify_startup_chain_v1,
    write_control_stage_admitted_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    validate_control_plane_boundary_v1,
    validate_day_activation_family_v1,
    validate_execution_build_family_v1,
    validate_global_context_family_v1,
    validate_session_authority_family_v1,
)
from constellation_2.common.session_authority_v1 import (
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    derive_target_day_build_payload_v1,
    write_active_session_v1,
    write_target_day_admission_v1,
    write_target_day_build_v1,
)
from constellation_2.common.session_promotion_gate_v1 import (
    derive_session_promotion_decision_payload_v1,
    write_session_promotion_decision_v1,
)
from constellation_2.common.tests.test_configuration_activation_authority_v1 import (
    _write_policy_snapshot,
)
from constellation_2.common.tests.test_execution_build_authority_v1 import (
    ACCOUNT,
    DAY,
    ENV,
    INTENT_HASH,
    SLEEVE,
    _candidate_roots,
    _patch_roots,
    _seal_global_context,
    _seed_candidate,
    _seed_complete_economic,
    _seed_execution_non_economic,
    _seed_raw_global_context,
)
from constellation_2.common.tests.test_session_authority_v1 import (
    SOURCE_ROOT,
    _artifact_row,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _seed_session_authority_success(truth_root: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        artifact_results=[
            _artifact_row(truth_root, "paper_policy_verdict_v1"),
            _artifact_row(truth_root, "trade_submit_readiness_c2_v1"),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=truth_root, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=truth_root,
        payload=derive_target_day_admission_payload_v1(
            truth_root=truth_root,
            target_day=DAY,
            build_ref=build_ref,
        ),
    )
    pre_open_payload = {
        "target_day": DAY,
        "materialization_state": "COMPLETE",
        "completion_state": "COMPLETE",
    }
    pre_open_path = (truth_root / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json").resolve()
    _write_json(pre_open_path, pre_open_payload)
    pre_open_ref = SimpleNamespace(path=pre_open_path, payload=pre_open_payload, sha256=_sha256_file(pre_open_path))
    promotion_payload = derive_session_promotion_decision_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        pre_open_bundle_ref=pre_open_ref,
        target_day_admission_ref=admission_ref,
        owner_tool="test_control_plane_bundle4_v2.py",
    )
    promotion_ref = write_session_promotion_decision_v1(
        truth_root=truth_root,
        payload=promotion_payload,
    )
    active_payload = derive_active_session_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        admission_ref=admission_ref,
        promotion_ref=promotion_ref,
    )
    write_active_session_v1(truth_root=truth_root, payload=active_payload)


def _seed_full_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id="A1001")
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    economic_module.run_economic_state_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type="fresh_paper_entry_v1",
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )
    _seed_session_authority_success(canonical_truth)
    execution_build_module.run_execution_build_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type="fresh_paper_entry_v1",
        candidate_path=candidate,
        materialize=False,
        emit_package=True,
    )
    return canonical_truth, sleeve_root, candidate


def test_boundary_validator_rejects_forbidden_repo_local_truth_root() -> None:
    report = validate_control_plane_boundary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=SOURCE_ROOT / "constellation_2" / "runtime" / "truth",
        truth_sleeves_root=SOURCE_ROOT / "constellation_2" / "runtime" / "truth_sleeves",
    )
    assert report["ok"] is False
    assert "CANONICAL_PATH_FORBIDDEN_ROOT" in report["errors"]


def test_day_activation_family_validator_passes_and_fails_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves"
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    day_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type="fresh_paper_entry_v1",
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )
    report = validate_day_activation_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is True

    package_path = Path(report["validated_family_refs"][1]["path"])
    package_path.unlink()
    failed = validate_day_activation_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert failed["ok"] is False
    assert "DAY_ACTIVATION_PACKAGE_V1_MISSING" in failed["errors"]


def test_global_context_family_validator_passes_and_fails_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves"
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()
    report = validate_global_context_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is True

    package_path = Path(report["validated_family_refs"][1]["path"])
    payload = json.loads(package_path.read_text(encoding="utf-8"))
    payload["day_activation_package_ref"]["path"] = str((sleeve_root / SLEEVE / ENV / "session_authority_status_v1" / "current.json").resolve())
    _write_json(package_path, payload)
    failed = validate_global_context_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert failed["ok"] is False
    assert "GLOBAL_CONTEXT_PACKAGE_DAY_STAGE_REF_MISMATCH" in failed["errors"]


def test_session_authority_family_validator_rejects_derived_surface_authority_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _ = _seed_full_chain(tmp_path, monkeypatch)
    active_payload = json.loads((canonical_truth / "active_session_v1" / "current.json").read_text(encoding="utf-8"))
    derived_path = (canonical_truth / "session_authority_status_v1" / "current.json").resolve()
    _write_json(derived_path, active_payload)
    monkeypatch.setattr(
        kernel_module,
        "resolve_active_session_path",
        lambda *, truth_root: derived_path,
    )

    report = validate_session_authority_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is False
    assert "ACTIVE_SESSION_V1_DERIVED_SURFACE_FORBIDDEN" in report["errors"]


def test_execution_build_family_validator_passes_and_fails_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, candidate = _seed_full_chain(tmp_path, monkeypatch)
    report = validate_execution_build_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is True

    package_path = Path(report["validated_family_refs"][1]["path"])
    payload = json.loads(package_path.read_text(encoding="utf-8"))
    payload["build_ref"]["path"] = str((canonical_truth / "reports" / "execution_build_v1" / DAY / "wrong" / "execution_build.v1.json").resolve())
    _write_json(package_path, payload)
    failed = validate_execution_build_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert failed["ok"] is False
    assert "EXECUTION_PACKAGE_BUILD_REF_MISMATCH" in failed["errors"]


def test_stage_admission_requires_upstream_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _ = _seed_full_chain(tmp_path, monkeypatch)
    day_report = validate_day_activation_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    context_report = validate_global_context_family_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    day_stage = write_control_stage_admitted_v1(
        artifact_id=CONTROL_STAGE_DAY_ADMITTED,
        stage_id="CONTROL_STAGE_DAY_ACTIVATION",
        family_report=day_report,
        canonical_truth_root=canonical_truth,
    )
    with pytest.raises(ControlPlaneStageAdmissionError):
        build_control_stage_admitted_payload_v1(
            artifact_id=CONTROL_STAGE_CONTEXT_ADMITTED,
            stage_id="CONTROL_STAGE_GLOBAL_CONTEXT",
            family_report=context_report,
            upstream_stage_ref=None,
        )

    payload = build_control_stage_admitted_payload_v1(
        artifact_id=CONTROL_STAGE_CONTEXT_ADMITTED,
        stage_id="CONTROL_STAGE_GLOBAL_CONTEXT",
        family_report=context_report,
        upstream_stage_ref=day_stage,
    )
    assert payload["upstream_stage_admission_ref"]["artifact_id"] == CONTROL_STAGE_DAY_ADMITTED


def test_startup_chain_certification_succeeds_and_emits_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, candidate = _seed_full_chain(tmp_path, monkeypatch)
    result = certify_startup_chain_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    assert result["ok"] is True
    assert len(result["stage_refs"]) == 4
    assert result["startup_chain_certification_payload"]["certification_status"] == "CERTIFIED"
    assert Path(result["startup_chain_certification_ref"]["artifact_path"]).exists()


def test_startup_chain_certification_succeeds_in_validator_only_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, candidate = _seed_full_chain(tmp_path, monkeypatch)
    result = certify_startup_chain_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    assert result["ok"] is True
    assert len(result["stage_payloads"]) == 4
    assert result["stage_refs"] == []
    assert result["stage_certification_payloads"] == []
    assert result["stage_certification_refs"] == []
    assert result["startup_chain_certification_payload"]["certification_status"] == "CERTIFIED"
    assert not Path(result["startup_chain_certification_ref"]["artifact_path"]).exists()


def test_startup_chain_certification_fails_closed_on_missing_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, candidate = _seed_full_chain(tmp_path, monkeypatch)
    context_hash = global_module.compute_global_context_hash_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        operation_type="fresh_paper_entry_v1",
    )
    missing_package = (
        sleeve_root / SLEEVE / ENV / "global_context_package_v1" / DAY / context_hash / "global_context_package.v1.json"
    ).resolve()
    missing_package.unlink()
    result = certify_startup_chain_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    assert result["ok"] is False
    assert result["startup_chain_certification_payload"]["certification_status"] == "FAILED"
    assert "GLOBAL_CONTEXT_PACKAGE_V1_MISSING" in result["startup_chain_certification_payload"]["blocking_codes"]


def test_startup_chain_certification_fails_closed_on_forbidden_root_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, candidate = _seed_full_chain(tmp_path, monkeypatch)
    result = certify_startup_chain_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=SOURCE_ROOT / "constellation_2" / "runtime" / "truth",
        truth_sleeves_root=SOURCE_ROOT / "constellation_2" / "runtime" / "truth_sleeves",
        emit_artifacts=False,
    )
    assert result["ok"] is False
    assert result["startup_chain_certification_payload"]["certification_status"] == "FAILED"
    assert any(code.endswith("FORBIDDEN_ROOT") for code in result["startup_chain_certification_payload"]["blocking_codes"])


def test_bundle3_configuration_activation_validator_still_passes(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T20:00:00Z",
        effective_at_utc="2026-04-18T20:00:00Z",
        logical_name="bundle4_regression_config",
        document_text='{"mode":"bundle4"}\n',
    )
    run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )
    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    assert report["ok"] is True
