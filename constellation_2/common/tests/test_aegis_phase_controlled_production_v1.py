from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import ops.tools.aegis_runtime_mode_v1 as runtime_mode
import ops.tools.aegis_submit_enforcement_v1 as enforcement
import ops.tools.promote_aegis_candidate_to_production_v1 as promote
import ops.tools.rollback_aegis_production_v1 as rollback
import ops.tools.run_aegis_production_promotion_gate_v1 as gate
import ops.tools.run_aegis_promotion_candidate_v1 as candidate
import ops.tools.run_aegis_promotion_validation_ledger_v1 as validation


DAY = "2026-05-04"
COMMIT = "c" * 40
PREV_COMMIT = "b" * 40


def _write(path: Path, payload: dict | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(root: Path, family: str, filename: str, payload: dict) -> None:
    _write(root / "reports" / family / DAY / filename, payload)


def _packet(root: Path, commit: str = COMMIT, mode: str = "CANDIDATE") -> None:
    _write(
        root / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md",
        "\n".join(
            [
                "# Aegis Packet",
                "- generated_at_utc: 2026-05-04T13:00:00Z",
                f"- runtime_mode: {mode}",
                f"- git_commit: {commit}",
                "- git_dirty_status: CLEAN",
            ]
        ),
    )


def _seed_roots(tmp_path: Path) -> tuple[Path, Path]:
    prod = tmp_path / "production_truth"
    cand = tmp_path / "candidate_truth"
    _write(
        prod / "governance" / "production_version.v1.json",
        {
            "schema_version": "production_version.v1",
            "promoted_commit": PREV_COMMIT,
            "promoted_at_utc": "2026-05-03T13:00:00Z",
            "promoted_by": "test",
            "promotion_id": "prev",
            "rollback_commit": "a" * 40,
            "status": "ACTIVE",
        },
    )
    for root, status, blocker, submit in (
        (prod, "PAPER_READY", "", False),
        (cand, "PAPER_READY", "", False),
    ):
        _report(root, "aegis_day_run_v1", "day_run.v1.json", {"final_status": status, "canonical_blocker": blocker})
        _report(
            root,
            "aegis_control_plane_v1",
            "control_plane.v1.json",
            {
                "day_utc": DAY,
                "final_status": "READY",
                "canonical_blocker": "",
                "submit_allowed": False,
                "runtime_mode": "PRODUCTION" if root == prod else "CANDIDATE",
            },
        )
        _report(root, "submit_boundary_status_v1", "submit_boundary_status.v1.json", {"status": "PASS", "submit_allowed": submit})
        _report(root, "action_validity_v1", "action_validity.v1.json", {"action_rules": [{"action_id": "submit_paper_order", "status": "ALLOWED" if submit else "FORBIDDEN"}]})
        _report(root, "truth_freshness_v1", "truth_freshness.v1.json", {"status": "PASS"})
        _report(root, "state_consistency_v1", "state_consistency.v1.json", {"status": "PASS"})
        _report(root, "aegis_requirement_graph_v1", "requirement_graph.v1.json", {"status": "PASS"})
        _report(root, "aegis_operator_projection_v1", "operator_projection.v1.json", {"status": "PASS", "evidence_paths": [str(root / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json")]})
    _packet(cand, mode="CANDIDATE")
    _packet(prod, commit=PREV_COMMIT, mode="PRODUCTION")
    return prod, cand


@pytest.fixture(autouse=True)
def _stable_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    monkeypatch.setattr(candidate, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(gate, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(validation, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(validation, "git_dirty_status_v1", lambda: "CLEAN")
    monkeypatch.setattr(promote, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(rollback, "git_commit_v1", lambda: COMMIT)


def test_candidate_runtime_cannot_write_production_truth() -> None:
    with pytest.raises(SystemExit, match="CANDIDATE_RUNTIME_CANNOT_WRITE_PRODUCTION_TRUTH"):
        runtime_mode.assert_candidate_cannot_write_production_v1(
            runtime_mode="CANDIDATE",
            output_path=Path("/tmp/production_truth/reports/aegis_control_plane_v1/2026-05-04/control_plane.v1.json"),
        )


def test_promotion_candidate_blocks_regression_stale_packet_and_submit_expansion(tmp_path: Path) -> None:
    prod, cand = _seed_roots(tmp_path)
    payload = candidate.build_promotion_candidate_v1(day_utc=DAY, candidate_root=cand, production_root=prod, tests_run=["focused tests passed"])
    assert payload["promotion_recommendation"] == "APPROVE"
    assert payload["regression_detected"] is False

    _report(cand, "aegis_day_run_v1", "day_run.v1.json", {"final_status": "NOT_READY", "canonical_blocker": "SESSION_AUTHORITY_MISSING"})
    payload = candidate.build_promotion_candidate_v1(day_utc=DAY, candidate_root=cand, production_root=prod, tests_run=["focused tests passed"])
    assert payload["regression_detected"] is True
    assert any(row["code"] == "READINESS_REGRESSION" for row in payload["blockers"])

    _report(cand, "aegis_day_run_v1", "day_run.v1.json", {"final_status": "PAPER_READY", "canonical_blocker": ""})
    _packet(cand, commit="d" * 40, mode="CANDIDATE")
    payload = candidate.build_promotion_candidate_v1(day_utc=DAY, candidate_root=cand, production_root=prod, tests_run=["focused tests passed"])
    assert any(row["code"] == "CANDIDATE_PACKET_NOT_CURRENT" for row in payload["blockers"])

    _packet(cand, mode="CANDIDATE")
    _report(cand, "submit_boundary_status_v1", "submit_boundary_status.v1.json", {"status": "PASS", "submit_allowed": True})
    _report(cand, "action_validity_v1", "action_validity.v1.json", {"action_rules": [{"action_id": "submit_paper_order", "status": "ALLOWED"}]})
    payload = candidate.build_promotion_candidate_v1(day_utc=DAY, candidate_root=cand, production_root=prod, tests_run=["focused tests passed"])
    assert any(row["code"] == "SUBMIT_PERMISSION_EXPANSION_REQUIRES_APPROVAL" for row in payload["blockers"])


def test_promotion_gate_requires_approval_tests_packet_and_no_regression(tmp_path: Path) -> None:
    prod, cand = _seed_roots(tmp_path)
    _, candidate_payload = candidate.run_promotion_candidate_v1(DAY, str(cand), str(prod), ["focused tests passed"])
    validation.run_promotion_validation_ledger_v1(day_utc=DAY, truth_root=str(cand), runtime_root=str(cand), focused_tests_passed=True, focused_test_details=["focused tests passed"])
    assert candidate_payload["promotion_recommendation"] == "APPROVE"

    _, payload = gate.run_promotion_gate_v1(DAY, "promo-1", str(cand), str(prod))
    assert payload["promotion_status"] == "BLOCKED"
    assert any(row["code"] == "HUMAN_APPROVAL_MISSING" for row in payload["blockers"])

    _write(
        gate.approval_path(cand, "promo-1"),
        {
            "promotion_id": "promo-1",
            "candidate_commit": COMMIT,
            "approved_by": "human",
            "approved_at_utc": "2026-05-04T14:00:00Z",
            "approval_scope": "production promotion",
            "rollback_commit": PREV_COMMIT,
            "risk_acknowledgement": "acknowledged",
            "status": "APPROVED",
        },
    )
    _, payload = gate.run_promotion_gate_v1(DAY, "promo-1", str(cand), str(prod))
    assert payload["promotion_status"] == "APPROVED_FOR_PROMOTION"

    _write(candidate.promotion_candidate_path(candidate_root=cand, day_utc=DAY), {**candidate_payload, "tests_run": ["1 failed"]})
    _, payload = gate.run_promotion_gate_v1(DAY, "promo-1", str(cand), str(prod))
    assert any(row["code"] == "CANDIDATE_TESTS_NOT_PROVEN_PASS" for row in payload["blockers"])

    _write(candidate.promotion_candidate_path(candidate_root=cand, day_utc=DAY), {**candidate_payload, "regression_detected": True})
    _, payload = gate.run_promotion_gate_v1(DAY, "promo-1", str(cand), str(prod))
    assert any(row["code"] == "READINESS_REGRESSION" for row in payload["blockers"])


def test_promotion_activation_updates_version_copies_manifest_and_regenerates_packet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prod, cand = _seed_roots(tmp_path)
    _, candidate_payload = candidate.run_promotion_candidate_v1(DAY, str(cand), str(prod), ["focused tests passed"])
    validation.run_promotion_validation_ledger_v1(day_utc=DAY, truth_root=str(cand), runtime_root=str(cand), focused_tests_passed=True, focused_test_details=["focused tests passed"])
    _write(
        gate.approval_path(cand, "promo-2"),
        {
            "promotion_id": "promo-2",
            "candidate_commit": COMMIT,
            "rollback_commit": PREV_COMMIT,
            "status": "APPROVED",
        },
    )
    _, gate_payload = gate.run_promotion_gate_v1(DAY, "promo-2", str(cand), str(prod))
    assert gate_payload["promotion_status"] == "APPROVED_FOR_PROMOTION"
    def _packet_run(*args: object, **kwargs: object) -> SimpleNamespace:
        env = kwargs.get("env") if isinstance(kwargs.get("env"), dict) else {}
        root = Path(str(env.get("AEGIS_PACKET_ROOT")))
        _packet(root, commit=COMMIT, mode="PRODUCTION")
        return SimpleNamespace(returncode=0, stdout="packet ok", stderr="")

    monkeypatch.setattr(promote.subprocess, "run", _packet_run)

    manifest = promote.promote_candidate_to_production_v1(day_utc=DAY, promotion_id="promo-2", candidate_root=cand, production_root=prod, promoted_by="human")
    version = runtime_mode.read_json_v1(prod / "governance" / "production_version.v1.json")
    assert version["promoted_commit"] == COMMIT
    assert version["promotion_id"] == "promo-2"
    assert manifest["status"] == "PROMOTED"
    assert any(row["status"] == "COPIED" for row in manifest["copied_artifacts"])
    assert candidate_payload["candidate_commit"] == COMMIT


def test_validation_ledger_and_gate_block_dirty_repo_failed_tests_schema_and_control_plane(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prod, cand = _seed_roots(tmp_path)
    monkeypatch.setattr(validation, "git_dirty_status_v1", lambda: "DIRTY")
    _path, payload = validation.run_promotion_validation_ledger_v1(day_utc=DAY, truth_root=str(cand), runtime_root=str(cand), focused_tests_passed=True)
    assert any(row["code"] == "REPO_DIRTY" for row in payload["blockers"])

    monkeypatch.setattr(validation, "git_dirty_status_v1", lambda: "CLEAN")
    _path, payload = validation.run_promotion_validation_ledger_v1(day_utc=DAY, truth_root=str(cand), runtime_root=str(cand), focused_tests_passed=False)
    assert any(row["code"] == "FOCUSED_TESTS_NOT_PROVEN_PASS" for row in payload["blockers"])

    original_registry_validation = validation._validate_registry_contract_v1
    monkeypatch.setattr(
        validation,
        "_validate_registry_contract_v1",
        lambda: ({"status": "FAIL", "blockers": [{"code": "REGISTRY_DEPENDENCY_FIELD_MISSING"}]}, {"status": "FAIL", "schema_checked": 0}),
    )
    _path, payload = validation.run_promotion_validation_ledger_v1(day_utc=DAY, truth_root=str(cand), runtime_root=str(cand), focused_tests_passed=True)
    assert any(row["code"] == "REGISTRY_VALIDATION_FAILED" for row in payload["blockers"])
    assert any(row["code"] == "SCHEMA_VALIDATION_FAILED" for row in payload["blockers"])
    monkeypatch.setattr(validation, "_validate_registry_contract_v1", original_registry_validation)

    (cand / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json").unlink()
    _path, payload = validation.run_promotion_validation_ledger_v1(day_utc=DAY, truth_root=str(cand), runtime_root=str(cand), focused_tests_passed=True)
    assert any(row["code"] == "CONTROL_PLANE_EVALUATION_FAILED" for row in payload["blockers"])

    candidate.run_promotion_candidate_v1(DAY, str(cand), str(prod), ["focused tests passed"])
    _write(
        gate.approval_path(cand, "promo-invalid"),
        {"promotion_id": "promo-invalid", "candidate_commit": COMMIT, "rollback_commit": PREV_COMMIT, "status": "APPROVED"},
    )
    _gate_path, gate_payload = gate.run_promotion_gate_v1(DAY, "promo-invalid", str(cand), str(prod))
    assert any(row["code"] == "PROMOTION_VALIDATION_LEDGER_BLOCKER" for row in gate_payload["blockers"])


def test_rollback_restores_previous_commit_and_marks_prior_promotion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prod, _cand = _seed_roots(tmp_path)
    _write(
        prod / "governance" / "production_version.v1.json",
        {
            "schema_version": "production_version.v1",
            "promoted_commit": COMMIT,
            "promotion_id": "promo-3",
            "rollback_commit": PREV_COMMIT,
            "status": "ACTIVE",
        },
    )
    _write(promote.promotion_manifest_path(prod, "promo-3"), {"promotion_id": "promo-3", "status": "PROMOTED"})
    def _packet_run(*args: object, **kwargs: object) -> SimpleNamespace:
        env = kwargs.get("env") if isinstance(kwargs.get("env"), dict) else {}
        root = Path(str(env.get("AEGIS_PACKET_ROOT")))
        _packet(root, commit=COMMIT, mode="PRODUCTION")
        return SimpleNamespace(returncode=0, stdout="packet ok", stderr="")

    monkeypatch.setattr(rollback.subprocess, "run", _packet_run)

    manifest = rollback.rollback_aegis_production_v1(promotion_id="promo-3", production_root=prod, rolled_back_by="human")
    version = runtime_mode.read_json_v1(prod / "governance" / "production_version.v1.json")
    prior_manifest = runtime_mode.read_json_v1(promote.promotion_manifest_path(prod, "promo-3"))
    assert version["promoted_commit"] == PREV_COMMIT
    assert version["rollback_commit"] == COMMIT
    assert prior_manifest["status"] == "ROLLED_BACK"
    assert manifest["status"] == "ROLLED_BACK"
