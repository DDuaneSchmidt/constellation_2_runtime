from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops.tools import aegis_artifact_ledger_v1 as ledger
from ops.tools import run_candidate_to_production_promotion_v1 as promotion
from ops.tools import run_aegis_control_plane_v1 as cp


DAY = "2026-04-29"
COMMIT = "a" * 40


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _artifact(path: Path, *, truth: Path, runtime: Path, runtime_mode: str = "PRODUCTION", day: str = DAY) -> dict:
    payload = {
        "schema_id": "test_artifact",
        "schema_version": "v1",
        "day_utc": day,
        "truth_root": str(truth.resolve()),
        "runtime_root": str(runtime.resolve()),
        "runtime_mode": runtime_mode,
        "status": "PASS",
        "producer_contract_v1": {
            "producer_name": "ops/tools/test_producer_v1.py",
            "code_version_git_commit": COMMIT,
            "source_dirty_status": "CLEAN",
            "generated_at_utc": f"{day}T14:00:00Z",
            "input_artifacts": [],
            "output_artifacts": [{"path": str(path.resolve())}],
        },
    }
    _write(path, payload)
    return payload


@pytest.fixture(autouse=True)
def _stable_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ledger, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(promotion, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(cp, "_current_git_commit_v1", lambda: COMMIT)


def test_ledger_missing_record_and_hash_mismatch_are_rejected(tmp_path: Path) -> None:
    truth = tmp_path / "production_truth"
    runtime = tmp_path
    path = truth / "reports" / "x_v1" / DAY / "x.json"
    _artifact(path, truth=truth, runtime=runtime)

    issues = ledger.verify_artifact_ledger_record_v1(
        artifact_path=path,
        artifact_type="x_v1",
        truth_root=truth,
        runtime_root=runtime,
        runtime_mode="PRODUCTION",
        day=DAY,
    )
    assert {row["code"] for row in issues} == {"ARTIFACT_LEDGER_RECORD_MISSING"}

    ledger.write_artifact_ledger_record_v1(
        artifact_path=path,
        artifact_type="x_v1",
        truth_root=truth,
        runtime_root=runtime,
        runtime_mode="PRODUCTION",
        day=DAY,
        artifact_id="x:1",
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["status"] = "MUTATED"
    _write(path, payload)
    issues = ledger.verify_artifact_ledger_record_v1(
        artifact_path=path,
        artifact_type="x_v1",
        truth_root=truth,
        runtime_root=runtime,
        runtime_mode="PRODUCTION",
        day=DAY,
    )
    assert "ARTIFACT_LEDGER_ARTIFACT_HASH_MISMATCH" in {row["code"] for row in issues}


def test_ledger_rejects_duplicate_artifact_id_with_different_hash(tmp_path: Path) -> None:
    truth = tmp_path / "production_truth"
    runtime = tmp_path
    path = truth / "reports" / "x_v1" / DAY / "x.json"
    _artifact(path, truth=truth, runtime=runtime)
    _ledger_path, record = ledger.write_artifact_ledger_record_v1(
        artifact_path=path,
        artifact_type="x_v1",
        truth_root=truth,
        runtime_root=runtime,
        runtime_mode="PRODUCTION",
        day=DAY,
        artifact_id="same-id",
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["status"] = "DIFFERENT"
    _write(path, payload)
    changed = ledger.build_artifact_ledger_record_v1(
        artifact_path=path,
        artifact_type="x_v1",
        truth_root=truth,
        runtime_root=runtime,
        runtime_mode="PRODUCTION",
        day=DAY,
        artifact_id=record["artifact_id"],
    )
    with pytest.raises(ValueError, match="DUPLICATE_ARTIFACT_ID_DIFFERENT_HASH"):
        ledger.append_artifact_ledger_record_v1(record=changed, ledger_path=ledger.ledger_path_v1(truth_root=truth, day=DAY))


def test_ledger_rejects_wrong_day_truth_root_runtime_mode_and_missing_metadata(tmp_path: Path) -> None:
    truth = tmp_path / "production_truth"
    runtime = tmp_path
    path = truth / "reports" / "x_v1" / DAY / "x.json"
    _artifact(path, truth=truth, runtime=runtime)
    with pytest.raises(ValueError, match="DAY_MISMATCH"):
        ledger.build_artifact_ledger_record_v1(artifact_path=path, artifact_type="x_v1", truth_root=truth, runtime_root=runtime, runtime_mode="PRODUCTION", day="2026-04-30")
    with pytest.raises(ValueError, match="TRUTH_ROOT_MISMATCH"):
        ledger.build_artifact_ledger_record_v1(artifact_path=path, artifact_type="x_v1", truth_root=tmp_path / "other_truth", runtime_root=runtime, runtime_mode="PRODUCTION", day=DAY)
    with pytest.raises(ValueError, match="RUNTIME_MODE_MISMATCH"):
        ledger.build_artifact_ledger_record_v1(artifact_path=path, artifact_type="x_v1", truth_root=truth, runtime_root=runtime, runtime_mode="CANDIDATE", day=DAY)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("producer_contract_v1")
    _write(path, payload)
    with pytest.raises(ValueError, match="PRODUCER_METADATA_MISSING"):
        ledger.build_artifact_ledger_record_v1(artifact_path=path, artifact_type="x_v1", truth_root=truth, runtime_root=runtime, runtime_mode="PRODUCTION", day=DAY)


def test_production_control_plane_acceptance_requires_ledger_and_promotion(tmp_path: Path) -> None:
    truth = tmp_path / "production_truth"
    runtime = tmp_path
    path = truth / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json"
    payload = _artifact(path, truth=truth, runtime=runtime)
    payload.update(
        {
            "schema_id": "aegis_control_plane",
            "schema_version": "aegis_control_plane.v1",
            "artifact_path": str(path.resolve()),
            "actual_artifact_path": str(path.resolve()),
            "producer_contract_output_artifact_path": str(path.resolve()),
            "final_status": "NOT_READY",
            "submit_allowed": False,
            "canonical_blocker": "TEST",
        }
    )
    _write(path, payload)
    codes = {row["code"] for row in cp.control_plane_acceptance_issues_v1(payload, actual_path=path)}
    assert "ARTIFACT_LEDGER_RECORD_MISSING" in codes

    _ledger_path, record = ledger.write_artifact_ledger_record_v1(
        artifact_path=path,
        artifact_type="aegis_control_plane_v1",
        truth_root=truth,
        runtime_root=runtime,
        runtime_mode="PRODUCTION",
        day=DAY,
    )
    codes = {row["code"] for row in cp.control_plane_acceptance_issues_v1(payload, actual_path=path)}
    assert "PROMOTION_ATTESTATION_MISSING" in codes

    attestation_path = ledger.promotion_attestation_path_v1(truth_root=truth, day=DAY, artifact_id=record["artifact_id"])
    _write(
        attestation_path,
        {
            "schema_id": "aegis_promotion_attestation",
            "schema_version": "aegis_promotion_attestation.v1",
            "attestation_id": "test",
            "artifact_id": record["artifact_id"],
            "artifact_type": "aegis_control_plane_v1",
            "source_candidate_artifact_path": str(path.resolve()),
            "source_candidate_artifact_hash": record["artifact_hash"],
            "source_ledger_record_ref": record,
            "destination_production_path": str(path.resolve()),
            "destination_production_hash": record["artifact_hash"],
            "promotion_policy_version": "test",
            "producer_module": "ops/tools/run_candidate_to_production_promotion_v1.py",
            "producer_git_commit": COMMIT,
            "promoted_at": f"{DAY}T14:00:00Z",
            "validation_status": "PASS",
            "blockers": [],
        },
    )
    assert cp.control_plane_acceptance_issues_v1(payload, actual_path=path) == []


def test_promotion_rejects_missing_ledger_wrong_paths_runtime_and_hash(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate_truth"
    production = tmp_path / "production_truth"
    runtime = tmp_path
    source = candidate / "reports" / "x_v1" / DAY / "x.json"
    destination = production / "reports" / "x_v1" / DAY / "x.json"
    _artifact(source, truth=candidate, runtime=runtime, runtime_mode="CANDIDATE")

    with pytest.raises(RuntimeError, match="PROMOTION_SOURCE_LEDGER_RECORD_MISSING"):
        promotion.promote_candidate_to_production_v1(
            source_candidate_artifact_path=source,
            destination_production_path=destination,
            candidate_truth_root=candidate,
            production_truth_root=production,
            day_utc=DAY,
            artifact_type="x_v1",
            schema_relpath="",
        )

    ledger.write_artifact_ledger_record_v1(
        artifact_path=source,
        artifact_type="x_v1",
        truth_root=candidate,
        runtime_root=runtime,
        runtime_mode="CANDIDATE",
        day=DAY,
    )
    with pytest.raises(RuntimeError, match="PROMOTION_DESTINATION_NOT_UNDER_PRODUCTION_TRUTH"):
        promotion.promote_candidate_to_production_v1(
            source_candidate_artifact_path=source,
            destination_production_path=tmp_path / "other" / "x.json",
            candidate_truth_root=candidate,
            production_truth_root=production,
            day_utc=DAY,
            artifact_type="x_v1",
            schema_relpath="",
        )

    copied = production / "copied_candidate.json"
    _artifact(copied, truth=production, runtime=runtime, runtime_mode="CANDIDATE")
    with pytest.raises(RuntimeError, match="PROMOTION_SOURCE_NOT_UNDER_CANDIDATE_TRUTH"):
        promotion.promote_candidate_to_production_v1(
            source_candidate_artifact_path=copied,
            destination_production_path=destination,
            candidate_truth_root=candidate,
            production_truth_root=production,
            day_utc=DAY,
            artifact_type="x_v1",
            schema_relpath="",
        )

    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["status"] = "MUTATED_AFTER_LEDGER"
    _write(source, payload)
    with pytest.raises(RuntimeError, match="ARTIFACT_LEDGER_ARTIFACT_HASH_MISMATCH"):
        promotion.promote_candidate_to_production_v1(
            source_candidate_artifact_path=source,
            destination_production_path=destination,
            candidate_truth_root=candidate,
            production_truth_root=production,
            day_utc=DAY,
            artifact_type="x_v1",
            schema_relpath="",
        )


def test_promotion_rejects_stale_source_producer_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = tmp_path / "candidate_truth"
    production = tmp_path / "production_truth"
    runtime = tmp_path
    source = candidate / "reports" / "x_v1" / DAY / "x.json"
    destination = production / "reports" / "x_v1" / DAY / "x.json"
    _artifact(source, truth=candidate, runtime=runtime, runtime_mode="CANDIDATE")
    ledger.write_artifact_ledger_record_v1(
        artifact_path=source,
        artifact_type="x_v1",
        truth_root=candidate,
        runtime_root=runtime,
        runtime_mode="CANDIDATE",
        day=DAY,
    )
    monkeypatch.setattr(promotion, "git_commit_v1", lambda: "b" * 40)
    with pytest.raises(RuntimeError, match="PROMOTION_SOURCE_PRODUCER_GIT_MISMATCH"):
        promotion.promote_candidate_to_production_v1(
            source_candidate_artifact_path=source,
            destination_production_path=destination,
            candidate_truth_root=candidate,
            production_truth_root=production,
            day_utc=DAY,
            artifact_type="x_v1",
            schema_relpath="",
        )
