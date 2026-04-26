from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.tools import run_paper_day_readiness_proof_v1 as proof_module
from constellation_2.phaseD.tools import c2_submit_paper_v5 as submit_tool


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _install_phasec_fixture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    fixture_root = (tmp_path / "_smoketest_phasec_2026_04_02").resolve()
    fixture_root.mkdir(parents=True, exist_ok=True)
    _write_json(
        fixture_root / "equity_order_plan.v2.json",
        {
            "plan_id": "phasec-fixture-plan-v1",
            "source_intent_id": "proof_intent_seed_v1",
            "intent_hash": "a" * 64,
            "intent_sha256": "a" * 64,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "symbol": "SPY",
            "currency": "USD",
            "qty_shares": 1,
            "action": "BUY",
            "order_terms": {"order_type": "LIMIT", "limit_price": "1.23", "time_in_force": "DAY"},
        },
    )
    monkeypatch.setattr(proof_module, "PHASEC_FIXTURE", fixture_root)
    return fixture_root


def _write_submit_pair_fixture(
    *,
    tmp_path: Path,
    day: str,
    closure_status: str,
    dependency_detail: str,
) -> dict[str, Path | str]:
    pair_root = (tmp_path / "submit_pair").resolve()
    pair_root.mkdir(parents=True, exist_ok=True)
    build_path = (pair_root / "execution_build.v1.json").resolve()
    _write_json(
        build_path,
        {
            "schema_id": "execution_build",
            "schema_version": "v1",
            "closure_status": closure_status,
            "dependency_results": [
                {
                    "dependency_id": "capital_authority_allocation_v1",
                    "status": "BLOCKED",
                    "detail": dependency_detail,
                }
            ],
        },
    )
    return {
        "execution_build_path": build_path,
        "execution_package_path": (pair_root / "execution_package.v1.json").resolve(),
        "submission_record_path": (pair_root / "submission_record.v1.json").resolve(),
        "submission_id": "a" * 64,
    }


def _write_submission_record_fixture(
    *,
    path: Path,
    package_path: Path,
    package_sha: str,
    submission_id: str,
) -> None:
    _write_json(
        path,
        {
            "status": "READY_TO_SUBMIT",
            "submission_id": submission_id,
            "execution_package_ref": {
                "path": str(package_path.resolve()),
                "sha256": package_sha,
            },
        },
    )


def _call_submit(
    *,
    truth_root: Path,
    dry_run: str,
    eval_time_utc: str,
    execution_package_path: Path,
    submission_record_path: Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    call_env = os.environ.copy()
    call_env["C2_TRUTH_ROOT"] = str(truth_root)
    if env:
        call_env.update(env)
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py"),
            "--eval_time_utc",
            eval_time_utc,
            "--execution_package_path",
            str(execution_package_path.resolve()),
            "--submission_record_path",
            str(submission_record_path.resolve()),
            "--risk_budget",
            str(ROOT / "constellation_2" / "phaseD" / "inputs" / "sample_risk_budget.v1.json"),
            "--ib_host",
            "127.0.0.1",
            "--ib_port",
            "4002",
            "--ib_client_id",
            "7",
            "--ib_account",
            "DUO847203",
            "--dry_run",
            dry_run,
            "--submissions_root_override",
            str((truth_root / "execution_evidence_v1" / "submissions").resolve()),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=call_env,
    )


def test_broker_boundary_fails_closed_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_phasec_fixture(monkeypatch, tmp_path)
    day = proof_module.DEFAULT_DAY
    produced_utc = proof_module.DEFAULT_PRODUCED_UTC
    truth_root = Path("/tmp/constellation_2_foundation/paper_day_readiness_test_submit_v1") / "truth_sleeves" / "PRIMARY" / "PAPER"
    shutil.rmtree(truth_root.parents[2], ignore_errors=True)
    seed = proof_module._seed_submit_prerequisites(
        truth_root=truth_root,
        day=day,
        produced_utc=produced_utc,
        ib_account="DUO847203",
    )
    pair = _write_submit_pair_fixture(
        tmp_path=tmp_path,
        day=day,
        closure_status="BLOCKED",
        dependency_detail="READINESS_EXECUTION_BUILD_NOT_COMPLETE",
    )
    monkeypatch.setattr(proof_module, "_resolve_existing_submit_pair_for_day", lambda **kwargs: pair)
    with pytest.raises(SystemExit, match="READINESS_EXECUTION_BUILD_NOT_COMPLETE"):
        proof_module._materialize_submit_interface_inputs(
            truth_root=truth_root,
            day=day,
            produced_utc=produced_utc,
            ib_account="DUO847203",
            seed=seed,
        )


def test_paper_day_readiness_selects_canonical_capital_lineage_and_reveals_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_phasec_fixture(monkeypatch, tmp_path)
    day = proof_module.DEFAULT_DAY
    produced_utc = proof_module.DEFAULT_PRODUCED_UTC
    truth_root = Path("/tmp/constellation_2_foundation/paper_day_readiness_test_submit_v2") / "truth_sleeves" / "PRIMARY" / "PAPER"
    shutil.rmtree(truth_root.parents[2], ignore_errors=True)
    seed = proof_module._seed_submit_prerequisites(
        truth_root=truth_root,
        day=day,
        produced_utc=produced_utc,
        ib_account="DUO847203",
    )
    pair = _write_submit_pair_fixture(
        tmp_path=tmp_path,
        day=day,
        closure_status="BLOCKED",
        dependency_detail="CAPITAL_AUTHORITY_NOT_AUTHORIZED",
    )
    monkeypatch.setattr(proof_module, "_resolve_existing_submit_pair_for_day", lambda **kwargs: pair)
    with pytest.raises(SystemExit, match="CAPITAL_AUTHORITY_NOT_AUTHORIZED"):
        proof_module._materialize_submit_interface_inputs(
            truth_root=truth_root,
            day=day,
            produced_utc=produced_utc,
            ib_account="DUO847203",
            seed=seed,
        )


def test_submit_live_enablement_contract_remains_explicit_and_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("C2_ENABLE_BROKER_TRANSMIT", raising=False)
    with pytest.raises(SystemExit, match="C2_ENABLE_BROKER_TRANSMIT=YES"):
        submit_tool._require_broker_transmit_enabled(dry_run="NO")
    submit_tool._require_broker_transmit_enabled(dry_run="YES")
    monkeypatch.setenv("C2_ENABLE_BROKER_TRANSMIT", "YES")
    submit_tool._require_broker_transmit_enabled(dry_run="NO")


def test_submit_tool_keeps_paths_when_execution_build_constitutional_payload_is_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_path = tmp_path / "execution_package.v1.json"
    build_path = tmp_path / "execution_build.v1.json"
    candidate_path = tmp_path / "candidate"
    candidate_path.mkdir(parents=True, exist_ok=True)
    submission_path = tmp_path / "submission_record.v1.json"

    _write_json(build_path, {"schema_id": "execution_build", "schema_version": "v1"})
    _write_json(
        package_path,
        {
            "schema_id": "execution_package",
            "schema_version": "v1",
            "operation_type": "fresh_paper_entry_v1",
            "day_utc": "2026-04-24",
            "submission_id": "a" * 64,
            "canonical_json_hash": "b" * 64,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "build_ref": {"path": str(build_path.resolve())},
            "candidate_ref": {
                "phasec_out_dir": str(candidate_path.resolve()),
                "execution_truth_root": str((tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()),
            },
        },
    )
    _write_submission_record_fixture(
        path=submission_path,
        package_path=package_path,
        package_sha="b" * 64,
        submission_id="a" * 64,
    )

    monkeypatch.setattr(submit_tool, "validate_governed_artifact_payload_v1", lambda **kwargs: {})

    def _should_not_refresh(**kwargs):  # noqa: ANN003
        raise AssertionError("unexpected refresh")

    monkeypatch.setattr(submit_tool, "run_execution_build_authority_v1", _should_not_refresh)

    refreshed_package_path, refreshed_submission_path = submit_tool._refresh_submit_paths_if_constitutional_dependency_stale(
        repo_root=ROOT,
        eval_time_utc="2026-04-25T00:10:00Z",
        execution_package_path=package_path,
        submission_record_path=submission_path,
    )
    assert refreshed_package_path == package_path.resolve()
    assert refreshed_submission_path == submission_path.resolve()


def test_submit_tool_refreshes_submission_bridge_when_execution_build_dependency_sha_is_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_path = tmp_path / "execution_package.v1.json"
    build_path = tmp_path / "execution_build.v1.json"
    candidate_path = tmp_path / "candidate"
    candidate_path.mkdir(parents=True, exist_ok=True)
    submission_path = tmp_path / "submission_record.v1.json"
    refreshed_package_path = tmp_path / "refreshed" / "execution_package.v1.json"

    _write_json(build_path, {"schema_id": "execution_build", "schema_version": "v1"})
    _write_json(
        package_path,
        {
            "schema_id": "execution_package",
            "schema_version": "v1",
            "operation_type": "fresh_paper_entry_v1",
            "day_utc": "2026-04-24",
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "submission_id": "a" * 64,
            "canonical_json_hash": "a" * 64,
            "build_ref": {"path": str(build_path.resolve())},
            "candidate_ref": {
                "phasec_out_dir": str(candidate_path.resolve()),
                "canonical_truth_root": str((tmp_path / "truth").resolve()),
                "execution_truth_root": str((tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()),
            },
        },
    )
    _write_json(
        refreshed_package_path,
        {
            "schema_id": "execution_package",
            "schema_version": "v1",
            "day_utc": "2026-04-24",
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "submission_id": "a" * 64,
            "canonical_json_hash": "b" * 64,
            "candidate_ref": {
                "execution_truth_root": str((tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve())
            },
        },
    )

    def _raise_stale(**kwargs):  # noqa: ANN003
        raise submit_tool.ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_DEPENDENCY_REF_SHA256_MISMATCH:artifact_id=execution_build_v1"
        )

    monkeypatch.setattr(submit_tool, "validate_governed_artifact_payload_v1", _raise_stale)
    monkeypatch.setattr(
        submit_tool,
        "run_execution_build_authority_v1",
        lambda **kwargs: {"package_path": str(refreshed_package_path.resolve())},
    )
    monkeypatch.setattr(submit_tool, "_refresh_trade_submit_readiness_for_submit", lambda **kwargs: None)

    class _FakeSubmissionRecord:
        def to_dict(self) -> dict:
            return {
                "schema_id": "execution_submission_record",
                "schema_version": "v1",
                "record_id": "a" * 64,
                "submission_record_id": "a" * 64,
                "execution_intent_id": "exec_intent_v1",
                "promotion_record_id": "promo_v1",
                "household_id": "PAPER:PRIMARY",
                "day_utc": "2026-04-24",
                "produced_utc": "2026-04-25T00:10:00Z",
                "contract_version": "execution_submission_record_contract_v1",
                "builder_version": "execution_submission_record_package_bridge_v1",
                "submission_id": "a" * 64,
                "trade_instance_id": None,
                "idempotency_key": "c" * 64,
                "status": "READY_TO_SUBMIT",
                "candidate_ref": {"path": str(candidate_path.resolve()), "sha256": "d" * 64},
                "downstream_payload_ref": {"path": str(candidate_path.resolve()), "sha256": "e" * 64},
                "execution_build_ref": {"path": str(build_path.resolve()), "sha256": "f" * 64},
                "execution_package_ref": {"path": str(refreshed_package_path.resolve()), "sha256": "b" * 64},
                "input_record_refs": [],
                "parent_lineage_refs": [],
                "source_artifact_refs": [],
                "canonical_json_hash": "0" * 64,
            }

    monkeypatch.setattr(
        submit_tool,
        "build_execution_submission_record_from_execution_package_v1",
        lambda **kwargs: _FakeSubmissionRecord(),
    )

    out_package_path, out_submission_path = submit_tool._refresh_submit_paths_if_constitutional_dependency_stale(
        repo_root=ROOT,
        eval_time_utc="2026-04-25T00:10:00Z",
        execution_package_path=package_path,
        submission_record_path=submission_path,
    )
    assert out_package_path == refreshed_package_path.resolve()
    assert out_submission_path.exists()
    assert out_submission_path.name.endswith(".submission_record.v1.json")
    assert "_submit_refresh_v1" in str(out_submission_path)
    bridge_obj = json.loads(out_submission_path.read_text(encoding="utf-8"))
    assert bridge_obj["execution_package_ref"]["path"] == str(refreshed_package_path.resolve())


def test_submit_tool_disables_boundary_readiness_refresh_after_submit_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_path = tmp_path / "execution_package.v1.json"
    submission_path = tmp_path / "submission_record.v1.json"
    package_path.write_text("{}\n", encoding="utf-8")
    submission_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(
        submit_tool,
        "_refresh_submit_paths_if_constitutional_dependency_stale",
        lambda **kwargs: (package_path.resolve(), submission_path.resolve()),
    )

    captured: dict = {}

    def _fake_boundary(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(submit_tool, "run_submit_boundary_paper_v4", _fake_boundary)
    monkeypatch.setattr(sys, "argv", [
        "c2_submit_paper_v5.py",
        "--eval_time_utc",
        "2026-04-25T00:10:00Z",
        "--execution_package_path",
        str(package_path.resolve()),
        "--submission_record_path",
        str(submission_path.resolve()),
        "--ib_host",
        "127.0.0.1",
        "--ib_port",
        "4002",
        "--ib_client_id",
        "7",
        "--ib_account",
        "DUO847203",
        "--dry_run",
        "YES",
    ])

    assert submit_tool.main() == 0
    assert captured["refresh_trade_submit_readiness"] is False


def test_submit_tool_refreshes_readiness_before_execution_build_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_path = tmp_path / "execution_package.v1.json"
    build_path = tmp_path / "execution_build.v1.json"
    candidate_path = tmp_path / "candidate"
    candidate_path.mkdir(parents=True, exist_ok=True)
    submission_path = tmp_path / "submission_record.v1.json"
    refreshed_package_path = tmp_path / "refreshed" / "execution_package.v1.json"
    _write_json(build_path, {"schema_id": "execution_build", "schema_version": "v1"})
    _write_json(
        package_path,
        {
            "schema_id": "execution_package",
            "schema_version": "v1",
            "operation_type": "fresh_paper_entry_v1",
            "day_utc": "2026-04-24",
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "submission_id": "a" * 64,
            "canonical_json_hash": "a" * 64,
            "build_ref": {"path": str(build_path.resolve())},
            "candidate_ref": {
                "phasec_out_dir": str(candidate_path.resolve()),
                "canonical_truth_root": str((tmp_path / "truth").resolve()),
                "execution_truth_root": str((tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()),
            },
        },
    )
    _write_json(
        refreshed_package_path,
        {
            "schema_id": "execution_package",
            "schema_version": "v1",
            "day_utc": "2026-04-24",
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "submission_id": "a" * 64,
            "canonical_json_hash": "b" * 64,
            "candidate_ref": {
                "execution_truth_root": str((tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve())
            },
        },
    )

    def _raise_stale(**kwargs):  # noqa: ANN003
        raise submit_tool.ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_DEPENDENCY_REF_SHA256_MISMATCH:artifact_id=execution_build_v1"
        )

    monkeypatch.setattr(submit_tool, "validate_governed_artifact_payload_v1", _raise_stale)
    call_order: list[str] = []
    monkeypatch.setattr(
        submit_tool,
        "_refresh_trade_submit_readiness_for_submit",
        lambda **kwargs: call_order.append("readiness"),
    )

    def _refresh_build(**kwargs):  # noqa: ANN003
        call_order.append("build")
        return {"package_path": str(refreshed_package_path.resolve())}

    monkeypatch.setattr(submit_tool, "run_execution_build_authority_v1", _refresh_build)

    class _FakeSubmissionRecord:
        def to_dict(self) -> dict:
            return {
                "schema_id": "execution_submission_record",
                "schema_version": "v1",
                "record_id": "a" * 64,
                "submission_record_id": "a" * 64,
                "execution_intent_id": "exec_intent_v1",
                "promotion_record_id": "promo_v1",
                "household_id": "PAPER:PRIMARY",
                "day_utc": "2026-04-24",
                "produced_utc": "2026-04-25T00:10:00Z",
                "contract_version": "execution_submission_record_contract_v1",
                "builder_version": "execution_submission_record_package_bridge_v1",
                "submission_id": "a" * 64,
                "trade_instance_id": None,
                "idempotency_key": "c" * 64,
                "status": "READY_TO_SUBMIT",
                "candidate_ref": {"path": str(candidate_path.resolve()), "sha256": "d" * 64},
                "downstream_payload_ref": {"path": str(candidate_path.resolve()), "sha256": "e" * 64},
                "execution_build_ref": {"path": str(build_path.resolve()), "sha256": "f" * 64},
                "execution_package_ref": {"path": str(refreshed_package_path.resolve()), "sha256": "b" * 64},
                "input_record_refs": [],
                "parent_lineage_refs": [],
                "source_artifact_refs": [],
                "canonical_json_hash": "0" * 64,
            }

    monkeypatch.setattr(
        submit_tool,
        "build_execution_submission_record_from_execution_package_v1",
        lambda **kwargs: _FakeSubmissionRecord(),
    )

    submit_tool._refresh_submit_paths_if_constitutional_dependency_stale(
        repo_root=ROOT,
        eval_time_utc="2026-04-25T00:10:00Z",
        execution_package_path=package_path,
        submission_record_path=submission_path,
    )
    assert call_order == ["readiness", "build"]


def test_submit_tool_fails_closed_when_execution_package_build_ref_path_is_missing(tmp_path: Path) -> None:
    package_path = tmp_path / "execution_package.v1.json"
    submission_path = tmp_path / "submission_record.v1.json"
    _write_json(
        package_path,
        {
            "schema_id": "execution_package",
            "schema_version": "v1",
            "operation_type": "fresh_paper_entry_v1",
            "day_utc": "2026-04-24",
            "submission_id": "a" * 64,
            "canonical_json_hash": "b" * 64,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "build_ref": {},
            "candidate_ref": {
                "phasec_out_dir": str((tmp_path / "candidate").resolve()),
                "execution_truth_root": str((tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()),
            },
        },
    )
    _write_submission_record_fixture(
        path=submission_path,
        package_path=package_path,
        package_sha="b" * 64,
        submission_id="a" * 64,
    )

    with pytest.raises(SystemExit, match="execution_package_build_ref_path_missing"):
        submit_tool._refresh_submit_paths_if_constitutional_dependency_stale(
            repo_root=ROOT,
            eval_time_utc="2026-04-25T00:10:00Z",
            execution_package_path=package_path,
            submission_record_path=submission_path,
        )


def test_paper_day_validated_proof_command_produces_expected_outputs(tmp_path: Path) -> None:
    day = proof_module.DEFAULT_DAY
    proof_root = Path("/tmp/constellation_2_foundation/paper_day_readiness_test_proof_v1")
    shutil.rmtree(proof_root, ignore_errors=True)
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "ops" / "tools" / "run_paper_day_readiness_proof_v1.py"),
            "--proof_root",
            str(proof_root),
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    assert completed.returncode != 0
    stderr = completed.stderr + completed.stdout
    assert "REQUIRED_PHASEC_FIXTURE_MISSING" in stderr


def test_paper_day_proof_fails_closed_on_missing_phasec_fixture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing_fixture = tmp_path / "missing_phasec_fixture"
    monkeypatch.setattr(proof_module, "PHASEC_FIXTURE", missing_fixture)
    with pytest.raises(SystemExit, match="REQUIRED_PHASEC_FIXTURE_MISSING"):
        proof_module.run_readiness_proof(
            proof_root=Path("/tmp/constellation_2_foundation/paper_day_readiness_test_missing_fixture_v1"),
            day="2026-04-02",
            produced_utc="2026-04-02T14:30:00Z",
            ib_account="DUO847203",
        )


def test_paper_day_runbook_matches_validated_command() -> None:
    runbook = (ROOT / "governance" / "05_CONTRACTS" / "C2" / "paper_day_readiness_runbook_v1.contract.md").read_text(encoding="utf-8")
    assert "python3 ops/tools/run_paper_day_readiness_proof_v1.py" in runbook
    assert "C2_ENABLE_BROKER_TRANSMIT=YES" in runbook
    assert "FAIL_CLOSED" in runbook
    assert "Micro-Live Checklist" in runbook
    assert "Pre-Live Abort Conditions" in runbook
