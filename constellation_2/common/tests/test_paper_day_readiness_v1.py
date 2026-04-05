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


def _call_submit(*, truth_root: Path, dry_run: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    call_env = os.environ.copy()
    call_env["C2_TRUTH_ROOT"] = str(truth_root)
    if env:
        call_env.update(env)
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py"),
            "--eval_time_utc",
            "2026-04-02T14:30:00Z",
            "--phasec_out_dir",
            str(ROOT / "_smoketest_phasec_2026_04_02"),
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
        ],
        check=False,
        capture_output=True,
        text=True,
        env=call_env,
    )


def test_broker_boundary_fails_closed_by_default(tmp_path: Path) -> None:
    truth_root = Path("/tmp/constellation_2_foundation/paper_day_readiness_test_submit_v1") / "truth_sleeves" / "PRIMARY" / "PAPER"
    shutil.rmtree(truth_root.parents[2], ignore_errors=True)
    proof_module._seed_submit_prerequisites(
        truth_root=truth_root,
        day="2026-04-02",
        produced_utc="2026-04-02T14:30:00Z",
        ib_account="DUO847203",
    )
    completed = _call_submit(truth_root=truth_root, dry_run="NO")
    assert completed.returncode != 0
    assert "broker transmit disabled by default" in (completed.stderr + completed.stdout)


def test_paper_day_safe_submit_path_succeeds(tmp_path: Path) -> None:
    truth_root = Path("/tmp/constellation_2_foundation/paper_day_readiness_test_submit_v2") / "truth_sleeves" / "PRIMARY" / "PAPER"
    shutil.rmtree(truth_root.parents[2], ignore_errors=True)
    proof_module._seed_submit_prerequisites(
        truth_root=truth_root,
        day="2026-04-02",
        produced_utc="2026-04-02T14:30:00Z",
        ib_account="DUO847203",
    )
    completed = _call_submit(truth_root=truth_root, dry_run="YES")
    assert completed.returncode == 0, completed.stderr
    submissions = list((truth_root / "execution_evidence_v1" / "submissions" / "2026-04-02").glob("*/broker_submission_record.v2.json"))
    assert len(submissions) == 1


def test_submit_live_enablement_contract_remains_explicit_and_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("C2_ENABLE_BROKER_TRANSMIT", raising=False)
    with pytest.raises(SystemExit, match="C2_ENABLE_BROKER_TRANSMIT=YES"):
        submit_tool._require_broker_transmit_enabled(dry_run="NO")
    submit_tool._require_broker_transmit_enabled(dry_run="YES")
    monkeypatch.setenv("C2_ENABLE_BROKER_TRANSMIT", "YES")
    submit_tool._require_broker_transmit_enabled(dry_run="NO")


def test_paper_day_validated_proof_command_produces_expected_outputs(tmp_path: Path) -> None:
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
    assert completed.returncode == 0, completed.stderr
    assert "PAPER_DAY_READINESS_PROOF_V1" in completed.stdout
    assert "broker_submission_record=" in completed.stdout
    assert "execution_truth_gap=" in completed.stdout
    assert "runtime_trace_bundle=" in completed.stdout
    assert "replay_manifest=" in completed.stdout
    truth_root = proof_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    replay_root = proof_root / "replay_truth"
    advisor_output_root = proof_module.advisor_runtime_root()
    submissions = sorted((truth_root / "execution_evidence_v1" / "submissions" / "2026-04-02").iterdir())
    assert len(submissions) == 1
    submission_id = submissions[0].name
    expected_paths = [
        truth_root / "reports" / "execution_completion_gap_report_v1" / "2026-04-02" / "execution_completion_gap_report.v1.json",
        truth_root / "reports" / "authorization_gate_verdict_v1" / "2026-04-02" / "authorization_gate_verdict.v1.json",
        truth_root / "reports" / "runtime_trace_bundle_v1" / "2026-04-02" / f"{submission_id}.runtime_trace_bundle.v1.json",
        replay_root / "reports" / "replay_manifest_v1" / "2026-04-02" / f"{submission_id}.replay_manifest.v1.json",
        advisor_output_root / "PAPER" / "reports" / "authority_registry_v1" / "2026-04-02" / "authority_registry.v1.json",
        advisor_output_root / "PAPER" / "publication_gate_result_v1" / "2026-04-02" / "publication_gate_result.v1.json",
        advisor_output_root / "PAPER" / "promotion_gate_result_v1" / "2026-04-02" / "promotion_gate_result.v1.json",
    ]
    for path in expected_paths:
        assert path.exists(), path
    replay_manifest = json.loads((replay_root / "reports" / "replay_manifest_v1" / "2026-04-02" / f"{submission_id}.replay_manifest.v1.json").read_text(encoding="utf-8"))
    assert replay_manifest["status"] == "OK"


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
