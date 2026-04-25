from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "ops" / "tools" / "run_replay_certification_gate_v1.py"
DAY = "2030-01-20"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_bundle_inputs_for_pass(truth_root: Path) -> None:
    _write_json(truth_root / "reports" / "pipeline_manifest_v1" / DAY / "pipeline_manifest.v1.json", {"schema_id": "pipeline_manifest"})
    _write_json(truth_root / "allocation_v1" / "summary" / DAY / "allocation_summary.v1.json", {"schema_id": "allocation_summary"})
    _write_json(truth_root / "market_data_snapshot_v1" / "dataset_manifest.json", {"schema_id": "dataset_manifest"})
    _write_json(truth_root / "monitoring_v1" / "engine_correlation_matrix" / DAY / "engine_correlation_matrix.v1.json", {"schema_id": "corr"})
    _write_json(truth_root / "reports" / "convex_risk_assessment_v1" / DAY / "convex_risk_assessment.v1.json", {"schema_id": "convex"})
    _write_json(truth_root / "reports" / "depth_liquidity_stress_v1" / DAY / "depth_liquidity_stress.v1.json", {"schema_id": "depth"})
    _write_json(truth_root / "reports" / "broker_reconciliation_v2" / DAY / "broker_reconciliation.v2.json", {"schema_id": "reconciliation"})
    _write_json(truth_root / "accounting_v2" / "nav" / DAY / "nav.v2.json", {"schema_id": "nav"})
    _write_json(truth_root / "reports" / "gate_stack_verdict_v1" / DAY / "gate_stack_verdict.v1.json", {"schema_id": "gate_stack"})


def _write_legacy_submission_index(truth_root: Path) -> None:
    _write_json(truth_root / "execution_evidence_v1" / "submission_index" / DAY / "submission_index.v1.json", {"schema_id": "submission_index"})


def _write_submission_dir(truth_root: Path, submission_id: str = "a" * 64) -> None:
    sub_dir = truth_root / "execution_evidence_v1" / "submissions" / DAY / submission_id
    sub_dir.mkdir(parents=True, exist_ok=True)
    _write_json(sub_dir / "broker_submission_record.v2.json", {"schema_id": "broker_submission_record"})


def _write_placeholder_submission_dir(truth_root: Path, submission_id: str = "d" * 64) -> None:
    sub_dir = truth_root / "execution_evidence_v1" / "submissions" / DAY / submission_id
    sub_dir.mkdir(parents=True, exist_ok=True)
    _write_json(sub_dir / "equity_order_plan.v1.json", {"schema_id": "equity_order_plan"})
    _write_json(sub_dir / "mapping_ledger_record.v2.json", {"schema_id": "mapping_ledger_record"})
    _write_json(sub_dir / "veto_record.v1.json", {"schema_id": "veto_record"})


def _write_pillars_decision(truth_root: Path, *, version: str = "pillars_v1r1", decision_id: str = "b" * 64) -> None:
    _write_json(
        truth_root / version / DAY / "decisions" / f"{decision_id}.submission_decision_record.v1.json",
        {
            "schema_id": "submission_decision_record",
            "schema_version": "v1",
            "day_utc": DAY,
            "decision_id": decision_id,
            "produced_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation_2_runtime", "git_sha": "a" * 40, "module": "tests"},
            "status": "OK",
            "decision": {"disposition": "ALLOW", "detail": None},
            "reason_codes": ["TEST"],
            "input_manifest": [],
            "decision_sha256": "c" * 64,
        },
    )


def test_replay_gate_first_run_fails_when_bundle_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    gate_path = truth_root / "reports" / "replay_certification_gate_v1" / DAY / "replay_certification_gate.v1.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert gate["status"] == "FAIL"
    assert gate["reason_codes"] == ["REPLAY_CERT_FIRST_RUN", "REPLAY_CERT_BUNDLE_FAIL_CLOSED"]


def test_replay_gate_first_run_passes_when_bundle_passes(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)
    _write_legacy_submission_index(truth_root)
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    gate_path = truth_root / "reports" / "replay_certification_gate_v1" / DAY / "replay_certification_gate.v1.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert gate["status"] == "PASS"
    assert gate["reason_codes"] == ["REPLAY_CERT_FIRST_RUN", "REPLAY_CERT_BUNDLE_PASS"]


def test_replay_gate_prefers_pillars_submission_evidence_over_legacy_submission_index(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)
    _write_submission_dir(truth_root)
    _write_legacy_submission_index(truth_root)
    _write_pillars_decision(truth_root, version="pillars_v1r1")

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    submission_entry = next(e for e in bundle["input_entries"] if e["type"] == "submission_evidence")
    assert submission_entry["present"] is True
    assert submission_entry["path"] == f"pillars_v1r1/{DAY}/decisions"
    assert bundle["status"] == "PASS"
    assert len(bundle["hashes"]["submission_bundle_hashes"]) == 1


def test_replay_gate_falls_back_to_legacy_submission_index_when_pillars_absent(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)
    _write_submission_dir(truth_root)
    _write_legacy_submission_index(truth_root)

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    submission_entry = next(e for e in bundle["input_entries"] if e["type"] == "submission_evidence")
    assert submission_entry["path"] == f"execution_evidence_v1/submission_index/{DAY}/submission_index.v1.json"
    assert bundle["status"] == "PASS"


def test_replay_gate_allows_no_submissions_without_submission_evidence_artifact(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    submission_entry = next(e for e in bundle["input_entries"] if e["type"] == "submission_evidence")
    assert submission_entry["present"] is True
    assert submission_entry["path"] == f"execution_evidence_v1/submissions/{DAY}"
    assert bundle["hashes"]["submission_bundle_hashes"] == []
    assert bundle["status"] == "PASS"


def test_replay_gate_treats_placeholder_submission_dirs_without_broker_submission_records_as_no_submissions(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)
    _write_placeholder_submission_dir(truth_root)

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    submission_entry = next(e for e in bundle["input_entries"] if e["type"] == "submission_evidence")
    assert submission_entry["present"] is True
    assert submission_entry["path"] == f"execution_evidence_v1/submissions/{DAY}"
    assert bundle["hashes"]["submission_bundle_hashes"] == []
    assert bundle["status"] == "PASS"


def test_replay_gate_fails_closed_when_submission_evidence_missing_for_existing_submissions(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)
    _write_submission_dir(truth_root)

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert bundle["status"] == "FAIL"
    assert bundle["inputs"]["missing_types"] == ["submission_evidence"]


def test_replay_gate_repairs_stale_false_pass_when_bundle_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    gate_path = truth_root / "reports" / "replay_certification_gate_v1" / DAY / "replay_certification_gate.v1.json"
    _write_json(
        gate_path,
        {
            "schema_id": "C2_REPLAY_CERTIFICATION_GATE_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation_2_runtime", "git_sha": "a" * 40, "module": "ops/tools/run_replay_certification_gate_v1.py"},
            "status": "PASS",
            "first_run": True,
            "two_run_equality": None,
            "existing_bundle_sha256": None,
            "candidate_bundle_sha256": "0" * 64,
            "reason_codes": ["REPLAY_CERT_FIRST_RUN"],
            "gate_sha256": "1" * 64,
        },
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert completed.returncode == 0, completed.stderr

    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert bundle["status"] == "FAIL"
    assert gate["status"] == "FAIL"
    assert gate["reason_codes"] == ["REPLAY_CERT_FIRST_RUN", "REPLAY_CERT_BUNDLE_FAIL_CLOSED"]
    assert (gate_path.parent / "__quarantine__").is_dir()


def test_replay_gate_refreshes_stale_pass_when_bundle_candidate_changes(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)

    initial = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert initial.returncode == 0, initial.stderr

    # Simulate a stale pre-fix bundle that used the old contract and failed closed on missing legacy submission_index.
    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    stale_bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    stale_bundle["status"] = "FAIL"
    stale_bundle["fail_closed"] = True
    stale_bundle["inputs"]["present_types"] = [t for t in stale_bundle["inputs"]["present_types"] if t != "submission_evidence"]
    stale_bundle["inputs"]["missing_types"] = ["submission_index"]
    stale_bundle["input_entries"] = [
        {
            "type": "submission_index",
            "path": f"execution_evidence_v1/submission_index/{DAY}/submission_index.v1.json",
            "sha256": "0" * 64,
            "present": False,
        }
        if entry["type"] == "submission_evidence"
        else entry
        for entry in stale_bundle["input_entries"]
    ]
    stale_bundle["hashes"]["submission_bundle_hashes"] = ["0" * 64]
    _write_json(bundle_path, stale_bundle)

    gate_path = truth_root / "reports" / "replay_certification_gate_v1" / DAY / "replay_certification_gate.v1.json"
    stale_gate = json.loads(gate_path.read_text(encoding="utf-8"))
    stale_gate["candidate_bundle_sha256"] = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    stale_gate["gate_sha256"] = "e" * 64
    _write_json(gate_path, stale_gate)

    _write_placeholder_submission_dir(truth_root)

    refreshed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert refreshed.returncode == 0, refreshed.stderr
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert bundle["status"] == "PASS"
    assert gate["status"] == "PASS"
    assert (bundle_path.parent / "__quarantine__").is_dir()
    assert (gate_path.parent / "__quarantine__").is_dir()


def test_replay_gate_refreshes_stale_fail_artifacts_when_pillars_become_available(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_bundle_inputs_for_pass(truth_root)
    _write_submission_dir(truth_root)

    first = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert first.returncode == 0, first.stderr

    _write_pillars_decision(truth_root, version="pillars_v1r1")

    second = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert second.returncode == 0, second.stderr

    bundle_path = truth_root / "reports" / "replay_certification_bundle_v1" / DAY / "replay_certification_bundle.v1.json"
    gate_path = truth_root / "reports" / "replay_certification_gate_v1" / DAY / "replay_certification_gate.v1.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert bundle["status"] == "PASS"
    assert gate["status"] == "PASS"
    assert (bundle_path.parent / "__quarantine__").is_dir()
    assert (gate_path.parent / "__quarantine__").is_dir()
