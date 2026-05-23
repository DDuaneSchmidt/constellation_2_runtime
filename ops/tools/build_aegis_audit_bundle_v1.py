#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.decision_ledger_v1 import decision_ledger_path_v1, write_decision_ledger_v1  # noqa: E402
from ops.aegis.evidence_event_store_v1 import evidence_events_path_v1, rebuild_evidence_snapshot_v1  # noqa: E402
from ops.aegis.pure_runtime_evaluator_v1 import evaluate_runtime, runtime_policy_bundle_v1  # noqa: E402
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1, stable_json_bytes_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, read_canonical_runtime_policy_bundle_v1


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "UNKNOWN"


def audit_bundle_root_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    safe_run = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in run_id)
    return Path(truth_root).expanduser().resolve() / "audit_bundles" / "aegis_audit_bundle_v1" / day_utc / safe_run


def build_aegis_audit_bundle_v1(
    *,
    truth_root: Path,
    day_utc: str,
    run_id: str,
    parent_run_id: str,
    generated_at_utc: str,
    command_args: list[str],
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    git_sha = _git_sha()
    snapshot = rebuild_evidence_snapshot_v1(truth_root=root, day_utc=day_utc)
    canonical_policy = read_canonical_runtime_policy_bundle_v1(truth_root=root, day_utc=day_utc)
    canonical_evaluation = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    if canonical_policy and canonical_evaluation:
        policy = canonical_policy
        evaluation = canonical_evaluation
        replayed = evaluate_runtime(day_utc, snapshot, policy)
        if replayed.get("deterministic_output_hash") != evaluation.get("deterministic_output_hash"):
            raise SystemExit("FAIL: canonical RuntimeEvaluation does not replay from current evidence ledger")
    else:
        policy = runtime_policy_bundle_v1(run_id=run_id, parent_run_id=parent_run_id, generated_at_utc=generated_at_utc, git_sha=git_sha)
        evaluation = evaluate_runtime(day_utc, snapshot, policy)
    decision_paths = write_decision_ledger_v1(truth_root=root, evaluation=evaluation)
    bundle = audit_bundle_root_v1(truth_root=root, day_utc=day_utc, run_id=run_id)
    bundle.mkdir(parents=True, exist_ok=True)
    files: dict[str, Path] = {
        "runtime_evaluation": bundle / "runtime_evaluation.v1.json",
        "evidence_events": bundle / "evidence_events.jsonl",
        "evidence_snapshot": bundle / "evidence_snapshot.v1.json",
        "dependency_dag": bundle / "dependency_dag.v1.json",
        "policy_bundle": bundle / "policy_bundle.v1.json",
        "schema_registry": bundle / "schema_registry.v1.json",
        "producer_contract_registry": bundle / "producer_contract_registry.v1.json",
        "producer_contract_coverage": bundle / "producer_contract_coverage.v1.json",
        "blocker_states": bundle / "blocker_states.v1.json",
        "legacy_scan_only_artifacts": bundle / "legacy_scan_only_artifacts.v1.json",
        "source_data_manifest": bundle / "source_data_manifest.v1.json",
        "manual_intent_manifest": bundle / "manual_intent_manifest.v1.json",
        "repair_semantics_report": bundle / "repair_semantics_report.v1.json",
        "repair_plan": bundle / "repair_plan.v1.json",
        "repair_attempts": bundle / "repair_attempts.jsonl",
        "repair_diff": bundle / "repair_diff.v1.txt",
        "manual_intent_report": bundle / "manual_intent_report.v1.json",
        "critical_bridge_usage": bundle / "critical_bridge_usage.v1.json",
        "canonical_vs_quarantine_events": bundle / "canonical_vs_quarantine_events.v1.json",
        "contract_validation_results": bundle / "contract_validation_results.v1.json",
        "command_args": bundle / "command_args.v1.json",
        "environment_summary": bundle / "environment_summary.v1.json",
        "decision_ledger": bundle / "decisions.jsonl",
    }
    _write_json(files["runtime_evaluation"], evaluation)
    _write_json(files["evidence_snapshot"], snapshot)
    _write_json(files["dependency_dag"], {"schema_id": "aegis_runtime_dependency_dag", "schema_version": "v1", "capability_dag": policy["capability_dag"]})
    _write_json(files["policy_bundle"], policy)
    _write_json(files["schema_registry"], _schema_registry())
    from ops.aegis.producer_contracts_v1 import load_producer_contract_registry_v1
    from ops.aegis.producer_contract_reports_v1 import producer_contract_coverage_report_v1, legacy_scan_only_artifacts_report_v1, source_data_manifest_v1, manual_intent_manifest_v1, critical_bridge_usage_report_v1, repair_semantics_report_v1
    from ops.aegis.producer_contract_validator_v1 import validate_evidence_event_contract_v1
    from ops.aegis.event_append_transaction_v1 import canonical_vs_quarantine_report_v1
    from ops.aegis.manual_intent_v1 import manual_intent_report_v1
    events = snapshot.get("events") if isinstance(snapshot.get("events"), list) else []
    _write_json(files["producer_contract_registry"], load_producer_contract_registry_v1())
    _write_json(files["producer_contract_coverage"], producer_contract_coverage_report_v1(artifact_statuses=[], events=events))
    _write_json(files["blocker_states"], {"schema_id": "aegis_blocker_states_report", "schema_version": "v1", "blocker_state": evaluation.get("blocker_state", []), "root_blockers": evaluation.get("root_blockers", [])})
    _write_json(files["legacy_scan_only_artifacts"], legacy_scan_only_artifacts_report_v1(artifact_statuses=[], events=events))
    _write_json(files["source_data_manifest"], source_data_manifest_v1(events=events))
    _write_json(files["manual_intent_manifest"], manual_intent_manifest_v1(events=events))
    _write_json(files["repair_semantics_report"], repair_semantics_report_v1(events=events))
    _copy_optional_or_empty(root / "reports" / "aegis_runtime_repair_v1" / day_utc / "repair_plan.v1.json", files["repair_plan"], json_empty={"schema_id": "aegis_repair_plan", "schema_version": "v1", "repairs": []})
    _copy_optional_or_empty(root / "reports" / "aegis_runtime_repair_v1" / day_utc / "repair_attempts.jsonl", files["repair_attempts"])
    _copy_optional_or_empty(root / "reports" / "aegis_runtime_repair_v1" / day_utc / "repair_diff.v1.txt", files["repair_diff"])
    _write_json(files["manual_intent_report"], manual_intent_report_v1(truth_root=root, day_utc=day_utc, runtime_evaluation_hash=str(evaluation.get("deterministic_output_hash") or ""), generated_at_utc=generated_at_utc))
    _write_json(files["critical_bridge_usage"], critical_bridge_usage_report_v1(events=events))
    _write_json(files["canonical_vs_quarantine_events"], canonical_vs_quarantine_report_v1(truth_root=root, day_utc=day_utc))
    _write_json(files["contract_validation_results"], {"schema_id": "aegis_contract_validation_results", "schema_version": "v1", "results": [validate_evidence_event_contract_v1(event) for event in events]})
    _write_json(files["command_args"], {"argv": command_args, "day_utc": day_utc, "run_id": run_id})
    _write_json(files["environment_summary"], _environment_summary(git_sha=git_sha))
    event_path = evidence_events_path_v1(truth_root=root, day_utc=day_utc)
    files["evidence_events"].write_text(event_path.read_text(encoding="utf-8") if event_path.exists() else "", encoding="utf-8")
    decision_path = Path(decision_paths.get("decision_ledger") or decision_ledger_path_v1(truth_root=root, day_utc=day_utc))
    files["decision_ledger"].write_text(decision_path.read_text(encoding="utf-8") if decision_path.exists() else "", encoding="utf-8")
    manifest = _bundle_manifest(bundle=bundle, files=files, evaluation=evaluation)
    manifest_path = bundle / "bundle_manifest.json"
    _write_json(manifest_path, manifest)
    bundle_hash = stable_hash_v1(manifest)
    (bundle / "bundle_hash.txt").write_text(bundle_hash + "\n", encoding="utf-8")
    return {"bundle_path": str(bundle), "bundle_hash": bundle_hash, "runtime_evaluation_hash": evaluation["deterministic_output_hash"], "manifest_path": str(manifest_path)}


def _bundle_manifest(*, bundle: Path, files: dict[str, Path], evaluation: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": "aegis_audit_bundle_manifest",
        "schema_version": "v1",
        "bundle_path": str(bundle),
        "runtime_evaluation_hash": evaluation["deterministic_output_hash"],
        "files": {
            name: {"path": path.name, "sha256": _file_hash(path), "required": True}
            for name, path in sorted(files.items())
        },
        "deterministic_replay_command": f"python3 ops/tools/replay_aegis_runtime_v1.py --audit-bundle {bundle}",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_bytes(stable_json_bytes_v1(payload) + b"\n")


def _copy_optional_or_empty(source: Path, dest: Path, *, json_empty: dict[str, Any] | None = None) -> None:
    if source.exists():
        dest.write_bytes(source.read_bytes())
    elif json_empty is not None:
        _write_json(dest, json_empty)
    else:
        dest.write_text("", encoding="utf-8")


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return stable_hash_v1(path.read_bytes().decode("utf-8", errors="replace"))


def _schema_registry() -> dict[str, Any]:
    return {
        "schema_id": "aegis_audit_bundle_schema_registry",
        "schema_version": "v1",
        "schemas": [
            "governance/02_REGISTRIES/AEGIS_RUNTIME_EVALUATION_SCHEMA_V1.json",
            "governance/02_REGISTRIES/AEGIS_EVIDENCE_EVENT_SCHEMA_V1.json",
            "governance/04_DATA/SCHEMAS/C2/REPORTS/market_data_inputs.v1.schema.json",
            "governance/04_DATA/SCHEMAS/C2/REPORTS/market_data_readiness.v1.schema.json",
        ],
    }


def _environment_summary(*, git_sha: str) -> dict[str, Any]:
    return {
        "git_sha": git_sha,
        "python_executable": sys.executable,
        "cwd": str(REPO_ROOT),
        "env_keys": sorted(key for key in os.environ if key.startswith("AEGIS_") or key in {"TARGET_DAY"}),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_audit_bundle_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--run-id", dest="run_id", default="")
    parser.add_argument("--parent-run-id", dest="parent_run_id", default="")
    parser.add_argument("--generated-at-utc", dest="generated_at_utc", default="")
    args = parser.parse_args(argv)
    generated_at = args.generated_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    run_id = args.run_id or f"audit-bundle:{args.day_utc}:{generated_at}"
    result = build_aegis_audit_bundle_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        run_id=run_id,
        parent_run_id=str(args.parent_run_id or ""),
        generated_at_utc=generated_at,
        command_args=sys.argv[1:] if argv is None else argv,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
