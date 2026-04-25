#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_startup_authorization_convergence_v1 import (  # noqa: E402
    derive_paper_startup_authorization_convergence_payload_v1,
    write_paper_startup_authorization_convergence_v1,
)
from constellation_2.common.gate_authority_foundation_v1 import write_gate_authority_plane  # noqa: E402
from constellation_2.common.paper_startup_intent_input_convergence_v1 import (  # noqa: E402
    resolve_paper_startup_intent_input_convergence_path,
)
from constellation_2.common.paper_session_path_alignment_v1 import (  # noqa: E402
    resolve_bod_execution_environment_proof_path,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance_release_current_first_v1  # noqa: E402
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1  # noqa: E402
from constellation_2.common.trade_submit_readiness_authority_v1 import (  # noqa: E402
    resolve_governed_sleeve_truth_bindings,
)


def _git_sha() -> str:
    try:
        provenance = resolve_release_provenance_release_current_first_v1(
            caller="ops/tools/run_paper_startup_authorization_convergence_v1.py"
        )
        if isinstance(provenance, dict):
            git_sha = str(provenance.get("git_sha") or "").strip()
            if git_sha:
                return git_sha
    except Exception:
        pass
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _tool_result(script_relpath: str, *args: str) -> Dict[str, Any]:
    command = [sys.executable, str((REPO_ROOT / script_relpath).resolve()), *args]
    proc = subprocess.run(command, cwd=str(REPO_ROOT), capture_output=True, text=True)
    return {
        "script": script_relpath,
        "command": command,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _materialize_authorization_verdict(
    *,
    day_utc: str,
    produced_utc: str,
    mode: str,
    sleeve_truth_root: Path,
) -> Dict[str, Any]:
    command = [
        "write_gate_authority_plane",
        "--day_utc",
        day_utc,
        "--truth_root",
        str(sleeve_truth_root),
        "--produced_utc",
        produced_utc,
        "--mode",
        mode,
    ]
    try:
        writes = write_gate_authority_plane(
            repo_root=REPO_ROOT,
            truth_root=sleeve_truth_root,
            day_utc=day_utc,
            produced_utc=produced_utc,
            mode=mode,
        )
        summary = {
            "lifecycle_state_action": writes["lifecycle_state"].action,
            "authorization_verdict_action": writes["authorization_verdict"].action,
            "economic_verdict_action": writes["economic_verdict"].action,
            "ledger_action": writes["ledger"].action,
        }
        return {
            "script": "constellation_2/common/gate_authority_foundation_v1.py:write_gate_authority_plane",
            "command": command,
            "return_code": 0,
            "stdout": json.dumps(summary, sort_keys=True),
            "stderr": "",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "script": "constellation_2/common/gate_authority_foundation_v1.py:write_gate_authority_plane",
            "command": command,
            "return_code": 1,
            "stdout": "",
            "stderr": repr(exc),
        }


def _engine_registry_path() -> Path:
    return (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()


def _active_engines() -> List[Dict[str, str]]:
    registry = _read_json(_engine_registry_path())
    engines = registry.get("engines")
    if not isinstance(engines, list):
        raise ValueError("ENGINE_MODEL_REGISTRY_ENGINES_INVALID")
    rows: List[Dict[str, str]] = []
    for row in engines:
        if not isinstance(row, dict):
            continue
        if str(row.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue
        engine_id = str(row.get("engine_id") or "").strip()
        runner_path = str(row.get("engine_runner_path") or "").strip()
        runner_sha256 = str(row.get("engine_runner_sha256") or "").strip()
        if engine_id:
            rows.append(
                {
                    "engine_id": engine_id,
                    "engine_runner_path": runner_path,
                    "engine_runner_sha256": runner_sha256,
                }
            )
    rows.sort(key=lambda row: row["engine_id"])
    return rows


def _emit_heartbeats(*, sleeve_truth_root: Path, day_utc: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    registry_path = _engine_registry_path()
    registry_sha256 = _sha256_file(registry_path)
    for engine in _active_engines():
        fingerprints = [
            f"engine_registry_v1|{registry_path}|{registry_sha256}|true",
        ]
        runner_path = str(engine.get("engine_runner_path") or "").strip()
        runner_sha256 = str(engine.get("engine_runner_sha256") or "").strip() or ("0" * 64)
        if runner_path:
            fingerprints.append(f"engine_runner_path|{runner_path}|{runner_sha256}|true")
        result = _tool_result(
            "ops/tools/run_engine_heartbeat_emit_v1.py",
            "--day_utc",
            day_utc,
            "--engine_id",
            str(engine["engine_id"]),
            "--status",
            "OK",
            "--reason_code",
            "STARTUP_AUTHORIZATION_CONVERGENCE_HEARTBEAT",
            "--last_run_utc",
            f"{day_utc}T00:00:00Z",
            "--expected_period_seconds",
            "86400",
            "--stale_after_seconds",
            "172800",
            "--truth_root",
            str(sleeve_truth_root),
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_module",
            "ops/tools/run_paper_startup_authorization_convergence_v1.py",
            "--producer_git_sha",
            _git_sha(),
            *sum((["--fingerprint", fp] for fp in fingerprints), []),
        )
        results.append(result)
    return results


def _artifact_result(
    *,
    artifact_id: str,
    artifact_path: Path,
    target_day: str,
    required: bool,
    acceptable_statuses: Iterable[str],
    presence_only: bool = False,
) -> Dict[str, Any]:
    path = Path(artifact_path).resolve()
    observed_status = "MISSING"
    observed_day = ""
    ready = False
    reason_codes: List[str] = []
    schema_id = ""
    if path.exists() and path.is_file():
        payload = _read_json(path)
        observed_status = (
            str(
                payload.get("status")
                or payload.get("overall_status")
                or payload.get("final_status")
                or payload.get("convergence_status")
                or ""
            ).strip().upper()
            or "UNKNOWN"
        )
        observed_day = str(payload.get("day_utc") or payload.get("target_day") or "").strip()
        schema_id = str(payload.get("schema_id") or "").strip()
        reason_codes = [str(code).strip() for code in (payload.get("reason_codes") or payload.get("blocking_codes") or []) if str(code).strip()]
        if presence_only:
            ready = observed_day == target_day or not observed_day
        else:
            ready = observed_day == target_day and observed_status in {str(item).strip().upper() for item in acceptable_statuses}
    blocker_code = ""
    summary = ""
    if not ready:
        if not path.exists():
            blocker_code = f"{artifact_id.upper()}_MISSING"
            summary = f"{artifact_id} missing"
        elif observed_day != target_day:
            blocker_code = f"{artifact_id.upper()}_DAY_MISMATCH"
            summary = f"{artifact_id} day mismatch"
        else:
            blocker_code = f"{artifact_id.upper()}_NOT_READY"
            summary = f"{artifact_id} status={observed_status or 'UNKNOWN'}"
    return {
        "artifact_id": artifact_id,
        "required": bool(required),
        "artifact_path": str(path),
        "schema_id": schema_id,
        "target_day_expected": target_day,
        "target_day_observed": observed_day,
        "observed_status": observed_status,
        "ready": bool(ready),
        "reason_codes": reason_codes,
        "blocker_code": blocker_code,
        "summary": summary,
    }


def _resolve_primary_binding(*, environment: str, ib_account: str):
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return binding
    if not bindings:
        raise ValueError("NO_GOVERNED_SLEEVE_TRUTH_BINDINGS")
    return bindings[0]


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_startup_authorization_convergence_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    environment = str(args.environment).strip().upper()
    ib_account = str(args.ib_account).strip()
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    binding = _resolve_primary_binding(environment=environment, ib_account=ib_account)
    sleeve_truth_root = Path(binding.truth_root).resolve()

    source_refs: List[Dict[str, Any]] = []
    produced_utc = f"{day_utc}T00:00:00Z"
    for result in (
        _tool_result(
            "ops/tools/run_bod_execution_environment_proof_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(truth_root),
        ),
        _tool_result(
            "ops/tools/run_paper_startup_intent_input_convergence_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(truth_root),
            "--environment",
            environment,
            "--ib_account",
            ib_account,
        ),
        _tool_result(
            "ops/tools/run_trading_day_intent_generation_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
        ),
        _tool_result(
            "ops/tools/run_engine_daily_returns_day_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
        ),
        _tool_result(
            "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
        ),
        _tool_result(
            "ops/tools/run_correlation_envelope_gate_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--produced_utc",
            produced_utc,
            "--mode",
            environment,
        ),
        _tool_result(
            "ops/tools/run_feed_attestation_gate_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
        ),
        *_emit_heartbeats(sleeve_truth_root=sleeve_truth_root, day_utc=day_utc),
        _tool_result(
            "ops/tools/run_heartbeat_gate_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
        ),
        _tool_result(
            "ops/tools/run_replay_certification_gate_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--produced_utc",
            produced_utc,
            "--mode",
            environment,
        ),
        _tool_result(
            "ops/tools/run_gate_stack_verdict_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--produced_utc",
            produced_utc,
            "--mode",
            environment,
        ),
        _tool_result(
            "ops/tools/run_gate_authority_plane_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--produced_utc",
            produced_utc,
            "--mode",
            environment,
        ),
        _materialize_authorization_verdict(
            day_utc=day_utc,
            produced_utc=produced_utc,
            mode=environment,
            sleeve_truth_root=sleeve_truth_root,
        ),
    ):
        source_refs.append(result)

    artifact_results = [
        _artifact_result(
            artifact_id="bod_execution_environment_proof_v1",
            artifact_path=resolve_bod_execution_environment_proof_path(
                truth_root=truth_root,
                day_utc=day_utc,
            ),
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS",),
        ),
        _artifact_result(
            artifact_id="paper_startup_intent_input_convergence_v1",
            artifact_path=resolve_paper_startup_intent_input_convergence_path(
                truth_root=truth_root,
                day_utc=day_utc,
            ),
            target_day=day_utc,
            required=True,
            acceptable_statuses=("SUCCESS",),
        ),
        _artifact_result(
            artifact_id="trading_day_intent_generation_v1",
            artifact_path=sleeve_truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("INTENTS_PRESENT", "VALID_ZERO"),
        ),
        _artifact_result(
            artifact_id="engine_daily_returns_v1",
            artifact_path=sleeve_truth_root / "monitoring_v1" / "engine_daily_returns_v1" / day_utc / "engine_daily_returns.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("ACTIVE", "NOT_AVAILABLE"),
            presence_only=True,
        ),
        _artifact_result(
            artifact_id="engine_correlation_matrix_v1",
            artifact_path=sleeve_truth_root / "monitoring_v1" / "engine_correlation_matrix" / day_utc / "engine_correlation_matrix.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("OK", "DEGRADED_INSUFFICIENT_HISTORY"),
        ),
        _artifact_result(
            artifact_id="correlation_envelope_gate_v1",
            artifact_path=sleeve_truth_root / "reports" / "correlation_envelope_gate_v1" / day_utc / "correlation_envelope_gate.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS",),
        ),
        _artifact_result(
            artifact_id="feed_attestation_gate_v1",
            artifact_path=sleeve_truth_root / "reports" / "feed_attestation_gate_v1" / day_utc / "feed_attestation_gate.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS",),
        ),
        _artifact_result(
            artifact_id="heartbeat_gate_v1",
            artifact_path=sleeve_truth_root / "reports" / "heartbeat_gate_v1" / day_utc / "heartbeat_gate.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS",),
        ),
        _artifact_result(
            artifact_id="replay_certification_gate_v1",
            artifact_path=sleeve_truth_root / "reports" / "replay_certification_gate_v1" / day_utc / "replay_certification_gate.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS",),
        ),
        _artifact_result(
            artifact_id="authorization_gate_verdict_v1",
            artifact_path=sleeve_truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS", "BOOTSTRAP_PASS"),
        ),
        _artifact_result(
            artifact_id="economic_health_gate_verdict_v1",
            artifact_path=sleeve_truth_root / "reports" / "economic_health_gate_verdict_v1" / day_utc / "economic_health_gate_verdict.v1.json",
            target_day=day_utc,
            required=False,
            acceptable_statuses=("PASS",),
        ),
        _artifact_result(
            artifact_id="gate_stack_verdict_v1",
            artifact_path=sleeve_truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json",
            target_day=day_utc,
            required=False,
            acceptable_statuses=("PASS",),
        ),
    ]

    payload = derive_paper_startup_authorization_convergence_payload_v1(
        truth_root=truth_root,
        target_day=day_utc,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=str(binding.sleeve_id),
        sleeve_truth_root=sleeve_truth_root,
        artifact_results=artifact_results,
        source_refs=source_refs,
    )
    ref = write_paper_startup_authorization_convergence_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "convergence_status": payload["convergence_status"],
                "authorization_verdict_ready": payload["authorization_verdict_ready"],
            },
            sort_keys=True,
        )
    )
    return 0 if payload["convergence_status"] == "SUCCESS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
