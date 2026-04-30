#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    collect_intent_files_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_trading_day_intent_generation_path
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools.run_intent_arbitration_v1 import build_intent_arbitration
from ops.tools.run_sleeve_evaluation_kernel_v1 import build_sleeve_evaluation_kernel


OUTPUT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_intent_generation.v1.schema.json"
)
ENGINE_REGISTRY_RELPATH = "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
ENGINE_REGISTRY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json"
NO_INTENTS_MARKER_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"
NO_INTENTS_MARKER_TOOL = (REPO_ROOT / "ops/tools/run_no_intents_day_marker_v1.py").resolve()
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
PAPER_MODE = "PAPER"
DEFENSIVE_TAIL_ENGINE_ID = "C2_DEFENSIVE_TAIL_V1"
DEFENSIVE_TAIL_INPUT_BRIDGE_TOOL = (
    REPO_ROOT / "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py"
).resolve()


@dataclass(frozen=True)
class ProducerSpec:
    engine_id: str
    script_path: Path
    registry_runner_sha256: str


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _run(cmd: List[str], *, truth_root: Path) -> Dict[str, Any]:
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(truth_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _first_nonempty(values: List[str]) -> str:
    for value in values:
        if str(value).strip():
            return str(value).strip()
    return ""


_NONZERO_REASON_RE = re.compile(r"(?<![A-Z0-9_])([A-Z][A-Z0-9_]{2,})(?=:\s|$)")


def _classify_producer_nonzero_reason(*, stdout: str, stderr: str) -> str:
    text_blocks = [str(stderr or "").strip(), str(stdout or "").strip()]
    for block in text_blocks:
        if not block:
            continue
        for line in block.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            for match in _NONZERO_REASON_RE.finditer(stripped):
                candidate = match.group(1)
                if candidate not in {"FAIL", "TRACEBACK"}:
                    return candidate
            lowered = stripped.lower()
            if "missing market data manifest:" in lowered:
                return "MARKET_DATA_MANIFEST_MISSING"
            if "missing required source" in lowered:
                return "MISSING_REQUIRED_INPUTS"
    return "PRODUCER_NONZERO_RC"


def _intents_dir(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day_utc).resolve()


def _marker_path(*, truth_root: Path, day_utc: str) -> Path:
    return (_intents_dir(truth_root=truth_root, day_utc=day_utc) / "no_intents_day.v1.json").resolve()


def _load_registry() -> tuple[dict[str, Any], Path, str]:
    path = (REPO_ROOT / ENGINE_REGISTRY_RELPATH).resolve()
    payload = read_json_object_v1(path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, ENGINE_REGISTRY_SCHEMA_RELPATH)
    return payload, path, _sha256_file(path)


def _load_required_producer_specs() -> tuple[List[ProducerSpec], List[Dict[str, str]]]:
    registry, _, _ = _load_registry()
    engines = registry.get("engines")
    if not isinstance(engines, list):
        raise SystemExit("FAIL: ENGINE_MODEL_REGISTRY_MISSING_ENGINES")
    specs: List[ProducerSpec] = []
    skipped: List[Dict[str, str]] = []
    for row in engines:
        if not isinstance(row, dict):
            raise SystemExit("FAIL: ENGINE_MODEL_REGISTRY_ENGINE_ENTRY_NOT_OBJECT")
        engine_id = str(row.get("engine_id") or "").strip()
        if not engine_id:
            raise SystemExit("FAIL: ENGINE_MODEL_REGISTRY_ENGINE_ID_MISSING")
        if str(row.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue
        if engine_id == SIMULATOR_ENGINE_ID:
            skipped.append({"engine_id": engine_id, "reason_code": "ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED"})
            continue
        runner_relpath = str(row.get("engine_runner_path") or "").strip()
        runner_sha = str(row.get("engine_runner_sha256") or "").strip().lower()
        if not runner_relpath:
            raise SystemExit(f"FAIL: ACTIVE_ENGINE_RUNNER_PATH_MISSING engine_id={engine_id}")
        if not runner_relpath.endswith("_intents_day_v1.py"):
            raise SystemExit(f"FAIL: ACTIVE_ENGINE_PRESTART_WRITER_UNSUPPORTED engine_id={engine_id} runner={runner_relpath}")
        script_path = (REPO_ROOT / runner_relpath).resolve()
        if not script_path.exists() or not script_path.is_file():
            raise SystemExit(f"FAIL: ACTIVE_ENGINE_RUNNER_MISSING engine_id={engine_id} path={script_path}")
        specs.append(ProducerSpec(engine_id=engine_id, script_path=script_path, registry_runner_sha256=runner_sha))
    specs.sort(key=lambda item: item.engine_id)
    return specs, skipped


def _validate_existing_marker(path: Path, day_utc: str) -> None:
    payload = read_json_object_v1(path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, NO_INTENTS_MARKER_SCHEMA_RELPATH)
    if str(payload.get("day_utc") or "").strip() != day_utc:
        raise SystemExit(f"FAIL: NO_INTENTS_MARKER_DAY_MISMATCH path={path}")


def _classify_no_intent(stdout: str) -> bool:
    text = str(stdout or "").strip()
    if "NO_INTENT" in text:
        return True
    try:
        obj = json.loads(text)
    except Exception:
        return False
    return isinstance(obj, dict) and str(obj.get("status") or "").strip().upper() == "NO_INTENT"


def _producer_cmd(*, spec: ProducerSpec, day_utc: str, truth_root: Path) -> List[str]:
    return [
        sys.executable,
        str(spec.script_path),
        "--day_utc",
        day_utc,
        "--mode",
        PAPER_MODE,
        "--truth_root",
        str(truth_root),
    ]


def _defensive_tail_inputs_cmd(*, day_utc: str) -> List[str]:
    return [
        sys.executable,
        str(DEFENSIVE_TAIL_INPUT_BRIDGE_TOOL),
        "--day_utc",
        day_utc,
        "--symbol",
        "SPY",
    ]


def _producer_result(
    *,
    logical_name: str,
    engine_id: str,
    script_path: Path,
    script_sha256: str,
    registry_runner_sha256: str,
    status: str,
    return_code: int | None,
    reason_codes: List[str],
    stdout: str,
    stderr: str,
    output_paths: List[str],
) -> Dict[str, Any]:
    return {
        "logical_name": logical_name,
        "engine_id": engine_id,
        "script_path": str(script_path),
        "script_sha256": script_sha256,
        "registry_runner_sha256": registry_runner_sha256,
        "status": status,
        "return_code": return_code,
        "reason_codes": reason_codes,
        "stdout": stdout,
        "stderr": stderr,
        "output_paths": output_paths,
    }


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_trading_day_intent_generation_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    decision_truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    intent_truth_root = resolve_paper_intent_truth_root_v1(
        truth_root=decision_truth_root,
        repo_root=REPO_ROOT,
    )
    produced_at_utc = now_utc_iso_v1()
    generation_run_id = f"trading_day_intent_generation:{day_utc}:{produced_at_utc}"
    output_path = resolve_trading_day_intent_generation_path(truth_root=decision_truth_root, day_utc=day_utc)
    registry_payload, registry_path, registry_sha = _load_registry()
    producer_specs, skipped_active_engines = _load_required_producer_specs()
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=day_utc,
        truth_root=decision_truth_root,
        execution_root=intent_truth_root,
        environment=PAPER_MODE,
    )
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    preopen_build_mode = readiness_mode in {"PREOPEN_BUILD", "PREOPEN_ADMISSION"}

    sleeve_rollup = build_sleeve_evaluation_kernel(day_utc=day_utc, truth_root=decision_truth_root, environment=PAPER_MODE)
    arbitration = build_intent_arbitration(day_utc=day_utc, truth_root=decision_truth_root, environment=PAPER_MODE)
    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    candidate_intents = arbitration.get("candidate_intents") if isinstance(arbitration.get("candidate_intents"), list) else []
    blocking_codes = sorted(
        {
            str(row.get("canonical_blocker") or "").strip()
            for row in sleeve_rollup.get("outcomes", [])
            if isinstance(row, dict) and str(row.get("canonical_blocker") or "").strip()
        }
    )
    if arbitration.get("status") == "SELECTED":
        final_status = "INTENTS_PRESENT"
        first_blocker_code = ""
        first_blocker_artifact_path = ""
        return_code = 0
    elif arbitration.get("status") == "NO_EXECUTABLE_INTENT":
        final_status = "VALID_ZERO"
        first_blocker_code = "NO_EXECUTABLE_INTENT"
        first_blocker_artifact_path = str(arbitration.get("artifact_path") or "")
        return_code = 0
    elif preopen_build_mode and str(arbitration.get("canonical_blocker") or "").strip() == "MISSING_REQUIRED_INPUTS":
        final_status = "VALID_ZERO"
        first_blocker_code = "PREOPEN_BUILD_INTENT_INPUTS_NOT_REQUIRED"
        first_blocker_artifact_path = str(readiness_path)
        blocking_codes = []
        return_code = 0
    else:
        final_status = "BLOCKED_BY_DEFECT"
        first_blocker_code = str(arbitration.get("canonical_blocker") or (blocking_codes[0] if blocking_codes else "INTENT_ARBITRATION_BLOCKED"))
        first_blocker_artifact_path = str(arbitration.get("artifact_path") or "")
        return_code = 3
    day_dir = _intents_dir(truth_root=resolve_paper_intent_truth_root_v1(truth_root=decision_truth_root, repo_root=REPO_ROOT), day_utc=day_utc)
    producer_results = []
    registry_by_engine = {
        str(row.get("engine_id") or ""): row
        for row in registry_payload.get("engines", [])
        if isinstance(row, dict)
    }
    for row in sleeve_rollup.get("outcomes", []) if isinstance(sleeve_rollup.get("outcomes"), list) else []:
        if not isinstance(row, dict):
            continue
        registry_row = registry_by_engine.get(str(row.get("engine_id") or "")) or {}
        runner_path = (REPO_ROOT / str(registry_row.get("engine_runner_path") or "")).resolve()
        script_sha = _sha256_file(runner_path) if runner_path.exists() and runner_path.is_file() else "0" * 64
        status_map = {
            "INTENT_CREATED": "INTENT_WRITTEN",
            "NO_INTENT": "NO_INTENT",
            "BLOCKED": "BLOCKED_BY_DEFECT",
            "FILTERED_OUT": "SKIPPED",
            "DISABLED": "SKIPPED",
        }
        row_status = status_map.get(str(row.get("status") or ""), "BLOCKED_BY_DEFECT")
        reason_codes = [str(code) for code in row.get("reason_codes", [])] if isinstance(row.get("reason_codes"), list) else []
        if preopen_build_mode and row_status == "BLOCKED_BY_DEFECT" and str(row.get("canonical_blocker") or "").strip() == "MISSING_REQUIRED_INPUTS":
            row_status = "SKIPPED"
            reason_codes = ["PREOPEN_BUILD_INTENT_INPUTS_NOT_REQUIRED"]
        producer_results.append(
            _producer_result(
                logical_name=str(row.get("sleeve_id") or row.get("engine_id") or ""),
                engine_id=str(row.get("engine_id") or ""),
                script_path=runner_path if str(registry_row.get("engine_runner_path") or "") else output_path,
                script_sha256=script_sha,
                registry_runner_sha256=str(registry_row.get("engine_runner_sha256") or script_sha),
                status=row_status,
                return_code=int(row.get("exit_code") or 0),
                reason_codes=reason_codes,
                stdout=str(row.get("stdout_summary") or ""),
                stderr=str(row.get("stderr_summary") or ""),
                output_paths=[str(item.get("intent_path") or "") for item in row.get("output_intents", []) if isinstance(item, dict) and str(item.get("intent_path") or "")],
            )
        )
    payload = {
        "schema_id": "trading_day_intent_generation",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
        "day_utc": day_utc,
        "generation_run_id": f"trading_day_intent_generation:{day_utc}:{produced_at_utc}",
        "produced_at_utc": produced_at_utc,
        "producer": producer_block_v1(module="ops/tools/run_trading_day_intent_generation_v1.py"),
        "truth_root": str(decision_truth_root),
        "active_engine_registry_path": str(registry_path),
        "active_engine_registry_sha256": registry_sha,
        "producer_topology": [
            {
                "engine_id": spec.engine_id,
                "script_path": str(spec.script_path),
                "script_sha256": _sha256_file(spec.script_path),
                "registry_runner_sha256": spec.registry_runner_sha256,
                "required": True,
            }
            for spec in producer_specs
        ],
        "skipped_active_engines": skipped_active_engines,
        "producer_results": producer_results,
        "final_status": final_status,
        "first_blocker_code": first_blocker_code,
        "first_blocker_artifact_path": first_blocker_artifact_path,
        "canonical_outputs": {
            "intents_dir": str(day_dir),
            "intent_output_paths": [str(row.get("intent_path") or "") for row in candidate_intents if isinstance(row, dict)],
            "no_intents_marker_path": "",
            "output_count": len(candidate_intents),
        },
        "blocking_codes": blocking_codes,
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": str(readiness.get("readiness_mode") or "").strip(),
        "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
        "mode_specific_blocker": bool(preopen_build_mode),
        "human_readable_summary": f"Intent generation reached {final_status} for {day_utc}.",
    }
    atomic_write_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA_RELPATH_V1,
    )
    print(
        json.dumps(
            {
                "final_status": final_status,
                "first_blocker_code": first_blocker_code,
                "generation_run_id": payload["generation_run_id"],
                "intent_output_count": len(candidate_intents),
                "selected_intent_pointer_path": str(arbitration.get("selected_intent_pointer_path") or ""),
                "no_intents_marker_path": "",
                "path": str(output_path),
                "sha256": _sha256_file(output_path),
            },
            sort_keys=True,
        )
    )
    return return_code

    day_dir = _intents_dir(truth_root=intent_truth_root, day_utc=day_utc)
    marker_path = _marker_path(truth_root=intent_truth_root, day_utc=day_utc)
    existing_intent_paths = [str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)]
    marker_exists = marker_path.exists() and marker_path.is_file()
    producer_results: List[Dict[str, Any]] = []
    blocking_codes: set[str] = set()
    first_blocker_code = ""
    first_blocker_artifact_path = ""

    producer_topology = [
        {
            "engine_id": spec.engine_id,
            "script_path": str(spec.script_path),
            "script_sha256": _sha256_file(spec.script_path),
            "registry_runner_sha256": spec.registry_runner_sha256,
            "required": True,
        }
        for spec in producer_specs
    ]

    if existing_intent_paths and marker_exists:
        blocking_codes.add("TRADING_DAY_INTENT_GENERATION_CONTRADICTORY_EXISTING_INTENTS_AND_MARKER")
        first_blocker_code = "TRADING_DAY_INTENT_GENERATION_CONTRADICTORY_EXISTING_INTENTS_AND_MARKER"
        first_blocker_artifact_path = str(marker_path)
        final_status = "BLOCKED_BY_DEFECT"
    elif existing_intent_paths:
        final_status = "INTENTS_PRESENT"
        for spec in producer_specs:
            producer_results.append(
                _producer_result(
                    logical_name=spec.engine_id,
                    engine_id=spec.engine_id,
                    script_path=spec.script_path,
                    script_sha256=_sha256_file(spec.script_path),
                    registry_runner_sha256=spec.registry_runner_sha256,
                    status="SKIPPED",
                    return_code=None,
                    reason_codes=["EXISTING_CANONICAL_INTENTS_PRESENT"],
                    stdout="",
                    stderr="",
                    output_paths=[],
                )
            )
    elif marker_exists:
        _validate_existing_marker(marker_path, day_utc)
        final_status = "VALID_ZERO"
        for spec in producer_specs:
            producer_results.append(
                _producer_result(
                    logical_name=spec.engine_id,
                    engine_id=spec.engine_id,
                    script_path=spec.script_path,
                    script_sha256=_sha256_file(spec.script_path),
                    registry_runner_sha256=spec.registry_runner_sha256,
                    status="SKIPPED",
                    return_code=None,
                    reason_codes=["EXISTING_VALID_ZERO_MARKER_PRESENT"],
                    stdout="",
                    stderr="",
                    output_paths=[],
                )
            )
    else:
        for spec in producer_specs:
            script_sha = _sha256_file(spec.script_path)
            if spec.registry_runner_sha256 and script_sha.lower() != spec.registry_runner_sha256:
                reason_code = "ENGINE_RUNNER_SHA256_MISMATCH"
                blocking_codes.add(reason_code)
                if not first_blocker_code:
                    first_blocker_code = reason_code
                    first_blocker_artifact_path = str(spec.script_path)
                producer_results.append(
                    _producer_result(
                        logical_name=spec.engine_id,
                        engine_id=spec.engine_id,
                        script_path=spec.script_path,
                        script_sha256=script_sha,
                        registry_runner_sha256=spec.registry_runner_sha256,
                        status="BLOCKED_BY_DEFECT",
                        return_code=None,
                        reason_codes=[reason_code],
                        stdout="",
                        stderr="",
                        output_paths=[],
                    )
                )
                continue

            if spec.engine_id == DEFENSIVE_TAIL_ENGINE_ID:
                prep_result = _run(_defensive_tail_inputs_cmd(day_utc=day_utc), truth_root=intent_truth_root)
                if prep_result["return_code"] != 0:
                    reason_code = _classify_producer_nonzero_reason(
                        stdout=prep_result["stdout"],
                        stderr=prep_result["stderr"],
                    )
                    blocking_codes.add(reason_code)
                    if not first_blocker_code:
                        first_blocker_code = reason_code
                        first_blocker_artifact_path = str(DEFENSIVE_TAIL_INPUT_BRIDGE_TOOL)
                    producer_results.append(
                        _producer_result(
                            logical_name=spec.engine_id,
                            engine_id=spec.engine_id,
                            script_path=spec.script_path,
                            script_sha256=script_sha,
                            registry_runner_sha256=spec.registry_runner_sha256,
                            status="BLOCKED_BY_DEFECT",
                            return_code=prep_result["return_code"],
                            reason_codes=[reason_code],
                            stdout=prep_result["stdout"],
                            stderr=prep_result["stderr"],
                            output_paths=[],
                        )
                    )
                    continue

            before = {str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)}
            result = _run(_producer_cmd(spec=spec, day_utc=day_utc, truth_root=intent_truth_root), truth_root=intent_truth_root)
            after = {str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)}
            new_outputs = sorted(after - before)
            if result["return_code"] != 0:
                reason_code = _classify_producer_nonzero_reason(
                    stdout=result["stdout"],
                    stderr=result["stderr"],
                )
                blocking_codes.add(reason_code)
                if not first_blocker_code:
                    first_blocker_code = reason_code
                    first_blocker_artifact_path = str(spec.script_path)
                producer_results.append(
                    _producer_result(
                        logical_name=spec.engine_id,
                        engine_id=spec.engine_id,
                        script_path=spec.script_path,
                        script_sha256=script_sha,
                        registry_runner_sha256=spec.registry_runner_sha256,
                        status="BLOCKED_BY_DEFECT",
                        return_code=result["return_code"],
                        reason_codes=[reason_code],
                        stdout=result["stdout"],
                        stderr=result["stderr"],
                        output_paths=new_outputs,
                    )
                )
            elif len(new_outputs) > 1:
                reason_code = "PRODUCER_MULTIPLE_OUTPUTS_UNEXPECTED"
                blocking_codes.add(reason_code)
                if not first_blocker_code:
                    first_blocker_code = reason_code
                    first_blocker_artifact_path = str(spec.script_path)
                producer_results.append(
                    _producer_result(
                        logical_name=spec.engine_id,
                        engine_id=spec.engine_id,
                        script_path=spec.script_path,
                        script_sha256=script_sha,
                        registry_runner_sha256=spec.registry_runner_sha256,
                        status="BLOCKED_BY_DEFECT",
                        return_code=result["return_code"],
                        reason_codes=[reason_code],
                        stdout=result["stdout"],
                        stderr=result["stderr"],
                        output_paths=new_outputs,
                    )
                )
            elif new_outputs:
                producer_results.append(
                    _producer_result(
                        logical_name=spec.engine_id,
                        engine_id=spec.engine_id,
                        script_path=spec.script_path,
                        script_sha256=script_sha,
                        registry_runner_sha256=spec.registry_runner_sha256,
                        status="INTENT_WRITTEN",
                        return_code=result["return_code"],
                        reason_codes=["INTENT_OUTPUT_CREATED"],
                        stdout=result["stdout"],
                        stderr=result["stderr"],
                        output_paths=new_outputs,
                    )
                )
            elif _classify_no_intent(result["stdout"]):
                producer_results.append(
                    _producer_result(
                        logical_name=spec.engine_id,
                        engine_id=spec.engine_id,
                        script_path=spec.script_path,
                        script_sha256=script_sha,
                        registry_runner_sha256=spec.registry_runner_sha256,
                        status="NO_INTENT",
                        return_code=result["return_code"],
                        reason_codes=["NO_INTENT_DECLARED"],
                        stdout=result["stdout"],
                        stderr=result["stderr"],
                        output_paths=[],
                    )
                )
            else:
                reason_code = "PRODUCER_ZERO_RC_WITHOUT_CANONICAL_OUTPUT"
                blocking_codes.add(reason_code)
                if not first_blocker_code:
                    first_blocker_code = reason_code
                    first_blocker_artifact_path = str(spec.script_path)
                producer_results.append(
                    _producer_result(
                        logical_name=spec.engine_id,
                        engine_id=spec.engine_id,
                        script_path=spec.script_path,
                        script_sha256=script_sha,
                        registry_runner_sha256=spec.registry_runner_sha256,
                        status="BLOCKED_BY_DEFECT",
                        return_code=result["return_code"],
                        reason_codes=[reason_code],
                        stdout=result["stdout"],
                        stderr=result["stderr"],
                        output_paths=[],
                    )
                )

        existing_intent_paths = [str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)]
        marker_exists = marker_path.exists() and marker_path.is_file()

        if marker_exists and existing_intent_paths:
            blocking_codes.add("TRADING_DAY_INTENT_GENERATION_CONTRADICTORY_MARKER_AND_INTENTS")
            if not first_blocker_code:
                first_blocker_code = "TRADING_DAY_INTENT_GENERATION_CONTRADICTORY_MARKER_AND_INTENTS"
                first_blocker_artifact_path = str(marker_path)
            final_status = "BLOCKED_BY_DEFECT"
        elif any(row["status"] == "BLOCKED_BY_DEFECT" for row in producer_results):
            final_status = "BLOCKED_BY_DEFECT"
        elif existing_intent_paths:
            final_status = "INTENTS_PRESENT"
        elif producer_results and all(row["status"] == "NO_INTENT" for row in producer_results):
            marker_result = _run(
                [
                    sys.executable,
                    str(NO_INTENTS_MARKER_TOOL),
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(intent_truth_root),
                ],
                truth_root=intent_truth_root,
            )
            marker_exists = marker_path.exists() and marker_path.is_file()
            if marker_result["return_code"] != 0 or not marker_exists:
                blocking_codes.add("TRADING_DAY_INTENT_GENERATION_VALID_ZERO_WRITE_FAILED")
                if not first_blocker_code:
                    first_blocker_code = "TRADING_DAY_INTENT_GENERATION_VALID_ZERO_WRITE_FAILED"
                    first_blocker_artifact_path = str(marker_path)
                producer_results.append(
                    _producer_result(
                        logical_name="NO_INTENTS_DAY_MARKER_V1",
                        engine_id="",
                        script_path=NO_INTENTS_MARKER_TOOL,
                        script_sha256=_sha256_file(NO_INTENTS_MARKER_TOOL),
                        registry_runner_sha256="",
                        status="BLOCKED_BY_DEFECT",
                        return_code=marker_result["return_code"],
                        reason_codes=["VALID_ZERO_WRITE_FAILED"],
                        stdout=marker_result["stdout"],
                        stderr=marker_result["stderr"],
                        output_paths=[],
                    )
                )
                final_status = "BLOCKED_BY_DEFECT"
            else:
                _validate_existing_marker(marker_path, day_utc)
                producer_results.append(
                    _producer_result(
                        logical_name="NO_INTENTS_DAY_MARKER_V1",
                        engine_id="",
                        script_path=NO_INTENTS_MARKER_TOOL,
                        script_sha256=_sha256_file(NO_INTENTS_MARKER_TOOL),
                        registry_runner_sha256="",
                        status="VALID_ZERO_WRITTEN",
                        return_code=marker_result["return_code"],
                        reason_codes=["VALID_ZERO_MARKER_CREATED"],
                        stdout=marker_result["stdout"],
                        stderr=marker_result["stderr"],
                        output_paths=[str(marker_path)],
                    )
                )
                final_status = "VALID_ZERO"
        else:
            blocking_codes.add("TRADING_DAY_INTENT_GENERATION_NO_CANONICAL_OUTPUT")
            if not first_blocker_code:
                first_blocker_code = "TRADING_DAY_INTENT_GENERATION_NO_CANONICAL_OUTPUT"
                first_blocker_artifact_path = str(day_dir)
            final_status = "BLOCKED_BY_DEFECT"

    marker_exists = marker_path.exists() and marker_path.is_file()
    existing_intent_paths = [str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)]
    if not first_blocker_code and final_status.startswith("BLOCKED"):
        first_blocker_code = _first_nonempty(sorted(blocking_codes))
    if not first_blocker_artifact_path and final_status == "VALID_ZERO":
        first_blocker_artifact_path = str(marker_path)

    payload = {
        "schema_id": "trading_day_intent_generation",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
        "day_utc": day_utc,
        "generation_run_id": generation_run_id,
        "produced_at_utc": produced_at_utc,
        "producer": producer_block_v1(module="ops/tools/run_trading_day_intent_generation_v1.py"),
        "truth_root": str(decision_truth_root),
        "active_engine_registry_path": str(registry_path),
        "active_engine_registry_sha256": registry_sha,
        "producer_topology": producer_topology,
        "skipped_active_engines": skipped_active_engines,
        "producer_results": producer_results,
        "final_status": final_status,
        "first_blocker_code": first_blocker_code,
        "first_blocker_artifact_path": first_blocker_artifact_path,
        "canonical_outputs": {
            "intents_dir": str(day_dir),
            "intent_output_paths": existing_intent_paths,
            "no_intents_marker_path": str(marker_path) if marker_exists else "",
            "output_count": len(existing_intent_paths),
        },
        "blocking_codes": sorted(blocking_codes),
        "human_readable_summary": (
            f"Intent generation reached {final_status} for {day_utc}."
            if final_status in {"INTENTS_PRESENT", "VALID_ZERO"}
            else f"Intent generation blocked for {day_utc}. First blocker: {first_blocker_code or 'UNKNOWN'}."
        ),
    }
    ref = atomic_write_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA_RELPATH_V1,
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "generation_run_id": generation_run_id,
                "final_status": final_status,
                "first_blocker_code": first_blocker_code,
                "intent_output_count": len(existing_intent_paths),
                "no_intents_marker_path": str(marker_path) if marker_exists else "",
            },
            sort_keys=True,
        )
    )
    if final_status in {"INTENTS_PRESENT", "VALID_ZERO"}:
        return 0
    if final_status == "BLOCKED_VALID":
        return 2
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
