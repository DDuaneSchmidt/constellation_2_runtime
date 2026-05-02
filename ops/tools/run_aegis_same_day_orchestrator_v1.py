#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_market_calendar_record_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1


PRODUCER = "ops/tools/run_aegis_same_day_orchestrator_v1.py"
DAG_PATH = REPO_ROOT / "governance/02_REGISTRIES/aegis_same_day_producer_dag_v1.json"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/orchestration_run.v1.schema.json"
REPORT_FAMILY = "orchestration_run_v1"
REPORT_FILENAME = "orchestration_run.v1.json"
PHASES = {"PRE_OPEN", "MARKET_OPEN", "INTRADAY", "PRE_SUBMIT", "EOD"}


@dataclass(frozen=True)
class OrchestratorContext:
    day_utc: str
    phase: str
    truth_root: Path
    execution_root: Path
    runtime_root: Path
    environment: str
    ib_account: str
    intent_id: str
    dry_run: bool = False


def report_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def _load_json(path: Path) -> dict[str, Any]:
    payload = read_json_v1(path)
    if not payload:
        raise ValueError(f"JSON_MISSING_OR_INVALID:path={path}")
    return payload


def load_producer_dag_v1(path: Path = DAG_PATH) -> dict[str, Any]:
    dag = _load_json(path)
    nodes = dag.get("producer_nodes") if isinstance(dag.get("producer_nodes"), list) else []
    ids = [str(node.get("producer_id") or "") for node in nodes if isinstance(node, dict)]
    if len(ids) != len(set(ids)):
        raise ValueError("PRODUCER_DAG_DUPLICATE_PRODUCER_ID")
    return dag


def _node_map(dag: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(node.get("producer_id")): node
        for node in dag.get("producer_nodes", [])
        if isinstance(node, dict) and str(node.get("producer_id") or "")
    }


def phase_nodes_v1(dag: dict[str, Any], phase: str) -> list[dict[str, Any]]:
    phase_text = str(phase or "").strip().upper()
    scheduler = dag.get("phase_scheduler") if isinstance(dag.get("phase_scheduler"), dict) else {}
    phase_spec = scheduler.get(phase_text) if isinstance(scheduler.get(phase_text), dict) else None
    if phase_spec is None:
        raise ValueError(f"UNKNOWN_PHASE:{phase_text}")
    required = [str(item) for item in phase_spec.get("required_producers", [])]
    allowed = set(str(item) for item in phase_spec.get("allowed_producers", []))
    forbidden = set(str(item) for item in phase_spec.get("forbidden_producers", []))
    nodes = _node_map(dag)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_with_deps(producer_id: str) -> None:
        if producer_id in seen:
            return
        if producer_id in forbidden:
            raise ValueError(f"PHASE_FORBIDS_REQUIRED_PRODUCER:{phase_text}:{producer_id}")
        node = nodes.get(producer_id)
        if node is None:
            raise ValueError(f"PRODUCER_DAG_UNKNOWN_NODE:{producer_id}")
        for dep in node.get("inputs", []) if isinstance(node.get("inputs"), list) else []:
            dep_id = str(dep)
            if dep_id in allowed:
                add_with_deps(dep_id)
        if producer_id not in allowed:
            raise ValueError(f"PHASE_PRODUCER_NOT_ALLOWED:{phase_text}:{producer_id}")
        seen.add(producer_id)
        out.append(node)

    for producer_id in required:
        add_with_deps(producer_id)
    return out


def _format_template(template: str, ctx: OrchestratorContext) -> str:
    return str(template or "").format(
        day_utc=ctx.day_utc,
        truth_root=str(ctx.truth_root),
        execution_root=str(ctx.execution_root),
        runtime_root=str(ctx.runtime_root),
        environment=ctx.environment,
        ib_account=ctx.ib_account,
        intent_id=ctx.intent_id,
    )


def _glob_from_template(path_text: str) -> list[Path]:
    if "<submission_id>" not in path_text:
        return [Path(path_text).expanduser().resolve()]
    return sorted(Path(path_text.replace("<submission_id>", "*")).expanduser().parent.glob(Path(path_text).name))


def _read_jsonl_first_last(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    first: dict[str, Any] = {}
    last: dict[str, Any] = {}
    if not path.exists() or not path.is_file():
        return first, last
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            if not first:
                first = row
            last = row
    return first, last


def _payload_day(payload: dict[str, Any]) -> str:
    for key in ("day_utc", "target_day", "trading_day", "date"):
        text = str(payload.get(key) or "").strip()
        if text:
            return text[:10]
    return ""


def _timestamp_day(payload: dict[str, Any]) -> str:
    for key in ("received_utc", "generated_at_utc", "generated_at", "produced_at_utc", "produced_utc", "evaluated_at_utc"):
        text = str(payload.get(key) or "").strip()
        if len(text) >= 10:
            return text[:10]
    return ""


def _schema_status(schema_path: str, payload: dict[str, Any]) -> tuple[str, str]:
    if not str(schema_path or "").strip():
        return "NOT_AVAILABLE", ""
    path = (REPO_ROOT / str(schema_path)).resolve()
    if not path.exists():
        return "FAIL", "SCHEMA_MISSING"
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
        errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    except Exception:
        return "FAIL", "SCHEMA_INVALID"
    if errors:
        return "FAIL", "SCHEMA_VALIDATION_FAILED"
    return "PASS", ""


def validate_artifact_v1(spec: dict[str, Any], *, ctx: OrchestratorContext, producer_id: str) -> dict[str, Any]:
    path_template = _format_template(str(spec.get("path") or ""), ctx)
    candidates = _glob_from_template(path_template)
    path = next((item for item in candidates if item.exists() and item.is_file()), candidates[0] if candidates else Path(path_template))
    artifact_type = str(spec.get("artifact_type") or "json").lower()
    artifact_id = str(spec.get("artifact_id") or producer_id)
    blocker = ""
    schema_status = "SKIPPED"
    target_day_status = "SKIPPED"
    truth_root_status = "SKIPPED"
    freshness_status = "SKIPPED"

    if not path.exists() or not path.is_file():
        blocker = f"{artifact_id.upper()}_MISSING"
        return {
            "producer_id": producer_id,
            "artifact_id": artifact_id,
            "artifact_path": str(path),
            "status": "FAIL",
            "blocker_code": blocker,
            "schema_status": schema_status,
            "target_day_status": target_day_status,
            "truth_root_status": truth_root_status,
            "freshness_status": freshness_status,
        }

    expected_root = ctx.truth_root if str(spec.get("truth_root_required") or "") == "truth_root" else ctx.execution_root
    try:
        path.relative_to(expected_root)
        truth_root_status = "PASS"
    except ValueError:
        truth_root_status = "FAIL"
        blocker = f"{artifact_id.upper()}_WRONG_ROOT"

    payload: dict[str, Any] = {}
    if artifact_type == "jsonl":
        _first, last = _read_jsonl_first_last(path)
        payload = last
        schema_status = "NOT_AVAILABLE"
    else:
        payload = read_json_v1(path)
        if not payload:
            schema_status = "FAIL"
            blocker = blocker or f"{artifact_id.upper()}_INVALID_JSON"
        else:
            schema_status, schema_blocker = _schema_status(str(spec.get("schema_path") or ""), payload)
            blocker = blocker or (f"{artifact_id.upper()}_{schema_blocker}" if schema_blocker else "")

    if spec.get("target_day_required") is True:
        day = _payload_day(payload) or _timestamp_day(payload)
        target_day_status = "PASS" if day == ctx.day_utc else "FAIL"
        if target_day_status == "FAIL":
            blocker = blocker or f"{artifact_id.upper()}_WRONG_DAY"

    freshness_seconds = spec.get("freshness_seconds")
    if freshness_seconds is None:
        freshness_status = "NOT_REQUIRED"
    else:
        try:
            age_seconds = max(0.0, time.time() - path.stat().st_mtime)
            freshness_status = "PASS" if age_seconds <= float(freshness_seconds) else "FAIL"
        except Exception:
            freshness_status = "FAIL"
        if freshness_status == "FAIL":
            blocker = blocker or f"{artifact_id.upper()}_STALE"

    status = "PASS" if not blocker and schema_status != "FAIL" and target_day_status != "FAIL" and truth_root_status != "FAIL" and freshness_status != "FAIL" else "FAIL"
    return {
        "producer_id": producer_id,
        "artifact_id": artifact_id,
        "artifact_path": str(path),
        "status": status,
        "blocker_code": blocker,
        "schema_status": schema_status,
        "target_day_status": target_day_status,
        "truth_root_status": truth_root_status,
        "freshness_status": freshness_status,
    }


def _default_command_runner(command: str) -> dict[str, Any]:
    proc = subprocess.run(command, shell=True, cwd=str(REPO_ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    return {
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or ""),
        "stderr": str(proc.stderr or ""),
    }


def _calendar_session_state(*, truth_root: Path, day_utc: str) -> str:
    try:
        state = resolve_market_calendar_record_v1(truth_root=truth_root, day_utc=day_utc)
    except Exception:
        state = {"status": "ERROR", "record": None}
    record = state.get("record") if isinstance(state, dict) else None
    if isinstance(record, dict) and record.get("is_trading_session") is True:
        return "TRADING_SESSION"
    if isinstance(record, dict) and record.get("is_trading_session") is False:
        return "NON_TRADING_DAY"
    return "UNKNOWN_CALENDAR_STATE"


def run_orchestration_v1(
    *,
    ctx: OrchestratorContext,
    dag_path: Path = DAG_PATH,
    command_runner: Callable[[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    started = now_iso_v1()
    dag = load_producer_dag_v1(dag_path)
    session_state = _calendar_session_state(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    run_id = f"orchestration:{ctx.day_utc}:{ctx.phase}:{started}"
    producer_results: list[dict[str, Any]] = []
    artifact_results: list[dict[str, Any]] = []
    blockers: list[str] = []
    recovery_commands: list[str] = []
    runner = command_runner or _default_command_runner

    if session_state == "NON_TRADING_DAY":
        final_status = "SKIPPED_NON_TRADING_DAY"
    else:
        final_status = "PASS"
        for node in phase_nodes_v1(dag, ctx.phase):
            producer_id = str(node["producer_id"])
            command = _format_template(str(node.get("command") or ""), ctx)
            recovery = str(node.get("recovery_command") or "")
            mode = str(node.get("execution_mode") or "ONE_SHOT").upper()
            if mode == "SERVICE_EVIDENCE_ONLY" or ctx.dry_run:
                run_result = {"returncode": 0, "stdout": "", "stderr": ""}
            else:
                run_result = runner(command)
            producer_status = "PASS" if int(run_result.get("returncode") or 0) == 0 else "FAILED"
            node_artifacts: list[dict[str, Any]] = []
            if producer_status == "PASS":
                for output in node.get("outputs", []) if isinstance(node.get("outputs"), list) else []:
                    result = validate_artifact_v1(output, ctx=ctx, producer_id=producer_id)
                    node_artifacts.append(result)
                    artifact_results.append(result)
            validation_status = "PASS" if node_artifacts and all(row["status"] == "PASS" for row in node_artifacts) else ("SKIPPED" if producer_status != "PASS" else "FAIL")
            blocker = ""
            if producer_status != "PASS":
                blocker = f"{producer_id.upper()}_PRODUCER_FAILED"
            elif validation_status == "FAIL":
                blocker = next((row["blocker_code"] for row in node_artifacts if row["blocker_code"]), f"{producer_id.upper()}_ARTIFACT_INVALID")
            artifact_path = next((row["artifact_path"] for row in node_artifacts if row.get("artifact_path")), "")
            producer_results.append(
                {
                    "producer_id": producer_id,
                    "command": command,
                    "status": "PASS" if not blocker else ("FAILED" if producer_status != "PASS" else "BLOCKED"),
                    "artifact_path": artifact_path,
                    "validation_status": validation_status,
                    "blocker_code": blocker,
                    "recovery_command": recovery,
                }
            )
            if blocker:
                blockers.append(blocker)
                if recovery:
                    recovery_commands.append(recovery)
                final_status = "BLOCKED"
                break

    payload = {
        "schema_id": "orchestration_run_v1",
        "schema_version": "orchestration_run.v1",
        "orchestration_run_id": run_id,
        "day_utc": ctx.day_utc,
        "phase": ctx.phase,
        "session_state": session_state,
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
        "runtime_root": str(ctx.runtime_root),
        "started_at": started,
        "finished_at": now_iso_v1(),
        "producer_results": producer_results,
        "artifact_results": artifact_results,
        "blockers": blockers,
        "recovery_commands": sorted(set(recovery_commands)),
        "final_status": final_status,
        "readiness_authority": "aegis_control_plane_v1",
        "submit_authority": "aegis_submit_enforcement_v1",
        "readiness_effect": "NONE",
        "submit_effect": "NONE",
    }
    return payload


def validate_ledger_v1(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("ORCHESTRATION_RUN_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def write_ledger_v1(*, ctx: OrchestratorContext, payload: dict[str, Any], command: str) -> Path:
    path = report_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name=PRODUCER,
        producer_command=command,
        input_artifacts=[DAG_PATH],
        output_artifacts=[path],
        schema_versions={"orchestration_run_v1": "orchestration_run.v1"},
    )
    validate_ledger_v1(payload)
    write_json_v1(path, payload)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_same_day_orchestrator_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--phase", required=True, choices=sorted(PHASES))
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--execution_root", required=True)
    parser.add_argument("--runtime_root", default="")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--ib_account", default="DUO847203")
    parser.add_argument("--intent_id", default="")
    parser.add_argument("--dag_path", default=str(DAG_PATH))
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args(argv)
    day = parse_day_utc_v1(args.day_utc)
    truth_root = Path(args.truth_root).expanduser().resolve()
    execution_root = Path(args.execution_root).expanduser().resolve()
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else truth_root
    ctx = OrchestratorContext(
        day_utc=day,
        phase=str(args.phase).strip().upper(),
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        environment=str(args.environment).strip().upper(),
        ib_account=str(args.ib_account).strip(),
        intent_id=str(args.intent_id or "").strip(),
        dry_run=bool(args.dry_run),
    )
    payload = run_orchestration_v1(ctx=ctx, dag_path=Path(args.dag_path).expanduser().resolve())
    command = "PYTHONPATH=\"$PWD\" python3 " + " ".join([PRODUCER, *sys.argv[1:]])
    path = write_ledger_v1(ctx=ctx, payload=payload, command=command)
    print(json.dumps({"status": payload["final_status"], "path": str(path), "blockers": payload["blockers"]}, sort_keys=True))
    return 0 if payload["final_status"] in {"PASS", "SKIPPED_NON_TRADING_DAY"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
