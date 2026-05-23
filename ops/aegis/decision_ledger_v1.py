from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from ops.aegis.runtime_evaluation_v1 import stable_hash_v1, stable_json_bytes_v1, validate_runtime_evaluation_v1


DECISION_FAMILY = "aegis_decision_ledger_v1"
DECISION_TRACE_FAMILY = "aegis_decision_trace_v1"


def decision_ledger_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "decisions" / DECISION_FAMILY / day_utc / "decisions.jsonl"


def decision_trace_json_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / DECISION_TRACE_FAMILY / day_utc / "decision_trace.v1.json"


def decision_trace_text_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / DECISION_TRACE_FAMILY / day_utc / "decision_trace.v1.txt"


def decision_hash_v1(decision: dict[str, Any]) -> str:
    candidate = dict(decision)
    candidate["decision_hash"] = ""
    return stable_hash_v1(candidate)


def read_decision_ledger_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = decision_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"Decision ledger row {index} is not an object")
        if payload.get("day_utc") != day_utc:
            raise ValueError(f"Decision ledger wrong-day row {index}")
        previous = rows[-1]["decision_hash"] if rows else ""
        if payload.get("previous_decision_hash") != previous:
            raise ValueError(f"Decision ledger hash chain broken at line {index}")
        if payload.get("decision_hash") != decision_hash_v1(payload):
            raise ValueError(f"Decision ledger hash mismatch at line {index}")
        rows.append(payload)
    return rows


def write_decision_ledger_v1(*, truth_root: Path, evaluation: dict[str, Any]) -> dict[str, str]:
    validate_runtime_evaluation_v1(evaluation)
    day_utc = str(evaluation["day_utc"])
    existing = read_decision_ledger_v1(truth_root=truth_root, day_utc=day_utc)
    previous_hash = existing[-1]["decision_hash"] if existing else ""
    path = decision_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for trace in evaluation.get("decision_trace") or []:
        capability = str(trace.get("capability") or "")
        row = {
            "schema_id": "aegis_capability_decision",
            "schema_version": "v1",
            "day_utc": day_utc,
            "run_id": evaluation["run_id"],
            "capability": capability,
            "allowed": bool(trace.get("allowed", False)),
            "direct_dependencies": trace.get("direct_dependencies") or [],
            "evidence_refs_used": trace.get("evidence_refs_used") or [],
            "validation_results": trace.get("validation_results") or [],
            "blocker_chain": _compressed_blockers(trace.get("blocker_chain") or []),
            "final_reason": str(trace.get("final_reason") or ""),
            "evaluation_hash": evaluation["deterministic_output_hash"],
            "previous_decision_hash": previous_hash,
            "decision_hash": "",
        }
        if not row["allowed"] and not row["final_reason"]:
            raise ValueError(f"Blocked capability missing reason: {capability}")
        row["decision_hash"] = decision_hash_v1(row)
        previous_hash = row["decision_hash"]
        rows.append(row)
    if rows:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            for row in rows:
                os.write(fd, stable_json_bytes_v1(row) + b"\n")
            os.fsync(fd)
        finally:
            os.close(fd)
    trace_payload = build_decision_trace_payload_v1(evaluation=evaluation, ledger_rows=rows)
    json_path = decision_trace_json_path_v1(truth_root=truth_root, day_utc=day_utc)
    txt_path = decision_trace_text_path_v1(truth_root=truth_root, day_utc=day_utc)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_bytes(stable_json_bytes_v1(trace_payload) + b"\n")
    txt_path.write_text(render_decision_trace_text_v1(trace_payload), encoding="utf-8")
    return {"decision_ledger": str(path), "decision_trace_json": str(json_path), "decision_trace_txt": str(txt_path)}


def build_decision_trace_payload_v1(*, evaluation: dict[str, Any], ledger_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "schema_id": "aegis_decision_trace",
        "schema_version": "v1",
        "day_utc": evaluation["day_utc"],
        "run_id": evaluation["run_id"],
        "evaluation_hash": evaluation["deterministic_output_hash"],
        "runtime_truth_classification": evaluation["runtime_truth_classification"],
        "highest_readiness_layer": evaluation["highest_readiness_layer"],
        "capabilities": evaluation["capabilities"],
        "decision_trace": evaluation["decision_trace"],
        "ledger_rows_written": len(ledger_rows or []),
    }


def render_decision_trace_text_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS DECISION TRACE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"run_id: {payload.get('run_id')}",
        f"evaluation_hash: {payload.get('evaluation_hash')}",
        f"runtime_truth_classification: {payload.get('runtime_truth_classification')}",
        "",
    ]
    for row in payload.get("decision_trace") or []:
        allowed = str(bool(row.get("allowed", False))).lower()
        lines.append(f"{row.get('capability')}={allowed}")
        reason = str(row.get("final_reason") or "")
        if reason:
            lines.append(f"because: {reason}")
        for blocker in _compressed_blockers(row.get("blocker_chain") or []):
            lines.append(f"  - {blocker}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _compressed_blockers(blockers: list[Any]) -> list[str]:
    return sorted(set(str(item) for item in blockers if str(item)))
