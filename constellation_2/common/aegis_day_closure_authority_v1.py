from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.execution_evidence_current_head_v1 import current_head_output_path
from constellation_2.common.submission_index_v1 import submission_index_output_path


SCHEMA_VERSION = "aegis_day_closure_authority.v1"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"INVALID_JSON_OBJECT:{path}")
    return payload


def _block(code: str, path: Path, detail: str) -> dict[str, str]:
    return {"code": code, "path": str(path), "detail": detail}


def closure_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "aegis_day_closure_authority_v1" / day_utc / "aegis_day_closure_authority.v1.json").resolve()


def evaluate_aegis_day_closure_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    trade_submit_readiness_path: Path,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve()
    blocking_evidence: list[dict[str, str]] = []
    source_surfaces: dict[str, Any] = {}

    required_surfaces = {
        "trading_day_state_machine": (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").resolve(),
        "submit_boundary_status": (truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").resolve(),
        "paper_session_ledger": (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").resolve(),
        "latest_execution_pointer": (execution_root / "execution_evidence_v1" / "latest_pointer.v1.json").resolve(),
        "trade_submit_readiness": Path(trade_submit_readiness_path).resolve(),
        "submission_index": submission_index_output_path(execution_root=execution_root, day_utc=day_utc),
        "execution_current_head": current_head_output_path(execution_root=execution_root, day_utc=day_utc),
    }
    execution_stream_failure_path = (execution_root / "execution_stream_v1" / "failures" / day_utc / "failure.json").resolve()

    payloads: dict[str, dict[str, Any]] = {}
    for logical_name, path in required_surfaces.items():
        if not path.exists() or not path.is_file():
            blocking_evidence.append(
                _block(
                    "REQUIRED_CONTROL_SURFACE_MISSING",
                    path,
                    f"required control surface missing: {logical_name}",
                )
            )
            continue
        try:
            payloads[logical_name] = _read_json(path)
        except Exception as exc:
            blocking_evidence.append(
                _block(
                    "REQUIRED_CONTROL_SURFACE_INVALID",
                    path,
                    f"invalid control surface JSON for {logical_name}: {type(exc).__name__}",
                )
            )

    state_machine_path = required_surfaces["trading_day_state_machine"]
    submit_boundary_path = required_surfaces["submit_boundary_status"]
    ledger_path = required_surfaces["paper_session_ledger"]
    pointer_path = required_surfaces["latest_execution_pointer"]
    readiness_path = required_surfaces["trade_submit_readiness"]
    submission_index_path = required_surfaces["submission_index"]
    current_head_path = required_surfaces["execution_current_head"]

    state_machine = payloads.get("trading_day_state_machine", {})
    state_machine_status = str(state_machine.get("final_start_decision") or "").strip().upper()
    source_surfaces["trading_day_state_machine"] = {
        "path": str(state_machine_path),
        "observed_status": state_machine_status,
    }
    if state_machine_status == "BLOCKED_BY_DEFECT":
        blocking_evidence.append(
            _block(
                "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES",
                state_machine_path,
                "trading_day_state_machine final_start_decision=BLOCKED_BY_DEFECT",
            )
        )

    submit_boundary = payloads.get("submit_boundary_status", {})
    submit_boundary_status = str(submit_boundary.get("boundary_status") or "").strip().upper()
    source_surfaces["submit_boundary_status"] = {
        "path": str(submit_boundary_path),
        "observed_status": submit_boundary_status,
    }
    if submit_boundary_status == "AUTHORIZED" and state_machine_status == "BLOCKED_BY_DEFECT":
        blocking_evidence.append(
            _block(
                "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES",
                submit_boundary_path,
                "submit_boundary_status=AUTHORIZED while trading_day_state_machine=BLOCKED_BY_DEFECT",
            )
        )

    ledger = payloads.get("paper_session_ledger", {})
    post_submit = ledger.get("post_submit_lifecycle") if isinstance(ledger.get("post_submit_lifecycle"), dict) else {}
    submit_lifecycle = ledger.get("submit_lifecycle") if isinstance(ledger.get("submit_lifecycle"), dict) else {}
    lineage_status = str(post_submit.get("lineage_status") or "").strip().upper()
    submit_result_status = str(submit_lifecycle.get("submit_result_status") or "").strip().upper()
    source_surfaces["paper_session_ledger"] = {
        "path": str(ledger_path),
        "observed_status": lineage_status or submit_result_status,
    }
    if lineage_status == "GAP":
        blocking_evidence.append(
            _block(
                "POST_SUBMIT_LINEAGE_GAP",
                ledger_path,
                "paper_session_ledger post_submit_lifecycle.lineage_status=GAP",
            )
        )
    if submit_result_status == "FAIL":
        blocking_evidence.append(
            _block(
                "POST_SUBMIT_LINEAGE_GAP",
                ledger_path,
                "paper_session_ledger submit_lifecycle.submit_result_status=FAIL",
            )
        )

    source_surfaces["execution_stream_failure"] = {
        "path": str(execution_stream_failure_path),
        "present": bool(execution_stream_failure_path.exists() and execution_stream_failure_path.is_file()),
    }
    if execution_stream_failure_path.exists() and execution_stream_failure_path.is_file():
        blocking_evidence.append(
            _block(
                "EXECUTION_STREAM_FAILURE_PRESENT",
                execution_stream_failure_path,
                "execution_stream failure artifact is present",
            )
        )

    pointer = payloads.get("latest_execution_pointer", {})
    pointer_day = str(pointer.get("day_utc") or pointer.get("asof_day_utc") or "").strip()
    pointer_status = str(pointer.get("status") or "").strip().upper()
    source_surfaces["latest_execution_pointer"] = {
        "path": str(pointer_path),
        "observed_day": pointer_day,
        "observed_status": pointer_status,
    }
    if pointer_day and pointer_day != day_utc:
        blocking_evidence.append(
            _block(
                "EXECUTION_POINTER_DAY_MISMATCH",
                pointer_path,
                f"latest_pointer day mismatch expected={day_utc} observed={pointer_day}",
            )
        )
    if pointer_status and pointer_status not in {"OK", "PASS"}:
        blocking_evidence.append(
            _block(
                "STALE_EXECUTION_POINTER",
                pointer_path,
                f"latest_pointer status indicates failure: {pointer_status}",
            )
        )

    readiness = payloads.get("trade_submit_readiness", {})
    readiness_status = str(readiness.get("state") or readiness.get("status") or "").strip().upper()
    reasons = [str(item or "").strip() for item in (readiness.get("reasons") or [])]
    source_surfaces["trade_submit_readiness"] = {
        "path": str(readiness_path),
        "observed_status": readiness_status,
        "observed_reason": ";".join(reasons),
    }
    if readiness_status in {"OK", "PASS"}:
        incoherent = False
        for reason in reasons:
            token = reason.upper()
            if "NOT_PASS" in token or token.startswith("FAIL:"):
                incoherent = True
                break
        if incoherent:
            blocking_evidence.append(
                _block(
                    "SUBMIT_READINESS_POLICY_INCOHERENT",
                    readiness_path,
                    "trade_submit_readiness reports OK while reasons include policy not-pass/fail",
                )
            )

    submission_index = payloads.get("submission_index", {})
    submission_index_status = str(submission_index.get("status") or "").strip().upper()
    source_surfaces["submission_index"] = {
        "path": str(submission_index_path),
        "observed_status": submission_index_status,
    }
    if submission_index_status != "PASS":
        blocking_evidence.append(
            _block(
                "POST_SUBMIT_LINEAGE_GAP",
                submission_index_path,
                f"submission_index status={submission_index_status or 'MISSING'}",
            )
        )

    current_head = payloads.get("execution_current_head", {})
    current_head_status = str(current_head.get("status") or "").strip().upper()
    source_surfaces["execution_current_head"] = {
        "path": str(current_head_path),
        "observed_status": current_head_status,
    }
    if current_head_status != "PASS":
        blocking_evidence.append(
            _block(
                "STALE_EXECUTION_POINTER",
                current_head_path,
                f"execution current-head status={current_head_status or 'MISSING'}",
            )
        )

    status = "PASS" if not blocking_evidence else "FAIL"
    canonical_blocker = blocking_evidence[0]["code"] if blocking_evidence else None
    return {
        "schema_version": SCHEMA_VERSION,
        "day": day_utc,
        "status": status,
        "canonical_blocker": canonical_blocker,
        "blocking_evidence": blocking_evidence,
        "source_surfaces": source_surfaces,
        "generated_at_utc": _utc_now_iso(),
    }


def write_aegis_day_closure_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = closure_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return output_path
