#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.event_append_transaction_v1 import (  # noqa: E402
    append_event_transaction_v1,
    canonical_payload_hash_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)
from ops.aegis.manual_intent_v1 import (  # noqa: E402
    ALLOWED_MANUAL_INTENTS,
    build_manual_intent_v1,
    manual_intent_path_v1,
    validate_manual_intent_v1,
    write_manual_intent_v1,
)
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1, stable_json_bytes_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1  # noqa: E402

ALLOWED_FUTURE_SKEW_SECONDS = 300
MANUAL_RECEIPT_PRODUCER_ID = "ops/tools/write_manual_intent_v1.py:manual_execution_receipt"
EOD_ACK_PRODUCER_ID = "ops/tools/write_manual_intent_v1.py:aegis_lite_eod_report_acknowledgment"
MANUAL_INTENT_PRODUCER_ID = "ops/tools/write_manual_intent_v1.py"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_manual_intent_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--operator-id", "--operator_id", dest="operator_id", required=True)
    parser.add_argument("--operator-intent", "--operator_intent", dest="operator_intent", required=True, choices=sorted(ALLOWED_MANUAL_INTENTS))
    parser.add_argument("--reason", required=True)
    parser.add_argument("--runtime-evaluation-hash", "--runtime_evaluation_hash", dest="runtime_evaluation_hash", required=True)
    parser.add_argument("--intent-timestamp-utc", "--intent_timestamp_utc", dest="intent_timestamp_utc", required=True)
    parser.add_argument("--mode", choices=["dry-run", "execute"], default="dry-run")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day_utc = str(args.day_utc)
    evaluation = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    errors = _runtime_evaluation_errors(evaluation=evaluation, day_utc=day_utc, supplied_hash=str(args.runtime_evaluation_hash))
    timestamp = _parse_utc(str(args.intent_timestamp_utc))
    now = datetime.now(UTC).replace(microsecond=0)
    if timestamp is None:
        errors.append("INTENT_TIMESTAMP_MALFORMED")
    elif timestamp > now + timedelta(seconds=ALLOWED_FUTURE_SKEW_SECONDS):
        errors.append("INTENT_TIMESTAMP_FUTURE")
    runtime_hash = str((evaluation or {}).get("deterministic_output_hash") or args.runtime_evaluation_hash)
    payload = build_manual_intent_v1(
        operator_id=str(args.operator_id),
        operator_intent=str(args.operator_intent),
        intent_timestamp_utc=str(args.intent_timestamp_utc),
        day_utc=day_utc,
        reason=str(args.reason),
        runtime_evaluation_hash=runtime_hash,
    )
    intent_reasons = validate_manual_intent_v1(payload, day_utc=day_utc, runtime_evaluation_hash=runtime_hash, generated_at_utc=None)
    errors.extend(intent_reasons)
    errors = sorted(set(errors))
    target = _target_for_intent(str(args.operator_intent))
    intent_path = manual_intent_path_v1(truth_root=root, day_utc=day_utc, intent_id=str(payload["intent_id"]))
    preview = {
        "ok": not errors,
        "mode": str(args.mode),
        "day_utc": day_utc,
        "operator_id": str(args.operator_id),
        "operator_intent": str(args.operator_intent),
        "intent_id": payload["intent_id"],
        "acknowledgment_hash": payload["acknowledgment_hash"],
        "reason_hash": stable_hash_v1(str(args.reason)),
        "runtime_evaluation_hash": runtime_hash,
        "canonical_runtime_evaluation_hash": str((evaluation or {}).get("deterministic_output_hash") or ""),
        "manual_intent_path": str(intent_path),
        "target_schema_id": target,
        "validation_errors": errors,
        "trade_advice_allowed": False,
        "autonomous_execution_allowed": False,
    }
    if args.mode == "dry-run":
        print(json.dumps(preview, sort_keys=True))
        return 0 if not errors else 2
    if errors:
        blocked = _append_manual_action_event(
            truth_root=root,
            day_utc=day_utc,
            payload=payload,
            event_type="ManualActionBlocked",
            validation_status="BLOCKED:" + ",".join(errors),
            output_hash="",
            artifact_paths=[],
        )
        print(json.dumps({**preview, "ok": False, "manual_action_event_id": blocked.get("event", {}).get("event_id"), "manual_action_status": blocked.get("status")}, sort_keys=True), file=sys.stderr)
        return 2

    written_intent_path = write_manual_intent_v1(truth_root=root, payload=payload)
    manual_action = _append_manual_action_event(
        truth_root=root,
        day_utc=day_utc,
        payload=payload,
        event_type="ManualActionAccepted",
        validation_status="ACCEPTED",
        output_hash=sha256_file_v1(written_intent_path),
        artifact_paths=[str(written_intent_path)],
    )
    manual_action_event_id = str(manual_action.get("event", {}).get("event_id") or "")
    evidence_results: list[dict[str, Any]] = []
    output_path = ""
    if target == "manual_execution_receipt":
        artifact_path, artifact_payload = _manual_execution_receipt_payload(root=root, day_utc=day_utc, payload=payload, manual_action_event_id=manual_action_event_id)
        _write_json_atomic(artifact_path, artifact_payload)
        output_path = str(artifact_path)
        evidence_results = _emit_target_evidence(root=root, day_utc=day_utc, artifact_path=artifact_path, artifact_payload=artifact_payload, producer_id=MANUAL_RECEIPT_PRODUCER_ID, payload=payload, manual_action_event_id=manual_action_event_id)
    elif target == "aegis_lite_eod_report":
        artifact_path, artifact_payload = _aegis_lite_eod_ack_payload(root=root, day_utc=day_utc, payload=payload, manual_action_event_id=manual_action_event_id)
        _write_json_atomic(artifact_path, artifact_payload)
        output_path = str(artifact_path)
        evidence_results = _emit_target_evidence(root=root, day_utc=day_utc, artifact_path=artifact_path, artifact_payload=artifact_payload, producer_id=EOD_ACK_PRODUCER_ID, payload=payload, manual_action_event_id=manual_action_event_id)

    print(
        json.dumps(
            {
                **preview,
                "ok": True,
                "manual_action_event_id": manual_action_event_id,
                "manual_action_status": manual_action.get("status"),
                "target_artifact_path": output_path,
                "target_evidence_event_ids": [str(row.get("event", {}).get("event_id") or "") for row in evidence_results],
                "target_evidence_statuses": [str(row.get("status") or "") for row in evidence_results],
            },
            sort_keys=True,
        )
    )
    return 0


def _runtime_evaluation_errors(*, evaluation: dict[str, Any] | None, day_utc: str, supplied_hash: str) -> list[str]:
    if not evaluation:
        return ["RUNTIME_EVALUATION_MISSING"]
    errors: list[str] = []
    if str(evaluation.get("day_utc") or "") != day_utc:
        errors.append("RUNTIME_EVALUATION_DAY_MISMATCH")
    if str(evaluation.get("deterministic_output_hash") or "") != supplied_hash:
        errors.append("RUNTIME_EVALUATION_HASH_MISMATCH")
    return errors


def _target_for_intent(intent: str) -> str:
    if intent == "NONE_DECLARED":
        return "manual_execution_receipt"
    if intent in {"REPORT_ACKNOWLEDGED", "REVIEW_COMPLETED"}:
        return "aegis_lite_eod_report"
    return "manual_intent"


def _append_manual_action_event(*, truth_root: Path, day_utc: str, payload: dict[str, Any], event_type: str, validation_status: str, output_hash: str, artifact_paths: list[str]) -> dict[str, Any]:
    event = {
        "event_id": event_type + ":" + stable_hash_v1({"payload": payload, "validation_status": validation_status})[:32],
        "event_type": event_type,
        "run_id": str(payload.get("intent_id") or "manual-intent"),
        "parent_run_id": "",
        "day_utc": day_utc,
        "created_at_utc": str(payload.get("intent_timestamp_utc") or ""),
        "producer": MANUAL_INTENT_PRODUCER_ID,
        "producer_version": "v1",
        "git_sha": _git_sha(),
        "schema_id": "manual_intent",
        "schema_version": "v1",
        "input_hashes": _intent_input_hashes(payload, manual_action_event_id=""),
        "output_hashes": {artifact_paths[0]: output_hash} if artifact_paths and output_hash else {"manual_intent_payload": stable_hash_v1(payload)},
        "artifact_paths": artifact_paths,
        "validation_status": validation_status,
        "previous_event_hash": "",
        "event_hash": "",
    }
    return append_event_transaction_v1(truth_root=truth_root, day_utc=day_utc, event=event)


def _emit_target_evidence(*, root: Path, day_utc: str, artifact_path: Path, artifact_payload: dict[str, Any], producer_id: str, payload: dict[str, Any], manual_action_event_id: str) -> list[dict[str, Any]]:
    return emit_artifact_evidence_transaction_v1(
        truth_root=root,
        day_utc=day_utc,
        artifact_path=artifact_path,
        payload=artifact_payload,
        producer_id=producer_id,
        producer_version="v1",
        run_id=str(payload.get("intent_id") or producer_id),
        created_at_utc=str(payload.get("intent_timestamp_utc") or ""),
        git_sha=_git_sha(),
        input_hashes=_intent_input_hashes(payload, manual_action_event_id=manual_action_event_id),
        validation_status="VALID",
    )


def _intent_input_hashes(payload: dict[str, Any], *, manual_action_event_id: str) -> dict[str, str]:
    reason = str(payload.get("reason") or "")
    out = {
        "runtime_evaluation_hash": str(payload.get("runtime_evaluation_hash") or ""),
        "operator_intent_hash": str(payload.get("acknowledgment_hash") or ""),
        "manual_intent_hash": stable_hash_v1(payload),
        "reason_hash": stable_hash_v1(reason),
        "operator_id_hash": stable_hash_v1(str(payload.get("operator_id") or "")),
    }
    if manual_action_event_id:
        out["operator_intent_event_id"] = manual_action_event_id
    return out


def _manual_execution_receipt_payload(*, root: Path, day_utc: str, payload: dict[str, Any], manual_action_event_id: str) -> tuple[Path, dict[str, Any]]:
    artifact = {
        "schema_id": "manual_execution_receipt",
        "schema_version": "v1",
        "artifact_id": "manual_execution_receipt_v1",
        "generated_at_utc": str(payload["intent_timestamp_utc"]),
        "generated_at": str(payload["intent_timestamp_utc"]),
        "day_utc": day_utc,
        "receipt_type": "NONE_DECLARED",
        "operator_declared_no_manual_execution": True,
        "manual_fill_present": False,
        "fill_details_present": False,
        "source": "operator_intent",
        "operator_id": str(payload["operator_id"]),
        "operator_intent": str(payload["operator_intent"]),
        "operator_intent_event_id": manual_action_event_id,
        "operator_intent_hash": str(payload["acknowledgment_hash"]),
        "runtime_evaluation_hash": str(payload["runtime_evaluation_hash"]),
        "reason_hash": stable_hash_v1(str(payload.get("reason") or "")),
        "trade_ids": [],
        "evidence_paths": [],
        "result": "NO_MANUAL_EXECUTION_DECLARED",
        "manual_trade_execution_proven": False,
        "broker_submission_by_aegis": False,
        "autonomous_execution": False,
        "broker_submit_required": False,
        "trade_advice_allowed": False,
        "autonomous_execution_allowed": False,
        "evidence_hash": "",
    }
    artifact["evidence_hash"] = canonical_payload_hash_v1({**artifact, "evidence_hash": ""})
    return root / "reports" / "manual_execution_receipt_v1" / day_utc / "index" / "manual_execution_receipt.v1.json", artifact


def _aegis_lite_eod_ack_payload(*, root: Path, day_utc: str, payload: dict[str, Any], manual_action_event_id: str) -> tuple[Path, dict[str, Any]]:
    report_path = _latest_existing(root / "reports" / "aegis_lite_eod_report_v1" / day_utc, "aegis_lite_eod_report.v1.json")
    report_hash = sha256_file_v1(report_path) if report_path else ""
    artifact = {
        "schema_id": "aegis_lite_eod_report",
        "schema_version": "v1",
        "artifact_id": "aegis_lite_eod_report_manual_acknowledgment_v1",
        "day_utc": day_utc,
        "generated_at_utc": str(payload["intent_timestamp_utc"]),
        "report_status": "MANUAL_ACKNOWLEDGED",
        "manual_acknowledgment_status": "ACCEPTED",
        "operator_id": str(payload["operator_id"]),
        "operator_intent": str(payload["operator_intent"]),
        "operator_intent_event_id": manual_action_event_id,
        "operator_intent_hash": str(payload["acknowledgment_hash"]),
        "runtime_evaluation_hash": str(payload["runtime_evaluation_hash"]),
        "reason_hash": stable_hash_v1(str(payload.get("reason") or "")),
        "acknowledged_report_path": str(report_path or ""),
        "acknowledged_report_hash": report_hash,
        "broker_submit_required": False,
        "trade_advice_allowed": False,
        "autonomous_execution_allowed": False,
        "canonical_json_hash": "",
    }
    artifact["canonical_json_hash"] = canonical_payload_hash_v1({**artifact, "canonical_json_hash": ""})
    safe_intent = str(payload["intent_id"]).replace(":", "_")
    path = root / "reports" / "aegis_lite_eod_report_v1" / day_utc / "manual_acknowledgment" / safe_intent / "aegis_lite_eod_report.v1.json"
    return path, artifact


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(stable_json_bytes_v1(payload) + b"\n")
    os.replace(tmp, path)


def _latest_existing(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    rows = sorted(path for path in root.rglob(filename) if path.is_file())
    return rows[-1] if rows else None


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "UNKNOWN"


if __name__ == "__main__":
    raise SystemExit(main())
