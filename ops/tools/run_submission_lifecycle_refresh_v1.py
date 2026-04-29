#!/usr/bin/env python3
"""
run_submission_lifecycle_refresh_v1.py

Submission-scoped post-submit lifecycle refresh.

Reads:
  <TRUTH_ROOT>/execution_evidence_v1/submissions/<DAY>/<submission_id>/
    - broker_submission_record.v2.json
    - execution_event_record.v1.json (optional existing)
  <TRUTH_ROOT>/execution_stream_v1/<DAY>/*.execution_event_stream_record.v1.json

Writes:
  - refreshes execution_event_record.v1.json for the target submission only
  - refreshes fill_ledger_v1 for the target submission only by invoking
    run_fill_ledger_day_v1.py in submission-scoped mode

Fail-closed:
  - target submission must be authoritative
  - target refresh must be a deterministic monotonic upgrade
  - unrelated same-day submission conflicts are not consulted
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Mapping, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.broker_reconciliation_artifacts_v1 import (
    build_broker_acknowledgement_v1,
    build_broker_order_outcome_v1,
    build_broker_submit_attempt_v1,
    write_broker_acknowledgement_v1,
    write_broker_order_outcome_v1,
    write_broker_submit_attempt_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
import ops.tools.run_fill_ledger_day_v1 as fill_ledger


BSR_SCHEMA = "constellation_2/schemas/broker_submission_record.v2.schema.json"
EER_SCHEMA = "constellation_2/schemas/execution_event_record.v1.schema.json"


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --truth_root: {p}")
    return p


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    if text.startswith("+"):
        text = text[1:]
    if text.isdigit():
        try:
            return int(text)
        except Exception:
            return None
    return None


def _parse_ib_args_map(raw_row: Mapping[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    ib_fields = raw_row.get("ib_fields")
    if not isinstance(ib_fields, dict):
        return out
    args = ib_fields.get("args")
    if not isinstance(args, list):
        return out
    for item in args:
        if not isinstance(item, dict):
            continue
        text = str(item.get("value") or "").strip()
        if not text or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key_norm = key.strip().lower()
        if not key_norm:
            continue
        out[key_norm] = value.strip()
    return out


def _status_from_order_state_blob(raw_open_order_state: str) -> str:
    text = str(raw_open_order_state or "")
    marker = "'status':"
    pos = text.find(marker)
    if pos < 0:
        return ""
    remainder = text[pos + len(marker) :].lstrip()
    if not remainder.startswith("'"):
        return ""
    remainder = remainder[1:]
    end = remainder.find("'")
    if end < 0:
        return ""
    return remainder[:end].strip().upper()


def _scan_broker_events_for_order(truth_root: Path, *, order_id: int) -> Dict[str, Any]:
    events_root = (truth_root / "execution_evidence_v1" / "broker_events").resolve()
    if not events_root.exists() or not events_root.is_dir():
        return {
            "latest_status": "",
            "latest_perm_id": None,
            "latest_received_utc": "",
            "error_201": False,
            "error_message": "",
            "evidence_artifacts": [],
        }
    latest_status = ""
    latest_perm_id: int | None = None
    latest_received_at: datetime | None = None
    latest_received_utc = ""
    error_201 = False
    error_message = ""
    evidence: List[str] = []
    for day_dir in sorted(events_root.glob("*")):
        if not day_dir.is_dir():
            continue
        log_path = (day_dir / "broker_event_log.v1.jsonl").resolve()
        if not log_path.exists() or not log_path.is_file():
            continue
        try:
            with log_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    row_text = line.strip()
                    if not row_text:
                        continue
                    try:
                        row = json.loads(row_text)
                    except Exception:
                        continue
                    if not isinstance(row, dict):
                        continue
                    event_type = str(row.get("event_type") or "").strip().lower()
                    args_map = _parse_ib_args_map(row)
                    received_utc = str(row.get("received_utc") or "").strip()
                    received_at = _parse_utc(received_utc)
                    if event_type == "error":
                        req_id = _safe_int(args_map.get("reqid"))
                        err_code = _safe_int(args_map.get("errorcode"))
                        if req_id == order_id and err_code == 201:
                            error_201 = True
                            error_message = str(args_map.get("errorstring") or "").strip()
                            evidence.append(str(log_path))
                        continue
                    candidate_order_id = _safe_int(args_map.get("orderid"))
                    if candidate_order_id != order_id:
                        continue
                    evidence.append(str(log_path))
                    if event_type == "orderstatus":
                        status = str(args_map.get("status") or "").strip().upper()
                    elif event_type == "openorder":
                        status = _status_from_order_state_blob(str(args_map.get("orderstate") or ""))
                    else:
                        status = ""
                    perm_candidate = _safe_int(args_map.get("permid"))
                    if received_at is not None and (latest_received_at is None or received_at >= latest_received_at):
                        latest_received_at = received_at
                        latest_received_utc = received_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                        if status:
                            latest_status = status
                        if perm_candidate is not None:
                            latest_perm_id = perm_candidate
                    elif latest_received_at is None:
                        if status:
                            latest_status = status
                        if perm_candidate is not None:
                            latest_perm_id = perm_candidate
        except Exception:
            continue
    return {
        "latest_status": latest_status,
        "latest_perm_id": latest_perm_id,
        "latest_received_utc": latest_received_utc,
        "error_201": error_201,
        "error_message": error_message,
        "evidence_artifacts": sorted(set(evidence)),
    }


def _normalize_broker_status(raw_status: str) -> str:
    return str(raw_status or "").strip().upper()


def _classify_outcome_state(
    *,
    broker_submission_status: str,
    stream_status: str,
    broker_event_status: str,
    broker_submission_error: Dict[str, Any] | None,
    event_error_201: bool,
    event_error_message: str,
) -> Tuple[str, str | None, List[str], Dict[str, Any] | None]:
    error_obj = broker_submission_error if isinstance(broker_submission_error, dict) else None
    if event_error_201:
        return (
            "BROKER_REJECTED",
            _normalize_broker_status(stream_status or broker_event_status or broker_submission_status) or "REJECTED",
            ["IB_ERROR_201_RISKLESS_COMBINATION", "BROKER_REJECTED"],
            {
                "code": "IB_ERROR_201",
                "message": event_error_message or "Riskless combination orders are not allowed.",
            },
        )
    if isinstance(error_obj, dict):
        error_code = str(error_obj.get("code") or "").strip().upper()
        if error_code == "DRY_RUN_NO_BROKER_ID":
            return (
                "UNKNOWN_PENDING",
                _normalize_broker_status(stream_status or broker_event_status or broker_submission_status) or "PENDINGSUBMIT",
                ["DRY_RUN_NO_BROKER_ID", "BROKER_OUTCOME_NOT_RECONCILED"],
                {
                    "code": str(error_obj.get("code") or "DRY_RUN_NO_BROKER_ID").strip(),
                    "message": str(error_obj.get("message") or "Dry-run submit has no broker callbacks.").strip(),
                },
            )
        if error_code and error_code != "DRY_RUN_NO_BROKER_ID":
            if error_code == "IB_ERROR_201":
                return (
                    "BROKER_REJECTED",
                    _normalize_broker_status(stream_status or broker_event_status or broker_submission_status) or "REJECTED",
                    ["IB_ERROR_201_RISKLESS_COMBINATION", "BROKER_REJECTED"],
                    {
                        "code": "IB_ERROR_201",
                        "message": str(error_obj.get("message") or "Riskless combination orders are not allowed.").strip(),
                    },
                )
            return (
                "BROKER_REJECTED",
                _normalize_broker_status(stream_status or broker_event_status or broker_submission_status) or "REJECTED",
                ["BROKER_ERROR_FROM_SUBMISSION_RECORD", "BROKER_REJECTED"],
                {
                    "code": str(error_obj.get("code") or "BROKER_ERROR").strip(),
                    "message": str(error_obj.get("message") or "Broker error recorded in submission record.").strip(),
                },
            )

    status = _normalize_broker_status(stream_status) or _normalize_broker_status(broker_event_status) or _normalize_broker_status(
        broker_submission_status
    )
    if status in {"FILLED"}:
        return ("FILLED", status, ["OUTCOME_FROM_STATUS"], None)
    if status in {"PARTIALLY_FILLED"}:
        return ("PARTIALLY_FILLED", status, ["OUTCOME_FROM_STATUS"], None)
    if status in {"REJECTED", "INACTIVE"}:
        return ("BROKER_REJECTED", status, ["BROKER_REJECTED_FROM_STATUS"], error_obj)
    if status in {"CANCELLED", "CANCELLEDAPI", "APICANCELLED"}:
        return ("BROKER_CANCELLED", status, ["BROKER_CANCELLED_FROM_STATUS"], error_obj)
    if status in {"SUBMITTED", "PRESUBMITTED"}:
        return ("BROKER_ACCEPTED", status, ["BROKER_ACCEPTED_FROM_STATUS"], error_obj)
    if status in {"PENDINGSUBMIT", "PENDING_SUBMIT"}:
        return ("UNKNOWN_PENDING", status, ["BROKER_OUTCOME_NOT_RECONCILED"], error_obj)
    return (
        "UNKNOWN_PENDING",
        status or None,
        ["BROKER_OUTCOME_NOT_RECONCILED"],
        error_obj,
    )


def _write_bytes_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(prefix=f".{path.name}.tmp.", dir=str(path.parent), delete=False) as tmp:
        tmp.write(payload)
        tmp.flush()
        import os
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _normalized_exec_status(raw_status: str) -> str:
    value = raw_status.strip().upper()
    if value in {
        "SUBMITTED",
        "ACKNOWLEDGED",
        "REJECTED",
        "CANCELLED",
        "PARTIALLY_FILLED",
        "FILLED",
    }:
        return value
    return "UNKNOWN"


def _sort_stream_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        records,
        key=lambda obj: (
            str(obj.get("event_time_utc") or ""),
            str(obj.get("canonical_json_hash") or ""),
        ),
    )


def _target_submission_dir(truth_root: Path, day: str, submission_id: str) -> Path:
    subdir = (truth_root / "execution_evidence_v1" / "submissions" / day / submission_id).resolve()
    if not subdir.exists() or not subdir.is_dir():
        raise RuntimeError(f"MISSING_SUBMISSION_DIR: {subdir}")
    bsr_path = (subdir / "broker_submission_record.v2.json").resolve()
    if not bsr_path.exists():
        raise RuntimeError(f"MISSING_BROKER_SUBMISSION_RECORD: {bsr_path}")
    bsr = _read_json_obj(bsr_path)
    validate_against_repo_schema_v1(bsr, REPO_ROOT, BSR_SCHEMA)
    if str(bsr.get("submission_id") or "").strip() != submission_id:
        raise RuntimeError(f"SUBMISSION_ID_MISMATCH: {subdir}")
    return subdir


def _stream_records_for_submission(truth_root: Path, day: str, submission_id: str) -> List[Dict[str, Any]]:
    stream_dir = (truth_root / "execution_stream_v1" / day).resolve()
    if not stream_dir.exists() or not stream_dir.is_dir():
        return []
    matches: List[Dict[str, Any]] = []
    for path in sorted(stream_dir.glob("*.execution_event_stream_record.v1.json")):
        obj = _read_json_obj(path)
        if str(obj.get("submission_id") or "").strip() != submission_id:
            continue
        obj["_source_path"] = str(path)
        matches.append(obj)
    return _sort_stream_records(matches)


def _stream_record_has_valid_broker_ids(stream_record: Mapping[str, Any]) -> bool:
    broker_ids = stream_record.get("broker_ids") if isinstance(stream_record.get("broker_ids"), dict) else {}
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    return isinstance(order_id, int) and isinstance(perm_id, int)


def _stream_record_is_dry_run_snapshot(stream_record: Mapping[str, Any]) -> bool:
    reason_codes = stream_record.get("reason_codes")
    if not isinstance(reason_codes, list):
        return False
    return any(str(code).strip().upper() == "DRY_RUN_SUBMISSION_SNAPSHOT" for code in reason_codes)


def _broker_submission_is_dry_run_no_broker_id(subdir: Path) -> bool:
    bsr_path = (subdir / "broker_submission_record.v2.json").resolve()
    if not bsr_path.exists() or not bsr_path.is_file():
        return False
    bsr = _read_json_obj(bsr_path)
    error = bsr.get("error") if isinstance(bsr.get("error"), dict) else {}
    error_code = str(error.get("code") or "").strip().upper()
    broker_ids = bsr.get("broker_ids") if isinstance(bsr.get("broker_ids"), dict) else {}
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    return (
        error_code == "DRY_RUN_NO_BROKER_ID"
        and not isinstance(order_id, int)
        and not isinstance(perm_id, int)
    )


def _build_execution_event_record(
    subdir: Path,
    latest_stream: Dict[str, Any],
    existing: Dict[str, Any] | None,
) -> Dict[str, Any]:
    bsr = _read_json_obj((subdir / "broker_submission_record.v2.json").resolve())
    broker_ids = latest_stream.get("broker_ids") if isinstance(latest_stream.get("broker_ids"), dict) else {}
    order_state = latest_stream.get("order_state") if isinstance(latest_stream.get("order_state"), dict) else {}
    fill = latest_stream.get("fill") if isinstance(latest_stream.get("fill"), dict) else {}

    order_id = broker_ids.get("order_id", (bsr.get("broker_ids") or {}).get("order_id"))
    perm_id = broker_ids.get("perm_id", (bsr.get("broker_ids") or {}).get("perm_id"))
    if not isinstance(order_id, int) or not isinstance(perm_id, int):
        raise RuntimeError(f"STREAM_BROKER_IDS_INVALID: {subdir.name}")

    raw_status = str(order_state.get("status") or "").strip().upper()
    fill_qty = fill.get("fill_qty", order_state.get("filled_qty", 0))
    if not isinstance(fill_qty, int) or fill_qty < 0:
        raise RuntimeError(f"STREAM_FILL_QTY_INVALID: {subdir.name}")

    avg_price = str(fill.get("fill_price") or order_state.get("avg_fill_price") or "0").strip() or "0"
    event_time_utc = (
        str(latest_stream.get("event_time_utc") or "").strip()
        or str(latest_stream.get("observed_at_utc") or "").strip()
        or str(existing.get("event_time_utc") if isinstance(existing, dict) else "" or "").strip()
    )
    if not event_time_utc:
        raise RuntimeError(f"STREAM_EVENT_TIME_MISSING: {subdir.name}")

    created_at_utc = ""
    if isinstance(existing, dict):
        created_at_utc = str(existing.get("created_at_utc") or "").strip()
    if not created_at_utc:
        created_at_utc = str(bsr.get("submitted_at_utc") or "").strip()
    if not created_at_utc:
        created_at_utc = str(latest_stream.get("observed_at_utc") or event_time_utc).strip()

    upstream_hash = str(latest_stream.get("canonical_json_hash") or "").strip() or None
    out: Dict[str, Any] = {
        "schema_id": "execution_event_record",
        "schema_version": "v1",
        "created_at_utc": created_at_utc,
        "event_time_utc": event_time_utc,
        "binding_hash": str(bsr.get("binding_hash") or "").strip(),
        "broker_submission_hash": str(bsr.get("canonical_json_hash") or "").strip(),
        "broker_order_id": str(order_id),
        "perm_id": str(perm_id),
        "status": _normalized_exec_status(raw_status),
        "filled_qty": int(fill_qty),
        "avg_price": avg_price,
        "raw_broker_status": raw_status or None,
        "raw_payload_digest": None,
        "sequence_num": None,
        "canonical_json_hash": None,
        "upstream_hash": upstream_hash,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, EER_SCHEMA)
    return out


def _execution_status_rank(value: str) -> int:
    ranks = {
        "UNKNOWN": 0,
        "SUBMITTED": 1,
        "ACKNOWLEDGED": 2,
        "PARTIALLY_FILLED": 3,
        "FILLED": 4,
        "CANCELLED": 5,
        "REJECTED": 5,
    }
    return ranks.get(str(value or "").strip().upper(), -1)


def _is_safe_execution_event_refresh_upgrade(existing: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    identity_fields = (
        "schema_id",
        "schema_version",
        "binding_hash",
        "broker_submission_hash",
        "broker_order_id",
        "perm_id",
    )
    for field in identity_fields:
        if existing.get(field) != candidate.get(field):
            return False

    existing_event_time = str(existing.get("event_time_utc") or "")
    candidate_event_time = str(candidate.get("event_time_utc") or "")
    placeholder_bootstrap = (
        not str(existing.get("upstream_hash") or "").strip()
        and str(existing.get("status") or "").strip().upper() == "UNKNOWN"
        and existing.get("filled_qty") == 0
        and existing.get("raw_broker_status") in (None, "")
    )
    if candidate_event_time < existing_event_time and not placeholder_bootstrap:
        return False

    existing_filled = existing.get("filled_qty")
    candidate_filled = candidate.get("filled_qty")
    if not isinstance(existing_filled, int) or not isinstance(candidate_filled, int):
        return False
    if candidate_filled < existing_filled:
        return False

    if candidate_filled == existing_filled and candidate.get("avg_price") != existing.get("avg_price"):
        return False

    if _execution_status_rank(str(candidate.get("status") or "")) < _execution_status_rank(str(existing.get("status") or "")):
        return False

    existing_upstream = str(existing.get("upstream_hash") or "").strip()
    candidate_upstream = str(candidate.get("upstream_hash") or "").strip()
    if existing_upstream and not candidate_upstream:
        return False
    if candidate_event_time == existing_event_time and existing_upstream and candidate_upstream == existing_upstream:
        return True
    if candidate_event_time == existing_event_time and existing_upstream and candidate_upstream != existing_upstream:
        return False
    return True


def _write_execution_event_record(path: Path, candidate: Dict[str, Any]) -> str:
    payload = canonical_json_bytes_v1(candidate) + b"\n"
    if path.exists():
        existing_bytes = path.read_bytes()
        if _sha256_bytes(existing_bytes) == _sha256_bytes(payload):
            return "SKIP_IDENTICAL"
        existing_obj = _read_json_obj(path)
        if not _is_safe_execution_event_refresh_upgrade(existing_obj, candidate):
            raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
        _write_bytes_replace(path, payload)
        return "BACKFILL_REPAIRED"
    _write_bytes_replace(path, payload)
    return "WROTE"


def _stream_status_from_record(stream_record: Mapping[str, Any] | None) -> str:
    if not isinstance(stream_record, Mapping):
        return ""
    order_state = stream_record.get("order_state")
    if isinstance(order_state, dict):
        return str(order_state.get("status") or "").strip().upper()
    return ""


def _backfill_submit_attempt_if_missing(
    *,
    repo_root: Path,
    subdir: Path,
    day_utc: str,
    environment: str,
    submission_id: str,
    attempted_at_utc: str,
    ib_account: str | None,
    dry_run: bool,
    evidence_artifacts: List[str],
) -> str:
    attempt_path = (subdir / "broker_submit_attempt_v1.json").resolve()
    if attempt_path.exists() and attempt_path.is_file():
        return "EXISTS"
    payload = build_broker_submit_attempt_v1(
        day_utc=day_utc,
        environment=environment,
        submission_id=submission_id,
        attempted_at_utc=attempted_at_utc,
        ib_account=ib_account,
        dry_run=dry_run,
        reason_codes=["LIFECYCLE_REFRESH_BACKFILL_SUBMIT_ATTEMPT"],
        evidence_artifacts=evidence_artifacts,
    )
    return write_broker_submit_attempt_v1(
        repo_root=repo_root,
        submission_dir=subdir,
        payload=payload,
    )


def _backfill_ack_if_missing(
    *,
    repo_root: Path,
    subdir: Path,
    day_utc: str,
    environment: str,
    submission_id: str,
    acknowledged_at_utc: str,
    order_id: int | None,
    perm_id: int | None,
    status: str,
    ib_account: str | None,
    evidence_artifacts: List[str],
) -> str:
    if not isinstance(order_id, int):
        return "SKIP_NO_ORDER_ID"
    ack_path = (subdir / "broker_acknowledgement_v1.json").resolve()
    if ack_path.exists() and ack_path.is_file():
        return "EXISTS"
    reason_codes = ["LIFECYCLE_REFRESH_BACKFILL_ACK"]
    if not isinstance(perm_id, int) or perm_id <= 0:
        reason_codes.append("BROKER_ID_ASSIGNED_WEAK_PERM_ID")
    payload = build_broker_acknowledgement_v1(
        day_utc=day_utc,
        environment=environment,
        submission_id=submission_id,
        acknowledged_at_utc=acknowledged_at_utc,
        order_id=order_id,
        perm_id=perm_id if isinstance(perm_id, int) else None,
        status=status,
        ib_account=ib_account,
        reason_codes=reason_codes,
        evidence_artifacts=evidence_artifacts,
    )
    return write_broker_acknowledgement_v1(
        repo_root=repo_root,
        submission_dir=subdir,
        payload=payload,
    )


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_submission_lifecycle_refresh_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--submission_id", required=True)
    args = ap.parse_args(argv)

    day = str(args.day_utc).strip()
    submission_id = str(args.submission_id).strip()
    truth_root = _require_truth_root(args.truth_root)
    subdir = _target_submission_dir(truth_root, day, submission_id)

    bsr_path = (subdir / "broker_submission_record.v2.json").resolve()
    bsr_obj = _read_json_obj(bsr_path)
    broker_obj = bsr_obj.get("broker") if isinstance(bsr_obj.get("broker"), dict) else {}
    submit_environment = str(broker_obj.get("environment") or "PAPER").strip().upper()
    bsr_status = str(bsr_obj.get("status") or "").strip().upper()
    bsr_error = bsr_obj.get("error") if isinstance(bsr_obj.get("error"), dict) else None
    bsr_ids = bsr_obj.get("broker_ids") if isinstance(bsr_obj.get("broker_ids"), dict) else {}
    bsr_order_id = _safe_int(bsr_ids.get("order_id"))
    bsr_perm_id = _safe_int(bsr_ids.get("perm_id"))
    submitted_at_utc = str(bsr_obj.get("submitted_at_utc") or "").strip() or f"{day}T00:00:00Z"
    ib_account = str(bsr_obj.get("ib_account") or "").strip() or None
    dry_run_marker = False
    if isinstance(bsr_error, dict):
        dry_run_marker = str(bsr_error.get("code") or "").strip().upper() == "DRY_RUN_NO_BROKER_ID"
    dry_run = bool(bsr_obj.get("dry_run") is True or dry_run_marker)

    stream_records = _stream_records_for_submission(truth_root, day, submission_id)
    event_path = (subdir / "execution_event_record.v1.json").resolve()
    latest_stream_hash = ""
    latest_stream: Dict[str, Any] | None = None
    if stream_records:
        eligible_streams = [row for row in stream_records if _stream_record_has_valid_broker_ids(row)]
        if eligible_streams:
            latest_stream = eligible_streams[-1]
            existing = _read_json_obj(event_path) if event_path.exists() else None
            candidate = _build_execution_event_record(subdir, latest_stream, existing)
            event_action = _write_execution_event_record(event_path, candidate)
            latest_stream_hash = str(candidate.get("upstream_hash") or "")
        elif event_path.exists() and event_path.is_file():
            if _broker_submission_is_dry_run_no_broker_id(subdir) or all(
                _stream_record_is_dry_run_snapshot(row) for row in stream_records
            ):
                event_action = "DRY_RUN_NO_BROKER_ID_KEEP_EXISTING_EVENT"
            else:
                event_action = "PENDING_STREAM_NO_BROKER_IDS_KEEP_EXISTING_EVENT"
        else:
            if _broker_submission_is_dry_run_no_broker_id(subdir) or all(
                _stream_record_is_dry_run_snapshot(row) for row in stream_records
            ):
                event_action = "DRY_RUN_NO_BROKER_ID"
            else:
                event_action = "PENDING_STREAM_NO_BROKER_IDS"
    elif event_path.exists() and event_path.is_file():
        event_action = "SKIP_NO_STREAM_KEEP_EXISTING_EVENT"
    else:
        event_action = "PENDING_NO_STREAM_RECORDS"

    fill_ledger.main(
        [
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--submission_id",
            submission_id,
        ]
    )

    order_id = bsr_order_id
    perm_id = bsr_perm_id
    if latest_stream is not None:
        stream_ids = latest_stream.get("broker_ids") if isinstance(latest_stream.get("broker_ids"), dict) else {}
        stream_order_id = _safe_int(stream_ids.get("order_id"))
        stream_perm_id = _safe_int(stream_ids.get("perm_id"))
        if not isinstance(order_id, int) and isinstance(stream_order_id, int):
            order_id = stream_order_id
        if not isinstance(perm_id, int) and isinstance(stream_perm_id, int):
            perm_id = stream_perm_id

    broker_event_summary = _scan_broker_events_for_order(truth_root, order_id=order_id) if isinstance(order_id, int) else {
        "latest_status": "",
        "latest_perm_id": None,
        "latest_received_utc": "",
        "error_201": False,
        "error_message": "",
        "evidence_artifacts": [],
    }
    if (not isinstance(perm_id, int) or perm_id <= 0) and isinstance(broker_event_summary.get("latest_perm_id"), int):
        perm_id = int(broker_event_summary["latest_perm_id"])

    evidence_for_backfill: List[str] = [str(bsr_path)]
    if latest_stream is not None:
        source_path = str(latest_stream.get("_source_path") or "").strip()
        if source_path:
            evidence_for_backfill.append(source_path)
    evidence_for_backfill.extend(broker_event_summary.get("evidence_artifacts") or [])
    evidence_for_backfill = sorted(set(path for path in evidence_for_backfill if str(path).strip()))

    attempt_action = _backfill_submit_attempt_if_missing(
        repo_root=REPO_ROOT,
        subdir=subdir,
        day_utc=day,
        environment=submit_environment,
        submission_id=submission_id,
        attempted_at_utc=submitted_at_utc,
        ib_account=ib_account,
        dry_run=dry_run,
        evidence_artifacts=evidence_for_backfill,
    )

    acknowledgment_at_utc = str(broker_event_summary.get("latest_received_utc") or "").strip()
    if not acknowledgment_at_utc and latest_stream is not None:
        acknowledgment_at_utc = str(latest_stream.get("event_time_utc") or "").strip()
    if not acknowledgment_at_utc:
        acknowledgment_at_utc = submitted_at_utc
    ack_action = _backfill_ack_if_missing(
        repo_root=REPO_ROOT,
        subdir=subdir,
        day_utc=day,
        environment=submit_environment,
        submission_id=submission_id,
        acknowledged_at_utc=acknowledgment_at_utc,
        order_id=order_id if isinstance(order_id, int) else None,
        perm_id=perm_id if isinstance(perm_id, int) else None,
        status=str(_stream_status_from_record(latest_stream) or broker_event_summary.get("latest_status") or bsr_status or "UNKNOWN"),
        ib_account=ib_account,
        evidence_artifacts=evidence_for_backfill,
    )

    stream_status = _stream_status_from_record(latest_stream)
    outcome_state, outcome_status, outcome_reason_codes, outcome_error = _classify_outcome_state(
        broker_submission_status=bsr_status,
        stream_status=stream_status,
        broker_event_status=str(broker_event_summary.get("latest_status") or ""),
        broker_submission_error=bsr_error,
        event_error_201=bool(broker_event_summary.get("error_201")),
        event_error_message=str(broker_event_summary.get("error_message") or ""),
    )
    evaluated_at_utc = str(broker_event_summary.get("latest_received_utc") or "").strip()
    if not evaluated_at_utc and latest_stream is not None:
        evaluated_at_utc = str(latest_stream.get("event_time_utc") or latest_stream.get("observed_at_utc") or "").strip()
    if not evaluated_at_utc:
        evaluated_at_utc = submitted_at_utc
    outcome_payload = build_broker_order_outcome_v1(
        day_utc=day,
        environment=submit_environment,
        submission_id=submission_id,
        evaluated_at_utc=evaluated_at_utc,
        outcome_state=outcome_state,
        status=outcome_status,
        order_id=order_id if isinstance(order_id, int) else None,
        perm_id=perm_id if isinstance(perm_id, int) else None,
        ib_account=ib_account,
        reason_codes=outcome_reason_codes,
        evidence_artifacts=evidence_for_backfill,
        error=outcome_error,
    )
    outcome_action = write_broker_order_outcome_v1(
        repo_root=REPO_ROOT,
        submission_dir=subdir,
        payload=outcome_payload,
    )

    print(
        f"OK: SUBMISSION_LIFECYCLE_REFRESH day={day} submission_id={submission_id} "
        f"execution_event_action={event_action} latest_stream_hash={latest_stream_hash} "
        f"submit_attempt_action={attempt_action} ack_action={ack_action} "
        f"outcome_action={outcome_action} outcome_state={outcome_state}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(format_failure_line("run_submission_lifecycle_refresh_v1", classify_failure(exc), error=repr(exc)), file=sys.stderr)
        raise SystemExit(2)
