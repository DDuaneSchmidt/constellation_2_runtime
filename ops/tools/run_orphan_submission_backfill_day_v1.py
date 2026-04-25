#!/usr/bin/env python3
"""
run_orphan_submission_backfill_day_v1.py

Backfills missing post-handoff submission evidence for orphan submission dirs.

Reads:
  <TRUTH_ROOT>/execution_evidence_v1/submissions/<DAY>/<submission_id>/
    - binding_record.v2.json
    - veto_record.v1.json
    - equity_order_plan.v1.json|equity_order_plan.v2.json|order_plan.v1.json
  <STREAM_TRUTH_ROOT>/execution_stream_v1/<DAY>/*.execution_event_stream_record.v1.json

Writes if provable and missing:
  <TRUTH_ROOT>/execution_evidence_v1/submissions/<DAY>/<submission_id>/
    - broker_submission_record.v2.json
    - execution_event_record.v1.json

Fail-closed:
- only post-handoff schema-contract orphans are backfilled
- requires deterministic execution-stream evidence for the submission_id
- refuses overwrite if different bytes already exist
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


BSR_SCHEMA = "constellation_2/schemas/broker_submission_record.v2.schema.json"
EER_SCHEMA = "constellation_2/schemas/execution_event_record.v1.schema.json"


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid truth root: {p}")
    return p


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_immutable(path: Path, obj: Dict[str, Any]) -> bool:
    payload = canonical_json_bytes_v1(obj) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload):
            return False
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    import os
    os.replace(tmp, path)
    return True


def _list_submission_dirs(truth_root: Path, day: str) -> List[Path]:
    d = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not d.exists() or not d.is_dir():
        return []
    return sorted([p for p in d.iterdir() if p.is_dir()], key=lambda p: p.name)


def _stream_records_by_submission(stream_truth_root: Path, day: str) -> Dict[str, List[Dict[str, Any]]]:
    d = (stream_truth_root / "execution_stream_v1" / day).resolve()
    if not d.exists() or not d.is_dir():
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(d.glob("*.execution_event_stream_record.v1.json")):
        obj = _read_json_obj(path)
        submission_id = str(obj.get("submission_id") or "").strip()
        if not submission_id:
            raise RuntimeError(f"STREAM_RECORD_MISSING_SUBMISSION_ID: {path}")
        obj["_source_path"] = str(path)
        out.setdefault(submission_id, []).append(obj)
    return out


def _pick_seed_stream_record(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        raise RuntimeError("NO_STREAM_RECORDS_FOR_SUBMISSION")
    return sorted(
        records,
        key=lambda obj: (
            str(obj.get("event_time_utc") or ""),
            str(obj.get("canonical_json_hash") or ""),
        ),
    )[0]


def _supported_plan_path(subdir: Path) -> Path | None:
    for name in ("equity_order_plan.v2.json", "equity_order_plan.v1.json", "order_plan.v1.json"):
        p = (subdir / name).resolve()
        if p.exists():
            return p
    return None


def _is_post_handoff_schema_orphan(subdir: Path) -> bool:
    bsr = (subdir / "broker_submission_record.v2.json").resolve()
    eer = (subdir / "execution_event_record.v1.json").resolve()
    if bsr.exists() and eer.exists():
        return False
    if _supported_plan_path(subdir) is None:
        return False
    binding = (subdir / "binding_record.v2.json").resolve()
    veto = (subdir / "veto_record.v1.json").resolve()
    if not binding.exists() or not veto.exists():
        return False
    veto_obj = _read_json_obj(veto)
    if str(veto_obj.get("reason_code") or "").strip() != "C2_SUBMIT_FAIL_CLOSED_REQUIRED":
        return False
    detail = str(veto_obj.get("reason_detail") or "")
    return "broker_submission_record.v2.schema.json" in detail


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


def _submission_binding_hash(subdir: Path) -> str:
    binding = _read_json_obj((subdir / "binding_record.v2.json").resolve())
    binding_hash = str(binding.get("binding_hash") or binding.get("canonical_json_hash") or "").strip()
    if not binding_hash:
        raise RuntimeError(f"BINDING_HASH_MISSING: {subdir}")
    return binding_hash


def _submitted_at_utc(subdir: Path, stream_obj: Dict[str, Any], day: str) -> str:
    veto_path = (subdir / "veto_record.v1.json").resolve()
    if veto_path.exists():
        veto = _read_json_obj(veto_path)
        observed = str(veto.get("observed_at_utc") or "").strip()
        if observed:
            return observed
    observed = str(stream_obj.get("observed_at_utc") or "").strip()
    if observed:
        return observed
    event_time = str(stream_obj.get("event_time_utc") or "").strip()
    if event_time:
        return event_time
    return f"{day}T00:00:00Z"


def _build_broker_submission_record(subdir: Path, stream_obj: Dict[str, Any], day: str) -> Dict[str, Any]:
    binding_hash = _submission_binding_hash(subdir)
    stream_binding_hash = str(stream_obj.get("binding_hash") or "").strip()
    if stream_binding_hash and stream_binding_hash != binding_hash:
        raise RuntimeError(
            f"BINDING_HASH_MISMATCH: submission_id={subdir.name} stream={stream_binding_hash} binding={binding_hash}"
        )

    broker = stream_obj.get("broker")
    if not isinstance(broker, dict):
        raise RuntimeError(f"STREAM_BROKER_MISSING: {subdir.name}")
    broker_name = str(broker.get("name") or "").strip()
    broker_env = str(broker.get("environment") or "").strip()
    if not broker_name or not broker_env:
        raise RuntimeError(f"STREAM_BROKER_INVALID: {subdir.name}")

    broker_ids = stream_obj.get("broker_ids")
    if not isinstance(broker_ids, dict):
        raise RuntimeError(f"STREAM_BROKER_IDS_MISSING: {subdir.name}")
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    if not isinstance(order_id, int) or not isinstance(perm_id, int):
        raise RuntimeError(f"STREAM_BROKER_IDS_INVALID: {subdir.name}")

    order_state = stream_obj.get("order_state")
    if not isinstance(order_state, dict):
        raise RuntimeError(f"STREAM_ORDER_STATE_MISSING: {subdir.name}")
    raw_status = str(order_state.get("status") or "").strip().upper()
    if not raw_status:
        raise RuntimeError(f"STREAM_ORDER_STATUS_MISSING: {subdir.name}")

    out: Dict[str, Any] = {
        "schema_id": "broker_submission_record",
        "schema_version": "v2",
        "submission_id": subdir.name,
        "submitted_at_utc": _submitted_at_utc(subdir, stream_obj, day),
        "binding_hash": binding_hash,
        "broker": {"name": broker_name, "environment": broker_env},
        "status": raw_status,
        "broker_ids": {"order_id": int(order_id), "perm_id": int(perm_id)},
        "error": {
            "code": "RECONSTRUCTED_FROM_EXECUTION_STREAM_V1",
            "message": "Backfilled from execution_stream_v1 after post-handoff contract failure.",
        },
        "canonical_json_hash": None,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, BSR_SCHEMA)
    return out


def _build_execution_event_record(
    subdir: Path,
    stream_obj: Dict[str, Any],
    bsr_obj: Dict[str, Any],
    day: str,
) -> Dict[str, Any]:
    broker_ids = stream_obj.get("broker_ids")
    if not isinstance(broker_ids, dict):
        raise RuntimeError(f"STREAM_BROKER_IDS_MISSING: {subdir.name}")
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    if not isinstance(order_id, int) or not isinstance(perm_id, int):
        raise RuntimeError(f"STREAM_BROKER_IDS_INVALID: {subdir.name}")

    fill = stream_obj.get("fill")
    if not isinstance(fill, dict):
        raise RuntimeError(f"STREAM_FILL_MISSING: {subdir.name}")
    fill_qty = fill.get("fill_qty")
    fill_price = str(fill.get("fill_price") or "0").strip()
    if not isinstance(fill_qty, int) or fill_qty < 0:
        raise RuntimeError(f"STREAM_FILL_QTY_INVALID: {subdir.name}")
    if not fill_price:
        fill_price = "0"

    order_state = stream_obj.get("order_state")
    if not isinstance(order_state, dict):
        raise RuntimeError(f"STREAM_ORDER_STATE_MISSING: {subdir.name}")
    raw_status = str(order_state.get("status") or "").strip().upper()

    event_time = str(stream_obj.get("event_time_utc") or "").strip() or f"{day}T00:00:00Z"
    created_at = _submitted_at_utc(subdir, stream_obj, day)
    stream_hash = str(stream_obj.get("canonical_json_hash") or "").strip() or None

    out: Dict[str, Any] = {
        "schema_id": "execution_event_record",
        "schema_version": "v1",
        "created_at_utc": created_at,
        "event_time_utc": event_time,
        "binding_hash": str(bsr_obj["binding_hash"]),
        "broker_submission_hash": str(bsr_obj["canonical_json_hash"]),
        "broker_order_id": str(order_id),
        "perm_id": str(perm_id),
        "status": _normalized_exec_status(raw_status),
        "filled_qty": int(fill_qty),
        "avg_price": fill_price,
        "raw_broker_status": raw_status or None,
        "raw_payload_digest": None,
        "sequence_num": None,
        "canonical_json_hash": None,
        "upstream_hash": stream_hash,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, EER_SCHEMA)
    return out


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_orphan_submission_backfill_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--stream_truth_root", default=None)
    args = ap.parse_args(argv)

    day = str(args.day_utc).strip()
    truth_root = _require_truth_root(args.truth_root)
    stream_truth_root = _require_truth_root(args.stream_truth_root or args.truth_root)

    stream_records = _stream_records_by_submission(stream_truth_root, day)
    wrote_bsr = 0
    wrote_exec = 0
    touched = 0

    for subdir in _list_submission_dirs(truth_root, day):
        if not _is_post_handoff_schema_orphan(subdir):
            continue
        touched += 1
        stream_obj = _pick_seed_stream_record(stream_records.get(subdir.name, []))
        bsr_obj = _build_broker_submission_record(subdir, stream_obj, day)
        eer_obj = _build_execution_event_record(subdir, stream_obj, bsr_obj, day)

        if _write_immutable((subdir / "broker_submission_record.v2.json").resolve(), bsr_obj):
            wrote_bsr += 1
        if _write_immutable((subdir / "execution_event_record.v1.json").resolve(), eer_obj):
            wrote_exec += 1

    print(
        f"OK: ORPHAN_SUBMISSION_BACKFILL day={day} touched={touched} "
        f"wrote_broker_submission_record={wrote_bsr} wrote_execution_event_record={wrote_exec}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(format_failure_line("run_orphan_submission_backfill_day_v1", classify_failure(exc), error=repr(exc)), file=sys.stderr)
        raise SystemExit(2)
