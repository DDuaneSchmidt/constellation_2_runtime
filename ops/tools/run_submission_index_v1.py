#!/usr/bin/env python3
"""
Bundle A: submission_index.v1.json writer (immutable truth artifact).

Institutional posture:
- Deterministic, audit-grade, fail-closed.
- Classifies each submission record as REAL_IB_PAPER only if broker truth is proven via
  governed broker_event_day_manifest for the DAY.
- Otherwise, SIMULATED if explicit SYNTH_ markers exist; else FAIL (unproven broker truth).

Inputs:
  constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/*/
    - broker_submission_record.v2.json
    - execution_event_record.v1.json

Broker truth (required to classify REAL):
  constellation_2/runtime/truth/execution_evidence_v1/broker_events/<DAY>/
    - broker_event_day_manifest.v1.json (preferred) OR broker_event_day_manifest.v1.<sha>.json

Output:
  constellation_2/runtime/truth/execution_evidence_v1/submission_index/<DAY>/submission_index.v1.json

Schema (governed):
  governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/submission_index.v1.schema.json

Run:
  python3 ops/tools/run_submission_index_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# --- Import bootstrap (audit-grade, deterministic) ---
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore


TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/submission_index.v1.schema.json"

SUBMISSIONS_ROOT = (TRUTH / "execution_evidence_v1" / "submissions").resolve()
BROKER_EVENTS_ROOT = (TRUTH / "execution_evidence_v1" / "broker_events").resolve()
OUT_ROOT = (TRUTH / "execution_evidence_v1" / "submission_index").resolve()

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    import subprocess

    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_schema_available(schema_path: Path) -> None:
    if not schema_path.exists():
        raise SystemExit(f"FAIL: missing governed schema: {schema_path}")


def _validate_jsonschema_or_fail(obj: Dict[str, Any], schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except Exception as e:
        raise SystemExit(f"FAIL: jsonschema not available for validation: {e}")

    schema = _read_json(schema_path)
    try:
        jsonschema.validate(instance=obj, schema=schema)
    except Exception as e:
        raise SystemExit(f"FAIL: schema validation failed: {e}")


@dataclass(frozen=True)
class _WriteResult:
    path: str
    sha256: str
    action: str


def _write_immutable_canonical_json(path: Path, obj: Dict[str, Any]) -> _WriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return _WriteResult(path=str(path), sha256=sha, action="EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()

    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _compute_self_sha_field(obj: Dict[str, Any], field_name: str) -> str:
    obj2 = dict(obj)
    obj2[field_name] = None
    canon = canonical_json_bytes_v1(obj2) + b"\n"
    return _sha256_bytes(canon)


def _synth_marker_present_in_submission_dir(sub_dir: Path) -> bool:
    for p in sorted(sub_dir.rglob("*.json")):
        try:
            t = p.read_text(encoding="utf-8")
        except Exception:
            continue
        if "SYNTH_" in t:
            return True
    return False


def _load_broker_day_manifest(day: str) -> Tuple[Path, Dict[str, Any]]:
    """
    Prefer fixed-name manifest if present, otherwise accept hashed-name manifest.
    """
    day_dir = (BROKER_EVENTS_ROOT / day).resolve()
    if not day_dir.exists():
        raise SystemExit(
            "FAIL: UNPROVEN_BROKER_TRUTH_MISSING_DAY_MANIFEST_DIR: "
            f"expected dir {day_dir}"
        )

    fixed = (day_dir / "broker_event_day_manifest.v1.json").resolve()
    if fixed.exists():
        return fixed, _read_json(fixed)

    hashed = sorted(day_dir.glob("broker_event_day_manifest.v1.*.json"))
    if len(hashed) == 0:
        raise SystemExit(
            "FAIL: UNPROVEN_BROKER_TRUTH_MISSING_DAY_MANIFEST_FILE: "
            f"expected broker_event_day_manifest.v1.json or broker_event_day_manifest.v1.*.json under {day_dir}"
        )
    return hashed[0].resolve(), _read_json(hashed[0])


def _assert_manifest_proves_ib_paper(manifest_path: Path, m: Dict[str, Any]) -> None:
    """
    Fail-closed requirements to claim broker truth exists for REAL_IB_PAPER:
      - status must be OK or PASS
      - log_sha256 must exist and be non-empty
      - broker must be Interactive Brokers (or IB) and environment must be PAPER (using manifest fields)
    """
    status = str(m.get("status") or "").strip().upper()
    if status not in ("OK", "PASS"):
        raise SystemExit(f"FAIL: BROKER_DAY_MANIFEST_STATUS_NOT_OK: status={status!r} file={manifest_path}")

    # Manifest stores log sha under "log.log_sha256"
    log_obj = m.get("log")
    log_sha = ""
    if isinstance(log_obj, dict):
        log_sha = str(log_obj.get("log_sha256") or "").strip()
    if log_sha == "":
        raise SystemExit(f"FAIL: BROKER_DAY_MANIFEST_MISSING_LOG_LOG_SHA256: file={manifest_path}")

    line_count = None
    if isinstance(log_obj, dict):
        line_count = log_obj.get("line_count")
    try:
        if line_count is None or int(line_count) <= 0:
            raise SystemExit(f"FAIL: BROKER_DAY_MANIFEST_EMPTY_LOG: line_count={line_count!r} file={manifest_path}")
    except ValueError:
        raise SystemExit(f"FAIL: BROKER_DAY_MANIFEST_BAD_LINE_COUNT: line_count={line_count!r} file={manifest_path}")

    # Broker / environment fields vary; accept common forms but fail if cannot confirm.
    broker = m.get("broker")
    broker_name = ""
    broker_env = ""

    if isinstance(broker, dict):
        broker_name = str(broker.get("name") or broker.get("broker_name") or "").strip()
        broker_env = str(broker.get("environment") or broker.get("env") or "").strip()
    else:
        broker_name = str(m.get("broker_name") or m.get("broker") or "").strip()
        broker_env = str(m.get("environment") or m.get("broker_environment") or "").strip()

    bn = broker_name.upper().replace(" ", "_")
    be = broker_env.upper()

    if bn not in ("INTERACTIVE_BROKERS", "IB", "INTERACTIVEBROKERS"):
        raise SystemExit(f"FAIL: BROKER_DAY_MANIFEST_NOT_IB: broker={broker_name!r} file={manifest_path}")
    if be != "PAPER":
        raise SystemExit(f"FAIL: BROKER_DAY_MANIFEST_NOT_PAPER: environment={broker_env!r} file={manifest_path}")


def _detect_mode(day: str, sub_dir: Path, exec_event: Dict[str, Any], broker_record: Dict[str, Any]) -> str:
    """
    Institutional classifier (fail-closed):
      - SIMULATED if any SYNTH_ marker exists under the submission directory.
      - REAL_IB_PAPER only if broker day manifest proves IB PAPER broker truth exists.
      - Otherwise: FAIL (unproven broker truth).
    """
    if _synth_marker_present_in_submission_dir(sub_dir):
        return "SIMULATED"

    manifest_path, m = _load_broker_day_manifest(day)
    _assert_manifest_proves_ib_paper(manifest_path, m)
    return "REAL_IB_PAPER"


def _coerce_float_or_none(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _extract_order_id(broker_record: Dict[str, Any]) -> Optional[str]:
    # Prefer nested broker_ids in broker_submission_record.v2
    broker_ids = broker_record.get("broker_ids")
    if isinstance(broker_ids, dict):
        oid = broker_ids.get("order_id")
        if oid is not None:
            return str(oid)
    # Fallbacks
    for k in ["order_id", "broker_order_id", "ib_order_id", "orderId"]:
        v = broker_record.get(k)
        if v is not None:
            return str(v)
    return None


def _extract_engine_id(broker_record: Dict[str, Any]) -> str:
    for k in ["engine_id", "engine", "source_engine_id"]:
        v = broker_record.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return "UNKNOWN_ENGINE"


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_submission_index_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    _validate_schema_available(schema_path)

    produced_utc = _now_utc_iso()

    day_dir = (SUBMISSIONS_ROOT / day).resolve()
    records: List[Dict[str, Any]] = []

    if day_dir.exists():
        sub_dirs = sorted([p for p in day_dir.iterdir() if p.is_dir()])
        for sub_dir in sub_dirs:
            bsr = (sub_dir / "broker_submission_record.v2.json").resolve()
            exr = (sub_dir / "execution_event_record.v1.json").resolve()

            if not bsr.exists():
                raise SystemExit(f"FAIL: missing broker_submission_record.v2.json: {bsr}")
            if not exr.exists():
                raise SystemExit(f"FAIL: missing execution_event_record.v1.json: {exr}")

            broker_record = _read_json(bsr)
            exec_event = _read_json(exr)

            mode = _detect_mode(day, sub_dir, exec_event, broker_record)

            submission_id = str(broker_record.get("submission_id") or sub_dir.name).strip()
            engine_id = _extract_engine_id(broker_record)

            order_id = _extract_order_id(broker_record)
            if order_id is None:
                # Schema allows null order_id; keep null.
                order_id_out: Optional[str] = None
            else:
                order_id_out = str(order_id)

            # NOTE: Account is not present in your submission record today.
            # For audit-grade readiness, account must ultimately be wired into evidence.
            # For now, we emit empty string (schema allows string), and Bundle A gate can enforce non-empty later.
            account = ""

            final_state = str(exec_event.get("status") or exec_event.get("final_state") or "UNKNOWN").strip()
            filled_qty_raw = exec_event.get("filled_qty")
            filled_qty = float(_coerce_float_or_none(filled_qty_raw) or 0.0)

            avg_px = _coerce_float_or_none(exec_event.get("avg_fill_px"))
            if avg_px is None:
                avg_px = _coerce_float_or_none(exec_event.get("avg_price"))

            submitted_utc = str(broker_record.get("submitted_utc") or broker_record.get("submitted_at_utc") or produced_utc)
            last_update_utc = str(exec_event.get("last_update_utc") or exec_event.get("event_time_utc") or exec_event.get("created_at_utc") or produced_utc)

            rec: Dict[str, Any] = {
                "submission_id": submission_id,
                "engine_id": engine_id,
                "intent_hash": broker_record.get("intent_hash"),
                "oms_decision_hash": broker_record.get("oms_decision_hash"),
                "allocation_decision_hash": broker_record.get("allocation_decision_hash"),
                "broker": {"venue": "IB", "account": account, "order_id": order_id_out},
                "status": {"final_state": final_state, "filled_qty": filled_qty, "avg_fill_px": avg_px},
                "mode": mode,
                "execution_event_record_hash": _sha256_file(exr),
                "timestamps": {"submitted_utc": submitted_utc, "last_update_utc": last_update_utc},
            }
            records.append(rec)

    payload: Dict[str, Any] = {
        "schema_id": "submission_index.v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"component": "ops/tools/run_submission_index_v1.py", "version": "v1", "git_sha": _git_sha()},
        "records": records,
        "index_sha256": None,
    }
    payload["index_sha256"] = _compute_self_sha_field(payload, "index_sha256")

    _validate_jsonschema_or_fail(payload, schema_path)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "submission_index.v1.json").resolve()
    wr = _write_immutable_canonical_json(out_path, payload)

    print(
        "OK: SUBMISSION_INDEX_WRITTEN "
        f"day_utc={day} path={wr.path} sha256={wr.sha256} action={wr.action} records={len(records)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
