from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict


HEX64 = re.compile(r"^[a-f0-9]{64}$")


def _repo_root_from_this_file_failclosed() -> Path:
    """
    Deterministically derive repo root from this script location:
      <repo_root>/ops/tools/synthesize_phased_submission_for_day_v1.py
    Fail-closed if expected structure is not present.
    """
    this_file = Path(__file__).resolve()

    # Expect: .../<repo_root>/ops/tools/<this_file>
    try:
        repo_root = this_file.parents[2]
    except Exception as e:
        raise SystemExit(f"FAIL: cannot derive repo_root from __file__: {e!r}")

    # Fail-closed structural proofs (in-process)
    if not (repo_root / "ops").is_dir():
        raise SystemExit(f"FAIL: derived repo_root missing ops/: {str(repo_root)}")
    if not (repo_root / "constellation_2").is_dir():
        raise SystemExit(f"FAIL: derived repo_root missing constellation_2/: {str(repo_root)}")

    return repo_root


def _ensure_repo_on_syspath_failclosed(repo_root: Path) -> None:
    """
    Ensure imports like `constellation_2.*` resolve when running as a script.
    Deterministic: insert repo_root at sys.path[0] iff not already present.
    """
    rr = str(repo_root)
    if rr not in sys.path:
        sys.path.insert(0, rr)

    # Fail-closed: prove the package dir is visible at runtime
    if not (repo_root / "constellation_2").is_dir():
        raise SystemExit(f"FAIL: constellation_2 dir not found under repo_root: {rr}")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.load(open(p, "r", encoding="utf-8"))
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return o


def _canon_bytes(obj: Dict[str, Any]) -> bytes:
    # Use repo canonicalizer (fail-closed if unavailable)
    from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore

    return canonical_json_bytes_v1(obj)


def _write_new_file_failclosed(p: Path, data: bytes) -> None:
    if p.exists():
        raise ValueError(f"ATTEMPTED_REWRITE: {str(p)}")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def _require_iso_z(ts: str, name: str) -> None:
    s = (ts or "").strip()
    if not s.endswith("Z") or "T" not in s:
        raise ValueError(f"{name}_NOT_ISO_Z: {s!r}")


def main(argv: list[str] | None = None) -> int:
    # Deterministic import bootstrap (no env vars required)
    repo_root = _repo_root_from_this_file_failclosed()
    _ensure_repo_on_syspath_failclosed(repo_root)

    submissions_root = (repo_root / "constellation_2" / "phaseD" / "outputs" / "submissions").resolve()

    ap = argparse.ArgumentParser(prog="synthesize_phased_submission_for_day_v1")
    ap.add_argument("--phasec_out_dir", required=True, help="PhaseC out_dir containing order_plan/mapping/binding")
    ap.add_argument("--eval_time_utc", required=True, help="UTC ISO-8601 Z timestamp; also used as submitted_at_utc")
    ap.add_argument("--filled_qty", type=int, default=1, help="Filled quantity (integer)")
    args = ap.parse_args(argv)

    phasec_out = Path(args.phasec_out_dir).resolve()
    eval_time_utc = str(args.eval_time_utc).strip()
    _require_iso_z(eval_time_utc, "EVAL_TIME_UTC")

    p_bind = phasec_out / "binding_record.v1.json"
    p_plan = phasec_out / "order_plan.v1.json"

    if not p_bind.exists():
        raise SystemExit(f"FAIL: missing {str(p_bind)}")
    if not p_plan.exists():
        raise SystemExit(f"FAIL: missing {str(p_plan)}")

    bind = _read_json_obj(p_bind)
    plan = _read_json_obj(p_plan)

    binding_hash = str(bind.get("canonical_json_hash") or "").strip()
    if not HEX64.fullmatch(binding_hash):
        raise SystemExit(f"FAIL: binding_record.canonical_json_hash not 64 hex: {binding_hash!r}")

    # submission_id == binding_hash (PhaseD convention)
    submission_id = binding_hash

    # Determine avg_price from plan.order_terms.limit_price when present; else "0.00"
    avg_price = "0.00"
    ot = plan.get("order_terms")
    if isinstance(ot, dict):
        lp = ot.get("limit_price")
        if isinstance(lp, str) and lp.strip():
            avg_price = lp.strip()

    # Create broker_submission_record.v2.json
    broker_obj: Dict[str, Any] = {
        "schema_id": "broker_submission_record",
        "schema_version": "v2",
        "submission_id": submission_id,
        "binding_hash": submission_id,
        "broker": "IB_PAPER_SYNTH_V1",
        "broker_ids": {"order_id": f"SYNTH-{submission_id[:12]}", "perm_id": f"SYNTH-{submission_id[:12]}"},
        "status": "SUBMITTED",
        "submitted_at_utc": eval_time_utc,
    }

    # Hash broker record deterministically
    broker_bytes = _canon_bytes(broker_obj)
    broker_sha = _sha256_bytes(broker_bytes)

    # Create execution_event_record.v1.json
    evt_obj: Dict[str, Any] = {
        "schema_id": "execution_event_record",
        "schema_version": "v1",
        "created_at_utc": eval_time_utc,
        "event_time_utc": eval_time_utc,
        "binding_hash": submission_id,
        "broker_submission_hash": broker_sha,
        "broker_order_id": str(broker_obj["broker_ids"]["order_id"]),
        "perm_id": str(broker_obj["broker_ids"]["perm_id"]),
        "status": "FILLED",
        "filled_qty": int(args.filled_qty),
        "avg_price": avg_price,
        "raw_broker_status": "SYNTH_FILLED",
        "raw_payload_digest": None,
        "sequence_num": None,
        "canonical_json_hash": None,
        "upstream_hash": None,
    }

    # Validate against repo schemas (fail-closed)
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(
        broker_obj, repo_root, "constellation_2/schemas/broker_submission_record.v2.schema.json"
    )
    validate_against_repo_schema_v1(
        evt_obj, repo_root, "constellation_2/schemas/execution_event_record.v1.schema.json"
    )

    out_dir = (submissions_root / submission_id).resolve()
    out_broker = out_dir / "broker_submission_record.v2.json"
    out_evt = out_dir / "execution_event_record.v1.json"

    _write_new_file_failclosed(out_broker, broker_bytes + b"\n")
    evt_bytes = _canon_bytes(evt_obj)
    _write_new_file_failclosed(out_evt, evt_bytes + b"\n")

    print("OK: SYNTH_PHASED_SUBMISSION_WRITTEN")
    print(f"submission_id={submission_id}")
    print(f"out_dir={str(out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
