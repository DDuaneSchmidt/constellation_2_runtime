#!/usr/bin/env python3
"""
run_exposure_reconciliation_v1.py

Bundled C: Exposure reconciliation + delta order plan writers (immutable truth artifacts).

Outputs:
  - reports/exposure_reconciliation_report_v1/<DAY>/exposure_reconciliation_report.v1.json
  - reports/delta_order_plan_v1/<DAY>/delta_order_plan.v1.json

Deterministic, audit-grade, fail-closed.
No floats in outputs (decimal strings only).

Run:
  python3 ops/tools/run_exposure_reconciliation_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

# --- Import bootstrap (audit-grade, deterministic, fail-closed) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import ast
import hashlib
import json
import subprocess
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RECON = "governance/04_DATA/SCHEMAS/C2/REPORTS/exposure_reconciliation_report.v1.schema.json"
SCHEMA_PLAN = "governance/04_DATA/SCHEMAS/C2/REPORTS/delta_order_plan.v1.schema.json"
SCHEMA_INTENT = "constellation_2/schemas/exposure_intent.v1.schema.json"

INTENTS_ROOT = (TRUTH / "intents_v1" / "snapshots").resolve()
POS_PTR_ROOT = (TRUTH / "positions_v1" / "effective_v1" / "days").resolve()
KILL_ROOT = (TRUTH / "risk_v1" / "kill_switch_v1").resolve()

OUT_RECON_ROOT = (TRUTH / "reports" / "exposure_reconciliation_report_v1").resolve()
OUT_PLAN_ROOT = (TRUTH / "reports" / "delta_order_plan_v1").resolve()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    o2 = dict(obj)
    o2[field] = None
    return _sha256_bytes(_canonical_bytes(o2))


def _dec_str(x: Decimal) -> str:
    s = format(x, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s != "" else "0"


def _parse_pct(s: Any) -> Decimal:
    if not isinstance(s, str):
        raise ValueError(f"TARGET_NOTIONAL_PCT_NOT_STRING: {s!r}")
    try:
        d = Decimal(s)
    except InvalidOperation as e:
        raise ValueError(f"TARGET_NOTIONAL_PCT_INVALID_DECIMAL: {s!r}") from e
    if d < Decimal("0") or d > Decimal("1"):
        raise ValueError(f"TARGET_NOTIONAL_PCT_OUT_OF_RANGE_0_1: {s!r}")
    return d


def _normalize_underlying(u: Any) -> str:
    if isinstance(u, dict):
        sym = str(u.get("symbol") or "").strip()
        if sym:
            return sym
    if isinstance(u, str):
        t = u.strip()
        if t.startswith("{") and "symbol" in t:
            try:
                d = ast.literal_eval(t)
                if isinstance(d, dict):
                    sym = str(d.get("symbol") or "").strip()
                    if sym:
                        return sym
            except Exception:
                pass
        return t
    return str(u).strip() or "UNKNOWN"


def _load_kill_state(day: str) -> Tuple[bool, Path, str]:
    p = (KILL_ROOT / day / "global_kill_switch_state.v1.json").resolve()
    if not p.exists():
        return (True, p, _sha256_bytes(b""))  # fail-closed default ACTIVE
    sha = _sha256_file(p)
    obj = _read_json_obj(p)
    state = str(obj.get("state") or "").strip().upper()
    return ((state != "INACTIVE"), p, sha)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_exposure_reconciliation_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = _now_utc_iso()

    intents_day = (INTENTS_ROOT / day).resolve()
    ptr_path = (POS_PTR_ROOT / day / "positions_effective_pointer.v1.json").resolve()
    kill_active, kill_path, kill_sha = _load_kill_state(day)

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    input_manifest.append({"type": "global_kill_switch_state_v1", "path": str(kill_path), "sha256": kill_sha})

    if intents_day.exists() and intents_day.is_dir():
        intent_files = sorted([p for p in intents_day.glob("*.json") if p.is_file()])
        if not intent_files:
            reason_codes.append("C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED")
            notes.append(f"intents day dir empty: {str(intents_day)}")
        input_manifest.append({"type": "intents_day_dir_present", "path": str(intents_day), "sha256": _sha256_bytes(b"present")})
    else:
        reason_codes.append("C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED")
        input_manifest.append({"type": "intents_day_dir_missing", "path": str(intents_day), "sha256": _sha256_bytes(b"")})
        intent_files = []

    if ptr_path.exists():
        input_manifest.append({"type": "positions_effective_pointer", "path": str(ptr_path), "sha256": _sha256_file(ptr_path)})
    else:
        reason_codes.append("C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED")
        input_manifest.append({"type": "positions_effective_pointer_missing", "path": str(ptr_path), "sha256": _sha256_bytes(b"")})

    pos_items: List[Dict[str, Any]] = []
    if ptr_path.exists():
        try:
            ptr = _read_json_obj(ptr_path)
            sp = ptr.get("pointers", {}).get("snapshot_path")
            if not isinstance(sp, str) or not sp.strip():
                raise ValueError("POSITIONS_EFFECTIVE_POINTER_MISSING_SNAPSHOT_PATH")
            pos_snap_path = Path(sp).resolve()
            input_manifest.append({"type": "positions_snapshot", "path": str(pos_snap_path), "sha256": _sha256_file(pos_snap_path) if pos_snap_path.exists() else _sha256_bytes(b"")})
            if pos_snap_path.exists():
                snap = _read_json_obj(pos_snap_path)
                items = snap.get("positions", {}).get("items", [])
                if isinstance(items, list):
                    pos_items = [x for x in items if isinstance(x, dict)]
        except Exception as e:
            reason_codes.append("C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED")
            notes.append(f"positions pointer/snapshot load failed: {e!r}")

    targets_map: Dict[Tuple[str, str], Decimal] = {}
    for p in intent_files:
        try:
            intent = _read_json_obj(p)
            validate_against_repo_schema_v1(intent, REPO_ROOT, SCHEMA_INTENT)
            eng = str(intent.get("engine", {}).get("engine_id") or "").strip() or "UNKNOWN_ENGINE"
            und = _normalize_underlying(intent.get("underlying"))
            tnp = _parse_pct(intent.get("target_notional_pct"))
            k = (eng, und)
            targets_map[k] = targets_map.get(k, Decimal("0")) + tnp
            input_manifest.append({"type": "intent", "path": str(p.resolve()), "sha256": _sha256_file(p)})
        except Exception as e:
            reason_codes.append("C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED")
            notes.append(f"intent invalid or schema fail: file={str(p)} err={e!r}")

    has_open_positions = any(str(it.get("status") or "").strip().upper() == "OPEN" for it in pos_items)
    actuals_map: Dict[Tuple[str, str], Decimal] = {}

    if has_open_positions:
        reason_codes.append("C2_EXPOSURE_ACTUAL_UNKNOWN_FAILCLOSED")
        notes.append("positions present but actual notional exposure not provable; forcing reduce/flatten-only.")
    # else: actuals remain empty (implicitly zero)

    targets_arr = [{"engine_id": eng, "underlying": und, "target_notional_pct": _dec_str(pct)} for (eng, und), pct in sorted(targets_map.items())]
    actuals_arr = [{"engine_id": eng, "underlying": und, "target_notional_pct": _dec_str(pct)} for (eng, und), pct in sorted(actuals_map.items())]

    deltas_arr = []
    for (eng, und), tgt in sorted(targets_map.items()):
        direction = "UNKNOWN"
        delta = tgt
        if kill_active:
            direction = "FLATTEN"
            delta = Decimal("0") - tgt
        deltas_arr.append({"engine_id": eng, "underlying": und, "delta_notional_pct": _dec_str(delta), "direction": direction, "notes": []})

    mode = "NORMAL"
    status = "OK"
    if kill_active:
        mode = "FLATTEN_ONLY"
        reason_codes.append("C2_EXPOSURE_KILL_SWITCH_FORCES_FLATTEN")
        status = "DEGRADED"
    if has_open_positions:
        if mode != "FLATTEN_ONLY":
            mode = "REDUCE_ONLY"
        status = "FAIL"
    if "C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED" in reason_codes:
        status = "FAIL"
        if mode != "FLATTEN_ONLY":
            mode = "REDUCE_ONLY"

    reason_codes = sorted(list(dict.fromkeys(reason_codes)))

    recon: Dict[str, Any] = {
        "schema_id": "exposure_reconciliation_report",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_exposure_reconciliation_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest if input_manifest else [{"type": "none", "path": "<none>", "sha256": _sha256_bytes(b"")}],
        "targets": targets_arr,
        "actuals": actuals_arr,
        "deltas": deltas_arr,
        "mode": mode,
        "report_sha256": None,
    }
    recon["report_sha256"] = _compute_self_sha(recon, "report_sha256")
    validate_against_repo_schema_v1(recon, REPO_ROOT, SCHEMA_RECON)

    orders: List[Dict[str, Any]] = []
    if kill_active:
        for it in sorted(pos_items, key=lambda x: str(x.get("position_id") or "")):
            if str(it.get("status") or "").strip().upper() != "OPEN":
                continue
            inst = it.get("instrument", {})
            und = _normalize_underlying(inst.get("underlying"))
            orders.append(
                {
                    "engine_id": str(it.get("engine_id") or "unknown"),
                    "underlying": und or "UNKNOWN",
                    "action": "CLOSE",
                    "delta_notional_pct": "-1",
                    "delta_notional_cents": None,
                    "reason_codes": ["C2_EXPOSURE_KILL_SWITCH_FORCES_FLATTEN"],
                    "notes": [],
                }
            )
    else:
        for (eng, und), _tgt in sorted(targets_map.items()):
            orders.append(
                {
                    "engine_id": eng,
                    "underlying": und,
                    "action": "HOLD",
                    "delta_notional_pct": "0",
                    "delta_notional_cents": None,
                    "reason_codes": ["C2_EXPOSURE_ACTUAL_UNKNOWN_FAILCLOSED"] if has_open_positions else [],
                    "notes": [],
                }
            )

    plan_status = "OK" if status != "FAIL" else "FAIL"
    plan: Dict[str, Any] = {
        "schema_id": "delta_order_plan",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_exposure_reconciliation_v1.py", "git_sha": _git_sha()},
        "status": plan_status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest if input_manifest else [{"type": "none", "path": "<none>", "sha256": _sha256_bytes(b"")}],
        "mode": mode,
        "orders": orders,
        "plan_sha256": None,
    }
    plan["plan_sha256"] = _compute_self_sha(plan, "plan_sha256")
    validate_against_repo_schema_v1(plan, REPO_ROOT, SCHEMA_PLAN)

    out_recon = (OUT_RECON_ROOT / day / "exposure_reconciliation_report.v1.json").resolve()
    out_plan = (OUT_PLAN_ROOT / day / "delta_order_plan.v1.json").resolve()

    try:
        wr1 = write_file_immutable_v1(path=out_recon, data=_canonical_bytes(recon), create_dirs=True)
        wr2 = write_file_immutable_v1(path=out_plan, data=_canonical_bytes(plan), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: EXPOSURE_RECONCILIATION_WRITTEN day_utc={day} status={status} path={wr1.path} sha256={wr1.sha256} action={wr1.action}")
    print(f"OK: DELTA_ORDER_PLAN_WRITTEN day_utc={day} status={plan_status} path={wr2.path} sha256={wr2.sha256} action={wr2.action}")

    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
