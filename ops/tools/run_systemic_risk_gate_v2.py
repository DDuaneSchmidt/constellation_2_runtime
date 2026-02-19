#!/usr/bin/env python3
"""
run_systemic_risk_gate_v2.py

Bundle X: root-level systemic risk enforcement verdict (fail-closed, deterministic, immutable).

Reads immutable truth:
- monitoring_v1/regime_snapshot_v2/<DAY>/regime_snapshot.v2.json
- monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json
- risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json
- intents_v1/snapshots/<DAY>/ (optional but expected after engines)

Writes immutable truth:
- reports/systemic_risk_gate_v2/<DAY>/systemic_risk_gate.v2.json

Deterministic produced_utc: <DAY>T00:00:00Z
Fail-closed: missing/invalid systemic inputs => status=FAIL, *_ok=false

Bootstrap policy:
- correlation matrix status DEGRADED_INSUFFICIENT_HISTORY is acceptable ONLY when matrix is 1x1 (no pairwise risk possible).
"""

from __future__ import annotations

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
import hashlib
import json
import subprocess
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/systemic_risk_gate.v2.schema.json"
OUT_ROOT = (TRUTH / "reports" / "systemic_risk_gate_v2").resolve()

PATH_REGIME_V2 = (TRUTH / "monitoring_v1" / "regime_snapshot_v2").resolve()
PATH_CORR_MATRIX = (TRUTH / "monitoring_v1" / "engine_correlation_matrix").resolve()
PATH_KILL = (TRUTH / "risk_v1" / "kill_switch_v1").resolve()
PATH_INTENTS_DAY = (TRUTH / "intents_v1" / "snapshots").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


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
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return o


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    o2 = dict(obj)
    o2[field] = None
    return _sha256_bytes(_canonical_bytes(o2))


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _count_intents_by_engine(day: str) -> Tuple[int, List[Dict[str, Any]], List[str], List[Dict[str, str]]]:
    reason: List[str] = []
    manifest: List[Dict[str, str]] = []
    out: Dict[str, int] = {}
    root = (PATH_INTENTS_DAY / day).resolve()

    if not root.exists() or not root.is_dir():
        manifest.append({"type": "intents_day_dir_missing", "path": str(root), "sha256": _sha256_bytes(b"")})
        reason.append("MISSING_INTENTS_DAY_DIR")
        return (0, [], reason, manifest)

    files = sorted([p for p in root.rglob("*.json") if p.is_file()])
    if len(files) == 0:
        manifest.append({"type": "intents_day_dir_empty", "path": str(root), "sha256": _sha256_bytes(b"")})
        reason.append("EMPTY_INTENTS_DAY_DIR")
        return (0, [], reason, manifest)

    listing = "\n".join([str(p.relative_to(root)) for p in files]).encode("utf-8")
    manifest.append({"type": "intents_day_dir", "path": str(root), "sha256": _sha256_bytes(listing)})

    for p in files:
        try:
            o = _read_json_obj(p)
        except Exception:
            continue
        eid = str(o.get("engine_id") or o.get("engine") or "").strip()
        if eid == "":
            continue
        out[eid] = out.get(eid, 0) + 1

    items = [{"engine_id": k, "count": int(out[k])} for k in sorted(out.keys())]
    total = int(sum([x["count"] for x in items]))
    return (total, items, reason, manifest)


def _eval_regime(day: str) -> Tuple[bool, Dict[str, Any], List[str], List[Dict[str, str]]]:
    rc: List[str] = []
    manifest: List[Dict[str, str]] = []
    p = (PATH_REGIME_V2 / day / "regime_snapshot.v2.json").resolve()
    if not p.exists():
        manifest.append({"type": "regime_snapshot_v2_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
        rc.append("MISSING_REGIME_SNAPSHOT_V2")
        return (False, {}, rc, manifest)

    manifest.append({"type": "regime_snapshot_v2", "path": str(p), "sha256": _sha256_file(p)})
    try:
        o = _read_json_obj(p)
    except Exception:
        rc.append("REGIME_SNAPSHOT_V2_PARSE_ERROR")
        return (False, {}, rc, manifest)

    blocking = bool(o.get("blocking"))
    status = str(o.get("status") or "").strip().upper()
    if status != "OK":
        rc.append("REGIME_STATUS_NOT_OK")
        return (False, o, rc, manifest)
    if blocking:
        rc.append("REGIME_BLOCKING_TRUE")
        return (False, o, rc, manifest)
    return (True, o, rc, manifest)


def _eval_kill(day: str) -> Tuple[bool, Dict[str, Any], List[str], List[Dict[str, str]]]:
    rc: List[str] = []
    manifest: List[Dict[str, str]] = []
    p = (PATH_KILL / day / "global_kill_switch_state.v1.json").resolve()
    if not p.exists():
        manifest.append({"type": "global_kill_switch_state_v1_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
        rc.append("MISSING_KILL_SWITCH_STATE_V1")
        return (False, {}, rc, manifest)

    manifest.append({"type": "global_kill_switch_state_v1", "path": str(p), "sha256": _sha256_file(p)})
    try:
        o = _read_json_obj(p)
    except Exception:
        rc.append("KILL_SWITCH_PARSE_ERROR")
        return (False, {}, rc, manifest)

    state = str(o.get("state") or "").strip().upper()
    allow_entries = bool(o.get("allow_entries"))
    if not (state == "INACTIVE" and allow_entries is True):
        rc.append("KILL_SWITCH_ACTIVE_OR_ENTRIES_DISABLED")
        return (False, o, rc, manifest)
    return (True, o, rc, manifest)


def _eval_corr(day: str, threshold: str) -> Tuple[bool, Dict[str, Any], Dict[str, Any], List[str], List[Dict[str, str]]]:
    rc: List[str] = []
    manifest: List[Dict[str, str]] = []
    shock: Dict[str, Any] = {"max_pairwise": "0.000000", "threshold_max_pairwise": threshold, "flagged_pairs_count": 0, "flagged_pairs": []}

    p = (PATH_CORR_MATRIX / day / "engine_correlation_matrix.v1.json").resolve()
    if not p.exists():
        manifest.append({"type": "engine_correlation_matrix_v1_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
        rc.append("MISSING_ENGINE_CORRELATION_MATRIX_V1")
        return (False, {}, shock, rc, manifest)

    manifest.append({"type": "engine_correlation_matrix_v1", "path": str(p), "sha256": _sha256_file(p)})
    try:
        o = _read_json_obj(p)
    except Exception:
        rc.append("ENGINE_CORR_MATRIX_PARSE_ERROR")
        return (False, {}, shock, rc, manifest)

    status = str(o.get("status") or "").strip().upper()
    if status not in ("OK", "DEGRADED_INSUFFICIENT_HISTORY"):
        rc.append("ENGINE_CORR_MATRIX_STATUS_NOT_OK")
        return (False, o, shock, rc, manifest)

    try:
        thr = Decimal(threshold)
    except Exception:
        rc.append("BAD_THRESHOLD_FORMAT")
        return (False, o, shock, rc, manifest)

    matrix = o.get("matrix") or {}
    engine_ids = matrix.get("engine_ids")
    corr = matrix.get("corr")
    if not isinstance(engine_ids, list) or not isinstance(corr, list):
        rc.append("ENGINE_CORR_MATRIX_SHAPE_INVALID")
        return (False, o, shock, rc, manifest)

    n = len(engine_ids)

    max_pair = Decimal("0")
    flagged: List[Dict[str, Any]] = []
    for i in range(n):
        for j in range(i + 1, n):
            try:
                c = Decimal(str(corr[i][j]))
            except Exception:
                rc.append("ENGINE_CORR_MATRIX_VALUE_PARSE_ERROR")
                return (False, o, shock, rc, manifest)
            if abs(c) > abs(max_pair):
                max_pair = c
            if abs(c) >= thr:
                flagged.append({"engine_a": str(engine_ids[i]), "engine_b": str(engine_ids[j]), "corr": f"{c:.6f}"})

    shock = {
        "max_pairwise": f"{max_pair:.6f}",
        "threshold_max_pairwise": threshold,
        "flagged_pairs_count": int(len(flagged)),
        "flagged_pairs": flagged,
    }

    if abs(max_pair) >= thr:
        rc.append("CORRELATION_THRESHOLD_BREACH")
        return (False, o, shock, rc, manifest)

    if status == "DEGRADED_INSUFFICIENT_HISTORY":
        if n != 1:
            rc.append("CORRELATION_DEGRADED_MULTI_ENGINE_BLOCKED")
            return (False, o, shock, rc, manifest)
        rc.append("CORRELATION_DEGRADED_BOOTSTRAP_ACCEPTED")

    return (True, o, shock, rc, manifest)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_systemic_risk_gate_v2")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--max_pairwise_threshold", default="0.75", help="e.g. 0.75")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    threshold = str(args.max_pairwise_threshold).strip()

    reason_codes: List[str] = []
    input_manifest: List[Dict[str, str]] = []

    regime_ok, _regime_obj, rc_r, man_r = _eval_regime(day)
    kill_ok, _kill_obj, rc_k, man_k = _eval_kill(day)
    corr_ok, _corr_obj, shock, rc_c, man_c = _eval_corr(day, threshold)

    total_intents, intents_by_engine, rc_i, man_i = _count_intents_by_engine(day)

    input_manifest.extend(man_r)
    input_manifest.extend(man_k)
    input_manifest.extend(man_c)
    input_manifest.extend(man_i)

    reason_codes.extend(rc_r)
    reason_codes.extend(rc_k)
    reason_codes.extend(rc_c)
    reason_codes.extend(rc_i)

    status = "OK" if (regime_ok and kill_ok and corr_ok) else "FAIL"

    payload: Dict[str, Any] = {
        "schema_id": "systemic_risk_gate",
        "schema_version": "v2",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_systemic_risk_gate_v2.py", "git_sha": _git_sha()},
        "status": status,
        "regime_ok": bool(regime_ok),
        "correlation_ok": bool(corr_ok),
        "kill_switch_ok": bool(kill_ok),
        "shock_model_output": shock,
        "cluster_exposure_metrics": {"total_intents": int(total_intents), "intents_by_engine": intents_by_engine},
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "input_manifest": input_manifest if len(input_manifest) > 0 else [{"type": "unknown", "path": str(TRUTH), "sha256": _sha256_bytes(b"")}],
        "gate_sha256": None,
    }
    payload["gate_sha256"] = _compute_self_sha(payload, "gate_sha256")

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / day / "systemic_risk_gate.v2.json").resolve()
    try:
        wr = write_file_immutable_v1(path=out_path, data=_canonical_bytes(payload), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: SYSTEMIC_RISK_GATE_V2_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
