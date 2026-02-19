#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

SCHEMA_ID = "C2_ENGINE_CORRELATION_GATE_V1"
SCHEMA_VERSION = "1.0.0"

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _json_dumps_deterministic(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")

def _atomic_write(path: Path, content_bytes: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content_bytes)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _immut_write(path: Path, content_bytes: bytes) -> None:
    if path.exists():
        existing = path.read_bytes()
        if hashlib.sha256(existing).hexdigest() != hashlib.sha256(content_bytes).hexdigest():
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content_bytes)

def _dec(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid decimal: {x!r}")

def _dec_str(d: Decimal) -> str:
    q = d.quantize(Decimal("0.00000001"))
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s

def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _max_offdiag(corr: List[List[Any]]) -> Decimal:
    m = Decimal("-999")
    n = len(corr)
    for i in range(n):
        row = corr[i]
        if not isinstance(row, list) or len(row) != n:
            raise ValueError("corr must be NxN list")
        for j in range(n):
            if i == j:
                continue
            v = _dec(row[j])
            if v > m:
                m = v
    if m == Decimal("-999"):
        return Decimal("0")
    return m

def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_engine_correlation_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--max_pairwise_threshold", default="0.75", help="Fail if max off-diagonal correlation exceeds this")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    thr = _dec(args.max_pairwise_threshold)

    corr_path = TRUTH_ROOT / "monitoring_v1" / "engine_correlation_matrix" / day / "engine_correlation_matrix.v1.json"
    notes: List[str] = []

    status = "PASS"
    fail_closed = False
    max_pairwise = Decimal("0")

    if not corr_path.exists():
        status = "MISSING_INPUTS"
        fail_closed = True
        notes.append(f"MISSING: {corr_path.relative_to(TRUTH_ROOT)}")
    else:
        o = _load_json(corr_path)
        cm = o.get("corr", None) or o.get("correlation_matrix", None)
        if cm is None:
            status = "MISSING_INPUTS"
            fail_closed = True
            notes.append("Missing corr matrix in engine_correlation_matrix artifact.")
        else:
            max_pairwise = _max_offdiag(cm)
            if max_pairwise > thr:
                status = "FAIL"
                fail_closed = True
                notes.append(f"BREACH: max_pairwise={_dec_str(max_pairwise)} > threshold={_dec_str(thr)}")
            else:
                status = "PASS"
                fail_closed = False

    out = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": "ops/tools/run_c2_engine_correlation_gate_v1.py",
        "input_correlation_matrix_path": str(corr_path.relative_to(TRUTH_ROOT)),
        "input_correlation_matrix_sha256": _sha256_file(corr_path) if corr_path.exists() else "0"*64,
        "threshold_max_pairwise": _dec_str(thr),
        "max_pairwise": _dec_str(max_pairwise),
        "status": status,
        "fail_closed": fail_closed,
        "notes": notes
    }

    out_dir = TRUTH_ROOT / "reports" / "engine_correlation_gate_v1" / day
    out_path = out_dir / "engine_correlation_gate.v1.json"
    _immut_write(out_path, _json_dumps_deterministic(out))

    latest_path = TRUTH_ROOT / "reports" / "engine_correlation_gate_v1" / "latest.json"
    latest = {
        "schema_id": "C2_LATEST_POINTER_V1",
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "producer": "ops/tools/run_c2_engine_correlation_gate_v1.py",
        "path": str(out_path.relative_to(TRUTH_ROOT)),
        "artifact_sha256": _sha256_file(out_path),
        "status": status,
        "fail_closed": fail_closed
    }
    _atomic_write(latest_path, _json_dumps_deterministic(latest))

    print(f"OK: wrote {out_path}")
    print(f"OK: updated {latest_path}")

    return 0 if status == "PASS" else 2

if __name__ == "__main__":
    raise SystemExit(main())
