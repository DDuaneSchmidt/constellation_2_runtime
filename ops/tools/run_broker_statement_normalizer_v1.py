#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path("/home/node/constellation_2_runtime")
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

SCHEMA_ID = "C2_BROKER_STATEMENT_NORMALIZED_V1"
SCHEMA_VERSION = "1.0.0"

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

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
        if _sha256_bytes(existing) != _sha256_bytes(content_bytes):
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path} existing_sha={_sha256_bytes(existing)} candidate_sha={_sha256_bytes(content_bytes)}")
        return
    _atomic_write(path, content_bytes)

def _dec_str(x: Any) -> str:
    try:
        d = Decimal(str(x))
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid decimal: {x!r}")
    q = d.quantize(Decimal("0.00000001"))
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
        if s == "-0":
            s = "0"
    return s

def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--source", required=True, choices=["IB_FLEX", "IB_TWS_EXPORT", "IB_PORTAL_EXPORT", "OTHER"])
    ap.add_argument("--account_id", required=True)
    ap.add_argument("--currency", required=True)
    ap.add_argument("--input_json", required=True, help="Path to operator-provided raw statement JSON (already on disk)")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    in_path = Path(args.input_json).expanduser().resolve()
    if not in_path.exists():
        raise SystemExit(f"FATAL: input_json missing: {in_path}")

    src_sha = _sha256_file(in_path)
    raw = _load_json(in_path)

    cash_end = _dec_str(raw.get("cash_end"))
    fees_total = _dec_str(raw.get("fees_total", "0"))
    positions_in = raw.get("positions", [])
    if not isinstance(positions_in, list):
        raise SystemExit("FATAL: raw.positions must be list")

    positions: List[Dict[str, str]] = []
    for r in positions_in:
        if not isinstance(r, dict):
            raise SystemExit("FATAL: each raw.positions[] must be object")
        symbol = str(r.get("symbol", "")).strip()
        sec_type = str(r.get("sec_type", "")).strip()
        if symbol == "" or sec_type == "":
            raise SystemExit("FATAL: raw.positions[] missing symbol/sec_type")
        item = {
            "symbol": symbol,
            "sec_type": sec_type,
            "qty": _dec_str(r.get("qty", "0")),
            "avg_cost": _dec_str(r.get("avg_cost", "0")),
            "market_value": _dec_str(r.get("market_value", "0")),
        }
        ccy = r.get("currency", None)
        if ccy is not None:
            item["currency"] = str(ccy).strip()
        positions.append(item)

    notes_in = raw.get("notes", [])
    if notes_in is None:
        notes_in = []
    if not isinstance(notes_in, list):
        raise SystemExit("FATAL: raw.notes must be list")
    notes = [str(x) for x in notes_in]

    out = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": "ops/tools/run_broker_statement_normalizer_v1.py",
        "source": str(args.source),
        "source_file_sha256": src_sha,
        "account_id": str(args.account_id).strip(),
        "currency": str(args.currency).strip(),
        "cash_end": cash_end,
        "fees_total": fees_total,
        "positions": positions,
        "notes": notes,
    }

    out_dir = TRUTH_ROOT / "execution_evidence_v1" / "broker_statement_normalized_v1" / day
    out_path = out_dir / "broker_statement_normalized.v1.json"

    content = _json_dumps_deterministic(out)
    _immut_write(out_path, content)

    print(f"OK: wrote {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
