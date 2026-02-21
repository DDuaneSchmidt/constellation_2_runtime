#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")

def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _immut_write(path: Path, content: bytes) -> None:
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(content).hexdigest():
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content)

def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return Decimal("0")

def _ds(d: Decimal) -> str:
    q = d.quantize(Decimal("0.00000001"))
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s

def main() -> int:
    ap = argparse.ArgumentParser(prog="run_accounting_nav_v2_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--producer_repo", default="constellation_2_runtime")
    ap.add_argument("--producer_git_sha", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()

    cash_path = TRUTH_ROOT / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json"
    pos_path = TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
    marks_path = TRUTH_ROOT / "market_data_snapshot_v1" / "broker_marks_v1" / day / "broker_marks.v1.json"

    missing = []
    for p in [cash_path, pos_path, marks_path]:
        if not p.exists():
            missing.append(str(p.relative_to(TRUTH_ROOT)))

    if missing:
        raise SystemExit("FATAL: missing required inputs: " + ", ".join(missing))

    cash = _load_json(cash_path)
    marks = _load_json(marks_path)

    cash_total_cents = int(cash["snapshot"]["cash_total_cents"])
    cash_total = int(cash_total_cents // 100)

    components: List[Dict[str, Any]] = [
        {
            "kind": "CASH",
            "symbol": "USD",
            "qty": str(cash_total),
            "mv": cash_total,
            "mark": {"bid": None, "ask": None, "last": None, "source": "CASH_LEDGER", "asof_utc": f"{day}T00:00:00Z"},
        }
    ]

    gross_mv = 0
    unreal = 0

    for m in marks.get("marks", []):
        sym = str(m.get("symbol") or "").strip()
        sec = str(m.get("sec_type") or "").strip()
        qty = _d(m.get("qty"))
        mv = int(_d(m.get("market_value")))
        ip = _d(m.get("implied_price"))
        avg = _d(m.get("avg_cost"))
        gross_mv += mv
        unreal += int(qty * (ip - avg))

        components.append(
            {
                "kind": "BROKER_MARK",
                "symbol": sym,
                "sec_type": sec,
                "qty": _ds(qty),
                "mv": mv,
                "mark": {"bid": None, "ask": None, "last": _ds(ip), "source": "BROKER_MARKS_V1", "asof_utc": f"{day}T00:00:00Z"},
            }
        )

    nav_total = int(cash_total) + int(gross_mv)

    input_manifest = [
        {"type": "cash_ledger", "path": str(cash_path), "sha256": _sha256_file(cash_path), "day_utc": day, "producer": "cash_ledger_v1"},
        {"type": "positions_truth", "path": str(pos_path), "sha256": _sha256_file(pos_path), "day_utc": day, "producer": "positions_v1"},
        {"type": "broker_marks", "path": str(marks_path), "sha256": _sha256_file(marks_path), "day_utc": day, "producer": "broker_marks_v1"},
    ]

    out = {
        "schema_id": "C2_ACCOUNTING_NAV_V2",
        "schema_version": 2,
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": {"repo": str(args.producer_repo), "git_sha": str(args.producer_git_sha), "module": "ops/tools/run_accounting_nav_v2_day_v1.py"},
        "status": "ACTIVE",
        "reason_codes": ["BROKER_MARKS_SOURCE_V1"],
        "input_manifest": input_manifest,
        "nav": {
            "currency": str(marks.get("currency", "USD")),
            "nav_total": nav_total,
            "cash_total": cash_total,
            "gross_positions_value": int(gross_mv),
            "realized_pnl_to_date": 0,
            "unrealized_pnl": int(unreal),
            "components": components,
            "notes": ["marks derived from broker-of-record (IB Flex)"]
        },
        "history": {}
    }

    out_dir = TRUTH_ROOT / "accounting_v2" / "nav" / day
    out_path = out_dir / "nav.v2.json"
    _immut_write(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
