#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation, DivisionByZero
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

DAY0_NOTE_ALLOWED = "DAY0_BOOTSTRAP_BROKER_MARKS_MISSING_ALLOWED"


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
        raise ValueError(f"invalid decimal: {x!r}")


def _ds(d: Decimal) -> str:
    q = d.quantize(Decimal("0.00000001"))
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (TRUTH_ROOT / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        # Fail-closed: if we cannot enumerate, treat as NOT bootstrap.
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_broker_marks_snapshot_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--produced_utc", required=False, help="UTC ISO-8601 Z timestamp (deterministic). If omitted, defaults to DAYT00:00:00Z")
    args = ap.parse_args()
    day = str(args.day_utc).strip()

    produced_utc = str(args.produced_utc).strip() if args.produced_utc is not None else ""
    if produced_utc == "":
        produced_utc = f"{day}T00:00:00Z"

    src = TRUTH_ROOT / "execution_evidence_v1" / "broker_statement_normalized_v1" / day / "broker_statement_normalized.v1.json"

    out_dir = TRUTH_ROOT / "market_data_snapshot_v1" / "broker_marks_v1" / day
    out_path = out_dir / "broker_marks.v1.json"

    # Day-0 bootstrap: if no submissions yet, broker marks snapshot may be missing.
    if not src.exists():
        if _bootstrap_window_true(day):
            out = {
                "schema_id": "C2_BROKER_MARKS_SNAPSHOT_V1",
                "schema_version": "1.0.0",
                "produced_utc": produced_utc,
                "day_utc": day,
                "producer": "ops/tools/run_broker_marks_snapshot_day_v1.py",
                "source_broker_statement_path": str(src.relative_to(TRUTH_ROOT)),
                "source_broker_statement_sha256": "0" * 64,
                "currency": "USD",
                "cash_end": "0",
                "marks": [],
                "notes": [
                    "Day-0 bootstrap: no submissions yet; broker statement not required for broker marks snapshot.",
                    DAY0_NOTE_ALLOWED,
                ],
            }
            _immut_write(out_path, _json_bytes(out))
            print(f"OK: wrote {out_path} (DAY0_BOOTSTRAP)")
            return 0

        raise SystemExit(f"FATAL: missing broker_statement_normalized: {src}")

    o = _load_json(src)
    positions = o.get("positions", [])
    currency = str(o.get("currency", "USD"))
    cash_end = str(o.get("cash_end", "0"))

    marks: List[Dict[str, str]] = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        sym = str(p.get("symbol", "")).strip()
        sec = str(p.get("sec_type", "")).strip()
        if sym == "" or sec == "":
            continue
        qty = _d(p.get("qty", "0"))
        mv = _d(p.get("market_value", "0"))
        avg = _d(p.get("avg_cost", "0"))

        if qty == 0:
            ip = Decimal("0")
        else:
            try:
                ip = (mv / qty)
            except (DivisionByZero, InvalidOperation):
                ip = Decimal("0")

        item = {
            "symbol": sym,
            "sec_type": sec,
            "qty": _ds(qty),
            "avg_cost": _ds(avg),
            "market_value": _ds(mv),
            "implied_price": _ds(ip),
        }
        ccy = p.get("currency", None)
        if ccy is not None:
            item["currency"] = str(ccy).strip()
        marks.append(item)

    out = {
        "schema_id": "C2_BROKER_MARKS_SNAPSHOT_V1",
        "schema_version": "1.0.0",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": "ops/tools/run_broker_marks_snapshot_day_v1.py",
        "source_broker_statement_path": str(src.relative_to(TRUTH_ROOT)),
        "source_broker_statement_sha256": _sha256_file(src),
        "currency": currency,
        "cash_end": cash_end,
        "marks": marks,
        "notes": ["Marks derived from broker-of-record normalized statement (IB Flex)."],
    }

    _immut_write(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
