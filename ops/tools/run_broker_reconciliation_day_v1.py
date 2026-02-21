#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime")
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

SCHEMA_ID = "C2_BROKER_RECONCILIATION_V1"
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


def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


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


def _pos_key(p: Dict[str, Any]) -> Tuple[str, str]:
    return (str(p.get("symbol", "")).strip(), str(p.get("sec_type", "")).strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD to reconcile (must have broker normalized + internal snapshots)")
    ap.add_argument("--cash_abs_tol", default="0.01", help="absolute tolerance for cash difference")
    ap.add_argument("--qty_abs_tol", default="0", help="absolute tolerance for quantity difference")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    cash_tol = _dec(args.cash_abs_tol)
    qty_tol = _dec(args.qty_abs_tol)

    broker_path = TRUTH_ROOT / "execution_evidence_v1" / "broker_statement_normalized_v1" / day / "broker_statement_normalized.v1.json"
    internal_pos_path = TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
    internal_cash_path = TRUTH_ROOT / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json"

    missing: List[str] = []
    if not broker_path.exists():
        missing.append(str(broker_path.relative_to(TRUTH_ROOT)))
    if not internal_pos_path.exists():
        missing.append(str(internal_pos_path.relative_to(TRUTH_ROOT)))
    if not internal_cash_path.exists():
        missing.append(str(internal_cash_path.relative_to(TRUTH_ROOT)))

    status = "PASS"
    fail_closed = False
    position_mismatches: List[Dict[str, str]] = []
    cash_diff = Decimal("0")

    notes: List[str] = []

    if missing:
        status = "MISSING_INPUTS"
        fail_closed = True
        notes.append("Missing required inputs; reconciliation cannot be performed.")
        for m in missing:
            notes.append(f"MISSING: {m}")
    else:
        broker = _load_json(broker_path)

        cash_obj = _load_json(internal_cash_path)
        if "cash_end" in cash_obj:
            internal_cash = _dec(cash_obj["cash_end"])
        elif "cash_balance" in cash_obj:
            internal_cash = _dec(cash_obj["cash_balance"])
        else:
            status = "MISSING_INPUTS"
            fail_closed = True
            notes.append("Internal cash ledger snapshot missing cash_end/cash_balance field.")
            internal_cash = Decimal("0")

        broker_cash = _dec(broker.get("cash_end", "0"))
        cash_diff = internal_cash - broker_cash

        pos_obj = _load_json(internal_pos_path)
        internal_positions = pos_obj.get("positions", [])
        if not isinstance(internal_positions, list):
            status = "MISSING_INPUTS"
            fail_closed = True
            notes.append("Internal positions snapshot missing positions[] list.")
            internal_positions = []

        broker_positions = broker.get("positions", [])
        if not isinstance(broker_positions, list):
            status = "MISSING_INPUTS"
            fail_closed = True
            notes.append("Broker normalized statement missing positions[] list.")
            broker_positions = []

        imap: Dict[Tuple[str, str], Decimal] = {}
        for p in internal_positions:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("symbol", "")).strip()
            sec = str(p.get("sec_type", "")).strip()
            if sym == "" or sec == "":
                continue
            qty = _dec(p.get("qty", "0"))
            imap[(sym, sec)] = imap.get((sym, sec), Decimal("0")) + qty

        bmap: Dict[Tuple[str, str], Decimal] = {}
        for p in broker_positions:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("symbol", "")).strip()
            sec = str(p.get("sec_type", "")).strip()
            if sym == "" or sec == "":
                continue
            qty = _dec(p.get("qty", "0"))
            bmap[(sym, sec)] = bmap.get((sym, sec), Decimal("0")) + qty

        all_keys = sorted(set(imap.keys()) | set(bmap.keys()))

        for k in all_keys:
            iq = imap.get(k, Decimal("0"))
            bq = bmap.get(k, Decimal("0"))
            diff = iq - bq
            if abs(diff) > qty_tol:
                position_mismatches.append(
                    {
                        "symbol": k[0],
                        "sec_type": k[1],
                        "internal_qty": _dec_str(iq),
                        "broker_qty": _dec_str(bq),
                        "qty_diff": _dec_str(diff),
                    }
                )

        if abs(cash_diff) > cash_tol:
            notes.append(f"CASH_DIFF_BREACH: internal_cash - broker_cash = {_dec_str(cash_diff)} > tol {_dec_str(cash_tol)}")

        if position_mismatches or abs(cash_diff) > cash_tol:
            status = "FAIL"
            fail_closed = True
        else:
            status = "PASS"
            fail_closed = False

    out = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": "ops/tools/run_broker_reconciliation_day_v1.py",
        "status": status,
        "fail_closed": fail_closed,
        "broker_statement_path": str(broker_path.relative_to(TRUTH_ROOT)),
        "broker_statement_sha256": _sha256_file(broker_path) if broker_path.exists() else "0" * 64,
        "internal_positions_path": str(internal_pos_path.relative_to(TRUTH_ROOT)),
        "internal_positions_sha256": _sha256_file(internal_pos_path) if internal_pos_path.exists() else "0" * 64,
        "internal_cash_ledger_path": str(internal_cash_path.relative_to(TRUTH_ROOT)),
        "internal_cash_ledger_sha256": _sha256_file(internal_cash_path) if internal_cash_path.exists() else "0" * 64,
        "cash_diff": _dec_str(cash_diff),
        "position_mismatches": position_mismatches,
        "tolerances": {"cash_abs": _dec_str(cash_tol), "qty_abs": _dec_str(qty_tol)},
        "notes": notes,
    }

    out_dir = TRUTH_ROOT / "reports" / "broker_reconciliation_v1" / day
    out_path = out_dir / "broker_reconciliation.v1.json"
    _immut_write(out_path, _json_dumps_deterministic(out))

    print(f"OK: BROKER_RECONCILIATION_V1_WRITTEN day_utc={day} status={status} path={out_path} sha256={_sha256_file(out_path)}")
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
