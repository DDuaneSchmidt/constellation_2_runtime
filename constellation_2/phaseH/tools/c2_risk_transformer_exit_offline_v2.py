#!/usr/bin/env python3
"""
c2_risk_transformer_exit_offline_v2.py

Same intent as v1 exit transformer, but emits:
- equity_order_plan.v2.json with required lineage fields:
  engine_id, source_intent_id, intent_sha256 (sha256 of ExposureIntent file bytes)

Outputs:
  equity_intent.v1.json
  equity_order_plan.v2.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseC.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402


class ExitTransformerError(Exception):
    pass


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise ExitTransformerError(f"TEMP_EXISTS: {tmp}")
    try:
        with tmp.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp), str(path))
    except Exception as e:  # noqa: BLE001
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise ExitTransformerError(f"ATOMIC_WRITE_FAILED: {path}: {e}") from e


def _ensure_out_dir_ready(out_dir: Path) -> None:
    if out_dir.exists():
        if not out_dir.is_dir():
            raise ExitTransformerError(f"OUT_DIR_NOT_DIR: {out_dir}")
        if list(out_dir.iterdir()):
            raise ExitTransformerError(f"OUT_DIR_NOT_EMPTY: {out_dir}")
        return
    out_dir.mkdir(parents=True, exist_ok=False)


def _dec(s: str, name: str) -> Decimal:
    if not isinstance(s, str) or not s.strip():
        raise ExitTransformerError(f"DECIMAL_STRING_REQUIRED: {name}")
    try:
        return Decimal(s.strip())
    except InvalidOperation as e:
        raise ExitTransformerError(f"DECIMAL_PARSE_FAILED: {name}={s!r}") from e


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ExitTransformerError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ExitTransformerError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _parse_day_utc(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ExitTransformerError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _find_positions_snapshot_for_day(repo_root: Path, day: str) -> Path:
    day_dir = (repo_root / "constellation_2/runtime/truth/positions_v1/snapshots" / day).resolve()
    if not day_dir.exists() or not day_dir.is_dir():
        raise ExitTransformerError(f"POSITIONS_SNAPSHOT_DAY_DIR_MISSING: {day_dir}")
    for v in (5, 4, 3, 2, 1):
        p = day_dir / f"positions_snapshot.v{v}.json"
        if p.exists() and p.is_file():
            return p
    raise ExitTransformerError(f"NO_POSITIONS_SNAPSHOT_FOUND_FOR_DAY: {day_dir}")


def _exit_qty_from_positions_snapshot(pos_obj: Dict[str, Any], engine_id: str, symbol: str) -> int:
    pos = pos_obj.get("positions")
    if not isinstance(pos, dict):
        raise ExitTransformerError("POSITIONS_BLOCK_MISSING")
    items = pos.get("items")
    if not isinstance(items, list):
        raise ExitTransformerError("POSITIONS_ITEMS_NOT_LIST")

    total_qty = 0
    matched = 0

    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("status") or "").strip() != "OPEN":
            continue
        if str(it.get("engine_id") or "").strip() != engine_id:
            continue
        inst = it.get("instrument")
        if not isinstance(inst, dict):
            continue
        if str(inst.get("kind") or "").strip() != "EQUITY":
            continue
        underlying = inst.get("underlying")
        if not isinstance(underlying, str) or underlying.strip() == "":
            continue
        if underlying.strip() != symbol:
            continue
        qty = it.get("qty")
        if not isinstance(qty, int):
            raise ExitTransformerError("POSITION_QTY_NOT_INT")
        matched += 1
        total_qty += qty

    if matched <= 0 or total_qty <= 0:
        raise ExitTransformerError(f"NO_MATCHING_OPEN_EQUITY_POSITION_FOR_EXIT: engine_id={engine_id} symbol={symbol}")
    return int(total_qty)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_risk_transformer_exit_offline_v2")
    ap.add_argument("--exposure_intent", required=True, help="Path to ExposureIntent v1 JSON (EXIT intent target=0)")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--positions_snapshot_path", default="")
    args = ap.parse_args(argv)

    day = _parse_day_utc(args.day_utc)
    out_dir = Path(args.out_dir).resolve()
    _ensure_out_dir_ready(out_dir)

    exp_path = Path(args.exposure_intent).resolve()
    exp_bytes = exp_path.read_bytes()
    exp_sha256 = hashlib.sha256(exp_bytes).hexdigest()
    exp = json.loads(exp_bytes.decode("utf-8"))
    if not isinstance(exp, dict):
        raise ExitTransformerError("EXPOSURE_INTENT_NOT_OBJECT")

    validate_against_repo_schema_v1(exp, REPO_ROOT, "constellation_2/schemas/exposure_intent.v1.schema.json")

    if exp.get("exposure_type") != "LONG_EQUITY":
        raise ExitTransformerError(f"UNSUPPORTED_EXPOSURE_TYPE_FOR_EXIT_V2: {exp.get('exposure_type')!r}")

    target_pct = _dec(str(exp.get("target_notional_pct") or ""), "target_notional_pct")
    if target_pct != Decimal("0"):
        raise ExitTransformerError(f"EXIT_TRANSFORMER_REQUIRES_TARGET_ZERO: target={str(target_pct)}")

    engine = exp.get("engine")
    if not isinstance(engine, dict):
        raise ExitTransformerError("EXPOSURE_INTENT_ENGINE_NOT_OBJECT")
    engine_id = str(engine.get("engine_id") or "").strip()
    suite = str(engine.get("suite") or "").strip()
    mode = str(engine.get("mode") or "").strip()
    if not engine_id or not suite or not mode:
        raise ExitTransformerError("ENGINE_FIELDS_MISSING")

    und = exp.get("underlying")
    if not isinstance(und, dict):
        raise ExitTransformerError("UNDERLYING_NOT_OBJECT")
    sym = str(und.get("symbol") or "").strip()
    ccy = str(und.get("currency") or "").strip()
    if not sym or not ccy:
        raise ExitTransformerError("UNDERLYING_FIELDS_MISSING")

    if str(args.positions_snapshot_path or "").strip():
        pos_path = Path(str(args.positions_snapshot_path).strip()).resolve()
    else:
        pos_path = _find_positions_snapshot_for_day(REPO_ROOT, day)

    pos_obj = _read_json_obj(pos_path)
    qty = _exit_qty_from_positions_snapshot(pos_obj, engine_id=engine_id, symbol=sym)

    eq_intent: Dict[str, Any] = {
        "schema_id": "equity_intent",
        "schema_version": "v1",
        "intent_id": exp["intent_id"],
        "created_at_utc": args.eval_time_utc,
        "engine": {"engine_id": engine_id, "suite": suite, "mode": mode},
        "underlying": {"symbol": sym, "currency": ccy},
        "intent_type": "EQUITY_LONG_CLOSE",
        "sizing": {"target_notional_pct": "0", "max_risk_pct": "0"},
        "exit_policy": {"policy_id": "c2_equity_exit_immediate_v1", "time_exit": {"enabled": True, "max_holding_days": 0}},
        "canonical_json_hash": None,
    }
    validate_against_repo_schema_v1(eq_intent, REPO_ROOT, "constellation_2/schemas/equity_intent.v1.schema.json")
    eq_intent["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_intent)

    intent_hash = canonical_hash_for_c2_artifact_v1(eq_intent)

    eq_plan: Dict[str, Any] = {
        "schema_id": "equity_order_plan",
        "schema_version": "v2",
        "plan_id": exp["intent_id"],
        "created_at_utc": args.eval_time_utc,
        "intent_hash": intent_hash,
        "structure": "EQUITY_SPOT",
        "symbol": sym,
        "currency": ccy,
        "action": "SELL",
        "qty_shares": int(qty),
        "order_terms": {"order_type": "MARKET", "limit_price": None, "time_in_force": "DAY"},
        "risk_proof": None,
        "engine_id": engine_id,
        "source_intent_id": str(exp.get("intent_id") or "").strip(),
        "intent_sha256": exp_sha256,
        "canonical_json_hash": None,
    }
    validate_against_repo_schema_v1(eq_plan, REPO_ROOT, "constellation_2/schemas/equity_order_plan.v2.schema.json")
    eq_plan["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_plan)

    _atomic_write_bytes(out_dir / "equity_intent.v1.json", canonical_json_bytes_v1(eq_intent) + b"\n")
    _atomic_write_bytes(out_dir / "equity_order_plan.v2.json", canonical_json_bytes_v1(eq_plan) + b"\n")

    print("OK: EXIT_RISK_TRANSFORMER_V2_EMITTED_EQUITY_CLOSE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
