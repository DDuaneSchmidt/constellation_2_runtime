from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Fail-closed import root
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseC.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402


class TransformerError(Exception):
    pass


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise TransformerError(f"TEMP_EXISTS: {tmp}")
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
        raise TransformerError(f"ATOMIC_WRITE_FAILED: {path}: {e}") from e


def _ensure_out_dir_ready(out_dir: Path) -> None:
    if out_dir.exists():
        if not out_dir.is_dir():
            raise TransformerError(f"OUT_DIR_NOT_DIR: {out_dir}")
        if list(out_dir.iterdir()):
            raise TransformerError(f"OUT_DIR_NOT_EMPTY: {out_dir}")
        return
    out_dir.mkdir(parents=True, exist_ok=False)


def _dec(s: str, name: str) -> Decimal:
    if not isinstance(s, str) or not s.strip():
        raise TransformerError(f"DECIMAL_STRING_REQUIRED: {name}")
    try:
        return Decimal(s.strip())
    except InvalidOperation as e:
        raise TransformerError(f"DECIMAL_PARSE_FAILED: {name}={s!r}") from e


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise TransformerError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise TransformerError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _load_nav_usd_from_accounting_latest(repo_root: Path) -> Tuple[int, str]:
    """
    Deterministic: read accounting_v1/latest.json -> pointers.nav_path -> nav.json -> nav.nav_total (int dollars).
    Fail-closed if any field missing or wrong type.
    Returns (nav_total_usd_int, nav_path_str).
    """
    p_latest = repo_root / "constellation_2/runtime/truth/accounting_v1/latest.json"
    latest = _read_json_obj(p_latest)

    ptrs = latest.get("pointers")
    if not isinstance(ptrs, dict):
        raise TransformerError("ACCOUNTING_LATEST_POINTERS_MISSING")
    nav_path_s = ptrs.get("nav_path")
    if not isinstance(nav_path_s, str) or not nav_path_s.strip():
        raise TransformerError("ACCOUNTING_LATEST_NAV_PATH_MISSING")
    p_nav = Path(nav_path_s).resolve()
    nav_obj = _read_json_obj(p_nav)

    nav = nav_obj.get("nav")
    if not isinstance(nav, dict):
        raise TransformerError("ACCOUNTING_NAV_OBJECT_MISSING")
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int):
        raise TransformerError("ACCOUNTING_NAV_TOTAL_NOT_INT")
    return nav_total, str(p_nav)


@dataclass(frozen=True)
class CapsV1:
    per_trade_notional_pct_max: Decimal  # v1 conservative: treat risk as notional
    portfolio_net_delta_pct_max: Decimal
    underlying_concentration_pct_max: Decimal
    engine_allocation_pct_max: Decimal


CAPS = CapsV1(
    per_trade_notional_pct_max=Decimal("0.01"),
    portfolio_net_delta_pct_max=Decimal("0.60"),
    underlying_concentration_pct_max=Decimal("0.05"),
    engine_allocation_pct_max=Decimal("0.40"),
)


def _risk_multiplier_from_drawdown_pct(drawdown_pct: Optional[Decimal]) -> Decimal:
    # drawdown_pct expected negative for drawdown. If missing, assume 1.0 (fail-open is forbidden; but missing drawdown is common bootstrap).
    if drawdown_pct is None:
        return Decimal("1.0")
    if drawdown_pct <= Decimal("-0.15"):
        return Decimal("0.25")
    if drawdown_pct <= Decimal("-0.10"):
        return Decimal("0.50")
    if drawdown_pct <= Decimal("-0.05"):
        return Decimal("0.75")
    return Decimal("1.0")


def _parse_drawdown_pct_from_nav(nav_obj: Dict[str, Any]) -> Optional[Decimal]:
    """
    Best-effort: accounting nav has history.drawdown_pct (may be null).
    If missing or null -> None.
    """
    hist = nav_obj.get("history")
    if not isinstance(hist, dict):
        return None
    dd = hist.get("drawdown_pct")
    if dd is None:
        return None
    if isinstance(dd, (int, float)):
        # floats forbidden elsewhere; but this is input JSON; still fail-closed because determinism standard forbids floats.
        raise TransformerError("ACCOUNTING_DRAWDOWN_PCT_FLOAT_FORBIDDEN")
    if isinstance(dd, str):
        return _dec(dd, "drawdown_pct")
    return None


def _equity_qty_from_notional(nav_total_usd_int: int, target_pct: Decimal, ref_price: Decimal) -> int:
    if nav_total_usd_int <= 0:
        raise TransformerError("NAV_TOTAL_NONPOSITIVE")
    if ref_price <= Decimal("0"):
        raise TransformerError("REFERENCE_PRICE_NONPOSITIVE")
    notional = (Decimal(nav_total_usd_int) * target_pct)
    qty = (notional / ref_price).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    q = int(qty)
    return q if q >= 1 else 1


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_risk_transformer_offline_v1")
    ap.add_argument("--exposure_intent", required=True, help="Path to ExposureIntent v1 JSON")
    ap.add_argument("--day_utc", required=True, help="Day key YYYY-MM-DD used for reading portfolio state")
    ap.add_argument("--eval_time_utc", required=True, help="ISO-8601 Z timestamp (deterministic clock)")
    ap.add_argument("--out_dir", required=True, help="Output directory (must not exist or must be empty)")
    ap.add_argument("--equity_reference_price", default="", help="Required for LONG_EQUITY: decimal string price (deterministic operator input)")
    args = ap.parse_args(argv)

    repo_root = REPO_ROOT
    out_dir = Path(args.out_dir).resolve()
    _ensure_out_dir_ready(out_dir)

    exp_path = Path(args.exposure_intent).resolve()
    exp = _read_json_obj(exp_path)

    # Validate exposure intent schema
    validate_against_repo_schema_v1(exp, repo_root, "constellation_2/schemas/exposure_intent.v1.schema.json")

    exposure_type = exp.get("exposure_type")
    if not isinstance(exposure_type, str):
        raise TransformerError("EXPOSURE_TYPE_MISSING")

    target_pct = _dec(exp["target_notional_pct"], "target_notional_pct")

    # Conservative per-trade cap for v1 equity (treat notional as risk proxy)
    if target_pct > CAPS.per_trade_notional_pct_max:
        raise TransformerError(f"PER_TRADE_NOTIONAL_CAP_EXCEEDED: target={str(target_pct)} cap={str(CAPS.per_trade_notional_pct_max)}")

    nav_total_usd_int, nav_path = _load_nav_usd_from_accounting_latest(repo_root)

    # Drawdown scaling (from nav history if present)
    nav_obj = _read_json_obj(Path(nav_path))
    dd_pct = _parse_drawdown_pct_from_nav(nav_obj)
    mult = _risk_multiplier_from_drawdown_pct(dd_pct)
    scaled_pct = (target_pct * mult)

    # Output routing
    if exposure_type == "LONG_EQUITY":
        ref_price_s = (args.equity_reference_price or "").strip()
        if not ref_price_s:
            raise TransformerError("EQUITY_REFERENCE_PRICE_REQUIRED_FOR_LONG_EQUITY")
        ref_price = _dec(ref_price_s, "equity_reference_price")

        qty = _equity_qty_from_notional(nav_total_usd_int, scaled_pct, ref_price)

        sym = exp["underlying"]["symbol"]
        ccy = exp["underlying"]["currency"]

        eq_intent = {
            "schema_id": "equity_intent",
            "schema_version": "v1",
            "intent_id": exp["intent_id"],
            "created_at_utc": args.eval_time_utc,
            "engine": exp["engine"],
            "underlying": {"symbol": sym, "currency": ccy},
            "intent_type": "EQUITY_LONG_OPEN",
            "sizing": {
                "target_notional_pct": str(scaled_pct),
                "max_risk_pct": str(CAPS.per_trade_notional_pct_max),
            },
            "exit_policy": {
                "policy_id": "c2_equity_time_exit_only_v1",
                "time_exit": {"enabled": True, "max_holding_days": int(exp["expected_holding_days"])},
            },
            "canonical_json_hash": None,
        }
        eq_intent["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_intent)

        intent_hash = canonical_hash_for_c2_artifact_v1(eq_intent)

        eq_plan = {
            "schema_id": "equity_order_plan",
            "schema_version": "v1",
            "plan_id": exp["intent_id"],
            "created_at_utc": args.eval_time_utc,
            "intent_hash": intent_hash,
            "structure": "EQUITY_SPOT",
            "symbol": sym,
            "currency": ccy,
            "action": "BUY",
            "qty_shares": qty,
            "order_terms": {"order_type": "LIMIT", "limit_price": str(ref_price), "time_in_force": "DAY"},
            "risk_proof": None,
            "canonical_json_hash": None,
        }
        eq_plan["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_plan)

        _atomic_write_bytes(out_dir / "equity_intent.v1.json", canonical_json_bytes_v1(eq_intent) + b"\n")
        _atomic_write_bytes(out_dir / "equity_order_plan.v1.json", canonical_json_bytes_v1(eq_plan) + b"\n")

        print("OK: RISK_TRANSFORMER_EMITTED_EQUITY")
        return 0

    raise TransformerError(f"UNSUPPORTED_EXPOSURE_TYPE_V1: {exposure_type!r}")


if __name__ == "__main__":
    raise SystemExit(main())
