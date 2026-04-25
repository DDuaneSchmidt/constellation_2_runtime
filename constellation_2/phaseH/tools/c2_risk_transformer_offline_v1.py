from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Drawdown convention authority (canonical, negative underwater)
# Contract: C2_DRAWDOWN_CONVENTION_V1
C2_DRAWDOWN_CONTRACT_ID = "C2_DRAWDOWN_CONVENTION_V1"
DRAWDOWN_QUANT = Decimal("0.000001")  # 6dp per contract
PRICE_QUANT = Decimal("0.01")


# Fail-closed import root
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseC.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402
from constellation_2.common.c2_risk_policy_loader_v1 import (  # noqa: E402
    RiskPolicyLoaderError,
    get_per_trade_notional_pct_max_or_fail,
)
from constellation_2.common.canonical_fact_store_v1 import capture_executed_code_identity  # noqa: E402
from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402


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


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _repo_git_sha_or_fail(repo_root: Path) -> str:
    git_sha = str(capture_executed_code_identity(repo_root=repo_root).get("git_sha") or "").strip()
    if git_sha:
        return git_sha
    raise TransformerError("REPO_GIT_SHA_UNAVAILABLE_FAIL_CLOSED")


def _parse_day_utc_or_fail(day_utc: str) -> str:
    d = (day_utc or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise TransformerError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _resolve_truth_root_arg(repo_root: Path, truth_root_arg: str) -> Path:
    raw = (truth_root_arg or "").strip()
    if raw:
        p = Path(raw).expanduser().resolve()
    else:
        env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
        if env_root:
            p = Path(env_root).expanduser().resolve()
        else:
            p = resolve_truth_root(repo_root=repo_root)
    if not p.is_absolute() or (not p.exists()) or (not p.is_dir()):
        raise TransformerError(f"TRUTH_ROOT_INVALID_OR_MISSING: {p}")
    return p


def _load_nav_usd_from_accounting_day(repo_root: Path, day_utc: str, truth_root: Path) -> Tuple[int, str]:
    """
    Deterministic (day-keyed): prefer accounting_compat_v1, then accounting_v2, then accounting_v1.
    Fail-closed if any field missing or wrong type.
    Returns (nav_total_usd_int, nav_path_str).
    """
    day = _parse_day_utc_or_fail(day_utc)
    p_nav_compat = (truth_root / "accounting_compat_v1" / "nav" / day / "nav_snapshot.v1.json").resolve()
    if p_nav_compat.exists() and p_nav_compat.is_file():
        nav_obj = _read_json_obj(p_nav_compat)
        nav = nav_obj.get("nav")
        if not isinstance(nav, dict):
            raise TransformerError("ACCOUNTING_NAV_OBJECT_MISSING")
        nav_total = nav.get("nav_total")
        if not isinstance(nav_total, int):
            raise TransformerError("ACCOUNTING_NAV_TOTAL_NOT_INT")
        return nav_total, str(p_nav_compat)

    p_nav_v2 = (truth_root / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    if p_nav_v2.exists() and p_nav_v2.is_file():
        nav_obj = _read_json_obj(p_nav_v2)
        nav = nav_obj.get("nav")
        if not isinstance(nav, dict):
            raise TransformerError("ACCOUNTING_NAV_OBJECT_MISSING")
        nav_total = nav.get("nav_total")
        if not isinstance(nav_total, int):
            raise TransformerError("ACCOUNTING_NAV_TOTAL_NOT_INT")
        return nav_total, str(p_nav_v2)

    p_nav = (truth_root / "accounting_v1" / "nav" / day / "nav.json").resolve()
    nav_obj = _read_json_obj(p_nav)

    nav = nav_obj.get("nav")
    if not isinstance(nav, dict):
        raise TransformerError("ACCOUNTING_NAV_OBJECT_MISSING")
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int):
        raise TransformerError("ACCOUNTING_NAV_TOTAL_NOT_INT")
    return nav_total, str(p_nav)


def drawdown_multiplier_v1(drawdown_pct: Decimal) -> Decimal:
    """
    Canonical drawdown-to-multiplier rule per C2_DRAWDOWN_CONVENTION_V1.

    - drawdown_pct == 0 at peaks
    - drawdown_pct < 0 underwater (negative)
    - inclusive thresholds, evaluated from most severe to least
    - clamp at 0.25 for drawdown < -0.15
    """
    dd = drawdown_pct.quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)

    if dd <= Decimal("-0.150000"):
        return Decimal("0.25")
    if dd <= Decimal("-0.100000"):
        return Decimal("0.50")
    if dd <= Decimal("-0.050000"):
        return Decimal("0.75")
    return Decimal("1.00")


def _parse_drawdown_pct_from_nav_or_fail(nav_obj: Dict[str, Any]) -> Decimal:
    """
    Contract enforcement: drawdown_pct must exist and must be a DECIMAL STRING.
    Fail-closed if missing/null/invalid.

    NOTE: This is Blocker A hardening; missing drawdown is no longer allowed.
    """
    hist = nav_obj.get("history")
    if not isinstance(hist, dict):
        raise TransformerError("DRAWDOWN_HISTORY_MISSING_FAIL_CLOSED")
    dd = hist.get("drawdown_pct")
    if dd is None:
        raise TransformerError("DRAWDOWN_MISSING_FAIL_CLOSED")
    if isinstance(dd, (int, float)):
        raise TransformerError("ACCOUNTING_DRAWDOWN_PCT_FLOAT_FORBIDDEN")
    if isinstance(dd, str):
        return _dec(dd, "drawdown_pct").quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)
    raise TransformerError("DRAWDOWN_INVALID_TYPE_FAIL_CLOSED")


def _equity_qty_from_notional(nav_total_usd_int: int, target_pct: Decimal, ref_price: Decimal) -> int:
    if nav_total_usd_int <= 0:
        raise TransformerError("NAV_TOTAL_NONPOSITIVE")
    if ref_price <= Decimal("0"):
        raise TransformerError("REFERENCE_PRICE_NONPOSITIVE")
    notional = (Decimal(nav_total_usd_int) * target_pct)
    qty = (notional / ref_price).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    q = int(qty)
    return q if q >= 1 else 1


def _load_per_trade_notional_pct_max_or_fail(engine_id: str) -> Decimal:
    try:
        raw = get_per_trade_notional_pct_max_or_fail(engine_id)
    except RiskPolicyLoaderError as e:
        raise TransformerError(f"GOVERNED_RISK_POLICY_LOAD_FAILED: {e}") from e
    return _dec(raw, "per_trade_notional_pct_max")


def _require_stop_loss_bps_from_exposure_or_fail(exposure_intent: Dict[str, Any]) -> int:
    constraints = exposure_intent.get("constraints")
    if not isinstance(constraints, dict):
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: constraints missing")
    stop_loss_bps = constraints.get("stop_loss_bps")
    if not isinstance(stop_loss_bps, int) or stop_loss_bps <= 0:
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: stop_loss_bps missing or non-positive")
    return int(stop_loss_bps)


def _derive_stop_price_or_fail(*, entry_price: Decimal, action: str, stop_loss_bps: int) -> Decimal:
    if entry_price <= Decimal("0"):
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: entry reference price non-positive")
    bps = Decimal(stop_loss_bps)
    if bps <= Decimal("0"):
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: stop_loss_bps non-positive")
    if action == "BUY":
        stop = entry_price * (Decimal("1") - (bps / Decimal("10000")))
    elif action == "SELL":
        stop = entry_price * (Decimal("1") + (bps / Decimal("10000")))
    else:
        raise TransformerError(f"INTENT_PROTECTIVE_STOP_MISSING: unsupported action={action!r}")
    stop_q = stop.quantize(PRICE_QUANT, rounding=ROUND_HALF_UP)
    if stop_q <= Decimal("0"):
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: derived stop price non-positive")
    if action == "BUY" and stop_q >= entry_price:
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: derived stop must be below entry price for BUY")
    if action == "SELL" and stop_q <= entry_price:
        raise TransformerError("INTENT_PROTECTIVE_STOP_MISSING: derived stop must be above entry price for SELL")
    return stop_q


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_risk_transformer_offline_v1")
    ap.add_argument("--exposure_intent", required=True, help="Path to ExposureIntent v1 JSON")
    ap.add_argument("--day_utc", required=True, help="Day key YYYY-MM-DD used for reading portfolio state")
    ap.add_argument("--eval_time_utc", required=True, help="ISO-8601 Z timestamp (deterministic clock)")
    ap.add_argument("--out_dir", required=True, help="Output directory (must not exist or must be empty)")
    ap.add_argument("--equity_reference_price", default="", help="Required for LONG_EQUITY: decimal string price (deterministic operator input)")
    ap.add_argument("--truth_root", default="", help="Absolute truth root for accounting/nav inputs")
    args = ap.parse_args(argv)

    repo_root = REPO_ROOT
    out_dir = Path(args.out_dir).resolve()
    _ensure_out_dir_ready(out_dir)
    truth_root = _resolve_truth_root_arg(repo_root, str(args.truth_root or ""))

    exp_path = Path(args.exposure_intent).resolve()
    exp = _read_json_obj(exp_path)
    exp_sha256 = _sha256_file(exp_path)

    # Validate exposure intent schema
    validate_against_repo_schema_v1(exp, repo_root, "constellation_2/schemas/exposure_intent.v1.schema.json")

    exposure_type = exp.get("exposure_type")
    if not isinstance(exposure_type, str):
        raise TransformerError("EXPOSURE_TYPE_MISSING")

    target_pct = _dec(exp["target_notional_pct"], "target_notional_pct")

    # Output routing
    if exposure_type == "LONG_EQUITY":
        ref_price_s = (args.equity_reference_price or "").strip()
        if not ref_price_s:
            raise TransformerError("EQUITY_REFERENCE_PRICE_REQUIRED_FOR_LONG_EQUITY")
        ref_price = _dec(ref_price_s, "equity_reference_price")

        sym = exp["underlying"]["symbol"]
        ccy = exp["underlying"]["currency"]
        engine = exp.get("engine")
        if not isinstance(engine, dict):
            raise TransformerError("EXPOSURE_INTENT_ENGINE_NOT_OBJECT")
        engine_id = str(engine.get("engine_id") or "").strip()
        if not engine_id:
            raise TransformerError("EXPOSURE_INTENT_ENGINE_ID_MISSING")
        per_trade_notional_pct_max = _load_per_trade_notional_pct_max_or_fail(engine_id)
        stop_loss_bps = _require_stop_loss_bps_from_exposure_or_fail(exp)

        # Conservative per-trade cap for v1 equity (treat notional as risk proxy)
        if target_pct > per_trade_notional_pct_max:
            raise TransformerError(f"PER_TRADE_NOTIONAL_CAP_EXCEEDED: target={str(target_pct)} cap={str(per_trade_notional_pct_max)}")

        nav_total_usd_int, nav_path = _load_nav_usd_from_accounting_day(repo_root, args.day_utc, truth_root)

        # Drawdown scaling (strict: fail closed if drawdown missing)
        nav_obj = _read_json_obj(Path(nav_path))
        dd_pct = _parse_drawdown_pct_from_nav_or_fail(nav_obj)
        mult = drawdown_multiplier_v1(dd_pct)
        scaled_pct = (target_pct * mult)

        qty = _equity_qty_from_notional(nav_total_usd_int, scaled_pct, ref_price)
        stop_price = _derive_stop_price_or_fail(entry_price=ref_price, action="BUY", stop_loss_bps=stop_loss_bps)

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
                "max_risk_pct": str(per_trade_notional_pct_max),
            },
            "exit_policy": {
                "policy_id": "c2_equity_time_exit_only_v1",
                "time_exit": {"enabled": True, "max_holding_days": int(exp["expected_holding_days"])},
                "protective_stop": {
                    "enabled": True,
                    "stop_price": str(stop_price),
                    "stop_loss_bps": int(stop_loss_bps),
                    "basis": "ENTRY_REFERENCE_PRICE",
                },
            },
            "canonical_json_hash": None,
        }
        eq_intent["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_intent)

        intent_hash = canonical_hash_for_c2_artifact_v1(eq_intent)
        producer_git_sha = _repo_git_sha_or_fail(repo_root)
        lineage_envelope = {
            "schema_id": "lineage_envelope.v1",
            "schema_version": "v1",
            "engine_id": engine_id,
            "source_intent_id": str(exp.get("intent_id") or "").strip(),
            "intent_sha256": exp_sha256,
            "producer": "constellation_2.phaseH.tools.c2_risk_transformer_offline_v1",
            "producer_git_sha": producer_git_sha,
            "invoked_day_utc": str(args.day_utc),
            "upstream_fact_refs": [
                {
                    "fact_type": "intent_fact.v1",
                    "path": str(exp_path),
                    "sha256": exp_sha256,
                }
            ],
            "generated_at_utc": args.eval_time_utc,
        }
        validate_against_repo_schema_v1(
            lineage_envelope,
            repo_root,
            "governance/04_DATA/SCHEMAS/C2/FACTS/lineage_envelope.v1.schema.json",
        )
        lineage_envelope_sha256 = hashlib.sha256(canonical_json_bytes_v1(lineage_envelope)).hexdigest()

        eq_plan = {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": exp["intent_id"],
            "created_at_utc": args.eval_time_utc,
            "intent_hash": intent_hash,
            "structure": "EQUITY_SPOT",
            "symbol": sym,
            "currency": ccy,
            "action": "BUY",
            "qty_shares": qty,
            "order_terms": {"order_type": "LIMIT", "limit_price": str(ref_price), "time_in_force": "DAY"},
            "protective_stop": {
                "order_type": "STOP",
                "stop_price": str(stop_price),
                "time_in_force": "DAY",
                "basis": "ENTRY_REFERENCE_PRICE",
                "stop_loss_bps": int(stop_loss_bps),
            },
            "take_profit": None,
            "bracket": {
                "enabled": True,
                "oca_group": None,
                "transmit_sequence": "PARENT_FALSE_FINAL_CHILD_TRUE",
            },
            "risk_proof": None,
            "engine_id": engine_id,
            "source_intent_id": str(exp.get("intent_id") or "").strip(),
            "intent_sha256": exp_sha256,
            "lineage_envelope_ref": {
                "path": "lineage_envelope.v1.json",
                "sha256": lineage_envelope_sha256,
            },
            "canonical_json_hash": None,
        }
        validate_against_repo_schema_v1(eq_plan, repo_root, "constellation_2/schemas/equity_order_plan.v2.schema.json")
        eq_plan["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_plan)

        _atomic_write_bytes(out_dir / "equity_intent.v1.json", canonical_json_bytes_v1(eq_intent) + b"\n")
        _atomic_write_bytes(out_dir / "lineage_envelope.v1.json", canonical_json_bytes_v1(lineage_envelope) + b"\n")
        _atomic_write_bytes(out_dir / "equity_order_plan.v2.json", canonical_json_bytes_v1(eq_plan) + b"\n")

        print("OK: RISK_TRANSFORMER_EMITTED_EQUITY")
        return 0

    raise TransformerError(f"UNSUPPORTED_EXPOSURE_TYPE_V1: {exposure_type!r}")


if __name__ == "__main__":
    raise SystemExit(main())
