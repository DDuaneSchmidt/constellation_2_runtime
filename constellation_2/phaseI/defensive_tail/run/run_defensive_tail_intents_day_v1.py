"""
run_defensive_tail_intents_day_v1.py

Constellation 2.0 — Engine 5 (Defensive / Tail Hedge Sleeve)
Exposure-only intent emitter (ExposureIntent v2)

NON-NEGOTIABLE PROPERTIES:
- Deterministic
- Fail-closed
- Uses ONLY existing truth spines (no new truth roots):
    - market_data_snapshot_v1
    - accounting_v1/nav
    - positions_snapshot_v2
    - monitoring_v1/engine_correlation_matrix
    - monitoring_v1/regime_snapshot_v2
- Writes at most 1 ExposureIntent v2 per day into:
    constellation_2/runtime/truth/intents_v1/snapshots/<day_utc>/<sha256>.exposure_intent.v2.json
- REFUSE overwrite (immutable truth)

ENTRY RULE (Deterministic):
Enter defensive overlay if ANY:
- regime != "NORMAL"
- max_pairwise_corr > 0.70
- drawdown_pct < -0.05

If none -> deterministic NO_INTENT (exit code 0)

Exposure:
- exposure_type: LONG_EQUITY
- underlying: SPY (symbol+currency)
- target_notional_pct: 0.05 (hard cap)

Hashing:
- canonical_json_hash = canonical_hash_for_c2_artifact_v1(obj)
- filename prefix = sha256(canonical_json_bytes_v1(obj)+b"\\n") (matches PhaseC/rollup conventions)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

MD_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1" / "snapshots").resolve()
# Updated for Bundle B: consume authoritative accounting_v2 NAV
NAV_ROOT = (TRUTH_ROOT / "accounting_v2" / "nav").resolve()
POS_ROOT = (TRUTH_ROOT / "positions_snapshot_v2" / "snapshots").resolve()
COR_ROOT = (TRUTH_ROOT / "monitoring_v1" / "engine_correlation_matrix").resolve()
REG_ROOT = (TRUTH_ROOT / "monitoring_v1" / "regime_snapshot_v2").resolve()

EXPOSURE_INTENT_SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v2.schema.json"

ENGINE_ID = "C2_DEFENSIVE_TAIL_V1"
ENGINE_SUITE = "C2_HYBRID_V1"
RISK_CLASS = "DEFENSIVE_OVERLAY"

UNDERLYING_SYMBOL = "SPY"
UNDERLYING_CCY = "USD"

THRESH_CORR = Decimal("0.70")
THRESH_DD = Decimal("-0.05")
TARGET_NOTIONAL_PCT = "0.05"
MAX_RISK_PCT = "0.05"  # conservative placeholder constraint (ENTRY requires > 0)

getcontext().prec = 28


class DefensiveTailError(Exception):
    pass


RC_NO_INTENT = "NO_INTENT"
RC_REGIME_NON_NORMAL = "REGIME_NON_NORMAL"
RC_CORR_HIGH = "CORRELATION_HIGH"
RC_DRAWDOWN_BREACH = "DRAWDOWN_BREACH"
RC_FORCE_ENTER_TEST_ONLY = "FORCE_ENTER_TEST_ONLY"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_bytes_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(data):
            # EXISTS_IDENTICAL_OK: rerunnable without rewriting immutable truth.
            return
        raise DefensiveTailError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise DefensiveTailError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
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
        except Exception:  # noqa: BLE001
            pass
        raise DefensiveTailError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise DefensiveTailError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise DefensiveTailError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _dec_str(x: Any, field: str) -> Decimal:
    try:
        return Decimal(str(x).strip())
    except (InvalidOperation, ValueError) as e:
        raise DefensiveTailError(f"DECIMAL_PARSE_FAILED: field={field} value={x!r}") from e


def _max_pairwise_corr(corr_obj: Dict[str, Any]) -> Decimal:
    cm = corr_obj.get("correlation_matrix", {})
    if not isinstance(cm, dict):
        raise DefensiveTailError("CORRELATION_MATRIX_NOT_OBJECT")

    m: Optional[Decimal] = None
    for _, row in cm.items():
        if not isinstance(row, dict):
            continue
        for _, v in row.items():
            d = _dec_str(v, "correlation_matrix.*")
            if m is None or d > m:
                m = d
    if m is None:
        return Decimal("0")
    return m


@dataclass(frozen=True)
class _Inputs:
    md_path: Path
    nav_path: Path
    pos_path: Path
    cor_path: Path
    reg_path: Path


def _resolve_inputs(day_utc: str, symbol: str) -> _Inputs:
    md_path = (MD_ROOT / day_utc / f"{symbol}.market_data_snapshot.v1.json").resolve()
    # Updated for Bundle B: v2 NAV artifact
    nav_path = (NAV_ROOT / day_utc / "nav.v2.json").resolve()
    pos_path = (POS_ROOT / day_utc / "positions_snapshot.v2.json").resolve()
    cor_path = (COR_ROOT / day_utc / "engine_correlation_matrix.v1.json").resolve()
    reg_path = (REG_ROOT / day_utc / "regime_snapshot.v2.json").resolve()

    missing = [p for p in [md_path, nav_path, pos_path, cor_path, reg_path] if not p.exists()]
    if missing:
        raise FileNotFoundError("MISSING_REQUIRED_INPUTS: " + ";".join([str(p) for p in missing]))

    return _Inputs(md_path=md_path, nav_path=nav_path, pos_path=pos_path, cor_path=cor_path, reg_path=reg_path)


def _build_intent_v2(
    *,
    day_utc: str,
    produced_utc: str,
    mode: str,
    reason_codes: List[str],
    inputs: _Inputs,
) -> Dict[str, Any]:
    # Deterministic intent_id derived from deterministic content spine: engine+day+target+reasons
    intent_id_seed = f"{ENGINE_ID}|{day_utc}|{TARGET_NOTIONAL_PCT}|" + ",".join(reason_codes)
    intent_id = _sha256_bytes(intent_id_seed.encode("utf-8"))

    inp = {
        "market_data_snapshot_v1": {"path": str(inputs.md_path), "sha256": _sha256_file(inputs.md_path)},
        "accounting_nav_snapshot": {"path": str(inputs.nav_path), "sha256": _sha256_file(inputs.nav_path)},
        "positions_snapshot_v2": {"path": str(inputs.pos_path), "sha256": _sha256_file(inputs.pos_path)},
        "engine_correlation_matrix_v1": {"path": str(inputs.cor_path), "sha256": _sha256_file(inputs.cor_path)},
        "regime_snapshot_v2": {"path": str(inputs.reg_path), "sha256": _sha256_file(inputs.reg_path)},
    }

    manifest_inputs = [
        inp["market_data_snapshot_v1"],
        inp["accounting_nav_snapshot"],
        inp["positions_snapshot_v2"],
        inp["engine_correlation_matrix_v1"],
        inp["regime_snapshot_v2"],
    ]

    obj: Dict[str, Any] = {
        "schema_id": "exposure_intent",
        "schema_version": "v2",
        "intent_id": intent_id,
        "created_at_utc": produced_utc,
        "produced_utc": produced_utc,
        "producer": "run_defensive_tail_intents_day_v1",
        "engine": {"engine_id": ENGINE_ID, "suite": ENGINE_SUITE, "mode": mode},
        "underlying": {"symbol": UNDERLYING_SYMBOL, "currency": UNDERLYING_CCY},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": TARGET_NOTIONAL_PCT,
        "expected_holding_days": 1,
        "risk_class": RISK_CLASS,
        "constraints": {"max_risk_pct": MAX_RISK_PCT},
        "reason_codes": list(reason_codes),
        "inputs": inp,
        "manifest": {"inputs": manifest_inputs, "code": []},
        "canonical_json_hash": None,
    }
    obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(obj)
    return obj


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_defensive_tail_intents_day_v1",
        description="Engine 5 Defensive Tail: emit ExposureIntent v2 (deterministic, fail-closed).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"], help="Engine mode")
    ap.add_argument("--symbol", default=UNDERLYING_SYMBOL, help="Underlying symbol (default SPY)")
    ap.add_argument("--force_enter_test_only", action="store_true", help="TEST ONLY: force emitting entry intent")
    args = ap.parse_args(argv)

    day_utc = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()
    symbol = str(args.symbol).strip().upper()
    produced_utc = f"{day_utc}T00:00:00Z"

    # Resolve inputs (fail-closed if missing)
    inputs = _resolve_inputs(day_utc, symbol)

    reason_codes: List[str] = []
    if bool(args.force_enter_test_only):
        reason_codes = [RC_FORCE_ENTER_TEST_ONLY]
    else:
        nav = _read_json_obj(inputs.nav_path)
        dd = _dec_str(nav.get("history", {}).get("drawdown_pct"), "history.drawdown_pct")

        cor = _read_json_obj(inputs.cor_path)
        mx = _max_pairwise_corr(cor)

        reg = _read_json_obj(inputs.reg_path)
        regime = str(reg.get("regime") or reg.get("regime_label") or "").strip()

        if regime != "NORMAL":
            reason_codes.append(RC_REGIME_NON_NORMAL)
        if mx > THRESH_CORR:
            reason_codes.append(RC_CORR_HIGH)
        if dd < THRESH_DD:
            reason_codes.append(RC_DRAWDOWN_BREACH)

    if not reason_codes:
        print(
            "OK: DEF_TAIL_NO_INTENT "
            + json.dumps(
                {"day_utc": day_utc, "engine_id": ENGINE_ID, "status": RC_NO_INTENT},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0

    intent_obj = _build_intent_v2(day_utc=day_utc, produced_utc=produced_utc, mode=mode, reason_codes=reason_codes, inputs=inputs)

    # Governed schema validation (v2 schema)
    validate_against_repo_schema_v1(intent_obj, REPO_ROOT, EXPOSURE_INTENT_SCHEMA_RELPATH)

    try:
        payload = canonical_json_bytes_v1(intent_obj) + b"\n"
    except CanonicalizationError as e:
        raise DefensiveTailError(f"CANONICALIZATION_FAILED: {e}") from e

    intent_hash = _sha256_bytes(payload)

    out_day_dir = (INTENTS_ROOT / day_utc).resolve()
    if not out_day_dir.exists():
        out_day_dir.mkdir(parents=True, exist_ok=False)
    if not out_day_dir.is_dir():
        raise DefensiveTailError(f"INTENTS_DAY_DIR_NOT_DIR: {str(out_day_dir)}")

    out_path = out_day_dir / f"{intent_hash}.exposure_intent.v2.json"
    _atomic_write_bytes_refuse_overwrite(out_path, payload)

    print(
        "OK: DEF_TAIL_INTENT_WRITTEN "
        + json.dumps(
            {
                "day_utc": day_utc,
                "symbol": symbol,
                "intent_hash": intent_hash,
                "out_path": str(out_path),
                "reason_codes": reason_codes,
                "engine_id": ENGINE_ID,
                "suite": ENGINE_SUITE,
                "mode": mode,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {e}", file=sys.stderr)
        raise
