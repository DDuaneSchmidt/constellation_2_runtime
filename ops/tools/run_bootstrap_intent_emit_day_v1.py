#!/usr/bin/env python3
"""
run_bootstrap_intent_emit_day_v1.py

Infrastructure-only deterministic bootstrap intent emitter.
Writes exactly one ExposureIntent v1 into intents_v1/snapshots/<DAY>/.

Fail-closed:
- refuses overwrite
- schema validates at write time
- canonical JSON + sha256 intent_hash used for filename

This does NOT bypass Bundle A. It only creates an intent artifact for pipeline validation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Hard anchor (repo root)
REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# Ensure repo import path for canonicalization helpers
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import (  # noqa: E402
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402


INTENTS_ROOT = (TRUTH_ROOT / "intents_v1/snapshots").resolve()
SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v1.schema.json"


class BootstrapIntentError(Exception):
    pass


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _stable_json_dumps(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise BootstrapIntentError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _parse_decimal_pct(s: str, name: str) -> str:
    v = (s or "").strip()
    if not v:
        raise BootstrapIntentError(f"MISSING_REQUIRED: {name}")
    # Must match schema regex: ^(0(\.[0-9]+)?|1(\.0+)?)$
    # We implement a strict parser by attempting Decimal-like float conversion via string rules.
    # Reject leading "+" or "-" or exponent.
    if any(ch in v for ch in ["e", "E", "+", "-"]):
        raise BootstrapIntentError(f"INVALID_PCT_FORMAT: {name}={v!r}")
    # Allow "0", "0.xxx", "1", "1.0", "1.00"
    if v == "0" or v.startswith("0."):
        return v
    if v == "1":
        return v
    if v.startswith("1.") and set(v[2:]).issubset({"0"}) and len(v) >= 3:
        return v
    raise BootstrapIntentError(f"INVALID_PCT_FORMAT: {name}={v!r}")


def _atomic_write_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        raise BootstrapIntentError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise BootstrapIntentError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
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
        raise BootstrapIntentError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e!r}") from e


def _seed_hash(
    *,
    day_utc: str,
    engine_id: str,
    suite: str,
    mode: str,
    symbol: str,
    currency: str,
    exposure_type: str,
    target_notional_pct: str,
    expected_holding_days: int,
    risk_class: str,
    max_risk_pct: Optional[str],
) -> str:
    seed: Dict[str, Any] = {
        "day_utc": day_utc,
        "engine_id": engine_id,
        "suite": suite,
        "mode": mode,
        "symbol": symbol,
        "currency": currency,
        "exposure_type": exposure_type,
        "target_notional_pct": target_notional_pct,
        "expected_holding_days": int(expected_holding_days),
        "risk_class": risk_class,
        "max_risk_pct": max_risk_pct,
    }
    return _sha256_bytes(_stable_json_dumps(seed).encode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_bootstrap_intent_emit_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--engine_id", required=True, help="Engine id string (<=32 chars, schema enforced)")
    ap.add_argument("--suite", default="C2_HYBRID_V1", choices=["C2_HYBRID_V1"])
    ap.add_argument("--symbol", required=True, help="Underlying symbol (e.g., SPY)")
    ap.add_argument("--currency", default="USD", help="3-letter currency (default USD)")
    ap.add_argument("--exposure_type", required=True, choices=["LONG_EQUITY", "SHORT_VOL_DEFINED"])
    ap.add_argument("--target_notional_pct", required=True, help="Decimal string in [0,1]")
    ap.add_argument("--expected_holding_days", type=int, required=True, help="0..365")
    ap.add_argument("--risk_class", required=True, help="Risk class label (string)")
    ap.add_argument("--max_risk_pct", default="", help="Optional constraints.max_risk_pct decimal string in [0,1]")

    args = ap.parse_args()

    day_utc = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()
    engine_id = str(args.engine_id).strip()
    suite = str(args.suite).strip()
    symbol = str(args.symbol).strip().upper()
    currency = str(args.currency).strip().upper()
    exposure_type = str(args.exposure_type).strip().upper()
    target_notional_pct = _parse_decimal_pct(str(args.target_notional_pct), "target_notional_pct")

    expected_holding_days = int(args.expected_holding_days)
    if expected_holding_days < 0 or expected_holding_days > 365:
        raise BootstrapIntentError(f"EXPECTED_HOLDING_DAYS_OUT_OF_RANGE: {expected_holding_days}")

    risk_class = str(args.risk_class).strip()
    if not risk_class:
        raise BootstrapIntentError("RISK_CLASS_EMPTY")

    max_risk_pct_raw = str(args.max_risk_pct).strip()
    max_risk_pct: Optional[str] = None
    constraints: Optional[Dict[str, str]] = None
    if max_risk_pct_raw:
        max_risk_pct = _parse_decimal_pct(max_risk_pct_raw, "max_risk_pct")
        constraints = {"max_risk_pct": max_risk_pct}

    # Deterministic id
    seed = _seed_hash(
        day_utc=day_utc,
        engine_id=engine_id,
        suite=suite,
        mode=mode,
        symbol=symbol,
        currency=currency,
        exposure_type=exposure_type,
        target_notional_pct=target_notional_pct,
        expected_holding_days=expected_holding_days,
        risk_class=risk_class,
        max_risk_pct=max_risk_pct,
    )
    intent_id = f"c2_bootstrap_{seed}"

    created_at_utc = datetime(int(day_utc[0:4]), int(day_utc[5:7]), int(day_utc[8:10]), 0, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    intent: Dict[str, Any] = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": created_at_utc,
        "engine": {"engine_id": engine_id, "suite": suite, "mode": mode},
        "underlying": {"symbol": symbol, "currency": currency},
        "exposure_type": exposure_type,
        "target_notional_pct": target_notional_pct,
        "expected_holding_days": expected_holding_days,
        "risk_class": risk_class,
    }
    if constraints is not None:
        intent["constraints"] = constraints

    # canonical_json_hash convention
    intent["canonical_json_hash"] = None
    try:
        intent["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(intent)
    except Exception as e:  # noqa: BLE001
        raise BootstrapIntentError(f"CANONICAL_HASH_FAILED: {e!r}") from e

    # Schema validate
    validate_against_repo_schema_v1(intent, REPO_ROOT, SCHEMA_RELPATH)

    # Canonical bytes + deterministic intent_hash for filename
    try:
        payload = canonical_json_bytes_v1(intent) + b"\n"
    except CanonicalizationError as e:
        raise BootstrapIntentError(f"CANONICALIZATION_FAILED: {e}") from e

    intent_hash = _sha256_bytes(payload)

    out_dir = (INTENTS_ROOT / day_utc).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = (out_dir / f"{intent_hash}.exposure_intent.v1.json").resolve()

    _atomic_write_refuse_overwrite(out_path, payload)

    print(
        _stable_json_dumps(
            {
                "status": "OK_INTENT_WRITTEN",
                "day_utc": day_utc,
                "intent_id": intent_id,
                "intent_hash": intent_hash,
                "out_path": str(out_path),
                "engine_id": engine_id,
                "mode": mode,
                "symbol": symbol,
                "exposure_type": exposure_type,
                "target_notional_pct": target_notional_pct,
                "expected_holding_days": expected_holding_days,
                "risk_class": risk_class,
                "max_risk_pct": max_risk_pct,
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {e!r}", file=sys.stderr)
        raise
