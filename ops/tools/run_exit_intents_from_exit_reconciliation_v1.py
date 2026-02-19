#!/usr/bin/env python3
"""
run_exit_intents_from_exit_reconciliation_v1.py

Purpose:
- Convert Exit Reconciliation obligations into explicit ExposureIntent v1 EXIT intents
  (target_notional_pct="0") written into intents_v1/snapshots/<DAY>/.

Why:
- Makes exits actionable in the existing PhaseC→OMS→Allocation chain.
- Deterministic, fail-closed, hostile-review safe.

Inputs:
- constellation_2/runtime/truth/exit_reconciliation_v1/<DAY>/exit_reconciliation.v1.json

Outputs (immutable-by-bytes):
- constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/<sha256>.exposure_intent.v1.json

Fail-closed:
- If exit_reconciliation missing => FAIL.
- If any obligation lacks required underlying symbol => FAIL.
- If an output file exists with different bytes => FAIL (refuse overwrite).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

EXIT_RECON_ROOT = (TRUTH / "exit_reconciliation_v1").resolve()
INTENTS_ROOT = (TRUTH / "intents_v1/snapshots").resolve()

SCHEMA_EXPOSURE_INTENT = "constellation_2/schemas/exposure_intent.v1.schema.json"

SUITE = "C2_HYBRID_V1"
MODE = "PAPER"


class ExitIntentError(Exception):
    pass


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ExitIntentError(f"MISSING_OR_NOT_FILE: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ExitIntentError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _refuse_overwrite_if_different(path: Path, payload: bytes) -> str:
    """
    Write payload iff:
      - file does not exist, OR
      - file exists and bytes identical
    Otherwise FAIL (refuse overwrite).
    Returns action string.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload):
            return "EXISTS_IDENTICAL"
        raise ExitIntentError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        raise ExitIntentError(f"TEMP_FILE_ALREADY_EXISTS: {tmp}")
    tmp.write_bytes(payload)
    import os
    os.replace(tmp, path)
    return "WRITTEN"


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ExitIntentError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _intent_id_for_exit(day: str, engine_id: str, position_id: str, underlying: str, exposure_type: str) -> str:
    """
    Deterministic intent_id (stable across reruns):
    sha256("EXIT|<day>|<engine>|<position>|<underlying>|<exposure_type>")[:64]
    """
    s = f"EXIT|{day}|{engine_id}|{position_id}|{underlying}|{exposure_type}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _build_exit_intent(*, day: str, engine_id: str, position_id: str, underlying: str, currency: str, exposure_type: str) -> Dict[str, Any]:
    created_at_utc = f"{day}T00:00:00Z"
    intent_id = _intent_id_for_exit(day, engine_id, position_id, underlying, exposure_type)

    # EXIT intent: target=0, constraints=null permitted by schema
    obj: Dict[str, Any] = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": created_at_utc,
        "engine": {"engine_id": engine_id, "suite": SUITE, "mode": MODE},
        "underlying": {"symbol": underlying, "currency": currency},
        "exposure_type": exposure_type,
        "target_notional_pct": "0",
        "expected_holding_days": 0,
        "risk_class": "EXIT_RECONCILIATION_V1",
        "constraints": None,
        "canonical_json_hash": None,
    }
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_exit_intents_from_exit_reconciliation_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    recon_path = (EXIT_RECON_ROOT / day / "exit_reconciliation.v1.json").resolve()
    recon = _read_json_obj(recon_path)

    status = str(recon.get("status") or "").strip()
    if status.startswith("FAIL"):
        raise ExitIntentError(f"EXIT_RECONCILIATION_STATUS_FAIL: {status}")

    obligations = recon.get("obligations")
    if not isinstance(obligations, list):
        raise ExitIntentError("EXIT_RECONCILIATION_OBLIGATIONS_NOT_LIST")

    out_dir = (INTENTS_ROOT / day).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    wrote = 0
    exists = 0

    # deterministic processing order
    def _key(o: Any) -> Tuple[str, str]:
        if isinstance(o, dict):
            return (str(o.get("engine_id") or ""), str(o.get("position_id") or ""))
        return ("", "")

    for ob in sorted(obligations, key=_key):
        if not isinstance(ob, dict):
            raise ExitIntentError("OBLIGATION_NOT_OBJECT")

        engine_id = str(ob.get("engine_id") or "").strip()
        position_id = str(ob.get("position_id") or "").strip()
        currency = str(ob.get("currency") or "").strip()
        exposure_type = str(ob.get("recommended_exposure_type") or "").strip()

        inst = ob.get("instrument")
        if not isinstance(inst, dict):
            raise ExitIntentError("OBLIGATION_INSTRUMENT_NOT_OBJECT")
        underlying = inst.get("underlying")

        if not engine_id or not position_id:
            raise ExitIntentError("OBLIGATION_ENGINE_OR_POSITION_ID_MISSING")
        if not isinstance(underlying, str) or not underlying.strip():
            raise ExitIntentError(f"OBLIGATION_MISSING_UNDERLYING_FAILCLOSED: engine_id={engine_id} position_id={position_id}")
        if not currency or len(currency) != 3:
            raise ExitIntentError(f"OBLIGATION_CURRENCY_INVALID: {currency!r}")

        intent_obj = _build_exit_intent(
            day=day,
            engine_id=engine_id,
            position_id=position_id,
            underlying=underlying.strip(),
            currency=currency,
            exposure_type=exposure_type if exposure_type else "LONG_EQUITY",
        )

        payload = _canonical_json_bytes(intent_obj)
        intent_hash = _sha256_bytes(payload)
        out_path = (out_dir / f"{intent_hash}.exposure_intent.v1.json").resolve()

        action = _refuse_overwrite_if_different(out_path, payload)
        if action == "WRITTEN":
            wrote += 1
        else:
            exists += 1

    print(f"OK: EXIT_INTENTS_WRITTEN day={day} obligations={len(obligations)} wrote={wrote} exists_identical={exists} recon_path={str(recon_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
