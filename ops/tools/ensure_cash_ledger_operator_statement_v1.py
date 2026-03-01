#!/usr/bin/env python3
"""
ensure_cash_ledger_operator_statement_v1.py

Ensure an operator cash-ledger bootstrap statement exists for a given DAY_UTC.

Modes:
  ZERO       -> cash_total = 0.00
  SEED_100K  -> cash_total = 100000.00

Target path:
  constellation_2/operator_inputs/cash_ledger_operator_statements/<DAY>/operator_statement.v1.json

Fail-closed:
- No overwrite.
- Day-integrity enforced.
- Deterministic JSON.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
OUT_ROOT = (REPO_ROOT / "constellation_2" / "operator_inputs" / "cash_ledger_operator_statements").resolve()


def _day_prefix(day_utc: str) -> str:
    return f"{day_utc}T"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: cannot parse json: {path}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: operator statement top-level not object: {path}")
    return obj


def _require_str(obj: Dict[str, Any], key: str) -> str:
    v = obj.get(key)
    if not isinstance(v, str) or not v.strip():
        raise SystemExit(f"FAIL: REQUIRED_STRING_MISSING: {key}")
    return v.strip()


def _validate_existing(day_utc: str, path: Path) -> None:
    obj = _read_json_obj(path)
    obs = _require_str(obj, "observed_at_utc")
    if not obs.startswith(_day_prefix(day_utc)):
        raise SystemExit(f"FAIL: OPERATOR_STATEMENT_DAY_MISMATCH: day_utc={day_utc} observed_at_utc={obs}")


def _build_zero(day_utc: str, ib_account: str) -> Dict[str, Any]:
    return {
        "observed_at_utc": f"{day_utc}T00:00:00Z",
        "currency": "USD",
        "cash_total": "0.00",
        "nlv_total": "0.00",
        "available_funds": None,
        "excess_liquidity": None,
        "account_id": ib_account,
        "notes": [
            "BOOTSTRAP_OPERATOR_STATEMENT_V1: values set to 0.00 pending IB account snapshot capture",
            "SAFE_IDLE bootstrap for paper day orchestration",
        ],
    }


def _build_seed_100k(day_utc: str, ib_account: str) -> Dict[str, Any]:
    return {
        "observed_at_utc": f"{day_utc}T00:00:00Z",
        "currency": "USD",
        "cash_total": "100000.00",
        "nlv_total": "100000.00",
        "available_funds": None,
        "excess_liquidity": None,
        "account_id": ib_account,
        "notes": [
            "CAPITAL_SEED_V1: deterministic 100k USD initial funding for paper bootstrap",
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="ensure_cash_ledger_operator_statement_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ib_account", required=True, help="IB account id (DU*)")
    ap.add_argument("--mode", required=True, choices=["ZERO", "SEED_100K"])
    ap.add_argument("--allow_create", required=True, choices=["YES", "NO"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    ib = str(args.ib_account).strip()
    if not ib:
        raise SystemExit("FAIL: bad --ib_account (empty)")

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_statement.v1.json").resolve()

    if out_path.exists():
        _validate_existing(day, out_path)
        print(f"OK: OPERATOR_STATEMENT_EXISTS day_utc={day} path={out_path}")
        return 0

    if str(args.allow_create) != "YES":
        raise SystemExit(f"FAIL: OPERATOR_STATEMENT_MISSING day_utc={day} path={out_path}")

    out_dir.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        _validate_existing(day, out_path)
        print(f"OK: OPERATOR_STATEMENT_EXISTS day_utc={day} path={out_path}")
        return 0

    if args.mode == "ZERO":
        payload = _build_zero(day, ib)
    else:
        payload = _build_seed_100k(day, ib)

    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    out_path.write_text(text, encoding="utf-8")

    _validate_existing(day, out_path)

    print(f"OK: OPERATOR_STATEMENT_WRITTEN day_utc={day} path={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
