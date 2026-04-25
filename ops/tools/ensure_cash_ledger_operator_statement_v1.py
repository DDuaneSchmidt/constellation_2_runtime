#!/usr/bin/env python3
"""
ensure_cash_ledger_operator_statement_v1.py

Ensure an operator cash-ledger bootstrap statement exists for a given DAY_UTC.

Modes:
  ZERO       -> cash_total = 0.00
  SEED_100K  -> cash_total = 100000.00
  GOVERNED_SEED -> materialize from paper_capital_seed_v1 input

Target path:
  <TRUTH_ROOT>/operator_inputs/cash_ledger_operator_statements/<DAY>/operator_statement.v1.json

Fail-closed:
- No overwrite.
- Day-integrity enforced.
- Deterministic JSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_root,
    resolve_paper_capital_seed_path,
)

def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --truth_root: {p}")
    return p


def _operator_statement_root(truth_root: Path) -> Path:
    return resolve_operator_statement_root(operator_input_root=truth_root)


def _paper_capital_seed_path(truth_root: Path, day_utc: str) -> Path:
    return resolve_paper_capital_seed_path(operator_input_root=truth_root, day_utc=day_utc)


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


def _build_governed_seed(day_utc: str, truth_root: Path, ib_account: str) -> Dict[str, Any]:
    seed_path = _paper_capital_seed_path(truth_root, day_utc)
    if not seed_path.exists() or not seed_path.is_file():
        raise SystemExit(f"FAIL: GOVERNED_SEED_INPUT_MISSING: {seed_path}")

    seed = _read_json_obj(seed_path)
    seed_day = _require_str(seed, "day_utc")
    if seed_day != day_utc:
        raise SystemExit(f"FAIL: GOVERNED_SEED_DAY_MISMATCH: day_utc={day_utc} seed_day_utc={seed_day}")
    if _require_str(seed, "environment") != "PAPER":
        raise SystemExit("FAIL: GOVERNED_SEED_ENVIRONMENT_INVALID")
    currency = _require_str(seed, "currency")
    if currency != "USD":
        raise SystemExit(f"FAIL: GOVERNED_SEED_CURRENCY_INVALID: {currency}")
    cash_total = _require_str(seed, "cash_total")
    nlv_total = _require_str(seed, "nlv_total")
    if cash_total != nlv_total:
        raise SystemExit(f"FAIL: GOVERNED_SEED_CASH_NLV_MISMATCH: cash_total={cash_total} nlv_total={nlv_total}")
    notes = seed.get("notes")
    if not isinstance(notes, list) or not notes or any(not isinstance(item, str) or not item.strip() for item in notes):
        raise SystemExit("FAIL: GOVERNED_SEED_NOTES_INVALID")

    return {
        "observed_at_utc": f"{day_utc}T00:00:00Z",
        "currency": currency,
        "cash_total": cash_total,
        "nlv_total": nlv_total,
        "available_funds": None,
        "excess_liquidity": None,
        "account_id": ib_account,
        "notes": [str(item).strip() for item in notes],
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="ensure_cash_ledger_operator_statement_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--truth_root", required=True, help="Authoritative runtime truth root")
    ap.add_argument("--ib_account", required=True, help="IB account id (DU*)")
    ap.add_argument("--mode", required=True, choices=["ZERO", "SEED_100K", "GOVERNED_SEED"])
    ap.add_argument("--allow_create", required=True, choices=["YES", "NO"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    truth_root = _require_truth_root(args.truth_root)

    ib = str(args.ib_account).strip()
    if not ib:
        raise SystemExit("FAIL: bad --ib_account (empty)")

    out_dir = (_operator_statement_root(truth_root) / day).resolve()
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
    elif args.mode == "SEED_100K":
        payload = _build_seed_100k(day, ib)
    else:
        payload = _build_governed_seed(day, truth_root, ib)

    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    out_path.write_text(text, encoding="utf-8")

    _validate_existing(day, out_path)

    print(f"OK: OPERATOR_STATEMENT_WRITTEN day_utc={day} path={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
