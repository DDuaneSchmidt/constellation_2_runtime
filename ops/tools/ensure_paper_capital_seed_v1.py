#!/usr/bin/env python3
"""
ensure_paper_capital_seed_v1.py

Ensure a governed paper-capital seed input exists for a given DAY_UTC.

Target path:
  <TRUTH_ROOT>/operator_inputs/paper_capital_seed_v1/<DAY>/paper_capital_seed.v1.json

Fail-closed:
- Policy file required and validated.
- No overwrite.
- Day-integrity enforced.
- Deterministic JSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_path_alignment_v1 import resolve_paper_capital_seed_path

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_PAPER_CAPITAL_SEED_POLICY_V1.json").resolve()


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --truth_root: {p}")
    return p


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: cannot parse json: {path}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: json top-level not object: {path}")
    return obj


def _require_str(obj: Dict[str, Any], key: str) -> str:
    v = obj.get(key)
    if not isinstance(v, str) or not v.strip():
        raise SystemExit(f"FAIL: REQUIRED_STRING_MISSING: {key}")
    return v.strip()


def _require_decimal_string(raw: str, *, key: str) -> Decimal:
    s = str(raw).strip()
    if not s:
        raise SystemExit(f"FAIL: REQUIRED_DECIMAL_EMPTY: {key}")
    try:
        value = Decimal(s)
    except (InvalidOperation, ValueError) as e:
        raise SystemExit(f"FAIL: INVALID_DECIMAL: {key}={raw!r}") from e
    if value != value.quantize(Decimal("0.01")):
        raise SystemExit(f"FAIL: INVALID_DECIMAL_QUANTIZATION: {key}={raw!r}")
    return value


def _decimal_to_money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_policy() -> Dict[str, Any]:
    if not POLICY_PATH.exists() or not POLICY_PATH.is_file():
        raise SystemExit(f"FAIL: POLICY_PATH_MISSING: {POLICY_PATH}")
    policy = _read_json_obj(POLICY_PATH)
    if _require_str(policy, "schema_id") != "C2_PAPER_CAPITAL_SEED_POLICY_V1":
        raise SystemExit(f"FAIL: POLICY_SCHEMA_ID_INVALID: {POLICY_PATH}")
    rules = policy.get("explicit_seed_rules")
    if not isinstance(rules, dict):
        raise SystemExit(f"FAIL: POLICY_RULES_MISSING: {POLICY_PATH}")
    return policy


def _validate_existing(day_utc: str, path: Path) -> None:
    obj = _read_json_obj(path)
    if _require_str(obj, "day_utc") != day_utc:
        raise SystemExit(f"FAIL: PAPER_CAPITAL_SEED_DAY_MISMATCH: day_utc={day_utc} path={path}")


def _build_seed_payload(*, day_utc: str, ib_account: str, seed_usd: Decimal, policy: Dict[str, Any]) -> Dict[str, Any]:
    rules = policy["explicit_seed_rules"]
    min_seed = _require_decimal_string(_require_str(rules, "min_seed_usd"), key="min_seed_usd")
    max_seed = _require_decimal_string(_require_str(rules, "max_seed_usd"), key="max_seed_usd")
    if seed_usd < min_seed:
        raise SystemExit(f"FAIL: SEED_USD_BELOW_MIN: seed_usd={_decimal_to_money(seed_usd)} min_seed_usd={_decimal_to_money(min_seed)}")
    if seed_usd > max_seed:
        raise SystemExit(f"FAIL: SEED_USD_ABOVE_MAX: seed_usd={_decimal_to_money(seed_usd)} max_seed_usd={_decimal_to_money(max_seed)}")
    if bool(rules.get("allow_negative")) and seed_usd < Decimal("0.00"):
        raise SystemExit("FAIL: NEGATIVE_SEED_NOT_SUPPORTED")
    if seed_usd < Decimal("0.00"):
        raise SystemExit("FAIL: NEGATIVE_SEED_NOT_ALLOWED")

    currency = _require_str(policy.get("scope") if isinstance(policy.get("scope"), dict) else {}, "currency")
    notes_prefix = _require_str(rules, "notes_prefix")
    seed_money = _decimal_to_money(seed_usd)
    return {
        "schema_id": "C2_PAPER_CAPITAL_SEED",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "environment": "PAPER",
        "ib_account": ib_account,
        "currency": currency,
        "seed_mode": _require_str(rules, "seed_mode"),
        "cash_total": seed_money,
        "nlv_total": seed_money,
        "policy_ref": {
            "path": str(POLICY_PATH),
            "sha256": _sha256_file(POLICY_PATH),
            "policy_id": _require_str(policy, "policy_id"),
        },
        "notes": [
            f"{notes_prefix}: governed paper capital seed",
            f"seed_usd={seed_money}",
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="ensure_paper_capital_seed_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--truth_root", required=True, help="Operator input root")
    ap.add_argument("--ib_account", required=True, help="IB account id (DU*)")
    ap.add_argument("--seed_usd", required=True, help="Explicit paper seed in USD with 2 decimals")
    ap.add_argument("--allow_create", required=True, choices=["YES", "NO"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    truth_root = _require_truth_root(args.truth_root)
    ib = str(args.ib_account).strip()
    if not ib:
        raise SystemExit("FAIL: bad --ib_account (empty)")
    seed_usd = _require_decimal_string(args.seed_usd, key="seed_usd")
    policy = _load_policy()

    out_path = resolve_paper_capital_seed_path(operator_input_root=truth_root, day_utc=day)
    if out_path.exists():
        _validate_existing(day, out_path)
        print(f"OK: PAPER_CAPITAL_SEED_EXISTS day_utc={day} path={out_path}")
        return 0

    if str(args.allow_create) != "YES":
        raise SystemExit(f"FAIL: PAPER_CAPITAL_SEED_MISSING day_utc={day} path={out_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        _validate_existing(day, out_path)
        print(f"OK: PAPER_CAPITAL_SEED_EXISTS day_utc={day} path={out_path}")
        return 0

    payload = _build_seed_payload(day_utc=day, ib_account=ib, seed_usd=seed_usd, policy=policy)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    _validate_existing(day, out_path)
    print(f"OK: PAPER_CAPITAL_SEED_WRITTEN day_utc={day} path={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
