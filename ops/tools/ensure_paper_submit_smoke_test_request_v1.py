#!/usr/bin/env python3
"""
ensure_paper_submit_smoke_test_request_v1.py

Ensure a governed PAPER-only smoke-test request input exists for a given DAY_UTC.

Target path:
  <OPERATOR_INPUT_ROOT>/operator_inputs/paper_submit_smoke_test_v1/<DAY>/paper_submit_smoke_test_request.v1.json

Fail-closed:
- Policy file required and validated.
- PAPER only.
- No overwrite.
- Day-integrity enforced.
- Deterministic JSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_path_alignment_v1 import (
    normalize_paper_submit_smoke_test_request_nonce,
    resolve_paper_submit_smoke_test_request_path,
)

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_PAPER_SUBMIT_SMOKE_TEST_POLICY_V1.json").resolve()


def _require_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --operator_input_root: {p}")
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
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"FAIL: REQUIRED_STRING_MISSING: {key}")
    return value.strip()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_policy() -> Dict[str, Any]:
    if not POLICY_PATH.exists() or not POLICY_PATH.is_file():
        raise SystemExit(f"FAIL: POLICY_PATH_MISSING: {POLICY_PATH}")
    policy = _read_json_obj(POLICY_PATH)
    if _require_str(policy, "schema_id") != "C2_PAPER_SUBMIT_SMOKE_TEST_POLICY_V1":
        raise SystemExit(f"FAIL: POLICY_SCHEMA_ID_INVALID: {POLICY_PATH}")
    scope = policy.get("scope")
    rules = policy.get("smoke_test_rules")
    if not isinstance(scope, dict) or not isinstance(rules, dict):
        raise SystemExit(f"FAIL: POLICY_STRUCTURE_INVALID: {POLICY_PATH}")
    if _require_str(scope, "environment") != "PAPER":
        raise SystemExit("FAIL: POLICY_ENVIRONMENT_INVALID")
    if rules.get("test_only") is not True:
        raise SystemExit("FAIL: POLICY_TEST_ONLY_REQUIRED")
    return policy


def _validate_existing(day_utc: str, ib_account: str, request_nonce: str, path: Path) -> None:
    obj = _read_json_obj(path)
    if _require_str(obj, "day_utc") != day_utc:
        raise SystemExit(f"FAIL: SMOKE_REQUEST_DAY_MISMATCH: day_utc={day_utc} path={path}")
    if _require_str(obj, "environment") != "PAPER":
        raise SystemExit(f"FAIL: SMOKE_REQUEST_ENVIRONMENT_INVALID: path={path}")
    if _require_str(obj, "ib_account") != ib_account:
        raise SystemExit(f"FAIL: SMOKE_REQUEST_ACCOUNT_MISMATCH: ib_account={ib_account} path={path}")
    existing_nonce = str(obj.get("request_nonce") or "").strip()
    if existing_nonce != request_nonce:
        raise SystemExit(
            "FAIL: SMOKE_REQUEST_NONCE_MISMATCH: "
            f"expected_request_nonce={request_nonce!r} existing_request_nonce={existing_nonce!r} path={path}"
        )


def _build_request_payload(
    *,
    day_utc: str,
    ib_account: str,
    request_nonce: str,
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    scope = policy["scope"]
    rules = policy["smoke_test_rules"]
    instrument = rules.get("instrument")
    order_terms = rules.get("order_terms")
    if not isinstance(instrument, dict) or not isinstance(order_terms, dict):
        raise SystemExit("FAIL: POLICY_ORDER_INPUTS_INVALID")
    quantity_shares = int(rules.get("quantity_shares") or 0)
    if quantity_shares <= 0:
        raise SystemExit("FAIL: POLICY_QUANTITY_INVALID")
    request_identity_obj = {
        "day_utc": day_utc,
        "environment": "PAPER",
        "ib_account": ib_account,
        "engine_id": _require_str(scope, "engine_id"),
        "symbol": _require_str(instrument, "symbol"),
        "quantity_shares": quantity_shares,
    }
    if request_nonce:
        request_identity_obj["request_nonce"] = request_nonce
    request_id = hashlib.sha256(
        json.dumps(
            request_identity_obj,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    notes_prefix = _require_str(rules, "request_notes_prefix")
    payload = {
        "schema_id": "paper_submit_smoke_test_request",
        "schema_version": "v1",
        "request_id": request_id,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "environment": "PAPER",
        "ib_account": ib_account,
        "sleeve_id": _require_str(scope, "sleeve_id"),
        "engine_id": _require_str(scope, "engine_id"),
        "instrument": {
            "kind": _require_str(instrument, "kind"),
            "symbol": _require_str(instrument, "symbol"),
            "currency": _require_str(instrument, "currency"),
            "ib_conId": int(instrument.get("ib_conId") or 0),
            "ib_localSymbol": _require_str(instrument, "ib_localSymbol"),
        },
        "side": _require_str(rules, "side"),
        "quantity_shares": quantity_shares,
        "order_terms": dict(order_terms),
        "test_only": True,
        "policy_ref": {
            "path": str(POLICY_PATH),
            "sha256": _sha256_file(POLICY_PATH),
            "policy_id": _require_str(policy, "policy_id"),
        },
        "notes": [
            f"{notes_prefix}: explicit paper-only broker submit smoke request",
            "TEST_ONLY",
            "PAPER_ONLY",
        ],
    }
    if request_nonce:
        payload["request_nonce"] = request_nonce
        payload["notes"].append(f"REQUEST_NONCE={request_nonce}")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(prog="ensure_paper_submit_smoke_test_request_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--operator_input_root", required=True, help="Operator input root")
    ap.add_argument("--ib_account", required=True, help="IB account id (DU*)")
    ap.add_argument("--request_nonce", default="", help="Optional same-day smoke request nonce")
    ap.add_argument("--allow_create", required=True, choices=["YES", "NO"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    operator_input_root = _require_root(args.operator_input_root)
    ib_account = str(args.ib_account).strip()
    if not ib_account:
        raise SystemExit("FAIL: bad --ib_account (empty)")
    if not ib_account.startswith("DU"):
        raise SystemExit(f"FAIL: bad --ib_account (expected DU*): {ib_account}")
    try:
        request_nonce = normalize_paper_submit_smoke_test_request_nonce(request_nonce=args.request_nonce)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc
    policy = _load_policy()

    out_path = resolve_paper_submit_smoke_test_request_path(
        operator_input_root=operator_input_root,
        day_utc=day,
        request_nonce=request_nonce,
    )
    if out_path.exists():
        _validate_existing(day, ib_account, request_nonce, out_path)
        print(f"OK: PAPER_SUBMIT_SMOKE_TEST_REQUEST_EXISTS day_utc={day} path={out_path}")
        return 0

    if str(args.allow_create).strip().upper() != "YES":
        raise SystemExit(f"FAIL: PAPER_SUBMIT_SMOKE_TEST_REQUEST_MISSING day_utc={day} path={out_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        _validate_existing(day, ib_account, request_nonce, out_path)
        print(f"OK: PAPER_SUBMIT_SMOKE_TEST_REQUEST_EXISTS day_utc={day} path={out_path}")
        return 0

    payload = _build_request_payload(
        day_utc=day,
        ib_account=ib_account,
        request_nonce=request_nonce,
        policy=policy,
    )
    out_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    _validate_existing(day, ib_account, request_nonce, out_path)
    print(f"OK: PAPER_SUBMIT_SMOKE_TEST_REQUEST_WRITTEN day_utc={day} path={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
