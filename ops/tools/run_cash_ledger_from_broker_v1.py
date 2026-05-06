#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"
MODULE = "ops/tools/run_cash_ledger_from_broker_v1.py"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: BROKER_SUPPLY_MISSING:{path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"FAIL: BROKER_SUPPLY_JSON_INVALID:{path}:{type(exc).__name__}:{exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"FAIL: BROKER_SUPPLY_NOT_OBJECT:{path}")
    return payload


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))


def _existing_valid(path: Path, *, day_utc: str) -> bool:
    if not path.exists():
        return False
    payload = _read_json(path)
    if str(payload.get("schema_id") or "") != "C2_CASH_LEDGER_SNAPSHOT_V1":
        raise SystemExit(f"FAIL: EXISTING_CASH_LEDGER_SCHEMA_INVALID:{path}")
    if int(payload.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL: EXISTING_CASH_LEDGER_SCHEMA_VERSION_INVALID:{path}")
    if str(payload.get("day_utc") or "") != day_utc:
        raise SystemExit(f"FAIL: EXISTING_CASH_LEDGER_DAY_MISMATCH:{path}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    return True


def _require_cents(values: dict[str, Any], key: str) -> int:
    raw = values.get(key)
    if not isinstance(raw, int):
        raise SystemExit(f"FAIL: BROKER_SUPPLY_ACCOUNT_VALUE_INVALID:{key}")
    return raw


def _optional_cents(values: dict[str, Any], key: str) -> int | None:
    raw = values.get(key)
    if raw is None:
        return None
    if not isinstance(raw, int):
        raise SystemExit(f"FAIL: BROKER_SUPPLY_ACCOUNT_VALUE_INVALID:{key}")
    return raw


def build_cash_ledger_from_broker_v1(
    *,
    day_utc: str,
    truth_root: Path,
    account: str,
    environment: str,
    producer_git_sha: str,
    producer_repo: str,
) -> tuple[Path, dict[str, Any]]:
    if environment != "PAPER":
        raise SystemExit(f"FAIL: UNSUPPORTED_ENVIRONMENT:{environment}")
    if not truth_root.is_absolute() or not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: TRUTH_ROOT_INVALID:{truth_root}")
    if len(producer_git_sha) < 7 or len(producer_git_sha) > 40:
        raise SystemExit("FAIL: PRODUCER_GIT_SHA_INVALID")

    broker_path = truth_root / "reports" / "broker_supply_v1" / day_utc / "broker_supply.v1.json"
    out_path = truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"

    if _existing_valid(out_path, day_utc=day_utc):
        print(f"OK: CASH_LEDGER_FROM_BROKER_EXISTS day_utc={day_utc} path={out_path} sha256={_sha256_file(out_path)}")
        return out_path, _read_json(out_path)

    broker = _read_json(broker_path)
    if str(broker.get("schema_id") or "") != "broker_supply":
        raise SystemExit(f"FAIL: BROKER_SUPPLY_SCHEMA_INVALID:{broker_path}")
    if str(broker.get("schema_version") or "") != "broker_supply.v1":
        raise SystemExit(f"FAIL: BROKER_SUPPLY_SCHEMA_VERSION_INVALID:{broker_path}")
    if str(broker.get("day_utc") or "") != day_utc:
        raise SystemExit(f"FAIL: BROKER_SUPPLY_DAY_MISMATCH:{broker_path}")
    if str(broker.get("environment") or "").strip().upper() != "PAPER":
        raise SystemExit(f"FAIL: BROKER_SUPPLY_ENVIRONMENT_INVALID:{broker_path}")
    if str(broker.get("status") or "").strip().upper() != "PASS":
        blocker = str(broker.get("canonical_blocker") or "BROKER_SUPPLY_NOT_PASS").strip()
        raise SystemExit(f"FAIL: BROKER_SUPPLY_NOT_PASS:{blocker}")

    account_identity = broker.get("account_identity") if isinstance(broker.get("account_identity"), dict) else {}
    expected = str(account_identity.get("expected_account") or account).strip()
    if expected and expected != account:
        raise SystemExit(f"FAIL: BROKER_SUPPLY_ACCOUNT_EXPECTATION_MISMATCH:expected={expected}:account={account}")
    if account_identity and account_identity.get("match") is not True:
        raise SystemExit("FAIL: BROKER_SUPPLY_ACCOUNT_IDENTITY_NOT_MATCH")

    account_values = broker.get("account_values") if isinstance(broker.get("account_values"), dict) else {}
    currency = str(account_values.get("currency") or "USD").strip() or "USD"
    cash_total_cents = _require_cents(account_values, "total_cash_value_cents")
    nlv_total_cents = _require_cents(account_values, "net_liquidation_cents")
    available_funds_cents = _optional_cents(account_values, "available_funds_cents")
    excess_liquidity_cents = _optional_cents(account_values, "excess_liquidity_cents")

    produced_utc = str(broker.get("generated_at_utc") or "").strip() or f"{day_utc}T00:00:00Z"
    notes = ["BROKER_BACKED_CASH_LEDGER"]
    carry_forward_reason = str(broker.get("carry_forward_reason") or "").strip()
    if carry_forward_reason:
        notes.append(carry_forward_reason)

    payload: dict[str, Any] = {
        "schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "authority_basis": "broker_account_values",
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_git_sha,
            "module": MODULE,
        },
        "status": "OK",
        "reason_codes": ["BROKER_BACKED_CASH_LEDGER"],
        "input_manifest": [
            {
                "type": "broker_account_values",
                "path": str(broker_path),
                "sha256": _sha256_file(broker_path),
                "day_utc": day_utc,
                "producer": "ops/tools/run_broker_supply_v1.py",
            }
        ],
        "snapshot": {
            "observed_at_utc": produced_utc,
            "currency": currency,
            "cash_total_cents": cash_total_cents,
            "nlv_total_cents": nlv_total_cents,
            "available_funds_cents": available_funds_cents,
            "excess_liquidity_cents": excess_liquidity_cents,
            "account_id": account,
            "notes": notes,
        },
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    _atomic_write(out_path, _canonical_bytes(payload))
    print(f"OK: CASH_LEDGER_FROM_BROKER_WRITTEN day_utc={day_utc} path={out_path} sha256={_sha256_file(out_path)}")
    return out_path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_cash_ledger_from_broker_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--environment", required=True, choices=["PAPER"])
    parser.add_argument("--producer_git_sha", required=True)
    parser.add_argument("--producer_repo", default="constellation_2_runtime")
    args = parser.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    if len(day_utc) != 10 or day_utc[4] != "-" or day_utc[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC:{day_utc!r}")
    build_cash_ledger_from_broker_v1(
        day_utc=day_utc,
        truth_root=Path(str(args.truth_root)).expanduser().resolve(),
        account=str(args.account).strip(),
        environment=str(args.environment).strip().upper(),
        producer_git_sha=str(args.producer_git_sha).strip(),
        producer_repo=str(args.producer_repo).strip() or "constellation_2_runtime",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
