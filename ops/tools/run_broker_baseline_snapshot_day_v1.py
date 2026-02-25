#!/usr/bin/env python3
"""
run_broker_baseline_snapshot_day_v1.py

Day-0 baseline anchor WITHOUT broker statement.

This v1 implementation is INTERNAL-truth based (no broker libs required):
- Uses governed internal snapshots as the baseline anchor.
- Intended for PAPER Day-0 readiness where broker statement is missing.

Writes immutable truth:
  constellation_2/runtime/truth/execution_evidence_v1/broker_baseline_snapshot_v1/<DAY>/broker_baseline_snapshot.v1.json

Inputs (governed truth spines):
  - positions_v1/snapshots/<DAY>/positions_snapshot.v2.json
  - cash_ledger_v1/snapshots/<DAY>/cash_ledger_snapshot.v1.json

Fail-closed:
- If required internal snapshots are missing/invalid, exits non-zero and writes nothing.

Rerun-safety:
- If the day-keyed artifact already exists AND matches (schema_id, schema_version, day_utc, environment, account_id),
  treat it as authoritative and exit 0 (action=EXISTS).
- Do NOT attempt rewrite (prevents immutable overwrite failures on reruns when produced_utc/git_sha differ).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
OUT_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "broker_baseline_snapshot_v1").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _utc_now_z() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _canon_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _immut_write(path: Path, content: bytes) -> str:
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) != _sha256_bytes(content):
            raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: ATTEMPTED_REWRITE: {path}")
        return "EXISTS_IDENTICAL"
    _atomic_write(path, content)
    return "WRITTEN"


def _parse_day(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _load_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: INPUT_FILE_MISSING: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: INPUT_JSON_PARSE_ERROR: {path} err={e!r}")
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: INPUT_TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _extract_positions_v2(pos_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    p = pos_obj.get("positions")
    if isinstance(p, list):
        return [x for x in p if isinstance(x, dict)]
    if isinstance(p, dict):
        items = p.get("items")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    raise SystemExit("FAIL: POSITIONS_SNAPSHOT_V2_MISSING_positions.items")


def _extract_cash_total_cents(cash_obj: Dict[str, Any]) -> int:
    snap = cash_obj.get("snapshot")
    if not isinstance(snap, dict):
        raise SystemExit("FAIL: CASH_LEDGER_SNAPSHOT_MISSING_snapshot")
    if "cash_total_cents" not in snap:
        raise SystemExit("FAIL: CASH_LEDGER_SNAPSHOT_MISSING_cash_total_cents")
    try:
        return int(snap["cash_total_cents"])
    except Exception:
        raise SystemExit("FAIL: CASH_LEDGER_SNAPSHOT_cash_total_cents_NOT_INT")


def _return_if_existing_ok(out_path: Path, *, day: str, env: str, acct: str) -> int | None:
    """
    Rerun-safety:
    - If file exists and matches identity fields, treat as authoritative and return 0.
    - If file exists but mismatches identity fields, fail-closed (returns SystemExit).
    - If file does not exist, return None (caller should compute/write).
    """
    if not out_path.exists():
        return None

    existing_sha = _sha256_bytes(out_path.read_bytes())
    existing = _load_json_obj(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = str(existing.get("schema_version") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()
    environment = str(existing.get("environment") or "").strip().upper()
    account_id = str(existing.get("account_id") or "").strip()

    if schema_id != "broker_baseline_snapshot":
        raise SystemExit(f"FAIL: EXISTING_BASELINE_SCHEMA_ID_MISMATCH: {schema_id!r} path={out_path}")
    if schema_version != "v1":
        raise SystemExit(f"FAIL: EXISTING_BASELINE_SCHEMA_VERSION_MISMATCH: {schema_version!r} path={out_path}")
    if day_utc != day:
        raise SystemExit(f"FAIL: EXISTING_BASELINE_DAY_MISMATCH: {day_utc!r} expected={day!r} path={out_path}")
    if environment != env:
        raise SystemExit(f"FAIL: EXISTING_BASELINE_ENV_MISMATCH: {environment!r} expected={env!r} path={out_path}")
    if account_id != acct:
        raise SystemExit(f"FAIL: EXISTING_BASELINE_ACCOUNT_MISMATCH: {account_id!r} expected={acct!r} path={out_path}")

    print(f"OK: BROKER_BASELINE_SNAPSHOT_V1_WRITTEN day_utc={day} env={env} account_id={acct} path={out_path} sha256={existing_sha} action=EXISTS")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_broker_baseline_snapshot_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args()

    day = _parse_day(args.day_utc)
    env = str(args.environment).strip().upper()
    acct = str(args.ib_account).strip()
    if acct == "":
        raise SystemExit("FAIL: IB_ACCOUNT_EMPTY")

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "broker_baseline_snapshot.v1.json").resolve()

    existing_rc = _return_if_existing_ok(out_path, day=day, env=env, acct=acct)
    if existing_rc is not None:
        return int(existing_rc)

    pos_path = (TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json").resolve()
    cash_path = (TRUTH_ROOT / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json").resolve()

    pos_obj = _load_json_obj(pos_path)
    cash_obj = _load_json_obj(cash_path)

    positions = _extract_positions_v2(pos_obj)
    cash_total_cents = _extract_cash_total_cents(cash_obj)

    produced_utc = _utc_now_z()
    sha = _git_sha()

    payload: Dict[str, Any] = {
        "schema_id": "broker_baseline_snapshot",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "environment": env,
        "account_id": acct,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_broker_baseline_snapshot_day_v1.py", "git_sha": sha},
        "baseline_source": "INTERNAL_TRUTH",
        "inputs": {
            "positions_snapshot_path": str(pos_path.relative_to(TRUTH_ROOT)),
            "cash_ledger_snapshot_path": str(cash_path.relative_to(TRUTH_ROOT)),
        },
        "cash_total_cents": cash_total_cents,
        "positions_items": positions,
        "state_sha256": None,
    }

    payload["state_sha256"] = _sha256_bytes(_canon_bytes({k: v for k, v in payload.items() if k != "state_sha256"}))
    content = _canon_bytes(payload)

    action = _immut_write(out_path, content)
    digest = _sha256_bytes(content)

    print(f"OK: BROKER_BASELINE_SNAPSHOT_V1_WRITTEN day_utc={day} env={env} account_id={acct} path={out_path} sha256={digest} action={action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
