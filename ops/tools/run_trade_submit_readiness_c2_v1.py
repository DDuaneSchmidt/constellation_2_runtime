#!/usr/bin/env python3
"""
run_trade_submit_readiness_c2_v1.py

C2-native trade submit readiness writer (v1).

Writes:
  - constellation_2/runtime/truth/trade_submit_readiness_c2_v1/status.json
  - constellation_2/runtime/truth/trade_submit_readiness_c2_v1/latest_pointer.v1.json

Deterministic, fail-closed:
- Uses day-anchored timestamps (no wall clock).
- Consumes C2 ib_api_handshake spine (latest_pointer -> day artifact).
- Binds provenance to C2 canonical truth_root and registry sha.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
OUT_DIR = (TRUTH_ROOT / "trade_submit_readiness_c2_v1").resolve()

SCHEMA_STATUS = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.latest_pointer.v1.schema.json"
REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _read_json(p: Path) -> Any:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: missing_required_file: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(str(tmp), str(path))


def _git_sha() -> str:
    try:
        import subprocess

        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _day_anchor_ts(day_utc: str) -> Tuple[str, str]:
    as_of = f"{day_utc}T00:00:00Z"
    expires = f"{day_utc}T00:02:00Z"
    return as_of, expires


def _load_registry_account(ib_account: str) -> Optional[Dict[str, Any]]:
    reg = _read_json(REGISTRY_PATH)
    if not isinstance(reg, dict):
        raise SystemExit("FAIL: registry_not_object")
    accounts = reg.get("accounts")
    if not isinstance(accounts, list):
        raise SystemExit("FAIL: registry_accounts_not_list")
    for a in accounts:
        if isinstance(a, dict) and str(a.get("account_id") or "").strip() == ib_account:
            return a
    return None


def _handshake_paths() -> Tuple[Path, Optional[Path]]:
    ptr = (TRUTH_ROOT / "ib_api_handshake" / "latest_pointer.v1.json").resolve()
    if not ptr.exists():
        return ptr, None
    try:
        o = _read_json(ptr)
        day = str(o.get("day_utc") or "").strip()
        pointers = o.get("pointers")
        if isinstance(pointers, dict):
            pth = str(pointers.get("path") or pointers.get("artifact_path") or "").strip()
            if pth:
                return ptr, Path(pth).resolve()
        if day:
            return ptr, (TRUTH_ROOT / "ib_api_handshake" / day / "ib_api_handshake.v1.json").resolve()
    except Exception:
        pass
    return ptr, None


def _handshake_ok(handshake_obj: Any) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    if not isinstance(handshake_obj, dict):
        return False, ["HANDSHAKE_NOT_OBJECT"]

    for key in ["ok", "connected", "ready"]:
        if handshake_obj.get(key) is True:
            reasons.append(f"HANDSHAKE_{key.upper()}_TRUE")
            return True, reasons

    status = str(handshake_obj.get("status") or handshake_obj.get("state") or "").strip().upper()
    if status in {"OK", "PASS", "READY", "CONNECTED"}:
        reasons.append(f"HANDSHAKE_STATUS_{status}")
        return True, reasons

    reasons.append("HANDSHAKE_NOT_OK")
    if status:
        reasons.append(f"HANDSHAKE_STATUS={status}")
    return False, reasons


def _validate_against_repo_schema(obj: Dict[str, Any], rel_schema_path: str) -> None:
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

    validate_against_repo_schema_v1(obj, REPO_ROOT, rel_schema_path)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_trade_submit_readiness_c2_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ib_account", required=True, help="IB account id")
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    ib_account = str(args.ib_account).strip()
    env = str(args.environment).strip().upper()

    if not TRUTH_ROOT.exists() or not TRUTH_ROOT.is_dir():
        raise SystemExit(f"FAIL: truth_root_missing: {TRUTH_ROOT}")

    if not REGISTRY_PATH.exists():
        raise SystemExit(f"FAIL: missing_registry: {REGISTRY_PATH}")

    reg_sha = _sha256_file(REGISTRY_PATH)
    acct = _load_registry_account(ib_account)
    reasons: List[str] = []

    if acct is None:
        ok_registry = False
        reasons.append("FAIL:IB_ACCOUNT_NOT_IN_REGISTRY")
    else:
        enabled = bool(acct.get("enabled_for_submission") is True)
        acct_env = str(acct.get("environment") or "").strip().upper()
        if not enabled:
            reasons.append("FAIL:IB_ACCOUNT_DISABLED_FOR_SUBMISSION")
        if acct_env != env:
            reasons.append(f"FAIL:IB_ACCOUNT_ENV_MISMATCH registry={acct_env} requested={env}")
        if env == "PAPER" and not ib_account.startswith("DU"):
            reasons.append("FAIL:PAPER_ACCOUNT_ID_NOT_DU")
        ok_registry = (enabled is True) and (acct_env == env) and ((env != "PAPER") or ib_account.startswith("DU"))

    ptr_path, hs_path = _handshake_paths()
    input_manifest: List[Dict[str, Any]] = []

    if ptr_path.exists():
        input_manifest.append({"type": "ib_api_handshake_latest_pointer_v1", "path": str(ptr_path), "sha256": _sha256_file(ptr_path)})
    else:
        input_manifest.append({"type": "ib_api_handshake_latest_pointer_v1_missing", "path": str(ptr_path), "sha256": None})
        reasons.append("FAIL:IB_API_HANDSHAKE_POINTER_MISSING")

    ok_handshake = False
    if hs_path is None:
        reasons.append("FAIL:IB_API_HANDSHAKE_POINTER_UNREADABLE")
    else:
        if hs_path.exists():
            input_manifest.append({"type": "ib_api_handshake_v1", "path": str(hs_path), "sha256": _sha256_file(hs_path)})
            hs_obj = _read_json(hs_path)
            ok_handshake, hs_reasons = _handshake_ok(hs_obj)
            reasons.extend(hs_reasons)
            if not ok_handshake:
                reasons.append("FAIL:IB_API_HANDSHAKE_NOT_OK")
        else:
            input_manifest.append({"type": "ib_api_handshake_v1_missing", "path": str(hs_path), "sha256": None})
            reasons.append("FAIL:IB_API_HANDSHAKE_ARTIFACT_MISSING")

    ok = bool(ok_registry and ok_handshake)
    state = "OK" if ok else "FAIL"

    as_of_utc, expires_utc = _day_anchor_ts(day)

    status_obj: Dict[str, Any] = {
        "schema_id": "trade_submit_readiness_c2",
        "schema_version": "v1",
        "as_of_utc": as_of_utc,
        "expires_utc": expires_utc,
        "ok": ok,
        "state": state,
        "environment": env,
        "ib_account": ib_account,
        "reasons": reasons,
        "input_manifest": input_manifest,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_trade_submit_readiness_c2_v1.py", "git_sha": _git_sha()},
        "provenance": {"truth_root": str(TRUTH_ROOT), "registry_sha256": reg_sha},
    }

    _validate_against_repo_schema(status_obj, SCHEMA_STATUS)

    status_bytes = _canonical_json_bytes(status_obj)
    status_sha = _sha256_bytes(status_bytes)

    latest_obj: Dict[str, Any] = {
        "schema_id": "trade_submit_readiness_c2_latest_pointer",
        "schema_version": "v1",
        "as_of_utc": as_of_utc,
        "expires_utc": expires_utc,
        "ok": ok,
        "state": state,
        "target_path": "status.json",
        "target_sha256": status_sha,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_trade_submit_readiness_c2_v1.py", "git_sha": _git_sha()},
        "provenance": {"truth_root": str(TRUTH_ROOT)},
    }

    _validate_against_repo_schema(latest_obj, SCHEMA_LATEST)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_write(OUT_DIR / "status.json", status_bytes)
    _atomic_write(OUT_DIR / "latest_pointer.v1.json", _canonical_json_bytes(latest_obj))

    print(f"OK: TRADE_SUBMIT_READINESS_C2_V1 state={state} ok={ok} ib_account={ib_account} env={env} out_dir={OUT_DIR}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
