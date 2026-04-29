#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    collect_intent_files_v1,
    parse_day_utc_v1,
    read_json_object_v1,
    resolve_paper_intent_truth_root_v1,
)
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry


OPTIONS_CHAIN_SCHEMA = "constellation_2/schemas/options_chain_snapshot.v1.schema.json"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_truth_root() -> Path:
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    return resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id="PRIMARY",
    ).execution_root_path.resolve()


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _option_symbols_from_intents(*, truth_root: Path, day_utc: str) -> List[str]:
    symbols: List[str] = []
    for path in collect_intent_files_v1(truth_root=truth_root, day_utc=day_utc):
        try:
            payload = read_json_object_v1(path)
        except ValueError:
            continue
        option = payload.get("option")
        exposure_type = str(payload.get("exposure_type") or "").strip().upper()
        has_option_structure = isinstance(option, dict) or exposure_type in {"SHORT_VOL_DEFINED", "VOL_INCOME_DEFINED"}
        if not has_option_structure:
            continue
        underlying = payload.get("underlying")
        symbol = ""
        if isinstance(underlying, dict):
            symbol = str(underlying.get("symbol") or "").strip().upper()
        elif isinstance(underlying, str):
            symbol = underlying.strip().upper()
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _raw_exists_for_symbol(*, truth_root: Path, day_utc: str, symbol: str) -> bool:
    root = (truth_root / "options_chain_raw_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return False
    for capture_dir in sorted(root.iterdir()):
        raw_path = (capture_dir / "raw_chain.json").resolve()
        if not raw_path.exists() or not raw_path.is_file():
            continue
        try:
            raw_obj = read_json_object_v1(raw_path)
        except ValueError:
            continue
        underlying = raw_obj.get("underlying") if isinstance(raw_obj.get("underlying"), dict) else {}
        if str(underlying.get("symbol") or "").strip().upper() == symbol.upper():
            return True
    return False


def _snapshot_valid_for_symbol(*, truth_root: Path, day_utc: str, symbol: str, eval_time_utc: str) -> Dict[str, Any]:
    root = (truth_root / "options_chain_snapshot_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return {"ok": False, "reason_code": "OPTIONS_SNAPSHOT_ROOT_MISSING", "path": str(root)}
    failures: List[str] = []
    for capture_dir in sorted(root.iterdir()):
        snap_path = (capture_dir / "options_chain_snapshot.v1.json").resolve()
        cert_path = (capture_dir / "freshness_certificate.v1.json").resolve()
        if not snap_path.exists() or not snap_path.is_file():
            continue
        if not cert_path.exists() or not cert_path.is_file():
            failures.append(f"FRESHNESS_CERT_MISSING:{cert_path}")
            continue
        try:
            snap_obj = read_json_object_v1(snap_path)
            cert_obj = read_json_object_v1(cert_path)
            validate_against_repo_schema_v1(snap_obj, REPO_ROOT, OPTIONS_CHAIN_SCHEMA)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OPTIONS_SNAPSHOT_INVALID:{type(exc).__name__}:{snap_path}")
            continue
        underlying = snap_obj.get("underlying") if isinstance(snap_obj.get("underlying"), dict) else {}
        if str(underlying.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        if not str(snap_obj.get("as_of_utc") or "").startswith(day_utc):
            failures.append(f"OPTIONS_SNAPSHOT_STALE:{snap_path}")
            continue
        valid_until = _parse_iso(cert_obj.get("valid_until_utc"))
        eval_time = _parse_iso(eval_time_utc)
        if valid_until is None or eval_time is None or valid_until < eval_time:
            failures.append(f"OPTIONS_SNAPSHOT_FRESHNESS_EXPIRED:{snap_path}")
            continue
        contracts = snap_obj.get("contracts")
        if not isinstance(contracts, list) or not contracts:
            failures.append(f"OPTIONS_SNAPSHOT_CONTRACTS_EMPTY:{snap_path}")
            continue
        return {
            "ok": True,
            "reason_code": "",
            "path": str(snap_path),
            "freshness_certificate_path": str(cert_path),
        }
    return {
        "ok": False,
        "reason_code": "OPTIONS_SNAPSHOT_SYMBOL_MISSING",
        "path": str(root),
        "failures": failures,
    }


def _run_tool(cmd: List[str], *, truth_root: Path) -> Dict[str, Any]:
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(truth_root)
    timeout_s = int(str(os.environ.get("C2_OPTIONS_SNAPSHOT_STEP_TIMEOUT_SECONDS") or "60").strip())
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
            env=env,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": cmd,
            "return_code": 124,
            "stdout": str(exc.stdout or "").strip(),
            "stderr": f"OPTIONS_SNAPSHOT_CAPTURE_TIMEOUT:{timeout_s}s",
            "timed_out": True,
        }
    return {
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }


def _capture_symbol(*, truth_root: Path, day_utc: str, symbol: str, eval_time_utc: str) -> Dict[str, Any]:
    return _run_tool(
        [
            sys.executable,
            "ops/tools/run_options_chain_capture_ib_day_v1.py",
            "--day_utc",
            day_utc,
            "--eval_time_utc",
            eval_time_utc,
            "--symbol",
            symbol,
            "--truth_root",
            str(truth_root),
            "--ib_host",
            str(os.environ.get("C2_IB_HOST") or "127.0.0.1").strip(),
            "--ib_port",
            str(os.environ.get("C2_IB_PORT") or "4002").strip(),
            "--ib_client_id",
            str(os.environ.get("C2_IB_CLIENT_ID") or "7").strip(),
        ],
        truth_root=truth_root,
    )


def _promote_symbol(*, truth_root: Path, day_utc: str, symbol: str, eval_time_utc: str) -> Dict[str, Any]:
    return _run_tool(
        [
            sys.executable,
            "ops/tools/run_options_chain_truth_promotion_day_v1.py",
            "--day_utc",
            day_utc,
            "--eval_time_utc",
            eval_time_utc,
            "--symbol",
            symbol,
            "--truth_root",
            str(truth_root),
        ],
        truth_root=truth_root,
    )


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_options_chain_snapshot_required_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--symbol", action="append", default=[])
    ap.add_argument("--symbols_from_intents", choices=["YES", "NO"], default="NO")
    ap.add_argument("--capture_missing", choices=["YES", "NO"], default="YES")
    ap.add_argument("--eval_time_utc", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    eval_time_utc = str(args.eval_time_utc or "").strip() or _utc_now_iso()
    truth_root = Path(args.truth_root).expanduser().resolve() if str(args.truth_root or "").strip() else _default_truth_root()
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    symbols = [str(symbol or "").strip().upper() for symbol in args.symbol if str(symbol or "").strip()]
    if args.symbols_from_intents == "YES":
        symbols.extend(_option_symbols_from_intents(truth_root=intent_truth_root, day_utc=day_utc))
    required_symbols = sorted(set(symbols))

    steps: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    if not required_symbols:
        print(json.dumps({"status": "OK", "day_utc": day_utc, "required_symbols": [], "reason": "NO_ACTIVE_OPTION_INTENTS"}, sort_keys=True))
        return 0

    for symbol in required_symbols:
        before = _snapshot_valid_for_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol, eval_time_utc=eval_time_utc)
        if before["ok"]:
            results.append({"symbol": symbol, "status": "PASS", "path": before["path"]})
            continue
        capture_result: Dict[str, Any] | None = None
        if args.capture_missing == "YES":
            capture_result = _capture_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol, eval_time_utc=eval_time_utc)
            steps.append({"symbol": symbol, "producer": "options_chain_capture_ib_day_v1", **capture_result})
        if _raw_exists_for_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol):
            steps.append({"symbol": symbol, "producer": "options_chain_truth_promotion_day_v1", **_promote_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol, eval_time_utc=eval_time_utc)})
        after = _snapshot_valid_for_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol, eval_time_utc=eval_time_utc)
        reason_code = str(after.get("reason_code") or "")
        if not after["ok"] and capture_result is not None and int(capture_result.get("return_code") or 0) != 0:
            reason_code = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        results.append(
            {
                "symbol": symbol,
                "status": "PASS" if after["ok"] else "FAIL",
                "reason_code": reason_code,
                "path": str(after.get("path") or ""),
                "failures": list(after.get("failures") or []),
            }
        )

    failed = [row for row in results if row.get("status") != "PASS"]
    out = {
        "status": "OK" if not failed else "BLOCKED_VALID",
        "day_utc": day_utc,
        "required_symbols": required_symbols,
        "results": results,
        "steps": steps,
        "producer_command": (
            f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --symbols_from_intents YES"
        ),
        "eval_time_utc": eval_time_utc,
    }
    print(json.dumps(out, sort_keys=True))
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
