#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    NON_AUTHORITY_SCOPE,
    atomic_write_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    collect_intent_files_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
)
from ops.tools.run_phasec_identity_materializer_day_v1 import _same_day_market_close_sources


NEW_YORK_TZ = ZoneInfo("America/New_York")


def _resolve_report_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "startup_materialization_inputs_prep_v1"
        / day_utc
        / "startup_materialization_inputs_prep.v1.json"
    ).resolve()


def _liquidity_gate_artifact_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "liquidity_slippage_gate_v1"
        / day_utc
        / "liquidity_slippage_gate.v1.json"
    ).resolve()


def _run_liquidity_gate(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_liquidity_slippage_gate_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _parse_utc_timestamp(text: str) -> Optional[datetime]:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    return parsed.astimezone(timezone.utc)


def _core_session_open_utc(day_utc: str) -> datetime:
    target_day = date.fromisoformat(str(day_utc).strip())
    return datetime(
        target_day.year,
        target_day.month,
        target_day.day,
        9,
        30,
        0,
        tzinfo=NEW_YORK_TZ,
    ).astimezone(timezone.utc)


def _run_market_data_refresh_for_symbol(*, day_utc: str, truth_root: Path, symbol: str, run_utc: str) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py").resolve()),
        "--run_utc",
        str(run_utc).strip(),
        "--dataset_version",
        "v1",
        "--symbol",
        str(symbol).strip().upper(),
        "--start_year",
        str(day_utc).strip()[:4],
        "--end_year",
        str(day_utc).strip()[:4],
        "--host",
        str(os.environ.get("C2_IB_HOST") or "127.0.0.1").strip(),
        "--port",
        str(os.environ.get("C2_IB_PORT") or "4002").strip(),
        "--client_id",
        str(os.environ.get("C2_IB_CLIENT_ID") or "7").strip(),
        "--sleep_sec",
        str(os.environ.get("C2_IB_SLEEP_SEC") or "0.1").strip(),
        "--use_rth",
        "1",
    ]
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(truth_root)
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _positive_equity_entry_symbols(intent_files: List[Path]) -> List[str]:
    symbols: List[str] = []
    for path in intent_files:
        try:
            intent_obj = read_json_object_v1(path)
        except ValueError:
            continue
        underlying = intent_obj.get("underlying")
        symbol = ""
        if isinstance(underlying, dict):
            symbol = str(underlying.get("symbol") or "").strip().upper()
        elif isinstance(underlying, str):
            symbol = underlying.strip().upper()
        if not symbol:
            continue
        try:
            target_pct = Decimal(str(intent_obj.get("target_notional_pct") or "").strip())
        except (InvalidOperation, ValueError):
            continue
        if target_pct > Decimal("0"):
            symbols.append(symbol)
    return sorted(set(symbols))


def _gate_price_from_payload(*, gate_payload: Dict[str, Any], symbol: str) -> Optional[str]:
    status = str(gate_payload.get("status") or "").strip().upper()
    if status not in {"PASS", "OK"}:
        return None
    results = gate_payload.get("results")
    per_intent = results.get("per_intent") if isinstance(results, dict) else None
    if not isinstance(per_intent, list):
        return None
    symbol_upper = symbol.strip().upper()
    for row in per_intent:
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol") or "").strip().upper() != symbol_upper:
            continue
        metrics = row.get("metrics")
        if not isinstance(metrics, dict):
            continue
        close = str(metrics.get("close") or "").strip()
        if not close:
            continue
        try:
            close_value = Decimal(close)
        except InvalidOperation:
            continue
        if close_value <= Decimal("0"):
            continue
        return close
    return None


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_startup_materialization_inputs_prep_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    produced_at_utc = now_utc_iso_v1()
    session_id = canonical_paper_session_id_v1(day_utc)

    intent_files = collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)
    inputs_checked: List[Dict[str, Any]] = [
        build_fact_dependency_row_v1(
            logical_name="intents_day_dir",
            absolute_path=(intent_truth_root / "intents_v1" / "snapshots" / day_utc).resolve(),
            status="PRESENT" if intent_files else "MISSING",
            reason_codes=[] if intent_files else ["STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT"],
            day_utc=day_utc,
        )
    ]
    inputs_checked.extend(
        build_fact_dependency_row_v1(
            logical_name=f"intent_file:{path.name}",
            absolute_path=path,
            status="PRESENT",
            reason_codes=[],
            day_utc=day_utc,
        )
        for path in intent_files
    )

    blocking_codes: List[str] = []
    equity_entry_symbols = _positive_equity_entry_symbols(intent_files)
    default_equity_reference_price = ""
    default_equity_reference_price_source = "NONE"
    default_equity_reference_price_artifact_path = ""
    liquidity_gate_path = _liquidity_gate_artifact_path(truth_root=truth_root, day_utc=day_utc)
    liquidity_gate_result: Dict[str, Any] = {
        "returncode": 0,
        "stdout": "",
        "stderr": "",
        "artifact_path": str(liquidity_gate_path),
        "artifact_status": "NOT_REQUIRED",
    }

    if not intent_files:
        blocking_codes.append("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT")
        status = "BLOCKED_VALID"
        human_readable_summary = "No same-day intent snapshots exist for startup materialization input prep."
    elif not equity_entry_symbols:
        status = "PASS"
        human_readable_summary = "No positive equity entry intents require a default reference price."
    elif len(equity_entry_symbols) > 1:
        blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:DEFAULT_EQUITY_REFERENCE_PRICE_MULTI_SYMBOL_UNSUPPORTED")
        status = "BLOCKED_VALID"
        human_readable_summary = "Multiple positive equity entry symbols require deterministic reference-price resolution."
    else:
        symbol = equity_entry_symbols[0]
        same_day_close, same_day_paths = _same_day_market_close_sources(
            truth_root=truth_root,
            day_utc=day_utc,
            symbol=symbol,
            evaluation_time_utc=produced_at_utc,
        )
        evaluation_dt = _parse_utc_timestamp(produced_at_utc)
        if not str(same_day_close or "").strip() and evaluation_dt is not None and evaluation_dt >= _core_session_open_utc(day_utc):
            _run_market_data_refresh_for_symbol(
                day_utc=day_utc,
                truth_root=truth_root,
                symbol=symbol,
                run_utc=produced_at_utc,
            )
            same_day_close, same_day_paths = _same_day_market_close_sources(
                truth_root=truth_root,
                day_utc=day_utc,
                symbol=symbol,
                evaluation_time_utc=produced_at_utc,
            )
        for source_path in same_day_paths:
            inputs_checked.append(
                build_fact_dependency_row_v1(
                    logical_name=f"same_day_core_session_price_source:{source_path.name}",
                    absolute_path=source_path,
                    status="PRESENT" if source_path.exists() else "MISSING",
                    reason_codes=[] if source_path.exists() else ["STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:SAME_DAY_CORE_SESSION_PRICE_SOURCE_MISSING"],
                    day_utc=day_utc,
                )
            )
        if str(same_day_close or "").strip():
            default_equity_reference_price = str(same_day_close).strip()
            default_equity_reference_price_source = "SAME_DAY_CORE_SESSION_PRICE"
            default_equity_reference_price_artifact_path = str(same_day_paths[-1]) if same_day_paths else ""
            status = "PASS"
            human_readable_summary = (
                f"Resolved default equity reference price from a governed positive same-day core-session price for {symbol}."
            )
        else:
            blocking_codes.append("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE")
            status = "BLOCKED_VALID"
            human_readable_summary = (
                f"No governed positive same-day 09:30+ America/New_York core-session price is yet available for {symbol}."
            )

    payload: Dict[str, Any] = {
        "schema_id": "startup_materialization_inputs_prep",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "status": status,
        "equity_entry_symbols": equity_entry_symbols,
        "default_equity_reference_price": default_equity_reference_price,
        "default_equity_reference_price_source": default_equity_reference_price_source,
        "default_equity_reference_price_artifact_path": default_equity_reference_price_artifact_path,
        "required_inputs_checked": inputs_checked,
        "blocking_codes": sorted(set(blocking_codes)),
        "producer": producer_block_v1(
            module="ops/tools/run_startup_materialization_inputs_prep_v1.py",
            git_sha=repo_git_sha_v1(),
        ),
        "produced_at_utc": produced_at_utc,
        "liquidity_gate_result": {
            "returncode": int(liquidity_gate_result["returncode"]),
            "stdout": str(liquidity_gate_result["stdout"]),
            "stderr": str(liquidity_gate_result["stderr"]),
            "artifact_path": str(liquidity_gate_result["artifact_path"]),
            "artifact_status": str(liquidity_gate_result["artifact_status"]),
        },
        "human_readable_summary": human_readable_summary,
    }
    ref = atomic_write_validated_json_v1(
        path=_resolve_report_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization_inputs_prep.v1.schema.json",
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "status": status}, sort_keys=True))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
