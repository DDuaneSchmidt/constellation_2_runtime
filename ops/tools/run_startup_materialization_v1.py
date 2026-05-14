#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_execution_authority_v1 import resolve_governed_paper_execution_roots
from constellation_2.common.paper_session_fact_plane_v1 import (
    NON_AUTHORITY_SCOPE,
    atomic_write_idempotent_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    collect_intent_files_v1,
    discover_phasec_identity_dirs_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    phasec_root_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
    resolve_startup_materialization_path,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.startup_materialization_input_convergence_v1 import (
    resolve_startup_materialization_input_convergence_path,
)


def _startup_materialization_inputs_prep_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "startup_materialization_inputs_prep_v1"
        / day_utc
        / "startup_materialization_inputs_prep.v1.json"
    ).resolve()


def _phasec_risk_inputs_prep_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "phasec_risk_inputs_prep_v1"
        / day_utc
        / "phasec_risk_inputs_prep.v1.json"
    ).resolve()


def _startup_materialization_input_convergence_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_startup_materialization_input_convergence_path(truth_root=truth_root, day_utc=day_utc)


def _run_input_convergence(
    *,
    day_utc: str,
    truth_root: Path,
    operator_input_root: Path,
    execution_truth_root: Path,
    ib_account: str,
) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_startup_materialization_input_convergence_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
        "--operator_input_root",
        str(operator_input_root),
        "--execution_truth_root",
        str(execution_truth_root),
        "--environment",
        "PAPER",
        "--ib_account",
        ib_account,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _run_inputs_prep(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_startup_materialization_inputs_prep_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _run_phasec_risk_inputs_prep(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_phasec_risk_inputs_prep_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _load_inputs_prep_payload(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        return read_json_object_v1(path)
    except ValueError:
        return None


def _load_input_convergence_payload(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        return read_json_object_v1(path)
    except ValueError:
        return None


def _load_phasec_risk_inputs_prep_payload(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        return read_json_object_v1(path)
    except ValueError:
        return None


def _run_phasec_materializer(
    *,
    day_utc: str,
    truth_root: Path,
    execution_truth_root: Path,
    default_equity_reference_price: str = "",
    equity_reference_prices_by_symbol: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_phasec_identity_materializer_day_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--eval_time_utc",
        f"{day_utc}T00:00:00Z",
        "--truth_root",
        str(truth_root),
        "--execution_truth_root",
        str(execution_truth_root),
    ]
    if str(default_equity_reference_price or "").strip():
        cmd.extend(["--default_equity_reference_price", str(default_equity_reference_price).strip()])
    clean_symbol_prices = {
        str(symbol or "").strip().upper(): str(price or "").strip()
        for symbol, price in (equity_reference_prices_by_symbol or {}).items()
        if str(symbol or "").strip() and str(price or "").strip()
    }
    if clean_symbol_prices:
        cmd.extend(
            [
                "--equity_reference_prices_by_symbol_json",
                json.dumps(clean_symbol_prices, sort_keys=True, separators=(",", ":")),
            ]
        )
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _short_vol_symbols(intent_files: List[Path]) -> List[str]:
    symbols: List[str] = []
    for path in intent_files:
        try:
            intent_obj = read_json_object_v1(path)
        except ValueError:
            continue
        if str(intent_obj.get("exposure_type") or "").strip().upper() != "SHORT_VOL_DEFINED":
            continue
        underlying = intent_obj.get("underlying")
        symbol = ""
        if isinstance(underlying, dict):
            symbol = str(underlying.get("symbol") or "").strip().upper()
        elif isinstance(underlying, str):
            symbol = underlying.strip().upper()
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _options_raw_exists_for_symbol(*, truth_root: Path, day_utc: str, symbol: str) -> bool:
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


def _options_snapshot_exists_for_symbol(*, truth_root: Path, day_utc: str, symbol: str) -> bool:
    root = (truth_root / "options_chain_snapshot_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return False
    for capture_dir in sorted(root.iterdir()):
        snap_path = (capture_dir / "options_chain_snapshot.v1.json").resolve()
        cert_path = (capture_dir / "freshness_certificate.v1.json").resolve()
        if not snap_path.exists() or not snap_path.is_file() or not cert_path.exists() or not cert_path.is_file():
            continue
        try:
            snap_obj = read_json_object_v1(snap_path)
        except ValueError:
            continue
        underlying = snap_obj.get("underlying") if isinstance(snap_obj.get("underlying"), dict) else {}
        if str(underlying.get("symbol") or "").strip().upper() == symbol.upper():
            return True
    return False


def _run_options_truth_promotion_for_symbol(*, day_utc: str, truth_root: Path, symbol: str) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_options_chain_truth_promotion_day_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--eval_time_utc",
        f"{day_utc}T00:00:00Z",
        "--symbol",
        symbol,
        "--truth_root",
        str(truth_root),
    ]
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(truth_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, env=env)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _run_options_raw_capture_for_symbol(*, day_utc: str, truth_root: Path, symbol: str) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_options_chain_capture_ib_day_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--eval_time_utc",
        f"{day_utc}T00:00:00Z",
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
    ]
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(truth_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, env=env)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _resolve_execution_truth_root() -> Path:
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    return resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id="PRIMARY",
    ).execution_root_path


def _identity_output_rows(identity_dirs: List[Path], *, day_utc: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for identity_dir in identity_dirs:
        rows.append(
            build_fact_dependency_row_v1(
                logical_name=f"phasec_identity_dir:{identity_dir.name}",
                absolute_path=identity_dir,
                status="PRESENT",
                reason_codes=[],
                day_utc=day_utc,
            )
        )
        for child in sorted(identity_dir.iterdir()):
            if not child.is_file():
                continue
            rows.append(
                build_fact_dependency_row_v1(
                    logical_name=f"phasec_materialized_output:{identity_dir.name}:{child.name}",
                    absolute_path=child,
                    status="PRESENT",
                    reason_codes=[],
                    day_utc=day_utc,
                )
            )
    return rows


def _scan_attempt_dir_vetoes(attempt_dir: Path) -> List[Dict[str, Any]]:
    veto_rows: List[Dict[str, Any]] = []
    veto_paths = sorted(attempt_dir.glob("*.veto_record.v1.json"), key=lambda item: item.name, reverse=True)
    for veto_path in veto_paths:
        try:
            veto_obj = read_json_object_v1(veto_path)
        except ValueError:
            continue
        selector = veto_path.name.removesuffix(".veto_record.v1.json").strip().lower()
        veto_rows.append(
            {
                "path": veto_path,
                "selector": selector,
                "reason_code": str(veto_obj.get("reason_code") or "").strip(),
                "reason_detail": str(veto_obj.get("reason_detail") or "").strip(),
            }
        )
    return veto_rows


def _collect_phasec_vetoes(*, phasec_root: Path, active_attempt_dir: Optional[Path]) -> List[Dict[str, Any]]:
    if active_attempt_dir is not None:
        return _scan_attempt_dir_vetoes(active_attempt_dir)
    if not phasec_root.exists() or not phasec_root.is_dir():
        return []
    attempt_dirs = sorted(
        [item for item in phasec_root.iterdir() if item.is_dir() and item.name.startswith("attempt_")],
        key=lambda item: item.name,
        reverse=True,
    )
    for attempt_dir in attempt_dirs:
        vetoes = _scan_attempt_dir_vetoes(attempt_dir)
        if vetoes:
            return vetoes
    return []


def _extract_selected_intent_hashes(identity_dirs: List[Path]) -> Set[str]:
    selected_hashes: Set[str] = set()
    for identity_dir in identity_dirs:
        dir_name = identity_dir.name.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", dir_name):
            selected_hashes.add(dir_name)
        identity_record_path = (identity_dir / "execution_identity_record.v1.json").resolve()
        if not identity_record_path.exists() or not identity_record_path.is_file():
            continue
        try:
            identity_obj = read_json_object_v1(identity_record_path)
        except ValueError:
            continue
        intent_hash = str(identity_obj.get("intent_hash") or "").strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", intent_hash):
            selected_hashes.add(intent_hash)
    return selected_hashes


def _phasec_veto_codes_from_row(veto: Dict[str, Any]) -> List[str]:
    codes: List[str] = []
    reason_code = str(veto.get("reason_code") or "").strip()
    reason_detail = str(veto.get("reason_detail") or "").strip()
    if reason_code:
        codes.append(f"STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:{reason_code}")
    if "DRAWDOWN_MISSING_FAIL_CLOSED" in reason_detail:
        codes.append("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DRAWDOWN_MISSING_FAIL_CLOSED")
    missing_match = re.search(r"INPUT_FILE_MISSING:\s*([^'\n]+)", reason_detail)
    if missing_match:
        missing_path = missing_match.group(1).strip()
        codes.append(f"STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INPUT_FILE_MISSING:{missing_path}")
    return codes


def _phasec_veto_evaluation(
    *,
    phasec_root: Path,
    active_attempt_dir: Optional[Path],
    selected_intent_hashes: Set[str],
    day_utc: str,
) -> Dict[str, Any]:
    vetoes = _collect_phasec_vetoes(phasec_root=phasec_root, active_attempt_dir=active_attempt_dir)
    if not vetoes:
        return {"blocking_codes": [], "ignored_rows": []}

    ignored_rows: List[Dict[str, Any]] = []

    # When no released identities are available, preserve fail-closed behavior:
    # block on the latest veto exactly as before.
    if not selected_intent_hashes:
        return {
            "blocking_codes": _phasec_veto_codes_from_row(vetoes[0]),
            "ignored_rows": ignored_rows,
        }

    matched = [
        row
        for row in vetoes
        if str(row.get("selector") or "").strip().lower() in selected_intent_hashes
    ]
    ignored = [
        row
        for row in vetoes
        if str(row.get("selector") or "").strip().lower() not in selected_intent_hashes
    ]
    for row in ignored:
        selector = str(row.get("selector") or "").strip().lower() or "UNKNOWN"
        ignored_rows.append(
            build_fact_dependency_row_v1(
                logical_name=f"phasec_veto_ignored_non_selected:{selector}",
                absolute_path=Path(row["path"]).resolve(),
                status="PRESENT",
                reason_codes=[
                    "STARTUP_MATERIALIZATION_DIAGNOSTIC:PHASEC_VETO_IGNORED_NON_SELECTED"
                ],
                day_utc=day_utc,
            )
        )

    blocking_codes: List[str] = []
    for row in matched:
        blocking_codes.extend(_phasec_veto_codes_from_row(row))
    return {
        "blocking_codes": sorted(set(blocking_codes)),
        "ignored_rows": ignored_rows,
    }


def _classify_status(*, blocking_codes: List[str], phasec_rc: int, identity_dirs: List[Path]) -> str:
    if not blocking_codes and phasec_rc == 0 and identity_dirs:
        return "SUCCESS"
    if any(code.startswith("STARTUP_MATERIALIZATION_MALFORMED") for code in blocking_codes):
        return "MALFORMED"
    if any(code.startswith("STARTUP_MATERIALIZATION_STALE") for code in blocking_codes):
        return "STALE"
    if any(code.startswith("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY") for code in blocking_codes):
        return "MISSING_DEPENDENCY"
    if any(code.startswith("STARTUP_MATERIALIZATION_UNKNOWN") for code in blocking_codes):
        return "UNKNOWN"
    return "FAIL"


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_startup_materialization_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--operator_input_root", default=str((REPO_ROOT / "constellation_2").resolve()))
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    operator_input_root = Path(str(args.operator_input_root or "").strip()).expanduser().resolve()
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    execution_truth_root = _resolve_execution_truth_root()
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
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

    inputs_prep_path = _startup_materialization_inputs_prep_path(truth_root=truth_root, day_utc=day_utc)
    input_convergence_path = _startup_materialization_input_convergence_path(truth_root=truth_root, day_utc=day_utc)
    phasec_risk_inputs_prep_path = _phasec_risk_inputs_prep_path(truth_root=truth_root, day_utc=day_utc)
    input_convergence_result = _run_input_convergence(
        day_utc=day_utc,
        truth_root=truth_root,
        operator_input_root=operator_input_root,
        execution_truth_root=execution_truth_root,
        ib_account=ib_account,
    )
    input_convergence_payload = _load_input_convergence_payload(input_convergence_path)
    input_convergence_blocking_codes: List[str] = []
    if input_convergence_payload is None:
        inputs_checked.append(
            build_fact_dependency_row_v1(
                logical_name="startup_materialization_input_convergence_v1",
                absolute_path=input_convergence_path,
                status="MISSING",
                reason_codes=["STARTUP_MATERIALIZATION_FAIL:INPUT_CONVERGENCE_OUTPUT_MISSING"],
                day_utc=day_utc,
            )
        )
        input_convergence_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:INPUT_CONVERGENCE_OUTPUT_MISSING")
    else:
        convergence_status = str(input_convergence_payload.get("convergence_status") or "").strip().upper()
        convergence_codes = [
            f"STARTUP_MATERIALIZATION_FAIL:{str(row.get('blocker_code') or row.get('artifact_id') or '').strip()}"
            for row in (input_convergence_payload.get("blocker_chain") or [])
            if isinstance(row, dict) and str(row.get("blocker_code") or row.get("artifact_id") or "").strip()
        ]
        inputs_checked.append(
            build_fact_dependency_row_v1(
                logical_name="startup_materialization_input_convergence_v1",
                absolute_path=input_convergence_path,
                status="PRESENT",
                reason_codes=convergence_codes,
                day_utc=day_utc,
            )
        )
        if convergence_status != "SUCCESS":
            input_convergence_blocking_codes.extend(
                convergence_codes or ["STARTUP_MATERIALIZATION_FAIL:INPUT_CONVERGENCE_BLOCKED"]
            )
    inputs_prep_result = _run_inputs_prep(day_utc=day_utc, truth_root=truth_root)
    inputs_prep_payload = _load_inputs_prep_payload(inputs_prep_path)
    prep_blocking_codes: List[str] = []
    phasec_risk_prep_blocking_codes: List[str] = []
    default_equity_reference_price = ""
    equity_reference_prices_by_symbol: Dict[str, str] = {}
    if inputs_prep_payload is None:
        inputs_checked.append(
            build_fact_dependency_row_v1(
                logical_name="startup_materialization_inputs_prep_v1",
                absolute_path=inputs_prep_path,
                status="MISSING",
                reason_codes=["STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_OUTPUT_MISSING"],
                day_utc=day_utc,
            )
        )
        prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_OUTPUT_MISSING")
    else:
        prep_codes = [
            str(code).strip()
            for code in (inputs_prep_payload.get("blocking_codes") or [])
            if str(code).strip()
        ]
        prep_status = str(inputs_prep_payload.get("status") or "").strip().upper()
        inputs_checked.append(
            build_fact_dependency_row_v1(
                logical_name="startup_materialization_inputs_prep_v1",
                absolute_path=inputs_prep_path,
                status="PRESENT",
                reason_codes=prep_codes,
                day_utc=day_utc,
            )
        )
        if prep_status == "PASS":
            default_equity_reference_price = str(
                inputs_prep_payload.get("default_equity_reference_price") or ""
            ).strip()
            raw_symbol_prices = inputs_prep_payload.get("equity_reference_prices_by_symbol")
            if isinstance(raw_symbol_prices, dict):
                equity_reference_prices_by_symbol = {
                    str(symbol or "").strip().upper(): str(price or "").strip()
                    for symbol, price in raw_symbol_prices.items()
                    if str(symbol or "").strip() and str(price or "").strip()
                }
        elif prep_status in {"BLOCKED_VALID", "BLOCKED_BY_DEFECT"}:
            prep_blocking_codes.extend(prep_codes or ["STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_BLOCKED"])
        else:
            prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_STATUS_UNKNOWN")

    if not input_convergence_blocking_codes and not prep_blocking_codes:
        _run_phasec_risk_inputs_prep(day_utc=day_utc, truth_root=truth_root)
        phasec_risk_prep_payload = _load_phasec_risk_inputs_prep_payload(phasec_risk_inputs_prep_path)
        if phasec_risk_prep_payload is None:
            inputs_checked.append(
                build_fact_dependency_row_v1(
                    logical_name="phasec_risk_inputs_prep_v1",
                    absolute_path=phasec_risk_inputs_prep_path,
                    status="MISSING",
                    reason_codes=["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_OUTPUT_MISSING"],
                    day_utc=day_utc,
                )
            )
            phasec_risk_prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_OUTPUT_MISSING")
        else:
            risk_codes = [
                f"STARTUP_MATERIALIZATION_FAIL:{str(code).strip()}"
                for code in (phasec_risk_prep_payload.get("blocking_codes") or [])
                if str(code).strip()
            ]
            risk_status = str(phasec_risk_prep_payload.get("status") or "").strip().upper()
            inputs_checked.append(
                build_fact_dependency_row_v1(
                    logical_name="phasec_risk_inputs_prep_v1",
                    absolute_path=phasec_risk_inputs_prep_path,
                    status="PRESENT",
                    reason_codes=risk_codes,
                    day_utc=day_utc,
                )
            )
            if risk_status in {"BLOCKED_VALID", "BLOCKED_BY_DEFECT"}:
                phasec_risk_prep_blocking_codes.extend(risk_codes or ["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_BLOCKED"])
            elif risk_status != "PASS":
                phasec_risk_prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_STATUS_UNKNOWN")

    if not input_convergence_blocking_codes and not prep_blocking_codes and not phasec_risk_prep_blocking_codes:
        for symbol in _short_vol_symbols(intent_files):
            if _options_snapshot_exists_for_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol):
                continue
            if not _options_raw_exists_for_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol):
                capture_result = _run_options_raw_capture_for_symbol(
                    day_utc=day_utc,
                    truth_root=intent_truth_root,
                    symbol=symbol,
                )
                if int(capture_result.get("returncode") or 0) != 0:
                    continue
            if not _options_raw_exists_for_symbol(truth_root=intent_truth_root, day_utc=day_utc, symbol=symbol):
                continue
            _run_options_truth_promotion_for_symbol(day_utc=day_utc, truth_root=intent_truth_root, symbol=symbol)

    phasec_root = phasec_root_v1(truth_root=execution_truth_root, day_utc=day_utc)
    latest_active_pointer = (phasec_root / "latest_active_attempt.v1.json").resolve()
    identity_dirs: List[Path] = []
    materialized_outputs: List[Dict[str, Any]] = []
    phasec_veto_codes: List[str] = []
    phasec_veto_ignored_rows: List[Dict[str, Any]] = []

    blocking_codes: List[str] = []
    if not intent_files:
        blocking_codes.append("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT")
    blocking_codes.extend(input_convergence_blocking_codes)
    blocking_codes.extend(prep_blocking_codes)
    blocking_codes.extend(phasec_risk_prep_blocking_codes)
    if input_convergence_blocking_codes or prep_blocking_codes or phasec_risk_prep_blocking_codes:
        phasec_result = {
            "cmd": [],
            "returncode": -1,
            "stdout": "",
            "stderr": "SKIPPED_PRESTART_PREP_BLOCKED",
        }
    else:
        phasec_result = _run_phasec_materializer(
            day_utc=day_utc,
            truth_root=intent_truth_root,
            execution_truth_root=execution_truth_root,
            default_equity_reference_price=default_equity_reference_price,
            equity_reference_prices_by_symbol=equity_reference_prices_by_symbol,
        )
        identity_dirs = discover_phasec_identity_dirs_v1(truth_root=execution_truth_root, day_utc=day_utc)
        materialized_outputs = _identity_output_rows(identity_dirs, day_utc=day_utc)
        active_attempt_dir: Optional[Path] = None
        if phasec_result["returncode"] != 0:
            blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:PHASEC_MATERIALIZER_NONZERO")
        if latest_active_pointer.exists() and latest_active_pointer.is_file():
            try:
                pointer_obj = read_json_object_v1(latest_active_pointer)
            except ValueError:
                blocking_codes.append("STARTUP_MATERIALIZATION_MALFORMED:LATEST_ACTIVE_ATTEMPT_POINTER_INVALID")
            else:
                if str(pointer_obj.get("day_utc") or "").strip() != day_utc:
                    blocking_codes.append("STARTUP_MATERIALIZATION_STALE:LATEST_ACTIVE_ATTEMPT_DAY_MISMATCH")
                else:
                    active_attempt_dir = Path(str(pointer_obj.get("attempt_dir") or "")).expanduser().resolve()
        selected_intent_hashes = _extract_selected_intent_hashes(identity_dirs)
        veto_eval = _phasec_veto_evaluation(
            phasec_root=phasec_root,
            active_attempt_dir=active_attempt_dir,
            selected_intent_hashes=selected_intent_hashes,
            day_utc=day_utc,
        )
        phasec_veto_codes = list(veto_eval["blocking_codes"])
        phasec_veto_ignored_rows = list(veto_eval["ignored_rows"])
        if phasec_veto_codes:
            blocking_codes.extend(phasec_veto_codes)
        else:
            if not latest_active_pointer.exists() or not latest_active_pointer.is_file():
                blocking_codes.append("STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING")
            if not identity_dirs:
                blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS")

    if phasec_veto_ignored_rows:
        inputs_checked.extend(phasec_veto_ignored_rows)

    status = _classify_status(
        blocking_codes=sorted(set(blocking_codes)),
        phasec_rc=int(phasec_result["returncode"]),
        identity_dirs=identity_dirs,
    )
    phasec_pointer_exists = latest_active_pointer.exists() and latest_active_pointer.is_file()
    phasec_evidence_present = bool(identity_dirs) or bool(phasec_veto_codes) or phasec_pointer_exists
    phasec_materializer_executed = int(phasec_result["returncode"]) == 0
    freshness_verdict = "CURRENT" if (status == "SUCCESS" or (phasec_materializer_executed and phasec_evidence_present)) else "UNKNOWN"
    linkage_verdict = "LINKED" if (status == "SUCCESS" or (phasec_materializer_executed and phasec_evidence_present)) else "UNLINKED"
    payload: Dict[str, Any] = {
        "schema_id": "startup_materialization",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "status": status,
        "required_inputs_checked": inputs_checked,
        "materialized_outputs": materialized_outputs,
        "blocking_codes": sorted(set(blocking_codes)),
        "producer": producer_block_v1(module="ops/tools/run_startup_materialization_v1.py", git_sha=repo_git_sha_v1()),
        "produced_at_utc": produced_at_utc,
        "freshness_verdict": freshness_verdict,
        "linkage_verdict": linkage_verdict,
        "path_resolution_evidence": {
            "phasec_root": str(phasec_root),
            "latest_active_attempt_path": str(latest_active_pointer),
        },
        "producer_run_id": f"startup_materialization_v1:{day_utc}",
        "phasec_materializer_result": {
            "returncode": int(phasec_result["returncode"]),
            "stdout": str(phasec_result["stdout"]),
            "stderr": str(phasec_result["stderr"]),
        },
        "startup_materialization_input_convergence_result": {
            "returncode": int(input_convergence_result["returncode"]),
            "stdout": str(input_convergence_result["stdout"]),
            "stderr": str(input_convergence_result["stderr"]),
        },
    }
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_startup_materialization_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json",
        volatile_field_names=("produced_at_utc",),
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "status": status}, sort_keys=True))
    return 0 if status == "SUCCESS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
