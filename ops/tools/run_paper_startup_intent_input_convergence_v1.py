#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_startup_intent_input_convergence_v1 import (  # noqa: E402
    derive_paper_startup_intent_input_convergence_payload_v1,
    write_paper_startup_intent_input_convergence_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance_release_current_first_v1  # noqa: E402
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1  # noqa: E402
from constellation_2.common.trade_submit_readiness_authority_v1 import (  # noqa: E402
    resolve_governed_sleeve_truth_bindings,
)
from constellation_2.common.trading_day_readiness_authority_v1 import (  # noqa: E402
    PREOPEN_MODES,
    read_or_evaluate_trading_day_readiness_authority_v1,
)


ENGINE_MODEL_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json")
SLEEVE_CONTRACTS_RELPATH = Path("governance/02_REGISTRIES/SLEEVE_CONTRACTS_V1.json")
DAILY_MARKET_SNAPSHOT_TEMPLATE = "market_data_snapshot_v1/snapshots/{day}/{symbol}.market_data_snapshot.v1.json"


def _git_sha() -> str:
    try:
        provenance = resolve_release_provenance_release_current_first_v1(
            caller="ops/tools/run_paper_startup_intent_input_convergence_v1.py"
        )
        if isinstance(provenance, dict):
            git_sha = str(provenance.get("git_sha") or "").strip()
            if git_sha:
                return git_sha
    except Exception:
        pass
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _active_engine_allowed_symbols_by_id(*, repo_root: Path = REPO_ROOT) -> Dict[str, List[str]]:
    registry_path = Path(repo_root).resolve() / ENGINE_MODEL_REGISTRY_RELPATH
    payload = _read_json(registry_path)
    engines = payload.get("engines")
    if not isinstance(engines, list):
        raise ValueError(f"ENGINE_REGISTRY_ENGINES_NOT_LIST:path={registry_path}")
    active: Dict[str, List[str]] = {}
    for row in engines:
        if not isinstance(row, dict):
            continue
        engine_id = str(row.get("engine_id") or "").strip()
        activation_status = str(row.get("activation_status") or "").strip().upper()
        if not engine_id or activation_status != "ACTIVE":
            continue
        symbols = [
            str(symbol).strip().upper()
            for symbol in (row.get("allowed_symbols") or [])
            if str(symbol).strip()
        ]
        active[engine_id] = sorted(dict.fromkeys(symbols))
    return active


def _required_daily_market_snapshot_symbols_v1(*, repo_root: Path = REPO_ROOT) -> Dict[str, Any]:
    repo = Path(repo_root).resolve()
    engine_registry_path = repo / ENGINE_MODEL_REGISTRY_RELPATH
    sleeve_contracts_path = repo / SLEEVE_CONTRACTS_RELPATH
    active_symbols = _active_engine_allowed_symbols_by_id(repo_root=repo)
    contracts_payload = _read_json(sleeve_contracts_path)
    contracts = contracts_payload.get("contracts")
    if not isinstance(contracts, list):
        raise ValueError(f"SLEEVE_CONTRACTS_NOT_LIST:path={sleeve_contracts_path}")

    required_symbols: List[str] = []
    engines: List[Dict[str, Any]] = []
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        engine_id = str(contract.get("engine_id") or "").strip()
        if engine_id not in active_symbols:
            continue
        if bool(contract.get("can_generate_intents")) is not True:
            continue
        matching_inputs: List[Dict[str, str]] = []
        for required_input in contract.get("required_inputs") or []:
            if not isinstance(required_input, dict):
                continue
            if bool(required_input.get("required")) is not True:
                continue
            if str(required_input.get("truth_owner") or "").strip() != "sleeve_truth_root":
                continue
            if str(required_input.get("required_scope") or "").strip() != "allowed_symbols":
                continue
            if str(required_input.get("expected_artifact_type") or "").strip() != "json":
                continue
            path_template = str(required_input.get("path_template") or "").strip()
            if path_template != DAILY_MARKET_SNAPSHOT_TEMPLATE:
                continue
            symbols = active_symbols.get(engine_id, [])
            required_symbols.extend(symbols)
            matching_inputs.append(
                {
                    "name": str(required_input.get("name") or "").strip(),
                    "path_template": path_template,
                }
            )
        if matching_inputs:
            engines.append(
                {
                    "engine_id": engine_id,
                    "required_symbols": active_symbols.get(engine_id, []),
                    "required_inputs": matching_inputs,
                }
            )

    return {
        "required_snapshot_symbols": sorted(dict.fromkeys(required_symbols)),
        "source_registry_paths": [
            str(engine_registry_path),
            str(sleeve_contracts_path),
        ],
        "source_registry_hashes": {
            "ENGINE_MODEL_REGISTRY_V1": _sha256_file(engine_registry_path),
            "SLEEVE_CONTRACTS_V1": _sha256_file(sleeve_contracts_path),
        },
        "required_snapshot_engines": engines,
    }


def _tool_result(script_relpath: str, *args: str, extra_env: Dict[str, str] | None = None) -> Dict[str, Any]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    command = [sys.executable, str((REPO_ROOT / script_relpath).resolve()), *args]
    proc = subprocess.run(command, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "script": script_relpath,
        "command": command,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _module_result(module_name: str, *args: str, extra_env: Dict[str, str] | None = None) -> Dict[str, Any]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    command = [sys.executable, "-m", module_name, *args]
    proc = subprocess.run(command, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "script": module_name,
        "command": command,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _artifact_result(
    *,
    artifact_id: str,
    artifact_path: Path,
    target_day: str,
    required: bool,
    acceptable_statuses: Iterable[str],
    presence_only: bool = False,
) -> Dict[str, Any]:
    path = Path(artifact_path).resolve()
    observed_status = "MISSING"
    observed_day = ""
    ready = False
    reason_codes: List[str] = []
    schema_id = ""
    if path.exists() and path.is_file():
        payload = _read_json(path)
        observed_status = (
            str(payload.get("status") or payload.get("overall_status") or payload.get("final_status") or "").strip().upper()
            or "UNKNOWN"
        )
        observed_day = str(payload.get("day_utc") or payload.get("target_day") or "").strip()
        schema_id = str(payload.get("schema_id") or "").strip()
        reason_codes = [
            str(code).strip()
            for code in (payload.get("reason_codes") or payload.get("blocking_codes") or [])
            if str(code).strip()
        ]
        if presence_only:
            ready = observed_day == target_day or not observed_day
        else:
            ready = observed_day == target_day and observed_status in {str(item).strip().upper() for item in acceptable_statuses}
    blocker_code = ""
    summary = ""
    if not ready:
        if not path.exists():
            blocker_code = f"{artifact_id.upper()}_MISSING"
            summary = f"{artifact_id} missing"
        elif observed_day != target_day:
            blocker_code = f"{artifact_id.upper()}_DAY_MISMATCH"
            summary = f"{artifact_id} day mismatch"
        else:
            blocker_code = f"{artifact_id.upper()}_NOT_READY"
            summary = f"{artifact_id} status={observed_status or 'UNKNOWN'}"
    return {
        "artifact_id": artifact_id,
        "required": bool(required),
        "artifact_path": str(path),
        "schema_id": schema_id,
        "target_day_expected": target_day,
        "target_day_observed": observed_day,
        "observed_status": observed_status,
        "ready": bool(ready),
        "reason_codes": reason_codes,
        "blocker_code": blocker_code,
        "summary": summary,
    }


def _daily_market_snapshot_convergence_result(
    *,
    sleeve_truth_root: Path,
    target_day: str,
    required_symbols: List[str],
) -> tuple[Dict[str, Any], List[str], List[str], List[Dict[str, Any]]]:
    normalized_required_symbols = sorted(
        dict.fromkeys(str(item).strip().upper() for item in required_symbols if str(item).strip())
    )
    per_symbol_results: List[Dict[str, Any]] = []
    materialized_symbols: List[str] = []
    missing_symbols: List[str] = []
    for symbol in normalized_required_symbols:
        artifact_path = (
            Path(sleeve_truth_root).resolve()
            / "market_data_snapshot_v1"
            / "snapshots"
            / str(target_day).strip()
            / f"{symbol}.market_data_snapshot.v1.json"
        )
        row = _artifact_result(
            artifact_id=f"market_data_snapshot_v1:{symbol}",
            artifact_path=artifact_path,
            target_day=target_day,
            required=True,
            acceptable_statuses=("OK",),
            presence_only=True,
        )
        row["symbol"] = symbol
        per_symbol_results.append(row)
        if bool(row.get("ready")) is True:
            materialized_symbols.append(symbol)
        else:
            missing_symbols.append(symbol)

    ready = bool(normalized_required_symbols) and not missing_symbols
    aggregate_path = (
        Path(sleeve_truth_root).resolve()
        / "market_data_snapshot_v1"
        / "snapshots"
        / str(target_day).strip()
    )
    reason_codes = [f"MARKET_DATA_SNAPSHOT_V1_MISSING_SYMBOL:{symbol}" for symbol in missing_symbols]
    return (
        {
            "artifact_id": "market_data_snapshot_v1",
            "required": True,
            "artifact_path": str(aggregate_path),
            "schema_id": "market_data_snapshot_v1",
            "target_day_expected": str(target_day).strip(),
            "target_day_observed": str(target_day).strip() if ready else "",
            "observed_status": "OK" if ready else "MISSING",
            "ready": ready,
            "reason_codes": reason_codes,
            "blocker_code": "" if ready else "MARKET_DATA_SNAPSHOT_V1_MISSING_SYMBOLS",
            "summary": ""
            if ready
            else f"market_data_snapshot_v1 missing symbols: {', '.join(missing_symbols) if missing_symbols else 'NONE'}",
            "required_snapshot_symbols": normalized_required_symbols,
            "materialized_snapshot_symbols": materialized_symbols,
            "missing_snapshot_symbols": missing_symbols,
        },
        materialized_symbols,
        missing_symbols,
        per_symbol_results,
    )


def _not_required_artifact_result(*, artifact_id: str, artifact_path: Path, target_day: str, reason: str) -> Dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "required": False,
        "artifact_path": str(Path(artifact_path).resolve()),
        "schema_id": "",
        "target_day_expected": target_day,
        "target_day_observed": target_day,
        "observed_status": str(reason or "NOT_REQUIRED").strip().upper(),
        "ready": True,
        "reason_codes": [str(reason or "NOT_REQUIRED").strip().upper()],
        "blocker_code": "",
        "summary": f"{artifact_id} not required by trading-day readiness mode",
    }


def _resolve_primary_binding(*, environment: str, ib_account: str):
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return binding
    if not bindings:
        raise ValueError("NO_GOVERNED_SLEEVE_TRUTH_BINDINGS")
    return bindings[0]


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_startup_intent_input_convergence_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", required=True)
    ap.add_argument("--bridge_symbol", default="")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    environment = str(args.environment).strip().upper()
    ib_account = str(args.ib_account).strip()
    bridge_symbol = str(args.bridge_symbol).strip().upper()
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    binding = _resolve_primary_binding(environment=environment, ib_account=ib_account)
    sleeve_truth_root = Path(binding.truth_root).resolve()
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=day_utc,
        truth_root=truth_root,
        execution_root=sleeve_truth_root,
        environment=environment,
    )
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    preopen_mode = readiness_mode in PREOPEN_MODES
    git_sha = _git_sha()
    shared_env = {"C2_MODE": environment, "C2_TRUTH_ROOT": str(sleeve_truth_root)}
    snapshot_requirements = (
        {
            "required_snapshot_symbols": [],
            "source_registry_paths": [
                str((REPO_ROOT / ENGINE_MODEL_REGISTRY_RELPATH).resolve()),
                str((REPO_ROOT / SLEEVE_CONTRACTS_RELPATH).resolve()),
            ],
            "source_registry_hashes": {},
            "required_snapshot_engines": [],
        }
        if preopen_mode
        else _required_daily_market_snapshot_symbols_v1(repo_root=REPO_ROOT)
    )
    required_snapshot_symbols = [
        str(symbol).strip().upper()
        for symbol in (snapshot_requirements.get("required_snapshot_symbols") or [])
        if str(symbol).strip()
    ]
    symbol_diagnostics = {
        "selected_intent_symbol": "",
        "required_options_symbol": "",
        "options_snapshot_symbol": "",
        "symbol_source": "PREOPEN_NOT_REQUIRED" if preopen_mode else "ENGINE_MODEL_REGISTRY_V1+SLEEVE_CONTRACTS_V1",
        "stale_default_symbol_detected": False,
        "bridge_symbol": bridge_symbol,
        "bridge_symbol_source": "DEPRECATED_ARGUMENT_IGNORED" if bridge_symbol else "NOT_PROVIDED",
        "bridge_symbol_used_for_market_snapshot": False,
        "required_snapshot_symbols": required_snapshot_symbols,
        "materialized_snapshot_symbols": [],
        "missing_snapshot_symbols": [],
        "source_registry_paths": snapshot_requirements.get("source_registry_paths") or [],
        "source_registry_hashes": snapshot_requirements.get("source_registry_hashes") or {},
        "required_snapshot_engines": snapshot_requirements.get("required_snapshot_engines") or [],
    }

    source_refs: List[Dict[str, Any]] = []
    positions_v5_result = _module_result(
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
        "--day_utc",
        day_utc,
        "--producer_git_sha",
        git_sha,
        "--producer_repo",
        REPO_ROOT.name,
        "--truth_root",
        str(sleeve_truth_root),
        "--ib_account",
        ib_account,
        extra_env=shared_env,
    )
    source_refs.append(positions_v5_result)

    positions_v2_cmd = [
        sys.executable,
        "-m",
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
        "--day_utc",
        day_utc,
        "--producer_git_sha",
        git_sha,
        "--producer_repo",
        REPO_ROOT.name,
        "--truth_root",
        str(sleeve_truth_root),
    ]
    if int(positions_v5_result.get("return_code") or 0) == 0:
        positions_v2_result = _module_result(
            "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
            "--day_utc",
            day_utc,
            "--producer_git_sha",
            git_sha,
            "--producer_repo",
            REPO_ROOT.name,
            "--truth_root",
            str(sleeve_truth_root),
            extra_env=shared_env,
        )
    else:
        positions_v2_result = {
            "script": "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
            "command": positions_v2_cmd,
            "return_code": 99,
            "stdout": "",
            "stderr": "SKIPPED_POSITIONS_SNAPSHOT_V5_FAILED",
        }
    source_refs.append(positions_v2_result)

    if preopen_mode:
        source_refs.extend(
            [
                {
                    "script": "ops/tools/run_regime_snapshot_v2.py",
                    "command": [],
                    "return_code": 0,
                    "stdout": "",
                    "stderr": f"SKIPPED_BY_TRADING_DAY_READINESS_MODE:{readiness_mode}",
                },
                {
                    "script": "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py",
                    "command": [],
                    "return_code": 0,
                    "stdout": "",
                    "stderr": f"SKIPPED_BY_TRADING_DAY_READINESS_MODE:{readiness_mode}:future_market_snapshot_forbidden",
                },
            ]
        )
    else:
        source_refs.append(
            _tool_result(
                "ops/tools/run_regime_snapshot_v2.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(sleeve_truth_root),
                extra_env={"C2_MODE": environment},
            )
        )
        for symbol in required_snapshot_symbols:
            source_refs.append(
                _tool_result(
                    "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py",
                    "--day_utc",
                    day_utc,
                    "--symbol",
                    symbol,
                    extra_env=shared_env,
                )
            )

    artifact_results = [
        _artifact_result(
            artifact_id="positions_snapshot_v1",
            artifact_path=sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("OK",),
            presence_only=True,
        ),
    ]
    if preopen_mode:
        artifact_results.extend(
            [
                _not_required_artifact_result(
                    artifact_id="regime_snapshot_v2",
                    artifact_path=sleeve_truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json",
                    target_day=day_utc,
                    reason="PREOPEN_INPUTS_NOT_REQUIRED",
                ),
                _not_required_artifact_result(
                    artifact_id="positions_snapshot_v2",
                    artifact_path=sleeve_truth_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
                    target_day=day_utc,
                    reason="PREOPEN_LEGACY_POSITION_INPUT_NOT_REQUIRED",
                ),
                _not_required_artifact_result(
                    artifact_id="accounting_nav_snapshot_v1",
                    artifact_path=sleeve_truth_root / "accounting_v1" / "nav" / day_utc / "nav_snapshot.v1.json",
                    target_day=day_utc,
                    reason="PREOPEN_NAV_OWNED_BY_CAPITAL_SUPPLY_AND_SAFETY_STATE",
                ),
                _not_required_artifact_result(
                    artifact_id="market_data_snapshot_v1",
                    artifact_path=sleeve_truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / "__preopen_not_required__.market_data_snapshot.v1.json",
                    target_day=day_utc,
                    reason="PREOPEN_FUTURE_MARKET_DATA_FORBIDDEN",
                ),
            ]
        )
    else:
        (
            market_snapshot_result,
            materialized_snapshot_symbols,
            missing_snapshot_symbols,
            per_symbol_market_snapshot_results,
        ) = _daily_market_snapshot_convergence_result(
            sleeve_truth_root=sleeve_truth_root,
            target_day=day_utc,
            required_symbols=required_snapshot_symbols,
        )
        symbol_diagnostics["materialized_snapshot_symbols"] = materialized_snapshot_symbols
        symbol_diagnostics["missing_snapshot_symbols"] = missing_snapshot_symbols
        symbol_diagnostics["market_data_snapshot_results"] = per_symbol_market_snapshot_results
        artifact_results.extend(
            [
                _artifact_result(
                    artifact_id="regime_snapshot_v2",
                    artifact_path=sleeve_truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json",
                    target_day=day_utc,
                    required=True,
                    acceptable_statuses=("OK", "PASS", "BOOTSTRAP_OK"),
                    presence_only=True,
                ),
                _artifact_result(
                    artifact_id="positions_snapshot_v2",
                    artifact_path=sleeve_truth_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
                    target_day=day_utc,
                    required=True,
                    acceptable_statuses=("OK",),
                    presence_only=True,
                ),
                _artifact_result(
                    artifact_id="accounting_nav_snapshot_v1",
                    artifact_path=sleeve_truth_root / "accounting_v1" / "nav" / day_utc / "nav_snapshot.v1.json",
                    target_day=day_utc,
                    required=True,
                    acceptable_statuses=("OK",),
                    presence_only=True,
                ),
                market_snapshot_result,
            ]
        )
    required_input_ids = ["positions_snapshot_v1"] if preopen_mode else [
        "positions_snapshot_v1",
        "regime_snapshot_v2",
        "positions_snapshot_v2",
        "accounting_nav_snapshot_v1",
        "market_data_snapshot_v1",
    ]

    payload = derive_paper_startup_intent_input_convergence_payload_v1(
        truth_root=truth_root,
        target_day=day_utc,
        sleeve_id=str(binding.sleeve_id),
        environment=environment,
        ib_account=ib_account,
        sleeve_truth_root=sleeve_truth_root,
        required_inputs=required_input_ids,
        artifact_results=artifact_results,
        source_refs=source_refs,
        readiness_authority_path=str(readiness_path),
        readiness_mode=readiness_mode,
        evidence_policy_used=readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
        symbol_diagnostics=symbol_diagnostics,
    )
    ref = write_paper_startup_intent_input_convergence_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "convergence_status": payload["convergence_status"],
                "target_day": day_utc,
            },
            sort_keys=True,
        )
    )
    return 0 if payload["convergence_status"] == "SUCCESS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
