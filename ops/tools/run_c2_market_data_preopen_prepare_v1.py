#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.common.level3_day_common_v1 import active_controllable_runtime_engine_ids_for_sleeve
from constellation_2.common.engine_universe_v1 import EngineUniverseError, resolve_engine_candidate_basis


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
GLOBAL_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
POLICY_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json").resolve()
ALLOWED_SYMBOL_SOURCE_CLASSES = {"DYNAMIC_SAME_DAY", "GOVERNED_CURATED", "FIXED_STRUCTURAL"}


def _no_intents_day_marker_path(*, truth_root: Path, day: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day / "no_intents_day.v1.json").resolve()


def _no_intents_day_marker_present(*, truth_root: Path, day: str) -> bool:
    marker_path = _no_intents_day_marker_path(truth_root=truth_root, day=day)
    return marker_path.exists() and marker_path.is_file()


def _same_day_intent_symbols_for_engine(*, truth_root: Path, day: str, engine_id: str) -> List[str]:
    intents_dir = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    if not intents_dir.exists() or not intents_dir.is_dir():
        return []
    symbols: List[str] = []
    for path in sorted(intents_dir.iterdir(), key=lambda p: p.name):
        if not path.is_file() or not path.name.endswith(".exposure_intent.v1.json"):
            continue
        try:
            intent = _read_json(path)
        except SystemExit:
            continue
        if str(intent.get("schema_id") or "").strip() != "exposure_intent":
            continue
        engine = intent.get("engine") if isinstance(intent.get("engine"), dict) else {}
        if str(engine.get("engine_id") or "").strip().upper() != engine_id:
            continue
        underlying = intent.get("underlying") if isinstance(intent.get("underlying"), dict) else {}
        symbol = str(underlying.get("symbol") or "").strip().upper()
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT path={path}")
    return obj


def _policy_rows() -> Dict[str, Dict[str, Any]]:
    registry = _read_json(POLICY_REGISTRY_PATH)
    policies = registry.get("policies")
    if not isinstance(policies, list):
        raise SystemExit("FAIL: policy registry policies not list")
    out: Dict[str, Dict[str, Any]] = {}
    for policy in policies:
        if not isinstance(policy, dict):
            continue
        engine_id = str(policy.get("engine_id") or "").strip().upper()
        if engine_id:
            out[engine_id] = policy
    return out


def _curated_symbols_for_engine(policy: Dict[str, Any]) -> List[str]:
    universe_mode = str(policy.get("universe_mode") or "").strip().upper()
    if universe_mode == "CURATED_SYMBOLS":
        return sorted({str(symbol).strip().upper() for symbol in (policy.get("curated_symbols") or []) if str(symbol).strip()})
    if universe_mode == "CURATED_PAIRS":
        symbols: List[str] = []
        for pair in (policy.get("curated_pairs") or []):
            if not isinstance(pair, list) or len(pair) != 2:
                continue
            symbols.extend([str(pair[0]).strip().upper(), str(pair[1]).strip().upper()])
        return sorted({symbol for symbol in symbols if symbol})
    return []


def _symbol_source_class_for_engine(*, engine_id: str, policy: Dict[str, Any]) -> str:
    symbol_source_class = str(policy.get("symbol_source_class") or "").strip().upper()
    if symbol_source_class not in ALLOWED_SYMBOL_SOURCE_CLASSES:
        raise SystemExit(f"FAIL: PREOPEN_SYMBOL_SOURCE_CLASS_INVALID engine_id={engine_id}")
    return symbol_source_class


def _candidate_symbols_for_ranked_engine(*, day: str, truth_root: Path, engine_id: str) -> List[str]:
    basis = resolve_engine_candidate_basis(engine_id=engine_id, day_utc=day, truth_root=truth_root)
    return sorted(set(basis.candidate_symbols))


def _run_cmd(cmd: List[str]):
    import subprocess
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)


def _parse_json_text(stdout: str) -> Dict[str, Any]:
    text = str(stdout or '').strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _paper_expected_calendar_no_op(*, truth_root: Path, day: str) -> bool:
    posture_path = (
        truth_root
        / "reports"
        / "paper_trading_posture_v1"
        / str(day).strip()
        / "paper_trading_posture.v1.json"
    ).resolve()
    posture = _load_json_if_exists(posture_path)
    if not posture:
        return False
    posture_class = str(posture.get("posture_class") or "").strip().upper()
    blocking_family = str(posture.get("blocking_family") or "").strip().upper()
    expected_no_op_today = bool(posture.get("expected_no_op_today") is True)
    reason_codes = {
        str(code).strip()
        for code in (posture.get("blocking_reason_codes") or [])
        if str(code).strip()
    }
    return (
        posture_class == "PAPER_READY_NO_OP"
        and blocking_family == "EXPECTED_NO_OP"
        and expected_no_op_today
        and "MARKET_CALENDAR_NON_TRADING_SESSION" in reason_codes
    )


def _canonical_refresh_report_path(*, truth_root: Path, day: str) -> Path:
    return (truth_root / "reports" / "canonical_market_data_refresh_v1" / day / "canonical_market_data_refresh.v1.json").resolve()


def _truth_spine_verification_report_path(*, truth_root: Path, day: str) -> Path:
    return (
        truth_root
        / "reports"
        / "market_data_truth_spine_verification_v1"
        / day
        / "market_data_truth_spine_verification.v1.json"
    ).resolve()


def _persisted_market_data_pass(*, truth_root: Path, day: str) -> Tuple[bool, Dict[str, Any], Dict[str, Any]]:
    refresh_payload = _load_json_if_exists(_canonical_refresh_report_path(truth_root=truth_root, day=day))
    verify_payload = _load_json_if_exists(_truth_spine_verification_report_path(truth_root=truth_root, day=day))
    refresh_ok = str(refresh_payload.get("status") or "").strip().upper() == "PASS"
    verify_ok = str(verify_payload.get("status") or "").strip().upper() == "PASS"
    return refresh_ok and verify_ok, refresh_payload, verify_payload


def _fallback_engines_from_same_day_intents(*, day: str, truth_root: Path, engine_ids: List[str]) -> Tuple[Dict[str, List[str]], List[Dict[str, Any]]]:
    fallback_symbols: Dict[str, List[str]] = {}
    fallback_details: List[Dict[str, Any]] = []
    for engine_id in sorted(set(engine_ids)):
        symbols = _same_day_intent_symbols_for_engine(truth_root=truth_root, day=day, engine_id=engine_id)
        if not symbols:
            continue
        fallback_symbols[engine_id] = symbols
        fallback_details.append(
            {
                "engine_id": engine_id,
                "reason_code": f"CANDIDATE_BASIS_SAME_DAY_UNAVAILABLE:{engine_id}:{day}",
                "fallback_source": "SAME_DAY_EXPOSURE_INTENTS",
                "required_symbols": list(symbols),
            }
        )
    return fallback_symbols, fallback_details


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_market_data_preopen_prepare_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--produced_utc", default="")
    ap.add_argument("--truth_root", default=str(GLOBAL_TRUTH_ROOT))
    ap.add_argument("--sleeve_id", default="PRIMARY")
    ap.add_argument("--mode", default="PAPER")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = str(args.produced_utc).strip() or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    truth_root = Path(str(args.truth_root).strip() or str(GLOBAL_TRUTH_ROOT)).resolve()
    expected_calendar_no_op = _paper_expected_calendar_no_op(truth_root=truth_root, day=day)
    active_engine_ids = active_controllable_runtime_engine_ids_for_sleeve(
        sleeve_id=str(args.sleeve_id).strip(),
        mode=str(args.mode).strip(),
    )
    policy_rows = _policy_rows()
    ranked_engine_ids: List[str] = []
    curated_symbols: List[str] = []
    symbol_source_details: List[Dict[str, Any]] = []
    for engine_id in active_engine_ids:
        policy = policy_rows.get(engine_id)
        if policy is None:
            raise SystemExit(f"FAIL: PREOPEN_POLICY_MISSING engine_id={engine_id}")
        universe_mode = str(policy.get("universe_mode") or "").strip().upper()
        symbol_source_class = _symbol_source_class_for_engine(engine_id=engine_id, policy=policy)
        if universe_mode == "LIQUIDITY_RANKED_SYMBOLS":
            ranked_engine_ids.append(engine_id)
        else:
            engine_symbols = _curated_symbols_for_engine(policy)
            curated_symbols.extend(engine_symbols)
            symbol_source_details.append(
                {
                    "engine_id": engine_id,
                    "symbol_source_class": symbol_source_class,
                    "required_symbol_source_artifact": str(POLICY_REGISTRY_PATH),
                    "required_symbols": engine_symbols,
                    "fallback_used": False,
                }
            )

    candidate_basis_result: Dict[str, Any] = {"name": "ranked_engine_candidate_basis_day_v1", "status": "PASS", "failures": [], "results": []}
    if ranked_engine_ids:
        candidate_proc = _run_cmd(
            [
                sys.executable,
                "ops/tools/run_ranked_engine_candidate_basis_day_v1.py",
                "--day_utc",
                day,
                "--produced_utc",
                produced_utc,
                "--truth_root",
                str(truth_root),
                "--sleeve_id",
                str(args.sleeve_id).strip(),
                "--mode",
                str(args.mode).strip(),
            ]
        )
        candidate_basis_result = _parse_json_text(candidate_proc.stdout)
        candidate_basis_result.setdefault("failures", [])
        candidate_basis_result["returncode"] = int(candidate_proc.returncode)
    fallback_basis_by_engine: Dict[str, List[str]] = {}
    fallback_basis_details: List[Dict[str, Any]] = []
    candidate_basis_failures = [
        str(engine_id).strip().upper()
        for engine_id in (candidate_basis_result.get("failures") or [])
        if str(engine_id).strip()
    ]
    fallback_engines = sorted(fallback_basis_by_engine.keys())
    inactive_engine_ids_due_to_missing_candidate_basis = sorted(set(candidate_basis_failures) - set(fallback_engines))

    ranked_candidate_symbols_by_engine: Dict[str, List[str]] = {}
    candidate_basis_resolution_failures: List[str] = []
    for engine_id in sorted(ranked_engine_ids):
        if engine_id in inactive_engine_ids_due_to_missing_candidate_basis:
            continue
        try:
            ranked_candidate_symbols_by_engine[engine_id] = _candidate_symbols_for_ranked_engine(
                day=day,
                truth_root=truth_root,
                engine_id=engine_id,
            )
        except EngineUniverseError:
            candidate_basis_resolution_failures.append(engine_id)
    if candidate_basis_resolution_failures:
        inactive_engine_ids_due_to_missing_candidate_basis = sorted(
            set(inactive_engine_ids_due_to_missing_candidate_basis) | set(candidate_basis_resolution_failures)
        )
    ranked_candidate_symbols = sorted({symbol for symbols in ranked_candidate_symbols_by_engine.values() for symbol in symbols})
    for engine_id in sorted(ranked_engine_ids):
        required_symbols = list(ranked_candidate_symbols_by_engine.get(engine_id, []))
        fallback_used = False
        if engine_id in fallback_basis_by_engine:
            required_symbols = list(fallback_basis_by_engine[engine_id])
            fallback_used = True
        symbol_source_details.append(
            {
                "engine_id": engine_id,
                "symbol_source_class": _symbol_source_class_for_engine(engine_id=engine_id, policy=policy_rows[engine_id]),
                "required_symbol_source_artifact": str(
                    (
                        GLOBAL_TRUTH_ROOT
                        / "reports"
                        / "engine_universe_candidate_basis_v1"
                        / day
                        / engine_id
                        / "engine_universe_candidate_basis.v1.json"
                    ).resolve()
                ),
                "required_symbols": required_symbols,
                "fallback_used": fallback_used,
                **({"fallback_source": "SAME_DAY_EXPOSURE_INTENTS"} if fallback_used else {}),
            }
        )
    symbols = sorted(set(ranked_candidate_symbols))
    unresolved_ranked_candidate_basis = bool(inactive_engine_ids_due_to_missing_candidate_basis)
    if unresolved_ranked_candidate_basis and not _no_intents_day_marker_present(truth_root=truth_root, day=day):
        if expected_calendar_no_op:
            payload = {
                "name": "c2_market_data_preopen_prepare_v1",
                "day_utc": day,
                "produced_utc": produced_utc,
                "truth_root": str(truth_root),
                "active_engine_ids": active_engine_ids,
                "ranked_engine_ids": sorted(ranked_engine_ids),
                "inactive_engine_ids_due_to_missing_candidate_basis": inactive_engine_ids_due_to_missing_candidate_basis,
                "symbol_source_classes": sorted({str(row["symbol_source_class"]) for row in symbol_source_details}),
                "required_symbol_basis_by_engine": symbol_source_details,
                "candidate_basis_fallbacks": fallback_basis_details,
                "candidate_basis_result": candidate_basis_result,
                "symbols_requested": [],
                "status": "CERTIFIED_NO_ACTION",
                "reason_codes": [
                    "NON_TRADING_SESSION_EXPECTED_NO_OP",
                    "RANKED_ENGINE_CANDIDATE_BASIS_UNAVAILABLE",
                ],
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        payload = {
            "name": "c2_market_data_preopen_prepare_v1",
            "day_utc": day,
            "produced_utc": produced_utc,
            "truth_root": str(truth_root),
            "active_engine_ids": active_engine_ids,
            "ranked_engine_ids": sorted(ranked_engine_ids),
            "inactive_engine_ids_due_to_missing_candidate_basis": inactive_engine_ids_due_to_missing_candidate_basis,
            "symbol_source_classes": sorted({str(row["symbol_source_class"]) for row in symbol_source_details}),
            "required_symbol_basis_by_engine": symbol_source_details,
            "candidate_basis_fallbacks": fallback_basis_details,
            "candidate_basis_result": candidate_basis_result,
            "symbols_requested": [],
            "status": "FAIL",
            "reason_codes": ["RANKED_ENGINE_CANDIDATE_BASIS_UNAVAILABLE"],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2
    if not symbols:
        if ranked_engine_ids:
            if expected_calendar_no_op:
                payload = {
                    "name": "c2_market_data_preopen_prepare_v1",
                    "day_utc": day,
                    "produced_utc": produced_utc,
                    "truth_root": str(truth_root),
                    "active_engine_ids": active_engine_ids,
                    "ranked_engine_ids": sorted(ranked_engine_ids),
                    "inactive_engine_ids_due_to_missing_candidate_basis": inactive_engine_ids_due_to_missing_candidate_basis,
                    "symbol_source_classes": sorted({str(row["symbol_source_class"]) for row in symbol_source_details}),
                    "required_symbol_basis_by_engine": symbol_source_details,
                    "candidate_basis_fallbacks": fallback_basis_details,
                    "candidate_basis_result": candidate_basis_result,
                    "symbols_requested": [],
                    "status": "CERTIFIED_NO_ACTION",
                    "reason_codes": [
                        "NON_TRADING_SESSION_EXPECTED_NO_OP",
                        "RANKED_ENGINE_CANDIDATE_BASIS_UNAVAILABLE",
                    ],
                }
                print(json.dumps(payload, indent=2, sort_keys=True))
                return 0
            payload = {
                "name": "c2_market_data_preopen_prepare_v1",
                "day_utc": day,
                "produced_utc": produced_utc,
                "truth_root": str(truth_root),
                "active_engine_ids": active_engine_ids,
                "ranked_engine_ids": sorted(ranked_engine_ids),
                "inactive_engine_ids_due_to_missing_candidate_basis": inactive_engine_ids_due_to_missing_candidate_basis,
                "symbol_source_classes": sorted({str(row["symbol_source_class"]) for row in symbol_source_details}),
                "required_symbol_basis_by_engine": symbol_source_details,
                "candidate_basis_fallbacks": fallback_basis_details,
                "candidate_basis_result": candidate_basis_result,
                "symbols_requested": [],
                "status": "FAIL",
                "reason_codes": ["RANKED_ENGINE_CANDIDATE_BASIS_UNAVAILABLE"],
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 2
        payload = {
            "name": "c2_market_data_preopen_prepare_v1",
            "day_utc": day,
            "produced_utc": produced_utc,
            "truth_root": str(truth_root),
            "active_engine_ids": active_engine_ids,
            "ranked_engine_ids": sorted(ranked_engine_ids),
            "inactive_engine_ids_due_to_missing_candidate_basis": inactive_engine_ids_due_to_missing_candidate_basis,
            "symbol_source_classes": sorted({str(row["symbol_source_class"]) for row in symbol_source_details}),
            "required_symbol_basis_by_engine": symbol_source_details,
            "candidate_basis_fallbacks": fallback_basis_details,
            "candidate_basis_result": candidate_basis_result,
            "symbols_requested": [],
            "status": "CERTIFIED_NO_ACTION",
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    refresh_cmd = [
        sys.executable,
        "ops/tools/run_c2_canonical_market_data_refresh_v1.py",
        "--day_utc",
        day,
        "--produced_utc",
        produced_utc,
        "--truth_root",
        str(truth_root),
    ]
    for symbol in symbols:
        refresh_cmd.extend(["--symbol", symbol])
    refresh_proc = _run_cmd(refresh_cmd)
    if refresh_proc.returncode != 0:
        persisted_ok, refresh_report, verify_report = _persisted_market_data_pass(truth_root=truth_root, day=day)
        if not persisted_ok:
            sys.stdout.write(refresh_proc.stdout)
            sys.stderr.write(refresh_proc.stderr)
            return 2

    verify_proc = _run_cmd(
        [
            sys.executable,
            "constellation_2/phaseJ/acceptance/verify_market_data_truth_spine_v1.py",
            "--day_utc",
            day,
            "--produced_utc",
            produced_utc,
            "--truth_root",
            str(truth_root),
        ]
    )
    if verify_proc.returncode != 0:
        persisted_ok, refresh_report, verify_report = _persisted_market_data_pass(truth_root=truth_root, day=day)
        if not persisted_ok:
            sys.stdout.write(verify_proc.stdout)
            sys.stderr.write(verify_proc.stderr)
            return 2
    else:
        _, refresh_report, verify_report = _persisted_market_data_pass(truth_root=truth_root, day=day)

    payload = {
        "name": "c2_market_data_preopen_prepare_v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "truth_root": str(truth_root),
        "active_engine_ids": active_engine_ids,
        "ranked_engine_ids": sorted(ranked_engine_ids),
        "inactive_engine_ids_due_to_missing_candidate_basis": inactive_engine_ids_due_to_missing_candidate_basis,
        "symbol_source_classes": sorted({str(row["symbol_source_class"]) for row in symbol_source_details}),
        "required_symbol_basis_by_engine": symbol_source_details,
        "fallback_used": any(bool(row.get("fallback_used")) for row in symbol_source_details),
        "candidate_basis_fallbacks": fallback_basis_details,
        "candidate_basis_result": candidate_basis_result,
        "symbols_requested": symbols,
        "status": "PASS",
        "canonical_market_data_refresh_status": str(refresh_report.get("status") or ""),
        "canonical_market_data_refresh_artifact_path": str(_canonical_refresh_report_path(truth_root=truth_root, day=day)),
        "market_data_truth_spine_verification_status": str(verify_report.get("status") or ""),
        "market_data_truth_spine_verification_artifact_path": str(
            _truth_spine_verification_report_path(truth_root=truth_root, day=day)
        ),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
