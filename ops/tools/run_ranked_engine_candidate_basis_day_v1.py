#!/usr/bin/env python3
from __future__ import annotations
import sys

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.engine_universe_v1 import (
    EngineUniverseError,
    emit_engine_candidate_basis_report,
    resolve_engine_candidate_basis,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
GLOBAL_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
POLICY_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json").resolve()
ENGINE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT path={path}")
    return obj


def _active_runtime_engine_ids() -> List[str]:
    registry = _read_json(ENGINE_REGISTRY_PATH)
    engines = registry.get("engines")
    if not isinstance(engines, list):
        raise SystemExit("FAIL: engine registry engines not list")
    out: List[str] = []
    for row in engines:
        if not isinstance(row, dict):
            continue
        if str(row.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue
        engine_id = str(row.get("engine_id") or "").strip().upper()
        if engine_id:
            out.append(engine_id)
    return sorted(set(out))


def _ranked_policy_engine_ids() -> List[str]:
    registry = _read_json(POLICY_REGISTRY_PATH)
    policies = registry.get("policies")
    if not isinstance(policies, list):
        raise SystemExit("FAIL: policy registry policies not list")
    out: List[str] = []
    for policy in policies:
        if not isinstance(policy, dict):
            continue
        if str(policy.get("universe_mode") or "").strip().upper() != "LIQUIDITY_RANKED_SYMBOLS":
            continue
        engine_id = str(policy.get("engine_id") or "").strip().upper()
        if engine_id:
            out.append(engine_id)
    return sorted(set(out))


def _active_ranked_engine_ids_for_sleeve(*, sleeve_id: str, mode: str) -> List[str]:
    active = set(_active_runtime_engine_ids())
    ranked = set(_ranked_policy_engine_ids())
    return sorted(active & ranked)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_ranked_engine_candidate_basis_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--produced_utc", default="")
    ap.add_argument("--truth_root", default=str(GLOBAL_TRUTH_ROOT))
    ap.add_argument("--sleeve_id", default="PRIMARY")
    ap.add_argument("--mode", default="PAPER")
    ap.add_argument("--engine_id", action="append", default=[])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = str(args.produced_utc).strip() or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    truth_root = Path(str(args.truth_root).strip() or str(GLOBAL_TRUTH_ROOT)).resolve()
    explicit_engine_ids = sorted({str(engine_id).strip().upper() for engine_id in (args.engine_id or []) if str(engine_id).strip()})
    engine_ids = explicit_engine_ids or _active_ranked_engine_ids_for_sleeve(
        sleeve_id=str(args.sleeve_id).strip(),
        mode=str(args.mode).strip(),
    )

    results: List[Dict[str, Any]] = []
    failures: List[str] = []
    for engine_id in engine_ids:
        try:
            basis = resolve_engine_candidate_basis(
                engine_id=engine_id,
                day_utc=day,
                repo_root=REPO_ROOT,
                truth_root=truth_root,
            )
            out_path = emit_engine_candidate_basis_report(
                engine_id=engine_id,
                day_utc=day,
                produced_utc=produced_utc,
                basis=basis,
                repo_root=REPO_ROOT,
                truth_root=truth_root,
                refreshable=True,
            )
            results.append(
                {
                    "engine_id": engine_id,
                    "status": "PASS",
                    "basis_day_utc": basis.basis_day_utc,
                    "candidate_symbol_count": len(basis.candidate_symbols),
                    "path": str(out_path),
                }
            )
        except EngineUniverseError as exc:
            failures.append(engine_id)
            results.append(
                {
                    "engine_id": engine_id,
                    "status": "FAIL",
                    "reason_code": str(exc),
                }
            )

    payload = {
        "name": "ranked_engine_candidate_basis_day_v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "truth_root": str(truth_root),
        "engine_ids": engine_ids,
        "failures": failures,
        "results": results,
        "status": "PASS" if not failures else "FAIL",
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
