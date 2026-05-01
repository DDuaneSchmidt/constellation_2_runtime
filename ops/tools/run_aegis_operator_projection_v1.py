#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1

SCHEMA_VERSION = "aegis_operator_projection.v1"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def operator_projection_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_operator_projection_v1" / day_utc / "operator_projection.v1.json").resolve()


def _report_path(ctx: bod.BodContext, family: str, filename: str) -> Path:
    return (ctx.truth_root / "reports" / family / ctx.day_utc / filename).resolve()


def _artifact_paths(ctx: bod.BodContext, blocker: str, graph: dict[str, Any]) -> list[str]:
    paths = [
        str(_report_path(ctx, "aegis_day_run_v1", "day_run.v1.json")),
        str(_report_path(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json")),
    ]
    if blocker == "SOURCE_REPRODUCIBILITY_BLOCKED":
        paths.append(str((bod.PROTECTION_STATUS_PATH if hasattr(bod, "PROTECTION_STATUS_PATH") else Path("/home/node/constellation_runtime_data/repo_protection_v1/status.json")).resolve()))
    if blocker.startswith("MARKET") or blocker.startswith("OPTIONS") or blocker in {"MARKET_DATA_BLOCKED", "MARKET_DATA_AUTHORITY_BLOCKED"}:
        paths.extend(
            [
                str(_report_path(ctx, "market_data_supply_v1", "market_data_supply.v1.json")),
                str(_report_path(ctx, "market_data_authority_v1", "market_data_authority.v1.json")),
                str(_report_path(ctx, "market_open_data_gate_v1", "market_open_data_gate.v1.json")),
            ]
        )
    if blocker == "NO_ELIGIBLE_OPTION_STRUCTURE":
        paths.extend(
            [
                str(_report_path(ctx, "authorization_supply_v1", "authorization_supply.v1.json")),
                str(_report_path(ctx, "structure_decision_supply_v1", "structure_decision_supply.v1.json")),
                str(_report_path(ctx, "option_structure_authorization_diagnostics_v1", "option_structure_authorization_diagnostics.v1.json")),
            ]
        )
    root = graph.get("root_requirement") if isinstance(graph.get("root_requirement"), dict) else {}
    if root.get("expected_path"):
        paths.append(str(root["expected_path"]))
    return list(dict.fromkeys(path for path in paths if path))


def _projection_for(ctx: bod.BodContext, ledger: dict[str, Any], graph: dict[str, Any]) -> dict[str, Any]:
    final_status = str(ledger.get("final_status") or "UNKNOWN").strip().upper()
    phase = str(ledger.get("canonical_phase") or "").strip()
    blocker = str(ledger.get("canonical_blocker") or "").strip()
    owner = phase or "UNKNOWN"
    root_cause = str((ledger.get("root_cause_chain") or [{}])[0].get("blocker_detail") or blocker or "No blocker") if isinstance(ledger.get("root_cause_chain"), list) else blocker
    next_actions: list[str]
    unsafe_actions = ["Do not submit orders outside the day-run ledger final readiness authority."]
    if blocker == "SOURCE_REPRODUCIBILITY_BLOCKED":
        owner = "source_reproducibility_authority"
        root_cause = "Canonical repo is dirty or not protected, so current source cannot be reproduced."
        next_actions = ["Clean/protect the canonical repo.", "Rerun python3 ops/tools/run_aegis_day_v1.py --day_utc " + ctx.day_utc + " --environment PAPER."]
        unsafe_actions.append("Do not treat downstream readiness artifacts as final while source integrity is blocked.")
    elif blocker.startswith("MARKET") or blocker.startswith("OPTIONS") or blocker == "MARKET_DATA_BLOCKED":
        owner = "market_data_authority"
        root_cause = "Market data authority or supply artifacts are missing, stale, or blocked."
        next_actions = ["Inspect market_data_supply_v1 and market_data_authority_v1.", "Rerun governed market-data capture/supply commands for " + ctx.day_utc + "."]
    elif blocker == "NO_ELIGIBLE_OPTION_STRUCTURE":
        owner = "authorization_supply"
        root_cause = "Authorization exists but no governed option structure is currently eligible."
        next_actions = ["Inspect authorization_supply_v1 and option-structure diagnostics.", "Keep submit blocked unless governed market conditions or policy produce an eligible structure."]
    elif blocker:
        next_actions = [str(ledger.get("operator_next_action") or "Resolve the canonical ledger blocker and rerun the day.")]
    else:
        next_actions = ["No blocker reported by the day-run ledger."]
    return {
        "schema_id": "aegis_operator_projection",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": "PASS" if final_status not in {"UNKNOWN", "NOT_READY"} else "BLOCKED",
        "canonical_blocker": blocker,
        "operator_next_action": next_actions[0],
        "final_status": final_status,
        "first_blocker": blocker,
        "owner": owner,
        "phase": phase,
        "root_cause": root_cause,
        "downstream_consequences": ledger.get("downstream_consequences") if isinstance(ledger.get("downstream_consequences"), list) else [],
        "artifact_paths": _artifact_paths(ctx, blocker, graph),
        "next_valid_actions": next_actions,
        "unsafe_actions": unsafe_actions,
        "confidence_in_diagnosis": "HIGH" if blocker else "MEDIUM",
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection is explanatory only; aegis_day_run_ledger_v1 remains final readiness authority.",
    }


def run_operator_projection_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    ledger_path = _report_path(ctx, "aegis_day_run_v1", "day_run.v1.json")
    graph_path = _report_path(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json")
    payload = _projection_for(ctx, _read_json(ledger_path), _read_json(graph_path))
    path = operator_projection_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_aegis_operator_projection_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_operator_projection_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=[ledger_path, graph_path],
        output_artifacts=[path],
        schema_versions={"aegis_operator_projection": SCHEMA_VERSION},
    )
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_operator_projection_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_operator_projection_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "operator_projection_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
