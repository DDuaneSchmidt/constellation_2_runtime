#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_authority_graph_v1 import build_aegis_authority_graph_v1, write_aegis_authority_graph_v1
from constellation_2.common.aegis_daily_operator_summary_v1 import (
    build_aegis_daily_operator_summary_v1,
    write_aegis_daily_operator_summary_v1,
)
from constellation_2.common.aegis_day_evidence_ledger_v1 import (
    finalize_aegis_day_evidence_ledger_v1,
    new_aegis_day_evidence_ledger_v1,
    record_command_v1,
    record_phase_v1,
    utc_now_iso,
    write_aegis_day_evidence_ledger_v1,
)
from constellation_2.common.aegis_operating_contract_v1 import build_aegis_operating_contract_v1, write_aegis_operating_contract_v1
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry


SCHEMAS = {
    "contract": "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_operating_contract.v1.schema.json",
    "graph": "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_authority_graph.v1.schema.json",
    "ledger": "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_day_evidence_ledger.v1.schema.json",
    "summary": "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_daily_operator_summary.v1.schema.json",
}


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _run(ledger: dict[str, Any], *, phase: str, name: str, cmd: list[str], env: dict[str, str]) -> dict[str, Any]:
    start = utc_now_iso()
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env, check=False)
    end = utc_now_iso()
    record_command_v1(
        ledger,
        phase=phase,
        name=name,
        command=cmd,
        start_utc=start,
        end_utc=end,
        exit_code=int(proc.returncode),
        stdout=proc.stdout,
        stderr=proc.stderr,
    )
    return {"name": name, "phase": phase, "return_code": int(proc.returncode), "stdout": proc.stdout, "stderr": proc.stderr}


def _intent_count(truth_root: Path, day_utc: str) -> int:
    path = truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json"
    payload = _read_json(path) or {}
    for key in ("intent_count", "output_count", "active_intent_count"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
    canonical_outputs = payload.get("canonical_outputs")
    if isinstance(canonical_outputs, dict):
        value = canonical_outputs.get("output_count")
        if isinstance(value, int):
            return value
        paths = canonical_outputs.get("intent_output_paths")
        if isinstance(paths, list):
            return len(paths)
    intents = payload.get("intents")
    return len(intents) if isinstance(intents, list) else 0


def _released_candidates(execution_root: Path, day_utc: str) -> list[Path]:
    root = execution_root / "phaseC_preflight_v1" / day_utc
    if not root.exists() or not root.is_dir():
        return []
    out: list[Path] = []
    for attempt_dir in sorted([p for p in root.iterdir() if p.is_dir()], reverse=True):
        for candidate in sorted([p for p in attempt_dir.iterdir() if p.is_dir()]):
            decision = _read_json(candidate / "submit_preflight_decision.v1.json") or {}
            if str(decision.get("decision") or "").upper() in {"ALLOW", "RELEASE", "RELEASED"}:
                out.append(candidate.resolve())
        if out:
            break
    return out


def _authority_state_map(graph: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for node in graph.get("authority_nodes", []):
        if isinstance(node, dict):
            out[str(node.get("authority_name") or "")] = str(node.get("observed_state") or node.get("status") or "UNKNOWN")
    return {k: v for k, v in out.items() if k}


def _first_blocker(*, commands: list[dict[str, Any]], graph: dict[str, Any]) -> dict[str, Any] | None:
    for command in commands:
        if int(command.get("return_code") or 0) != 0 and str(command.get("phase") or "") not in {"PACKET"}:
            return {
                "code": f"COMMAND_FAILED:{command.get('name')}",
                "owner": command.get("name"),
                "phase": command.get("phase"),
                "producer_command": " ".join(command.get("command") or []),
            }
    blockers = graph.get("blocking_nodes")
    if isinstance(blockers, list) and blockers:
        first = blockers[0]
        if isinstance(first, dict):
            return {
                "code": "AUTHORITY_BLOCKED",
                "owner": first.get("owner") or first.get("authority_name"),
                "path": first.get("artifact_path"),
                "producer_command": first.get("producer_command"),
            }
    return None


def _derive_outcome(*, truth_root: Path, execution_root: Path, day_utc: str, ledger: dict[str, Any], graph: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    submit_mode = classify_paper_submit_mode_status_v1(execution_root=execution_root, day_utc=day_utc)
    if str(submit_mode.get("submit_mode_status") or "").upper() == "DRY_RUN_COMPLETE":
        return "SUCCESS_DRY_RUN", []
    if submit_mode.get("broker_order_transmitted") is True:
        return "SUCCESS_TRANSMITTED", []
    if _intent_count(truth_root, day_utc) == 0:
        return "NO_INTENT_EXPECTED", []
    blocker = _first_blocker(commands=list(ledger.get("commands") or []), graph=graph)
    if blocker:
        if str(blocker.get("code") or "").startswith("COMMAND_FAILED:aegis_paper_submit"):
            return "FAILED_WITH_OWNER", [blocker]
        return "BLOCKED_WITH_REASON", [blocker]
    return "BLOCKED_WITH_REASON", [
        {
            "code": "NO_TERMINAL_SUCCESS_EVIDENCE",
            "owner": "aegis_paper_daily_v1",
            "producer_command": f"npm run aegis:paper:daily -- --day_utc {day_utc}",
        }
    ]


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_aegis_paper_daily_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--mode", default=os.environ.get("AEGIS_OPERATING_MODE", "DRY_RUN"), choices=["DRY_RUN", "PAPER_TRANSMIT", "LIVE"])
    parser.add_argument("--run_style", default=os.environ.get("AEGIS_RUN_STYLE", "MANUAL"), choices=["MANUAL", "AUTO"])
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(args.truth_root or "", repo_root=REPO_ROOT, caller="ops/tools/run_aegis_paper_daily_v1.py")
    account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_root = resolve_sleeve_execution_root_v1(repo_root=REPO_ROOT, environment="PAPER", ib_account=account, sleeve_id="PRIMARY").execution_root_path.resolve()
    env = dict(os.environ)
    if args.mode == "DRY_RUN":
        env.setdefault("C2_GOVERNED_SUBMIT_DRY_RUN", "YES")

    ledger = new_aegis_day_evidence_ledger_v1(day_utc=day_utc, mode=args.mode, run_style=args.run_style)

    record_phase_v1(ledger, phase="PRE_MARKET", status="STARTED")
    contract = build_aegis_operating_contract_v1(day_utc=day_utc, mode=args.mode, run_style=args.run_style)
    validate_against_repo_schema_v1(contract, REPO_ROOT, SCHEMAS["contract"])
    contract_path = write_aegis_operating_contract_v1(truth_root=truth_root, day_utc=day_utc, payload=contract)
    record_command_v1(ledger, phase="PRE_MARKET", name="aegis_operating_contract_v1", command=["internal", "build_aegis_operating_contract_v1"], start_utc=utc_now_iso(), end_utc=utc_now_iso(), exit_code=0, outputs=[str(contract_path)])

    graph = build_aegis_authority_graph_v1(day_utc=day_utc, truth_root=truth_root, execution_root=execution_root, repo_root=REPO_ROOT)
    ledger["authority_states_before"] = _authority_state_map(graph)
    validate_against_repo_schema_v1(graph, REPO_ROOT, SCHEMAS["graph"])
    graph_path = write_aegis_authority_graph_v1(truth_root=truth_root, day_utc=day_utc, payload=graph)
    record_command_v1(ledger, phase="PRE_MARKET", name="aegis_authority_graph_v1", command=["internal", "build_aegis_authority_graph_v1"], start_utc=utc_now_iso(), end_utc=utc_now_iso(), exit_code=0, outputs=[str(graph_path)])

    _run(ledger, phase="PRE_MARKET", name="aegis_paper_preflight", cmd=["npm", "run", "aegis:paper:preflight", "--", "--day_utc", day_utc], env=env)
    for name, cmd in (
        ("portfolio_account_authority_v1", [sys.executable, "ops/tools/run_portfolio_account_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
        ("market_data_authority_v1", [sys.executable, "ops/tools/run_market_data_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
        ("risk_sizing_authority_v1", [sys.executable, "ops/tools/run_risk_sizing_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
        (
            "runtime_service_authority_v1",
            [
                sys.executable,
                "ops/tools/run_runtime_service_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--expected_run_mode",
                "AUTOMATIC" if args.run_style == "AUTO" else args.run_style,
            ],
        ),
        ("execution_mode_authority_v1", [sys.executable, "ops/tools/run_execution_mode_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
    ):
        _run(ledger, phase="PRE_MARKET", name=name, cmd=cmd, env=env)
    record_phase_v1(ledger, phase="PRE_MARKET", status="COMPLETE")

    record_phase_v1(ledger, phase="INTENT", status="STARTED")
    _run(ledger, phase="INTENT", name="trading_day_intent_generation_v1", cmd=[sys.executable, "ops/tools/run_trading_day_intent_generation_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)], env=env)
    _run(ledger, phase="INTENT", name="strategy_decision_authority_v1", cmd=[sys.executable, "ops/tools/run_strategy_decision_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)], env=env)
    record_phase_v1(ledger, phase="INTENT", status="COMPLETE")

    record_phase_v1(ledger, phase="MATERIALIZATION", status="STARTED")
    _run(ledger, phase="MATERIALIZATION", name="phasec_identity_materializer_day_v1", cmd=[sys.executable, "ops/tools/run_phasec_identity_materializer_day_v1.py", "--day_utc", day_utc, "--eval_time_utc", f"{day_utc}T00:00:00Z", "--truth_root", str(truth_root), "--execution_truth_root", str(execution_root)], env=env)
    record_phase_v1(ledger, phase="MATERIALIZATION", status="COMPLETE")

    record_phase_v1(ledger, phase="SUBMIT", status="STARTED")
    submit_mode = classify_paper_submit_mode_status_v1(execution_root=execution_root, day_utc=day_utc)
    candidates = _released_candidates(execution_root, day_utc)
    if str(submit_mode.get("submit_mode_status") or "").upper() == "DRY_RUN_COMPLETE":
        record_phase_v1(ledger, phase="SUBMIT", status="SKIPPED", reason="DRY_RUN_ALREADY_COMPLETE")
    elif candidates:
        _run(ledger, phase="SUBMIT", name="aegis_paper_submit", cmd=["npm", "run", "aegis:paper:submit", "--", "--day_utc", day_utc], env=env)
    else:
        record_phase_v1(ledger, phase="SUBMIT", status="SKIPPED", reason="NO_RELEASED_PHASEC_CANDIDATE")
    record_phase_v1(ledger, phase="SUBMIT", status="COMPLETE")

    record_phase_v1(ledger, phase="POST_SUBMIT", status="STARTED")
    for name, cmd in (
        ("execution_lifecycle_authority_v1", [sys.executable, "ops/tools/run_execution_lifecycle_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
        ("trade_lineage_graph_v1", [sys.executable, "ops/tools/run_trade_lineage_graph_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
        ("execution_mode_authority_v1.final", [sys.executable, "ops/tools/run_execution_mode_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)]),
    ):
        _run(ledger, phase="POST_SUBMIT", name=name, cmd=cmd, env=env)
    record_phase_v1(ledger, phase="POST_SUBMIT", status="COMPLETE")

    record_phase_v1(ledger, phase="CLOSURE", status="STARTED")
    _run(ledger, phase="CLOSURE", name="trading_day_closure_authority_v1", cmd=[sys.executable, "ops/tools/run_trading_day_closure_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root)], env=env)
    record_phase_v1(ledger, phase="CLOSURE", status="COMPLETE")

    graph = build_aegis_authority_graph_v1(day_utc=day_utc, truth_root=truth_root, execution_root=execution_root, repo_root=REPO_ROOT)
    validate_against_repo_schema_v1(graph, REPO_ROOT, SCHEMAS["graph"])
    graph_path = write_aegis_authority_graph_v1(truth_root=truth_root, day_utc=day_utc, payload=graph)

    outcome, blockers = _derive_outcome(truth_root=truth_root, execution_root=execution_root, day_utc=day_utc, ledger=ledger, graph=graph)
    artifact_paths = {
        "aegis_operating_contract_v1": str(contract_path),
        "aegis_authority_graph_v1": str(graph_path),
    }
    ledger = finalize_aegis_day_evidence_ledger_v1(
        ledger,
        final_daily_outcome=outcome,
        blockers=blockers,
        diagnostics=[{"code": "PHASE_SKIPPED", **row} for row in ledger.get("phases", []) if row.get("status") == "SKIPPED"],
        artifact_paths=artifact_paths,
        authority_states_after=_authority_state_map(graph),
    )
    validate_against_repo_schema_v1(ledger, REPO_ROOT, SCHEMAS["ledger"])
    ledger_path = write_aegis_day_evidence_ledger_v1(truth_root=truth_root, day_utc=day_utc, payload=ledger)

    summary = build_aegis_daily_operator_summary_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        execution_root=execution_root,
        operating_contract=contract,
        authority_graph=graph,
        evidence_ledger=ledger,
    )
    validate_against_repo_schema_v1(summary, REPO_ROOT, SCHEMAS["summary"])
    summary_path = write_aegis_daily_operator_summary_v1(truth_root=truth_root, day_utc=day_utc, payload=summary)

    record_phase_v1(ledger, phase="PACKET", status="STARTED")
    _run(ledger, phase="PACKET", name="aegis_chatgpt_packet", cmd=["npm", "run", "aegis:chatgpt:packet"], env=env)
    _run(ledger, phase="PACKET", name="aegis_chatgpt_show", cmd=["npm", "run", "aegis:chatgpt:show"], env=env)
    record_phase_v1(ledger, phase="PACKET", status="COMPLETE")
    ledger["artifact_paths"].update(
        {
            "aegis_day_evidence_ledger_v1": str(ledger_path),
            "aegis_daily_operator_summary_v1": str(summary_path),
        }
    )
    validate_against_repo_schema_v1(ledger, REPO_ROOT, SCHEMAS["ledger"])
    ledger_path = write_aegis_day_evidence_ledger_v1(truth_root=truth_root, day_utc=day_utc, payload=ledger)

    print(
        json.dumps(
            {
                "day_utc": day_utc,
                "mode": args.mode,
                "run_style": args.run_style,
                "no_silent_day_outcome": outcome,
                "operating_contract_path": str(contract_path),
                "authority_graph_path": str(graph_path),
                "evidence_ledger_path": str(ledger_path),
                "operator_summary_path": str(summary_path),
                "first_blocker": blockers[0] if blockers else {},
            },
            sort_keys=True,
        )
    )
    return 0 if outcome in {"SUCCESS_DRY_RUN", "SUCCESS_TRANSMITTED", "NO_INTENT_EXPECTED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
