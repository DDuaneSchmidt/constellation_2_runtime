from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def daily_operator_summary_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_daily_operator_summary_v1"
        / day_utc
        / "aegis_daily_operator_summary.v1.json"
    ).resolve()


def _intent_count(truth_root: Path, day_utc: str) -> int:
    path = truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json"
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return 0
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


def _submission_count(execution_root: Path, day_utc: str) -> int:
    root = execution_root / "execution_evidence_v1" / "submissions" / day_utc
    if not root.exists() or not root.is_dir():
        return 0
    return len([p for p in root.iterdir() if p.is_dir() and not p.name.startswith("_")])


def build_aegis_daily_operator_summary_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    operating_contract: dict[str, Any],
    authority_graph: dict[str, Any],
    evidence_ledger: dict[str, Any],
    produced_utc: str | None = None,
) -> dict[str, Any]:
    mode = str(operating_contract.get("mode") or "DRY_RUN").strip().upper()
    outcome = str(evidence_ledger.get("final_daily_outcome") or evidence_ledger.get("no_silent_day_outcome") or "").strip()
    first_blocker = {}
    blockers = evidence_ledger.get("blockers")
    if isinstance(blockers, list) and blockers:
        first = blockers[0]
        if isinstance(first, dict):
            first_blocker = dict(first)
    if not first_blocker:
        graph_blockers = authority_graph.get("blocking_nodes")
        if isinstance(graph_blockers, list) and graph_blockers:
            first = graph_blockers[0]
            if isinstance(first, dict):
                first_blocker = {
                    "code": "AUTHORITY_BLOCKED",
                    "owner": first.get("owner") or first.get("authority_name") or "",
                    "path": first.get("artifact_path") or "",
                    "producer_command": first.get("producer_command"),
                }
    submit_boundary_path = truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    submit_boundary = _read_json(submit_boundary_path) or {}
    closure_path = truth_root / "reports" / "trading_day_closure_authority_v1" / day_utc / "trading_day_closure_authority.v1.json"
    closure = _read_json(closure_path) or {}
    intent_count = _intent_count(truth_root, day_utc)
    submission_count = _submission_count(execution_root, day_utc)
    broker_transmit_enabled = bool(
        submit_boundary.get("broker_transmit_enabled") is True
        or any(
            str((node or {}).get("authority_name") or "") == "execution_mode_authority_v1"
            and str((node or {}).get("observed_state") or "").upper() in {"TRANSMIT_ENABLED", "PAPER_TRANSMIT", "LIVE"}
            for node in authority_graph.get("authority_nodes", [])
            if isinstance(node, dict)
        )
    )
    broker_order_transmitted = bool(submit_boundary.get("broker_order_transmitted") is True)
    did_trade = bool(submission_count > 0)
    return {
        "schema_id": "aegis_daily_operator_summary",
        "schema_version": "v1",
        "day_utc": day_utc,
        "mode": mode,
        "run_style": operating_contract.get("run_style"),
        "no_silent_day_outcome": outcome,
        "did_aegis_trade_today": did_trade,
        "why_not": "" if did_trade else (first_blocker.get("code") or outcome or "NO_SUBMISSION_EVIDENCE"),
        "dry_run": bool(mode == "DRY_RUN" or submit_boundary.get("dry_run_policy") == "YES"),
        "transmitted": broker_order_transmitted,
        "intents_created": bool(intent_count > 0),
        "intent_count": intent_count,
        "intents_intentionally_absent": bool(outcome == "NO_INTENT_EXPECTED"),
        "submissions_created": bool(submission_count > 0),
        "submission_count": submission_count,
        "broker_orders_transmitted": broker_order_transmitted,
        "broker_transmit_enabled": broker_transmit_enabled,
        "closure_clean": bool(str(closure.get("status") or "").upper() in {"PASS", "WARN"}),
        "closure_status": str(closure.get("closure_state") or closure.get("status") or "UNKNOWN"),
        "first_blocker": first_blocker,
        "first_blocker_owner": first_blocker.get("owner") or first_blocker.get("owning_subsystem") or "",
        "fix_command": first_blocker.get("producer_command"),
        "evidence_paths": {
            "operating_contract": str(truth_root / "reports" / "aegis_operating_contract_v1" / day_utc / "aegis_operating_contract.v1.json"),
            "authority_graph": str(truth_root / "reports" / "aegis_authority_graph_v1" / day_utc / "aegis_authority_graph.v1.json"),
            "evidence_ledger": str(truth_root / "reports" / "aegis_day_evidence_ledger_v1" / day_utc / "aegis_day_evidence_ledger.v1.json"),
            "submit_boundary": str(submit_boundary_path),
            "trading_day_closure_authority": str(closure_path),
            "execution_submissions": str(execution_root / "execution_evidence_v1" / "submissions" / day_utc),
        },
        "produced_utc": produced_utc or _utc_now_iso(),
    }


def write_aegis_daily_operator_summary_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    path = daily_operator_summary_output_path(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path
