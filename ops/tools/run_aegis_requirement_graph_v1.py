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

from constellation_2.common.paper_session_fact_plane_v1 import (
    collect_intent_files_v1,
    parse_day_utc_v1,
    read_json_object_v1,
    resolve_paper_intent_truth_root_v1,
)
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools import run_options_chain_snapshot_required_day_v1 as options_required
from ops.tools.run_intent_arbitration_v1 import intent_arbitration_path, selected_intent_pointer_path

SCHEMA_VERSION = "aegis_requirement_graph.v1"
SOURCE_ACTIVE_INTENT = "ACTIVE_INTENT"
SOURCE_LIFECYCLE_PHASE = "LIFECYCLE_PHASE"
SOURCE_OPERATOR_INPUT = "OPERATOR_INPUT_CONTRACT"
SOURCE_BROKER_AUTHORITY = "BROKER_AUTHORITY"
SOURCE_STATIC_POLICY = "STATIC_POLICY"
DEFAULT_FRESHNESS_POLICY = "same_day_required; stale/contradictory/missing artifact => BLOCKED_OR_STALE"


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


def requirement_graph_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_requirement_graph_v1" / day_utc / "requirement_graph.v1.json").resolve()


def _is_option_intent(payload: dict[str, Any]) -> bool:
    option = payload.get("option")
    exposure_type = str(payload.get("exposure_type") or "").strip().upper()
    risk_class = str(payload.get("risk_class") or "").strip().upper()
    return isinstance(option, dict) or exposure_type in {"SHORT_VOL_DEFINED", "VOL_INCOME_DEFINED"} or "DEFINED" in risk_class


def _intent_symbol(payload: dict[str, Any]) -> str:
    underlying = payload.get("underlying")
    if isinstance(underlying, dict):
        return str(underlying.get("symbol") or "").strip().upper()
    if isinstance(underlying, str):
        return underlying.strip().upper()
    instrument = payload.get("instrument")
    if isinstance(instrument, dict):
        return str(instrument.get("symbol") or "").strip().upper()
    return str(payload.get("symbol") or "").strip().upper()


def _discover_active_intents(*, truth_root: Path, execution_root: Path, day_utc: str) -> list[dict[str, Any]]:
    pointer_path = selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)
    if pointer_path.exists() and pointer_path.is_file():
        pointer = _read_json(pointer_path)
        if str(pointer.get("status") or "").strip().upper() != "SELECTED":
            return []
        selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
        intent_path_text = str(selected.get("intent_path") or "").strip()
        path = Path(intent_path_text).expanduser().resolve() if intent_path_text else None
        if path is not None and path.exists() and path.is_file():
            payload = read_json_object_v1(path)
            intent_id = str(payload.get("intent_id") or selected.get("intent_id") or path.stem).strip()
            symbol = _intent_symbol(payload)
            return [
                {
                    "intent_id": intent_id,
                    "intent_path": str(path),
                    "instrument": symbol,
                    "requires_options": _is_option_intent(payload) and bool(symbol),
                    "risk_class": str(payload.get("risk_class") or "").strip(),
                    "exposure_type": str(payload.get("exposure_type") or "").strip(),
                }
            ]
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=execution_root, repo_root=REPO_ROOT)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for root in (intent_truth_root, truth_root):
        for path in collect_intent_files_v1(truth_root=root, day_utc=day_utc):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            try:
                payload = read_json_object_v1(path)
            except ValueError:
                continue
            intent_id = str(payload.get("intent_id") or payload.get("id") or path.stem).strip()
            symbol = _intent_symbol(payload)
            out.append(
                {
                    "intent_id": intent_id,
                    "intent_path": str(path.resolve()),
                    "instrument": symbol,
                    "requires_options": _is_option_intent(payload) and bool(symbol),
                    "risk_class": str(payload.get("risk_class") or "").strip(),
                    "exposure_type": str(payload.get("exposure_type") or "").strip(),
                }
            )
    return sorted(out, key=lambda row: (str(row.get("intent_id") or ""), str(row.get("intent_path") or "")))


def _latest_capture_diagnostic(*, execution_root: Path, day_utc: str, symbol: str) -> Path | None:
    root = execution_root / "reports" / "options_chain_capture_ib_day_v1" / day_utc
    if not root.exists() or not root.is_dir():
        return None
    candidates = [
        path.resolve()
        for path in root.glob(f"ib_capture_{symbol.upper()}_*/options_chain_capture_diagnostic.v1.json")
        if path.is_file()
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda path: path.stat().st_mtime)
    return candidates[-1]


def _capture_blocker_from_diagnostic(path: Path | None) -> tuple[str, str]:
    if path is None:
        return "", ""
    result = options_required._classify_options_capture_failure(
        {"return_code": 2, "stdout": "", "stderr": f"FAIL: DIAG_PATH={path}"}
    )
    return str(result.get("reason_code") or "").strip(), str(result.get("capture_error") or "").strip()


def _snapshot_paths(*, execution_root: Path, day_utc: str, symbol: str) -> tuple[Path, Path | None, Path | None]:
    root = (execution_root / "options_chain_snapshot_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return root, None, None
    for path in sorted(root.rglob("options_chain_snapshot.v1.json")):
        payload = _read_json(path)
        underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
        observed = str(underlying.get("symbol") or payload.get("symbol") or "").strip().upper()
        if observed == symbol.upper():
            cert = path.parent / "freshness_certificate.v1.json"
            return root, path.resolve(), cert.resolve()
    return root, None, None


def _node(
    *,
    requirement_id: str,
    owner_phase: str,
    source_type: str,
    source_id: str,
    instrument: str,
    required_artifact: str,
    expected_path: Path,
    producer_command: str,
    consumer: str,
    status: str,
    blocker: str = "",
    blocker_detail: str = "",
    schema_path: str = "",
    freshness_policy: str = DEFAULT_FRESHNESS_POLICY,
    blocking_class: str = "HARD_BLOCKER",
    dependencies: list[str] | None = None,
    downstream_consequences: list[str] | None = None,
    operator_next_action: str = "",
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "owner_phase": owner_phase,
        "source_type": source_type,
        "source_id": source_id,
        "instrument": instrument,
        "required_artifact": required_artifact,
        "expected_path": str(expected_path),
        "schema_path": schema_path,
        "producer_command": producer_command,
        "consumer": consumer,
        "freshness_policy": freshness_policy,
        "blocking_class": blocking_class,
        "dependencies": dependencies or [],
        "status": status,
        "blocker": blocker,
        "canonical_blocker": blocker,
        "blocker_detail": blocker_detail,
        "downstream_consequences": downstream_consequences or [],
        "operator_next_action": operator_next_action,
    }


def _freshness_blocker(path: Path, *, day_utc: str) -> str:
    payload = _read_json(path)
    if not payload:
        return ""
    payload_day = str(
        payload.get("day_utc")
        or payload.get("trading_day")
        or payload.get("business_day")
        or payload.get("target_day")
        or ""
    ).strip()
    if payload_day and payload_day != day_utc:
        return "STALE_ARTIFACT"
    for key in ("freshness_status", "freshness_verdict", "lineage_status", "stale_artifact_status"):
        if str(payload.get(key) or "").strip().upper() in {"STALE", "STALE_ARTIFACT"}:
            return "STALE_ARTIFACT"
    if str(payload.get("canonical_blocker") or "").strip().upper() == "STALE_ARTIFACT":
        return "STALE_ARTIFACT"
    return ""


def _status_for_path(path: Path, *, day_utc: str) -> str:
    if not path.exists():
        return "BLOCKED"
    if path.is_file() and _freshness_blocker(path, day_utc=day_utc):
        return "STALE"
    return "SATISFIED"


def _path_blocker(path: Path, blocker: str, *, day_utc: str) -> str:
    stale = _freshness_blocker(path, day_utc=day_utc) if path.exists() and path.is_file() else ""
    if stale:
        return stale
    return "" if path.exists() else blocker


def _path_action(path: Path, action: str, *, day_utc: str) -> str:
    if path.exists() and path.is_file() and _freshness_blocker(path, day_utc=day_utc):
        return f"Regenerate stale artifact at {path} for {day_utc}."
    return "" if path.exists() else action


def _option_requirement_nodes(
    *,
    day_utc: str,
    execution_root: Path,
    intent: dict[str, Any],
) -> list[dict[str, Any]]:
    symbol = str(intent.get("instrument") or "").strip().upper()
    source_id = str(intent.get("intent_id") or "").strip()
    root, snapshot_path, cert_path = _snapshot_paths(execution_root=execution_root, day_utc=day_utc, symbol=symbol)
    diagnostic_path = _latest_capture_diagnostic(execution_root=execution_root, day_utc=day_utc, symbol=symbol)
    capture_blocker, capture_detail = _capture_blocker_from_diagnostic(diagnostic_path)
    missing_blocker = capture_blocker or "OPTIONS_CHAIN_SNAPSHOT_MISSING"
    detail = capture_detail or (f"latest_capture_diagnostic={diagnostic_path}" if diagnostic_path else "no current-day options snapshot or capture diagnostic")
    action = (
        f"Restore IB market-data permissions for {symbol} options/underlying and rerun "
        f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}"
        if missing_blocker == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
        else f"Run python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}"
    )
    snapshot_status = "SATISFIED" if snapshot_path is not None else "BLOCKED"
    cert_status = "SATISFIED" if cert_path is not None and cert_path.exists() else "BLOCKED"
    consequences = ["market_data_authority", "strategy_decision_authority", "risk_sizing_authority", "submit_boundary"]
    return [
        _node(
            requirement_id=f"MARKET_DATA:{source_id}:{symbol}:UNDERLYING_SPOT",
            owner_phase="MARKET_DATA",
            source_type=SOURCE_ACTIVE_INTENT,
            source_id=source_id,
            instrument=symbol,
            required_artifact="underlying_spot",
            expected_path=snapshot_path or root,
            producer_command=f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}",
            consumer="options_chain_snapshot_required_day_v1",
            status="SATISFIED" if snapshot_path is not None else "BLOCKED",
            blocker="" if snapshot_path is not None else missing_blocker,
            blocker_detail="" if snapshot_path is not None else detail,
            downstream_consequences=consequences,
            operator_next_action="" if snapshot_path is not None else action,
        ),
        _node(
            requirement_id=f"MARKET_DATA:{source_id}:{symbol}:OPTION_CHAIN",
            owner_phase="MARKET_DATA",
            source_type=SOURCE_ACTIVE_INTENT,
            source_id=source_id,
            instrument=symbol,
            required_artifact="option_chain",
            expected_path=snapshot_path or root,
            producer_command=f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}",
            consumer="market_data_authority_v1",
            status=snapshot_status,
            blocker="" if snapshot_status == "SATISFIED" else missing_blocker,
            blocker_detail="" if snapshot_status == "SATISFIED" else detail,
            downstream_consequences=consequences,
            operator_next_action="" if snapshot_status == "SATISFIED" else action,
        ),
        _node(
            requirement_id=f"MARKET_DATA:{source_id}:{symbol}:BID_ASK_QUOTES",
            owner_phase="MARKET_DATA",
            source_type=SOURCE_ACTIVE_INTENT,
            source_id=source_id,
            instrument=symbol,
            required_artifact="bid_ask_quotes",
            expected_path=snapshot_path or root,
            producer_command=f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}",
            consumer="risk_sizing_authority_v1",
            status=snapshot_status,
            blocker="" if snapshot_status == "SATISFIED" else missing_blocker,
            blocker_detail="" if snapshot_status == "SATISFIED" else detail,
            downstream_consequences=consequences,
            operator_next_action="" if snapshot_status == "SATISFIED" else action,
        ),
        _node(
            requirement_id=f"MARKET_DATA:{source_id}:{symbol}:FRESHNESS_CERTIFICATE",
            owner_phase="MARKET_DATA",
            source_type=SOURCE_ACTIVE_INTENT,
            source_id=source_id,
            instrument=symbol,
            required_artifact="freshness_certificate",
            expected_path=cert_path or (root / "*/freshness_certificate.v1.json"),
            producer_command=f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}",
            consumer="market_data_authority_v1",
            status=cert_status,
            blocker="" if cert_status == "SATISFIED" else missing_blocker,
            blocker_detail="" if cert_status == "SATISFIED" else detail,
            downstream_consequences=consequences,
            operator_next_action="" if cert_status == "SATISFIED" else action,
        ),
        _node(
            requirement_id=f"MARKET_DATA:{source_id}:{symbol}:OPTIONS_SNAPSHOT",
            owner_phase="MARKET_DATA",
            source_type=SOURCE_ACTIVE_INTENT,
            source_id=source_id,
            instrument=symbol,
            required_artifact="options_snapshot_artifact",
            expected_path=snapshot_path or (root / "*/options_chain_snapshot.v1.json"),
            producer_command=f"python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc}",
            consumer="market_data_authority_v1",
            status=snapshot_status,
            blocker="" if snapshot_status == "SATISFIED" else missing_blocker,
            blocker_detail="" if snapshot_status == "SATISFIED" else detail,
            downstream_consequences=consequences,
            operator_next_action="" if snapshot_status == "SATISFIED" else action,
        ),
    ]


def _defined_risk_requirement_node(*, day_utc: str, execution_root: Path, intent: dict[str, Any]) -> dict[str, Any]:
    source_id = str(intent.get("intent_id") or "").strip()
    symbol = str(intent.get("instrument") or "").strip().upper()
    expected = execution_root / "phaseC_preflight_v1" / day_utc
    exists = expected.exists()
    return _node(
        requirement_id=f"AUTHORIZATION:{source_id}:{symbol}:DEFINED_RISK_PHASEC",
        owner_phase="AUTHORIZATION",
        source_type=SOURCE_ACTIVE_INTENT,
        source_id=source_id,
        instrument=symbol,
        required_artifact="phasec_defined_risk_evidence",
        expected_path=expected,
        producer_command=f"python3 ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {day_utc}",
        consumer="authorization_artifacts_day_v1",
        status="SATISFIED" if exists else "BLOCKED",
        blocker="" if exists else "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE",
        blocker_detail="" if exists else f"defined-risk PhaseC evidence missing for active intent {source_id}",
        downstream_consequences=["authorization_artifacts", "submit_boundary"],
        operator_next_action="" if exists else f"Create defined-risk PhaseC evidence via python3 ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {day_utc}",
    )


def _lifecycle_nodes(ctx: bod.BodContext) -> list[dict[str, Any]]:
    day = ctx.day_utc
    truth = ctx.truth_root
    execution = ctx.execution_root
    operator = ctx.operator_input_root
    freshness = _read_json(truth / "reports" / "truth_freshness_v1" / day / "truth_freshness.v1.json")
    freshness_by_path = {
        str(row.get("artifact_path") or ""): row
        for row in (freshness.get("freshness_records") if isinstance(freshness.get("freshness_records"), list) else [])
        if isinstance(row, dict)
    }
    items = [
        ("BROKER_HEALTH", SOURCE_BROKER_AUTHORITY, "broker_event_log", execution / "execution_evidence_v1" / "broker_events" / day, "ops/ib/c2_execution_observer_v1.py", "run_aegis_pre_open_verify_v1.py", "BROKER_EVENT_LOG_MISSING"),
        ("BOD_INPUTS", SOURCE_OPERATOR_INPUT, "paper_capital_seed", bod.resolve_paper_capital_seed_path(operator_input_root=operator, day_utc=day), f"python3 ops/tools/ensure_paper_capital_seed_v1.py --day_utc {day}", "paper_session_bootstrap_v1", "PAPER_CAPITAL_SEED_MISSING"),
        ("BOD_INPUTS", SOURCE_OPERATOR_INPUT, "operator_statement", bod.resolve_operator_statement_path(operator_input_root=operator, day_utc=day), f"python3 ops/tools/ensure_cash_ledger_operator_statement_v1.py --day_utc {day}", "paper_session_bootstrap_v1", "OPERATOR_STATEMENT_MISSING"),
        ("BOD_INPUTS", SOURCE_LIFECYCLE_PHASE, "pre_open_bundle", truth / "reports" / "pre_open_bundle_v1" / day / "pre_open_bundle.v1.json", f"python3 ops/tools/run_pre_open_materializer_v1.py --day_utc {day}", "paper_session_authority_v1", "PRE_OPEN_BUNDLE_INCOMPLETE"),
        ("SESSION_AUTHORITY", SOURCE_LIFECYCLE_PHASE, "session_authority", truth / "reports" / "paper_session_authority_v1" / day / "paper_session_authority.v1.json", f"python3 ops/tools/run_paper_session_bootstrap_v1.py --day_utc {day}", "paper_trading_day_authority_v1", "SESSION_AUTHORITY_MISSING"),
        ("MARKET_DATA", SOURCE_LIFECYCLE_PHASE, "market_data_authority", truth / "reports" / "market_data_authority_v1" / day / "market_data_authority.v1.json", f"python3 ops/tools/run_market_data_authority_v1.py --day_utc {day}", "strategy_decision_authority_v1", "MARKET_DATA_AUTHORITY_BLOCKED"),
        ("STRATEGY_AND_RISK", SOURCE_LIFECYCLE_PHASE, "portfolio_nav_evidence", truth / "reports" / "portfolio_account_authority_v1" / day / "portfolio_account_authority.v1.json", f"python3 ops/tools/run_portfolio_account_authority_v1.py --day_utc {day}", "risk_sizing_authority_v1", "PORTFOLIO_ACCOUNT_AUTHORITY_BLOCKED"),
        ("STRATEGY_AND_RISK", SOURCE_LIFECYCLE_PHASE, "capital_risk_envelope", execution / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json", f"python3 ops/tools/run_c2_capital_risk_envelope_gate_v2.py --out_day_utc {day}", "risk_sizing_authority_v1", "CAPITAL_RISK_ENVELOPE_MISSING"),
        ("AUTHORIZATION", SOURCE_LIFECYCLE_PHASE, "phasec_defined_risk_evidence", execution / "phaseC_preflight_v1" / day, f"python3 ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {day}", "authorization_artifacts_day_v1", "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE"),
        ("AUTHORIZATION", SOURCE_LIFECYCLE_PHASE, "authorization_evidence", execution / "engine_activity_v1" / "authorization_v1" / day, f"python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc {day}", "submit_boundary_status_v1", "AUTHORIZATION_EVIDENCE_MISSING"),
        ("STRATEGY_AND_RISK", SOURCE_LIFECYCLE_PHASE, "risk_sizing", truth / "reports" / "risk_sizing_authority_v1" / day / "risk_sizing_authority.v1.json", f"python3 ops/tools/run_risk_sizing_authority_v1.py --day_utc {day}", "trade_submit_readiness_c2_v1", "RISK_SIZING_AUTHORITY_BLOCKED"),
        ("SUBMIT_BOUNDARY", SOURCE_LIFECYCLE_PHASE, "submit_boundary", truth / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json", f"python3 ops/tools/run_submit_boundary_status_v1.py --day_utc {day}", "aegis_paper_ready_v1", "SUBMIT_BOUNDARY_NOT_AUTHORIZED"),
    ]
    nodes: list[dict[str, Any]] = []
    for owner_phase, source_type, artifact, path, command, consumer, blocker in items:
        status = _status_for_path(path, day_utc=day)
        canonical_blocker = _path_blocker(path, blocker, day_utc=day)
        fresh_row = freshness_by_path.get(str(path.resolve()))
        if fresh_row and str(fresh_row.get("freshness_status") or "") in {"STALE", "EXPIRED", "UNKNOWN"}:
            if str(fresh_row.get("blocking_class") or "") == "HARD_BLOCKER":
                status = "STALE" if fresh_row.get("freshness_status") != "UNKNOWN" else "BLOCKED"
                canonical_blocker = str(fresh_row.get("canonical_blocker") or "TRUTH_FRESHNESS_UNKNOWN")
        nodes.append(
            _node(
                requirement_id=f"{owner_phase}:{artifact}",
                owner_phase=owner_phase,
                source_type=source_type,
                source_id=owner_phase,
                instrument="",
                required_artifact=artifact,
                expected_path=path,
                producer_command=command,
                consumer=consumer,
                status=status,
                blocker=canonical_blocker,
                blocker_detail="" if status == "SATISFIED" else f"expected artifact missing or stale at {path}",
                downstream_consequences=[],
                operator_next_action=str((fresh_row or {}).get("operator_next_action") or "") or _path_action(path, f"Run {command}", day_utc=day),
            )
        )
    return nodes


def _root_requirement(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    phase_rank = {
        "SOURCE_INTEGRITY": 0,
        "BROKER_HEALTH": 1,
        "BOD_INPUTS": 2,
        "SESSION_AUTHORITY": 3,
        "MARKET_DATA": 4,
        "STRATEGY_AND_RISK": 5,
        "AUTHORIZATION": 6,
        "SUBMIT_BOUNDARY": 7,
        "PAPER_READY": 8,
    }
    blocked = [node for node in nodes if node.get("status") in {"BLOCKED", "STALE"} and node.get("blocking_class") == "HARD_BLOCKER"]
    if not blocked:
        return {}
    blocked.sort(key=lambda node: (phase_rank.get(str(node.get("owner_phase") or ""), 99), str(node.get("requirement_id") or "")))
    return blocked[0]


def build_requirement_graph(ctx: bod.BodContext) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    nodes.extend(_lifecycle_nodes(ctx))
    arbitration_path = intent_arbitration_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    pointer_path = selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    if not arbitration_path.exists() and not pointer_path.exists():
        nodes.append(
            _node(
                requirement_id="STRATEGY_AND_RISK:intent_arbitration",
                owner_phase="STRATEGY_AND_RISK",
                source_type=SOURCE_LIFECYCLE_PHASE,
                source_id="INTENT_ARBITRATION",
                instrument="",
                required_artifact="selected_intent_pointer",
                expected_path=pointer_path,
                producer_command=f"python3 ops/tools/run_intent_arbitration_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
                consumer="aegis_requirement_graph_v1",
                status="BLOCKED",
                blocker="INTENT_ARBITRATION_MISSING",
                blocker_detail=f"missing arbitration={arbitration_path} selected_pointer={pointer_path}",
                operator_next_action="Run sleeve evaluation and intent arbitration for the current day.",
            )
        )
        intents = _discover_active_intents(truth_root=ctx.truth_root, execution_root=ctx.execution_root, day_utc=ctx.day_utc)
    elif pointer_path.exists() and pointer_path.is_file() and str(_read_json(pointer_path).get("status") or "").strip().upper() != "SELECTED":
        pointer = _read_json(pointer_path)
        blocker = str(pointer.get("canonical_blocker") or pointer.get("status") or "NO_EXECUTABLE_INTENT").strip()
        nodes.append(
            _node(
                requirement_id="STRATEGY_AND_RISK:selected_intent",
                owner_phase="STRATEGY_AND_RISK",
                source_type=SOURCE_LIFECYCLE_PHASE,
                source_id="INTENT_ARBITRATION",
                instrument="",
                required_artifact="selected_intent_pointer",
                expected_path=pointer_path,
                producer_command=f"python3 ops/tools/run_market_session_intent_engine_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment} --once",
                consumer="aegis_requirement_graph_v1",
                status="BLOCKED",
                blocker=blocker,
                blocker_detail=f"selected intent pointer status={pointer.get('status')} blocker={blocker}",
                operator_next_action="Review sleeve scan outcomes and resolve arbitration blocker or accept no-trade day.",
            )
        )
        intents = []
    else:
        intents = _discover_active_intents(truth_root=ctx.truth_root, execution_root=ctx.execution_root, day_utc=ctx.day_utc)
    for intent in intents:
        if intent.get("requires_options"):
            nodes.extend(_option_requirement_nodes(day_utc=ctx.day_utc, execution_root=ctx.execution_root, intent=intent))
            nodes.append(_defined_risk_requirement_node(day_utc=ctx.day_utc, execution_root=ctx.execution_root, intent=intent))
    root = _root_requirement(nodes)
    status = "STALE" if str(root.get("status") or "") == "STALE" else ("BLOCKED" if root else "PASS")
    payload = {
        "schema_id": "aegis_requirement_graph",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
        "active_intents": intents,
        "requirements": nodes,
        "status": status,
        "root_requirement": root,
        "canonical_blocker": str(root.get("blocker") or "") if root else "",
        "operator_next_action": str(root.get("operator_next_action") or "") if root else "",
    }
    return payload


def run_requirement_graph_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_requirement_graph(ctx)
    path = requirement_graph_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_REQUIREMENT_GRAPH_COLLISION: {path}")
    input_paths = [row.get("intent_path") for row in payload.get("active_intents", []) if isinstance(row, dict)]
    input_paths.extend(row.get("expected_path") for row in payload.get("requirements", []) if isinstance(row, dict))
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_aegis_requirement_graph_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_requirement_graph_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=[str(path) for path in input_paths if str(path or "").strip()],
        output_artifacts=[path],
        schema_versions={"requirement_graph": SCHEMA_VERSION},
    )
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_requirement_graph_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_requirement_graph_v1(day_utc, environment, str(args.truth_root or ""))
    print(
        json.dumps(
            {
                "status": payload["status"],
                "canonical_blocker": payload["canonical_blocker"],
                "requirement_graph_path": str(path),
                "root_requirement_id": str((payload.get("root_requirement") or {}).get("requirement_id") or ""),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
