#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools import run_aegis_paper_ready_kernel_v1 as kernel

PASS = "PASS"
BLOCKED = "BLOCKED"
MISSING = "MISSING"
STALE = "STALE"

DOMAIN_ORDER = [
    "startup/materialization",
    "intent/arbitration",
    "market data",
    "structure",
    "authority/freshness",
    "exposure/risk/allocation",
    "executable authorization",
    "submit boundary",
    "broker/reconciliation",
]

STAGE_DOMAIN = {
    "broker_supply": "startup/materialization",
    "cash_ledger_from_broker": "startup/materialization",
    "positions_snapshot": "startup/materialization",
    "accounting_nav": "startup/materialization",
    "accounting_nav_compat_bridge": "startup/materialization",
    "capital_supply": "startup/materialization",
    "paper_startup_intent_input_convergence": "startup/materialization",
    "trading_day_intent_generation": "intent/arbitration",
    "portfolio_activation_gate": "intent/arbitration",
    "portfolio_scoring": "intent/arbitration",
    "intent_arbitration": "intent/arbitration",
    "aegis_requirement_graph": "market data",
    "market_open_data_gate": "market data",
    "market_data_supply": "market data",
    "structure_decision_supply": "structure",
    "paper_authority_pointer_refresh": "authority/freshness",
    "paper_authority_head_freshness": "authority/freshness",
    "risk_budget_supply": "exposure/risk/allocation",
    "exposure_net": "exposure/risk/allocation",
    "sleeve_edge_measurement": "exposure/risk/allocation",
    "governed_evaluation": "exposure/risk/allocation",
    "capital_authority_allocation": "exposure/risk/allocation",
    "phasec_identity_materializer": "executable authorization",
    "authorization_artifacts": "executable authorization",
    "authorization_supply": "executable authorization",
    "global_kill_switch": "submit boundary",
    "trading_day_readiness_authority": "submit boundary",
    "submit_boundary_status": "submit boundary",
}

SOURCE_FIXES = {
    "startup/materialization": [
        "Inspect the source artifact reader/validator for the failing startup surface.",
        "Confirm the expected artifact contract still matches the PAPER-ready kernel graph.",
    ],
    "intent/arbitration": [
        "Inspect intent generation and arbitration source contracts for missing selected-intent evidence.",
        "Confirm selected-intent pointer schema/day handling without editing runtime truth.",
    ],
    "market data": [
        "Inspect selected-intent market-data requirement extraction and freshness validation source.",
        "Confirm equity/options snapshot path conventions match the selected intent.",
    ],
    "structure": ["Inspect structure decision supply diagnostics and schema expectations."],
    "authority/freshness": ["Inspect authority-head freshness predicates and pointer lineage source validation."],
    "exposure/risk/allocation": ["Inspect exposure, risk budget, sleeve-edge, and capital allocation read contracts."],
    "executable authorization": ["Inspect authorization verdict/supply source contracts and identity materialization validators."],
    "submit boundary": ["Inspect kill-switch, trading-day readiness, and submit-boundary source validators."],
    "broker/reconciliation": ["Inspect broker/reconciliation readers for stale or missing evidence contracts."],
}

OPERATOR_ACTIONS = {
    "startup/materialization": ["Use the governed startup/materialization path at its scheduled boundary; do not synthesize artifacts."],
    "intent/arbitration": ["Use governed intent generation/arbitration only if the operator explicitly intends to refresh PAPER evidence."],
    "market data": ["Use governed market-data capture/materialization only during an eligible market-data window."],
    "structure": ["Keep fail-closed; refresh governed market-data and structure surfaces only through approved tooling."],
    "authority/freshness": ["Use governed pointer/authority refresh only through the canonical PAPER sequence."],
    "exposure/risk/allocation": ["Refresh governed risk/allocation surfaces only after upstream authority and market data are current."],
    "executable authorization": ["Use governed authorization materialization only after structure, authority, and allocation are current."],
    "submit boundary": ["Do not manually mark PAPER-ready; refresh governed submit boundary only after all upstream blockers clear."],
    "broker/reconciliation": ["Use governed broker/reconciliation capture paths; do not create broker facts by hand."],
}


@dataclass(frozen=True)
class ForecastContext:
    day_utc: str
    sleeve: str
    environment: str
    truth_root: Path
    canonical_truth_root: Path
    generated_at_utc: str


def _stable_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _status_of(payload: dict[str, Any]) -> str:
    for key in ("status", "final_status", "convergence_status", "boundary_status", "state"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    return "UNKNOWN"


def _blocker_of(payload: dict[str, Any], fallback: str) -> str:
    for key in ("canonical_blocker", "first_blocker", "primary_blocker", "blocker", "reason_code"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    for key in ("reason_codes", "blocking_codes", "blocking_reason_codes", "hard_blockers", "failed_reason_codes"):
        values = payload.get(key)
        if isinstance(values, list):
            for value in values:
                normalized = str(value).strip().upper()
                if normalized:
                    return normalized
    return fallback


def _timestamp(payload: dict[str, Any]) -> str:
    for key in (
        "generated_at_utc",
        "generated_utc",
        "produced_at_utc",
        "produced_utc",
        "created_at_utc",
        "evaluation_utc",
        "decided_at_utc",
        "quote_as_of_utc",
        "as_of_utc",
        "timestamp_utc",
        "ingested_utc",
    ):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    quote = payload.get("quote") if isinstance(payload.get("quote"), dict) else {}
    for key in ("as_of_utc", "timestamp_utc"):
        value = str(quote.get(key) or "").strip()
        if value:
            return value
    return ""


def _parse_utc(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _payload_day(payload: dict[str, Any]) -> str:
    for key in ("day_utc", "target_day", "current_day", "operational_day_utc"):
        value = str(payload.get(key) or "").strip()
        if len(value) == 10:
            return value
    return ""


def _stale_reason(payload: dict[str, Any], day_utc: str) -> str:
    observed_day = _payload_day(payload)
    if observed_day and observed_day != day_utc:
        return f"observed_day={observed_day} expected_day={day_utc}"
    parsed = _parse_utc(_timestamp(payload))
    if parsed is not None and parsed.date().isoformat() != day_utc:
        return f"timestamp_day={parsed.date().isoformat()} expected_day={day_utc}"
    stale_fields = [
        _status_of(payload),
        _blocker_of(payload, ""),
        str(payload.get("stale_status") or ""),
        str(payload.get("freshness_status") or ""),
        str(payload.get("reason_code") or ""),
    ]
    for key in ("reason_codes", "blocking_codes", "blocking_reason_codes", "hard_blockers", "failed_reason_codes"):
        values = payload.get(key)
        if isinstance(values, list):
            stale_fields.extend(str(value) for value in values)
    if any("STALE" in value.upper() for value in stale_fields):
        return "artifact text reports STALE"
    return ""


def _root_for_stage(ctx: ForecastContext, stage: kernel.KernelStage) -> Path:
    return ctx.truth_root if stage.truth_role == "PAPER_SLEEVE" else ctx.canonical_truth_root


def _opposite_root_for_stage(ctx: ForecastContext, stage: kernel.KernelStage) -> Path:
    return ctx.canonical_truth_root if stage.truth_role == "PAPER_SLEEVE" else ctx.truth_root


def _stage_path(ctx: ForecastContext, stage: kernel.KernelStage) -> Path | None:
    if stage.artifact_rel is None:
        return None
    return (_root_for_stage(ctx, stage) / stage.artifact_rel).resolve()


def _safe_stage_action(domain: str) -> tuple[list[str], list[str]]:
    return (list(SOURCE_FIXES.get(domain, [])), list(OPERATOR_ACTIONS.get(domain, [])))


def _root_mismatch_from_payload(path: Path, payload: dict[str, Any], expected_root: Path) -> dict[str, Any] | None:
    expected = expected_root.resolve()
    for key in ("truth_root", "execution_root", "execution_truth_root", "canonical_truth_root", "root"):
        raw = str(payload.get(key) or "").strip()
        if not raw:
            continue
        observed = Path(raw).expanduser().resolve()
        if observed != expected:
            return {
                "artifact_path": str(path),
                "field": key,
                "expected_root": str(expected),
                "observed_root": str(observed),
                "reason": "artifact declares a different truth root than the forecast path expects",
            }
    return None


def _same_artifact_on_wrong_root(ctx: ForecastContext, stage: kernel.KernelStage) -> dict[str, Any] | None:
    if stage.artifact_rel is None:
        return None
    expected = (_root_for_stage(ctx, stage) / stage.artifact_rel).resolve()
    opposite = (_opposite_root_for_stage(ctx, stage) / stage.artifact_rel).resolve()
    if expected.exists() or not opposite.exists():
        return None
    return {
        "stage_id": stage.stage_id,
        "expected_path": str(expected),
        "observed_wrong_root_path": str(opposite),
        "expected_truth_role": stage.truth_role,
        "reason": "artifact exists under the opposite truth root but not under the root expected by the PAPER graph",
    }


def _normalize_validation_status(validation: dict[str, Any]) -> tuple[str, str, str]:
    raw_status = str(validation.get("status") or "").strip().upper()
    artifact_status = str(validation.get("artifact_status") or "").strip().upper()
    blocker = str(validation.get("first_blocker") or validation.get("blocker") or "").strip().upper()
    detail = str(validation.get("detail") or "").strip()
    if raw_status == PASS:
        return PASS, "", detail
    if raw_status == MISSING or artifact_status == MISSING:
        return MISSING, blocker or "MISSING_ARTIFACT", detail or "artifact is missing"
    if "STALE" in raw_status or "STALE" in artifact_status or "STALE" in blocker:
        return STALE, blocker or "STALE_ARTIFACT", detail or "artifact reports stale evidence"
    return BLOCKED, blocker or raw_status or "ARTIFACT_BLOCKED", detail or "artifact does not satisfy stage predicate"


def _stage_result(ctx: ForecastContext, stage: kernel.KernelStage) -> dict[str, Any]:
    domain = STAGE_DOMAIN.get(stage.stage_id, "startup/materialization")
    source_fixes, operator_actions = _safe_stage_action(domain)
    path = _stage_path(ctx, stage)
    expected_root = _root_for_stage(ctx, stage).resolve()
    root_mismatch = _same_artifact_on_wrong_root(ctx, stage)
    base = {
        "stage_id": stage.stage_id,
        "domain": domain,
        "owner": stage.owner,
        "truth_role": stage.truth_role,
        "artifact_path": str(path) if path is not None else "",
        "expected_root": str(expected_root),
        "status": MISSING,
        "blocker_code": "MISSING_ARTIFACT",
        "artifact_status": MISSING,
        "reason": "artifact missing",
        "suggested_safe_source_only_fixes": source_fixes,
        "suggested_operator_governed_actions": operator_actions,
        "root_path_mismatch": root_mismatch,
    }
    if path is None:
        return {**base, "status": PASS, "blocker_code": "", "artifact_status": "NO_ARTIFACT_REQUIRED", "reason": ""}
    if not path.exists():
        if root_mismatch is not None:
            return {**base, "blocker_code": "ROOT_PATH_MISMATCH", "reason": root_mismatch["reason"]}
        return base
    if path.is_dir():
        if not any(path.iterdir()):
            return {**base, "blocker_code": "EMPTY_ARTIFACT_DIR", "reason": "artifact directory is empty"}
        return {**base, "status": PASS, "blocker_code": "", "artifact_status": "DIR_PRESENT", "reason": ""}

    payload = _read_json(path)
    if not payload:
        return {**base, "status": BLOCKED, "blocker_code": "ARTIFACT_JSON_INVALID", "artifact_status": "INVALID", "reason": "artifact is missing or invalid JSON"}
    declared_root_mismatch = _root_mismatch_from_payload(path, payload, expected_root)
    stale = _stale_reason(payload, ctx.day_utc)
    if declared_root_mismatch is not None:
        return {**base, "status": BLOCKED, "blocker_code": "ROOT_PATH_MISMATCH", "artifact_status": _status_of(payload), "reason": declared_root_mismatch["reason"], "root_path_mismatch": declared_root_mismatch}
    if stale:
        blocker = "GLOBAL_KILL_SWITCH_STALE" if stage.stage_id == "global_kill_switch" else "STALE_ARTIFACT"
        return {**base, "status": STALE, "blocker_code": blocker, "artifact_status": _status_of(payload), "reason": stale, "artifact_timestamp": _timestamp(payload)}

    validation = kernel._validate_stage_artifact(stage=stage, artifact_path=path, target_day=ctx.day_utc)
    status, blocker, reason = _normalize_validation_status(validation)
    artifact_status = str(validation.get("artifact_status") or _status_of(payload)).strip().upper()
    if status != PASS and not blocker:
        blocker = _blocker_of(payload, f"{stage.stage_id.upper()}_BLOCKED")
    return {
        **base,
        "status": status,
        "blocker_code": blocker,
        "artifact_status": artifact_status,
        "reason": reason,
        "artifact_timestamp": _timestamp(payload),
        "validation": {k: v for k, v in validation.items() if k not in {"producer_command", "repair_command"}},
    }


def _intent_symbol(intent: dict[str, Any]) -> str:
    for key in ("instrument", "symbol", "underlying_symbol"):
        value = str(intent.get(key) or "").strip().upper()
        if value:
            return value
    underlying = intent.get("underlying") if isinstance(intent.get("underlying"), dict) else {}
    return str(underlying.get("symbol") or "").strip().upper()


def _extract_selected_intent(ctx: ForecastContext) -> dict[str, Any]:
    pointer_path = (ctx.truth_root / "pointers" / "selected_intent_pointer.v1.json").resolve()
    payload = _read_json(pointer_path) if pointer_path.is_file() else {}
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else {}
    return {
        "available": bool(selected),
        "path": str(pointer_path),
        "intent_id": str(selected.get("intent_id") or payload.get("selected_intent_id") or "").strip(),
        "intent_hash": str(selected.get("intent_hash") or "").strip(),
        "engine_id": str(selected.get("engine_id") or "").strip(),
        "sleeve_id": str(selected.get("sleeve_id") or "").strip(),
        "symbol": _intent_symbol(selected) if selected else "",
        "status": str(payload.get("status") or "").strip().upper(),
    }


def _equity_snapshot_result(ctx: ForecastContext, selected_intent: dict[str, Any]) -> dict[str, Any] | None:
    symbol = str(selected_intent.get("symbol") or "").strip().upper()
    if not symbol:
        return None
    domain = "market data"
    source_fixes, operator_actions = _safe_stage_action(domain)
    root = (ctx.truth_root / "market_data_snapshot_v1" / "snapshots" / ctx.day_utc).resolve()
    path = (root / f"{symbol}.market_data_snapshot.v1.json").resolve()
    base = {
        "stage_id": "equity_market_data_snapshot",
        "domain": domain,
        "owner": "market_data",
        "truth_role": "PAPER_SLEEVE",
        "artifact_path": str(path),
        "expected_root": str(ctx.truth_root.resolve()),
        "status": MISSING,
        "blocker_code": "EQUITY_MARKET_DATA_SNAPSHOT_MISSING",
        "artifact_status": MISSING,
        "reason": f"current-day governed equity market-data snapshot missing for selected intent symbol {symbol}",
        "suggested_safe_source_only_fixes": source_fixes,
        "suggested_operator_governed_actions": operator_actions,
        "root_path_mismatch": None,
    }
    if not path.is_file():
        opposite = (ctx.canonical_truth_root / "market_data_snapshot_v1" / "snapshots" / ctx.day_utc / path.name).resolve()
        if opposite.is_file():
            base["blocker_code"] = "ROOT_PATH_MISMATCH"
            base["root_path_mismatch"] = {
                "stage_id": "equity_market_data_snapshot",
                "expected_path": str(path),
                "observed_wrong_root_path": str(opposite),
                "expected_truth_role": "PAPER_SLEEVE",
                "reason": "selected-intent equity snapshot exists under canonical root but PAPER graph expects sleeve root",
            }
        return base
    payload = _read_json(path)
    stale = _stale_reason(payload, ctx.day_utc)
    if stale:
        return {**base, "status": STALE, "blocker_code": "EQUITY_MARKET_DATA_STALE", "artifact_status": _status_of(payload), "reason": stale, "artifact_timestamp": _timestamp(payload)}
    observed_symbol = str(payload.get("symbol") or "").strip().upper()
    if observed_symbol and observed_symbol != symbol:
        return {**base, "status": BLOCKED, "blocker_code": "EQUITY_MARKET_DATA_SNAPSHOT_INVALID", "artifact_status": _status_of(payload), "reason": f"observed_symbol={observed_symbol} expected_symbol={symbol}"}
    return {**base, "status": PASS, "blocker_code": "", "artifact_status": _status_of(payload), "reason": "", "artifact_timestamp": _timestamp(payload)}


def _strip_version_suffix(value: str) -> str:
    return re.sub(r"_V[0-9]+$", "", str(value or "").strip())


def _strategy_sleeve_id_for_selected_intent(ctx: ForecastContext, selected_intent: dict[str, Any]) -> str:
    allocation_path = (
        ctx.truth_root
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / ctx.day_utc
        / "capital_authority_allocation.v1.json"
    ).resolve()
    allocation = _read_json(allocation_path) if allocation_path.is_file() else {}
    intent_id = str(selected_intent.get("intent_id") or "").strip()
    intent_hash = str(selected_intent.get("intent_hash") or "").strip()
    decision_chain = allocation.get("decision_chain") if isinstance(allocation.get("decision_chain"), dict) else {}
    for key in ("authorized_trade_intents", "trade_intents", "candidate_actions"):
        rows = decision_chain.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_intent_id = str(row.get("intent_id") or "").strip()
            row_intent_hash = str(row.get("intent_hash") or "").strip()
            if (intent_hash and row_intent_hash == intent_hash) or (intent_id and row_intent_id == intent_id):
                strategy_sleeve = str(row.get("strategy_sleeve_id") or "").strip()
                if strategy_sleeve:
                    return strategy_sleeve
    for key in ("sleeve_id", "engine_id"):
        value = _strip_version_suffix(str(selected_intent.get(key) or ""))
        if value:
            return value
    return ctx.sleeve


def _sleeve_edge_result(ctx: ForecastContext, selected_intent: dict[str, Any]) -> dict[str, Any]:
    domain = "exposure/risk/allocation"
    source_fixes, operator_actions = _safe_stage_action(domain)
    strategy_sleeve_id = _strategy_sleeve_id_for_selected_intent(ctx, selected_intent)
    base_dir = (ctx.truth_root / "reports" / "sleeve_edge_snapshot_v1" / ctx.day_utc / strategy_sleeve_id).resolve()
    candidates = sorted(base_dir.glob("*/sleeve_edge_snapshot.v1.json")) if base_dir.is_dir() else []
    path = candidates[-1] if candidates else (base_dir / "<revision>" / "sleeve_edge_snapshot.v1.json")
    base = {
        "stage_id": "sleeve_edge_snapshot",
        "domain": domain,
        "owner": "sleeve_edge",
        "truth_role": "PAPER_SLEEVE",
        "artifact_path": str(path),
        "expected_root": str(ctx.truth_root.resolve()),
        "status": MISSING,
        "blocker_code": "SLEEVE_EDGE_SNAPSHOT_MISSING",
        "artifact_status": MISSING,
        "reason": f"canonical sleeve-edge control snapshot missing before allocation for strategy sleeve {strategy_sleeve_id}",
        "suggested_safe_source_only_fixes": source_fixes,
        "suggested_operator_governed_actions": operator_actions,
        "root_path_mismatch": None,
    }
    if not candidates:
        return base
    payload = _read_json(path)
    stale = _stale_reason(payload, ctx.day_utc)
    if stale:
        return {**base, "status": STALE, "blocker_code": "SLEEVE_EDGE_SNAPSHOT_STALE", "artifact_status": _status_of(payload), "reason": stale, "artifact_timestamp": _timestamp(payload)}
    status = _status_of(payload)
    if status and status not in {"PASS", "OK", "READY", "QUALIFIED", "WATCHLIST"}:
        return {**base, "status": BLOCKED, "blocker_code": _blocker_of(payload, "SLEEVE_EDGE_SNAPSHOT_BLOCKED"), "artifact_status": status, "reason": "sleeve-edge snapshot status is not allocation-eligible"}
    return {**base, "status": PASS, "blocker_code": "", "artifact_status": status, "reason": "", "artifact_timestamp": _timestamp(payload)}


def _domain_rows(stage_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for domain in DOMAIN_ORDER:
        stages = [row for row in stage_results if row["domain"] == domain]
        status = BLOCKED if any(row["status"] in {BLOCKED, MISSING, STALE} for row in stages) else PASS
        rows.append({"domain": domain, "status": status, "stages": stages})
    return rows


def _blockers(stage_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in stage_results if row.get("status") in {BLOCKED, MISSING, STALE}]


def _risk_lists(stage_results: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    root_mismatches = []
    missing_edges = []
    stale = []
    reuse = []
    for row in stage_results:
        mismatch = row.get("root_path_mismatch")
        if isinstance(mismatch, dict):
            root_mismatches.append(mismatch)
        if row.get("status") == MISSING:
            missing_edges.append({"stage_id": row["stage_id"], "domain": row["domain"], "artifact_path": row["artifact_path"], "suspect": "producer edge may not have materialized this required artifact"})
        if row.get("status") == STALE:
            stale.append({"stage_id": row["stage_id"], "domain": row["domain"], "artifact_path": row["artifact_path"], "artifact_timestamp": row.get("artifact_timestamp", ""), "reason": row.get("reason", "")})
        if row["stage_id"] in {"exposure_net", "paper_authority_head_freshness", "submit_boundary_status"} and row.get("status") in {BLOCKED, STALE}:
            reuse.append({"stage_id": row["stage_id"], "artifact_path": row["artifact_path"], "risk": "existing artifact is not safe to reuse for the requested day/root", "reason": row.get("reason", "")})
    return root_mismatches, missing_edges, stale, reuse


def build_aegis_paper_blocker_forecast_v1(
    *,
    day_utc: str,
    sleeve: str,
    environment: str,
    truth_root: Path,
    canonical_truth_root: Path,
    generated_at_utc: str = "",
) -> dict[str, Any]:
    if environment.upper() != "PAPER":
        raise ValueError("aegis blocker forecast only supports PAPER")
    ctx = ForecastContext(
        day_utc=day_utc,
        sleeve=sleeve,
        environment=environment.upper(),
        truth_root=Path(truth_root).resolve(),
        canonical_truth_root=Path(canonical_truth_root).resolve(),
        generated_at_utc=generated_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    )
    stages = kernel._stages(
        target_day=ctx.day_utc,
        canonical_truth_root=ctx.canonical_truth_root,
        paper_sleeve_root=ctx.truth_root,
        environment=ctx.environment,
        ib_account="",
        release_commit="",
    )
    selected_intent = _extract_selected_intent(ctx)
    stage_results = [
        _sleeve_edge_result(ctx, selected_intent)
        if stage.stage_id == "sleeve_edge_measurement"
        else _stage_result(ctx, stage)
        for stage in stages
    ]
    equity_result = _equity_snapshot_result(ctx, selected_intent)
    if equity_result is not None:
        insert_at = next((i + 1 for i, row in enumerate(stage_results) if row["stage_id"] == "aegis_requirement_graph"), len(stage_results))
        stage_results.insert(insert_at, equity_result)
    blockers = _blockers(stage_results)
    first = blockers[0] if blockers else {}
    root_mismatches, missing_edges, stale, reuse = _risk_lists(stage_results)
    return {
        "schema_id": "aegis_paper_blocker_forecast_v1",
        "schema_version": "v1",
        "generated_at_utc": ctx.generated_at_utc,
        "day_utc": ctx.day_utc,
        "sleeve": ctx.sleeve,
        "environment": ctx.environment,
        "truth_root": str(ctx.truth_root),
        "canonical_truth_root": str(ctx.canonical_truth_root),
        "read_only": True,
        "runtime_mutation": False,
        "producers_called": False,
        "paper_ready_run": False,
        "manual_paper_ready": False,
        "fake_pass_evidence_used": False,
        "selected_intent": selected_intent,
        "first_canonical_blocker": first,
        "next_likely_blockers": blockers[1:10],
        "domains": _domain_rows(stage_results),
        "root_path_mismatches": root_mismatches,
        "missing_producer_edge_suspects": missing_edges,
        "stale_artifact_suspects": stale,
        "invalid_idempotency_reuse_risks": reuse,
        "exact_artifact_paths": {row["stage_id"]: row["artifact_path"] for row in stage_results},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_paper_blocker_forecast_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--sleeve", required=True)
    parser.add_argument("--environment", required=True, choices=["PAPER"])
    parser.add_argument("--truth_root", required=True, help="PAPER sleeve truth root to inspect.")
    parser.add_argument("--canonical_truth_root", required=True, help="Canonical truth root to inspect.")
    args = parser.parse_args(argv)
    payload = build_aegis_paper_blocker_forecast_v1(
        day_utc=str(args.day_utc),
        sleeve=str(args.sleeve),
        environment=str(args.environment),
        truth_root=Path(args.truth_root),
        canonical_truth_root=Path(args.canonical_truth_root),
    )
    sys.stdout.write(_stable_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
