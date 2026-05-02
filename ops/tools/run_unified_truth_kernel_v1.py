#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_truth_integrity_common_v1 import (
    READY_FINAL_STATUSES,
    artifact_specs_v1,
    blocker_of_v1,
    now_iso_v1,
    read_json_v1,
    report_path_v1,
    status_of_v1,
    write_json_v1,
)

SCHEMA_VERSION = "unified_truth_kernel.v1"
FINAL_STATUS_SOURCE = "aegis_day_run_ledger_v1"

PERFORMANCE_ARTIFACTS: tuple[dict[str, str], ...] = (
    {"artifact_type": "selection_quality_v1", "family": "selection_quality_v1", "filename": "selection_quality.v1.json"},
    {"artifact_type": "edge_attribution_v1", "family": "edge_attribution_v1", "filename": "edge_attribution.v1.json"},
    {"artifact_type": "regime_confidence_v1", "family": "regime_confidence_v1", "filename": "regime_confidence.v1.json"},
    {"artifact_type": "trade_outcome_v1", "family": "trade_outcome_v1", "filename": "trade_outcome.v1.json"},
    {"artifact_type": "decision_consistency_v1", "family": "decision_consistency_v1", "filename": "decision_consistency.v1.json"},
    {"artifact_type": "missed_opportunity_v1", "family": "missed_opportunity_v1", "filename": "missed_opportunity.v1.json"},
    {"artifact_type": "insight_engine_v1", "family": "insight_engine_v1", "filename": "insight_engine.v1.json"},
    {"artifact_type": "ai_advisory_review_v1", "family": "ai_advisory_review_v1", "filename": "ai_advisory_review.v1.json"},
    {"artifact_type": "strategy_change_governance_v1", "family": "strategy_change_governance_v1", "filename": "strategy_change_governance.v1.json"},
)


def unified_truth_kernel_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "unified_truth_kernel_v1" / day_utc / "unified_truth_kernel.v1.json").resolve()


def _artifact_ref(artifact_type: str, path: Path, payload: dict[str, Any], *, role: str) -> dict[str, Any]:
    contract = payload.get("producer_contract_v1") if isinstance(payload.get("producer_contract_v1"), dict) else {}
    return {
        "artifact_type": artifact_type,
        "artifact_role": role,
        "path": str(path),
        "exists": bool(path.exists() and path.is_file()),
        "status": status_of_v1(payload) if payload else "MISSING",
        "canonical_blocker": blocker_of_v1(payload) if payload else f"{artifact_type.upper()}_MISSING",
        "schema_version": str(payload.get("schema_version") or ""),
        "producer_contract_present": bool(contract),
        "advisory_only": role == "ADVISORY",
    }


def _action_rows(action_validity: dict[str, Any], statuses: set[str]) -> list[dict[str, Any]]:
    rows = action_validity.get("action_rules") if isinstance(action_validity.get("action_rules"), list) else []
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("status") not in statuses:
            continue
        result.append(
            {
                "action_id": str(row.get("action_id") or ""),
                "label": str(row.get("label") or row.get("action_id") or ""),
                "status": str(row.get("status") or ""),
                "risk_level": str(row.get("risk_level") or ""),
                "reason": str(row.get("reason") or ""),
                "operator_confirmation_required": bool(row.get("operator_confirmation_required") is True),
            }
        )
    return result


def _source_integrity_status(ledger: dict[str, Any]) -> str:
    source = ledger.get("source_repo_status") if isinstance(ledger.get("source_repo_status"), dict) else {}
    if source.get("source_reproducibility_status") == "REPRODUCIBLE" and source.get("git_dirty_status") == "CLEAN":
        return "PASS"
    if blocker_of_v1(ledger) == "SOURCE_REPRODUCIBILITY_BLOCKED":
        return "BLOCKED"
    return "UNKNOWN"


def _lineage_untrusted(lineage: dict[str, Any]) -> list[dict[str, Any]]:
    rows = lineage.get("lineage_nodes") if isinstance(lineage.get("lineage_nodes"), list) else []
    return [
        {
            "artifact_type": str(row.get("artifact_type") or ""),
            "path": str(row.get("artifact_path") or ""),
            "status": str(row.get("lineage_status") or "UNKNOWN"),
            "canonical_blocker": str(row.get("canonical_blocker") or ""),
            "blocking_class": str(row.get("blocking_class") or ""),
        }
        for row in rows
        if isinstance(row, dict) and str(row.get("lineage_status") or "").upper() not in {"PASS"}
    ]


def _freshness_untrusted(freshness: dict[str, Any]) -> list[dict[str, Any]]:
    rows = freshness.get("freshness_records") if isinstance(freshness.get("freshness_records"), list) else []
    return [
        {
            "artifact_type": str(row.get("artifact_type") or ""),
            "path": str(row.get("artifact_path") or ""),
            "status": str(row.get("freshness_status") or "UNKNOWN"),
            "canonical_blocker": str(row.get("canonical_blocker") or ""),
            "blocking_class": str(row.get("blocking_class") or ""),
        }
        for row in rows
        if isinstance(row, dict) and str(row.get("freshness_status") or "").upper() in {"STALE", "EXPIRED", "UNKNOWN"}
    ]


def _trade_health(ctx: bod.BodContext) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    advisory_refs: list[dict[str, Any]] = []
    payloads: dict[str, dict[str, Any]] = {}
    for spec in PERFORMANCE_ARTIFACTS:
        path = report_path_v1(ctx, spec["family"], spec["filename"])
        payload = read_json_v1(path)
        advisory_refs.append(_artifact_ref(spec["artifact_type"], path, payload, role="ADVISORY"))
        payloads[spec["artifact_type"]] = payload
    missed = payloads["missed_opportunity_v1"].get("alternatives")
    insight = payloads["insight_engine_v1"]
    drift_alerts = insight.get("drift_alerts") if isinstance(insight.get("drift_alerts"), list) else []
    strategy = payloads["strategy_change_governance_v1"]
    advisory_review = payloads["ai_advisory_review_v1"]
    edge_status = status_of_v1(payloads["edge_attribution_v1"]) if payloads["edge_attribution_v1"] else "UNPROVEN"
    if edge_status in {"MISSING", "UNKNOWN"}:
        edge_status = "UNPROVEN"
    trade_health = {
        "selection_confidence": str(payloads["selection_quality_v1"].get("confidence_level") or "UNKNOWN"),
        "edge_status": edge_status,
        "regime_status": status_of_v1(payloads["regime_confidence_v1"]) if payloads["regime_confidence_v1"] else "UNKNOWN",
        "outcome_status": str(payloads["trade_outcome_v1"].get("outcome_status") or status_of_v1(payloads["trade_outcome_v1"]) if payloads["trade_outcome_v1"] else "UNKNOWN"),
        "decision_consistency_status": status_of_v1(payloads["decision_consistency_v1"]) if payloads["decision_consistency_v1"] else "UNKNOWN",
        "missed_opportunity_count": len(missed) if isinstance(missed, list) else 0,
        "drift_alert_count": len(drift_alerts),
        "advisory_review_status": status_of_v1(advisory_review) if advisory_review else "UNKNOWN",
        "strategy_change_governance_status": status_of_v1(strategy) if strategy else "UNKNOWN",
        "human_review_required": bool(advisory_review.get("requires_human_review") is True or strategy.get("human_approval_required") is True),
        "automatic_deployment_allowed": bool(strategy.get("automatic_deployment_allowed") is True),
        "advisory_only": True,
    }
    return trade_health, advisory_refs


def _confidence(*, ledger: dict[str, Any], lineage: dict[str, Any], consistency: dict[str, Any], freshness: dict[str, Any], authoritative_missing: list[dict[str, Any]], untrusted: list[dict[str, Any]]) -> str:
    if not ledger:
        return "UNKNOWN"
    if status_of_v1(consistency) == "FAIL":
        return "LOW"
    if status_of_v1(lineage) == "FAIL" or status_of_v1(freshness) == "FAIL":
        return "LOW"
    if authoritative_missing or any(row.get("blocking_class") == "HARD_BLOCKER" for row in untrusted):
        return "LOW"
    if blocker_of_v1(ledger):
        return "MEDIUM"
    return "HIGH"


def build_unified_truth_kernel_v1(ctx: bod.BodContext) -> dict[str, Any]:
    paths = {
        "ledger": report_path_v1(ctx, "aegis_day_run_v1", "day_run.v1.json"),
        "requirement_graph": report_path_v1(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json"),
        "lineage": report_path_v1(ctx, "evidence_lineage_index_v1", "evidence_lineage_index.v1.json"),
        "consistency": report_path_v1(ctx, "state_consistency_v1", "state_consistency.v1.json"),
        "freshness": report_path_v1(ctx, "truth_freshness_v1", "truth_freshness.v1.json"),
        "action_validity": report_path_v1(ctx, "action_validity_v1", "action_validity.v1.json"),
        "projection": report_path_v1(ctx, "aegis_operator_projection_v1", "operator_projection.v1.json"),
        "live": report_path_v1(ctx, "aegis_live_intelligence_v1", "live_intelligence.v1.json"),
    }
    ledger = read_json_v1(paths["ledger"])
    graph = read_json_v1(paths["requirement_graph"])
    lineage = read_json_v1(paths["lineage"])
    consistency = read_json_v1(paths["consistency"])
    freshness = read_json_v1(paths["freshness"])
    action_validity = read_json_v1(paths["action_validity"])
    projection = read_json_v1(paths["projection"])
    live = read_json_v1(paths["live"])

    authoritative_artifacts: list[dict[str, Any]] = []
    diagnostic_artifacts: list[dict[str, Any]] = []
    for spec in artifact_specs_v1():
        path = report_path_v1(ctx, str(spec["family"]), str(spec["filename"]))
        payload = read_json_v1(path)
        ref = _artifact_ref(str(spec["artifact_type"]), path, payload, role="AUTHORITATIVE" if spec.get("authoritative") else "DIAGNOSTIC")
        if spec.get("authoritative"):
            authoritative_artifacts.append(ref)
        else:
            diagnostic_artifacts.append(ref)
    for key, path in paths.items():
        if key in {"ledger", "requirement_graph"}:
            continue
        payload = read_json_v1(path)
        diagnostic_artifacts.append(_artifact_ref(f"{key}_v1", path, payload, role="DIAGNOSTIC"))

    trade_health, advisory_artifacts = _trade_health(ctx)
    authoritative_missing = [row for row in authoritative_artifacts if not row["exists"]]
    untrusted = authoritative_missing + _lineage_untrusted(lineage) + _freshness_untrusted(freshness)
    failed_consistency = [
        row for row in (consistency.get("invariant_results") if isinstance(consistency.get("invariant_results"), list) else [])
        if isinstance(row, dict) and row.get("status") == "FAIL"
    ]
    if failed_consistency:
        untrusted.extend(
            {
                "artifact_type": "state_consistency_v1",
                "path": ",".join(str(item) for item in row.get("checked_artifacts", [])),
                "status": "FAIL",
                "canonical_blocker": str(row.get("canonical_blocker") or "STATE_CONSISTENCY_FAILED"),
                "blocking_class": str(row.get("severity") or "HARD_BLOCKER"),
            }
            for row in failed_consistency
        )

    final_status = str(ledger.get("final_status") or "UNKNOWN").strip().upper()
    first_blocker = blocker_of_v1(ledger)
    phase = str(ledger.get("canonical_phase") or "").strip()
    source_status = _source_integrity_status(ledger)
    allowed = _action_rows(action_validity, {"ALLOWED"})
    forbidden = _action_rows(action_validity, {"FORBIDDEN", "BLOCKED"})
    unsafe = [str(row.get("label") or row.get("action_id") or "") for row in forbidden]
    truth_confidence = _confidence(ledger=ledger, lineage=lineage, consistency=consistency, freshness=freshness, authoritative_missing=authoritative_missing, untrusted=untrusted)
    advisory_status = status_of_v1(live) if live else "UNKNOWN"
    trade_health_status = "ADVISORY_ONLY" if any(row["exists"] for row in advisory_artifacts) else "UNKNOWN"
    human_review_required = bool(trade_health.get("human_review_required") is True or final_status not in READY_FINAL_STATUSES or bool(untrusted))
    operator_next_action = str((allowed[0] if allowed else {}).get("label") or (projection.get("operator_next_action") if projection else "") or ledger.get("operator_next_action") or "Regenerate unified truth inputs.")

    return {
        "schema_id": "unified_truth_kernel",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "final_status": final_status,
        "final_status_source": FINAL_STATUS_SOURCE,
        "first_blocker": first_blocker,
        "first_blocker_phase": phase,
        "first_blocker_owner": phase or "aegis_day_run_ledger_v1",
        "canonical_blocker": first_blocker or (str(untrusted[0].get("canonical_blocker") or "") if untrusted else ""),
        "truth_confidence": truth_confidence,
        "source_integrity_status": source_status,
        "requirement_graph_status": status_of_v1(graph),
        "lineage_status": status_of_v1(lineage),
        "consistency_status": status_of_v1(consistency),
        "freshness_status": status_of_v1(freshness),
        "action_validity_status": status_of_v1(action_validity),
        "advisory_intelligence_status": advisory_status,
        "trade_health_status": trade_health_status,
        "trade_health": trade_health,
        "allowed_operator_actions": allowed,
        "forbidden_operator_actions": forbidden,
        "unsafe_actions": unsafe,
        "downstream_consequences": ledger.get("downstream_consequences") if isinstance(ledger.get("downstream_consequences"), list) else [],
        "authoritative_artifacts": authoritative_artifacts,
        "diagnostic_artifacts": diagnostic_artifacts,
        "advisory_artifacts": advisory_artifacts,
        "unknown_or_untrusted_artifacts": untrusted,
        "operator_next_action": operator_next_action,
        "human_review_required": human_review_required,
        "generated_at_utc": now_iso_v1(),
        "authority_note": "Unified Truth Kernel reads day-run ledger final_status; advisory and performance artifacts cannot change readiness.",
        "input_artifact_paths": [str(path) for path in paths.values()] + [row["path"] for row in advisory_artifacts],
    }


def run_unified_truth_kernel_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_unified_truth_kernel_v1(ctx)
    path = unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_unified_truth_kernel_v1.py",
        producer_command=f"python3 ops/tools/run_unified_truth_kernel_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=payload["input_artifact_paths"],
        output_artifacts=[path],
        schema_versions={"unified_truth_kernel": SCHEMA_VERSION},
    )
    payload["producer_contract"] = dict(payload["producer_contract_v1"])
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_unified_truth_kernel_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_unified_truth_kernel_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["final_status"], "canonical_blocker": payload["canonical_blocker"], "truth_confidence": payload["truth_confidence"], "unified_truth_kernel_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
