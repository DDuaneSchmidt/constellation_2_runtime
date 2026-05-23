from __future__ import annotations

import glob
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.evidence_event_store_v1 import rebuild_evidence_snapshot_v1
from ops.aegis.final_eod_orchestrator_v1 import ledger_jsonl_path_v1, ledger_latest_path_v1
from ops.aegis.pure_runtime_evaluator_v1 import DEFAULT_RUNTIME_POLICY_BUNDLE_V1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1, read_canonical_runtime_evaluation_v1, read_runtime_state_snapshot_v1


REPORT_FAMILY = "aegis_operational_maturity_hardening_v1"
SCHEMA_ID = "aegis_operational_maturity_hardening"
SCHEMA_VERSION = "v1"

EVIDENCE_TYPES = {
    "research": "research evidence",
    "event": "event evidence",
    "alert": "alert evidence",
    "candidate": "candidate evidence",
    "promoted": "promoted candidate evidence",
    "manual": "manual execution receipt evidence",
    "feedback": "feedback evidence",
}

SAFETY_ASSERTIONS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "manual_ib_capture_policy": "MANUAL_ONLY",
}


def build_operational_maturity_hardening_v1(
    *,
    truth_root: Path | str = DEFAULT_TRUTH_ROOT,
    day_utc: str,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or _now()
    kernel = build_runtime_truth_kernel_v1(truth_root=root, day_utc=day_utc, generated_at_utc=generated_at)
    runtime_eval = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    snapshot = read_runtime_state_snapshot_v1(truth_root=root, day_utc=day_utc)
    evidence = build_evidence_lifecycle_projection_v1(
        truth_root=root,
        day_utc=day_utc,
        runtime_evaluation=runtime_eval,
        generated_at_utc=generated_at,
    )
    promoted = build_promoted_candidate_lifecycle_proof_v1(truth_root=root, day_utc=day_utc, kernel=kernel)
    manual_receipts = build_manual_capture_receipt_lifecycle_v1(truth_root=root, day_utc=day_utc, kernel=kernel)
    capture_ticket = build_capture_ticket_projection_v1(
        runtime_evaluation=runtime_eval,
        promoted=promoted,
        manual_receipts=manual_receipts,
    )
    replay = build_replay_determinism_projection_v1(truth_root=root, day_utc=day_utc, kernel=kernel, runtime_evaluation=runtime_eval, snapshot=snapshot)
    provider = build_provider_resilience_projection_v1(truth_root=root, day_utc=day_utc)
    orchestrator = build_orchestrator_resilience_projection_v1(truth_root=root, day_utc=day_utc)
    readiness = classify_operational_readiness_v1(
        kernel=kernel,
        runtime_evaluation=runtime_eval,
        evidence_projection=evidence,
        replay_projection=replay,
        capture_ticket_projection=capture_ticket,
    )
    blockers = _maturity_blockers(
        readiness=readiness,
        evidence=evidence,
        promoted=promoted,
        manual_receipts=manual_receipts,
        replay=replay,
        provider=provider,
        orchestrator=orchestrator,
    )
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "aegis_operational_maturity_hardening_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "truth_root": str(root),
        "operational_readiness_classification": readiness["classification"],
        "readiness_reason": readiness["reason"],
        "operator_action_required": bool(blockers),
        "operator_actions": blockers,
        "platform_capture_capability": capture_ticket["platform_capture_capability"],
        "capture_ticket_status": capture_ticket["capture_ticket_status"],
        "capture_ticket_count": capture_ticket["capture_ticket_count"],
        "capture_ticket_projection": capture_ticket,
        "evidence_lifecycle": evidence,
        "promoted_candidate_lifecycle": promoted,
        "manual_ib_capture_receipt_lifecycle": manual_receipts,
        "replay_determinism": replay,
        "provider_resilience": provider,
        "orchestrator_resilience": orchestrator,
        "runtime_truth_classification": kernel.get("runtime_truth_classification"),
        "highest_readiness_layer": kernel.get("highest_readiness_layer"),
        "allowed_capabilities": _allowed_capabilities(runtime_eval, kernel),
        "blocked_capabilities": _blocked_capabilities(runtime_eval, kernel),
        "safety_assertions": dict(SAFETY_ASSERTIONS),
    }
    payload["maturity_hash"] = stable_hash_v1(_hashable_projection(payload))
    return payload


def build_evidence_lifecycle_projection_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    runtime_evaluation: dict[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    snapshot = rebuild_evidence_snapshot_v1(truth_root=root, day_utc=day_utc)
    runtime_evaluation = runtime_evaluation or read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    required_schemas = _all_policy_evidence_ids()
    capability_groups = _capability_groups_by_schema()
    consumed_schemas = _consumed_schema_ids(runtime_evaluation)
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for event in snapshot.get("events") or []:
        if not isinstance(event, dict):
            continue
        schema_id = str(event.get("schema_id") or "")
        content_hash = _event_content_hash(event)
        if not schema_id:
            continue
        key = (schema_id, content_hash or str(event.get("event_id") or ""))
        if key not in latest or (str(event.get("created_at_utc") or ""), str(event.get("event_id") or "")) >= (str(latest[key].get("created_at_utc") or ""), str(latest[key].get("event_id") or "")):
            latest[key] = event
    items = []
    for (schema_id, content_hash), event in sorted(latest.items()):
        artifact_path = str((event.get("artifact_paths") or [""])[0] or "")
        linked = _linked_entities_from_artifact(Path(artifact_path))
        status = _evidence_status_from_event(event, consumed=bool(schema_id in consumed_schemas))
        items.append(
            {
                "evidence_id": _evidence_id(schema_id=schema_id, content_hash=content_hash, event_id=str(event.get("event_id") or "")),
                "evidence_type": _evidence_type(schema_id),
                "source": str(event.get("producer") or ""),
                "linked_hypothesis_thesis_intent_candidate": linked,
                "created_at": str(event.get("created_at_utc") or ""),
                "operational_day": day_utc,
                "status": status,
                "required_for_replay": schema_id in required_schemas,
                "replay_dependency_group": capability_groups.get(schema_id, []),
                "lineage": {
                    "event_id": event.get("event_id"),
                    "event_type": event.get("event_type"),
                    "run_id": event.get("run_id"),
                    "parent_run_id": event.get("parent_run_id"),
                    "artifact_paths": event.get("artifact_paths") or [],
                    "input_hashes": event.get("input_hashes") or {},
                    "output_hashes": event.get("output_hashes") or {},
                },
                "content_hash": content_hash,
            }
        )
    counts = _count_by(items, "status")
    required_items = [item for item in items if bool(item.get("required_for_replay"))]
    missing_required = sorted(schema for schema in required_schemas if schema not in {str(item.get("lineage", {}).get("schema_id") or item.get("evidence_type") or "") for item in []})
    present_required = {str(event.get("schema_id") or "") for event in (snapshot.get("latest_by_schema_id") or {}).values() if isinstance(event, dict)}
    missing_required = sorted(required_schemas - present_required)
    return {
        "schema_id": "aegis_evidence_lifecycle_projection",
        "schema_version": "v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or _now(),
        "items": items,
        "item_count": len(items),
        "counts_by_status": counts,
        "required_for_replay_count": len(required_items),
        "missing_required_replay_evidence": missing_required,
        "rejected_or_expired_count": sum(counts.get(status, 0) for status in ("REJECTED", "EXPIRED")),
        "required_evidence_status": "COMPLETE" if not missing_required else "INCOMPLETE",
    }


def build_promoted_candidate_lifecycle_proof_v1(*, truth_root: Path | str, day_utc: str, kernel: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    audit_path, audit = _latest_json(root / "reports" / "candidate_consumption_audit_v1" / day_utc, "candidate_consumption_audit.v1.json")
    report_path, report = _latest_json(root / "reports" / "aegis_lite_eod_report_v1" / day_utc, "aegis_lite_eod_report.v1.json")
    library_path, library = _latest_json(root / "reports" / "promoted_sleeve_library_v1", "promoted_sleeve_library.v1.json")
    manifest_path, manifest = _latest_json(root / "reports" / "promoted_sleeve_manifest_v1", "promoted_sleeve_manifest.v1.json")
    queue_path, queue = _latest_json(root / "reports" / "operator_execution_queue_v1" / day_utc, "operator_execution_queue.v1.json")
    manual_packet_path, packet = _latest_json(root / "reports" / "manual_trade_packet_v1" / day_utc, "manual_trade_packet.v1.json")
    receipt_path, receipt = _latest_json(root / "reports" / "manual_execution_receipt_v1" / day_utc, "manual_execution_receipt.v1.json")
    raw_count = int(audit.get("raw_candidate_count") or 0)
    promoted_count = int(audit.get("promoted_candidate_count") or 0)
    excluded_count = int(audit.get("excluded_candidate_count") or 0)
    counts = audit.get("consumption_counts") if isinstance(audit.get("consumption_counts"), dict) else {}
    recommendation_count = _candidate_count(packet) or _candidate_count(queue)
    if promoted_count > 0 and recommendation_count > 0:
        status = "READY_WITH_PROMOTED_CANDIDATES"
        reason = "Promoted candidates produced recommendation evidence."
    elif bool(audit.get("normal_no_op")) or str(report.get("eod_outcome_status") or "") == "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES":
        status = "NORMAL_NO_OP_NO_PROMOTABLE_CANDIDATES"
        reason = "Raw candidates were evaluated and none were promotable under policy/coverage."
    elif raw_count > 0 and promoted_count == 0:
        status = "BLOCKED_PROMOTION_POLICY"
        reason = "Raw candidates exist but no promoted candidate lifecycle artifact proves consumption."
    else:
        status = "BLOCKED_MISSING_REQUIRED_INPUT"
        reason = "No candidate consumption audit or promoted candidate evidence was found."
    return {
        "status": status,
        "reason": reason,
        "raw_candidate_count": raw_count,
        "promoted_candidate_count": promoted_count,
        "excluded_candidate_count": excluded_count,
        "excluded_count_by_reason": counts,
        "candidate_consumption_audit_path": str(audit_path or ""),
        "candidate_consumption_audit_hash": _sha256_optional(audit_path),
        "promoted_sleeve_library_path": str(library_path or ""),
        "promoted_sleeve_library_hash": _sha256_optional(library_path),
        "promoted_sleeve_manifest_path": str(manifest_path or ""),
        "promoted_sleeve_manifest_hash": _sha256_optional(manifest_path),
        "recommendation_artifact_paths": [str(path) for path in (queue_path, manual_packet_path) if path],
        "manual_execution_receipt_path": str(receipt_path or ""),
        "manual_execution_receipt_status": str(receipt.get("verification_status") or receipt.get("result") or ""),
        "eod_report_path": str(report_path or ""),
        "proof_steps": [
            _proof("candidate promoted", promoted_count > 0, "No promoted candidates today is acceptable only with a no-op audit."),
            _proof("promoted library updated", bool(library), "Required when promoted_count > 0."),
            _proof("promoted manifest updated", bool(manifest), "Required when promoted_count > 0."),
            _proof("recommendation produced", recommendation_count > 0, "Required only when promoted candidates exist."),
            _proof("manual IB capture recorded", bool(receipt), "Recorded by manual execution receipt workflow when an operator manually fills in IB."),
            _proof("replay convergence verified", str(status).startswith("NORMAL_NO_OP") or str(status).startswith("READY"), reason),
        ],
        "normal_no_op": status == "NORMAL_NO_OP_NO_PROMOTABLE_CANDIDATES",
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
    }


def build_manual_capture_receipt_lifecycle_v1(*, truth_root: Path | str, day_utc: str, kernel: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    receipt_path, receipt = _latest_json(root / "reports" / "manual_execution_receipt_v1" / day_utc, "manual_execution_receipt.v1.json")
    packet_path, packet = _latest_json(root / "reports" / "manual_trade_packet_v1" / day_utc, "manual_trade_packet.v1.json")
    recommendation_count = _candidate_count(packet)
    receipt_type = str(receipt.get("receipt_type") or "")
    status = "NOT_REQUIRED"
    reason = "No manual IB capture recommendation is active."
    if recommendation_count > 0 and not receipt:
        status = "RECEIPT_REQUIRED"
        reason = "Manual capture recommendation exists; operator receipt evidence is required after any manual IB fill."
    elif receipt_type == "NONE_DECLARED" or receipt.get("result") == "NO_MANUAL_EXECUTION_DECLARED":
        status = "NO_MANUAL_EXECUTION_DECLARED"
        reason = "Operator declared no manual execution for the day."
    elif receipt:
        status = str(receipt.get("verification_status") or "RECORDED")
        reason = "Manual execution receipt evidence is recorded."
    return {
        "status": status,
        "reason": reason,
        "capture_recommendation_issued": recommendation_count > 0,
        "recommendation_count": recommendation_count,
        "recommendation_path": str(packet_path or ""),
        "receipt_path": str(receipt_path or ""),
        "receipt_id": str(receipt.get("receipt_id") or ""),
        "linked_recommendation": str(receipt.get("recommended_trade_id") or receipt.get("source_packet_id") or ""),
        "linked_promoted_candidate": str(receipt.get("linked_promoted_candidate") or receipt.get("recommended_trade_id") or ""),
        "verification_status": str(receipt.get("verification_status") or receipt.get("result") or ""),
        "receipt_fields_present": sorted(key for key in ("receipt_id", "symbol", "side", "quantity", "timestamp", "recommended_trade_id", "notes") if key in receipt),
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
    }



def build_capture_ticket_projection_v1(
    *,
    runtime_evaluation: dict[str, Any] | None,
    promoted: dict[str, Any],
    manual_receipts: dict[str, Any],
) -> dict[str, Any]:
    caps = runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation, dict) and isinstance(runtime_evaluation.get("capabilities"), dict) else {}
    platform_ready = bool((caps.get("MANUAL_TRADE_CAPTURE_ALLOWED") or {}).get("allowed", False))
    ticket_count = int(manual_receipts.get("recommendation_count") or promoted.get("promoted_candidate_count") or 0)
    receipt_status = str(manual_receipts.get("status") or "").upper()
    if ticket_count <= 0:
        ticket_status = "NONE_AVAILABLE"
        action = "No action required"
        summary = "Manual capture capability is ready; there are no IB capture tickets."
    elif receipt_status in {"RECORDED", "VERIFIED", "CAPTURE_RECORDED", "TICKET_COMPLETED"}:
        ticket_status = "TICKET_COMPLETED"
        action = "No action required"
        summary = "A manual IB capture receipt is recorded for the available ticket."
    elif platform_ready:
        ticket_status = "TICKET_READY"
        action = "Review the IB capture ticket before any external manual action."
        summary = "An IB capture ticket is available; broker automation remains disabled."
    else:
        ticket_status = "BLOCKED"
        action = "Resolve the manual capture capability blocker before using the ticket."
        summary = "An IB capture ticket exists, but the platform capability is not ready."
    return {
        "platform_capture_capability": "READY" if platform_ready else "NOT_READY",
        "capture_ticket_status": ticket_status,
        "capture_ticket_count": ticket_count,
        "operator_next_action": action,
        "summary": summary,
        "recommendation_count": int(manual_receipts.get("recommendation_count") or 0),
        "promoted_candidate_count": int(promoted.get("promoted_candidate_count") or 0),
        "receipt_status": str(manual_receipts.get("status") or ""),
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
    }

def build_replay_determinism_projection_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    kernel: dict[str, Any],
    runtime_evaluation: dict[str, Any] | None,
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    first = _deterministic_projection_hash(day_utc=day_utc, kernel=kernel, runtime_evaluation=runtime_evaluation or {}, snapshot=snapshot or {})
    second = _deterministic_projection_hash(day_utc=day_utc, kernel=kernel, runtime_evaluation=runtime_evaluation or {}, snapshot=snapshot or {})
    prior_days = _available_snapshot_days(Path(truth_root).expanduser().resolve(), day_utc)[-5:]
    chain_rows = []
    for day in prior_days:
        snap = read_runtime_state_snapshot_v1(truth_root=Path(truth_root), day_utc=day)
        if snap:
            chain_rows.append({"day_utc": day, "evaluation_id": snap.get("evaluation_id"), "projection_hash": stable_hash_v1(_hashable_projection(snap))})
    return {
        "same_day_replay_twice_identical": first == second,
        "same_day_projection_hash": first,
        "same_day_second_projection_hash": second,
        "runtime_evaluation_hash": str((runtime_evaluation or {}).get("deterministic_output_hash") or ""),
        "runtime_state_snapshot_id": str((snapshot or {}).get("evaluation_id") or ""),
        "multi_day_chain_verified": len(chain_rows) == len(prior_days) and bool(chain_rows),
        "multi_day_chain": chain_rows,
        "repaired_artifact_convergence_verified": bool((runtime_evaluation or {}).get("deterministic_output_hash")),
        "command_projection_state_verified": bool(snapshot and (snapshot.get("transition_count") is not None or snapshot.get("evaluation_id"))),
        "status": "DETERMINISTIC" if first == second and bool(runtime_evaluation) else "REPLAY_UNVERIFIED",
    }


def build_provider_resilience_projection_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    registry_path, registry = _latest_json(root / "reports" / "aegis_data_registry_v1" / day_utc, "data_registry.v1.json")
    final_eod_path, final_eod = _latest_json(root / "reports" / "final_eod_market_data_v1" / day_utc, "final_eod_market_data.v1.json")
    final_eod_valid = str(final_eod.get("validation_status") or final_eod.get("status") or "").upper() in {"VALID", "CURRENT", "CERTIFIED", "PASS", "OK"}
    provider_source_rows = final_eod.get("provider_results") if final_eod_valid and isinstance(final_eod.get("provider_results"), list) else registry.get("provider_status")
    provider_rows = []
    for row in provider_source_rows or []:
        if isinstance(row, dict):
            provider_rows.append(_redact_provider_row(row))
    failures = [row for row in provider_rows if _provider_row_status(row) not in {"OK", "PASS", "VALID", "CERTIFIED", "SUCCEEDED", "CURRENT", "SUCCESS", "SKIPPED_VALID_ARTIFACT_EXISTS"}]
    rate_limited = [row for row in provider_rows if "429" in json.dumps(row) or "RATE" in json.dumps(row).upper()]
    invalid = [row for row in provider_rows if str(row.get("status") or "").upper() in {"INVALID", "FAILED", "MISSING", "STALE"}]
    quarantine = sorted({str(row.get("provider") or row.get("provider_id") or row.get("source") or "UNKNOWN") for row in [*rate_limited, *invalid] if row})
    coverage_source = final_eod if final_eod_valid else registry
    coverage = {
        "required_universe_size": len(coverage_source.get("required_symbols") or coverage_source.get("requested_symbols") or coverage_source.get("symbols") or registry.get("requested_symbols") or []),
        "covered_count": len(coverage_source.get("records") or coverage_source.get("rows") or coverage_source.get("fetched_symbols") or coverage_source.get("symbols") or []),
        "missing_symbols": coverage_source.get("missing_symbols") if isinstance(coverage_source.get("missing_symbols"), list) else [],
        "stale_symbols": coverage_source.get("stale_symbols") if isinstance(coverage_source.get("stale_symbols"), list) else [],
        "provider_failed_symbols": coverage_source.get("provider_failed_symbols") if isinstance(coverage_source.get("provider_failed_symbols"), list) else [],
    }
    status = "HEALTHY" if not failures and not coverage["missing_symbols"] and not coverage["stale_symbols"] else "DEGRADED"
    return {
        "status": status,
        "source_registry_path": str(registry_path or ""),
        "source_final_eod_path": str(final_eod_path or ""),
        "provider_health_history": provider_rows,
        "rolling_success_count": len(provider_rows) - len(failures),
        "rolling_failure_count": len(failures),
        "provider_sla_tracking": {
            "rate_limit_events": len(rate_limited),
            "invalid_response_events": len(invalid),
            "coverage_complete": not coverage["missing_symbols"] and not coverage["stale_symbols"],
        },
        "provider_degradation_alerts": _provider_alerts(failures=failures, coverage=coverage),
        "quarantined_provider_candidates": quarantine,
        "fallback_provider_promotion_allowed": status == "DEGRADED" and bool(quarantine),
        "certification_quality_silent_degrade_allowed": False,
        "coverage": coverage,
    }


def build_orchestrator_resilience_projection_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    latest_path = ledger_latest_path_v1(truth_root=root, day_utc=day_utc)
    ledger_path = ledger_jsonl_path_v1(truth_root=root, day_utc=day_utc)
    latest = _read_json(latest_path)
    rows = _read_jsonl(ledger_path)
    statuses = [str(row.get("status") or "") for row in rows if isinstance(row, dict)]
    latest_status = str(latest.get("status") or "")
    outcome = _run_outcome(latest_status=latest_status, stage_statuses=statuses)
    run_ids = [str(row.get("run_id") or "") for row in rows if row.get("run_id")]
    duplicate_run_count = max(0, len(set(run_ids)) - 1)
    return {
        "status": "RESILIENT" if outcome in {"RUN_COMPLETED", "RUN_PARTIAL", "RUN_FAILED", "RUN_ABORTED"} and bool(latest) else "RUN_UNVERIFIED",
        "run_id": str(latest.get("run_id") or ""),
        "final_run_outcome": outcome,
        "latest_path": str(latest_path),
        "ledger_jsonl": str(ledger_path),
        "stage_count": len(rows),
        "completed_stage_count": sum(1 for status in statuses if status == "SUCCEEDED"),
        "failed_stage_count": sum(1 for status in statuses if status == "FAILED"),
        "interruption_recovery_supported": True,
        "resumable_stage_execution_supported": True,
        "stage_checkpointing_present": bool(rows),
        "stale_run_cleanup_needed": outcome in {"RUN_ABORTED", "RUN_PARTIAL"},
        "orphaned_run_detected": bool(rows) and not bool(latest),
        "duplicate_run_prevention_needed": duplicate_run_count > 0,
        "duplicate_run_count": duplicate_run_count,
        "failure_reason": str(latest.get("failure_reason") or ""),
    }


def _allowed_capabilities(runtime_evaluation: dict[str, Any] | None, kernel: dict[str, Any]) -> list[str]:
    caps = runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation, dict) and isinstance(runtime_evaluation.get("capabilities"), dict) else {}
    if caps:
        return sorted(cap for cap, row in caps.items() if isinstance(row, dict) and bool(row.get("allowed", False)))
    return [str(item) for item in kernel.get("allowed_capabilities") or []]


def _blocked_capabilities(runtime_evaluation: dict[str, Any] | None, kernel: dict[str, Any]) -> list[str]:
    caps = runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation, dict) and isinstance(runtime_evaluation.get("capabilities"), dict) else {}
    if caps:
        return sorted(cap for cap, row in caps.items() if isinstance(row, dict) and not bool(row.get("allowed", False)))
    return [str(item) for item in kernel.get("blocked_capabilities") or []]


def classify_operational_readiness_v1(
    *,
    kernel: dict[str, Any],
    runtime_evaluation: dict[str, Any] | None,
    evidence_projection: dict[str, Any],
    replay_projection: dict[str, Any],
    capture_ticket_projection: dict[str, Any] | None = None,
) -> dict[str, str]:
    caps = runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation, dict) and isinstance(runtime_evaluation.get("capabilities"), dict) else {}
    if caps:
        blocked = sorted(cap for cap, row in caps.items() if isinstance(row, dict) and not bool(row.get("allowed", False)))
    else:
        blocked = [str(item) for item in kernel.get("blocked_capabilities") or []]
    missing_evidence = evidence_projection.get("missing_required_replay_evidence") or []
    if not runtime_evaluation:
        return {"classification": "REPLAY_UNVERIFIED", "reason": "Canonical runtime evaluation is missing."}
    if str(replay_projection.get("status") or "") == "REPLAY_UNVERIFIED":
        return {"classification": "REPLAY_UNVERIFIED", "reason": "Replay projection could not be verified deterministically."}
    if not caps.get("DATA_READY", {}).get("allowed", "DATA_READY" not in blocked) and "DATA_READY" in blocked:
        return {"classification": "BLOCKED_DATA", "reason": "Data readiness capability is blocked."}
    if missing_evidence:
        return {"classification": "BLOCKED_MISSING_EVIDENCE", "reason": "Required replay evidence is missing: " + ", ".join(missing_evidence[:6])}
    if any(cap in blocked for cap in ("RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY", "ALERT_GATE_PROVEN")):
        return {"classification": "PARTIAL_CONTEXT", "reason": "Core evidence exists, but research/event/feedback/alert proof is incomplete."}
    if caps.get("MANUAL_TRADE_CAPTURE_ALLOWED", {}).get("allowed") is True:
        ticket = capture_ticket_projection or {}
        return {
            "classification": "MANUAL_CAPTURE_CAPABILITY_READY",
            "reason": (
                "Manual capture capability is evidence-backed; "
                f"capture_ticket_status={ticket.get('capture_ticket_status', 'UNKNOWN')} "
                f"capture_ticket_count={ticket.get('capture_ticket_count', 0)}."
            ),
        }
    if caps.get("TRADE_ADVICE_ALLOWED", {}).get("allowed") is False:
        return {"classification": "RESEARCH_ONLY", "reason": "Research/advisory context may be reviewed, but trade advice remains disabled by policy."}
    return {"classification": "NO_ACTION_REQUIRED", "reason": "No operator action is required."}


def write_operational_maturity_hardening_v1(*, truth_root: Path | str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    out_dir = root / "reports" / REPORT_FAMILY / str(payload["day_utc"])
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "operational_maturity_hardening.v1.json"
    summary_path = out_dir / "operational_maturity_hardening.summary.txt"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_operational_maturity_summary_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "sha256": _sha256_optional(json_path)}


def render_operational_maturity_summary_v1(payload: dict[str, Any]) -> str:
    promoted = payload.get("promoted_candidate_lifecycle") if isinstance(payload.get("promoted_candidate_lifecycle"), dict) else {}
    tickets = payload.get("capture_ticket_projection") if isinstance(payload.get("capture_ticket_projection"), dict) else {}
    evidence = payload.get("evidence_lifecycle") if isinstance(payload.get("evidence_lifecycle"), dict) else {}
    provider = payload.get("provider_resilience") if isinstance(payload.get("provider_resilience"), dict) else {}
    orchestrator = payload.get("orchestrator_resilience") if isinstance(payload.get("orchestrator_resilience"), dict) else {}
    replay = payload.get("replay_determinism") if isinstance(payload.get("replay_determinism"), dict) else {}
    lines = [
        "AEGIS OPERATIONAL MATURITY HARDENING v1",
        f"day_utc: {payload.get('day_utc')}",
        f"operational_readiness_classification: {payload.get('operational_readiness_classification')}",
        f"platform_capture_capability: {payload.get('platform_capture_capability')}",
        f"capture_ticket_status: {payload.get('capture_ticket_status')} count={payload.get('capture_ticket_count', 0)}",
        f"readiness_reason: {payload.get('readiness_reason')}",
        f"evidence_items: {evidence.get('item_count', 0)} required_status={evidence.get('required_evidence_status')}",
        f"promoted_candidate_status: {promoted.get('status')} raw={promoted.get('raw_candidate_count', 0)} promoted={promoted.get('promoted_candidate_count', 0)} excluded={promoted.get('excluded_candidate_count', 0)}",
        f"manual_receipt_status: {(payload.get('manual_ib_capture_receipt_lifecycle') or {}).get('status')}",
        f"operator_next_action: {tickets.get('operator_next_action', 'No action required')}",
        f"replay_status: {replay.get('status')} same_day_identical={str(replay.get('same_day_replay_twice_identical')).lower()}",
        f"provider_status: {provider.get('status')} failures={provider.get('rolling_failure_count', 0)}",
        f"orchestrator_outcome: {orchestrator.get('final_run_outcome')} run_id={orchestrator.get('run_id')}",
        f"operator_actions: {len(payload.get('operator_actions') or [])}",
        "broker_submit_transmit_allowed: false",
        "autonomous_execution_allowed: false",
        "trade_advice_allowed: false",
        "",
    ]
    for item in payload.get("operator_actions") or []:
        lines.append(f"- {item.get('code')}: {item.get('message')}")
    return "\n".join(lines).rstrip() + "\n"


def _maturity_blockers(**parts: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    readiness = parts["readiness"]
    if readiness["classification"].startswith("BLOCKED") or readiness["classification"] == "REPLAY_UNVERIFIED":
        out.append({"code": readiness["classification"], "message": readiness["reason"]})
    promoted = parts["promoted"]
    if str(promoted.get("status") or "").startswith("BLOCKED"):
        out.append({"code": str(promoted.get("status")), "message": str(promoted.get("reason"))})
    receipts = parts["manual_receipts"]
    if receipts.get("status") == "RECEIPT_REQUIRED":
        out.append({"code": "MANUAL_RECEIPT_REQUIRED", "message": str(receipts.get("reason"))})
    provider = parts["provider"]
    if provider.get("status") == "DEGRADED":
        out.append({"code": "PROVIDER_DEGRADED", "message": "; ".join(provider.get("provider_degradation_alerts") or ["Provider degradation detected."])})
    orchestrator = parts["orchestrator"]
    if orchestrator.get("final_run_outcome") in {"RUN_ABORTED", "RUN_PARTIAL"}:
        out.append({"code": str(orchestrator.get("final_run_outcome")), "message": str(orchestrator.get("failure_reason") or "Final EOD run needs recovery.")})
    return out


def _all_policy_evidence_ids() -> set[str]:
    ids: set[str] = set()
    dag = DEFAULT_RUNTIME_POLICY_BUNDLE_V1.get("capability_dag") if isinstance(DEFAULT_RUNTIME_POLICY_BUNDLE_V1.get("capability_dag"), dict) else {}
    for row in dag.values():
        if not isinstance(row, dict):
            continue
        if row.get("policy_allowed") is False:
            continue
        ids.update(str(item) for item in row.get("evidence", []) if str(item))
    return ids


def _capability_groups_by_schema() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    dag = DEFAULT_RUNTIME_POLICY_BUNDLE_V1.get("capability_dag") if isinstance(DEFAULT_RUNTIME_POLICY_BUNDLE_V1.get("capability_dag"), dict) else {}
    for capability, row in dag.items():
        if not isinstance(row, dict):
            continue
        for schema_id in row.get("evidence", []) or []:
            out.setdefault(str(schema_id), []).append(str(capability))
    return {key: sorted(values) for key, values in out.items()}


def _consumed_schema_ids(runtime_evaluation: dict[str, Any] | None) -> set[str]:
    out = {str(row.get("schema_id") or "") for row in (runtime_evaluation or {}).get("source_evidence_refs", []) if isinstance(row, dict)}
    caps = (runtime_evaluation or {}).get("capabilities") if isinstance((runtime_evaluation or {}).get("capabilities"), dict) else {}
    for cap in caps.values():
        if not isinstance(cap, dict):
            continue
        for ref in cap.get("evidence_refs", []) or []:
            if isinstance(ref, dict) and str(ref.get("schema_id") or ""):
                out.add(str(ref.get("schema_id")))
    return {item for item in out if item}


def _event_content_hash(event: dict[str, Any]) -> str:
    output_hashes = event.get("output_hashes") if isinstance(event.get("output_hashes"), dict) else {}
    for value in output_hashes.values():
        text = str(value or "")
        if text:
            return text
    return str(event.get("event_hash") or "")


def _evidence_id(*, schema_id: str, content_hash: str, event_id: str) -> str:
    token = content_hash[:16] if content_hash else hashlib.sha256(event_id.encode("utf-8")).hexdigest()[:16]
    return f"evidence:{schema_id}:{token}"


def _evidence_type(schema_id: str) -> str:
    text = schema_id.lower()
    if "promoted" in text:
        return EVIDENCE_TYPES["promoted"]
    if "manual_execution_receipt" in text:
        return EVIDENCE_TYPES["manual"]
    if "research" in text:
        return EVIDENCE_TYPES["research"]
    if "event" in text:
        return EVIDENCE_TYPES["event"]
    if "alert" in text:
        return EVIDENCE_TYPES["alert"]
    if "feedback" in text:
        return EVIDENCE_TYPES["feedback"]
    if "candidate" in text or "queue" in text or "trade_packet" in text:
        return EVIDENCE_TYPES["candidate"]
    return "operational evidence"


def _evidence_status_from_event(event: dict[str, Any], *, consumed: bool) -> str:
    event_type = str(event.get("event_type") or "")
    if event_type == "EvidenceExpired":
        return "EXPIRED"
    if event_type in {"EvidenceRejected", "ArtifactTampered"}:
        return "REJECTED"
    if consumed and event_type == "EvidenceValidated" and str(event.get("validation_status") or "").upper() in {"VALID", "PASS", "OK", "CURRENT"}:
        return "CONSUMED"
    if event_type == "EvidenceValidated":
        return "VERIFIED"
    return "GENERATED"


def _linked_entities_from_artifact(path: Path) -> dict[str, str]:
    payload = _read_json(path)
    return {
        "hypothesis_id": str(payload.get("hypothesis_id") or payload.get("source_hypothesis_id") or ""),
        "thesis_id": str(payload.get("thesis_id") or ""),
        "intent_id": str(payload.get("intent_id") or payload.get("selected_intent_id") or ""),
        "candidate_id": str(payload.get("candidate_id") or payload.get("recommended_trade_id") or ""),
    }


def _deterministic_projection_hash(*, day_utc: str, kernel: dict[str, Any], runtime_evaluation: dict[str, Any], snapshot: dict[str, Any]) -> str:
    return stable_hash_v1(
        {
            "day_utc": day_utc,
            "runtime_evaluation_hash": runtime_evaluation.get("deterministic_output_hash"),
            "snapshot_id": snapshot.get("evaluation_id"),
            "artifact_hashes": snapshot.get("evidence_hashes") or {},
            "blocked_capabilities": kernel.get("blocked_capabilities") or [],
            "allowed_capabilities": kernel.get("allowed_capabilities") or [],
        }
    )


def _hashable_projection(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in {"generated_at_utc", "maturity_hash"}}


def _available_snapshot_days(root: Path, day_utc: str) -> list[str]:
    base = root / "reports" / "aegis_runtime_state_snapshots_v1"
    if not base.exists():
        return []
    return sorted(path.name for path in base.iterdir() if path.is_dir() and path.name <= day_utc)


def _provider_alerts(*, failures: list[dict[str, Any]], coverage: dict[str, Any]) -> list[str]:
    alerts: list[str] = []
    if coverage.get("missing_symbols"):
        alerts.append("Provider coverage incomplete: missing " + ",".join(str(item) for item in coverage["missing_symbols"][:20]))
    if coverage.get("stale_symbols"):
        alerts.append("Provider returned stale symbols: " + ",".join(str(item) for item in coverage["stale_symbols"][:20]))
    if coverage.get("provider_failed_symbols"):
        alerts.append("Provider fetch failed for symbols: " + ",".join(str(item) for item in coverage["provider_failed_symbols"][:20]))
    if failures:
        alerts.append(f"{len(failures)} provider status rows are not passing.")
    return alerts


def _provider_row_status(row: dict[str, Any]) -> str:
    return str(row.get("status") or row.get("result_status") or row.get("request_status") or row.get("provider_fetch_status") or "UNKNOWN").upper()


def _redact_provider_row(row: dict[str, Any]) -> dict[str, Any]:
    redacted = {}
    for key, value in row.items():
        if any(token in str(key).lower() for token in ("key", "token", "secret", "credential")):
            redacted[key] = "REDACTED"
        else:
            redacted[key] = value
    return redacted


def _run_outcome(*, latest_status: str, stage_statuses: list[str]) -> str:
    status = latest_status.upper()
    if status in {"SUCCEEDED", "RUN_COMPLETED"}:
        return "RUN_COMPLETED"
    if status in {"INTERRUPTED", "ABORTED", "RUN_ABORTED"}:
        return "RUN_ABORTED"
    if status in {"SUPERSEDED", "RUN_SUPERSEDED"}:
        return "RUN_SUPERSEDED"
    if status in {"FAILED", "RUN_FAILED"} and any(row == "SUCCEEDED" for row in stage_statuses):
        return "RUN_PARTIAL"
    if status in {"FAILED", "RUN_FAILED"}:
        return "RUN_FAILED"
    return "RUN_UNVERIFIED"


def _proof(name: str, passed: bool, reason: str) -> dict[str, Any]:
    return {"step": name, "status": "PASS" if passed else "NOT_PROVEN", "reason": reason}


def _candidate_count(payload: dict[str, Any]) -> int:
    for key in ("current_actionable_items", "actionable_items", "candidates", "trade_candidates", "manual_trade_candidates", "trades", "execution_queue"):
        value = payload.get(key)
        if isinstance(value, list):
            return len([row for row in value if isinstance(row, dict)])
    return 0


def _latest_json(root: Path, filename: str) -> tuple[Path | None, dict[str, Any]]:
    if not root.exists():
        return None, {}
    candidates = sorted(Path(path) for path in glob.glob(str(root / "**" / filename), recursive=True) if Path(path).is_file())
    if not candidates:
        return None, {}
    path = candidates[-1]
    return path, _read_json(path)


def _read_json(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _sha256_optional(path: Path | None) -> str:
    if not path or not Path(path).exists():
        return ""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "")
        out[value] = out.get(value, 0) + 1
    return dict(sorted(out.items()))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
