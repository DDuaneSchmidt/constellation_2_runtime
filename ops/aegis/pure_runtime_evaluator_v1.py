from __future__ import annotations

from datetime import UTC, datetime
from ops.aegis.blocker_state_v1 import build_blocker_state_report_v1
from ops.aegis.producer_contracts_v1 import contract_registry_version_v1, load_producer_contract_registry_v1
from ops.aegis.producer_contract_validator_v1 import validate_evidence_event_contract_v1
from typing import Any

from ops.aegis.runtime_evaluation_v1 import RUNTIME_EVALUATOR_VERSION, finalize_runtime_evaluation_v1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1


DEFAULT_DAG_VERSION = "aegis_runtime_dependency_dag.v1"
DEFAULT_POLICY_VERSION = "aegis_runtime_policy.v1"
VALIDATION_PASS_VALUES = {"VALID", "PASS", "OK", "CURRENT"}


DEFAULT_RUNTIME_POLICY_BUNDLE_V1: dict[str, Any] = {
    "policy_version": DEFAULT_POLICY_VERSION,
    "dag_version": DEFAULT_DAG_VERSION,
    "capability_dag": {
        "DATA_READY": {
            "evidence": ["aegis_lite_operating_status", "aegis_lite_eod_report", "operator_execution_queue", "event_market_snapshot"],
            "capabilities": [],
        },
        "RESEARCH_READY": {
            "evidence": ["research_dataset_binding", "research_task_queue"],
            "capabilities": [],
        },
        "EVENT_READY": {
            "evidence": ["event_monitoring_status", "event_rules_registry", "event_market_snapshot", "event_validity_gate"],
            "capabilities": [],
        },
        "FEEDBACK_READY": {
            "evidence": ["ai_feedback_review"],
            "capabilities": [],
        },
        "ALERT_GATE_PROVEN": {
            "evidence": ["alert_transport_proof"],
            "capabilities": [],
        },
        "MANUAL_TRADE_CAPTURE_ALLOWED": {
            "evidence": ["manual_execution_receipt"],
            "capabilities": ["DATA_READY", "RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY"],
        },
        "TRADE_ADVICE_ALLOWED": {
            "evidence": ["manual_trade_packet", "promoted_candidate_evidence"],
            "capabilities": ["DATA_READY", "RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY"],
            "policy_allowed": False,
            "policy_reason": "Trade advice is disabled in RuntimeEvaluation v1.",
        },
        "AUTONOMOUS_EXECUTION_ALLOWED": {
            "evidence": [],
            "capabilities": [],
            "policy_allowed": False,
            "policy_reason": "Autonomous execution is disabled by design.",
        },
    },
    "producer_contract_registry_version": contract_registry_version_v1(),
    "producer_contract_registry_hash": stable_hash_v1(load_producer_contract_registry_v1()),
    "freshness_policy": {
        "default_max_age_seconds": 86400,
        "by_schema_id": {
            "manual_execution_receipt": 604800,
            "promoted_candidate_evidence": 604800,
            "research_task_queue": 604800,
            "ai_feedback_review": 604800,
        },
    },
}


def runtime_policy_bundle_v1(
    *,
    run_id: str,
    parent_run_id: str,
    generated_at_utc: str,
    git_sha: str,
    policy_version: str = DEFAULT_POLICY_VERSION,
    dag_version: str = DEFAULT_DAG_VERSION,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bundle = _deepcopy(DEFAULT_RUNTIME_POLICY_BUNDLE_V1)
    bundle.update(
        {
            "run_id": run_id,
            "parent_run_id": parent_run_id,
            "generated_at_utc": generated_at_utc,
            "git_sha": git_sha,
            "policy_version": policy_version,
            "dag_version": dag_version,
        }
    )
    for key, value in (overrides or {}).items():
        bundle[key] = value
    return bundle


def evaluate_runtime(day_utc: str, evidence_snapshot: dict[str, Any], policy_bundle: dict[str, Any]) -> dict[str, Any]:
    required_policy = ["run_id", "parent_run_id", "generated_at_utc", "git_sha", "policy_version", "dag_version", "capability_dag", "freshness_policy", "producer_contract_registry_version", "producer_contract_registry_hash"]
    missing_policy = [field for field in required_policy if field not in policy_bundle]
    if missing_policy:
        raise ValueError("Policy bundle missing fields: " + ",".join(missing_policy))
    generated_at = str(policy_bundle["generated_at_utc"])
    generated_dt = _parse_utc(generated_at)
    if generated_dt is None:
        raise ValueError("Policy bundle generated_at_utc must be parseable UTC timestamp")
    if evidence_snapshot.get("day_utc") != day_utc:
        return _final(
            day_utc=day_utc,
            policy_bundle=policy_bundle,
            capabilities={},
            blockers=[_blocker("SNAPSHOT_WRONG_DAY", "Evidence snapshot day does not match evaluation day.", evidence_refs=[])],
            trace=[],
            refs=[],
            warnings=[],
            errors=["Evidence snapshot day does not match evaluation day."],
        )
    if evidence_snapshot.get("hash_chain_valid") is False:
        return _final(
            day_utc=day_utc,
            policy_bundle=policy_bundle,
            capabilities={},
            blockers=[_blocker("EVIDENCE_HASH_CHAIN_INVALID", "Evidence event hash chain is invalid.", evidence_refs=[])],
            trace=[],
            refs=[],
            warnings=[],
            errors=["Evidence event hash chain is invalid."],
        )

    latest = evidence_snapshot.get("latest_by_schema_id") if isinstance(evidence_snapshot.get("latest_by_schema_id"), dict) else {}
    evidence_results = {
        schema_id: _evaluate_evidence(schema_id=schema_id, event=latest.get(schema_id), generated_dt=generated_dt, policy_bundle=policy_bundle, day_utc=day_utc)
        for schema_id in _all_required_evidence(policy_bundle)
    }
    capabilities = _evaluate_capability_graph(policy_bundle=policy_bundle, evidence_results=evidence_results)
    blockers = _collect_blockers(capabilities, evidence_results)
    blocker_report = build_blocker_state_report_v1(blockers=blockers, evidence_results=evidence_results)
    trace = _decision_trace(capabilities)
    refs = _source_refs(evidence_results)
    snapshot_runtime_truth = str(evidence_snapshot.get("runtime_truth_classification") or "").upper()
    if snapshot_runtime_truth in {"DEMO_ONLY", "DRY_RUN_ONLY"}:
        runtime_truth = snapshot_runtime_truth
    else:
        runtime_truth = "REAL_RUNTIME" if not _readiness_blockers(blockers) else "PARTIAL_CONTEXT"
    if runtime_truth != "REAL_RUNTIME":
        for forced_cap in ("TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"):
            if forced_cap in capabilities:
                capabilities[forced_cap]["allowed"] = False
                capabilities[forced_cap]["reason"] = f"Runtime truth {runtime_truth} does not allow this capability."
                capabilities[forced_cap]["blocker_chain"] = [capabilities[forced_cap]["reason"]]
        blockers = _collect_blockers(capabilities, evidence_results)
        blocker_report = build_blocker_state_report_v1(blockers=blockers, evidence_results=evidence_results)
        trace = _decision_trace(capabilities)
    highest = _highest_readiness_layer(capabilities=capabilities, runtime_truth=runtime_truth)
    return _final(
        day_utc=day_utc,
        policy_bundle=policy_bundle,
        capabilities=capabilities,
        blockers=blockers,
        trace=trace,
        refs=refs,
        warnings=[],
        errors=[],
        runtime_truth_classification=runtime_truth,
        highest_readiness_layer=highest,
        blocker_report=blocker_report,
    )


def _evaluate_capability_graph(*, policy_bundle: dict[str, Any], evidence_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    dag = policy_bundle.get("capability_dag") if isinstance(policy_bundle.get("capability_dag"), dict) else {}
    resolved: dict[str, Any] = {}
    visiting: set[str] = set()

    def resolve(capability: str) -> dict[str, Any]:
        if capability in resolved:
            return resolved[capability]
        if capability in visiting:
            row = _capability_row(capability, False, [], [], [], ["Dependency cycle detected."], "Dependency cycle detected.")
            resolved[capability] = row
            return row
        visiting.add(capability)
        spec = dag.get(capability) if isinstance(dag.get(capability), dict) else {}
        policy_allowed = spec.get("policy_allowed", True)
        policy_reason = str(spec.get("policy_reason") or "")
        direct_caps = sorted(str(item) for item in spec.get("capabilities", []) if str(item))
        evidence_ids = sorted(str(item) for item in spec.get("evidence", []) if str(item))
        dep_rows = [resolve(dep) for dep in direct_caps]
        blocker_chain: list[str] = []
        validation_results: list[dict[str, Any]] = []
        evidence_refs: list[dict[str, Any]] = []
        for dep in dep_rows:
            if not dep["allowed"]:
                blocker_chain.extend(dep.get("blocker_chain") or [dep.get("reason")])
        for schema_id in evidence_ids:
            ev = evidence_results.get(schema_id) or _missing_evidence(schema_id)
            validation_results.append({"schema_id": schema_id, "status": ev["status"], "reason": ev["reason"]})
            if ev.get("evidence_ref"):
                evidence_refs.append(ev["evidence_ref"])
            if ev["status"] != "OK":
                blocker_chain.append(f"{schema_id}: {ev['reason']}")
        if policy_allowed is False:
            blocker_chain.append(policy_reason or f"{capability} disabled by policy.")
        allowed = not blocker_chain
        reason = "All RuntimeEvaluation dependencies passed." if allowed else _compress_reasons(blocker_chain)
        row = _capability_row(capability, allowed, direct_caps, evidence_ids, evidence_refs, blocker_chain, reason, validation_results)
        resolved[capability] = row
        visiting.remove(capability)
        return row

    for capability in sorted(dag):
        resolve(capability)
    for forced in ("TRADE_ADVICE_ALLOWED", "AUTONOMOUS_EXECUTION_ALLOWED"):
        if forced in resolved and resolved[forced]["allowed"]:
            resolved[forced]["allowed"] = False
            resolved[forced]["reason"] = f"{forced} cannot be enabled by RuntimeEvaluation v1."
            resolved[forced]["blocker_chain"] = [resolved[forced]["reason"]]
    return {key: resolved[key] for key in sorted(resolved)}


def _evaluate_evidence(*, schema_id: str, event: Any, generated_dt: datetime, policy_bundle: dict[str, Any], day_utc: str) -> dict[str, Any]:
    if not isinstance(event, dict):
        return _missing_evidence(schema_id)
    contract_validation = validate_evidence_event_contract_v1(event)
    if not bool(contract_validation.get("contract_valid", False)):
        status = "UNKNOWN_PRODUCER" if contract_validation.get("status") == "UNKNOWN_PRODUCER" else "CONTRACT_VIOLATION"
        return _evidence_result(schema_id, status, str(contract_validation.get("reason") or contract_validation.get("status") or "Producer contract validation failed."), event, contract_validation=contract_validation)
    if event.get("day_utc") != day_utc:
        return _evidence_result(schema_id, "WRONG_DAY", f"Evidence event day {event.get('day_utc')} does not match {day_utc}.", event)
    if str(event.get("event_type") or "") in {"EvidenceRejected", "EvidenceExpired", "ArtifactTampered"}:
        return _evidence_result(schema_id, "REJECTED", f"Evidence event type {event.get('event_type')} is not acceptable.", event)
    if str(event.get("validation_status") or "").upper() not in VALIDATION_PASS_VALUES:
        return _evidence_result(schema_id, "INVALID", f"validation_status={event.get('validation_status')} is not passing.", event)
    created = _parse_utc(str(event.get("created_at_utc") or ""))
    if created is None:
        return _evidence_result(schema_id, "MALFORMED", "created_at_utc is missing or malformed.", event)
    if created > generated_dt:
        return _evidence_result(schema_id, "FUTURE_DATED", "Evidence created_at_utc is after evaluation generated_at_utc.", event)
    max_age = _max_age_seconds(schema_id, policy_bundle)
    age = int((generated_dt - created).total_seconds())
    if age > max_age:
        return _evidence_result(schema_id, "STALE", f"Evidence age {age}s exceeds freshness window {max_age}s.", event)
    if not isinstance(event.get("output_hashes"), dict) or not event.get("output_hashes"):
        return _evidence_result(schema_id, "UNVERIFIABLE", "Evidence output_hashes are missing.", event)
    return _evidence_result(schema_id, "OK", "Evidence is valid, current, and verifiable.", event)


def _final(
    *,
    day_utc: str,
    policy_bundle: dict[str, Any],
    capabilities: dict[str, Any],
    blockers: list[dict[str, Any]],
    trace: list[dict[str, Any]],
    refs: list[dict[str, Any]],
    warnings: list[str],
    errors: list[str],
    runtime_truth_classification: str = "PARTIAL_CONTEXT",
    highest_readiness_layer: str = "BLOCKED",
    blocker_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not capabilities:
        capabilities = {
            "TRADE_ADVICE_ALLOWED": _capability_row("TRADE_ADVICE_ALLOWED", False, [], [], [], ["RuntimeEvaluation failed closed."], "RuntimeEvaluation failed closed."),
            "MANUAL_TRADE_CAPTURE_ALLOWED": _capability_row("MANUAL_TRADE_CAPTURE_ALLOWED", False, [], [], [], ["RuntimeEvaluation failed closed."], "RuntimeEvaluation failed closed."),
            "AUTONOMOUS_EXECUTION_ALLOWED": _capability_row("AUTONOMOUS_EXECUTION_ALLOWED", False, [], [], [], ["Autonomous execution disabled by design."], "Autonomous execution disabled by design."),
        }
        trace = _decision_trace(capabilities)
    return finalize_runtime_evaluation_v1(
        {
            "run_id": str(policy_bundle["run_id"]),
            "parent_run_id": str(policy_bundle["parent_run_id"]),
            "day_utc": day_utc,
            "generated_at_utc": str(policy_bundle["generated_at_utc"]),
            "git_sha": str(policy_bundle["git_sha"]),
            "evaluator_version": RUNTIME_EVALUATOR_VERSION,
            "policy_version": str(policy_bundle["policy_version"]),
            "dag_version": str(policy_bundle["dag_version"]),
            "schema_version": "v1",
            "runtime_truth_classification": runtime_truth_classification,
            "highest_readiness_layer": highest_readiness_layer,
            "capabilities": capabilities,
            "blockers": blockers,
            "decision_trace": trace,
            "source_evidence_refs": refs,
            "root_blockers": (blocker_report or {}).get("root_blockers", []),
            "blocker_state": (blocker_report or {}).get("blocker_state", []),
            "repairability": (blocker_report or {}).get("repairability", {}),
            "producer_contract_ref": (blocker_report or {}).get("producer_contract_ref", {}),
            "next_safe_action": (blocker_report or {}).get("next_safe_action", {}),
            "producer_contract_registry_version": str(policy_bundle.get("producer_contract_registry_version") or ""),
            "producer_contract_registry_hash": str(policy_bundle.get("producer_contract_registry_hash") or ""),
            "warnings": warnings,
            "errors": errors,
            "deterministic_output_hash": "",
        }
    )


def _all_required_evidence(policy_bundle: dict[str, Any]) -> list[str]:
    dag = policy_bundle.get("capability_dag") if isinstance(policy_bundle.get("capability_dag"), dict) else {}
    evidence: set[str] = set()
    for spec in dag.values():
        if not isinstance(spec, dict):
            continue
        if spec.get("policy_allowed") is False:
            continue
        evidence.update(str(item) for item in spec.get("evidence", []) if str(item))
    return sorted(evidence)


def _missing_evidence(schema_id: str) -> dict[str, Any]:
    return {"schema_id": schema_id, "status": "MISSING", "reason": "Required evidence is missing.", "evidence_ref": None}


def _evidence_result(schema_id: str, status: str, reason: str, event: dict[str, Any], contract_validation: dict[str, Any] | None = None) -> dict[str, Any]:
    contract_validation = contract_validation or validate_evidence_event_contract_v1(event)
    return {
        "schema_id": schema_id,
        "status": status,
        "reason": reason,
        "contract_status": str(contract_validation.get("status") or ""),
        "contract_valid": bool(contract_validation.get("contract_valid", False)),
        "producer_contract_ref": contract_validation.get("producer_contract_ref") or {},
        "evidence_ref": {
            "schema_id": schema_id,
            "event_id": str(event.get("event_id") or ""),
            "event_hash": str(event.get("event_hash") or ""),
            "artifact_paths": sorted(str(path) for path in event.get("artifact_paths", []) if str(path)),
            "output_hashes": {str(key): str(value) for key, value in sorted((event.get("output_hashes") or {}).items())},
            "created_at_utc": str(event.get("created_at_utc") or ""),
            "validation_status": str(event.get("validation_status") or ""),
            "producer_contract_ref": contract_validation.get("producer_contract_ref") or {},
            "contract_status": str(contract_validation.get("status") or ""),
        },
    }


def _capability_row(
    capability: str,
    allowed: bool,
    direct_dependencies: list[str],
    evidence_ids: list[str],
    evidence_refs: list[dict[str, Any]],
    blocker_chain: list[str],
    reason: str,
    validation_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "capability": capability,
        "allowed": bool(allowed),
        "direct_dependencies": sorted(direct_dependencies),
        "required_evidence": sorted(evidence_ids),
        "evidence_refs_used": sorted(evidence_refs, key=lambda row: (row.get("schema_id", ""), row.get("event_id", ""))),
        "validation_results": sorted(validation_results or [], key=lambda row: str(row.get("schema_id") or "")),
        "blocker_chain": sorted(set(str(item) for item in blocker_chain if str(item))),
        "reason": reason,
    }


def _collect_blockers(capabilities: dict[str, Any], evidence_results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for schema_id, result in sorted(evidence_results.items()):
        if result.get("status") != "OK":
            blockers.append(_blocker(f"EVIDENCE_{result['status']}:{schema_id}", result["reason"], evidence_refs=[result.get("evidence_ref")] if result.get("evidence_ref") else []))
    for capability, row in sorted(capabilities.items()):
        if not row.get("allowed"):
            blockers.append(_blocker(f"CAPABILITY_BLOCKED:{capability}", str(row.get("reason") or ""), evidence_refs=row.get("evidence_refs_used") or []))
    return blockers


def _blocker(blocker_id: str, reason: str, *, evidence_refs: list[Any]) -> dict[str, Any]:
    return {"blocker_id": blocker_id, "reason": reason, "evidence_refs": [ref for ref in evidence_refs if ref]}

def _decision_trace(capabilities: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "capability": capability,
            "allowed": bool(row.get("allowed", False)),
            "direct_dependencies": row.get("direct_dependencies") or [],
            "evidence_refs_used": row.get("evidence_refs_used") or [],
            "validation_results": row.get("validation_results") or [],
            "blocker_chain": row.get("blocker_chain") or [],
            "final_reason": str(row.get("reason") or ""),
        }
        for capability, row in sorted(capabilities.items())
    ]


def _source_refs(evidence_results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    refs = [row["evidence_ref"] for row in evidence_results.values() if row.get("evidence_ref")]
    return sorted(refs, key=lambda row: (row.get("schema_id", ""), row.get("event_id", "")))


def _readiness_blockers(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in blockers if not str(row.get("blocker_id") or "").startswith("CAPABILITY_BLOCKED:TRADE_ADVICE_ALLOWED") and not str(row.get("blocker_id") or "").startswith("CAPABILITY_BLOCKED:AUTONOMOUS_EXECUTION_ALLOWED")]


def _highest_readiness_layer(*, capabilities: dict[str, Any], runtime_truth: str) -> str:
    if runtime_truth != "REAL_RUNTIME":
        return "BLOCKED"
    if capabilities.get("MANUAL_TRADE_CAPTURE_ALLOWED", {}).get("allowed") is True:
        return "MANUAL_TRADE_CAPTURE_ALLOWED"
    if all(capabilities.get(cap, {}).get("allowed") is True for cap in ("DATA_READY", "RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY")):
        return "ADVISORY_ONLY"
    return "BLOCKED"


def _compress_reasons(reasons: list[str]) -> str:
    return "; ".join(sorted(set(str(reason) for reason in reasons if str(reason)))) or "Blocked by RuntimeEvaluation."


def _max_age_seconds(schema_id: str, policy_bundle: dict[str, Any]) -> int:
    freshness = policy_bundle.get("freshness_policy") if isinstance(policy_bundle.get("freshness_policy"), dict) else {}
    by_schema = freshness.get("by_schema_id") if isinstance(freshness.get("by_schema_id"), dict) else {}
    return int(by_schema.get(schema_id) or freshness.get("default_max_age_seconds") or 0)


def _parse_utc(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _deepcopy(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _deepcopy(value[key]) for key in value}
    if isinstance(value, list):
        return [_deepcopy(item) for item in value]
    return value
