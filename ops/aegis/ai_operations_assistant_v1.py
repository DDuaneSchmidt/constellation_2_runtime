from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1

CONTEXT_FAMILY = "aegis_ai_operations_context_v1"
CONTEXT_FILENAME = "ai_operations_context.v1.json"
RESPONSE_FAMILY = "aegis_ai_operations_response_v1"
RESPONSE_FILENAME = "ai_operations_response.v1.json"

PROMPT_VERSION = "ai_operations_assistant_phase1_operational_v1"
MODEL_NAME = "DETERMINISTIC_GROUNDED_OPERATIONS_ASSISTANT"
MODEL_VERSION = "phase1_no_live_llm"
SCHEMA_VERSION = "v1"

ALLOWED_AI_SCOPE = ["explain", "summarize", "compare", "diagnose", "prioritize", "interpret"]
PHASE1_SUPPORTED_INTENTS = {
    "WHAT_HAPPENED",
    "WHAT_CHANGED",
    "NEEDS_ATTENTION",
    "FIX_FIRST",
    "WHY_BLOCKED",
    "OPERATOR_ACTION_REQUIRED",
}
FORBIDDEN_PATTERNS = (
    re.compile(r"\bbuy\b", re.IGNORECASE),
    re.compile(r"\bsell\b", re.IGNORECASE),
    re.compile(r"\bexit\s+now\b", re.IGNORECASE),
    re.compile(r"\bincrease\s+size\b", re.IGNORECASE),
    re.compile(r"\breduce\s+size\b", re.IGNORECASE),
    re.compile(r"\bexecute\s+(a\s+)?trade\b", re.IGNORECASE),
)


def ai_operations_context_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / CONTEXT_FAMILY / str(day_utc) / CONTEXT_FILENAME


def ai_operations_response_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / RESPONSE_FAMILY / str(day_utc) / RESPONSE_FILENAME


def build_ai_operations_context_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    sources = _source_paths(root, day)
    payloads: dict[str, Any] = {}
    source_artifacts: list[dict[str, Any]] = []
    source_hashes: dict[str, str] = {}
    null_reasons: list[dict[str, str]] = []
    for source_id, spec in sources.items():
        path = spec["path"]
        kind = spec.get("kind", "json")
        exists = path.exists()
        content_hash = _file_hash(path) if exists else ""
        if exists:
            source_hashes[source_id] = content_hash
        payloads[source_id] = _read_text(path) if kind == "text" else read_json_v1(path)
        status = "AVAILABLE" if exists else "MISSING"
        source_artifacts.append({
            "source_id": source_id,
            "artifact_family": spec.get("family", source_id),
            "path": str(path),
            "kind": kind,
            "status": status,
            "content_hash": content_hash,
        })
        if not exists:
            null_reasons.append({"source_id": source_id, "reason": "SOURCE_ARTIFACT_MISSING", "path": str(path)})

    evidence = _build_evidence_items(day=day, payloads=payloads, source_artifacts=source_artifacts)
    deterministic_body = {
        "day_utc": day,
        "source_artifact_hashes": source_hashes,
        "included_evidence": evidence,
        "excluded_evidence": [],
        "null_reasons": null_reasons,
        "allowed_ai_scope": ALLOWED_AI_SCOPE,
        "phase_scope": "OPERATIONAL_ONLY",
    }
    context_hash = _stable_hash(deterministic_body)
    payload = {
        "schema_id": CONTEXT_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "context_id": f"AIOPS-CTX-{day}-{context_hash[:12]}",
        "domain": "aegis_operations",
        "target_id": f"OPERATIONS-{day}",
        "day_utc": day,
        "source_day": day,
        "generated_at": generated_at,
        "as_of": generated_at,
        "source_artifacts": source_artifacts,
        "source_artifact_hashes": source_hashes,
        "input_context_hash": context_hash,
        "context_hash": context_hash,
        "data_quality_status": _context_quality(source_artifacts),
        "included_evidence": evidence,
        "excluded_evidence": [],
        "null_reasons": null_reasons,
        "allowed_ai_scope": ALLOWED_AI_SCOPE,
        "supported_question_categories": ["Operational"],
        "unsupported_question_categories": ["Position", "Candidate", "Sleeve", "Research", "Performance"],
        "safety": _safety(),
        "summary": {
            "source_count": len(source_artifacts),
            "available_source_count": sum(1 for row in source_artifacts if row.get("status") == "AVAILABLE"),
            "evidence_count": len(evidence),
            "phase_scope": "OPERATIONAL_ONLY",
        },
    }
    return payload


def write_ai_operations_context_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(ai_operations_context_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def build_ai_operations_response_v1(*, truth_root: Path | str, day_utc: str, question: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    context_path = ai_operations_context_path_v1(truth_root=root, day_utc=day)
    context = read_json_v1(context_path)
    if _context_needs_refresh(root, day, context):
        context = build_ai_operations_context_v1(truth_root=root, day_utc=day)
        write_ai_operations_context_v1(truth_root=root, day_utc=day, payload=context)
    response = answer_ai_operations_question_v1(context=context, question=question)
    existing = read_json_v1(ai_operations_response_path_v1(truth_root=root, day_utc=day))
    history = existing.get("responses") if isinstance(existing.get("responses"), list) else []
    history = [*history[-19:], response]
    payload = {
        "schema_id": RESPONSE_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day,
        "generated_at": response["generated_at"],
        "latest_response": response,
        "responses": history,
        "summary": {
            "response_count": len(history),
            "latest_intent": response.get("intent"),
            "latest_confidence": response.get("confidence"),
            "unsupported_claims_count": len(response.get("unsupported_claims") or []),
            "source_count": len(response.get("source_artifacts") or []),
        },
        "safety": _safety(),
    }
    return payload


def write_ai_operations_response_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(ai_operations_response_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def answer_ai_operations_question_v1(*, context: dict[str, Any], question: str) -> dict[str, Any]:
    generated_at = now_utc_v1()
    q = str(question or "").strip() or "What happened?"
    intent = classify_ai_operations_intent_v1(q)
    evidence = context.get("included_evidence") if isinstance(context.get("included_evidence"), list) else []
    selected = _select_evidence(evidence, intent)
    unsupported_claims: list[str] = []
    if intent == "UNSUPPORTED_PHASE1_SCOPE":
        unsupported_claims.append("Phase 1 supports operational questions only. Position, sleeve, hypothesis, candidate, and performance questions are Phase 2.")
        answer = "I do not have enough evidence to answer that Phase 2 question in this assistant scope. Phase 1 can answer operational questions such as what happened, what needs attention, what is blocked, and what should be fixed first."
        selected = _select_evidence(evidence, "WHAT_HAPPENED")
    elif not selected:
        unsupported_claims.append("No deterministic operational evidence was selected for this question.")
        answer = "I do not have enough evidence to answer."
    else:
        answer = _compose_answer(intent=intent, evidence=selected, context=context)
    confidence = _confidence(context=context, selected=selected, unsupported_claims=unsupported_claims)
    context_hash = str(context.get("context_hash") or context.get("input_context_hash") or "")
    context_day = str(context.get("day_utc") or "")
    source_day = str(context.get("source_day") or context_day)
    if context_day and source_day and context_day != source_day:
        unsupported_claims.append(f"Context day {context_day} does not match source day {source_day}.")
        answer = "I do not have enough current-day evidence to answer safely because the operations context day and source day differ."
    response_body = {
        "question": q,
        "requested_day": context_day,
        "context_day": context_day,
        "source_day": source_day,
        "intent": intent,
        "context_hash": context_hash,
        "selected_evidence": selected,
        "answer": answer,
        "confidence": confidence,
        "unsupported_claims": unsupported_claims,
        "source_artifacts": _sources_for_selected(context, selected),
        "source_artifact_hashes": context.get("source_artifact_hashes") or {},
    }
    response_id = f"AIOPS-RSP-{_stable_hash({'question': q, 'context_hash': context_hash, 'intent': intent})[:16]}"
    return {
        "response_id": response_id,
        "context_id": context.get("context_id") or "",
        "context_hash": context_hash,
        "input_context_hash": context.get("input_context_hash") or context_hash,
        "prompt_version": PROMPT_VERSION,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "temperature": 0,
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        **response_body,
        "supporting_artifacts": _sources_for_selected(context, selected),
        "safety": _safety(),
    }


def classify_ai_operations_intent_v1(question: str) -> str:
    q = question.lower()
    phase2_terms = ["position", "candidate", "sleeve", "hypothesis", "research", "performance", "p&l", "pnl"]
    if any(term in q for term in phase2_terms) and not any(term in q for term in ["blocked", "needs attention", "fix", "operator action"]):
        return "UNSUPPORTED_PHASE1_SCOPE"
    if "fix" in q or "first" in q or "repair" in q:
        return "FIX_FIRST"
    if "attention" in q or "needs" in q:
        return "NEEDS_ATTENTION"
    if "operator action" in q or "action required" in q:
        return "OPERATOR_ACTION_REQUIRED"
    if "blocked" in q or "blocker" in q or "why" in q:
        return "WHY_BLOCKED"
    if "changed" in q or "change" in q:
        return "WHAT_CHANGED"
    return "WHAT_HAPPENED"


def run_ai_operations_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    context = read_json_v1(ai_operations_context_path_v1(truth_root=root, day_utc=day))
    if _context_needs_refresh(root, day, context):
        context = build_ai_operations_context_v1(truth_root=root, day_utc=day)
        write_ai_operations_context_v1(truth_root=root, day_utc=day, payload=context)
    response_payload = read_json_v1(ai_operations_response_path_v1(truth_root=root, day_utc=day))
    latest = response_payload.get("latest_response") if isinstance(response_payload.get("latest_response"), dict) else {}
    if not latest or str(latest.get("context_hash") or "") != str(context.get("context_hash") or ""):
        response_payload = build_ai_operations_response_v1(truth_root=root, day_utc=day, question="What should be fixed first?")
        write_ai_operations_response_v1(truth_root=root, day_utc=day, payload=response_payload)
        latest = response_payload.get("latest_response") if isinstance(response_payload.get("latest_response"), dict) else {}
    failures: list[dict[str, str]] = []
    if not context:
        failures.append({"check": "context_exists", "reason": "aegis_ai_operations_context_v1 missing"})
    if not latest:
        failures.append({"check": "response_exists", "reason": "aegis_ai_operations_response_v1 latest_response missing"})
    if latest:
        if not latest.get("answer"):
            failures.append({"check": "answer_present", "reason": "answer missing"})
        if not latest.get("confidence"):
            failures.append({"check": "confidence_present", "reason": "confidence missing"})
        if not latest.get("context_hash"):
            failures.append({"check": "context_hash_present", "reason": "context_hash missing"})
        if not latest.get("source_artifacts"):
            failures.append({"check": "source_references_present", "reason": "source_artifacts missing"})
        if latest.get("unsupported_claims"):
            failures.append({"check": "unsupported_claims_empty", "reason": "unsupported_claims present"})
        answer = str(latest.get("answer") or "")
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(answer):
                failures.append({"check": "forbidden_trade_language_absent", "reason": f"forbidden language matched {pattern.pattern}"})
                break
    if context:
        if _context_needs_refresh(root, day, context):
            failures.append({"check": "context_fresh_for_day", "reason": "saved context is missing, wrong-day, or no longer matches current source artifact hashes/status"})
        if str(context.get("day_utc") or "") != day or str(context.get("source_day") or context.get("day_utc") or "") != day:
            failures.append({"check": "context_day_matches_requested_day", "reason": f"context_day={context.get('day_utc')} source_day={context.get('source_day')} requested={day}"})
        if not context.get("source_artifact_hashes"):
            failures.append({"check": "source_artifact_hashes_present", "reason": "source_artifact_hashes missing"})
        if not context.get("included_evidence"):
            failures.append({"check": "included_evidence_present", "reason": "included_evidence missing"})
    if latest:
        if str(latest.get("context_day") or day) != day or str(latest.get("source_day") or day) != day:
            failures.append({"check": "response_day_matches_requested_day", "reason": f"context_day={latest.get('context_day')} source_day={latest.get('source_day')} requested={day}"})
    failures.extend(_semantic_self_check_failures(context=context, day=day))
    return {
        "ok": not failures,
        "day_utc": day,
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "context_path": str(ai_operations_context_path_v1(truth_root=root, day_utc=day)),
        "response_path": str(ai_operations_response_path_v1(truth_root=root, day_utc=day)),
        "safety": _safety(),
    }



def _semantic_self_check_failures(*, context: dict[str, Any], day: str) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    questions = [
        "What should be fixed first?",
        "Why is runtime blocked?",
        "Is operator action required?",
        "Why is data readiness blocked?",
    ]
    for question in questions:
        response = answer_ai_operations_question_v1(context=context, question=question)
        label = question.lower().replace(" ", "_").replace("?", "")
        if str(response.get("requested_day") or "") != day or str(response.get("source_day") or "") != day or str(response.get("context_day") or "") != day:
            failures.append({"check": f"semantic_day_match:{label}", "reason": f"requested={response.get('requested_day')} source={response.get('source_day')} context={response.get('context_day')} expected={day}"})
        if not response.get("context_hash"):
            failures.append({"check": f"semantic_context_hash:{label}", "reason": "context_hash missing"})
        if not response.get("source_artifacts"):
            failures.append({"check": f"semantic_sources:{label}", "reason": "source_artifacts missing"})
        if not response.get("confidence"):
            failures.append({"check": f"semantic_confidence:{label}", "reason": "confidence missing"})
        if question == "Is operator action required?":
            queue_summary = _queue_summary_from_context(context)
            expected = _int_value(queue_summary.get("operator_action_required_count"))
            answer = str(response.get("answer") or "")
            if expected == 0 and "37 operator-action" in answer:
                failures.append({"check": "semantic_operator_action_count", "reason": "answer repeats stale inflated operator-action count"})
            if expected == 0 and "No row-level operator action is required" not in answer:
                failures.append({"check": "semantic_operator_action_count", "reason": "answer does not reflect zero operator-action rows"})
        if question == "Why is data readiness blocked?":
            answer = str(response.get("answer") or "").lower()
            if "data" not in answer and "runtime" not in answer and "block" not in answer:
                failures.append({"check": "semantic_data_readiness_answer", "reason": "answer does not address data/runtime blocker evidence"})
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(str(response.get("answer") or "")):
                failures.append({"check": f"semantic_forbidden_trade_language:{label}", "reason": f"forbidden language matched {pattern.pattern}"})
                break
    return failures


def _queue_summary_from_context(context: dict[str, Any]) -> dict[str, Any]:
    for row in context.get("included_evidence") or []:
        if isinstance(row, dict) and row.get("evidence_id") == "queue_audit":
            details = row.get("details") if isinstance(row.get("details"), dict) else {}
            summary = details.get("summary") if isinstance(details.get("summary"), dict) else {}
            return summary
    return {}

def _context_needs_refresh(root: Path, day: str, context: Any) -> bool:
    if not isinstance(context, dict) or not context:
        return True
    if str(context.get("day_utc") or "") != str(day):
        return True
    if str(context.get("source_day") or context.get("day_utc") or "") != str(day):
        return True
    current_hashes: dict[str, str] = {}
    current_statuses: dict[str, str] = {}
    for source_id, spec in _source_paths(root, day).items():
        path = spec["path"]
        current_statuses[source_id] = "AVAILABLE" if path.exists() else "MISSING"
        if path.exists():
            current_hashes[source_id] = _file_hash(path)
    saved_hashes = context.get("source_artifact_hashes") if isinstance(context.get("source_artifact_hashes"), dict) else {}
    saved_statuses = {str(row.get("source_id")): str(row.get("status")) for row in context.get("source_artifacts", []) if isinstance(row, dict)}
    return current_hashes != saved_hashes or current_statuses != saved_statuses


def _source_paths(root: Path, day: str) -> dict[str, dict[str, Any]]:
    reports = root / "reports"
    return {
        "verified_runtime_graph": {"family": "aegis_verified_runtime_graph_v1", "kind": "json", "path": reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json"},
        "audit_handoff": {"family": "aegis_audit_handoff_v1", "kind": "text", "path": reports / "aegis_audit_handoff_v1" / day / "aegis_audit_handoff.txt"},
        "control_packet": {"family": "aegis_chatgpt_control_packet_v1", "kind": "json", "path": reports / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json"},
        "hydrate_packet": {"family": "aegis_chatgpt_hydrate_packet_v1", "kind": "text", "path": reports / "aegis_verified_runtime_graph_v1" / day / "chatgpt_hydrate_packet.v1.md"},
        "queue_audit": {"family": "aegis_command_center_queue_audit_v1", "kind": "json", "path": reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json"},
        "operator_cockpit": {"family": "aegis_canonical_operator_state_v1", "kind": "json", "path": reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"},
    }


def _build_evidence_items(*, day: str, payloads: dict[str, Any], source_artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    graph = payloads.get("verified_runtime_graph") if isinstance(payloads.get("verified_runtime_graph"), dict) else {}
    control = payloads.get("control_packet") if isinstance(payloads.get("control_packet"), dict) else {}
    queue = payloads.get("queue_audit") if isinstance(payloads.get("queue_audit"), dict) else {}
    cockpit = payloads.get("operator_cockpit") if isinstance(payloads.get("operator_cockpit"), dict) else {}
    handoff = str(payloads.get("audit_handoff") or "")
    hydrate = str(payloads.get("hydrate_packet") or "")
    queue_summary = queue.get("summary") if isinstance(queue.get("summary"), dict) else {}
    runtime_blockers = _string_list(graph.get("runtime_blockers"))
    audit_blockers = _string_list(graph.get("audit_blockers"))
    actions = _string_list(control.get("next_operator_actions"))
    stale_sources = _rows(control, "stale_or_missing_sources")
    missing_inputs = _rows(cockpit, "missing_inputs")
    warnings = _rows(cockpit, "warnings")
    items = [
        {
            "evidence_id": "runtime_status",
            "title": "Runtime status",
            "summary": f"Verified graph is {graph.get('graph_status') or 'UNKNOWN'}; runtime readiness is {graph.get('runtime_readiness_status') or control.get('readiness_state', {}).get('classification') or 'UNKNOWN'}.",
            "details": {
                "graph_status": graph.get("graph_status"),
                "runtime_readiness_status": graph.get("runtime_readiness_status"),
                "active_mode_readiness_status": graph.get("active_mode_readiness_status"),
                "runtime_truth_classification": control.get("runtime_truth_classification"),
                "reason_if_blocked": control.get("reason_if_blocked"),
            },
            "source_ids": ["verified_runtime_graph", "control_packet"],
            "intents": ["WHAT_HAPPENED", "WHAT_CHANGED", "WHY_BLOCKED", "OPERATOR_ACTION_REQUIRED"],
        },
        {
            "evidence_id": "policy_gates",
            "title": "Safety and policy gates",
            "summary": "Trade advice, broker submit/transmit, live trading, and autonomous execution remain disabled by policy.",
            "details": {
                "trade_advice_allowed": graph.get("policy_gates", {}).get("trade_advice_allowed", control.get("trade_advice_allowed")),
                "broker_submit_transmit_policy": graph.get("policy_gates", {}).get("broker_submit_transmit_policy"),
                "autonomous_execution_policy": graph.get("policy_gates", {}).get("autonomous_execution_policy"),
                "autonomous_execution_allowed": control.get("autonomous_execution_allowed"),
                "manual_trade_capture_allowed": control.get("manual_trade_capture_allowed"),
            },
            "source_ids": ["verified_runtime_graph", "control_packet"],
            "intents": ["WHAT_HAPPENED", "WHY_BLOCKED", "OPERATOR_ACTION_REQUIRED"],
        },
        {
            "evidence_id": "runtime_blockers",
            "title": "Runtime blockers",
            "summary": f"{len(runtime_blockers)} runtime blocker(s) and {len(audit_blockers)} audit blocker(s) are present in the verified graph.",
            "details": {"runtime_blockers": runtime_blockers[:12], "audit_blockers": audit_blockers[:12]},
            "source_ids": ["verified_runtime_graph"],
            "intents": ["WHY_BLOCKED", "NEEDS_ATTENTION", "FIX_FIRST", "WHAT_HAPPENED"],
        },
        {
            "evidence_id": "next_operator_actions",
            "title": "Next operator actions",
            "summary": f"{len(actions)} repair action(s) are published by the control packet.",
            "details": {"next_operator_actions": actions[:8]},
            "source_ids": ["control_packet", "audit_handoff"],
            "intents": ["FIX_FIRST", "NEEDS_ATTENTION", "OPERATOR_ACTION_REQUIRED"],
        },
        {
            "evidence_id": "queue_audit",
            "title": "Command Center queue audit",
            "summary": _queue_summary_text(queue_summary, queue),
            "details": {"summary": queue_summary or _compact_queue(queue)},
            "source_ids": ["queue_audit"],
            "intents": ["NEEDS_ATTENTION", "OPERATOR_ACTION_REQUIRED", "WHAT_HAPPENED"],
        },
        {
            "evidence_id": "operator_cockpit_read_model",
            "title": "Operator cockpit read model",
            "summary": f"Canonical operator state has {len(_rows(cockpit, 'actions_required'))} action row(s), {len(missing_inputs)} missing input row(s), and {len(warnings)} warning row(s).",
            "details": {
                "status": cockpit.get("status"),
                "generated_at_utc": cockpit.get("generated_at_utc"),
                "actions_required_count": len(_rows(cockpit, "actions_required")),
                "missing_inputs_count": len(missing_inputs),
                "warnings_count": len(warnings),
            },
            "source_ids": ["operator_cockpit"],
            "intents": ["WHAT_HAPPENED", "WHAT_CHANGED", "NEEDS_ATTENTION", "OPERATOR_ACTION_REQUIRED"],
        },
        {
            "evidence_id": "missing_or_stale_sources",
            "title": "Missing or stale sources",
            "summary": f"{len(stale_sources)} stale or missing source row(s) are listed by the control packet.",
            "details": {"stale_or_missing_sources": stale_sources[:12]},
            "source_ids": ["control_packet"],
            "intents": ["WHY_BLOCKED", "FIX_FIRST", "NEEDS_ATTENTION"],
        },
        {
            "evidence_id": "audit_handoff_summary",
            "title": "Audit handoff summary",
            "summary": _first_nonempty(_line_with_prefix(handoff, "advisory_status:"), _line_with_prefix(handoff, "runtime_truth_classification:"), "Audit handoff text is available." if handoff else "Audit handoff text is missing."),
            "details": {"excerpt": "\n".join([line for line in handoff.splitlines()[:30]])},
            "source_ids": ["audit_handoff"],
            "intents": ["WHAT_HAPPENED", "WHY_BLOCKED", "FIX_FIRST", "OPERATOR_ACTION_REQUIRED"],
        },
        {
            "evidence_id": "hydrate_packet_summary",
            "title": "Hydrate packet availability",
            "summary": "Hydrate packet is available for operator-safe explanatory context." if hydrate else "Hydrate packet is missing for this day.",
            "details": {"excerpt": "\n".join(hydrate.splitlines()[:20])},
            "source_ids": ["hydrate_packet"],
            "intents": ["WHAT_HAPPENED", "WHAT_CHANGED"],
        },
    ]
    available_sources = {row["source_id"] for row in source_artifacts if row.get("status") == "AVAILABLE"}
    for item in items:
        item["source_available"] = [sid for sid in item.get("source_ids", []) if sid in available_sources]
        item["source_missing"] = [sid for sid in item.get("source_ids", []) if sid not in available_sources]
    return items


def _compose_answer(*, intent: str, evidence: list[dict[str, Any]], context: dict[str, Any]) -> str:
    by_id = {str(row.get("evidence_id")): row for row in evidence}
    runtime = by_id.get("runtime_status") or {}
    blockers = by_id.get("runtime_blockers") or {}
    actions = by_id.get("next_operator_actions") or {}
    queue = by_id.get("queue_audit") or {}
    missing = by_id.get("missing_or_stale_sources") or {}
    policy = by_id.get("policy_gates") or {}
    cockpit = by_id.get("operator_cockpit_read_model") or {}
    action_rows = _string_list((actions.get("details") or {}).get("next_operator_actions"))
    first_action = action_rows[0] if action_rows else "No repair action was published in the selected evidence."
    blocker_rows = _string_list((blockers.get("details") or {}).get("runtime_blockers"))
    first_blocker = blocker_rows[0] if blocker_rows else "No runtime blocker was published in the selected evidence."
    queue_details = (queue.get("details") or {}).get("summary") or {}
    operator_count = _int_value(queue_details.get("operator_action_required_count"))
    attention_count = _int_value(queue_details.get("needs_attention_rows_audited") or queue_details.get("needs_attention_count"))
    if intent == "FIX_FIRST":
        return f"The first operational fix published by the evidence is: {first_action} This is repair guidance for Aegis evidence readiness, not trade advice or execution guidance."
    if intent == "NEEDS_ATTENTION":
        return f"Command Center evidence shows {operator_count} operator-action row(s) and {attention_count} needs-attention row(s). The runtime evidence also lists {len(blocker_rows)} blocker(s); the first blocker is: {first_blocker}"
    if intent == "OPERATOR_ACTION_REQUIRED":
        if operator_count:
            return f"Yes. The queue audit reports {operator_count} operator-action row(s). Use the Command Center action queue for the row-level decision."
        return f"No row-level operator action is required by the queue audit. The audit still reviewed {attention_count} needs-attention row(s), but they are not classified as operator-actionable. System-level readiness may still be blocked; review the published repair action if you are preparing the runtime."
    if intent == "WHY_BLOCKED":
        return f"The verified runtime graph reports blocked readiness. The first blocker is: {first_blocker} The control packet's first repair action is: {first_action}"
    if intent == "WHAT_CHANGED":
        generated = context.get("generated_at") or "unknown"
        return f"The current evidence context was generated at {generated}. {runtime.get('summary', '')} {cockpit.get('summary', '')} {missing.get('summary', '')}"
    return f"Aegis is operating from the current verified runtime and operator artifacts. {runtime.get('summary', '')} {queue.get('summary', '')} {policy.get('summary', '')}"


def _select_evidence(evidence: list[dict[str, Any]], intent: str) -> list[dict[str, Any]]:
    selected = [row for row in evidence if intent in set(row.get("intents") or [])]
    return selected[:6]


def _sources_for_selected(context: dict[str, Any], selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    wanted = {sid for row in selected for sid in (row.get("source_ids") or [])}
    sources = context.get("source_artifacts") if isinstance(context.get("source_artifacts"), list) else []
    return [row for row in sources if row.get("source_id") in wanted]


def _confidence(*, context: dict[str, Any], selected: list[dict[str, Any]], unsupported_claims: list[str]) -> str:
    if unsupported_claims:
        return "LOW"
    sources = context.get("source_artifacts") if isinstance(context.get("source_artifacts"), list) else []
    available = sum(1 for row in sources if row.get("status") == "AVAILABLE")
    selected_sources = {sid for row in selected for sid in (row.get("source_ids") or [])}
    if available >= 5 and len(selected_sources) >= 2:
        return "HIGH"
    if available >= 3:
        return "MEDIUM"
    return "LOW"


def _context_quality(sources: list[dict[str, Any]]) -> str:
    required = {"verified_runtime_graph", "control_packet", "audit_handoff"}
    available = {row.get("source_id") for row in sources if row.get("status") == "AVAILABLE"}
    if required.issubset(available):
        return "CANONICAL"
    if available:
        return "PARTIAL"
    return "NOT_CANONICAL"


def _safety() -> dict[str, bool]:
    return {
        "read_only": True,
        "explains_only": True,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "live_trading_allowed": False,
        "autonomous_live_trading_allowed": False,
        "system_control_allowed": False,
    }


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []



def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0

def _compact_queue(queue: dict[str, Any]) -> dict[str, Any]:
    return {key: queue.get(key) for key in ("total_rows", "operator_action_required_count", "needs_attention_rows_audited", "count_by_classification") if key in queue}


def _queue_summary_text(summary: dict[str, Any], queue: dict[str, Any]) -> str:
    data = summary or _compact_queue(queue)
    if not data:
        return "Command Center queue audit is missing."
    return f"Queue audit totals: {data.get('total_rows', 'unknown')} row(s), {data.get('operator_action_required_count', 0)} operator-action row(s), {data.get('needs_attention_rows_audited', data.get('needs_attention_count', 0))} needs-attention row(s)."


def _line_with_prefix(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if line.strip().startswith(prefix):
            return line.strip()
    return ""


def _first_nonempty(*values: str) -> str:
    for value in values:
        if str(value or "").strip():
            return str(value).strip()
    return ""


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
