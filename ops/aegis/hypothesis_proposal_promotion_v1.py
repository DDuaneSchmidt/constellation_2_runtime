from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1

SAFETY_STATEMENT = "This is paper research only. Not trade advice. No broker execution. No live trading."

SAFETY = {
    "paper_research_only": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_real_money_action_allowed": False,
    "automatic_paper_sleeve_creation_allowed": False,
    "safety_statement": SAFETY_STATEMENT,
}

STATES = {
    "PROPOSED",
    "REJECTED_DUPLICATE",
    "REJECTED_UNTESTABLE",
    "REJECTED_LOW_SAMPLE_RATE",
    "NEEDS_DATA",
    "READY_FOR_SHADOW_TRIAL",
    "SHADOW_VALIDATION_RUNNING",
    "SHADOW_VALIDATION_FAILED",
    "SHADOW_VALIDATION_PASSED",
    "PAPER_PROMOTION_RECOMMENDED",
    "PAPER_PROMOTION_APPROVED",
    "PAPER_PROMOTION_REJECTED",
    "PAPER_PROMOTION_DEFERRED",
}

REPORT_SPECS = {
    "promotion": ("aegis_hypothesis_proposal_promotion_v1", "promotion_pipeline.v1.json"),
    "evidence": ("aegis_hypothesis_evidence_packet_v1", "evidence_packets.v1.json"),
    "shadow": ("aegis_hypothesis_shadow_trial_v1", "shadow_trials.v1.json"),
    "packet": ("aegis_hypothesis_promotion_packet_v1", "promotion_packets.v1.json"),
    "queue": ("aegis_paper_promotion_approval_queue_v1", "approval_queue.v1.json"),
}


def report_path_v1(truth_root: Path | str, key: str, day_utc: str) -> Path:
    family, filename = REPORT_SPECS[key]
    return Path(truth_root) / "reports" / family / str(day_utc) / filename


def promotion_pipeline_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "promotion", day_utc)


def evidence_packets_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "evidence", day_utc)


def shadow_trials_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "shadow", day_utc)


def promotion_packets_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "packet", day_utc)


def approval_queue_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "queue", day_utc)


def approval_events_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root) / "reports" / "aegis_paper_promotion_approval_queue_v1" / str(day_utc) / "approval_events.v1.jsonl"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        return rows
    except (OSError, json.JSONDecodeError):
        return []


def _write_jsonl_append(path: Path, row: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(row), sort_keys=True, separators=(",", ":")) + "\n")
    return path


def _research_store_root(repo_root: Path | None = None) -> Path:
    return (repo_root or _repo_root()) / "research_lab" / "research_store"


def _proposal_artifact_path(store: Path, row: Mapping[str, Any]) -> Path:
    uri = str(row.get("storage_uri") or "")
    if uri.startswith("research://event_intake/hypothesis_proposals/"):
        return store / "event_intake" / "hypothesis_proposals" / uri.rsplit("/", 1)[-1]
    return store / "event_intake" / "hypothesis_proposals" / f"{row.get('hypothesis_proposal_id')}.json"


def _load_readiness(store: Path, hypothesis_proposal_id: str) -> dict[str, Any]:
    registry = _read_jsonl(store / "registries" / "research_readiness_assessments.jsonl")
    matches = [row for row in registry if str(row.get("hypothesis_proposal_id") or "") == hypothesis_proposal_id]
    if not matches:
        return {}
    uri = str(matches[-1].get("storage_uri") or "")
    filename = uri.rsplit("/", 1)[-1] if uri else f"{matches[-1].get('research_readiness_assessment_id')}.json"
    return read_json_v1(store / "research_intake" / "readiness_assessments" / filename)


def _load_priority(store: Path, hypothesis_proposal_id: str) -> dict[str, Any]:
    registry = _read_jsonl(store / "registries" / "proposal_priority_scores.jsonl")
    matches = [row for row in registry if str(row.get("hypothesis_proposal_id") or "") == hypothesis_proposal_id]
    if not matches:
        return {}
    uri = str(matches[-1].get("storage_uri") or "")
    filename = uri.rsplit("/", 1)[-1] if uri else f"{matches[-1].get('proposal_priority_score_id')}.json"
    return read_json_v1(store / "research_intake" / "priority_scores" / filename)


def _load_cluster(store: Path, proposal: Mapping[str, Any]) -> dict[str, Any]:
    cluster_id = str(proposal.get("event_cluster_id") or "")
    if not cluster_id:
        return {}
    return read_json_v1(store / "event_intake" / "clusters" / f"{cluster_id}.json")


def _proposal_rows(store: Path) -> list[dict[str, Any]]:
    rows = _read_jsonl(store / "registries" / "hypothesis_proposals.jsonl")
    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        proposal_id = str(row.get("hypothesis_proposal_id") or "")
        if proposal_id:
            dedup[proposal_id] = row
    return [dedup[key] for key in sorted(dedup)]


def _frequency_from_proposal(proposal: Mapping[str, Any], readiness: Mapping[str, Any]) -> tuple[str, int]:
    family = str(proposal.get("event_family_id") or "")
    if "macro" in family:
        return "monthly_or_event_driven", 2
    if readiness.get("intraday_market_data_available") is True:
        return "weekly", 8
    return "unknown", 0


def _instrument_universe(proposal: Mapping[str, Any], readiness: Mapping[str, Any], cluster: Mapping[str, Any]) -> list[str]:
    symbols = readiness.get("required_symbols") or proposal.get("proposed_universe") or cluster.get("common_symbols") or []
    return sorted({str(item).strip().upper() for item in symbols if str(item).strip()})


def _duplicate_key(statement: str) -> str:
    return " ".join(statement.lower().replace("-", " ").split())


def build_evidence_packets_v1(*, truth_root: Path | str, day_utc: str, repo_root: Path | None = None) -> dict[str, Any]:
    store = _research_store_root(repo_root)
    generated_at = f"{day_utc}T00:00:00Z"
    seen: set[str] = set()
    packets: list[dict[str, Any]] = []
    stale_rows: list[dict[str, Any]] = []
    for row in _proposal_rows(store):
        proposal_path = _proposal_artifact_path(store, row)
        proposal = read_json_v1(proposal_path)
        proposal_id = str(row.get("hypothesis_proposal_id") or proposal.get("hypothesis_proposal_id") or "")
        registry_hash = str(row.get("content_hash") or "")
        proposal_hash = str(proposal.get("content_hash") or "")
        full_artifact_hash = _stable_hash(proposal) if proposal else ""
        if not proposal or not proposal_id or registry_hash != proposal_hash:
            stale_rows.append({
                "hypothesis_id": proposal_id,
                "input_proposal_file": str(proposal_path),
                "input_proposal_hash": proposal_hash,
                "registry_proposal_hash": registry_hash,
                "state": "REJECTED_UNTESTABLE",
                "reason_codes": ["STALE_OR_MISMATCHED_PROPOSAL_ARTIFACT"],
            })
            continue
        readiness = _load_readiness(store, proposal_id)
        priority = _load_priority(store, proposal_id)
        cluster = _load_cluster(store, proposal)
        statement = str(proposal.get("hypothesis") or proposal.get("title") or "")
        universe = _instrument_universe(proposal, readiness, cluster)
        frequency, samples_per_month = _frequency_from_proposal(proposal, readiness)
        key = _duplicate_key(statement)
        duplicate = key in seen
        seen.add(key)
        readiness_blockers = [str(item) for item in readiness.get("blocking_items") or [] if str(item)]
        required_fields = ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"]
        testability_score = 0
        testability_score += 25 if statement else 0
        testability_score += 20 if universe else 0
        testability_score += 20 if readiness.get("intraday_market_data_available") is True else 0
        testability_score += 15 if not readiness_blockers else 0
        testability_score += 10 if samples_per_month >= 4 else 0
        testability_score += 10 if priority.get("score") is not None else 0
        packet = {
            "schema_id": "aegis_hypothesis_evidence_packet_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": proposal_id,
            "hypothesis_statement": statement,
            "source_observation_cluster": proposal.get("event_cluster_id") or cluster.get("event_cluster_id") or "",
            "data_sources_used": ["research_store:hypothesis_proposals", "research_store:readiness_assessments", "research_store:priority_scores"],
            "instrument_universe": universe,
            "entry_logic": "Research-defined event entry after source event observation; no trade instruction.",
            "exit_logic": "Research-defined paper observation exit after expected holding period or validation window; no broker instruction.",
            "expected_holding_period": "1-5 sessions" if samples_per_month >= 4 else "event dependent",
            "expected_sample_frequency": frequency,
            "expected_samples_per_month": samples_per_month,
            "duplicate_check": {"is_duplicate": duplicate, "duplicate_key": key, "overlap_with_existing_sleeves": False},
            "testability_score": testability_score,
            "known_risks": _known_risks(proposal, readiness, readiness_blockers),
            "required_evidence_fields": required_fields,
            "readiness": readiness,
            "priority": priority,
            "input_proposal_file": str(proposal_path),
            "input_proposal_hash": proposal_hash,
            "input_proposal_full_artifact_hash": full_artifact_hash,
            "source_artifact_paths": [str(proposal_path)],
            "safety": dict(SAFETY),
        }
        packet["evidence_packet_hash"] = _stable_hash({**packet, "evidence_packet_hash": ""})
        packets.append(packet)
    payload = {
        "schema_id": "aegis_hypothesis_evidence_packet_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "evidence_packets": packets,
        "stale_or_rejected_inputs": stale_rows,
        "summary": {"packet_count": len(packets), "stale_or_rejected_input_count": len(stale_rows)},
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def _known_risks(proposal: Mapping[str, Any], readiness: Mapping[str, Any], blockers: list[str]) -> list[str]:
    risks = ["Research-only hypothesis may fail validation or duplicate an existing research pattern."]
    family = str(proposal.get("event_family_id") or "")
    if "macro" in family:
        risks.append("Macro calendar dependency can block repeatable sample generation.")
    if blockers:
        risks.append(f"Readiness blockers: {', '.join(blockers)}.")
    if readiness.get("macro_event_calendar_available") is False and "macro" in family:
        risks.append("Macro event calendar is not available.")
    return risks


def build_shadow_trials_v1(evidence_payload: Mapping[str, Any], *, day_utc: str) -> dict[str, Any]:
    generated_at = f"{day_utc}T00:00:00Z"
    trials: list[dict[str, Any]] = []
    for packet in evidence_payload.get("evidence_packets") or []:
        if not isinstance(packet, Mapping):
            continue
        triage = _triage_packet(packet)
        transitions = [{"from_state": "", "to_state": "PROPOSED", "reason_codes": ["PROPOSAL_LOADED"]}]
        transitions.append({"from_state": "PROPOSED", "to_state": triage["triage_state"], "reason_codes": triage["reason_codes"]})
        shadow_state = triage["triage_state"]
        statement_text = str(packet.get("hypothesis_statement") or "").lower()
        checks = {
            "data_availability": triage["triage_state"] == "READY_FOR_SHADOW_TRIAL",
            "sample_generation": int(packet.get("expected_samples_per_month") or 0) >= 4,
            "signal_stability": int(packet.get("testability_score") or 0) >= 70 and "unstable" not in statement_text,
            "duplicate_overlap": not bool((packet.get("duplicate_check") or {}).get("is_duplicate")),
            "time_to_statistical_sufficiency": int(packet.get("expected_samples_per_month") or 0) > 0,
            "paper_readiness_feasibility": triage["triage_state"] == "READY_FOR_SHADOW_TRIAL",
        }
        if triage["triage_state"] == "READY_FOR_SHADOW_TRIAL":
            transitions.append({"from_state": "READY_FOR_SHADOW_TRIAL", "to_state": "SHADOW_VALIDATION_RUNNING", "reason_codes": ["SHADOW_TRIAL_STARTED"]})
            if all(checks.values()):
                shadow_state = "SHADOW_VALIDATION_PASSED"
                reason_codes = ["SHADOW_TRIAL_CHECKS_PASSED"]
            else:
                shadow_state = "SHADOW_VALIDATION_FAILED"
                reason_codes = [f"SHADOW_CHECK_FAILED_{key.upper()}" for key, ok in checks.items() if not ok]
            transitions.append({"from_state": "SHADOW_VALIDATION_RUNNING", "to_state": shadow_state, "reason_codes": reason_codes})
        trial = {
            "schema_id": "aegis_hypothesis_shadow_trial_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": packet.get("hypothesis_id"),
            "evidence_packet_hash": packet.get("evidence_packet_hash"),
            "triage_decision": triage,
            "shadow_trial_result": shadow_state,
            "shadow_checks": checks,
            "expected_time_to_statistical_sufficiency": _timeline(packet),
            "state_transition_history": transitions,
            "source_artifact_paths": packet.get("source_artifact_paths") or [],
            "safety": dict(SAFETY),
        }
        trial["shadow_trial_hash"] = _stable_hash({**trial, "shadow_trial_hash": ""})
        trials.append(trial)
    payload = {
        "schema_id": "aegis_hypothesis_shadow_trial_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "shadow_trials": trials,
        "summary": {
            "trial_count": len(trials),
            "passed_count": sum(1 for row in trials if row["shadow_trial_result"] == "SHADOW_VALIDATION_PASSED"),
            "failed_count": sum(1 for row in trials if row["shadow_trial_result"] == "SHADOW_VALIDATION_FAILED"),
            "needs_data_count": sum(1 for row in trials if row["shadow_trial_result"] == "NEEDS_DATA"),
        },
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def _triage_packet(packet: Mapping[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    if bool((packet.get("duplicate_check") or {}).get("is_duplicate")):
        return {"triage_state": "REJECTED_DUPLICATE", "reason_codes": ["DUPLICATE_PROPOSAL"], "autonomous_decision": "reject_automatically"}
    if not packet.get("hypothesis_statement") or not packet.get("instrument_universe"):
        return {"triage_state": "REJECTED_UNTESTABLE", "reason_codes": ["MISSING_TESTABLE_STATEMENT_OR_UNIVERSE"], "autonomous_decision": "reject_automatically"}
    if int(packet.get("expected_samples_per_month") or 0) < 1:
        return {"triage_state": "REJECTED_LOW_SAMPLE_RATE", "reason_codes": ["EXPECTED_SAMPLE_RATE_TOO_LOW"], "autonomous_decision": "reject_automatically"}
    readiness = packet.get("readiness") if isinstance(packet.get("readiness"), Mapping) else {}
    blockers = [str(item) for item in readiness.get("blocking_items") or [] if str(item)]
    if blockers or readiness.get("ready_for_research") is False:
        reason_codes = blockers or ["READINESS_NOT_READY_FOR_RESEARCH"]
        return {"triage_state": "NEEDS_DATA", "reason_codes": reason_codes, "autonomous_decision": "needs_more_data"}
    if int(packet.get("expected_samples_per_month") or 0) < 4:
        return {"triage_state": "REJECTED_LOW_SAMPLE_RATE", "reason_codes": ["EXPECTED_SAMPLE_RATE_BELOW_WEEKLY"], "autonomous_decision": "reject_automatically"}
    if int(packet.get("testability_score") or 0) < 70:
        return {"triage_state": "REJECTED_UNTESTABLE", "reason_codes": ["TESTABILITY_SCORE_BELOW_THRESHOLD"], "autonomous_decision": "reject_automatically"}
    return {"triage_state": "READY_FOR_SHADOW_TRIAL", "reason_codes": ["TRIAGE_READY_FOR_SHADOW_TRIAL"], "autonomous_decision": "ready_for_shadow_trial"}


def _timeline(packet: Mapping[str, Any]) -> str:
    samples = int(packet.get("expected_samples_per_month") or 0)
    if samples >= 8:
        return "about 3 months for 20+ samples"
    if samples >= 4:
        return "about 5 months for 20+ samples"
    if samples > 0:
        return "more than 12 months unless sample rate improves"
    return "unknown until data is available"


def build_promotion_packets_v1(evidence_payload: Mapping[str, Any], shadow_payload: Mapping[str, Any], *, day_utc: str) -> dict[str, Any]:
    generated_at = f"{day_utc}T00:00:00Z"
    packet_by_id = {str(row.get("hypothesis_id")): row for row in evidence_payload.get("evidence_packets") or [] if isinstance(row, Mapping)}
    promotion_packets: list[dict[str, Any]] = []
    for shadow in shadow_payload.get("shadow_trials") or []:
        if not isinstance(shadow, Mapping):
            continue
        hypothesis_id = str(shadow.get("hypothesis_id") or "")
        evidence = packet_by_id.get(hypothesis_id, {})
        recommended = shadow.get("shadow_trial_result") == "SHADOW_VALIDATION_PASSED"
        final_state = "PAPER_PROMOTION_RECOMMENDED" if recommended else str(shadow.get("shadow_trial_result") or "NEEDS_DATA")
        transitions = list(shadow.get("state_transition_history") or [])
        if recommended:
            transitions.append({"from_state": "SHADOW_VALIDATION_PASSED", "to_state": "PAPER_PROMOTION_RECOMMENDED", "reason_codes": ["PAPER_READINESS_CHECKLIST_PASSED"]})
        packet = {
            "schema_id": "aegis_hypothesis_promotion_packet_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": hypothesis_id,
            "hypothesis_name": evidence.get("hypothesis_statement") or hypothesis_id,
            "hypothesis_proposal_version": "event_hypothesis_proposal.v1",
            "input_proposal_file": evidence.get("input_proposal_file", ""),
            "input_proposal_hash": evidence.get("input_proposal_hash", ""),
            "evidence_packet_hash": evidence.get("evidence_packet_hash", ""),
            "shadow_trial_hash": shadow.get("shadow_trial_hash", ""),
            "triage_result": shadow.get("triage_decision") or {},
            "shadow_validation_result": shadow.get("shadow_trial_result"),
            "promotion_decision": final_state,
            "paper_readiness_checklist": {
                "data_available": bool((shadow.get("shadow_checks") or {}).get("data_availability")),
                "sample_generation_feasible": bool((shadow.get("shadow_checks") or {}).get("sample_generation")),
                "signal_stability_acceptable": bool((shadow.get("shadow_checks") or {}).get("signal_stability")),
                "duplicate_overlap_clear": bool((shadow.get("shadow_checks") or {}).get("duplicate_overlap")),
                "paper_readiness_feasible": bool((shadow.get("shadow_checks") or {}).get("paper_readiness_feasibility")),
            },
            "risk_policy_status": "RESEARCH_ONLY_PASS",
            "entry_policy_status": "RESEARCH_ONLY_DEFINED",
            "exit_policy_status": "RESEARCH_ONLY_DEFINED",
            "expected_sample_frequency": evidence.get("expected_sample_frequency", "unknown"),
            "expected_validation_timeline": shadow.get("expected_time_to_statistical_sufficiency", "unknown"),
            "why_recommended": "Shadow validation passed deterministic data, sample, stability, duplicate, and paper-readiness checks." if recommended else "",
            "why_not_rejected": "No duplicate, data, testability, sample-rate, or shadow blockers remain." if recommended else "Not recommended because triage or shadow validation did not pass.",
            "risks_caveats": evidence.get("known_risks") or [],
            "state_transition_history": transitions,
            "reason_codes": _reason_codes(transitions),
            "source_artifact_paths": evidence.get("source_artifact_paths") or [],
            "safety_statement": SAFETY_STATEMENT,
            "safety": dict(SAFETY),
        }
        packet["promotion_packet_hash"] = _stable_hash({**packet, "promotion_packet_hash": ""})
        promotion_packets.append(packet)
    payload = {
        "schema_id": "aegis_hypothesis_promotion_packet_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "promotion_packets": promotion_packets,
        "summary": {
            "promotion_packet_count": len(promotion_packets),
            "paper_promotion_recommended_count": sum(1 for row in promotion_packets if row["promotion_decision"] == "PAPER_PROMOTION_RECOMMENDED"),
        },
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def _reason_codes(transitions: list[Any]) -> list[str]:
    out: list[str] = []
    for transition in transitions:
        if isinstance(transition, Mapping):
            out.extend(str(item) for item in transition.get("reason_codes") or [] if str(item))
    return sorted(set(out))


def build_approval_queue_v1(promotion_payload: Mapping[str, Any], *, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    generated_at = f"{day_utc}T00:00:00Z"
    events = _approval_events_by_hypothesis(truth_root=truth_root, day_utc=day_utc)
    items: list[dict[str, Any]] = []
    for packet in promotion_payload.get("promotion_packets") or []:
        if not isinstance(packet, Mapping) or packet.get("promotion_decision") != "PAPER_PROMOTION_RECOMMENDED":
            continue
        hypothesis_id = str(packet.get("hypothesis_id") or "")
        latest_event = events.get(hypothesis_id)
        approval_state = str((latest_event or {}).get("approval_decision") or "PENDING_DAVID_APPROVAL")
        state = {
            "APPROVED": "PAPER_PROMOTION_APPROVED",
            "REJECTED": "PAPER_PROMOTION_REJECTED",
            "DEFERRED": "PAPER_PROMOTION_DEFERRED",
        }.get(approval_state, "PAPER_PROMOTION_RECOMMENDED")
        item = {
            "schema_id": "aegis_paper_promotion_approval_queue_item_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": hypothesis_id,
            "hypothesis_name": packet.get("hypothesis_name") or hypothesis_id,
            "recommendation_reason": packet.get("why_recommended") or "Paper research promotion recommended.",
            "shadow_validation_result": packet.get("shadow_validation_result"),
            "expected_sample_frequency": packet.get("expected_sample_frequency"),
            "expected_validation_timeline": packet.get("expected_validation_timeline"),
            "risks_caveats": packet.get("risks_caveats") or [],
            "promotion_packet_hash": packet.get("promotion_packet_hash"),
            "approval_state": state,
            "one_click_approval_meaning": "Approve this hypothesis for paper research tracking.",
            "forbidden_meanings": ["approve trade", "approve broker execution", "approve real capital", "approve investment recommendation"],
            "available_actions": ["APPROVE_PAPER_TEST", "REJECT", "DEFER"] if state == "PAPER_PROMOTION_RECOMMENDED" else [],
            "latest_approval_event": latest_event or {},
            "safety_statement": SAFETY_STATEMENT,
            "safety": dict(SAFETY),
        }
        item["queue_item_hash"] = _stable_hash({**item, "queue_item_hash": ""})
        items.append(item)
    payload = {
        "schema_id": "aegis_paper_promotion_approval_queue_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "approval_queue": items,
        "recommendations": items,
        "recommendation_count": len([row for row in items if row["approval_state"] == "PAPER_PROMOTION_RECOMMENDED"]),
        "approval_queue_count": len(items),
        "summary": {
            "recommendation_count": len([row for row in items if row["approval_state"] == "PAPER_PROMOTION_RECOMMENDED"]),
            "approval_queue_count": len(items),
            "approved_count": sum(1 for row in items if row["approval_state"] == "PAPER_PROMOTION_APPROVED"),
            "rejected_count": sum(1 for row in items if row["approval_state"] == "PAPER_PROMOTION_REJECTED"),
            "deferred_count": sum(1 for row in items if row["approval_state"] == "PAPER_PROMOTION_DEFERRED"),
        },
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def _approval_events_by_hypothesis(*, truth_root: Path | str, day_utc: str) -> dict[str, dict[str, Any]]:
    rows = _read_jsonl(approval_events_path_v1(truth_root=truth_root, day_utc=day_utc))
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        hypothesis_id = str(row.get("hypothesis_id") or "")
        if hypothesis_id:
            out[hypothesis_id] = row
    return out


def build_all_hypothesis_proposal_promotion_v1(*, truth_root: Path | str, day_utc: str, repo_root: Path | None = None) -> dict[str, Any]:
    evidence = build_evidence_packets_v1(truth_root=truth_root, day_utc=day_utc, repo_root=repo_root)
    shadow = build_shadow_trials_v1(evidence, day_utc=day_utc)
    packets = build_promotion_packets_v1(evidence, shadow, day_utc=day_utc)
    queue = build_approval_queue_v1(packets, truth_root=truth_root, day_utc=day_utc)
    write_json_v1(evidence_packets_path_v1(truth_root=truth_root, day_utc=day_utc), evidence)
    write_json_v1(shadow_trials_path_v1(truth_root=truth_root, day_utc=day_utc), shadow)
    write_json_v1(promotion_packets_path_v1(truth_root=truth_root, day_utc=day_utc), packets)
    write_json_v1(approval_queue_path_v1(truth_root=truth_root, day_utc=day_utc), queue)
    promotion = {
        "schema_id": "aegis_hypothesis_proposal_promotion_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "proposal_states": [
            {
                "hypothesis_id": row.get("hypothesis_id"),
                "hypothesis_name": row.get("hypothesis_name"),
                "state": row.get("promotion_decision"),
                "triage_decision": (row.get("triage_result") or {}).get("triage_state"),
                "shadow_validation_result": row.get("shadow_validation_result"),
                "reason_codes": row.get("reason_codes") or [],
            }
            for row in packets.get("promotion_packets") or []
        ],
        "artifact_hashes": {
            "evidence_packets_hash": evidence.get("content_hash"),
            "shadow_trials_hash": shadow.get("content_hash"),
            "promotion_packets_hash": packets.get("content_hash"),
            "approval_queue_hash": queue.get("content_hash"),
        },
        "artifact_paths": {
            "evidence_packets": str(evidence_packets_path_v1(truth_root=truth_root, day_utc=day_utc)),
            "shadow_trials": str(shadow_trials_path_v1(truth_root=truth_root, day_utc=day_utc)),
            "promotion_packets": str(promotion_packets_path_v1(truth_root=truth_root, day_utc=day_utc)),
            "approval_queue": str(approval_queue_path_v1(truth_root=truth_root, day_utc=day_utc)),
        },
        "summary": {
            "proposal_count": len(packets.get("promotion_packets") or []),
            "promotion_recommendations_count": queue.get("recommendation_count", 0),
            "approval_queue_count": queue.get("approval_queue_count", 0),
            "stale_or_rejected_input_count": len(evidence.get("stale_or_rejected_inputs") or []),
        },
        "safety": dict(SAFETY),
    }
    promotion["content_hash"] = _stable_hash({**promotion, "content_hash": ""})
    write_json_v1(promotion_pipeline_path_v1(truth_root=truth_root, day_utc=day_utc), promotion)
    return {
        "ok": True,
        "day_utc": str(day_utc),
        "summary": promotion["summary"],
        "paths": {
            "promotion": str(promotion_pipeline_path_v1(truth_root=truth_root, day_utc=day_utc)),
            **promotion["artifact_paths"],
        },
        "proposal_states": promotion["proposal_states"],
        "safety": dict(SAFETY),
    }


def record_paper_promotion_approval_event_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    hypothesis_id: str,
    decision: str,
    actor: str = "David",
    reason: str = "",
    generated_at_utc: str | None = None,
    proposal_id: str = "",
    promotion_packet_hash: str = "",
    prior_state: str = "PAPER_PROMOTION_RECOMMENDED",
) -> dict[str, Any]:
    normalized = str(decision or "").strip().upper()
    if normalized not in {"APPROVED", "REJECTED", "DEFERRED"}:
        raise ValueError("decision must be APPROVED, REJECTED, or DEFERRED")
    action = {"APPROVED": "APPROVE_PAPER_TEST", "REJECTED": "REJECT", "DEFERRED": "DEFER"}[normalized]
    new_state = {"APPROVED": "PAPER_PROMOTION_APPROVED", "REJECTED": "PAPER_PROMOTION_REJECTED", "DEFERRED": "PAPER_PROMOTION_DEFERRED"}[normalized]
    generated_at = generated_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    proposal_ref = str(proposal_id or hypothesis_id)
    event = {
        "schema_id": "aegis_paper_promotion_approval_event_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "hypothesis_id": str(hypothesis_id),
        "proposal_id": proposal_ref,
        "hypothesis_proposal_id": proposal_ref,
        "promotion_packet_id": str(promotion_packet_hash or ""),
        "promotion_packet_hash": str(promotion_packet_hash or ""),
        "approval_action": action,
        "approval_decision": normalized,
        "actor": actor,
        "prior_state": str(prior_state or "PAPER_PROMOTION_RECOMMENDED"),
        "new_state": new_state,
        "reason": reason or "Operator paper research promotion action.",
        "approval_meaning": "Approve this hypothesis for paper research tracking." if normalized == "APPROVED" else "Operator declined or deferred paper research tracking.",
        "forbidden_meanings": ["execute a trade", "create broker order", "submit/transmit order", "allocate real capital", "enable live trading", "create investment advice", "approve trade", "approve broker execution", "approve real capital", "approve investment recommendation"],
        "safety_statement": SAFETY_STATEMENT,
        "no_broker_execution": True,
        "no_trade_advice": True,
        "no_live_trading": True,
        "no_real_capital": True,
        "safety": dict(SAFETY),
    }
    event["event_hash"] = _stable_hash({**event, "event_hash": ""})
    _write_jsonl_append(approval_events_path_v1(truth_root=truth_root, day_utc=day_utc), event)
    return event
