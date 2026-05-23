from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "hypothesis_priority_report_v1"

TIERS = [
    "TIER_1_ACTIVE",
    "TIER_2_PROMISING",
    "TIER_3_EXPERIMENTAL",
    "TIER_4_WATCHLIST",
    "TIER_5_ARCHIVE",
]

PRIORITY_COLUMNS = [
    "active",
    "promising",
    "experimental",
    "watchlist",
    "blocked",
    "captured",
    "archived",
]


def build_hypothesis_priority_report_v1(*, pipeline_payload: dict[str, Any], day_utc: str) -> dict[str, Any]:
    items = [row for row in pipeline_payload.get("items") or [] if isinstance(row, dict)]
    scored = [_score_hypothesis_v1(row) for row in items]
    ranked = sorted(scored, key=lambda row: (-float(row["priority_score"]), -float(row["attention_score"]), row["hypothesis_id"]))
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    by_id = {row["hypothesis_id"]: row for row in ranked}
    return {
        "schema_id": "hypothesis_priority_report",
        "schema_version": "v1",
        "artifact_id": "hypothesis_priority_report_v1",
        "generated_at_utc": _now(),
        "day_utc": day_utc,
        "tier_definitions": {
            "TIER_1_ACTIVE": "Highest-quality or operator-critical hypotheses: active research, paper-trial candidates, review-required, or capture-ready.",
            "TIER_2_PROMISING": "Interesting hypotheses with incomplete evidence, active monitoring, or recoverable data blockers.",
            "TIER_3_EXPERIMENTAL": "Speculative, low-confidence, fragile, or high-cost hypotheses that should not dominate attention.",
            "TIER_4_WATCHLIST": "Passive monitoring only; no active operator effort unless new evidence appears.",
            "TIER_5_ARCHIVE": "Closed, invalidated, obsolete, duplicate, rejected, or low-value hypotheses.",
        },
        "scoring_model": {
            "positive_factors": [
                "evidence_strength",
                "confidence",
                "regime_fit",
                "market_data_readiness",
                "validation_progress",
                "paper_trial_progress",
                "novelty_score",
            ],
            "negative_factors": [
                "blocker_severity",
                "stale_evidence_penalty",
                "operational_cost",
                "missing_data",
                "archived_or_rejected_state",
            ],
            "priority_score": "bounded weighted score from 0 to 100",
            "attention_score": "priority plus explicit operator-attention urgency",
            "execution_authority": "research prioritization only; no trade advice, broker submit, autonomous execution, or sleeve mutation",
        },
        "ranked_hypotheses": ranked,
        "priority_by_hypothesis_id": by_id,
        "recommended_focus_today": _recommended_focus_v1(ranked),
        "priority_pipeline": _priority_pipeline_v1(ranked),
        "priority_counts": _priority_counts_v1(ranked),
        "human_approval_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }


def enrich_pipeline_items_with_priority_v1(items: list[dict[str, Any]], priority_report: dict[str, Any]) -> list[dict[str, Any]]:
    by_id = priority_report.get("priority_by_hypothesis_id") if isinstance(priority_report.get("priority_by_hypothesis_id"), dict) else {}
    out: list[dict[str, Any]] = []
    for item in items:
        hypothesis_id = str(item.get("hypothesis_id") or "")
        priority = by_id.get(hypothesis_id) if isinstance(by_id.get(hypothesis_id), dict) else {}
        out.append({**item, **_priority_projection_fields(priority)})
    return out


def write_hypothesis_priority_report_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "hypothesis_priority_report.v1.json", payload)
    txt_path = out_dir / "hypothesis_priority_report.v1.txt"
    txt_path.write_text(render_hypothesis_priority_report_txt_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path)}


def render_hypothesis_priority_report_txt_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS HYPOTHESIS PRIORITY REPORT v1",
        f"day_utc: {payload.get('day_utc')}",
        f"hypothesis_count: {len(payload.get('ranked_hypotheses') or [])}",
        "execution_authority: research prioritization only; no broker submit, autonomous execution, or trade advice",
        "",
        "recommended_focus_today:",
    ]
    for row in payload.get("recommended_focus_today") or []:
        lines.append(f"{row.get('rank')}. {row.get('title')} | {row.get('tier')} | score={row.get('priority_score')} | {row.get('recommended_action')}")
    lines.extend(["", "ranked_hypotheses:"])
    for row in payload.get("ranked_hypotheses") or []:
        lines.append(
            f"{row.get('rank')}. {row.get('hypothesis_id')} | {row.get('tier')} | "
            f"priority={row.get('priority_score')} attention={row.get('attention_score')} | "
            f"blocker={row.get('blocker_summary') or 'none'} | {row.get('recommended_action')}"
        )
    return "\n".join(lines) + "\n"


def _score_hypothesis_v1(item: dict[str, Any]) -> dict[str, Any]:
    gate = str(item.get("current_gate") or "").upper()
    status = str(item.get("gate_status") or item.get("status") or "").upper()
    blocker = str(item.get("blocker") or "")
    lifecycle = str(item.get("operator_lifecycle_state") or "").upper()
    result_status = str(item.get("latest_result_status") or item.get("latest_result", {}).get("test_status") or "").upper()
    operator_blocker = item.get("operator_blocker") if isinstance(item.get("operator_blocker"), dict) else {}
    data_plan = operator_blocker.get("data_acquisition_plan") if isinstance(operator_blocker.get("data_acquisition_plan"), dict) else item.get("data_acquisition_plan") if isinstance(item.get("data_acquisition_plan"), dict) else {}
    data_needed_result = result_status in {"DATA_NEEDED", "MISSING_INPUT", "INSUFFICIENT_DATA"}
    blocked = status == "BLOCKED" or bool(blocker) or data_needed_result
    archived = gate == "REJECTED_ARCHIVED" or lifecycle == "ARCHIVED"
    operator_attention_required = bool(item.get("operator_action_required")) or status == "NEEDS_OPERATOR" or gate in {"RESULT_REVIEW", "SLEEVE_REVIEW"}

    evidence_strength = _evidence_strength(gate, result_status, item)
    confidence = _confidence_score(item, gate, result_status)
    regime_fit = _regime_fit(item, gate)
    market_data_readiness = _market_data_readiness(blocked, operator_blocker, data_plan)
    validation_progress = _validation_progress(gate)
    paper_trial_progress = 90 if gate in {"PAPER_TRIAL", "SLEEVE_REVIEW", "ACTIVE_OR_ADOPTED"} else 35 if gate == "RESULT_REVIEW" else 10
    blocker_severity = _blocker_severity(blocked, operator_blocker, data_plan)
    stale_evidence_penalty = 22 if "STALE" in json.dumps(operator_blocker, sort_keys=True).upper() or "STALE" in blocker.upper() else 0
    operational_cost = _operational_cost(item, blocked, data_plan)
    novelty_score = _novelty_score(item)

    positive = (
        evidence_strength * 0.24
        + confidence * 0.16
        + regime_fit * 0.10
        + market_data_readiness * 0.12
        + validation_progress * 0.20
        + paper_trial_progress * 0.12
        + novelty_score * 0.06
    )
    negative = blocker_severity * 0.20 + stale_evidence_penalty + operational_cost * 0.10
    priority_score = _clamp(round(positive - negative, 2), 0, 100)
    if archived:
        priority_score = min(priority_score, 8)
    attention_score = _clamp(
        round(
            priority_score
            + (22 if operator_attention_required else 0)
            + (12 if gate in {"PAPER_TRIAL", "SLEEVE_REVIEW"} else 0)
            + (8 if blocked and priority_score >= 45 else 0)
            - (28 if archived else 0),
            2,
        ),
        0,
        100,
    )
    tier = _tier_for_item(
        gate=gate,
        archived=archived,
        blocked=blocked,
        operator_attention_required=operator_attention_required,
        priority_score=priority_score,
        attention_score=attention_score,
        stale_evidence_penalty=stale_evidence_penalty,
        result_status=result_status,
    )
    return {
        "hypothesis_id": str(item.get("hypothesis_id") or ""),
        "title": str(item.get("readable_title") or item.get("title") or item.get("hypothesis_id") or "Research idea"),
        "tier": tier,
        "priority_score": priority_score,
        "attention_score": attention_score,
        "evidence_strength": round(evidence_strength, 2),
        "operational_cost": round(operational_cost, 2),
        "confidence": round(confidence, 2),
        "regime_fit": round(regime_fit, 2),
        "market_data_readiness": round(market_data_readiness, 2),
        "validation_progress": round(validation_progress, 2),
        "paper_trial_progress": round(paper_trial_progress, 2),
        "blocker_severity": round(blocker_severity, 2),
        "stale_evidence_penalty": round(stale_evidence_penalty, 2),
        "novelty_score": round(novelty_score, 2),
        "operator_attention_required": operator_attention_required,
        "blocker_summary": _blocker_summary(item, operator_blocker) if data_needed_result and gate not in {"PAPER_TRIAL", "SLEEVE_REVIEW", "ACTIVE_OR_ADOPTED"} else _blocker_summary(item, operator_blocker),
        "recommended_action": _recommended_action(item, tier, blocked, operator_attention_required),
        "expected_next_milestone": _expected_next_milestone(gate, tier, blocked),
        "evidence_freshness": "STALE" if stale_evidence_penalty else ("BLOCKED" if blocked else "CURRENT_OR_NOT_REQUIRED"),
        "current_gate": gate,
        "operator_lifecycle_state": item.get("operator_lifecycle_state"),
        "gate_status": status,
        "source_blocker": blocker,
        "blocker": item.get("blocker"),
        "next_action": item.get("next_action"),
        "operator_action_required": item.get("operator_action_required"),
        "operator_blocker": item.get("operator_blocker") if isinstance(item.get("operator_blocker"), dict) else {},
        "data_acquisition_plan": item.get("data_acquisition_plan") if isinstance(item.get("data_acquisition_plan"), dict) else data_plan,
        "symbols": item.get("symbols") or item.get("related_symbols") or item.get("universe") or [],
        "required_datasets": item.get("required_datasets") or [],
        "expected_output": item.get("expected_output") or [],
        "latest_result_status": item.get("latest_result_status"),
        "latest_result_summary": item.get("latest_result_summary"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def _priority_projection_fields(priority: dict[str, Any]) -> dict[str, Any]:
    if not priority:
        return {}
    keys = [
        "rank",
        "tier",
        "priority_score",
        "attention_score",
        "evidence_strength",
        "operational_cost",
        "confidence",
        "regime_fit",
        "market_data_readiness",
        "validation_progress",
        "paper_trial_progress",
        "blocker_severity",
        "stale_evidence_penalty",
        "operator_attention_required",
        "blocker_summary",
        "recommended_action",
        "expected_next_milestone",
        "evidence_freshness",
    ]
    return {key: priority.get(key) for key in keys if key in priority}


def _priority_pipeline_v1(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    pipeline = {key: [] for key in PRIORITY_COLUMNS}
    for row in rows:
        key = _priority_column(row)
        pipeline[key].append(row)
    return pipeline


def _priority_column(row: dict[str, Any]) -> str:
    tier = str(row.get("tier") or "")
    if str(row.get("current_gate") or "") == "REJECTED_ARCHIVED" or tier == "TIER_5_ARCHIVE":
        return "archived"
    if str(row.get("current_gate") or "") == "CAPTURED":
        return "captured"
    if row.get("blocker_summary") and row.get("gate_status") == "BLOCKED":
        return "blocked"
    if tier == "TIER_1_ACTIVE":
        return "active"
    if tier == "TIER_2_PROMISING":
        return "promising"
    if tier == "TIER_3_EXPERIMENTAL":
        return "experimental"
    if tier == "TIER_4_WATCHLIST":
        return "watchlist"
    return "watchlist"


def _priority_counts_v1(rows: list[dict[str, Any]]) -> dict[str, int]:
    pipeline = _priority_pipeline_v1(rows)
    counts = {key.upper(): len(value) for key, value in pipeline.items()}
    for tier in TIERS:
        counts[tier] = sum(1 for row in rows if row.get("tier") == tier)
    return counts


def _recommended_focus_v1(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("tier") == "TIER_1_ACTIVE"
        or row.get("operator_attention_required") is True
        or str(row.get("current_gate") or "") in {"PAPER_TRIAL", "SLEEVE_REVIEW", "RESULT_REVIEW"}
    ]
    ranked = sorted(eligible, key=lambda row: (-float(row.get("attention_score") or 0), -float(row.get("priority_score") or 0), row.get("hypothesis_id") or ""))
    return ranked[:5]


def _tier_for_item(*, gate: str, archived: bool, blocked: bool, operator_attention_required: bool, priority_score: float, attention_score: float, stale_evidence_penalty: float, result_status: str) -> str:
    if archived:
        return "TIER_5_ARCHIVE"
    if gate in {"PAPER_TRIAL", "SLEEVE_REVIEW", "ACTIVE_OR_ADOPTED"}:
        return "TIER_1_ACTIVE"
    if result_status in {"DATA_NEEDED", "MISSING_INPUT", "INSUFFICIENT_DATA"}:
        return "TIER_2_PROMISING" if priority_score >= 25 else "TIER_4_WATCHLIST"
    if operator_attention_required and attention_score >= 55:
        return "TIER_1_ACTIVE"
    if blocked and stale_evidence_penalty >= 20 and priority_score < 35:
        return "TIER_4_WATCHLIST"
    if blocked and priority_score >= 25:
        return "TIER_2_PROMISING"
    if gate in {"TEST_PLAN", "TESTING", "RESULT_REVIEW"} and priority_score >= 35:
        return "TIER_2_PROMISING"
    if gate == "INBOX":
        return "TIER_4_WATCHLIST"
    if result_status in {"FAIL", "REJECTED"} or priority_score < 25:
        return "TIER_3_EXPERIMENTAL"
    return "TIER_3_EXPERIMENTAL"


def _evidence_strength(gate: str, result_status: str, item: dict[str, Any]) -> float:
    evidence_count = len(item.get("evidence_available") or []) + len(item.get("source_artifacts") or [])
    base = {
        "INBOX": 15,
        "TRIAGE": 22,
        "TEST_PLAN": 35,
        "TESTING": 42,
        "RESULT_REVIEW": 58,
        "PAPER_TRIAL": 74,
        "SLEEVE_REVIEW": 80,
        "ACTIVE_OR_ADOPTED": 86,
        "REJECTED_ARCHIVED": 8,
    }.get(gate, 20)
    if result_status in {"PASS", "VALID", "READY"}:
        base += 12
    if result_status in {"FAIL", "DATA_NEEDED", "MISSING_INPUT"}:
        base -= 8
    return _clamp(base + min(evidence_count, 5) * 3, 0, 100)


def _confidence_score(item: dict[str, Any], gate: str, result_status: str) -> float:
    raw = str(item.get("confidence") or item.get("confidence_label") or item.get("evidence_quality") or "").upper()
    if "HIGH" in raw or "STRONG" in raw:
        return 82
    if "MEDIUM" in raw:
        return 58
    if "LOW" in raw or "WEAK" in raw:
        return 32
    if result_status in {"PASS", "VALID"}:
        return 72
    return {"PAPER_TRIAL": 64, "SLEEVE_REVIEW": 68, "RESULT_REVIEW": 55, "TESTING": 44, "TEST_PLAN": 38, "INBOX": 25}.get(gate, 35)


def _regime_fit(item: dict[str, Any], gate: str) -> float:
    raw = str(item.get("regime_fit") or item.get("edge_family") or item.get("classification") or "").upper()
    if "ACTIVE" in raw or "FIT" in raw:
        return 72
    if "UNKNOWN" in raw or not raw:
        return {"PAPER_TRIAL": 60, "SLEEVE_REVIEW": 62, "RESULT_REVIEW": 55}.get(gate, 48)
    return 54


def _market_data_readiness(blocked: bool, operator_blocker: dict[str, Any], data_plan: dict[str, Any]) -> float:
    source_type = str(data_plan.get("source_type") or "").upper()
    if not blocked:
        return 82
    if source_type == "AEGIS_CAN_FETCH_AUTOMATICALLY":
        return 46
    if source_type == "USER_MUST_UPLOAD":
        return 30
    if source_type == "USER_MUST_CONFIGURE_PROVIDER":
        return 18
    if source_type == "NOT_CURRENTLY_SUPPORTED":
        return 5
    state = str(operator_blocker.get("state") or "").upper()
    if "MARKET_DATA" in state:
        return 38
    return 42


def _validation_progress(gate: str) -> float:
    return {
        "INBOX": 10,
        "TRIAGE": 18,
        "TEST_PLAN": 28,
        "TESTING": 40,
        "RESULT_REVIEW": 62,
        "PAPER_TRIAL": 76,
        "SLEEVE_REVIEW": 86,
        "ACTIVE_OR_ADOPTED": 94,
        "REJECTED_ARCHIVED": 0,
    }.get(gate, 15)


def _blocker_severity(blocked: bool, operator_blocker: dict[str, Any], data_plan: dict[str, Any]) -> float:
    if not blocked:
        return 0
    source_type = str(data_plan.get("source_type") or "").upper()
    if source_type == "NOT_CURRENTLY_SUPPORTED":
        return 92
    if source_type == "USER_MUST_CONFIGURE_PROVIDER":
        return 76
    if source_type == "USER_MUST_UPLOAD":
        return 58
    if source_type == "AEGIS_CAN_FETCH_AUTOMATICALLY":
        return 38
    if str(operator_blocker.get("operator_action_required")).lower() == "true":
        return 48
    return 44


def _operational_cost(item: dict[str, Any], blocked: bool, data_plan: dict[str, Any]) -> float:
    cost = 25 + len(item.get("evidence_required") or []) * 4 + len(item.get("missing_evidence") or []) * 6
    if blocked:
        cost += 20
    if str(data_plan.get("source_type") or "").upper() in {"USER_MUST_UPLOAD", "USER_MUST_CONFIGURE_PROVIDER", "NOT_CURRENTLY_SUPPORTED"}:
        cost += 18
    return _clamp(cost, 0, 100)


def _novelty_score(item: dict[str, Any]) -> float:
    raw = str(item.get("novelty") or item.get("source") or item.get("classification") or "").upper()
    if "DUPLICATE" in raw:
        return 0
    if "CHATGPT" in raw or "SEED" in raw:
        return 48
    return 42


def _blocker_summary(item: dict[str, Any], operator_blocker: dict[str, Any]) -> str:
    if operator_blocker and operator_blocker.get("state") != "NO_BLOCKER":
        return str(operator_blocker.get("title") or operator_blocker.get("summary") or "")
    return str(item.get("blocker") or "")


def _recommended_action(item: dict[str, Any], tier: str, blocked: bool, operator_attention_required: bool) -> str:
    result_status = str(item.get("latest_result_status") or item.get("latest_result", {}).get("test_status") or "").upper()
    if blocked and result_status in {"DATA_NEEDED", "MISSING_INPUT", "INSUFFICIENT_DATA"}:
        return str((item.get("operator_blocker") or {}).get("next_action") or "Resolve the precise data requirement, then rerun the research test.")
    if blocked:
        operator_blocker = item.get("operator_blocker") if isinstance(item.get("operator_blocker"), dict) else {}
        return str(operator_blocker.get("next_action") or item.get("next_action") or "Resolve blocker.")
    if operator_attention_required:
        return str(item.get("next_action") or "Review hypothesis.")
    if tier == "TIER_1_ACTIVE":
        return "Review today and decide the next paper-trial or promotion step."
    if tier == "TIER_2_PROMISING":
        return str(item.get("next_action") or "Continue research when evidence is ready.")
    if tier == "TIER_3_EXPERIMENTAL":
        return "Keep low effort; require stronger evidence before promotion."
    if tier == "TIER_4_WATCHLIST":
        return "Monitor passively; no operator action now."
    return "Keep archived unless new evidence justifies reopening."


def _expected_next_milestone(gate: str, tier: str, blocked: bool) -> str:
    if blocked:
        return "Blocker cleared"
    return {
        "INBOX": "Triage decision",
        "TRIAGE": "Clarification or test plan",
        "TEST_PLAN": "Research plan built",
        "TESTING": "Evidence/result generated",
        "RESULT_REVIEW": "Operator review decision",
        "PAPER_TRIAL": "Paper-trial observation",
        "SLEEVE_REVIEW": "Sleeve review decision",
        "ACTIVE_OR_ADOPTED": "Ongoing monitoring",
        "REJECTED_ARCHIVED": "No active milestone",
    }.get(gate, "Next governed research milestone" if tier != "TIER_5_ARCHIVE" else "No active milestone")


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
