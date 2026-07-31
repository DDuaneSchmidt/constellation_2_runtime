from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from dataclasses import asdict, dataclass, field
from typing import Any

GENERATED_ONLY_STATUS = "GENERATED_ONLY"

RESEARCH_ADVERSARY_STATUSES = {
    GENERATED_ONLY_STATUS,
    "HUMAN_REVIEW_REQUIRED",
    "APPROVED_FOR_EXPERIMENT_DESIGN",
    "REJECTED",
    "RETIRED",
}

FORBIDDEN_AUTHORITY_PATTERNS = {
    "candidate promotion": re.compile(r"\b(candidate\s+promotion|promote\s+(the\s+)?candidate|promoted\s+candidate)\b", re.IGNORECASE),
    "trade recommendation": re.compile(r"\b(trade\s+recommendation|recommend\s+(a\s+)?trade|trading\s+recommendation|buy|sell)\b", re.IGNORECASE),
    "capital recommendation": re.compile(r"\b(capital\s+recommendation|recommend\s+capital|capital\s+allocation|allocate\s+capital)\b", re.IGNORECASE),
    "position sizing": re.compile(r"\b(position\s+sizing|position\s+size|size\s+(the\s+)?position|sizing\s+recommendation)\b", re.IGNORECASE),
    "replay override": re.compile(r"\b(replay\s+override|override\s+(the\s+)?replay)\b", re.IGNORECASE),
    "qualification override": re.compile(r"\b(qualification\s+override|override\s+(edge\s+)?qualification)\b", re.IGNORECASE),
    "governance override": re.compile(r"\b(governance\s+override|override\s+governance)\b", re.IGNORECASE),
}

RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY = {
    "artifact_contract_only": True,
    "research_adversary_review_allowed": True,
    "experiment_design_allowed_after_human_review": True,
    "candidate_promotion_authorized": False,
    "trade_recommendation_authorized": False,
    "capital_recommendation_authorized": False,
    "position_sizing_authorized": False,
    "replay_override_authorized": False,
    "qualification_override_authorized": False,
    "governance_override_authorized": False,
}

DEFAULT_FORBIDDEN_ACTIONS_ACKNOWLEDGED = {
    "candidate_promotion": True,
    "trade_recommendation": True,
    "capital_recommendation": True,
    "position_sizing": True,
    "replay_override": True,
    "qualification_override": True,
    "governance_override": True,
}


class ResearchAdversaryValidationError(ValueError):
    pass


class ResearchAdversaryGovernanceError(ResearchAdversaryValidationError):
    pass


@dataclass(frozen=True)
class AssumptionExtraction:
    assumption_id: str
    statement: str
    source_field: str
    confidence: float
    dependency: str | None = None
    testable: bool = True
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_no_forbidden_authority_terms(row)
        _validate_confidence(row["confidence"], "assumption confidence")
        _require_text(row, ["assumption_id", "statement", "source_field"])
        return row


@dataclass(frozen=True)
class ConstraintAnalysis:
    constraint_id: str
    constraint_type: str
    description: str
    impact: str
    severity: str
    mitigation: str = ""
    evidence_refs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_no_forbidden_authority_terms(row)
        _require_text(row, ["constraint_id", "constraint_type", "description", "impact", "severity"])
        return row


@dataclass(frozen=True)
class CompetingExplanationSet:
    explanation_set_id: str
    primary_mechanism: str
    alternative_explanations: list[str]
    null_explanation: str
    discriminating_evidence_needed: list[str] = field(default_factory=list)
    weakest_link: str = ""

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_no_forbidden_authority_terms(row)
        _require_text(row, ["explanation_set_id", "primary_mechanism", "null_explanation"])
        if not row["alternative_explanations"]:
            raise ResearchAdversaryValidationError("competing explanation set requires alternative_explanations")
        return row


@dataclass(frozen=True)
class FalsificationProposal:
    falsification_id: str
    target_assumption_id: str
    test_description: str
    expected_disconfirming_observation: str
    minimum_evidence_required: str
    priority: str
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_no_forbidden_authority_terms(row)
        _require_text(
            row,
            [
                "falsification_id",
                "target_assumption_id",
                "test_description",
                "expected_disconfirming_observation",
                "minimum_evidence_required",
                "priority",
            ],
        )
        return row


@dataclass(frozen=True)
class ExperimentProposal:
    experiment_id: str
    question: str
    design_summary: str
    required_inputs: list[str]
    success_criteria: list[str]
    failure_criteria: list[str]
    expected_artifacts: list[str]
    human_review_required: bool = True
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_no_forbidden_authority_terms(row)
        _require_text(row, ["experiment_id", "question", "design_summary"])
        for field_name in ["required_inputs", "success_criteria", "failure_criteria", "expected_artifacts"]:
            if not row[field_name]:
                raise ResearchAdversaryValidationError(f"experiment proposal requires {field_name}")
        return row


@dataclass(frozen=True)
class ResearchAdversaryReview:
    review_id: str
    source_observation_id: str
    anomaly_narrative: str
    mechanism_proposals: list[str]
    competing_explanations: list[dict[str, Any]]
    null_explanation: str
    assumptions: list[dict[str, Any]]
    constraints: list[dict[str, Any]]
    falsification_tests: list[dict[str, Any]]
    experiment_proposals: list[dict[str, Any]]
    evidence_supporting: list[str]
    evidence_weakening: list[str]
    authority_boundary: dict[str, Any]
    forbidden_actions_acknowledged: dict[str, bool]
    recommendation: str
    status: str
    source_claim_id: str | None = None
    source_hypothesis_id: str | None = None
    suspected_taxonomy_categories: list[str] = field(default_factory=list)
    taxonomy_confidence: float | None = None
    related_failure_patterns: list[dict[str, Any]] = field(default_factory=list)
    taxonomy_reasoning: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_research_adversary_review(row)
        return row


def validate_research_adversary_review(review: ResearchAdversaryReview | dict[str, Any]) -> bool:
    row = review.to_dict() if isinstance(review, ResearchAdversaryReview) else dict(review)
    if _is_legacy_research_adversary_review(row):
        return _validate_legacy_research_adversary_review(row)
    required_fields = [
        "review_id",
        "source_observation_id",
        "anomaly_narrative",
        "mechanism_proposals",
        "competing_explanations",
        "null_explanation",
        "assumptions",
        "constraints",
        "falsification_tests",
        "experiment_proposals",
        "evidence_supporting",
        "evidence_weakening",
        "authority_boundary",
        "forbidden_actions_acknowledged",
        "recommendation",
        "status",
    ]
    missing = [field_name for field_name in required_fields if field_name not in row]
    if missing:
        raise ResearchAdversaryValidationError(f"missing research adversary review fields: {missing}")
    _require_text(row, ["review_id", "source_observation_id", "anomaly_narrative", "null_explanation", "recommendation", "status"])
    if row["status"] not in RESEARCH_ADVERSARY_STATUSES:
        raise ResearchAdversaryValidationError(f"invalid research adversary status: {row['status']}")
    for list_field in [
        "mechanism_proposals",
        "competing_explanations",
        "assumptions",
        "constraints",
        "falsification_tests",
        "experiment_proposals",
    ]:
        if not row[list_field]:
            raise ResearchAdversaryValidationError(f"research adversary review requires {list_field}")
    validate_research_adversary_authority_boundary(row["authority_boundary"])
    validate_forbidden_actions_acknowledged(row["forbidden_actions_acknowledged"])
    _validate_optional_taxonomy_fields(row)
    validate_no_forbidden_authority_terms(_text_scan_payload(row))
    return True


def validate_no_forbidden_authority_terms(payload: Any) -> bool:
    violations: list[str] = []
    for path, value in _iter_string_values(payload):
        for label, pattern in FORBIDDEN_AUTHORITY_PATTERNS.items():
            if pattern.search(value):
                violations.append(f"{path}: {label}")
    if violations:
        raise ResearchAdversaryValidationError(f"forbidden authority language detected: {violations}")
    return True


def validate_research_adversary_authority_boundary(authority_boundary: dict[str, Any]) -> bool:
    if not isinstance(authority_boundary, dict):
        raise ResearchAdversaryValidationError("authority_boundary must be a dict")
    for key, expected in RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY.items():
        if authority_boundary.get(key) is not expected:
            raise ResearchAdversaryValidationError(f"authority_boundary[{key}] must be {expected}")
    return True


def validate_forbidden_actions_acknowledged(acknowledged: dict[str, bool]) -> bool:
    if not isinstance(acknowledged, dict):
        raise ResearchAdversaryValidationError("forbidden_actions_acknowledged must be a dict")
    missing = [key for key in DEFAULT_FORBIDDEN_ACTIONS_ACKNOWLEDGED if acknowledged.get(key) is not True]
    if missing:
        raise ResearchAdversaryValidationError(f"forbidden actions not acknowledged: {missing}")
    return True


def _text_scan_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in {"authority_boundary", "forbidden_actions_acknowledged"}}


def _iter_string_values(payload: Any, path: str = "$"):
    if isinstance(payload, str):
        yield path, payload
    elif isinstance(payload, dict):
        for key, value in payload.items():
            yield from _iter_string_values(value, f"{path}.{key}")
    elif isinstance(payload, (list, tuple, set)):
        for index, value in enumerate(payload):
            yield from _iter_string_values(value, f"{path}[{index}]")


def _require_text(row: dict[str, Any], fields: list[str]) -> None:
    missing = [field_name for field_name in fields if not str(row.get(field_name) or "").strip()]
    if missing:
        raise ResearchAdversaryValidationError(f"missing required text fields: {missing}")


def _validate_confidence(value: Any, label: str) -> None:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ResearchAdversaryValidationError(f"{label} must be numeric") from exc
    if not 0.0 <= number <= 1.0:
        raise ResearchAdversaryValidationError(f"{label} must be between 0 and 1")


LEGACY_RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY = {
    "research_only": True,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "candidate_promotion_authorized": False,
    "trade_recommendation_authorized": False,
    "replay_override_authorized": False,
    "qualification_override_authorized": False,
}


def create_research_adversary_review(
    *,
    review_id: str,
    created_at: str | None = None,
    mechanism_proposal: dict[str, Any],
    competing_explanations: list[dict[str, Any]],
    null_explanation: dict[str, Any],
    assumptions: list[dict[str, Any]],
    falsification_tests: list[dict[str, Any]],
    supporting_evidence: list[str],
    weakening_evidence: list[str],
    suspected_taxonomy_categories: list[str] | None = None,
    taxonomy_confidence: float | None = None,
    related_failure_patterns: list[dict[str, Any]] | None = None,
    taxonomy_reasoning: list[str] | None = None,
) -> dict[str, Any]:
    review = {
        "review_id": review_id,
        "created_at": created_at or _now(),
        "mechanism_proposal": dict(mechanism_proposal),
        "competing_explanations": list(competing_explanations),
        "null_explanation": dict(null_explanation),
        "assumptions": list(assumptions),
        "falsification_tests": list(falsification_tests),
        "supporting_evidence": list(supporting_evidence),
        "weakening_evidence": list(weakening_evidence),
        "suspected_taxonomy_categories": list(suspected_taxonomy_categories or []),
        "taxonomy_confidence": taxonomy_confidence,
        "related_failure_patterns": list(related_failure_patterns or []),
        "taxonomy_reasoning": list(taxonomy_reasoning or []),
        "evidence_status": GENERATED_ONLY_STATUS,
        "authority_boundary": dict(LEGACY_RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY),
        "human_review_required": True,
        "recommendation": "Collect adversarial evidence before any experiment design decision.",
    }
    validate_research_adversary_review(review)
    return review


def build_demo_research_adversary_review(*, created_at: str | None = None) -> dict[str, Any]:
    return create_research_adversary_review(
        review_id="research-adversary-demo",
        created_at=created_at,
        mechanism_proposal={
            "mechanism_id": "mech-demo-breakout-volume",
            "mechanism": "BREAKOUT",
            "proposal": "Breakout continuation may be driven by volume expansion after compression.",
        },
        competing_explanations=[
            {"explanation_id": "competing-regime-beta", "summary": "The effect may be broad market beta during trending regimes."}
        ],
        null_explanation={"explanation_id": "null-data-mining", "summary": "The apparent effect may be data-mined noise from generated-only trials."},
        assumptions=[{"assumption_id": "assumption-volume", "statement": "Volume expansion is measured consistently."}],
        falsification_tests=[
            {"test_id": "falsify-with-regime-control", "description": "Replay against a regime-matched null and reject if the volume term adds no lift."}
        ],
        supporting_evidence=["Generated observation clusters show continuation after compression."],
        weakening_evidence=["The same pattern may disappear after regime controls."],
        suspected_taxonomy_categories=["REGIME_DEPENDENCY", "PROXY_DEPENDENCY", "WARNING_RECURRENCE"],
        taxonomy_confidence=0.72,
        related_failure_patterns=[
            {
                "pattern_id": "atlas-failure-regime-dependency",
                "category": "REGIME_DEPENDENCY",
                "summary": "Apparent mechanism strength weakens when regime controls are added.",
            },
            {
                "pattern_id": "atlas-failure-proxy-dependency",
                "category": "PROXY_DEPENDENCY",
                "summary": "Observed lift depends on a proxy that may not isolate the proposed mechanism.",
            },
        ],
        taxonomy_reasoning=[
            "The mechanism depends on volume expansion and compression context.",
            "Weakening evidence already names regime control as the discriminating check.",
        ],
    )


def build_research_adversary_report(review: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    validate_research_adversary_review(review)
    report = {
        "schema_id": "atlas_v2_research_adversary_report_v1",
        "schema_version": "1.0",
        "report_type": "RESEARCH_ADVERSARY_REPORT",
        "created_at": created_at or review.get("created_at") or _now(),
        "review_id": review["review_id"],
        "mechanism_proposal": review["mechanism_proposal"],
        "competing_explanations": review["competing_explanations"],
        "null_explanation": review["null_explanation"],
        "assumptions": review["assumptions"],
        "falsification": review["falsification_tests"],
        "supporting_evidence": review["supporting_evidence"],
        "weakening_evidence": review["weakening_evidence"],
        "taxonomy_integration": {
            "suspected_taxonomy_categories": list(review.get("suspected_taxonomy_categories") or []),
            "taxonomy_confidence": review.get("taxonomy_confidence"),
            "related_failure_patterns": list(review.get("related_failure_patterns") or []),
            "taxonomy_reasoning": list(review.get("taxonomy_reasoning") or []),
            "evidence_status": GENERATED_ONLY_STATUS,
        },
        "evidence_status": review.get("evidence_status", GENERATED_ONLY_STATUS),
        "authority_boundary": dict(review["authority_boundary"]),
        "recommendation": review.get("recommendation"),
        "limitations": [
            "Research adversary output is generated-only until human-reviewed.",
            "No live trading, broker execution, capital allocation, position sizing, candidate promotion, replay override, or qualification override authority is granted.",
        ],
    }
    return report


def write_research_adversary_report(review: dict[str, Any], *, root: str | Path, day: str | None = None, created_at: str | None = None) -> dict[str, Path]:
    report = build_research_adversary_report(review, created_at=created_at)
    root_path = Path(root)
    day_value = day or str(report.get("created_at") or _today())[:10]
    out_dir = root_path / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "research_adversary_report.json"
    summary_path = out_dir / "research_adversary_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_research_adversary_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_research_adversary_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Research Adversary Report",
        "",
        f"Evidence Status: {report.get('evidence_status')}",
        "",
        "## Supporting Evidence",
    ]
    lines.extend(f"- {row}" for row in report.get("supporting_evidence", []))
    lines.extend(["", "## Weakening Evidence"])
    lines.extend(f"- {row}" for row in report.get("weakening_evidence", []))
    lines.extend(["", "## Falsification"])
    for row in report.get("falsification", []):
        lines.append(f"- {row.get('test_id')}: {row.get('description')}")
    taxonomy = report.get("taxonomy_integration") or {}
    lines.extend(["", "## Taxonomy Integration"])
    lines.append(f"- evidence_status: {taxonomy.get('evidence_status', GENERATED_ONLY_STATUS)}")
    lines.append(f"- taxonomy_confidence: {taxonomy.get('taxonomy_confidence')}")
    categories = taxonomy.get("suspected_taxonomy_categories") or []
    lines.append(f"- suspected_taxonomy_categories: {', '.join(categories) if categories else 'None'}")
    for row in taxonomy.get("related_failure_patterns") or []:
        lines.append(f"- related_failure_pattern: {row.get('pattern_id')} ({row.get('category')})")
    for row in taxonomy.get("taxonomy_reasoning") or []:
        lines.append(f"- taxonomy_reasoning: {row}")
    lines.extend(["", "## Authority Boundaries"])
    for key, value in sorted((report.get("authority_boundary") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    return "\n".join(lines)


def _is_legacy_research_adversary_review(row: dict[str, Any]) -> bool:
    return "mechanism_proposal" in row or "evidence_status" in row


def _validate_legacy_research_adversary_review(row: dict[str, Any]) -> bool:
    for field_name in ["review_id", "mechanism_proposal", "competing_explanations", "null_explanation", "assumptions", "falsification_tests", "supporting_evidence", "weakening_evidence", "authority_boundary"]:
        if field_name not in row:
            raise ResearchAdversaryGovernanceError(f"missing required field: {field_name}")
    if not row.get("null_explanation"):
        raise ResearchAdversaryGovernanceError("null explanation is required")
    if not row.get("competing_explanations"):
        raise ResearchAdversaryGovernanceError("at least one competing explanation is required")
    if not row.get("falsification_tests"):
        raise ResearchAdversaryGovernanceError("at least one falsification test is required")
    if row.get("evidence_status", GENERATED_ONLY_STATUS) != GENERATED_ONLY_STATUS:
        raise ResearchAdversaryGovernanceError("legacy research adversary evidence_status must be GENERATED_ONLY")
    try:
        _validate_optional_taxonomy_fields(row)
    except ResearchAdversaryValidationError as exc:
        raise ResearchAdversaryGovernanceError(str(exc)) from exc
    _validate_legacy_authority_boundary(row.get("authority_boundary") or {})
    _validate_legacy_no_forbidden_authority_terms(row)
    return True


def _validate_optional_taxonomy_fields(row: dict[str, Any]) -> None:
    if row.get("taxonomy_confidence") is not None:
        _validate_confidence(row["taxonomy_confidence"], "taxonomy confidence")
    for field_name in ["suspected_taxonomy_categories", "related_failure_patterns", "taxonomy_reasoning"]:
        if field_name in row and row[field_name] is not None and not isinstance(row[field_name], list):
            raise ResearchAdversaryValidationError(f"{field_name} must be a list")


def _validate_legacy_authority_boundary(boundary: dict[str, Any]) -> None:
    for key, expected in LEGACY_RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY.items():
        if boundary.get(key) is not expected:
            raise ResearchAdversaryGovernanceError(f"authority boundary mismatch: {key} must be {expected}")


def _validate_legacy_no_forbidden_authority_terms(row: dict[str, Any]) -> None:
    payload = {key: value for key, value in row.items() if key != "authority_boundary"}
    for path, value in _iter_string_values(payload):
        for label, pattern in FORBIDDEN_AUTHORITY_PATTERNS.items():
            if pattern.search(value):
                raise ResearchAdversaryGovernanceError(f"forbidden authority phrase detected at {path}: {label}")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
