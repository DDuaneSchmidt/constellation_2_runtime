from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .research_adversary import ResearchAdversaryValidationError, validate_no_forbidden_authority_terms


EVALUATION_STATUSES = {
    "EVALUATED_ONLY",
    "USEFUL",
    "NOT_USEFUL",
    "NEEDS_HUMAN_REVIEW",
    "RETIRE_CANDIDATE_CAPABILITY",
}

RECOMMENDATIONS = {"CONTINUE_EVALUATION", "EXPAND_TEST_SET", "HOLD", "RETIRE"}

AUTHORITY_BOUNDARY = {
    "evaluation_only": True,
    "automatic_memory_writes_authorized": False,
    "trade_recommendation_authorized": False,
    "capital_recommendation_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_allocation_authorized": False,
    "candidate_promotion_authorized": False,
    "replay_override_authorized": False,
    "qualification_override_authorized": False,
    "governance_override_authorized": False,
    "production_pipeline_integration_authorized": False,
}


class ResearchAdversaryEvaluationError(ValueError):
    pass


@dataclass(frozen=True)
class HistoricalFailureMatch:
    failure_id: str
    detected: bool
    matched_terms: list[str] = field(default_factory=list)
    missed_terms: list[str] = field(default_factory=list)
    failure_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AssumptionRecallResult:
    assumption_count: int
    matched_failure_ids: list[str]
    assumption_recall: float
    missed_failure_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConstraintRecallResult:
    constraint_count: int
    matched_failure_ids: list[str]
    constraint_recall: float
    missed_failure_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FalsificationQualityResult:
    falsification_test_count: int
    tests_with_disconfirming_condition: int
    quality_score: float
    missing_quality_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchAdversaryEvaluationCase:
    case_id: str
    review_id: str
    source_artifact_ids: list[str]
    assumptions: list[str]
    constraints: list[str]
    falsification_tests: list[str]
    review_text: str
    reviewer_usefulness_score: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def assumption_count(self) -> int:
        return len(self.assumptions)

    @property
    def constraint_count(self) -> int:
        return len(self.constraints)

    @property
    def falsification_count(self) -> int:
        return len(self.falsification_tests)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchAdversaryEvaluationResult:
    case_id: str
    review_id: str
    status: str
    assumption_count: int
    constraint_count: int
    falsification_test_count: int
    known_failure_mode_detected: bool
    failure_mode_recall: float
    assumption_recall: float
    constraint_recall: float
    false_positive_count: int
    reviewer_usefulness_score: float
    estimated_research_hours_saved: float
    authority_violation_count: int
    historical_failure_matches: list[dict[str, Any]]
    assumption_recall_result: dict[str, Any]
    constraint_recall_result: dict[str, Any]
    falsification_quality_result: dict[str, Any]
    missed_known_failure_modes: list[str] = field(default_factory=list)
    false_positives: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        if self.status not in EVALUATION_STATUSES:
            raise ResearchAdversaryEvaluationError(f"invalid evaluation status: {self.status}")
        return asdict(self)


@dataclass(frozen=True)
class ResearchAdversaryEvaluationSummary:
    evaluation_status: str
    recommendation: str
    cases_evaluated: int
    known_failures_tested: int
    assumptions_identified: int
    constraints_identified: int
    falsification_proposals_reviewed: int
    missed_known_failure_modes: int
    false_positive_count: int
    reviewer_usefulness_score: float
    estimated_research_hours_saved: float
    authority_violation_count: int

    def to_dict(self) -> dict[str, Any]:
        if self.evaluation_status not in EVALUATION_STATUSES:
            raise ResearchAdversaryEvaluationError(f"invalid evaluation status: {self.evaluation_status}")
        if self.recommendation not in RECOMMENDATIONS:
            raise ResearchAdversaryEvaluationError(f"invalid recommendation: {self.recommendation}")
        return asdict(self)


def build_research_adversary_evaluation(
    *,
    reviews: str | Path,
    failures: str | Path,
    historical_artifacts: list[str | Path] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    reviews_path = Path(reviews)
    failures_path = Path(failures)
    review_payloads = _load_review_payloads(reviews_path)
    failure_payloads = _load_failure_payloads(failures_path)
    historical_text = _load_historical_text(historical_artifacts or [])
    cases = [_case_from_review(payload, index=index + 1) for index, payload in enumerate(review_payloads)]
    results = [evaluate_research_adversary_case(case, failure_payloads, historical_text=historical_text) for case in cases]
    summary = _summary_from_results(results, known_failures_tested=len(failure_payloads))
    report = {
        "schema_id": "atlas_v2_research_adversary_evaluation.v1",
        "schema_version": "1.0.0",
        "created_at": created_at or _now(),
        "report_type": "research_adversary_evaluation",
        "evaluation_scope": "Evaluation-only assessment of Research Adversary V0.1 review usefulness. No production pipeline integration.",
        "input_paths": {
            "reviews": str(reviews_path),
            "failures": str(failures_path),
            "historical_artifacts": [str(path) for path in historical_artifacts or []],
        },
        "summary": summary.to_dict(),
        "cases": [case.to_dict() for case in cases],
        "results": [result.to_dict() for result in results],
        "known_failures": failure_payloads,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "authority_boundary_verification": {
            "verified": True,
            "authority_violation_count": summary.authority_violation_count,
            "no_trade_recommendations": True,
            "no_capital_recommendations": True,
            "no_position_sizing": True,
            "no_candidate_promotion": True,
            "no_replay_override": True,
            "no_qualification_override": True,
            "no_governance_override": True,
            "no_automatic_memory_writes": True,
        },
    }
    return report


def evaluate_research_adversary_case(
    case: ResearchAdversaryEvaluationCase,
    known_failures: list[dict[str, Any]],
    *,
    historical_text: str = "",
) -> ResearchAdversaryEvaluationResult:
    _assert_no_authority_violations(case.to_dict())
    failure_matches = [_match_failure(case.review_text, failure) for failure in known_failures]
    detected_ids = [match.failure_id for match in failure_matches if match.detected]
    missed_ids = [match.failure_id for match in failure_matches if not match.detected]
    failure_recall = _rate(len(detected_ids), len(known_failures))
    assumption_recall = _assumption_recall_from_items(case.assumptions, known_failures)
    constraint_recall = _constraint_recall_from_items(case.constraints, known_failures)
    falsification_quality = _falsification_quality(case.falsification_tests)
    evidence_tokens = _tokens(historical_text + " " + " ".join(_failure_text(failure) for failure in known_failures))
    false_positives = _false_positives(case.assumptions + case.constraints + case.falsification_tests, evidence_tokens)
    usefulness = case.reviewer_usefulness_score
    if usefulness is None:
        usefulness = _derived_usefulness(failure_recall, assumption_recall.assumption_recall, constraint_recall.constraint_recall, falsification_quality.quality_score, len(false_positives))
    hours_saved = round((case.assumption_count + case.constraint_count + case.falsification_count) * usefulness * 0.25, 3)
    status = _case_status(usefulness, failure_recall, len(false_positives))
    return ResearchAdversaryEvaluationResult(
        case_id=case.case_id,
        review_id=case.review_id,
        status=status,
        assumption_count=case.assumption_count,
        constraint_count=case.constraint_count,
        falsification_test_count=case.falsification_count,
        known_failure_mode_detected=bool(detected_ids),
        failure_mode_recall=failure_recall,
        assumption_recall=assumption_recall.assumption_recall,
        constraint_recall=constraint_recall.constraint_recall,
        false_positive_count=len(false_positives),
        reviewer_usefulness_score=round(usefulness, 6),
        estimated_research_hours_saved=hours_saved,
        authority_violation_count=0,
        historical_failure_matches=[match.to_dict() for match in failure_matches],
        assumption_recall_result=assumption_recall.to_dict(),
        constraint_recall_result=constraint_recall.to_dict(),
        falsification_quality_result=falsification_quality.to_dict(),
        missed_known_failure_modes=missed_ids,
        false_positives=false_positives,
    )


def write_research_adversary_evaluation_report(report: dict[str, Any], out: str | Path) -> dict[str, Path]:
    out_path = Path(out)
    out_path.mkdir(parents=True, exist_ok=True)
    json_path = out_path / "research_adversary_evaluation_summary.json"
    md_path = out_path / "research_adversary_evaluation_summary.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_research_adversary_evaluation_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def render_research_adversary_evaluation_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    results = report.get("results", [])
    lines = [
        "# Research Adversary Evaluation",
        "",
        "## Evaluation scope",
        "",
        str(report.get("evaluation_scope", "")),
        "",
        "## Historical cases reviewed",
        "",
        f"- Cases evaluated: {summary.get('cases_evaluated', 0)}",
        f"- Evaluation status: {summary.get('evaluation_status')}",
        "",
        "## Known failures tested",
        "",
        f"- Known failures tested: {summary.get('known_failures_tested', 0)}",
        f"- Missed known failure modes: {summary.get('missed_known_failure_modes', 0)}",
        "",
        "## Assumptions identified",
        "",
        f"- Assumptions identified: {summary.get('assumptions_identified', 0)}",
        "",
        "## Constraints identified",
        "",
        f"- Constraints identified: {summary.get('constraints_identified', 0)}",
        "",
        "## Falsification proposals reviewed",
        "",
        f"- Falsification proposals reviewed: {summary.get('falsification_proposals_reviewed', 0)}",
        "",
        "## Missed known failure modes",
        "",
    ]
    missed = sorted({failure_id for result in results for failure_id in result.get("missed_known_failure_modes", [])})
    if missed:
        lines.extend(f"- {failure_id}" for failure_id in missed)
    else:
        lines.append("- None.")
    lines.extend(["", "## False positives", ""])
    false_positives = [item for result in results for item in result.get("false_positives", [])]
    if false_positives:
        lines.extend(f"- {item}" for item in false_positives[:20])
    else:
        lines.append("- None.")
    lines.extend(
        [
            "",
            "## Reviewer usefulness score",
            "",
            f"- Reviewer usefulness score: {summary.get('reviewer_usefulness_score', 0.0)}",
            f"- Estimated research hours saved: {summary.get('estimated_research_hours_saved', 0.0)}",
            "",
            "## Authority boundary verification",
            "",
        ]
    )
    for key, value in report.get("authority_boundary_verification", {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "This evaluation has no authority to modify candidate, replay, qualification, capital, governance, paper-forward, portfolio, or memory systems.",
            "",
            "## Recommendation",
            "",
            str(summary.get("recommendation", "HOLD")),
            "",
        ]
    )
    return "\n".join(lines)


def _load_review_payloads(path: Path) -> list[dict[str, Any]]:
    if path.is_file():
        files = [path]
    else:
        files = sorted(path.rglob("*.json")) if path.exists() else []
    payloads = []
    for file_path in files:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if _looks_like_review(payload):
            payloads.append(payload)
    return payloads


def _load_failure_payloads(path: Path) -> list[dict[str, Any]]:
    if path.is_file():
        files = [path]
    else:
        files = sorted([*path.rglob("*.yaml"), *path.rglob("*.yml"), *path.rglob("*.json")]) if path.exists() else []
    failures = []
    for file_path in files:
        if file_path.suffix.lower() == ".json":
            raw = json.loads(file_path.read_text(encoding="utf-8"))
        else:
            raw = _parse_simple_yaml(file_path.read_text(encoding="utf-8"))
        if raw:
            raw.setdefault("id", file_path.stem)
            raw["path"] = str(file_path)
            failures.append(raw)
    return failures


def _case_from_review(payload: dict[str, Any], *, index: int) -> ResearchAdversaryEvaluationCase:
    review_id = str(payload.get("review_id") or payload.get("content_hash") or f"research_adversary_review_{index:03d}")
    if "review_items" in payload:
        assumptions = _extract_strings(payload.get("assumptions", []))
        constraints = _extract_strings(payload.get("constraints", []))
        falsification_tests = []
        for item in payload.get("review_items", []):
            assumptions.extend(_extract_strings(item.get("adversarial_questions", [])))
            constraints.extend(_extract_strings(item.get("missing_evidence", [])))
            constraints.extend(_extract_strings(item.get("risk_flags", [])))
            falsification_tests.extend(_extract_strings(item.get("invalidation_tests", [])))
        source_ids = [str(row.get("source_id")) for row in payload.get("source_artifacts", []) if row.get("source_id")]
    else:
        assumptions = _extract_strings(payload.get("assumptions", []))
        constraints = _extract_strings(payload.get("constraints", []))
        falsification_tests = _extract_strings(payload.get("falsification_tests", []))
        source_ids = [str(payload.get("source_observation_id") or payload.get("source_claim_id") or payload.get("source_hypothesis_id") or review_id)]
    reviewer_score = payload.get("reviewer_usefulness_score")
    return ResearchAdversaryEvaluationCase(
        case_id=f"research_adversary_eval_case_{index:03d}",
        review_id=review_id,
        source_artifact_ids=source_ids,
        assumptions=assumptions,
        constraints=constraints,
        falsification_tests=falsification_tests,
        review_text=json.dumps(payload, sort_keys=True),
        reviewer_usefulness_score=float(reviewer_score) if reviewer_score is not None else None,
        metadata={"schema_id": payload.get("schema_id"), "report_type": payload.get("report_type")},
    )


def _match_failure(text: str, failure: dict[str, Any]) -> HistoricalFailureMatch:
    failure_terms = _important_terms(_failure_text(failure))
    text_tokens = _tokens(text)
    matched = sorted(term for term in failure_terms if term in text_tokens)
    detected = len(matched) >= min(2, len(failure_terms)) if failure_terms else False
    return HistoricalFailureMatch(
        failure_id=str(failure.get("id") or failure.get("failure_id") or "unknown_failure"),
        detected=detected,
        matched_terms=matched,
        missed_terms=sorted(term for term in failure_terms if term not in text_tokens),
        failure_summary=_failure_text(failure)[:300],
    )


def _matched_and_missed_from_items(items: list[str], failures: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    item_text = " ".join(items)
    item_tokens = _tokens(item_text)
    matched_ids = []
    missed_ids = []
    for failure in failures:
        terms = _important_terms(_failure_text(failure))
        matched = [term for term in terms if term in item_tokens]
        if matched and len(matched) >= min(2, len(terms)):
            matched_ids.append(str(failure.get("id") or failure.get("failure_id") or "unknown_failure"))
        else:
            missed_ids.append(str(failure.get("id") or failure.get("failure_id") or "unknown_failure"))
    return matched_ids, missed_ids


def _assumption_recall_from_items(items: list[str], failures: list[dict[str, Any]]) -> AssumptionRecallResult:
    matched_ids, missed_ids = _matched_and_missed_from_items(items, failures)
    return AssumptionRecallResult(
        assumption_count=len(items),
        matched_failure_ids=matched_ids,
        assumption_recall=_rate(len(matched_ids), len(failures)),
        missed_failure_ids=missed_ids,
    )


def _constraint_recall_from_items(items: list[str], failures: list[dict[str, Any]]) -> ConstraintRecallResult:
    matched_ids, missed_ids = _matched_and_missed_from_items(items, failures)
    return ConstraintRecallResult(
        constraint_count=len(items),
        matched_failure_ids=matched_ids,
        constraint_recall=_rate(len(matched_ids), len(failures)),
        missed_failure_ids=missed_ids,
    )


def _falsification_quality(tests: list[str]) -> FalsificationQualityResult:
    quality_markers = re.compile(r"\b(reject|falsif|invalidat|disconfirm|fail|control|holdout|null|threshold|minimum|replay)\b", re.IGNORECASE)
    with_condition = [test for test in tests if quality_markers.search(test)]
    notes = []
    if not tests:
        notes.append("No falsification tests present.")
    if tests and len(with_condition) < len(tests):
        notes.append("Some falsification tests lack explicit disconfirming conditions.")
    return FalsificationQualityResult(
        falsification_test_count=len(tests),
        tests_with_disconfirming_condition=len(with_condition),
        quality_score=_rate(len(with_condition), len(tests)),
        missing_quality_notes=notes,
    )


def _false_positives(items: list[str], evidence_tokens: set[str]) -> list[str]:
    false_positives = []
    for item in items:
        terms = _important_terms(item)
        if terms and not any(term in evidence_tokens for term in terms):
            false_positives.append(item)
    return false_positives


def _summary_from_results(results: list[ResearchAdversaryEvaluationResult], *, known_failures_tested: int) -> ResearchAdversaryEvaluationSummary:
    cases = len(results)
    assumptions = sum(result.assumption_count for result in results)
    constraints = sum(result.constraint_count for result in results)
    falsifications = sum(result.falsification_test_count for result in results)
    missed = sum(len(result.missed_known_failure_modes) for result in results)
    false_positive_count = sum(result.false_positive_count for result in results)
    usefulness = round(sum(result.reviewer_usefulness_score for result in results) / cases, 6) if cases else 0.0
    hours_saved = round(sum(result.estimated_research_hours_saved for result in results), 3)
    authority_violations = sum(result.authority_violation_count for result in results)
    status = _summary_status(cases, usefulness, authority_violations)
    recommendation = _recommendation(cases, usefulness, false_positive_count, missed, known_failures_tested, authority_violations)
    return ResearchAdversaryEvaluationSummary(
        evaluation_status=status,
        recommendation=recommendation,
        cases_evaluated=cases,
        known_failures_tested=known_failures_tested,
        assumptions_identified=assumptions,
        constraints_identified=constraints,
        falsification_proposals_reviewed=falsifications,
        missed_known_failure_modes=missed,
        false_positive_count=false_positive_count,
        reviewer_usefulness_score=usefulness,
        estimated_research_hours_saved=hours_saved,
        authority_violation_count=authority_violations,
    )


def _summary_status(cases: int, usefulness: float, authority_violations: int) -> str:
    if authority_violations:
        return "RETIRE_CANDIDATE_CAPABILITY"
    if cases == 0:
        return "NEEDS_HUMAN_REVIEW"
    if usefulness >= 0.6:
        return "USEFUL"
    if usefulness >= 0.35:
        return "EVALUATED_ONLY"
    return "NOT_USEFUL"


def _recommendation(cases: int, usefulness: float, false_positives: int, missed: int, known_failures: int, authority_violations: int) -> str:
    if authority_violations:
        return "RETIRE"
    if cases == 0:
        return "HOLD"
    if known_failures == 0 or missed > 0:
        return "EXPAND_TEST_SET"
    if usefulness >= 0.6 and false_positives <= cases:
        return "CONTINUE_EVALUATION"
    if usefulness < 0.35:
        return "HOLD"
    return "EXPAND_TEST_SET"


def _case_status(usefulness: float, failure_recall: float, false_positive_count: int) -> str:
    if usefulness >= 0.6 and failure_recall >= 0.5 and false_positive_count <= 2:
        return "USEFUL"
    if usefulness < 0.25:
        return "NOT_USEFUL"
    return "EVALUATED_ONLY"


def _derived_usefulness(failure_recall: float, assumption_recall: float, constraint_recall: float, falsification_quality: float, false_positives: int) -> float:
    penalty = min(0.3, false_positives * 0.05)
    return round(max(0.0, (failure_recall * 0.35) + (assumption_recall * 0.2) + (constraint_recall * 0.2) + (falsification_quality * 0.25) - penalty), 6)


def _looks_like_review(payload: dict[str, Any]) -> bool:
    return bool(payload.get("review_items") or payload.get("assumptions") or payload.get("falsification_tests") or payload.get("schema_id") == "atlas_v2_research_os.research_adversary_review.v1")


def _extract_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        candidates = []
        for key in ["statement", "description", "summary", "test_description", "expected_disconfirming_observation", "minimum_evidence_required", "question", "constraint", "impact"]:
            if value.get(key):
                candidates.append(str(value[key]))
        return candidates or [json.dumps(value, sort_keys=True)]
    if isinstance(value, list):
        rows: list[str] = []
        for item in value:
            rows.extend(_extract_strings(item))
        return rows
    return [str(value)]


def _load_historical_text(paths: list[str | Path]) -> str:
    chunks = []
    for path_value in paths:
        path = Path(path_value)
        if path.exists() and path.is_file():
            chunks.append(path.read_text(encoding="utf-8"))
    return " ".join(chunks)


def _failure_text(failure: dict[str, Any]) -> str:
    return " ".join(str(failure.get(key, "")) for key in ["id", "failure_id", "what_we_expected", "what_failed", "why_failed", "what_to_try_next", "summary", "description", "failure_mode", "status"])


def _important_terms(text: str) -> set[str]:
    stop = {
        "the",
        "and",
        "or",
        "for",
        "with",
        "would",
        "could",
        "should",
        "this",
        "that",
        "from",
        "into",
        "than",
        "what",
        "next",
        "failed",
        "failure",
        "expected",
        "control",
        "controls",
        "mechanism",
        "source",
        "claim",
        "hypothesis",
        "evidence",
    }
    return {token for token in _tokens(text) if len(token) >= 5 and token not in stop}


def _tokens(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9_]+", text.lower()))


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    row: dict[str, Any] = {}
    current_key: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith("  ") and current_key:
            row[current_key] = f"{row.get(current_key, '')} {line.strip()}".strip()
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            current_key = key.strip()
            row[current_key] = value.strip().strip('"')
    return row


def _assert_no_authority_violations(payload: Any) -> None:
    try:
        validate_no_forbidden_authority_terms(payload)
    except ResearchAdversaryValidationError as exc:
        raise ResearchAdversaryEvaluationError(f"authority-violating language rejected: {exc}") from exc


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
