from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

DEFAULT_PERIOD = "2026-06-04"
UNKNOWN_VALUE = "UNKNOWN"

TEXT_SUFFIXES = {".md", ".txt"}
STRUCTURED_SUFFIXES = {".yaml", ".yml", ".json"}


def parse_historical_experience_source(path: Path, *, repo_root: Path) -> dict[str, Any] | None:
    rel = _relative(repo_root, path)
    suffix = path.suffix.lower()
    if suffix in TEXT_SUFFIXES:
        text = path.read_text(encoding="utf-8", errors="ignore")
        if rel.startswith("docs/aegis/adr/"):
            return _parse_adr(rel, text)
        if rel.startswith("docs/aegis/investment_thesis_factory/"):
            return _parse_investment_thesis_factory(rel, text)
        if rel.startswith("docs/aegis/technical_strategy_factory/"):
            return _parse_technical_strategy_factory(rel, text)
        if rel.startswith("research_journal/reports/"):
            return _parse_research_journal_report(rel, text)
        if _is_review_artifact(rel, text):
            return _parse_review_artifact(rel, text)
    if suffix in STRUCTURED_SUFFIXES:
        data = _load_structured(path)
        if not data:
            return None
        if rel.startswith("research_journal/failures/"):
            return _parse_failure_report(rel, data)
        if _is_failure_artifact(rel, data):
            return _parse_failure_report(rel, data)
        if _is_review_artifact(rel, json.dumps(data, sort_keys=True, default=str)):
            return _parse_structured_review(rel, data)
    return None


def _parse_adr(rel: str, text: str) -> dict[str, Any] | None:
    decision = _section(text, "Decision")
    consequences = _section(text, "Consequences")
    context = _section(text, "Context")
    status = _section(text, "Status")
    if not decision and not consequences:
        return None
    return _base(
        rel,
        source_type="DECISION",
        source_family="adr",
        parser_name="adr_document_parser_v1",
        decision_summary=_compact(decision or _heading(text) or rel),
        expected_outcome=_compact(context or decision) if context or decision else None,
        actual_outcome=_compact(consequences) if consequences else None,
        confidence=0.58 if str(status).lower().find("accepted") >= 0 else 0.5,
        regret_score=0.4,
        conversion_reason="ADR parser preserved explicit decision context and documented consequences without creating authority.",
        provenance_anchor="decision",
        lesson=_compact(consequences, 240) if consequences else None,
        extracted_fields={
            "decision": bool(decision),
            "expectation": bool(context or decision),
            "outcome": bool(consequences),
        },
    )


def _parse_investment_thesis_factory(rel: str, text: str) -> dict[str, Any] | None:
    expectation = _first_section(
        text,
        (
            "Claim Statement",
            "Null Hypothesis",
            "Success Criteria",
            "Decision Framework",
            "Purpose",
        ),
    )
    outcome = _first_section(
        text,
        (
            "Failure Criteria",
            "Failure must be recorded",
            "Outputs",
            "Evidence Requirements",
            "Decision rules",
        ),
    )
    contradiction = _first_section(text, ("Counterexample Selection Rules", "Contradiction", "Hindsight Bias Controls", "Survivorship Bias Controls"))
    decision = _first_section(text, ("Decision Framework", "Validation Procedure", "Promotion Standard", "Rejection Standard", "Retirement Standard"))
    if not any((expectation, outcome, contradiction, decision)):
        return None
    actual = outcome or contradiction
    return _base(
        rel,
        source_type="CONTRADICTION" if contradiction else "RESEARCH_OUTCOME",
        source_family="investment_thesis_factory",
        parser_name="investment_thesis_factory_parser_v1",
        decision_summary=_compact(decision or _heading(text) or rel),
        expected_outcome=_compact(expectation) if expectation else None,
        actual_outcome=_compact(actual) if actual else None,
        confidence=_confidence_from_text(text, default=0.56),
        regret_score=0.62 if contradiction else 0.48,
        conversion_reason="Investment Thesis Factory parser extracted stated expectations, failure criteria, and contradiction controls as read-only historical evidence.",
        provenance_anchor="investment-thesis-factory",
        lesson=_compact(contradiction or outcome, 240) if contradiction or outcome else None,
        extracted_fields={
            "expectation": bool(expectation),
            "outcome": bool(actual),
            "contradiction": bool(contradiction),
            "decision": bool(decision),
        },
    )


def _parse_technical_strategy_factory(rel: str, text: str) -> dict[str, Any] | None:
    lowered = rel.lower()
    expectation = _first_section(
        text,
        (
            "Claim",
            "Claim Statement",
            "Expected",
            "Success Criteria",
            "Test Objective",
            "Purpose",
            "Scope",
        ),
    )
    outcome = _first_section(
        text,
        (
            "Result Review Status",
            "Final Verdict",
            "Full-Period Metrics",
            "Development And Validation",
            "Impact",
            "Hostile Findings",
            "Conclusion",
        ),
    )
    contradiction = _first_section(text, ("Known Limitations", "Hostile Findings", "Required Next Evidence", "Failure Criteria", "Concerns"))
    decision = _first_section(text, ("Implementation Decision", "Decision", "Result Review Status", "Final Verdict"))
    if not any((expectation, outcome, contradiction, decision)):
        return None
    source_type = "CONTRADICTION" if "hostile_review" in lowered or contradiction else "RESEARCH_OUTCOME"
    if "implementation_decision" in lowered or decision:
        source_type = "DECISION" if not outcome and not contradiction else source_type
    actual = outcome or contradiction
    return _base(
        rel,
        source_type=source_type,
        source_family="technical_strategy_factory",
        parser_name="technical_strategy_factory_parser_v1",
        decision_summary=_compact(decision or _heading(text) or rel),
        expected_outcome=_compact(expectation) if expectation else None,
        actual_outcome=_compact(actual) if actual else None,
        confidence=_confidence_from_text(text, default=0.6),
        regret_score=0.68 if source_type == "CONTRADICTION" else 0.55,
        conversion_reason="Technical Strategy Factory parser extracted declared test expectations and review outcomes without certifying strategies or allocation.",
        provenance_anchor="technical-strategy-factory",
        lesson=_compact(contradiction or outcome, 240) if contradiction or outcome else None,
        extracted_fields={
            "expectation": bool(expectation),
            "outcome": bool(actual),
            "contradiction": bool(contradiction),
            "decision": bool(decision),
        },
    )


def _parse_research_journal_report(rel: str, text: str) -> dict[str, Any] | None:
    expectation = _first_section(text, ("Expected outcome", "Expected", "Expectation", "Claim", "Decision", "Prior belief", "Purpose"))
    outcome = _first_section(text, ("Actual outcome", "Outcome", "Result", "Conclusion", "Review result", "What changed"))
    contradiction = _first_section(text, ("Strongest contradictory evidence", "Contradiction", "Risks", "Root cause", "Failure"))
    lesson = _first_section(text, ("Lesson", "Lesson learned", "What we learned", "Recommended behavior change", "Next action"))
    decision = _first_section(text, ("Decision", "Recommendation", "Review status", "Status"))
    if not any((expectation, outcome, contradiction, lesson, decision)):
        return None
    actual = outcome or contradiction or lesson
    return _base(
        rel,
        source_type="CONTRADICTION" if contradiction else "RESEARCH_OUTCOME",
        source_family="research_journal_report",
        parser_name="research_journal_report_parser_v1",
        decision_summary=_compact(decision or _heading(text) or expectation or rel),
        expected_outcome=_compact(expectation) if expectation else None,
        actual_outcome=_compact(actual) if actual else None,
        confidence=_confidence_from_text(text, default=0.57),
        regret_score=0.66 if contradiction else 0.5,
        conversion_reason="Research journal report parser extracted explicit expectation/outcome or contradiction sections as historical evidence.",
        provenance_anchor="research-journal-report",
        lesson=_compact(lesson or contradiction or outcome, 240) if lesson or contradiction or outcome else None,
        extracted_fields={
            "expectation": bool(expectation),
            "outcome": bool(actual),
            "contradiction": bool(contradiction),
            "decision": bool(decision),
            "lesson": bool(lesson),
        },
    )


def _parse_failure_report(rel: str, data: dict[str, Any]) -> dict[str, Any] | None:
    expectation = _first_key(data, ("what_we_expected", "expected_outcome", "expected", "expectation", "claim", "hypothesis"))
    failure = _first_key(data, ("what_failed", "failure_reason", "why_failed", "actual_outcome", "outcome", "result"))
    lesson = _first_key(data, ("lesson", "lesson_learned", "what_we_learned", "prevention", "fix"))
    if not expectation and not failure:
        return None
    reason = _first_key(data, ("why_failed", "failure_reason", "root_cause")) or "Failure report preserves expected and failed outcome fields."
    return _base(
        rel,
        source_type="FAILURE",
        source_family="failure_report",
        parser_name="failure_report_parser_v1",
        historical_date=str(data.get("date") or DEFAULT_PERIOD),
        decision_summary=_compact(expectation or str(data.get("id") or rel)),
        expected_outcome=_compact(expectation) if expectation else None,
        actual_outcome=_compact(failure) if failure else None,
        confidence=_bounded_score(data.get("confidence"), default=0.65),
        regret_score=_bounded_score(data.get("regret_score"), default=0.75),
        conversion_reason=_compact(reason),
        provenance_anchor=str(data.get("id") or Path(rel).stem),
        lesson=_compact(lesson or reason, 240) if lesson or reason else None,
        extracted_fields={
            "expectation": bool(expectation),
            "outcome": bool(failure),
            "failure_reason": bool(reason),
            "lesson": bool(lesson),
        },
    )


def _parse_structured_review(rel: str, data: dict[str, Any]) -> dict[str, Any] | None:
    expectation = _first_key(data, ("expected", "expectation", "expected_outcome", "claim", "decision", "hypothesis"))
    outcome = _first_key(data, ("actual", "actual_outcome", "outcome", "result", "conclusion", "review_result"))
    contradiction = _first_key(data, ("contradiction", "counterevidence", "risk", "failure_reason"))
    if not any((expectation, outcome, contradiction)):
        return None
    return _base(
        rel,
        source_type="CONTRADICTION" if contradiction else "RESEARCH_OUTCOME",
        source_family="review_artifact",
        parser_name="structured_review_parser_v1",
        historical_date=str(data.get("date") or DEFAULT_PERIOD),
        decision_summary=_compact(_first_key(data, ("decision", "summary", "title")) or expectation or rel),
        expected_outcome=_compact(expectation) if expectation else None,
        actual_outcome=_compact(outcome or contradiction) if outcome or contradiction else None,
        confidence=_bounded_score(data.get("confidence"), default=0.55),
        regret_score=_bounded_score(data.get("regret_score"), default=0.58),
        conversion_reason="Structured review parser extracted explicit expectation, outcome, or contradiction fields.",
        provenance_anchor=str(data.get("id") or Path(rel).stem),
        lesson=_compact(_first_key(data, ("lesson", "lesson_learned", "what_we_learned")) or contradiction, 240) if _first_key(data, ("lesson", "lesson_learned", "what_we_learned")) or contradiction else None,
        extracted_fields={
            "expectation": bool(expectation),
            "outcome": bool(outcome or contradiction),
            "contradiction": bool(contradiction),
        },
    )


def _parse_review_artifact(rel: str, text: str) -> dict[str, Any] | None:
    return _parse_research_journal_report(rel, text)


def _base(
    rel: str,
    *,
    source_type: str,
    source_family: str,
    parser_name: str,
    decision_summary: str | None,
    expected_outcome: str | None,
    actual_outcome: str | None,
    confidence: float,
    regret_score: float,
    conversion_reason: str,
    provenance_anchor: str,
    historical_date: str = DEFAULT_PERIOD,
    lesson: str | None = None,
    extracted_fields: dict[str, bool] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "record_id": f"hist-{_stable_id(rel)}",
        "source_artifact": rel,
        "source_type": source_type,
        "source_family": source_family,
        "parser_name": parser_name,
        "historical_date": historical_date,
        "decision_summary": decision_summary or UNKNOWN_VALUE,
        "confidence": confidence,
        "regret_score": regret_score,
        "conversion_reason": conversion_reason,
        "provenance_reference": f"{rel}#{provenance_anchor}",
        "parser_extracted_fields": sorted(key for key, value in (extracted_fields or {}).items() if value),
    }
    if expected_outcome:
        record["expected_outcome"] = expected_outcome
    if actual_outcome:
        record["actual_outcome"] = actual_outcome
    if lesson:
        record["lesson"] = lesson
    return record


def _section(text: str, heading: str) -> str | None:
    marker = heading.strip().lower()
    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        title = stripped.lstrip("#").strip().lower()
        if title == marker or title.endswith(f". {marker}") or marker in title:
            body: list[str] = []
            for next_line in lines[index + 1:]:
                if next_line.strip().startswith("#"):
                    break
                cleaned = next_line.strip()
                if cleaned and not _table_separator(cleaned):
                    body.append(cleaned.strip("- "))
                if len(" ".join(body)) >= 900:
                    break
            return " ".join(body) if body else None
    pattern = re.compile(rf"(?im)^\s*(?:[-*]\s*)?{re.escape(heading)}\s*:\s*(.+)$")
    match = pattern.search(text)
    return match.group(1).strip() if match else None


def _first_section(text: str, headings: tuple[str, ...]) -> str | None:
    for heading in headings:
        value = _section(text, heading)
        if value:
            return value
    return None


def _first_key(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if value not in (None, "", [], {}):
            if isinstance(value, list):
                return "; ".join(str(item) for item in value if str(item).strip())
            if isinstance(value, dict):
                return json.dumps(value, sort_keys=True, default=str)
            return str(value)
    return None


def _heading(text: str) -> str | None:
    for line in text.splitlines():
        if line.startswith("#"):
            return line.lstrip("#").strip()
    return None


def _confidence_from_text(text: str, *, default: float) -> float:
    match = re.search(r"(?i)\bconfidence(?: score)?\s*[:|]\s*([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return default
    value = float(match.group(1))
    if value > 1:
        value = value / 10 if value <= 10 else value / 100
    return round(min(max(value, 0.0), 1.0), 6)


def _bounded_score(value: Any, *, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed > 1:
        parsed = parsed / 10 if parsed <= 10 else parsed / 100
    return round(min(max(parsed, 0.0), 1.0), 6)


def _is_review_artifact(rel: str, text: str) -> bool:
    lowered = f"{rel}\n{text}".lower()
    return any(token in lowered for token in ("review", "retrospective", "result_review", "hostile_review", "outcome_review"))


def _is_failure_artifact(rel: str, data: dict[str, Any]) -> bool:
    lowered = rel.lower()
    return "failure" in lowered or "fail_" in lowered or any(key in data for key in ("what_failed", "why_failed", "failure_reason"))


def _load_structured(path: Path) -> dict[str, Any]:
    if path.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            return {}
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _relative(repo_root: Path, path: Path) -> str:
    path = Path(path)
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _compact(value: Any, limit: int = 500) -> str:
    text = " ".join(str(value).split())
    return text[:limit] if len(text) > limit else text


def _table_separator(line: str) -> bool:
    stripped = line.replace("|", "").replace("-", "").replace(":", "").strip()
    return stripped == ""


def _stable_id(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
