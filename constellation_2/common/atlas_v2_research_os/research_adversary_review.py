from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA_ID = "atlas_v2_research_os.research_adversary_review.v1"
SCHEMA_VERSION = "1.0.0"
GENERATED_ONLY = "GENERATED_ONLY"

AUTHORITY_BOUNDARY: dict[str, bool | str] = {
    "artifact_status": GENERATED_ONLY,
    "external_api_calls_allowed": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_allocation_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "candidate_promotion_authorized": False,
    "replay_changes_authorized": False,
    "qualification_changes_authorized": False,
    "production_pipeline_integration_authorized": False,
    "human_reviewed": False,
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _read_source(path: Path, source_type: str) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    parsed: Any | None = None
    if path.suffix.lower() == ".json":
        parsed = json.loads(text)
    return {
        "source_type": source_type,
        "path": str(path),
        "content_hash": content_hash,
        "format": "json" if parsed is not None else "markdown",
        "parsed": parsed,
        "text": text,
    }


def _first_present(mapping: dict[str, Any], keys: list[str]) -> Any | None:
    for key in keys:
        if key in mapping and mapping[key] not in (None, "", []):
            return mapping[key]
    return None


def _stringify_statement(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return _stable_json(value)
    return str(value)


def _flatten_json_candidates(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _extract_subject(source: dict[str, Any]) -> dict[str, Any]:
    parsed = source.get("parsed")
    candidates = _flatten_json_candidates(parsed)
    primary = candidates[0] if candidates else {}
    text = str(source.get("text") or "").strip()
    identifier = (
        _first_present(
            primary,
            [
                "observation_id",
                "claim_id",
                "claim_seed_id",
                "hypothesis_id",
                "candidate_id",
                "artifact_id",
                "id",
            ],
        )
        or f"{source['source_type']}:{source['content_hash'][:12]}"
    )
    statement = _first_present(
        primary,
        [
            "claim_text",
            "hypothesis",
            "hypothesis_text",
            "observation",
            "description",
            "summary",
            "title",
        ],
    )
    if statement is None and text:
        statement = " ".join(text.split())[:500]
    mechanism = _first_present(primary, ["mechanism", "mechanism_family", "mechanism_tag"])
    regime = _first_present(primary, ["regime", "regime_context", "primary_regime"])
    confidence = _first_present(primary, ["confidence", "score", "edge_score"])
    return {
        "source_type": source["source_type"],
        "source_id": str(identifier),
        "statement": _stringify_statement(statement or "No explicit claim, observation, or hypothesis text found."),
        "mechanism": str(mechanism or "UNKNOWN"),
        "regime": str(regime or "UNKNOWN"),
        "confidence": confidence,
        "format": source["format"],
        "content_hash": source["content_hash"],
        "path": source["path"],
    }


def _missing_evidence(subject: dict[str, Any]) -> list[str]:
    gaps = []
    if subject["mechanism"] == "UNKNOWN":
        gaps.append("Mechanism is unspecified or cannot be extracted from the source artifact.")
    if subject["regime"] == "UNKNOWN":
        gaps.append("Regime context is unspecified or cannot be extracted from the source artifact.")
    if subject["confidence"] is None:
        gaps.append("Confidence or score field is absent.")
    if "No explicit claim" in subject["statement"]:
        gaps.append("Primary claim, observation, or hypothesis statement is absent.")
    gaps.extend(
        [
            "No independent holdout evidence is produced by this offline review.",
            "No causal mechanism evidence is produced by this offline review.",
            "No replay, qualification, promotion, or paper-placement evidence is produced by this offline review.",
        ]
    )
    return gaps


def _build_review_item(subject: dict[str, Any], index: int) -> dict[str, Any]:
    statement = subject["statement"]
    mechanism = subject["mechanism"]
    regime = subject["regime"]
    return {
        "review_item_id": f"research_adversary_item_{index:03d}",
        "source_id": subject["source_id"],
        "source_type": subject["source_type"],
        "evidence_level": GENERATED_ONLY,
        "artifact_status": GENERATED_ONLY,
        "statement_under_review": statement,
        "adversarial_read": f"Treat the statement as unproven until it survives explicit falsification in the stated mechanism/regime context: {mechanism}/{regime}.",
        "core_challenge": "The source may be pattern description, not evidence of repeatability or decision usefulness.",
        "adversarial_questions": [
            "What observable result would falsify this statement?",
            "Is the mechanism separable from broad market regime or data-selection effects?",
            "Does the source distinguish observation, claim, and hypothesis boundaries?",
            "Could duplicated observations or survivorship bias explain the apparent pattern?",
            "What minimum evidence would be required before any later human-reviewed promotion discussion?",
        ],
        "missing_evidence": _missing_evidence(subject),
        "invalidation_tests": [
            "Reject the statement for review escalation if the mechanism cannot be reconstructed from source data.",
            "Reject the statement for review escalation if regime context is unknown or conflicts with the source.",
            "Reject the statement for review escalation if the claim requires trading, sizing, broker, replay, or qualification authority.",
            "Reject the statement for review escalation if a simple null explanation is equally consistent with the source.",
        ],
        "risk_flags": [
            "GENERATED_ONLY output may sound more certain than the source supports.",
            "Offline template review cannot verify market data, chronology, independence, or execution feasibility.",
            "Source artifact may mix observation, claim, hypothesis, and candidate language.",
        ],
        "safe_next_human_review_action": "Human reviewer may edit, accept, or reject this critique; no automated promotion or pipeline action is authorized.",
    }


def build_research_adversary_review(
    *,
    observations: list[Path] | None = None,
    claims: list[Path] | None = None,
    hypotheses: list[Path] | None = None,
    generated_at: str = "2026-06-05T00:00:00Z",
) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    for path in observations or []:
        sources.append(_read_source(path, "observation"))
    for path in claims or []:
        sources.append(_read_source(path, "claim"))
    for path in hypotheses or []:
        sources.append(_read_source(path, "hypothesis"))
    if not sources:
        raise ValueError("At least one --observation, --claim, or --hypothesis path is required.")

    subjects = [_extract_subject(source) for source in sources]
    review_items = [_build_review_item(subject, index + 1) for index, subject in enumerate(subjects)]
    report = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "created_at": generated_at,
        "report_type": "offline_research_adversary_review",
        "artifact_status": GENERATED_ONLY,
        "evidence_level": GENERATED_ONLY,
        "human_reviewed": False,
        "generation_mode": "DETERMINISTIC_TEMPLATE",
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "source_artifacts": [
            {
                "source_type": subject["source_type"],
                "source_id": subject["source_id"],
                "path": subject["path"],
                "format": subject["format"],
                "content_hash": subject["content_hash"],
            }
            for subject in subjects
        ],
        "summary": {
            "source_count": len(subjects),
            "review_item_count": len(review_items),
            "generated_only_count": len(review_items),
            "human_review_required": True,
            "external_api_calls_made": False,
            "trading_recommendations_made": False,
            "candidate_promotions_made": False,
            "replay_or_qualification_changes_made": False,
        },
        "review_items": review_items,
    }
    report["content_hash"] = hashlib.sha256(_stable_json({k: v for k, v in report.items() if k != "content_hash"}).encode("utf-8")).hexdigest()
    return report


def render_research_adversary_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Research Adversary Review",
        "",
        f"Status: `{report['artifact_status']}`",
        f"Generation mode: `{report['generation_mode']}`",
        "",
        "## Summary",
        "",
        f"- Sources reviewed: {report['summary']['source_count']}",
        f"- Review items: {report['summary']['review_item_count']}",
        "- External API calls: none",
        "- Trading recommendations: none",
        "- Candidate promotion: none",
        "- Replay or qualification changes: none",
        "",
        "## Authority Boundary",
        "",
    ]
    for key, value in report["authority_boundary"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Review Items"])
    for item in report["review_items"]:
        lines.extend(
            [
                "",
                f"### `{item['source_id']}`",
                f"- Source type: `{item['source_type']}`",
                f"- Evidence level: `{item['evidence_level']}`",
                f"- Statement: {item['statement_under_review']}",
                f"- Adversarial read: {item['adversarial_read']}",
                f"- Core challenge: {item['core_challenge']}",
                "- Adversarial questions:",
            ]
        )
        for question in item["adversarial_questions"]:
            lines.append(f"  - {question}")
        lines.append("- Missing evidence:")
        for gap in item["missing_evidence"]:
            lines.append(f"  - {gap}")
        lines.append("- Invalidation tests:")
        for test in item["invalidation_tests"]:
            lines.append(f"  - {test}")
    lines.extend(
        [
            "",
            "## Human Review",
            "",
            "All output is GENERATED_ONLY until a human reviewer explicitly edits or approves it later.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_research_adversary_review(
    report: dict[str, Any],
    out_dir: str | Path,
) -> dict[str, Path]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    json_path = out_path / "research_adversary_review.json"
    summary_path = out_path / "research_adversary_review_summary.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_research_adversary_summary(report), encoding="utf-8")
    return {"json": json_path, "summary": summary_path}
