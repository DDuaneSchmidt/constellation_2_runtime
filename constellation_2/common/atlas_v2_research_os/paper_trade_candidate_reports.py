from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .edge_qualification import now_utc, qualify_edge, stable_id
from .edge_qualification_models import EDGE_AUTHORITY_LEVEL, PAPER_CANDIDATE_LIMITATION, EdgeQualificationInput
from .candidate_symbol_attribution import attribute_candidate_symbols
from .paper_trade_candidate_governance import validate_candidate_governance
from .paper_trade_candidate_models import PaperTradeCandidate, PaperTradeCandidateCertification

PAPER_TRADE_CANDIDATE_ROOT = Path("reports/atlas_v2_research_os/paper_trade_candidates")


def create_paper_trade_candidate(edge_input: EdgeQualificationInput | dict[str, Any], *, candidate_id: str | None = None, created_at: str | None = None) -> dict[str, Any]:
    row = edge_input if isinstance(edge_input, EdgeQualificationInput) else EdgeQualificationInput(**edge_input)
    qualified = qualify_edge(row, created_at=created_at)
    summary = qualified["input_summary"]
    metadata = dict(row.metadata)
    attribution = attribute_candidate_symbols({
        "candidate_id": candidate_id,
        "mechanism": (summary.get("mechanism_tags") or ["UNKNOWN"])[0],
        "regime": (summary.get("regime_context") or {}).get("label", "UNKNOWN"),
        "source_lineage": metadata,
    })
    lifecycle_state = "QUALIFIED_FOR_HUMAN_REVIEW" if qualified["eligible"] else "DISQUALIFIED"
    candidate = PaperTradeCandidate(
        candidate_id=candidate_id or stable_id("ptc", summary["source_artifact_ids"] + summary["source_hypothesis_ids"] + [qualified["qualification_id"]]),
        created_at=created_at or qualified["created_at"],
        source_artifact_ids=list(summary["source_artifact_ids"]),
        source_hypothesis_ids=list(summary["source_hypothesis_ids"]),
        source_experiment_ids=list(summary["source_experiment_ids"]),
        source_memory_ids=list(summary["source_memory_ids"]),
        mechanism_tags=list(summary["mechanism_tags"]),
        regime_context=dict(summary["regime_context"]),
        edge_score=float(qualified["score"]["edge_score"]),
        confidence=_candidate_confidence(qualified),
        evidence_level=summary["evidence_level"],
        lifecycle_state=lifecycle_state,
        qualification_reasons=list(qualified["qualification_reasons"]),
        disqualification_reasons=list(qualified["disqualification_reasons"]),
        paper_trade_eligible=bool(qualified["eligible"]),
        human_review_required=True,
        authority_level=EDGE_AUTHORITY_LEVEL,
        historical_replay_summary=dict(row.metadata.get("historical_replay_summary", {})),
        historical_replay_certification=dict(row.metadata.get("historical_replay_certification", {})),
        candidate_symbols=list(attribution.get("candidate_symbols", [])),
        candidate_universe_symbols=list(attribution.get("candidate_universe_symbols", [])),
        candidate_timeframes=list(attribution.get("candidate_timeframes", [])),
        candidate_source_observation_ids=list(attribution.get("candidate_source_observation_ids", [])),
        symbol_attribution_confidence=float(attribution.get("symbol_attribution_confidence") or 0.0),
        symbol_attribution_method=str(attribution.get("symbol_attribution_method") or "UNKNOWN"),
        metadata={"qualification_id": qualified["qualification_id"], "qualification": qualified, "symbol_attribution": attribution, "research_only": True, **row.metadata},
    ).to_dict()
    validate_candidate_governance(candidate)
    return candidate


def certify_paper_trade_candidate(candidate: dict[str, Any], *, certification_id: str | None = None, created_at: str | None = None) -> dict[str, Any]:
    try:
        validate_candidate_governance(candidate)
    except Exception as exc:
        return PaperTradeCandidateCertification(
            certification_id=certification_id or stable_id("ptc_cert", [candidate.get("candidate_id", "unknown"), str(exc)]),
            candidate_id=candidate.get("candidate_id", "unknown"),
            created_at=created_at or now_utc(),
            status="GOVERNANCE_FAIL",
            reasons=[str(exc)],
            governance_pass=False,
            lineage_complete=False,
            paper_trade_eligible=False,
            human_review_required=True,
        ).to_dict()
    reasons: list[str] = []
    lineage_complete = bool(candidate.get("source_artifact_ids"))
    if not lineage_complete:
        reasons.append("missing source artifact lineage")
    if candidate.get("paper_trade_eligible") is not True:
        reasons.extend(candidate.get("disqualification_reasons", []) or ["candidate is not paper-trade eligible"])
    if candidate.get("human_review_required") is not True:
        reasons.append("human review is required")
    status = "CERTIFIED_FOR_HUMAN_REVIEW" if not reasons else "NOT_CERTIFIED"
    if status == "CERTIFIED_FOR_HUMAN_REVIEW":
        reasons.append("eligible only for human-reviewed paper testing consideration")
    return PaperTradeCandidateCertification(
        certification_id=certification_id or stable_id("ptc_cert", [candidate["candidate_id"], status]),
        candidate_id=candidate["candidate_id"],
        created_at=created_at or now_utc(),
        status=status,
        reasons=reasons,
        governance_pass=True,
        lineage_complete=lineage_complete,
        paper_trade_eligible=bool(candidate.get("paper_trade_eligible")) and status == "CERTIFIED_FOR_HUMAN_REVIEW",
        human_review_required=True,
        metadata={"research_only": True},
    ).to_dict()


def build_paper_trade_candidate_report(edge_input: EdgeQualificationInput | dict[str, Any], *, candidate_id: str | None = None, created_at: str | None = None) -> dict[str, Any]:
    candidate = create_paper_trade_candidate(edge_input, candidate_id=candidate_id, created_at=created_at)
    certification = certify_paper_trade_candidate(candidate, created_at=created_at)
    report = {
        "schema_id": "atlas_v2_research_os_paper_trade_candidate_report_v1",
        "schema_version": "v1",
        "created_at": created_at or now_utc(),
        "candidate": candidate,
        "certification": certification,
        "edge_score": candidate["edge_score"],
        "score_explanation": candidate["metadata"]["qualification"]["score"]["explanation"],
        "qualification_reasons": candidate["qualification_reasons"],
        "disqualification_reasons": candidate["disqualification_reasons"],
        "paper_trade_eligible": candidate["paper_trade_eligible"],
        "human_review_required": candidate["human_review_required"],
        "authority_level": candidate["authority_level"],
        "historical_replay_summary": candidate.get("historical_replay_summary", {}),
        "historical_replay_certification": candidate.get("historical_replay_certification", {}),
        "limitations": [PAPER_CANDIDATE_LIMITATION],
        "recommendation": "Review for human-approved paper testing." if candidate["paper_trade_eligible"] else "Continue research; do not paper test yet.",
    }
    validate_candidate_governance(report["candidate"])
    return report


def write_paper_trade_candidate_report(edge_input: EdgeQualificationInput | dict[str, Any], *, root: str | Path = PAPER_TRADE_CANDIDATE_ROOT, day: str | None = None, candidate_id: str | None = None, created_at: str | None = None) -> dict[str, Path]:
    report = build_paper_trade_candidate_report(edge_input, candidate_id=candidate_id, created_at=created_at)
    day_value = day or date.today().isoformat()
    out_root = Path(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "paper_trade_candidate_report.json"
    md_path = out_dir / "paper_trade_candidate_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_paper_trade_candidate_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_paper_trade_candidate_summary(report: dict[str, Any]) -> str:
    candidate = report["candidate"]
    certification = report["certification"]
    return "\n".join(
        [
            "# Atlas PaperTradeCandidate Report",
            "",
            f"Candidate: {candidate['candidate_id']}",
            f"Edge score: {candidate['edge_score']}",
            f"Paper trade eligible: {candidate['paper_trade_eligible']}",
            f"Human review required: {candidate['human_review_required']}",
            f"Authority level: {candidate['authority_level']}",
            f"Certification status: {certification['status']}",
            f"Historical replay status: {candidate.get('historical_replay_certification', {}).get('status', 'NONE')}",
            f"Historical replay score: {candidate.get('historical_replay_summary', {}).get('score', 'NONE')}",
            f"Recommendation: {report['recommendation']}",
            f"Qualification reasons: {json.dumps(candidate['qualification_reasons'], sort_keys=True)}",
            f"Disqualification reasons: {json.dumps(candidate['disqualification_reasons'], sort_keys=True)}",
            f"Score explanation: {json.dumps(report['score_explanation'], sort_keys=True)}",
            f"Limitations: {json.dumps(report['limitations'], sort_keys=True)}",
            "",
        ]
    )


def audit_paper_trade_candidate_reports(root: str | Path = PAPER_TRADE_CANDIDATE_ROOT) -> dict[str, Any]:
    root_path = Path(root)
    failures: list[str] = []
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                candidate = payload.get("candidate", payload if isinstance(payload, dict) else {})
                if isinstance(candidate, dict):
                    validate_candidate_governance(candidate)
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"paper_trade_candidate_audit_ok": not failures, "paper_trade_candidate_audit_failures": failures}


def _candidate_confidence(qualification: dict[str, Any]) -> float:
    components = qualification.get("score", {}).get("components", {})
    confidence = (
        0.40 * float(components.get("evidence_maturity", 0.0))
        + 0.30 * float(components.get("lineage_completeness", 0.0))
        + 0.30 * float(components.get("research_effectiveness", 0.0))
    )
    return round(confidence, 6)
