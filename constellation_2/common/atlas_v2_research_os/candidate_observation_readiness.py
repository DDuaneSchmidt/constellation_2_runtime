from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "candidate_observation_readiness"
CLASSIFICATIONS = {"READY_FOR_OBSERVATION", "NEEDS_CLARIFICATION", "DUPLICATE_OR_OVERLAPPING", "REJECT_FOR_NOW"}
DECISIONS = {
    "READY_FOR_OBSERVATION": "APPROVE_FOR_PAPER_FORWARD_OBSERVATION",
    "NEEDS_CLARIFICATION": "REQUEST_CLARIFICATION",
    "DUPLICATE_OR_OVERLAPPING": "MERGE_WITH_RELATED_CANDIDATE",
    "REJECT_FOR_NOW": "REJECT_FOR_NOW",
}


def build_candidate_observation_readiness(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    day_value = day or _today()
    sources = _read_sources(root_path)
    observation = sources["paper_forward_observation"]
    trial = sources["methodology_trial"]
    failures = sources["failures"]
    trial_by_candidate = {str(row.get("paper_trade_candidate_id") or row.get("candidate_id")): row for row in trial.get("eligible_candidates", [])}
    plans = list(observation.get("plans", []))
    mechanism_counts = Counter(str(plan.get("mechanism") or "UNKNOWN") for plan in plans)
    reviews = [_review_plan(plan, trial_by_candidate.get(str(plan.get("candidate_id")), {}), mechanism_counts) for plan in plans]
    counts = Counter(row["classification"] for row in reviews)
    top_ready = sorted([row for row in reviews if row["classification"] == "READY_FOR_OBSERVATION"], key=lambda row: (-row["edge_score"], -row["replay_score"], row["candidate_id"]))[:5]
    missing_by_candidate = {row["candidate_id"]: row["missing_fields"] for row in reviews if row["missing_fields"]}
    return {
        "schema_id": "atlas_v2_research_os_candidate_observation_readiness_v1",
        "schema_version": "v1",
        "day": day_value,
        "created_at": _now(),
        "source_reports": {
            "candidate_review": str(root_path / "candidate_review" / "latest.json"),
            "paper_forward_observation": str(root_path / "paper_forward_observation" / "latest.json"),
            "methodology_trial": str(root_path / "methodology_trial" / "latest.json"),
            "historical_replay": str(root_path / "historical_replay" / "latest.json"),
            "failures": str(root_path / "failures" / "latest.json"),
        },
        "total_candidates_reviewed": len(reviews),
        "ready_for_observation_count": counts.get("READY_FOR_OBSERVATION", 0),
        "needs_clarification_count": counts.get("NEEDS_CLARIFICATION", 0),
        "duplicate_or_overlap_count": counts.get("DUPLICATE_OR_OVERLAPPING", 0),
        "reject_for_now_count": counts.get("REJECT_FOR_NOW", 0),
        "top_5_ready_candidates": top_ready,
        "candidate_specific_missing_fields": missing_by_candidate,
        "common_risks": _common_risks(reviews, failures),
        "recommended_human_decisions": [{"candidate_id": row["candidate_id"], "decision": row["recommended_human_decision"], "reason": row["classification_reason"]} for row in reviews],
        "candidate_reviews": reviews,
        "guardrails": {
            "live_trading_added": False,
            "broker_execution_added": False,
            "capital_authority_added": False,
            "position_sizing_added": False,
            "automatic_paper_trade_placement_added": False,
            "candidate_production_promotion_added": False,
        },
    }


def write_candidate_observation_readiness_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    root_path = Path(root)
    report = build_candidate_observation_readiness(root_path, day=day)
    out_root = root_path / REPORT_DIRNAME
    day_value = str(report["day"])
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "candidate_observation_readiness_report.json"
    summary_path = out_dir / "candidate_observation_readiness_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_candidate_observation_readiness_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_candidate_observation_readiness_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas Candidate Observation Readiness",
        "",
        f"Total candidates reviewed: {report.get('total_candidates_reviewed', 0)}",
        f"Ready for observation: {report.get('ready_for_observation_count', 0)}",
        f"Needs clarification: {report.get('needs_clarification_count', 0)}",
        f"Duplicate/overlap: {report.get('duplicate_or_overlap_count', 0)}",
        f"Reject for now: {report.get('reject_for_now_count', 0)}",
        f"Common risks: {json.dumps(report.get('common_risks', []), sort_keys=True)}",
        "",
        "## Top Ready Candidates",
    ]
    for row in report.get("top_5_ready_candidates", []):
        lines.append(f"- {row['candidate_id']} mechanism={row['mechanism']} edge={row['edge_score']} replay={row['replay_score']} decision={row['recommended_human_decision']}")
    lines.extend(["", "## Human Decisions"])
    for row in report.get("recommended_human_decisions", []):
        lines.append(f"- {row['candidate_id']}: {row['decision']} ({row['reason']})")
    lines.extend([
        "",
        "Authority: readiness review only; no live trading, broker execution, capital authority, position sizing, portfolio construction, production promotion, trade recommendation, or automatic paper placement.",
        "",
    ])
    return "\n".join(lines)


def _review_plan(plan: dict[str, Any], trial_candidate: dict[str, Any], mechanism_counts: Counter[str]) -> dict[str, Any]:
    candidate_id = str(plan.get("candidate_id") or "")
    mechanism = str(plan.get("mechanism") or "UNKNOWN")
    edge_score = float(plan.get("edge_score") or trial_candidate.get("edge_score") or 0.0)
    replay_score = float(plan.get("replay_score") or trial_candidate.get("replay_score") or 0.0)
    regime_constraints = dict(plan.get("regime_constraints") or {})
    primary_regime = str(regime_constraints.get("primary_regime") or "UNKNOWN")
    missing = _missing_fields(plan)
    duplicate_overlap = mechanism_counts[mechanism] > 1
    historical_limitations = _historical_replay_limitations(trial_candidate)
    clarity = {
        "hypothesis_clarity": _clarity(plan.get("hypothesis"), ["hypothesis", mechanism]),
        "entry_observation_condition_clarity": _clarity(plan.get("entry_observation_condition"), ["Record", "setup", "do not place an order"]),
        "exit_observation_condition_clarity": _clarity(plan.get("exit_observation_condition"), ["Close", "observation", "horizon"]),
        "invalidating_condition_clarity": "CLEAR" if len(plan.get("invalidating_conditions") or []) >= 3 else "MISSING_OR_THIN",
    }
    if edge_score < 0.70 or replay_score < 0.70:
        classification = "REJECT_FOR_NOW"
        reason = "edge or replay score is below readiness threshold"
    elif missing and primary_regime == "UNKNOWN":
        classification = "NEEDS_CLARIFICATION"
        reason = "plan is mechanically complete but lacks expected failure modes and has UNKNOWN regime constraints"
    elif missing:
        classification = "NEEDS_CLARIFICATION" if edge_score < 0.74 else "READY_FOR_OBSERVATION"
        reason = "expected failure modes are not explicit; high-score explicit-regime candidates can proceed with reviewer-added notes" if classification == "READY_FOR_OBSERVATION" else "missing expected failure modes"
    elif duplicate_overlap and primary_regime == "UNKNOWN":
        classification = "DUPLICATE_OR_OVERLAPPING"
        reason = "same mechanism appears in multiple candidates without a specific regime separator"
    else:
        classification = "READY_FOR_OBSERVATION"
        reason = "plan is complete enough for human-approved paper-forward observation"
    return {
        "candidate_id": candidate_id,
        "mechanism": mechanism,
        "edge_score": round(edge_score, 6),
        "replay_score": round(replay_score, 6),
        "hypothesis_clarity": clarity["hypothesis_clarity"],
        "entry_observation_condition_clarity": clarity["entry_observation_condition_clarity"],
        "exit_observation_condition_clarity": clarity["exit_observation_condition_clarity"],
        "invalidating_condition_clarity": clarity["invalidating_condition_clarity"],
        "regime_constraints": regime_constraints,
        "minimum_sample_size": int(plan.get("minimum_sample_size") or 0),
        "expected_failure_modes": plan.get("expected_failure_modes", []),
        "duplicate_mechanism_overlap_risk": "HIGH" if duplicate_overlap and primary_regime == "UNKNOWN" else "MEDIUM" if duplicate_overlap else "LOW",
        "historical_replay_limitations": historical_limitations,
        "missing_fields": missing,
        "human_review_readiness": classification,
        "classification": classification,
        "classification_reason": reason,
        "recommended_human_decision": DECISIONS[classification],
    }


def _missing_fields(plan: dict[str, Any]) -> list[str]:
    required = [
        "candidate_id",
        "mechanism",
        "hypothesis",
        "entry_observation_condition",
        "exit_observation_condition",
        "invalidating_conditions",
        "observation_window",
        "minimum_sample_size",
        "success_metrics",
        "failure_metrics",
        "regime_constraints",
        "human_review_required",
    ]
    missing = [field for field in required if not plan.get(field)]
    if not plan.get("expected_failure_modes"):
        missing.append("expected_failure_modes")
    return missing


def _clarity(value: Any, expected_terms: list[str]) -> str:
    text = str(value or "")
    if not text:
        return "MISSING"
    if all(term.lower() in text.lower() for term in expected_terms):
        return "CLEAR"
    return "PARTIAL"


def _historical_replay_limitations(trial_candidate: dict[str, Any]) -> list[str]:
    limitations = []
    sample_size = int(trial_candidate.get("replay_sample_size") or 0)
    if sample_size and sample_size < 30:
        limitations.append(f"historical replay sample size {sample_size} is below the paper-forward plan minimum sample size 30")
    if str(trial_candidate.get("regime") or "UNKNOWN") == "UNKNOWN":
        limitations.append("historical replay was not tied to a named regime")
    limitations.append("positive replay is not approval for trading or automatic paper placement")
    return limitations


def _common_risks(reviews: list[dict[str, Any]], failures: dict[str, Any]) -> list[str]:
    risks = []
    if any("expected_failure_modes" in row.get("missing_fields", []) for row in reviews):
        risks.append("expected failure modes are not explicit in the observation plans")
    if any((row.get("regime_constraints") or {}).get("primary_regime") == "UNKNOWN" for row in reviews):
        risks.append("most candidates use UNKNOWN regime constraints and need reviewer confirmation")
    if any(row.get("duplicate_mechanism_overlap_risk") in {"MEDIUM", "HIGH"} for row in reviews):
        risks.append("mechanism overlap exists across repeated BREAKOUT and MEAN_REVERSION candidates")
    if failures:
        risks.append("latest failure report exists; reviewer should check unresolved Research OS failures before approval")
    risks.append("readiness approval is limited to paper-forward observation planning, not trading authority")
    return risks


def _read_sources(root: Path) -> dict[str, Any]:
    return {
        "candidate_review": _read_json(root / "candidate_review" / "latest.json", {}),
        "paper_forward_observation": _read_json(root / "paper_forward_observation" / "latest.json", {}),
        "methodology_trial": _read_json(root / "methodology_trial" / "latest.json", {}),
        "historical_replay": _read_json(root / "historical_replay" / "latest.json", {}),
        "failures": _read_json(root / "failures" / "latest.json", {}),
    }


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
