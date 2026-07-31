from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .candidate_quality_baseline import QUALITY_ROOT, load_candidate_quality_baseline
from .candidate_quality_evaluation import evaluate_candidate_quality
from .candidate_quality_governance import validate_candidate_quality_measurement_allowed
from .candidate_quality_treatment import load_candidate_quality_treatment


def build_candidate_quality_report(baseline: dict[str, Any], treatment: dict[str, Any], *, evaluation_id: str = "candidate-quality-evaluation-demo", created_by: str = "atlas_research_os") -> dict[str, Any]:
    evaluation = evaluate_candidate_quality(baseline, treatment, evaluation_id=evaluation_id, created_by=created_by)
    recommendation = _recommendation(evaluation)
    validate_candidate_quality_measurement_allowed({"recommendation": recommendation, "metadata": {"measurement_only": True}})
    return {
        "schema_id": "atlas_candidate_quality_evaluation_v1",
        "schema_version": "v1",
        "evaluation_id": evaluation["evaluation_id"],
        "baseline_summary": _summary(baseline),
        "treatment_summary": _summary(treatment),
        "metric_values": evaluation["metric_set"],
        "metric_deltas": evaluation["delta"],
        "failure_category_deltas": evaluation["delta"].get("failure_category_distribution_delta", {}),
        "comparability_status": "COMPARABLE" if evaluation["comparable"] else "NON_COMPARABLE",
        "evidence_maturity_notes": _evidence_notes(evaluation),
        "governance_audit_result": evaluation["governance_status"],
        "certification_status": evaluation["certification_result"]["status"],
        "certification_result": evaluation["certification_result"],
        "limitations": evaluation["limitations"],
        "recommendation": recommendation,
        "evaluation": evaluation,
    }


def write_candidate_quality_report(*, baseline_id: str, treatment_id: str, root: str | Path = QUALITY_ROOT, day: str | None = None, evaluation_id: str = "candidate-quality-evaluation-demo") -> dict[str, Path]:
    report = build_candidate_quality_report(load_candidate_quality_baseline(baseline_id, root), load_candidate_quality_treatment(treatment_id, root), evaluation_id=evaluation_id)
    day_value = day or date.today().isoformat()
    out_root = Path(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "candidate_quality_evaluation.v1.json"
    md_path = out_dir / "candidate_quality_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_candidate_quality_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    eval_dir = out_root / "evaluations"
    eval_dir.mkdir(parents=True, exist_ok=True)
    (eval_dir / f"{evaluation_id}.json").write_text(payload, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md, "evaluation": eval_dir / f"{evaluation_id}.json"}


def render_candidate_quality_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas Candidate Quality Measurement Report",
        "",
        f"Evaluation: {report['evaluation_id']}",
        f"Comparability: {report['comparability_status']}",
        f"Governance audit: {report['governance_audit_result']}",
        f"Certification status: {report['certification_status']}",
        f"Recommendation: {report['recommendation']}",
        f"Baseline: {json.dumps(report['baseline_summary'], sort_keys=True)}",
        f"Treatment: {json.dumps(report['treatment_summary'], sort_keys=True)}",
        f"Deltas: {json.dumps(report['metric_deltas'], sort_keys=True)}",
        f"Failure category deltas: {json.dumps(report['failure_category_deltas'], sort_keys=True)}",
        f"Evidence maturity notes: {json.dumps(report['evidence_maturity_notes'], sort_keys=True)}",
        f"Limitations: {json.dumps(report['limitations'], sort_keys=True)}",
        "",
    ])


def audit_candidate_quality_reports(root: str | Path = QUALITY_ROOT) -> dict[str, Any]:
    root_path = Path(root)
    failures: list[str] = []
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                validate_candidate_quality_measurement_allowed(payload if isinstance(payload, dict) else {"rows": payload})
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"candidate_quality_audit_ok": not failures, "candidate_quality_audit_failures": failures}


def _summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "raw_signals": row.get("raw_signals"),
        "generated_candidates": row.get("generated_candidates"),
        "rejected_candidates": row.get("rejected_candidates"),
        "repeated_failures": row.get("repeated_failures"),
        "measurement_window": row.get("measurement_window"),
        "signal_universe_id": row.get("signal_universe_id"),
        "candidate_factory_version": row.get("candidate_factory_version"),
    }


def _evidence_notes(evaluation: dict[str, Any]) -> list[str]:
    notes = []
    for arm in ["baseline", "treatment"]:
        metric = evaluation.get("metric_set", {}).get(arm, {}).get("evidence_maturity_score", {})
        notes.extend(metric.get("notes", []))
    return notes


def _recommendation(evaluation: dict[str, Any]) -> str:
    if not evaluation["comparable"]:
        return "Treatment is not comparable to baseline."
    if evaluation["certification_result"]["status"] == "INSUFFICIENT_DATA":
        return "Collect more paper-forward observations."
    if evaluation["regression_detected"]:
        return "Investigate repeated failure categories."
    return "Continue measurement."
