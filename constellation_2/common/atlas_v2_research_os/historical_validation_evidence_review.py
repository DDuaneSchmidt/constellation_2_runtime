from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "historical_validation_evidence_review"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"

FINAL_DECISIONS = [
    "CONTINUE_TSLA_ONLY",
    "EXPAND_SIMILAR_SURFACE",
    "MONITOR_ONLY",
    "STOP_RESEARCH_LINE",
]

AUTHORITY_BOUNDARY = (
    "Research-only historical validation evidence review. No trading, broker execution, position sizing, "
    "recommendations, promotion, automatic paper placement, or capital allocation authority."
)

SCORECARD_COLUMNS = [
    "evidence_layer",
    "source_path",
    "source_status",
    "classification",
    "signal",
    "score",
    "decision_impact",
    "notes",
]
FAILURE_COLUMNS = ["failure_mode", "severity", "source_path", "reason", "required_remediation"]
DECISION_COLUMNS = [
    "decision",
    "required_answer",
    "target_surface",
    "research_authority",
    "confidence_impact",
    "rationale",
]


def run_historical_validation_evidence_review(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    report = build_historical_validation_evidence_review(root=root, created_at=created_at)
    write_historical_validation_evidence_review(report, root=root)
    return report


def build_historical_validation_evidence_review(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    evidence = _evidence_state(sources)
    scorecard = _validation_scorecard(sources, evidence)
    failures = _failure_modes(scorecard)
    decision = _final_decision(evidence, failures)
    answer = _required_answer(evidence)
    next_decision = [_decision_row(decision, answer, evidence, failures)]
    summary = {
        "decision": decision,
        "allowed_decisions": FINAL_DECISIONS,
        "required_answer": answer,
        "target_family_id": TARGET_FAMILY_ID,
        "target_surface": _target_surface(),
        "source_inputs_present": sum(1 for source in sources.values() if source["loaded"]),
        "source_inputs_expected": len(sources),
        "failure_mode_count": len(failures),
        "research_only": True,
        "trading_authority": False,
        "broker_execution_authority": False,
        "position_sizing_authority": False,
        "recommendation_authority": False,
        "promotion_authority": False,
        "rationale": next_decision[0]["rationale"],
    }
    return {
        "schema_id": "atlas_v2_research_os_historical_validation_evidence_review",
        "schema_version": "1.0",
        "report_type": "HISTORICAL_VALIDATION_EVIDENCE_REVIEW",
        "build": "169-170",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "evidence_state": evidence,
        "source_inputs": {name: source["path"] for name, source in sources.items()},
        "source_input_status": {
            name: {"exists": source["exists"], "loaded": source["loaded"]}
            for name, source in sources.items()
        },
        "validation_scorecard": scorecard,
        "failure_modes": failures,
        "next_decision": next_decision,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "No consumer may infer strict OOS survival when historical_oos_split_validation is missing.",
            "Missing validation layers must be reported as failure modes, not treated as neutral support.",
            "This review has no trading, broker, sizing, recommendation, promotion, or capital authority.",
        ],
    }


def write_historical_validation_evidence_review(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "scorecard": out_dir / "validation_scorecard.csv",
        "failure_modes": out_dir / "failure_modes.csv",
        "next_decision": out_dir / "next_decision.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_historical_validation_evidence_review_summary(report), encoding="utf-8")
    _write_csv(paths["scorecard"], SCORECARD_COLUMNS, report.get("validation_scorecard") or [])
    _write_csv(paths["failure_modes"], FAILURE_COLUMNS, report.get("failure_modes") or [])
    _write_csv(paths["next_decision"], DECISION_COLUMNS, report.get("next_decision") or [])
    return paths


def render_historical_validation_evidence_review_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 169-170 - Historical Validation Evidence Review",
        "",
        f"Decision: {summary.get('decision')}",
        f"Required answer: {summary.get('required_answer')}",
        f"Target surface: {summary.get('target_surface')}",
        f"Failure modes: {summary.get('failure_mode_count')}",
        "",
        "## Rationale",
        "",
        str(summary.get("rationale") or ""),
        "",
        "## Validation Scorecard",
        "",
    ]
    for row in report.get("validation_scorecard") or []:
        lines.append(
            f"- {row.get('evidence_layer')}: {row.get('classification')} "
            f"({row.get('decision_impact')}) - {row.get('notes')}"
        )
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "historical_oos_split_validation": root / "historical_oos_split_validation" / "latest.json",
        "walk_forward_validation": root / "walk_forward_validation" / "latest.json",
        "temporal_robustness_decay": root / "temporal_robustness_decay" / "latest.json",
        "null_model_randomized_control": root / "null_model_randomized_control" / "latest.json",
        "exact_replay": root / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost": root / "net_of_cost_evidence" / "latest.json",
        "expansion_evidence_review": root / "expansion_evidence_review" / "latest.json",
    }
    return {name: _source(path) for name, path in paths.items()}


def _source(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "loaded": False, "payload": {}}
    try:
        return {"path": str(path), "exists": True, "loaded": True, "payload": json.loads(path.read_text(encoding="utf-8"))}
    except json.JSONDecodeError:
        return {"path": str(path), "exists": True, "loaded": False, "payload": {}}


def _evidence_state(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    oos = sources["historical_oos_split_validation"]["payload"]
    walk = sources["walk_forward_validation"]["payload"]
    decay = sources["temporal_robustness_decay"]["payload"]
    null = sources["null_model_randomized_control"]["payload"]
    exact = sources["exact_replay"]["payload"]
    net = sources["net_of_cost"]["payload"]
    expansion = sources["expansion_evidence_review"]["payload"]
    expansion_summary = expansion.get("summary") or {}
    expansion_evidence = expansion.get("evidence_state") or {}
    return {
        "oos_classification": _classification(oos, "oos_classification", "final_classification", "classification"),
        "oos_degradation_detected": _summary_value(oos, "degradation_detected"),
        "oos_train_net_expectancy": _split_metric(oos, "train", "net_expectancy"),
        "oos_validation_net_expectancy": _split_metric(oos, "validation", "net_expectancy"),
        "oos_observation_net_expectancy": _split_metric(oos, "observation", "net_expectancy"),
        "walk_forward_classification": _classification(walk, "walk_forward_classification", "classification"),
        "temporal_decay_classification": _classification(decay, "decay_classification", "classification"),
        "null_model_classification": _classification(null, "null_model_classification", "classification"),
        "exact_classification": expansion_evidence.get("exact_tsla_classification") or _target_exact_classification(exact),
        "exact_samples": expansion_evidence.get("exact_tsla_samples"),
        "net_classification": expansion_evidence.get("tsla_cost_classification") or _target_net_classification(net),
        "net_expectancy_10bps": expansion_evidence.get("tsla_net_expectancy_10bps"),
        "expansion_decision": expansion_summary.get("decision"),
        "expansion_rationale": expansion_summary.get("rationale"),
    }


def _validation_scorecard(sources: dict[str, dict[str, Any]], evidence: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _score("historical_oos_split_validation", sources["historical_oos_split_validation"], evidence["oos_classification"] or "MISSING", _oos_signal(evidence), evidence.get("oos_observation_net_expectancy"), _impact_for_oos(evidence), _oos_notes(evidence)),
        _score("walk_forward_validation", sources["walk_forward_validation"], evidence["walk_forward_classification"] or "MISSING", "", "", _impact_for_optional(evidence["walk_forward_classification"]), "Walk-forward validation must be source-backed before increasing confidence."),
        _score("temporal_robustness_decay", sources["temporal_robustness_decay"], evidence["temporal_decay_classification"] or "MISSING", "", "", _impact_for_optional(evidence["temporal_decay_classification"]), "Temporal decay evidence must be source-backed before increasing confidence."),
        _score("null_model_randomized_control", sources["null_model_randomized_control"], evidence["null_model_classification"] or "MISSING", "", "", _impact_for_optional(evidence["null_model_classification"]), "Null-model control must reject randomized baseline before increasing confidence."),
        _score("exact_replay", sources["exact_replay"], evidence["exact_classification"] or "UNKNOWN", TARGET_SYMBOL, evidence.get("exact_samples"), "SUPPORTS_TSLA_ONLY" if evidence.get("exact_classification") == "EXACT_CONFIRMED_STRONG" else "FAILURE", "Exact replay supports only the TSLA target surface when strong."),
        _score("net_of_cost", sources["net_of_cost"], evidence["net_classification"] or "UNKNOWN", "10bps", evidence.get("net_expectancy_10bps"), "SUPPORTS_TSLA_ONLY" if evidence.get("net_classification") == "POSSIBLY_VIABLE" else "FAILURE", "Net-of-cost remains required for any continuation decision."),
        _score("expansion_evidence_review", sources["expansion_evidence_review"], evidence["expansion_decision"] or "UNKNOWN", TARGET_SYMBOL, "", "CONSTRAINS_TO_TSLA_ONLY" if evidence.get("expansion_decision") == "CONTINUE_TSLA_ONLY" else "CHECK_EXPANSION_DECISION", evidence.get("expansion_rationale") or "Expansion review must constrain the historical decision."),
    ]


def _failure_modes(scorecard: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failures = []
    for row in scorecard:
        classification = str(row.get("classification") or "")
        impact = str(row.get("decision_impact") or "")
        if classification in {"MISSING", "UNKNOWN"} or impact in {"MISSING_EVIDENCE", "FAILURE"}:
            failures.append(
                {
                    "failure_mode": f"{row.get('evidence_layer')}_{classification}".upper(),
                    "severity": "HIGH" if row.get("evidence_layer") == "historical_oos_split_validation" else "MEDIUM",
                    "source_path": row.get("source_path"),
                    "reason": row.get("notes"),
                    "required_remediation": "Materialize the source report with source-backed classification and rerun Builds 169-170.",
                }
            )
    return failures


def _final_decision(evidence: dict[str, Any], failures: list[dict[str, Any]]) -> str:
    if evidence.get("oos_classification") == "OOS_FAILED":
        return "STOP_RESEARCH_LINE"
    if evidence.get("oos_classification") == "OOS_SURVIVED" and not failures:
        if evidence.get("expansion_decision") == "EXPAND_SIMILAR_SYMBOLS":
            return "EXPAND_SIMILAR_SURFACE"
        return "CONTINUE_TSLA_ONLY"
    return "MONITOR_ONLY"


def _required_answer(evidence: dict[str, Any]) -> str:
    classification = evidence.get("oos_classification")
    if classification == "OOS_SURVIVED":
        return "YES_SOURCE_BACKED_OOS_SURVIVED"
    if classification == "OOS_WEAKENED":
        return "PARTIAL_OOS_WEAKENED"
    if classification == "OOS_FAILED":
        return "NO_OOS_FAILED"
    if classification == "OOS_INSUFFICIENT_SAMPLE":
        return "INSUFFICIENT_SAMPLE"
    return "NOT_PROVEN_HISTORICAL_OOS_SPLIT_VALIDATION_MISSING"


def _decision_row(decision: str, answer: str, evidence: dict[str, Any], failures: list[dict[str, Any]]) -> dict[str, Any]:
    if decision == "MONITOR_ONLY":
        rationale = "Strict historical OOS survival is not source-backed; monitor only until missing validation layers are materialized."
    elif decision == "STOP_RESEARCH_LINE":
        rationale = "Strict historical OOS failed or required exact/net evidence failed."
    elif decision == "EXPAND_SIMILAR_SURFACE":
        rationale = "Historical validation survived and expansion evidence supports similar-surface expansion."
    else:
        rationale = "Historical validation survived, but expansion evidence constrains the line to TSLA-only."
    if failures:
        rationale = f"{rationale} Failure modes: {len(failures)}."
    return {
        "decision": decision,
        "required_answer": answer,
        "target_surface": _target_surface(),
        "research_authority": "RESEARCH_ONLY",
        "confidence_impact": "NONE",
        "rationale": rationale,
    }


def _score(layer: str, source: dict[str, Any], classification: str, signal: Any, score: Any, impact: str, notes: str) -> dict[str, Any]:
    return {
        "evidence_layer": layer,
        "source_path": source["path"],
        "source_status": "LOADED" if source["loaded"] else "MISSING",
        "classification": classification,
        "signal": signal,
        "score": score,
        "decision_impact": impact if source["loaded"] else "MISSING_EVIDENCE",
        "notes": notes,
    }


def _classification(payload: dict[str, Any], *keys: str) -> str:
    summary = payload.get("summary") or {}
    for key in keys:
        value = summary.get(key) if key in summary else payload.get(key)
        if value:
            return str(value)
    return ""


def _summary_value(payload: dict[str, Any], key: str) -> Any:
    return (payload.get("summary") or {}).get(key, payload.get(key))


def _split_metric(payload: dict[str, Any], split: str, metric: str) -> Any:
    for row in payload.get("split_results") or payload.get("splits") or []:
        if str(row.get("split") or row.get("split_name") or "").lower() == split:
            return row.get(metric)
    return None


def _target_exact_classification(exact: dict[str, Any]) -> str:
    for row in exact.get("candidate_results") or []:
        if row.get("family_id") == TARGET_FAMILY_ID and row.get("symbol") == TARGET_SYMBOL and row.get("timeframe") == TARGET_TIMEFRAME:
            return str(row.get("classification") or "")
    return ""


def _target_net_classification(net: dict[str, Any]) -> str:
    for row in net.get("candidate_results") or []:
        if (
            row.get("family_id") == TARGET_FAMILY_ID
            and row.get("cost_scenario") == "10bps"
            and row.get("classification") == "NET_SURVIVES_STRONG"
        ):
            return "POSSIBLY_VIABLE"
    return ""


def _impact_for_oos(evidence: dict[str, Any]) -> str:
    return {
        "OOS_SURVIVED": "SUPPORTS_CONTINUATION",
        "OOS_WEAKENED": "WEAKENS_CONTINUATION",
        "OOS_FAILED": "FAILURE",
        "OOS_INSUFFICIENT_SAMPLE": "MISSING_EVIDENCE",
    }.get(str(evidence.get("oos_classification") or ""), "MISSING_EVIDENCE")


def _impact_for_optional(classification: str) -> str:
    if not classification:
        return "MISSING_EVIDENCE"
    if "FAILED" in classification:
        return "FAILURE"
    return "CONTEXT"


def _oos_signal(evidence: dict[str, Any]) -> str:
    return f"train={evidence.get('oos_train_net_expectancy')} validation={evidence.get('oos_validation_net_expectancy')} observation={evidence.get('oos_observation_net_expectancy')}"


def _oos_notes(evidence: dict[str, Any]) -> str:
    if not evidence.get("oos_classification"):
        return "Strict historical OOS split validation is missing; survival cannot be claimed."
    return f"degradation_detected={evidence.get('oos_degradation_detected')}"


def _target_surface() -> str:
    return f"{TARGET_SYMBOL} / {TARGET_TIMEFRAME} / {TARGET_MECHANISM} / {TARGET_REGIME} / {TARGET_FAMILY_ID}"


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
