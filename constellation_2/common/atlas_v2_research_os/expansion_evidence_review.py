from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "expansion_evidence_review"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"

ALLOWED_DECISIONS = [
    "CONTINUE_TSLA_ONLY",
    "EXPAND_SIMILAR_SYMBOLS",
    "EXPAND_REVERSAL_VARIANTS",
    "EXPAND_TIMEFRAMES",
    "EXPAND_COMBINED_NEIGHBORHOOD",
    "STOP_RESEARCH_LINE",
]

AUTHORITY_BOUNDARY = (
    "Research-only expansion evidence review. No trading, broker execution, position sizing, "
    "recommendations, promotion, automatic paper placement, or capital allocation authority."
)

SCORECARD_COLUMNS = [
    "surface",
    "source_path",
    "source_status",
    "evidence_status",
    "classification",
    "decision",
    "score",
    "rationale",
]
APPROVED_COLUMNS = ["surface_id", "decision", "symbol", "timeframe", "mechanism", "regime", "scope", "guardrail"]
REJECTED_COLUMNS = ["surface_id", "decision", "surface", "reason", "source_path", "unlock_requirement"]
DECISION_COLUMNS = ["decision", "confidence_impact", "research_authority", "approved_surface_count", "rejected_surface_count", "rationale"]


def run_expansion_evidence_review(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_expansion_evidence_review(root=root, created_at=created_at)
    write_expansion_evidence_review(report, root=root)
    return report


def build_expansion_evidence_review(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    evidence = _evidence_state(sources)
    scorecard = _scorecard(evidence, sources)
    decision = _decision(evidence)
    approved = _approved_surfaces(decision)
    rejected = _rejected_surfaces(decision, scorecard)
    next_phase = _next_phase(decision, approved, rejected, evidence)
    summary = {
        "decision": decision,
        "allowed_decisions": ALLOWED_DECISIONS,
        "confidence_impact": evidence.get("final_confidence_impact") or "NONE",
        "target_family_id": TARGET_FAMILY_ID,
        "target_surface": f"{TARGET_SYMBOL} / {TARGET_TIMEFRAME} / {TARGET_MECHANISM} / {TARGET_REGIME}",
        "approved_surface_count": len(approved),
        "rejected_surface_count": len(rejected),
        "source_inputs_present": sum(1 for source in sources.values() if source["loaded"]),
        "source_inputs_expected": len(sources),
        "research_only": True,
        "trading_authority": False,
        "promotion_authority": False,
        "rationale": _decision_rationale(decision, evidence),
    }
    return {
        "schema_id": "atlas_v2_research_os_expansion_evidence_review",
        "schema_version": "1.0",
        "report_type": "EXPANSION_EVIDENCE_REVIEW",
        "build": "159-160",
        "created_at": created,
        "day": created[:10],
        "allowed_decisions": ALLOWED_DECISIONS,
        "summary": summary,
        "evidence_state": evidence,
        "source_inputs": {name: source["path"] for name, source in sources.items()},
        "source_input_status": {name: {"exists": source["exists"], "loaded": source["loaded"]} for name, source in sources.items()},
        "expansion_scorecard": scorecard,
        "approved_next_surface": approved,
        "rejected_surfaces": rejected,
        "next_phase_decision": next_phase,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "Consumers must use source-backed report fields; do not infer missing neighborhood truth.",
            "Missing neighborhood evidence is rejected until a materialized report exists.",
            "Approved rows are research surfaces only, not candidates, trades, recommendations, or promotions.",
        ],
    }


def write_expansion_evidence_review(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "scorecard": out_dir / "expansion_scorecard.csv",
        "approved": out_dir / "approved_next_surface.csv",
        "rejected": out_dir / "rejected_surfaces.csv",
        "next_phase": out_dir / "next_phase_decision.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_expansion_evidence_review_summary(report), encoding="utf-8")
    _write_csv(paths["scorecard"], SCORECARD_COLUMNS, report.get("expansion_scorecard") or [])
    _write_csv(paths["approved"], APPROVED_COLUMNS, report.get("approved_next_surface") or [])
    _write_csv(paths["rejected"], REJECTED_COLUMNS, report.get("rejected_surfaces") or [])
    _write_csv(paths["next_phase"], DECISION_COLUMNS, report.get("next_phase_decision") or [])
    return paths


def render_expansion_evidence_review_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 159-160 - Expansion Evidence Review",
        "",
        f"Decision: {summary.get('decision')}",
        f"Target surface: {summary.get('target_surface')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        f"Approved surfaces: {summary.get('approved_surface_count')}",
        f"Rejected surfaces: {summary.get('rejected_surface_count')}",
        "",
        "## Rationale",
        "",
        str(summary.get("rationale") or ""),
        "",
        "## Scorecard",
        "",
    ]
    for row in report.get("expansion_scorecard") or []:
        lines.append(f"- {row.get('surface')}: {row.get('decision')} ({row.get('classification')}) - {row.get('rationale')}")
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "controlled_similar_symbol_expansion": root / "controlled_similar_symbol_expansion" / "latest.json",
        "reversal_neighborhood_expansion": root / "reversal_neighborhood_expansion" / "latest.json",
        "timeframe_neighborhood_expansion": root / "timeframe_neighborhood_expansion" / "latest.json",
        "regime_boundary_expansion": root / "regime_boundary_expansion" / "latest.json",
        "exact_replay": root / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost": root / "net_of_cost_evidence" / "latest.json",
        "final_verdict": root / "final_research_verdict" / "latest.json",
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
    similar = sources["controlled_similar_symbol_expansion"]["payload"]
    reversal = sources["reversal_neighborhood_expansion"]["payload"]
    timeframe = sources["timeframe_neighborhood_expansion"]["payload"]
    regime = sources["regime_boundary_expansion"]["payload"]
    exact = sources["exact_replay"]["payload"]
    net = sources["net_of_cost"]["payload"]
    final = sources["final_verdict"]["payload"]
    final_summary = final.get("summary") or {}
    final_evidence = final.get("evidence_state") or {}
    similar_summary = similar.get("summary") or {}
    similar_classifications = similar.get("classifications") or {}
    non_tsla_confirmed = [
        row.get("symbol")
        for row in similar.get("similar_symbol_comparison") or []
        if row.get("symbol") != TARGET_SYMBOL and row.get("classification") == "SIMILAR_SYMBOL_CONFIRMED"
    ]
    return {
        "final_verdict": final_summary.get("final_verdict"),
        "final_confidence_impact": final_summary.get("confidence_impact"),
        "exact_tsla_classification": final_evidence.get("exact_classification") or _target_exact_classification(exact),
        "exact_tsla_samples": final_evidence.get("exact_tsla_samples"),
        "tsla_cost_classification": final_evidence.get("tsla_cost_classification"),
        "tsla_net_expectancy_10bps": final_evidence.get("tsla_net_expectancy_10bps"),
        "generalization_classification": final_evidence.get("generalization_classification"),
        "edge_likely_exploitable": final_summary.get("edge_likely_exploitable"),
        "similar_expansion_conclusion": similar_summary.get("expansion_conclusion"),
        "similar_symbols_requested": similar_summary.get("symbols_requested", 0),
        "similar_symbols_confirmed": similar_summary.get("symbols_confirmed", 0),
        "non_tsla_similar_symbols_confirmed": non_tsla_confirmed,
        "similar_symbols_weak": similar_summary.get("symbols_weak", 0),
        "similar_symbols_failed": similar_summary.get("symbols_failed", 0),
        "similar_symbols_blocked": similar_summary.get("symbols_blocked", 0),
        "similar_classifications": similar_classifications,
        "reversal_loaded": bool(reversal),
        "reversal_top_classification": (reversal.get("summary") or {}).get("top_classification"),
        "timeframe_loaded": bool(timeframe),
        "timeframe_top_classification": (timeframe.get("summary") or {}).get("key_question_answer") or (timeframe.get("summary") or {}).get("top_classification"),
        "timeframe_classification_counts": (timeframe.get("summary") or {}).get("classification_counts") or {},
        "regime_boundary_loaded": bool(regime),
        "regime_boundary_classification": (regime.get("summary") or {}).get("overall_classification") or regime.get("overall_classification") or (regime.get("summary") or {}).get("classification"),
        "net_conservative_surviving_families": (net.get("summary") or {}).get("net_surviving_families"),
    }


def _scorecard(evidence: dict[str, Any], sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        _score(
            "similar_symbols",
            sources["controlled_similar_symbol_expansion"],
            "PRESENT" if sources["controlled_similar_symbol_expansion"]["loaded"] else "MISSING",
            "NO_NON_TSLA_CONFIRMED" if not evidence["non_tsla_similar_symbols_confirmed"] else "NON_TSLA_CONFIRMED",
            "REJECT_EXPAND_SIMILAR_SYMBOLS" if not evidence["non_tsla_similar_symbols_confirmed"] else "APPROVE_EXPAND_SIMILAR_SYMBOLS",
            evidence.get("similar_symbols_confirmed"),
            evidence.get("similar_expansion_conclusion") or "No source-backed similar-symbol conclusion.",
        ),
        _score(
            "reversal_neighborhood",
            sources["reversal_neighborhood_expansion"],
            "PRESENT" if sources["reversal_neighborhood_expansion"]["loaded"] else "MISSING",
            evidence.get("reversal_top_classification") or "MISSING_REPORT",
            "REJECT_EXPAND_REVERSAL_VARIANTS",
            "",
            "No materialized Build 153-154 reversal-neighborhood report is present." if not evidence.get("reversal_loaded") else "Loaded reversal variants are fragile or failed; no approval rule was met.",
        ),
        _score(
            "timeframe_neighborhood",
            sources["timeframe_neighborhood_expansion"],
            "PRESENT" if sources["timeframe_neighborhood_expansion"]["loaded"] else "MISSING",
            evidence.get("timeframe_top_classification") or "MISSING_REPORT",
            "REJECT_EXPAND_TIMEFRAMES",
            "",
            "Loaded timeframe report says 30m is special; timeframe neighborhood does not support expansion.",
        ),
        _score(
            "regime_boundary",
            sources["regime_boundary_expansion"],
            "PRESENT" if sources["regime_boundary_expansion"]["loaded"] else "MISSING",
            evidence.get("regime_boundary_classification") or "MISSING_REPORT",
            "REJECT_EXPAND_REGIME_BOUNDARY",
            "",
            "Loaded regime-boundary report is weak and does not justify neighborhood expansion.",
        ),
        _score(
            "exact_replay",
            sources["exact_replay"],
            "PRESENT" if sources["exact_replay"]["loaded"] else "MISSING",
            evidence.get("exact_tsla_classification") or "UNKNOWN",
            "SUPPORT_TSLA_ONLY" if evidence.get("exact_tsla_classification") == "EXACT_CONFIRMED_STRONG" else "REJECT_LINE",
            evidence.get("exact_tsla_samples"),
            "Exact replay supports the TSLA 30m target surface only.",
        ),
        _score(
            "net_of_cost",
            sources["net_of_cost"],
            "PRESENT" if sources["net_of_cost"]["loaded"] else "MISSING",
            evidence.get("tsla_cost_classification") or "UNKNOWN",
            "SUPPORT_TSLA_ONLY" if evidence.get("tsla_cost_classification") == "POSSIBLY_VIABLE" else "REJECT_LINE",
            evidence.get("tsla_net_expectancy_10bps"),
            "TSLA remains positive at 10 bps but is not broad robustness evidence.",
        ),
        _score(
            "final_verdict",
            sources["final_verdict"],
            "PRESENT" if sources["final_verdict"]["loaded"] else "MISSING",
            evidence.get("final_verdict") or "UNKNOWN",
            "SUPPORT_TSLA_ONLY" if evidence.get("final_verdict") == "CONTINUE_TSLA_ONLY" else "FOLLOW_FINAL_VERDICT",
            evidence.get("final_confidence_impact"),
            "Final verdict constrains the next phase decision vocabulary.",
        ),
    ]
    return rows


def _decision(evidence: dict[str, Any]) -> str:
    if evidence.get("final_verdict") == "RESEARCH_NOT_JUSTIFIED":
        return "STOP_RESEARCH_LINE"
    if evidence.get("non_tsla_similar_symbols_confirmed"):
        return "EXPAND_SIMILAR_SYMBOLS"
    if evidence.get("final_verdict") == "CONTINUE_TSLA_ONLY":
        return "CONTINUE_TSLA_ONLY"
    return "STOP_RESEARCH_LINE"


def _approved_surfaces(decision: str) -> list[dict[str, Any]]:
    if decision != "CONTINUE_TSLA_ONLY":
        return []
    return [
        {
            "surface_id": "approved_001",
            "decision": decision,
            "symbol": TARGET_SYMBOL,
            "timeframe": TARGET_TIMEFRAME,
            "mechanism": TARGET_MECHANISM,
            "regime": TARGET_REGIME,
            "scope": "Continue exact replay, forward observation, and net-of-cost review on the existing TSLA-only surface.",
            "guardrail": "No similar symbols, reversal variants, timeframe variants, regime boundary variants, combined neighborhoods, promotion, or trading authority.",
        }
    ]


def _rejected_surfaces(decision: str, scorecard: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rejected = []
    for row in scorecard:
        row_decision = str(row.get("decision") or "")
        if row_decision.startswith("REJECT_"):
            rejected.append(
                {
                    "surface_id": f"rejected_{len(rejected) + 1:03d}",
                    "decision": decision,
                    "surface": row.get("surface"),
                    "reason": row.get("rationale"),
                    "source_path": row.get("source_path"),
                    "unlock_requirement": "Requires source-backed exact replay, net-of-cost, and final verdict evidence for this surface.",
                }
            )
    if decision == "CONTINUE_TSLA_ONLY":
        rejected.append(
            {
                "surface_id": f"rejected_{len(rejected) + 1:03d}",
                "decision": decision,
                "surface": "combined_neighborhood",
                "reason": "Combined expansion is not allowed while component surfaces are weak, failed, blocked, or missing.",
                "source_path": "component scorecard",
                "unlock_requirement": "All component neighborhoods must have materialized positive source-backed evidence.",
            }
        )
    return rejected


def _next_phase(decision: str, approved: list[dict[str, Any]], rejected: list[dict[str, Any]], evidence: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "decision": decision,
            "confidence_impact": evidence.get("final_confidence_impact") or "NONE",
            "research_authority": "RESEARCH_ONLY",
            "approved_surface_count": len(approved),
            "rejected_surface_count": len(rejected),
            "rationale": _decision_rationale(decision, evidence),
        }
    ]


def _decision_rationale(decision: str, evidence: dict[str, Any]) -> str:
    if decision == "CONTINUE_TSLA_ONLY":
        return (
            "Final verdict is CONTINUE_TSLA_ONLY; exact replay confirms TSLA, net-of-cost is only possibly viable, "
            "similar-symbol expansion has no non-TSLA confirmed symbol, reversal variants are fragile, timeframe evidence says 30m is special, and regime-boundary evidence is weak."
        )
    if decision == "EXPAND_SIMILAR_SYMBOLS":
        return f"Non-TSLA similar symbols confirmed: {', '.join(evidence.get('non_tsla_similar_symbols_confirmed') or [])}."
    return "Required source-backed expansion evidence is absent or negative."


def _score(surface: str, source: dict[str, Any], evidence_status: str, classification: str, decision: str, score: Any, rationale: str) -> dict[str, Any]:
    return {
        "surface": surface,
        "source_path": source["path"],
        "source_status": "LOADED" if source["loaded"] else "MISSING",
        "evidence_status": evidence_status,
        "classification": classification,
        "decision": decision,
        "score": score,
        "rationale": rationale,
    }


def _target_exact_classification(exact: dict[str, Any]) -> str:
    for row in exact.get("candidate_results") or []:
        if row.get("family_id") == TARGET_FAMILY_ID and row.get("symbol") == TARGET_SYMBOL and row.get("timeframe") == TARGET_TIMEFRAME:
            return str(row.get("classification") or "")
    return ""


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
