from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "final_research_verdict"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"
FINAL_VERDICTS = [
    "CONTINUE_TSLA_ONLY",
    "EXPAND_TO_SIMILAR_SYMBOLS",
    "EXPAND_TO_REVERSAL_NEIGHBORHOOD",
    "RESEARCH_NOT_JUSTIFIED",
    "INSUFFICIENT_EVIDENCE",
]
CONFIDENCE_IMPACTS = ["NONE", "SMALL_INCREASE", "DECREASE"]
AUTHORITY_BOUNDARY = (
    "Research-only final verdict. No promotion authority, no trading authority, no broker execution, "
    "no capital allocation, no position sizing, no automatic paper placement, and no trade recommendations."
)
SCORECARD_COLUMNS = [
    "question_id",
    "question",
    "answer",
    "classification",
    "evidence_layers",
    "confidence_impact",
    "notes",
]
EVIDENCE_COLUMNS = [
    "evidence_layer",
    "source_path",
    "status",
    "classification",
    "signal",
    "score",
    "notes",
]
NEXT_PHASE_COLUMNS = [
    "phase",
    "action",
    "scope",
    "priority",
    "authority_boundary",
    "exit_condition",
]


def run_final_research_verdict(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_final_research_verdict(root=root, created_at=created_at)
    write_final_research_verdict(report, root=root)
    return report


def build_final_research_verdict(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    evidence = _evidence_state(sources)
    verdict = _final_verdict(evidence)
    confidence_impact = _confidence_impact(verdict, evidence)
    scorecard = _verdict_scorecard(verdict, confidence_impact, evidence)
    evidence_chain = _evidence_chain(evidence)
    next_phase = _recommended_next_phase(verdict, evidence)
    summary = {
        "final_verdict": verdict,
        "confidence_impact": confidence_impact,
        "target_family_id": TARGET_FAMILY_ID,
        "target_symbol": TARGET_SYMBOL,
        "target_timeframe": TARGET_TIMEFRAME,
        "target_mechanism": TARGET_MECHANISM,
        "target_regime": TARGET_REGIME,
        "signal_likely_exists": scorecard[0]["answer"],
        "edge_economically_meaningful": scorecard[1]["answer"],
        "edge_robust": scorecard[2]["answer"],
        "edge_likely_exploitable": scorecard[3]["answer"],
        "research_continue": scorecard[4]["answer"],
        "remaining_blockers": _remaining_blockers(evidence),
        "no_promotion_authority": True,
        "no_trading_authority": True,
    }
    return {
        "schema_id": "atlas_v2_research_os_final_research_verdict",
        "schema_version": "1.0",
        "report_type": "FINAL_RESEARCH_VERDICT",
        "build": "147-150",
        "created_at": created,
        "day": created[:10],
        "allowed_final_verdicts": FINAL_VERDICTS,
        "allowed_confidence_impacts": CONFIDENCE_IMPACTS,
        "summary": summary,
        "evidence_state": evidence,
        "source_inputs": {name: source["path"] for name, source in sources.items()},
        "source_input_status": {
            name: {"exists": source["exists"], "loaded": source["loaded"]}
            for name, source in sources.items()
        },
        "verdict_scorecard": scorecard,
        "evidence_chain": evidence_chain,
        "recommended_next_phase": next_phase,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "Do not promote this verdict into candidate approval.",
            "Do not infer execution authority from research evidence.",
            "Do not broaden beyond TSLA / 30m / REVERSAL / TRENDING until holdout and forward evidence mature.",
            "Treat forward observation and holdout blockers as confidence-neutral until measured source-backed outcomes exist.",
        ],
    }


def write_final_research_verdict(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "verdict_scorecard": out_dir / "verdict_scorecard.csv",
        "evidence_chain": out_dir / "evidence_chain.csv",
        "recommended_next_phase": out_dir / "recommended_next_phase.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_final_research_verdict_summary(report), encoding="utf-8")
    _write_csv(paths["verdict_scorecard"], SCORECARD_COLUMNS, report.get("verdict_scorecard") or [])
    _write_csv(paths["evidence_chain"], EVIDENCE_COLUMNS, report.get("evidence_chain") or [])
    _write_csv(paths["recommended_next_phase"], NEXT_PHASE_COLUMNS, report.get("recommended_next_phase") or [])
    return paths


def render_final_research_verdict_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    evidence = report.get("evidence_state") or {}
    lines = [
        "# Builds 147-150 - Final Research Verdict",
        "",
        f"Final verdict: {summary.get('final_verdict')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        f"Target: {summary.get('target_symbol')} {summary.get('target_timeframe')} {summary.get('target_mechanism')}/{summary.get('target_regime')}",
        f"Family: {summary.get('target_family_id')}",
        "",
        "## Required Questions",
        "",
    ]
    for row in report.get("verdict_scorecard") or []:
        lines.append(f"- {row.get('question')}: {row.get('answer')} ({row.get('classification')})")
    lines.extend(
        [
            "",
            "## Evidence",
            "",
            f"Exact replay: {evidence.get('exact_classification')} samples={evidence.get('exact_tsla_samples')} expectancy={evidence.get('exact_tsla_expectancy')} blocked={evidence.get('exact_blocked')}",
            f"Net-of-cost: {evidence.get('tsla_cost_classification')} net_expectancy_10bps={evidence.get('tsla_net_expectancy_10bps')} break_even_cost_bps={evidence.get('tsla_break_even_cost_bps')}",
            f"Generalization: {evidence.get('generalization_classification')} surviving_symbol={evidence.get('surviving_symbol')} symbol_classification={evidence.get('surviving_symbol_classification')}",
            f"Execution realism: {evidence.get('execution_overall')} / {evidence.get('execution_cost_overall')} TSLA={evidence.get('tsla_execution_classification')}",
            f"Forward observation: {evidence.get('forward_classification')} measured={evidence.get('forward_measured_outcomes')} pending={evidence.get('forward_pending_observations')}",
            f"Holdout: tested={evidence.get('holdout_families_tested')} confidence={evidence.get('holdout_confidence_impact')} remaining_readiness_blockers={evidence.get('holdout_remaining_blockers')}",
            f"Decision review: {evidence.get('decision_review_decision')} / {evidence.get('expansion_gate_decision')}",
            "",
            "## Remaining Blockers",
            "",
        ]
    )
    lines.extend(_bullet_list(summary.get("remaining_blockers") or []))
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _final_verdict(evidence: dict[str, Any]) -> str:
    if not evidence.get("has_minimum_evidence"):
        return "INSUFFICIENT_EVIDENCE"
    if evidence.get("exact_classification") != "EXACT_CONFIRMED_STRONG":
        return "RESEARCH_NOT_JUSTIFIED"
    if evidence.get("tsla_cost_classification") != "POSSIBLY_VIABLE":
        return "RESEARCH_NOT_JUSTIFIED"
    if evidence.get("surviving_symbol") == TARGET_SYMBOL and evidence.get("surviving_symbol_classification") == "SYMBOL_STRONG":
        if evidence.get("generalization_classification") == "FRAGILE" or evidence.get("expansion_gate_decision") == "EXPAND_NARROW_TSLA_ONLY":
            return "CONTINUE_TSLA_ONLY"
        return "EXPAND_TO_SIMILAR_SYMBOLS"
    return "INSUFFICIENT_EVIDENCE"


def _confidence_impact(verdict: str, evidence: dict[str, Any]) -> str:
    if verdict == "RESEARCH_NOT_JUSTIFIED":
        return "DECREASE"
    if verdict == "INSUFFICIENT_EVIDENCE":
        return "NONE"
    if evidence.get("forward_classification") in {"FORWARD_POSITIVE", "FORWARD_STRONG"} and evidence.get("holdout_survived", 0):
        return "SMALL_INCREASE"
    return "NONE"


def _verdict_scorecard(verdict: str, confidence_impact: str, evidence: dict[str, Any]) -> list[dict[str, str]]:
    if verdict == "INSUFFICIENT_EVIDENCE":
        return [
            _score("Q1", "Does a signal likely exist?", "INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE", "exact_replay", confidence_impact, "Minimum required evidence layers are missing."),
            _score("Q2", "Is the edge economically meaningful?", "INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE", "net_of_cost,execution", confidence_impact, "Minimum required evidence layers are missing."),
            _score("Q3", "Is the edge robust?", "INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE", "generalization,holdout", confidence_impact, "Minimum required evidence layers are missing."),
            _score("Q4", "Is the edge likely exploitable?", "INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE", "execution,forward_observation", confidence_impact, "Minimum required evidence layers are missing."),
            _score("Q5", "Should research continue?", "NO", "INSUFFICIENT_EVIDENCE", "decision_review", confidence_impact, "Do not continue without the required evidence chain."),
        ]
    signal_answer = "YES" if evidence.get("exact_classification") == "EXACT_CONFIRMED_STRONG" else "NO"
    economic_answer = "PARTIAL_TSLA_ONLY" if evidence.get("tsla_cost_classification") == "POSSIBLY_VIABLE" else "NO"
    robust_answer = "PARTIAL_NOT_BROADLY_ROBUST" if evidence.get("generalization_classification") == "FRAGILE" else "YES"
    exploitable_answer = "NOT_YET_PROVEN" if evidence.get("forward_classification") == "FORWARD_NOT_STARTED" or not evidence.get("holdout_families_tested") else "PARTIAL"
    continue_answer = "YES_TSLA_ONLY" if verdict == "CONTINUE_TSLA_ONLY" else "YES_EXPAND" if verdict.startswith("EXPAND") else "NO"
    return [
        _score("Q1", "Does a signal likely exist?", signal_answer, evidence.get("exact_classification") or "UNKNOWN", "exact_replay", confidence_impact, "TSLA 30m exact replay is source-backed and unblocked."),
        _score("Q2", "Is the edge economically meaningful?", economic_answer, evidence.get("tsla_cost_classification") or "UNKNOWN", "net_of_cost,execution_cost_failure_analysis", confidence_impact, "TSLA survives 10 bps but remains cost-sensitive and only possibly viable."),
        _score("Q3", "Is the edge robust?", robust_answer, evidence.get("generalization_classification") or "UNKNOWN", "generalization,controlled_surface_expansion_gate", confidence_impact, "Generalization is fragile; only TSLA is symbol-strong."),
        _score("Q4", "Is the edge likely exploitable?", exploitable_answer, evidence.get("execution_overall") or "UNKNOWN", "execution_realism,forward_observation,holdout", confidence_impact, "Execution is viable but fragile; forward and holdout proof are not mature."),
        _score("Q5", "Should research continue?", continue_answer, verdict, "research_decision_review,controlled_surface_expansion_gate", confidence_impact, "Continue only inside the TSLA 30m research surface with no promotion or trading authority."),
    ]


def _evidence_chain(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "evidence_layer": "exact_replay",
            "source_path": "exact_replay_without_fallback/latest.json + exact_replay_results.csv",
            "status": "PRESENT" if evidence.get("exact_loaded") else "MISSING",
            "classification": evidence.get("exact_classification") or "UNKNOWN",
            "signal": f"{TARGET_SYMBOL}_{TARGET_TIMEFRAME}",
            "score": evidence.get("exact_tsla_expectancy"),
            "notes": f"exact_blocked={evidence.get('exact_blocked')} samples={evidence.get('exact_tsla_samples')} profit_factor={evidence.get('exact_tsla_profit_factor')}",
        },
        {
            "evidence_layer": "net_of_cost",
            "source_path": "net_of_cost_evidence/latest.json + execution_cost_failure_analysis/tsla_execution_viability.csv",
            "status": "PRESENT" if evidence.get("net_loaded") else "MISSING",
            "classification": evidence.get("tsla_cost_classification") or "UNKNOWN",
            "signal": "10bps_cost_adjusted",
            "score": evidence.get("tsla_net_expectancy_10bps"),
            "notes": f"break_even_cost_bps={evidence.get('tsla_break_even_cost_bps')} net_profit_factor_10bps={evidence.get('tsla_net_profit_factor_10bps')}",
        },
        {
            "evidence_layer": "generalization",
            "source_path": "generalization_edge_magnitude_assessment/latest.json + cross_symbol_generalization.csv",
            "status": "PRESENT" if evidence.get("generalization_loaded") else "MISSING",
            "classification": evidence.get("generalization_classification") or "UNKNOWN",
            "signal": evidence.get("surviving_symbol"),
            "score": evidence.get("surviving_symbol_net_expectancy"),
            "notes": f"surviving_symbol_count={evidence.get('surviving_symbol_count')} symbol_classification={evidence.get('surviving_symbol_classification')}",
        },
        {
            "evidence_layer": "execution_realism",
            "source_path": "execution_realism_economic_viability/latest.json + execution_cost_failure_analysis/latest.json",
            "status": "PRESENT" if evidence.get("execution_loaded") else "MISSING",
            "classification": evidence.get("execution_overall") or "UNKNOWN",
            "signal": evidence.get("execution_cost_overall"),
            "score": evidence.get("tsla_surviving_realistic_rows"),
            "notes": f"liquidity={evidence.get('liquidity_classification')} slippage={evidence.get('slippage_classification')} tsla={evidence.get('tsla_execution_classification')}",
        },
        {
            "evidence_layer": "forward_observation",
            "source_path": "forward_observation_loop/latest.json",
            "status": "PRESENT" if evidence.get("forward_loaded") else "MISSING",
            "classification": evidence.get("forward_classification") or "UNKNOWN",
            "signal": "prospective_observation",
            "score": evidence.get("forward_measured_outcomes"),
            "notes": f"pending={evidence.get('forward_pending_observations')} new_signals={evidence.get('forward_new_signals')}",
        },
        {
            "evidence_layer": "holdout",
            "source_path": "holdout_replay/latest.json + holdout_readiness_after_backfill/latest.json",
            "status": "PRESENT" if evidence.get("holdout_loaded") else "MISSING",
            "classification": "NOT_TESTED" if not evidence.get("holdout_families_tested") else "TESTED",
            "signal": "source_backed_out_of_sample",
            "score": evidence.get("holdout_survived"),
            "notes": f"tested={evidence.get('holdout_families_tested')} remaining_readiness_blockers={evidence.get('holdout_remaining_blockers')}",
        },
        {
            "evidence_layer": "decision_review",
            "source_path": "research_decision_review/latest.json + controlled_surface_expansion_gate/latest.json",
            "status": "PRESENT" if evidence.get("decision_loaded") else "MISSING",
            "classification": evidence.get("decision_review_decision") or "UNKNOWN",
            "signal": evidence.get("expansion_gate_decision"),
            "score": "",
            "notes": "Prior review and gate both keep authority research-only and narrow.",
        },
    ]


def _recommended_next_phase(verdict: str, evidence: dict[str, Any]) -> list[dict[str, str]]:
    if verdict in {"INSUFFICIENT_EVIDENCE", "RESEARCH_NOT_JUSTIFIED"}:
        return [
            {
                "phase": "STOP_OR_REPAIR_EVIDENCE",
                "action": "Do not continue this line until missing or failed evidence is repaired.",
                "scope": "TSLA_30m_REVERSAL_TRENDING",
                "priority": "P0",
                "authority_boundary": "Research-only; no promotion or trading.",
                "exit_condition": "Minimum exact, net-of-cost, generalization, execution, forward, holdout, and decision-review evidence chain is present.",
            }
        ]
    return [
        {
            "phase": "TSLA_ONLY_FORWARD_OBSERVATION",
            "action": "Continue prospective observation for the surviving TSLA 30m family surface.",
            "scope": "TSLA_30m_REVERSAL_TRENDING_ONLY",
            "priority": "P0",
            "authority_boundary": "Research-only; no promotion or trading.",
            "exit_condition": "Mature source-backed forward sample is measured with positive net evidence.",
        },
        {
            "phase": "HOLDOUT_REPAIR_AND_REPLAY",
            "action": "Resolve governed holdout readiness blockers and rerun holdout replay.",
            "scope": "family_59cc928bca30cc44",
            "priority": "P0",
            "authority_boundary": "Research-only; no promotion or trading.",
            "exit_condition": "Holdout replay tests the target family without missing timestamp, split date, or return fields.",
        },
        {
            "phase": "EXECUTION_COST_MONITORING",
            "action": "Keep spread, slippage, and break-even cost checks attached to every observation cycle.",
            "scope": "TSLA_30m",
            "priority": "P1",
            "authority_boundary": "Research-only; no execution conclusion.",
            "exit_condition": "TSLA remains positive net-of-cost under realistic assumptions with no deterioration in execution fragility.",
        },
        {
            "phase": "LOCK_BROAD_EXPANSION",
            "action": "Do not expand to similar symbols, alternate timeframes, or reversal neighborhoods yet.",
            "scope": "non_TSLA_surfaces",
            "priority": "P1",
            "authority_boundary": "Research-only; no candidate promotion.",
            "exit_condition": "Only unlock after TSLA-only forward and holdout evidence mature and a new controlled gate approves expansion.",
        },
    ]


def _evidence_state(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    exact_latest = sources["exact_latest"]["data"] or {}
    exact_rows = sources["exact_rows"]["data"] or []
    tsla_exact = _select_row(exact_rows, family_id=TARGET_FAMILY_ID, symbol=TARGET_SYMBOL, timeframe=TARGET_TIMEFRAME)
    net_latest = sources["net_latest"]["data"] or {}
    cost_latest = sources["execution_cost_latest"]["data"] or {}
    tsla_cost_rows = sources["tsla_execution_viability"]["data"] or []
    tsla_cost = _select_row(tsla_cost_rows, family_id=TARGET_FAMILY_ID, symbol=TARGET_SYMBOL, timeframe=TARGET_TIMEFRAME)
    generalization_latest = sources["generalization_latest"]["data"] or {}
    symbol_rows = sources["cross_symbol_generalization"]["data"] or []
    tsla_symbol = _select_row(symbol_rows, symbol=TARGET_SYMBOL)
    execution_latest = sources["execution_latest"]["data"] or {}
    forward_latest = sources["forward_latest"]["data"] or {}
    holdout_latest = sources["holdout_latest"]["data"] or {}
    holdout_readiness = sources["holdout_readiness"]["data"] or {}
    decision_latest = sources["decision_latest"]["data"] or {}
    decision_summary = decision_latest.get("summary") or decision_latest
    decision_conclusion = decision_summary.get("conclusion") or {}
    gate_latest = sources["gate_latest"]["data"] or {}
    min_layers = ["exact_latest", "exact_rows", "net_latest", "execution_cost_latest", "tsla_execution_viability", "generalization_latest", "execution_latest", "forward_latest", "holdout_latest", "decision_latest", "gate_latest"]
    return {
        "has_minimum_evidence": all(sources[name]["loaded"] for name in min_layers),
        "exact_loaded": sources["exact_latest"]["loaded"] and sources["exact_rows"]["loaded"],
        "exact_blocked": (exact_latest.get("summary") or {}).get("exact_blocked"),
        "exact_classification": tsla_exact.get("classification"),
        "exact_tsla_samples": _int_value(tsla_exact.get("sample_size")),
        "exact_tsla_expectancy": _float_value(tsla_exact.get("expectancy")),
        "exact_tsla_profit_factor": _float_value(tsla_exact.get("profit_factor")),
        "net_loaded": sources["net_latest"]["loaded"] and sources["tsla_execution_viability"]["loaded"],
        "net_survives_strong": (net_latest.get("summary") or {}).get("net_survives_strong"),
        "tsla_cost_classification": tsla_cost.get("classification"),
        "tsla_net_classification_10bps": tsla_cost.get("classification_10bps"),
        "tsla_net_expectancy_10bps": _float_value(tsla_cost.get("net_expectancy_10bps")),
        "tsla_net_profit_factor_10bps": _float_value(tsla_cost.get("net_profit_factor_10bps")),
        "tsla_break_even_cost_bps": _float_value(tsla_cost.get("break_even_cost_bps")),
        "tsla_survives_realistic_assumptions": _bool_value(tsla_cost.get("survives_realistic_assumptions")),
        "execution_cost_overall": (cost_latest.get("summary") or {}).get("overall_classification"),
        "tsla_surviving_realistic_rows": ((cost_latest.get("summary") or {}).get("tsla_viability") or {}).get("surviving_realistic_rows"),
        "generalization_loaded": sources["generalization_latest"]["loaded"] and sources["cross_symbol_generalization"]["loaded"],
        "generalization_classification": (generalization_latest.get("summary") or {}).get("overall_classification") or generalization_latest.get("overall_classification"),
        "surviving_symbol_count": (generalization_latest.get("summary") or {}).get("surviving_symbol_count"),
        "surviving_symbol": TARGET_SYMBOL if tsla_symbol.get("classification") == "SYMBOL_STRONG" else "",
        "surviving_symbol_classification": tsla_symbol.get("classification"),
        "surviving_symbol_net_expectancy": _float_value(tsla_symbol.get("net_expectancy")),
        "execution_loaded": sources["execution_latest"]["loaded"] and sources["execution_cost_latest"]["loaded"],
        "execution_overall": (execution_latest.get("summary") or {}).get("overall_classification"),
        "execution_classification": (execution_latest.get("summary") or {}).get("execution_classification"),
        "liquidity_classification": (execution_latest.get("summary") or {}).get("liquidity_classification"),
        "slippage_classification": (execution_latest.get("summary") or {}).get("slippage_classification"),
        "tsla_execution_classification": ((cost_latest.get("summary") or {}).get("tsla_viability") or {}).get("classification") or tsla_cost.get("classification"),
        "forward_loaded": sources["forward_latest"]["loaded"],
        "forward_classification": (forward_latest.get("summary") or {}).get("forward_classification"),
        "forward_new_signals": (forward_latest.get("summary") or {}).get("new_signals"),
        "forward_pending_observations": (forward_latest.get("summary") or {}).get("pending_observations"),
        "forward_measured_outcomes": (forward_latest.get("summary") or {}).get("measured_outcomes"),
        "holdout_loaded": sources["holdout_latest"]["loaded"],
        "holdout_families_tested": (holdout_latest.get("summary") or {}).get("families_tested"),
        "holdout_survived": (holdout_latest.get("summary") or {}).get("survived"),
        "holdout_failed": (holdout_latest.get("summary") or {}).get("failed"),
        "holdout_confidence_impact": (holdout_latest.get("summary") or {}).get("confidence_impact"),
        "holdout_remaining_blockers": (holdout_readiness.get("summary") or {}).get("remaining_blockers"),
        "decision_loaded": sources["decision_latest"]["loaded"] and sources["gate_latest"]["loaded"],
        "decision_review_decision": decision_conclusion.get("decision"),
        "edge_durable": decision_conclusion.get("edge_durable"),
        "edge_exploitable": decision_conclusion.get("edge_exploitable"),
        "expansion_gate_decision": (gate_latest.get("summary") or {}).get("decision"),
    }


def _remaining_blockers(evidence: dict[str, Any]) -> list[str]:
    blockers = []
    if evidence.get("forward_classification") in {"FORWARD_NOT_STARTED", None, ""}:
        blockers.append("Forward observation has no mature measured TSLA target-family outcomes.")
    if not evidence.get("holdout_families_tested"):
        blockers.append("Holdout replay has not tested the target family with complete source-backed rows.")
    if evidence.get("generalization_classification") == "FRAGILE":
        blockers.append("Generalization remains fragile; only TSLA is symbol-strong.")
    if evidence.get("execution_cost_overall") == "EXECUTION_FRAGILE" or evidence.get("execution_overall") == "FRAGILE_EDGE":
        blockers.append("Execution evidence is fragile and cost/slippage-sensitive.")
    return blockers


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "exact_latest": root / "exact_replay_without_fallback" / "latest.json",
        "exact_rows": root / "exact_replay_without_fallback" / "exact_replay_results.csv",
        "net_latest": root / "net_of_cost_evidence" / "latest.json",
        "execution_cost_latest": root / "execution_cost_failure_analysis" / "latest.json",
        "tsla_execution_viability": root / "execution_cost_failure_analysis" / "tsla_execution_viability.csv",
        "generalization_latest": root / "generalization_edge_magnitude_assessment" / "latest.json",
        "cross_symbol_generalization": root / "generalization_edge_magnitude_assessment" / "cross_symbol_generalization.csv",
        "execution_latest": root / "execution_realism_economic_viability" / "latest.json",
        "forward_latest": root / "forward_observation_loop" / "latest.json",
        "holdout_latest": root / "holdout_replay" / "latest.json",
        "holdout_readiness": root / "holdout_readiness_after_backfill" / "latest.json",
        "decision_latest": root / "research_decision_review" / "latest.json",
        "gate_latest": root / "controlled_surface_expansion_gate" / "latest.json",
    }
    return {name: _load_path(path) for name, path in paths.items()}


def _load_path(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "loaded": False, "data": None}
    try:
        if path.suffix == ".csv":
            with path.open(newline="", encoding="utf-8") as handle:
                data = list(csv.DictReader(handle))
        else:
            data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"path": str(path), "exists": True, "loaded": False, "data": None, "error": str(exc)}
    return {"path": str(path), "exists": True, "loaded": True, "data": data}


def _select_row(rows: list[dict[str, Any]], **filters: str) -> dict[str, Any]:
    for row in rows:
        if all(str(row.get(key, "")) == value for key, value in filters.items()):
            return row
    return {}


def _score(question_id: str, question: str, answer: str, classification: str, layers: str, confidence: str, notes: str) -> dict[str, str]:
    return {
        "question_id": question_id,
        "question": question,
        "answer": answer,
        "classification": classification,
        "evidence_layers": layers,
        "confidence_impact": confidence,
        "notes": notes,
    }


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _bullet_list(values: list[str]) -> list[str]:
    return [f"- {value}" for value in values]


def _float_value(value: Any) -> float | None:
    try:
        return None if value in {None, ""} else float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int | None:
    try:
        return None if value in {None, ""} else int(float(value))
    except (TypeError, ValueError):
        return None


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
