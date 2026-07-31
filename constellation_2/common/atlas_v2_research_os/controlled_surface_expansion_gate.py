from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "controlled_surface_expansion_gate"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
PRIMARY_SYMBOL = "TSLA"
PRIMARY_TIMEFRAME = "30m"
PRIMARY_MECHANISM = "REVERSAL"
PRIMARY_REGIME = "TRENDING"
ALLOWED_DECISIONS = [
    "DO_NOT_EXPAND",
    "EXPAND_NARROW_TSLA_ONLY",
    "EXPAND_SIMILAR_SYMBOLS",
    "EXPAND_REVERSAL_TRENDING_NEIGHBORHOOD",
    "EXPAND_BROAD_SURFACE",
]
AUTHORITY_BOUNDARY = (
    "Research-only controlled surface expansion gate. No candidate promotion, no live trading, "
    "no broker execution, no capital allocation, no position sizing, no trade recommendation, "
    "no automatic paper placement, and no production promotion."
)
DECISION_COLUMNS = ["question", "decision", "status", "rationale", "evidence"]
APPROVED_COLUMNS = ["surface_id", "family_id", "dimension", "symbol", "timeframe", "mechanism", "regime", "variant", "approval_status", "guardrail"]
BLOCKED_COLUMNS = ["surface_id", "family_id", "dimension", "blocked_value", "blocked_decision", "reason", "unlock_requirement"]
RISK_COLUMNS = ["risk_id", "dimension", "risk_level", "evidence", "mitigation", "invalidation_condition"]


def run_controlled_surface_expansion_gate(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_controlled_surface_expansion_gate(root=root, created_at=created_at)
    write_controlled_surface_expansion_gate(report, root=root)
    return report


def build_controlled_surface_expansion_gate(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    evidence = _evidence_state(sources)
    decision = _gate_decision(evidence)
    approved = _approved_surface(decision, evidence)
    blocked = _blocked_surface(decision, evidence)
    risks = _risk_report(evidence)
    decisions = _decision_rows(decision, evidence)
    summary = {
        "decision": decision,
        "allowed_decisions": ALLOWED_DECISIONS,
        "target_family_id": TARGET_FAMILY_ID,
        "surviving_family_only": True,
        "approved_surface_count": len(approved),
        "blocked_surface_count": len(blocked),
        "high_risk_count": sum(1 for row in risks if row["risk_level"] == "HIGH"),
        "confidence_impact": "NONE",
        "candidate_promotion": False,
        "trading_authority": False,
        "answers": {
            "should_expand_around_reversal_trending_30m": "YES_NARROW_TSLA_ONLY" if decision == "EXPAND_NARROW_TSLA_ONLY" else "NO",
            "safest_dimensions_first": ["TSLA", "30m", "REVERSAL", "TRENDING", "baseline reversal variant only"],
            "dimensions_locked": ["nearby timeframes", "related reversal variants", "neighboring regimes", "similar symbols beyond TSLA", "broad TSLA-like volatility basket"],
            "what_invalidates_expansion": [row["invalidation_condition"] for row in risks],
        },
    }
    return {
        "schema_id": "atlas_v2_research_os_controlled_surface_expansion_gate",
        "schema_version": "1.0",
        "report_type": "CONTROLLED_SURFACE_EXPANSION_GATE",
        "build": "138-140",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "evidence_state": evidence,
        "source_inputs": {name: source["path"] for name, source in sources.items()},
        "source_input_status": {name: {"exists": source["exists"], "loaded": source["loaded"]} for name, source in sources.items()},
        "expansion_gate_decision": decisions,
        "approved_expansion_surface": approved,
        "blocked_expansion_surface": blocked,
        "expansion_risk_report": risks,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "Do not broadly expand yet.",
            "Only the surviving family is eligible for this gate.",
            "Approved rows are observation/research surfaces only, not candidates or promotions.",
            "Expansion is invalidated by failed TSLA-only observation, cost erosion above 10 bps, holdout failure, or evidence fabrication/lineage gaps.",
        ],
    }


def write_controlled_surface_expansion_gate(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "decision": out_dir / "expansion_gate_decision.csv",
        "approved": out_dir / "approved_expansion_surface.csv",
        "blocked": out_dir / "blocked_expansion_surface.csv",
        "risk": out_dir / "expansion_risk_report.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_controlled_surface_expansion_gate_summary(report), encoding="utf-8")
    _write_csv(paths["decision"], DECISION_COLUMNS, report.get("expansion_gate_decision") or [])
    _write_csv(paths["approved"], APPROVED_COLUMNS, report.get("approved_expansion_surface") or [])
    _write_csv(paths["blocked"], BLOCKED_COLUMNS, report.get("blocked_expansion_surface") or [])
    _write_csv(paths["risk"], RISK_COLUMNS, report.get("expansion_risk_report") or [])
    return paths


def render_controlled_surface_expansion_gate_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    evidence = report.get("evidence_state") or {}
    lines = [
        "# Builds 138-140 - Controlled Surface Expansion Gate",
        "",
        f"Decision: {summary.get('decision')}",
        f"Target family: {summary.get('target_family_id')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        f"Candidate promotion: {summary.get('candidate_promotion')}",
        f"Trading authority: {summary.get('trading_authority')}",
        "",
        "## Answers",
        "",
        f"Should Atlas expand around REVERSAL/TRENDING/30m? {summary.get('answers', {}).get('should_expand_around_reversal_trending_30m')}",
        f"Safest dimensions first: {', '.join(summary.get('answers', {}).get('safest_dimensions_first') or [])}",
        f"Dimensions locked: {', '.join(summary.get('answers', {}).get('dimensions_locked') or [])}",
        "",
        "## Evidence",
        "",
        f"Final conclusion: {evidence.get('final_overall_conclusion')} strongest_family={evidence.get('strongest_family')}",
        f"Cost robustness: {evidence.get('cost_fragility')} break_even_cost_bps={evidence.get('break_even_cost_bps')}",
        f"Generalization: {evidence.get('generalization_overall')} surviving_symbol={evidence.get('surviving_symbol')} symbol_classification={evidence.get('surviving_symbol_classification')}",
        f"Execution realism: {evidence.get('execution_overall')} liquidity={evidence.get('liquidity_classification')} slippage={evidence.get('slippage_classification')}",
        "",
        "## Invalidation Conditions",
        "",
        *_bullet_list(summary.get('answers', {}).get('what_invalidates_expansion') or []),
        "",
        "## Authority Boundary",
        "",
        AUTHORITY_BOUNDARY,
        "",
    ]
    return "\n".join(lines)


def _gate_decision(evidence: dict[str, Any]) -> str:
    if evidence.get("strongest_family") != TARGET_FAMILY_ID:
        return "DO_NOT_EXPAND"
    if evidence.get("final_overall_conclusion") not in {"RESEARCH_PROMISING", "RESEARCH_WEAK"}:
        return "DO_NOT_EXPAND"
    if evidence.get("surviving_symbol") == PRIMARY_SYMBOL and evidence.get("surviving_symbol_classification") == "SYMBOL_STRONG":
        if evidence.get("cost_fragility") == "COST_SENSITIVE" or evidence.get("generalization_overall") == "FRAGILE":
            return "EXPAND_NARROW_TSLA_ONLY"
        return "EXPAND_SIMILAR_SYMBOLS"
    return "DO_NOT_EXPAND"


def _approved_surface(decision: str, evidence: dict[str, Any]) -> list[dict[str, Any]]:
    if decision != "EXPAND_NARROW_TSLA_ONLY":
        return []
    return [
        {
            "surface_id": "approved_001",
            "family_id": TARGET_FAMILY_ID,
            "dimension": "surviving_symbol_baseline_surface",
            "symbol": PRIMARY_SYMBOL,
            "timeframe": PRIMARY_TIMEFRAME,
            "mechanism": PRIMARY_MECHANISM,
            "regime": PRIMARY_REGIME,
            "variant": "BASELINE_REVERSAL_ONLY",
            "approval_status": "APPROVED_FOR_RESEARCH_OBSERVATION_ONLY",
            "guardrail": "No new candidates, no promotion, no trading; collect TSLA-only direct evidence and rerun exact/net/holdout gates before any wider expansion.",
        }
    ]


def _blocked_surface(decision: str, evidence: dict[str, Any]) -> list[dict[str, Any]]:
    common_unlock = "Requires TSLA-only forward/exact evidence to remain positive net-of-cost, holdout blockers resolved, and no lineage gaps."
    rows = [
        ("blocked_001", "nearby_symbols", "AAPL, AMZN, BAC, IWM, JPM, META, MSFT, SPY, TLT, USO", "Only TSLA has SYMBOL_STRONG evidence; other observed symbols are net negative at 10 bps."),
        ("blocked_002", "nearby_timeframes", "15m, 1h, 5m, daily", "No direct surviving timeframe evidence beyond the current 30m surface; broad 30m aggregate is still TIMEFRAME_FAILED."),
        ("blocked_003", "related_reversal_variants", "GAP_UP, GAP_DOWN, COMPRESSION, FAILED_BREAKOUT", "Variants are observation hypotheses, not validated surviving surfaces for this family."),
        ("blocked_004", "neighboring_regimes", "CHOP, HIGH_VOLATILITY, LOW_VOLATILITY, RISK_ON, BEAR, VOL_CONTRACTION", "Only TRENDING is tied to the surviving family evidence, and broad regime aggregate is still REGIME_FAILED."),
        ("blocked_005", "similar_high_liquidity_symbols", "AAPL, MSFT, SPY, QQQ, META, AMZN", "High liquidity alone is not evidence; current symbol-level net-of-cost evidence is not surviving outside TSLA."),
        ("blocked_006", "tsla_like_volatility_profiles", "NVDA, AMD, COIN, MSTR, high-beta basket", "TSLA-like volatility profile is a hypothesis class, not direct family evidence."),
        ("blocked_007", "broad_surface", "all REVERSAL/TRENDING/30m neighborhoods", "Cost sensitivity, fragile generalization, and execution slippage risk reject broad expansion."),
    ]
    return [
        {
            "surface_id": sid,
            "family_id": TARGET_FAMILY_ID,
            "dimension": dimension,
            "blocked_value": value,
            "blocked_decision": "LOCKED" if decision != "DO_NOT_EXPAND" else "DO_NOT_EXPAND",
            "reason": reason,
            "unlock_requirement": common_unlock,
        }
        for sid, dimension, value, reason in rows
    ]


def _decision_rows(decision: str, evidence: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "question": "Should Atlas expand around REVERSAL/TRENDING/30m?",
            "decision": decision,
            "status": "APPROVE_NARROW_SURFACE" if decision == "EXPAND_NARROW_TSLA_ONLY" else "BLOCK_EXPANSION",
            "rationale": "TSLA is the only symbol-level survivor; broad timeframe/regime and execution evidence remain fragile.",
            "evidence": "final_synthesis + cost_robustness + generalization + execution_realism",
        },
        {
            "question": "Which dimensions are safest to expand first?",
            "decision": decision,
            "status": "TSLA_ONLY",
            "rationale": "Keep symbol, timeframe, mechanism, and regime fixed: TSLA / 30m / REVERSAL / TRENDING.",
            "evidence": f"surviving_symbol={evidence.get('surviving_symbol')} classification={evidence.get('surviving_symbol_classification')}",
        },
        {
            "question": "Which dimensions should remain locked?",
            "decision": decision,
            "status": "LOCK_BROAD_SURFACE",
            "rationale": "Cost, generalization, and execution realism do not support nearby timeframes, variants, regimes, or symbol baskets.",
            "evidence": f"cost={evidence.get('cost_fragility')} generalization={evidence.get('generalization_overall')} execution={evidence.get('execution_overall')}",
        },
        {
            "question": "What would invalidate expansion?",
            "decision": decision,
            "status": "INVALIDATION_DEFINED",
            "rationale": "Expansion must stop if TSLA-only evidence fails, cost erosion worsens, holdout fails, lineage breaks, or execution realism deteriorates.",
            "evidence": "expansion_risk_report.csv",
        },
    ]


def _risk_report(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "risk_id": "risk_001",
            "dimension": "cost",
            "risk_level": "HIGH",
            "evidence": f"Break-even cost is {evidence.get('break_even_cost_bps')} bps and family is {evidence.get('cost_fragility')}.",
            "mitigation": "Keep expansion to TSLA-only observation; rerun net-of-cost grid before widening.",
            "invalidation_condition": "Invalidate expansion if TSLA-only net expectancy is non-positive at 10 bps or cost erodes before 10 bps.",
        },
        {
            "risk_id": "risk_002",
            "dimension": "generalization",
            "risk_level": "HIGH",
            "evidence": f"Build 120 overall={evidence.get('generalization_overall')}; only {evidence.get('surviving_symbol')} survives by symbol.",
            "mitigation": "Do not add similar symbols until TSLA-only evidence survives direct replay and holdout checks.",
            "invalidation_condition": "Invalidate expansion if any non-TSLA surface is introduced before TSLA-only validation completes.",
        },
        {
            "risk_id": "risk_003",
            "dimension": "execution_realism",
            "risk_level": "HIGH",
            "evidence": f"Execution overall={evidence.get('execution_overall')}; slippage={evidence.get('slippage_classification')}; liquidity={evidence.get('liquidity_classification')}.",
            "mitigation": "Treat every approved row as research-only and require execution realism refresh after any new observations.",
            "invalidation_condition": "Invalidate expansion if slippage sensitivity increases or execution classification is no longer viable.",
        },
        {
            "risk_id": "risk_004",
            "dimension": "holdout_lineage",
            "risk_level": "MEDIUM",
            "evidence": f"Final synthesis blockers: {', '.join(evidence.get('remaining_blockers') or [])}.",
            "mitigation": "Resolve holdout/data blockers before broad expansion.",
            "invalidation_condition": "Invalidate expansion if holdout replay fails, remains data-blocked for the approved surface, or lineage coverage breaks.",
        },
    ]


def _evidence_state(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    final = sources["final_evidence_synthesis"]["payload"].get("final_report") or {}
    cost = sources["cost_robustness_expansion"]["payload"]
    gen = sources["generalization_edge_magnitude_assessment"]["payload"]
    exec_real = sources["execution_realism_economic_viability"]["payload"].get("summary") or {}
    family_rows = cost.get("family_cost_robustness") or []
    target_cost = next((row for row in family_rows if row.get("family_id") == TARGET_FAMILY_ID), {})
    symbol_rows = gen.get("cross_symbol_generalization") or []
    surviving = [row for row in symbol_rows if row.get("classification") in {"SYMBOL_STRONG", "SYMBOL_WEAK"}]
    surviving.sort(key=lambda row: (row.get("classification") != "SYMBOL_STRONG", str(row.get("symbol", ""))))
    best_symbol = surviving[0] if surviving else {}
    return {
        "strongest_family": final.get("strongest_family"),
        "final_overall_conclusion": final.get("overall_conclusion"),
        "remaining_blockers": final.get("remaining_blockers") or [],
        "cost_fragility": target_cost.get("fragility_classification"),
        "break_even_cost_bps": target_cost.get("break_even_cost_bps"),
        "generalization_overall": gen.get("overall_classification"),
        "surviving_symbol": best_symbol.get("symbol"),
        "surviving_symbol_classification": best_symbol.get("classification"),
        "surviving_symbol_net_expectancy": best_symbol.get("net_expectancy"),
        "symbol_count": (gen.get("summary") or {}).get("symbol_count"),
        "surviving_symbol_count": (gen.get("summary") or {}).get("surviving_symbol_count"),
        "timeframe_count": (gen.get("summary") or {}).get("timeframe_count"),
        "regime_count": (gen.get("summary") or {}).get("regime_count"),
        "execution_overall": exec_real.get("overall_classification"),
        "execution_classification": exec_real.get("execution_classification"),
        "liquidity_classification": exec_real.get("liquidity_classification"),
        "slippage_classification": exec_real.get("slippage_classification"),
        "data_available_for_tsla_30m": _databento_has_tsla_30m(sources["databento_download_symbols"]["path"]),
    }


def _load_sources(root_path: Path) -> dict[str, dict[str, Any]]:
    source_paths = {
        "final_evidence_synthesis": root_path / "final_evidence_synthesis" / "latest.json",
        "cost_robustness_expansion": root_path / "cost_robustness_expansion" / "latest.json",
        "generalization_edge_magnitude_assessment": root_path / "generalization_edge_magnitude_assessment" / "latest.json",
        "execution_realism_economic_viability": root_path / "execution_realism_economic_viability" / "latest.json",
        "databento_download_symbols": root_path / "databento_download_symbols" / "databento_download_symbols.csv",
    }
    return {name: _load_json_or_csv_marker(path) for name, path in source_paths.items()}


def _load_json_or_csv_marker(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "loaded": False, "payload": {}}
    if path.suffix == ".csv":
        return {"path": str(path), "exists": True, "loaded": True, "payload": {}}
    try:
        return {"path": str(path), "exists": True, "loaded": True, "payload": json.loads(path.read_text(encoding="utf-8"))}
    except json.JSONDecodeError:
        return {"path": str(path), "exists": True, "loaded": False, "payload": {}}


def _databento_has_tsla_30m(path_value: str) -> bool:
    path = Path(path_value)
    if not path.exists():
        return False
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("symbol") == PRIMARY_SYMBOL and PRIMARY_TIMEFRAME in str(row.get("timeframes", "")):
                return True
    return False


def _bullet_list(values: list[str]) -> list[str]:
    return [f"- {value}" for value in values]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
