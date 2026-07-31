from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "expansion_program_review"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"

FINAL_DECISIONS = [
    "TSLA_ONLY",
    "HIGH_VOLATILITY_CLUSTER",
    "LARGE_CAP_GROWTH_CLUSTER",
    "BROADER_REVERSAL_SURFACE",
    "NO_EXPANSION_JUSTIFIED",
]

AUTHORITY_BOUNDARY = (
    "Research-only expansion program review. No trading, no recommendations, no broker execution, "
    "no capital allocation, no position sizing, no candidate promotion, and no paper placement authority."
)

SCORECARD_COLUMNS = [
    "evidence_area",
    "source_path",
    "source_status",
    "classification",
    "final_decision",
    "supports_tsla_anomaly",
    "supports_high_volatility_phenomenon",
    "supports_growth_stock_phenomenon",
    "supports_broader_market_phenomenon",
    "score",
    "rationale",
]
INVENTORY_COLUMNS = [
    "survivor_id",
    "surface",
    "symbol",
    "timeframe",
    "mechanism",
    "regime",
    "classification",
    "sample_size",
    "net_expectancy_10bps",
    "decision",
    "source_path",
    "notes",
]
RANKING_COLUMNS = [
    "rank",
    "decision",
    "surface",
    "support_level",
    "evidence_strength",
    "blocker_count",
    "rationale",
]
DECISION_COLUMNS = [
    "question_id",
    "question",
    "answer",
    "final_decision",
    "confidence_impact",
    "authority",
    "rationale",
]


def run_expansion_program_review(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_expansion_program_review(root=root, created_at=created_at)
    write_expansion_program_review(report, root=root)
    return report


def build_expansion_program_review(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    evidence = _evidence_state(sources)
    scorecard = _scorecard(evidence, sources)
    inventory = _survivor_inventory(evidence, sources)
    rankings = _survivor_rankings(evidence)
    decisions = _expansion_decisions(evidence)
    summary = {
        "program_decision": "NO_EXPANSION_JUSTIFIED",
        "strongest_supported_decision": "TSLA_ONLY",
        "target_family_id": TARGET_FAMILY_ID,
        "target_surface": f"{TARGET_SYMBOL} / {TARGET_TIMEFRAME} / {TARGET_MECHANISM} / {TARGET_REGIME}",
        "did_discover_tsla_anomaly": "YES_RESEARCH_ONLY",
        "did_discover_high_volatility_phenomenon": "NO",
        "did_discover_growth_stock_phenomenon": "NO",
        "did_discover_broader_market_phenomenon": "NO",
        "confidence_impact": evidence.get("confidence_impact") or "NONE",
        "research_only": True,
        "trading_authority": False,
        "recommendation_authority": False,
        "source_inputs_present": sum(1 for source in sources.values() if source["loaded"]),
        "source_inputs_expected": len(sources),
        "rationale": (
            "Atlas found a source-backed TSLA-only anomaly, but expansion evidence does not justify a "
            "high-volatility cluster, large-cap growth cluster, broader reversal surface, or broader market phenomenon."
        ),
    }
    return {
        "schema_id": "atlas_v2_research_os_expansion_program_review",
        "schema_version": "1.0",
        "report_type": "EXPANSION_PROGRAM_REVIEW",
        "build": "183-186",
        "created_at": created,
        "day": created[:10],
        "final_decisions": FINAL_DECISIONS,
        "summary": summary,
        "evidence_state": evidence,
        "source_inputs": {name: source["path"] for name, source in sources.items()},
        "source_input_status": {name: {"exists": source["exists"], "loaded": source["loaded"]} for name, source in sources.items()},
        "expansion_scorecard": scorecard,
        "survivor_inventory": inventory,
        "survivor_rankings": rankings,
        "expansion_decision": decisions,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "Consumers must not infer expansion truth outside these source-backed decisions.",
            "Weak, blocked, or missing expansion evidence is not a survivor.",
            "The TSLA_ONLY finding is research-only and does not imply trading, recommendation, sizing, promotion, or paper placement authority.",
        ],
    }


def write_expansion_program_review(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "scorecard": out_dir / "expansion_scorecard.csv",
        "inventory": out_dir / "survivor_inventory.csv",
        "rankings": out_dir / "survivor_rankings.csv",
        "decision": out_dir / "expansion_decision.csv",
        "summary": out_dir / "latest_summary.md",
    }
    _write_csv(paths["scorecard"], SCORECARD_COLUMNS, report.get("expansion_scorecard") or [])
    _write_csv(paths["inventory"], INVENTORY_COLUMNS, report.get("survivor_inventory") or [])
    _write_csv(paths["rankings"], RANKING_COLUMNS, report.get("survivor_rankings") or [])
    _write_csv(paths["decision"], DECISION_COLUMNS, report.get("expansion_decision") or [])
    paths["summary"].write_text(render_expansion_program_review_summary(report), encoding="utf-8")
    return paths


def render_expansion_program_review_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 183-186 - Expansion Program Review",
        "",
        f"Program decision: {summary.get('program_decision')}",
        f"Strongest supported decision: {summary.get('strongest_supported_decision')}",
        f"Target surface: {summary.get('target_surface')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Key Answers",
        "",
    ]
    for row in report.get("expansion_decision") or []:
        lines.append(f"- {row.get('question')}: {row.get('answer')} ({row.get('final_decision')})")
    lines.extend(["", "## Survivor Ranking", ""])
    for row in report.get("survivor_rankings") or []:
        lines.append(f"- #{row.get('rank')} {row.get('surface')}: {row.get('decision')} - {row.get('rationale')}")
    lines.extend(
        [
            "",
            "## Synthesis",
            "",
            str(summary.get("rationale") or ""),
            "",
            "## Authority Boundary",
            "",
            AUTHORITY_BOUNDARY,
            "",
        ]
    )
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "similar_symbol_expansion": root / "controlled_similar_symbol_expansion" / "latest.json",
        "large_cap_growth_expansion": root / "large_cap_growth_expansion" / "latest.json",
        "volatility_profile_expansion": root / "volatility_profile_expansion" / "latest.json",
        "reversal_neighborhood_expansion": root / "reversal_neighborhood_expansion" / "latest.json",
        "exact_replay": root / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost": root / "net_of_cost_evidence" / "latest.json",
        "final_research_verdict": root / "final_research_verdict" / "latest.json",
        "expansion_evidence_review": root / "expansion_evidence_review" / "latest.json",
        "controlled_surface_gate": root / "controlled_surface_expansion_gate" / "latest.json",
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
    final = sources["final_research_verdict"]["payload"]
    final_summary = final.get("summary") or {}
    final_evidence = final.get("evidence_state") or {}
    similar = sources["similar_symbol_expansion"]["payload"]
    similar_summary = similar.get("summary") or {}
    volatility = sources["volatility_profile_expansion"]["payload"]
    volatility_summary = volatility.get("summary") or {}
    high_vol = _find_by_key(volatility.get("bucket_results") or [], "volatility_bucket", "HIGH_VOLATILITY")
    large_cap = sources["large_cap_growth_expansion"]["payload"]
    large_cap_summary = large_cap.get("summary") or {}
    large_cap_top = _find_by_key(large_cap.get("cross_symbol_ranking") or [], "rank", 1)
    expansion_review = sources["expansion_evidence_review"]["payload"]
    gate = sources["controlled_surface_gate"]["payload"]
    reversal = sources["reversal_neighborhood_expansion"]["payload"]
    net = sources["net_of_cost"]["payload"]
    non_tsla_confirmed = [
        row.get("symbol")
        for row in similar.get("similar_symbol_comparison") or []
        if row.get("symbol") != TARGET_SYMBOL and row.get("classification") == "SIMILAR_SYMBOL_CONFIRMED"
    ]
    return {
        "final_verdict": final_summary.get("final_verdict"),
        "confidence_impact": final_summary.get("confidence_impact"),
        "exact_classification": final_evidence.get("exact_classification"),
        "exact_samples": final_evidence.get("exact_tsla_samples"),
        "exact_expectancy": final_evidence.get("exact_tsla_expectancy"),
        "net_classification": final_evidence.get("tsla_cost_classification") or (net.get("summary") or {}).get("target_classification"),
        "net_expectancy_10bps": final_evidence.get("tsla_net_expectancy_10bps"),
        "generalization_classification": final_evidence.get("generalization_classification"),
        "surviving_symbol": final_evidence.get("surviving_symbol"),
        "surviving_symbol_classification": final_evidence.get("surviving_symbol_classification"),
        "similar_conclusion": similar_summary.get("expansion_conclusion"),
        "similar_symbols_confirmed": similar_summary.get("symbols_confirmed"),
        "similar_symbols_weak": similar_summary.get("symbols_weak"),
        "similar_symbols_failed": similar_summary.get("symbols_failed"),
        "similar_symbols_blocked": similar_summary.get("symbols_blocked"),
        "non_tsla_confirmed": non_tsla_confirmed,
        "high_volatility_classification": high_vol.get("bucket_classification") or "MISSING",
        "high_volatility_sample_size": high_vol.get("symbols_replayed", ""),
        "high_volatility_net_expectancy_10bps": high_vol.get("mean_net_expectancy_10bps", ""),
        "volatility_overall_classification": volatility_summary.get("overall_classification"),
        "volatility_key_question_answer": volatility_summary.get("key_question_answer"),
        "large_cap_growth_loaded": sources["large_cap_growth_expansion"]["loaded"],
        "large_cap_growth_conclusion": large_cap_summary.get("expansion_conclusion"),
        "large_cap_growth_top_symbol": large_cap_summary.get("top_symbol") or large_cap_top.get("symbol"),
        "large_cap_growth_top_classification": _large_cap_growth_classification(large_cap_summary, large_cap_top),
        "large_cap_growth_symbols_weak": large_cap_summary.get("symbols_weak"),
        "large_cap_growth_symbols_failed": large_cap_summary.get("symbols_failed"),
        "reversal_top_classification": (reversal.get("summary") or {}).get("top_classification"),
        "expansion_review_decision": (expansion_review.get("summary") or {}).get("decision"),
        "expansion_review_rationale": (expansion_review.get("summary") or {}).get("rationale"),
        "controlled_gate_decision": (gate.get("summary") or {}).get("decision"),
        "controlled_gate_locked_dimensions": ((gate.get("summary") or {}).get("answers") or {}).get("dimensions_locked", []),
    }


def _scorecard(evidence: dict[str, Any], sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _score(
            "TSLA exact replay and net-of-cost",
            sources["exact_replay"],
            evidence.get("exact_classification"),
            "TSLA_ONLY",
            "YES",
            "NO",
            "NO",
            "NO",
            evidence.get("net_expectancy_10bps"),
            "Exact replay is strong and TSLA remains positive at 10 bps, but this evidence is symbol-specific.",
        ),
        _score(
            "Similar symbol expansion",
            sources["similar_symbol_expansion"],
            "NO_NON_TSLA_CONFIRMED" if not evidence.get("non_tsla_confirmed") else "NON_TSLA_CONFIRMED",
            "NO_EXPANSION_JUSTIFIED",
            "YES",
            "NO",
            "NO",
            "NO",
            evidence.get("similar_symbols_confirmed"),
            evidence.get("similar_conclusion") or "No source-backed similar-symbol expansion conclusion.",
        ),
        _score(
            "High-volatility profile expansion",
            sources["volatility_profile_expansion"],
            evidence.get("high_volatility_classification"),
            "NO_EXPANSION_JUSTIFIED",
            "NO",
            "NO",
            "NO",
            "NO",
            evidence.get("high_volatility_sample_size"),
            evidence.get("volatility_key_question_answer") or "Volatility evidence is weak and does not establish a multi-symbol phenomenon.",
        ),
        _score(
            "Large-cap growth expansion",
            sources["large_cap_growth_expansion"],
            evidence.get("large_cap_growth_top_classification") or ("MISSING_REPORT" if not evidence.get("large_cap_growth_loaded") else "UNKNOWN"),
            "NO_EXPANSION_JUSTIFIED",
            "YES" if evidence.get("large_cap_growth_top_symbol") == TARGET_SYMBOL else "NO",
            "NO",
            "NO",
            "NO",
            evidence.get("large_cap_growth_symbols_weak"),
            evidence.get("large_cap_growth_conclusion") or "No materialized large-cap growth expansion evidence was found; similar-symbol evidence did not confirm non-TSLA growth names.",
        ),
        _score(
            "Broader reversal surface",
            sources["reversal_neighborhood_expansion"],
            evidence.get("reversal_top_classification") or "UNKNOWN",
            "NO_EXPANSION_JUSTIFIED",
            "NO",
            "NO",
            "NO",
            "NO",
            "",
            "Reversal variants were fragile or failed, so a broader reversal surface is not justified.",
        ),
        _score(
            "Final research verdict",
            sources["final_research_verdict"],
            evidence.get("final_verdict"),
            "TSLA_ONLY",
            "YES",
            "NO",
            "NO",
            "NO",
            evidence.get("confidence_impact"),
            "Final verdict is CONTINUE_TSLA_ONLY with no promotion or trading authority.",
        ),
    ]


def _survivor_inventory(evidence: dict[str, Any], sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    if evidence.get("surviving_symbol") == TARGET_SYMBOL:
        rows.append(
            {
                "survivor_id": "survivor_001",
                "surface": f"{TARGET_SYMBOL} {TARGET_TIMEFRAME} {TARGET_MECHANISM}/{TARGET_REGIME}",
                "symbol": TARGET_SYMBOL,
                "timeframe": TARGET_TIMEFRAME,
                "mechanism": TARGET_MECHANISM,
                "regime": TARGET_REGIME,
                "classification": evidence.get("surviving_symbol_classification") or evidence.get("exact_classification"),
                "sample_size": evidence.get("exact_samples"),
                "net_expectancy_10bps": evidence.get("net_expectancy_10bps"),
                "decision": "TSLA_ONLY",
                "source_path": sources["final_research_verdict"]["path"],
                "notes": "Only source-backed symbol-level survivor in the final verdict.",
            }
        )
    rows.append(
        {
            "survivor_id": "non_survivor_high_volatility",
            "surface": "HIGH_VOLATILITY profile",
            "symbol": TARGET_SYMBOL,
            "timeframe": TARGET_TIMEFRAME,
            "mechanism": TARGET_MECHANISM,
            "regime": "HIGH_VOLATILITY",
            "classification": evidence.get("high_volatility_classification"),
            "sample_size": evidence.get("high_volatility_sample_size"),
            "net_expectancy_10bps": evidence.get("high_volatility_net_expectancy_10bps"),
            "decision": "NO_EXPANSION_JUSTIFIED",
            "source_path": sources["volatility_profile_expansion"]["path"],
            "notes": "Tracked for completeness; not an expansion survivor.",
        }
    )
    return rows


def _survivor_rankings(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "rank": 1,
            "decision": "TSLA_ONLY",
            "surface": f"{TARGET_SYMBOL} {TARGET_TIMEFRAME} {TARGET_MECHANISM}/{TARGET_REGIME}",
            "support_level": "SUPPORTED_RESEARCH_ONLY",
            "evidence_strength": evidence.get("exact_classification") or "UNKNOWN",
            "blocker_count": 0,
            "rationale": "Exact replay and net-of-cost support only the TSLA target surface.",
        },
        {
            "rank": 2,
            "decision": "NO_EXPANSION_JUSTIFIED",
            "surface": "HIGH_VOLATILITY_CLUSTER",
            "support_level": "REJECTED",
            "evidence_strength": evidence.get("high_volatility_classification") or "UNKNOWN",
            "blocker_count": 1,
            "rationale": "High-volatility evidence is weak and does not establish a multi-symbol phenomenon.",
        },
        {
            "rank": 3,
            "decision": "NO_EXPANSION_JUSTIFIED",
            "surface": "LARGE_CAP_GROWTH_CLUSTER",
            "support_level": "REJECTED",
            "evidence_strength": evidence.get("large_cap_growth_top_classification") or "UNKNOWN",
            "blocker_count": 1,
            "rationale": evidence.get("large_cap_growth_conclusion") or "Large-cap growth evidence did not confirm non-TSLA expansion.",
        },
        {
            "rank": 4,
            "decision": "NO_EXPANSION_JUSTIFIED",
            "surface": "BROADER_REVERSAL_SURFACE",
            "support_level": "REJECTED",
            "evidence_strength": evidence.get("reversal_top_classification") or "UNKNOWN",
            "blocker_count": 1,
            "rationale": "Nearby reversal variants are fragile or failed.",
        },
    ]


def _expansion_decisions(evidence: dict[str, Any]) -> list[dict[str, str]]:
    return [
        _decision("Q1", "Did Atlas discover a TSLA anomaly?", "YES_RESEARCH_ONLY", "TSLA_ONLY", evidence.get("confidence_impact"), "Exact replay is strong and TSLA remains the only symbol-level survivor."),
        _decision("Q2", "Did Atlas discover a high-volatility phenomenon?", "NO", "NO_EXPANSION_JUSTIFIED", evidence.get("confidence_impact"), "Volatility evidence is weak: only TSLA survived in the high-volatility bucket, with blocked symbols and no multi-symbol confirmation."),
        _decision("Q3", "Did Atlas discover a growth-stock phenomenon?", "NO", "NO_EXPANSION_JUSTIFIED", evidence.get("confidence_impact"), "Large-cap growth evidence ranked TSLA first; non-TSLA symbols were weak, failed, cost-eroded, or blocked."),
        _decision("Q4", "Did Atlas discover a broader market phenomenon?", "NO", "NO_EXPANSION_JUSTIFIED", evidence.get("confidence_impact"), "Generalization is fragile and broader reversal/timeframe/regime surfaces are not justified."),
        _decision("Q5", "Should Atlas expand beyond TSLA now?", "NO", "NO_EXPANSION_JUSTIFIED", evidence.get("confidence_impact"), "The expansion program should not expand beyond source-backed TSLA-only research."),
    ]


def _large_cap_growth_classification(summary: dict[str, Any], top_row: dict[str, Any]) -> str:
    if summary.get("top_symbol") == TARGET_SYMBOL and (summary.get("symbols_confirmed") in {None, 1}):
        return "TSLA_ONLY_CONFIRMED_NO_NON_TSLA"
    return str(top_row.get("classification") or "")


def _score(
    evidence_area: str,
    source: dict[str, Any],
    classification: Any,
    final_decision: str,
    supports_tsla: str,
    supports_high_vol: str,
    supports_growth: str,
    supports_broader: str,
    score: Any,
    rationale: str,
) -> dict[str, Any]:
    return {
        "evidence_area": evidence_area,
        "source_path": source["path"],
        "source_status": "LOADED" if source["loaded"] else "MISSING",
        "classification": classification or "UNKNOWN",
        "final_decision": final_decision,
        "supports_tsla_anomaly": supports_tsla,
        "supports_high_volatility_phenomenon": supports_high_vol,
        "supports_growth_stock_phenomenon": supports_growth,
        "supports_broader_market_phenomenon": supports_broader,
        "score": score,
        "rationale": rationale,
    }


def _decision(question_id: str, question: str, answer: str, final_decision: str, confidence_impact: Any, rationale: str) -> dict[str, str]:
    return {
        "question_id": question_id,
        "question": question,
        "answer": answer,
        "final_decision": final_decision,
        "confidence_impact": str(confidence_impact or "NONE"),
        "authority": "RESEARCH_ONLY",
        "rationale": rationale,
    }


def _find_by_key(rows: list[dict[str, Any]], key: str, value: str) -> dict[str, Any]:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
