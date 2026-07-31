from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "final_evidence_synthesis"
SCORECARD_COLUMNS = ["evidence_layer", "source_report", "status", "confidence_impact", "summary"]
FAMILY_COLUMNS = ["family_id", "exact_classification", "holdout_classification", "stability_classification", "net_classification", "final_classification", "remaining_blockers", "next_research_only_recommendation"]
OPEN_COLUMNS = ["question_id", "family_id", "question", "blocking_evidence_needed"]


def run_final_evidence_synthesis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_final_evidence_synthesis(root=root, created_at=created_at)
    write_final_evidence_synthesis(report, root=root)
    return report


def build_final_evidence_synthesis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    exact = _read_json(root_path / "exact_replay_without_fallback" / "latest.json", {})
    holdout = _read_json(root_path / "holdout_replay_validation" / "latest.json", {})
    stability = _read_json(root_path / "family_stability_analysis" / "latest.json", {})
    net = _read_json(root_path / "net_of_cost_evidence" / "latest.json", {})
    lineage = _read_json(root_path / "evidence_lineage_graph" / "evidence_lineage_graph.json", {})
    families = _family_assessments(exact, holdout, stability, net)
    scorecard = _scorecard(exact, holdout, stability, net, lineage)
    open_questions = _open_questions(families)
    counts = Counter(row["final_classification"] for row in families)
    strongest = _strongest(families)
    weakest = _weakest(families)
    conclusion = {
        "overall_conclusion": _overall(counts),
        "strongest_family": strongest,
        "weakest_family": weakest,
        "confidence_impact": _confidence_impact(scorecard),
        "remaining_blockers": sorted({blocker for row in families for blocker in row["remaining_blockers"].split(";") if blocker}),
        "authority_boundary": "No trading authority, candidate promotion, production promotion, or trade recommendations.",
    }
    return {
        "schema_id": "atlas_v2_research_os_final_evidence_synthesis",
        "schema_version": "1.0",
        "report_type": "FINAL_EVIDENCE_SYNTHESIS",
        "build": "110",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "direct_replay": str(root_path / "direct_replay_attrition_audit" / "latest.json"),
            "exact_replay": str(root_path / "exact_replay_without_fallback" / "latest.json"),
            "holdout": str(root_path / "holdout_replay_validation" / "latest.json"),
            "forward_readiness": str(root_path / "holdout_aware_family_ranking" / "latest.json"),
            "lineage": str(root_path / "evidence_lineage_graph" / "evidence_lineage_graph.json"),
            "stability": str(root_path / "family_stability_analysis" / "latest.json"),
            "net_of_cost": str(root_path / "net_of_cost_evidence" / "latest.json"),
        },
        "required_conclusions": {
            "methodology_confidence": conclusion["confidence_impact"],
            "signal_likely_exists": any(row["final_classification"] == "RESEARCH_PROMISING" for row in families),
            "exploitable_edge": any(row["net_classification"] in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"} for row in families),
            "durable_edge": any(row["final_classification"] == "RESEARCH_PROMISING" and row["holdout_classification"] not in {"DATA_BLOCKED", ""} for row in families),
            "remaining_blockers": conclusion["remaining_blockers"],
            "next_research_only_recommendation": "Resolve exact/holdout/net-of-cost blockers before any confidence change or promotion discussion.",
        },
        "final_report": conclusion,
        "evidence_scorecard": scorecard,
        "family_final_assessment": families,
        "open_questions": open_questions,
        "confidence_impact_rule": "Confidence changes cite direct exact, holdout, and net-of-cost evidence only.",
        "authority_boundary": conclusion["authority_boundary"],
    }


def write_final_evidence_synthesis(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {"latest_json": out_dir / "latest.json", "latest_summary": out_dir / "latest_summary.md", "scorecard": out_dir / "evidence_scorecard.csv", "family": out_dir / "family_final_assessment.csv", "open": out_dir / "open_questions.csv"}
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_final_evidence_synthesis_summary(report), encoding="utf-8")
    _write_csv(paths["scorecard"], SCORECARD_COLUMNS, report.get("evidence_scorecard") or [])
    _write_csv(paths["family"], FAMILY_COLUMNS, report.get("family_final_assessment") or [])
    _write_csv(paths["open"], OPEN_COLUMNS, report.get("open_questions") or [])
    return paths


def render_final_evidence_synthesis_summary(report: dict[str, Any]) -> str:
    final = report.get("final_report", {})
    return "\n".join(["# Build 110 - Final Evidence Synthesis", "", f"Overall conclusion: {final.get('overall_conclusion')}", f"Strongest family: {final.get('strongest_family')}", f"Weakest family: {final.get('weakest_family')}", f"Confidence impact: {final.get('confidence_impact')}", f"Remaining blockers: {', '.join(final.get('remaining_blockers') or [])}", f"Authority boundary: {final.get('authority_boundary')}", "", "No trading authority, candidate promotion, production promotion, or trade recommendations.", ""])


def classify_final(exact: str, holdout: str, net: str) -> str:
    if "BLOCKED" in exact or holdout == "DATA_BLOCKED" or net == "NET_BLOCKED":
        return "RESEARCH_BLOCKED"
    if exact == "EXACT_NOT_REPEATABLE" or holdout in {"FAILED_HOLDOUT", "DEGRADED"} or net == "COST_ERODED":
        return "RESEARCH_REJECTED"
    if exact in {"EXACT_REPEATABLE_STRONG", "EXACT_REPEATABLE_WEAK"} and net in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}:
        return "RESEARCH_PROMISING"
    return "RESEARCH_WEAK"


def _family_assessments(exact: dict[str, Any], holdout: dict[str, Any], stability: dict[str, Any], net: dict[str, Any]) -> list[dict[str, str]]:
    ids = set()
    exact_by = {row.get("family_id", ""): row for row in exact.get("family_repeatability") or []}
    holdout_by = {row.get("family_id", ""): row for row in holdout.get("family_validations") or []}
    stability_by = {row.get("family_id", ""): row for row in stability.get("family_rows") or stability.get("family_stability_rankings") or []}
    net_by = {row.get("family_id", ""): row for row in net.get("family_results") or []}
    ids.update(exact_by)
    ids.update(holdout_by)
    ids.update(stability_by)
    ids.update(net_by)
    rows = []
    for family_id in sorted(item for item in ids if item):
        exact_class = exact_by.get(family_id, {}).get("family_classification", "")
        holdout_class = holdout_by.get(family_id, {}).get("classification", "")
        stability_class = stability_by.get(family_id, {}).get("classification", "")
        net_class = net_by.get(family_id, {}).get("family_classification", "")
        final = classify_final(exact_class, holdout_class, net_class)
        blockers = []
        if "BLOCKED" in exact_class:
            blockers.append("exact replay blocked")
        if holdout_class == "DATA_BLOCKED":
            blockers.append("holdout data blocked")
        if net_class in {"NET_BLOCKED", ""}:
            blockers.append("net-of-cost evidence blocked")
        rows.append({"family_id": family_id, "exact_classification": exact_class, "holdout_classification": holdout_class, "stability_classification": stability_class, "net_classification": net_class, "final_classification": final, "remaining_blockers": ";".join(blockers), "next_research_only_recommendation": _recommendation(final)})
    return rows


def _scorecard(exact: dict[str, Any], holdout: dict[str, Any], stability: dict[str, Any], net: dict[str, Any], lineage: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {"evidence_layer": "exact_replay", "source_report": "exact_replay_without_fallback/latest.json", "status": str((exact.get("summary") or {}).get("classification_counts", {})), "confidence_impact": str(exact.get("confidence_impact") or "NONE"), "summary": f"blocked={(exact.get('summary') or {}).get('exact_blocked')} run={(exact.get('summary') or {}).get('exact_replays_run')}"},
        {"evidence_layer": "holdout", "source_report": "holdout_replay_validation/latest.json", "status": str((holdout.get("summary") or {}).get("classification_counts", {})), "confidence_impact": str((holdout.get("summary") or {}).get("methodology_confidence_impact", "NONE")), "summary": f"survived={(holdout.get('summary') or {}).get('families_survived_holdout')} blocked={(holdout.get('summary') or {}).get('families_data_blocked')}"},
        {"evidence_layer": "stability", "source_report": "family_stability_analysis/latest.json", "status": str((stability.get("summary") or {}).get("classification_counts", {})), "confidence_impact": str(stability.get("confidence_impact") or "NONE"), "summary": f"families={(stability.get('summary') or {}).get('families_analyzed')}"},
        {"evidence_layer": "net_of_cost", "source_report": "net_of_cost_evidence/latest.json", "status": str((net.get("summary") or {})), "confidence_impact": str(net.get("confidence_impact") or "NONE"), "summary": f"cost_eroded={(net.get('summary') or {}).get('cost_eroded')}"},
        {"evidence_layer": "lineage", "source_report": "evidence_lineage_graph/evidence_lineage_graph.json", "status": "PRESENT" if lineage else "MISSING", "confidence_impact": "NONE", "summary": "Lineage is context evidence only for final synthesis."},
    ]


def _open_questions(families: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = []
    for i, row in enumerate([r for r in families if r.get("remaining_blockers")], start=1):
        rows.append({"question_id": f"open_{i:03d}", "family_id": row["family_id"], "question": "Can the remaining exact, holdout, and net-of-cost blockers be resolved with direct evidence?", "blocking_evidence_needed": row["remaining_blockers"]})
    return rows


def _overall(counts: Counter[str]) -> str:
    if counts["RESEARCH_PROMISING"]:
        return "RESEARCH_PROMISING"
    if counts["RESEARCH_WEAK"]:
        return "RESEARCH_WEAK"
    if counts["RESEARCH_REJECTED"]:
        return "RESEARCH_REJECTED"
    return "RESEARCH_BLOCKED"


def _confidence_impact(scorecard: list[dict[str, str]]) -> str:
    impacts = {row["confidence_impact"] for row in scorecard}
    return "SMALL_INCREASE" if "SMALL_INCREASE" in impacts else "NONE"


def _strongest(rows: list[dict[str, str]]) -> str:
    order = {"RESEARCH_PROMISING": 0, "RESEARCH_WEAK": 1, "RESEARCH_BLOCKED": 2, "RESEARCH_REJECTED": 3}
    return (sorted(rows, key=lambda row: (order.get(row["final_classification"], 9), row["family_id"])) or [{"family_id": "NONE"}])[0]["family_id"]


def _weakest(rows: list[dict[str, str]]) -> str:
    order = {"RESEARCH_REJECTED": 0, "RESEARCH_BLOCKED": 1, "RESEARCH_WEAK": 2, "RESEARCH_PROMISING": 3}
    return (sorted(rows, key=lambda row: (order.get(row["final_classification"], 9), row["family_id"])) or [{"family_id": "NONE"}])[0]["family_id"]


def _recommendation(final: str) -> str:
    if final == "RESEARCH_PROMISING":
        return "Continue research-only confirmation with additional direct exact and holdout evidence."
    if final == "RESEARCH_REJECTED":
        return "Archive or redesign hypothesis before further evidence spend."
    return "Resolve blockers before any confidence or promotion consideration."


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
