from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "evidence_review_board"

FINAL_CLASSIFICATIONS = (
    "RESEARCH_PROMISING",
    "RESEARCH_WEAK",
    "RESEARCH_REJECTED",
    "RESEARCH_INCONCLUSIVE",
)

SCORECARD_COLUMNS = [
    "evidence_layer",
    "source_report",
    "present",
    "status",
    "family_count",
    "support_count",
    "weak_count",
    "reject_count",
    "blocked_count",
    "confidence_impact",
    "summary",
]

FAMILY_COLUMNS = [
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "exact_replay",
    "holdout_status",
    "net_of_cost",
    "family_stability",
    "forward_observation",
    "lineage_status",
    "evidence_score",
    "signal_likely_present",
    "robust",
    "economically_meaningful",
    "further_research_justified",
    "final_classification",
    "review_notes",
]

QUESTION_COLUMNS = ["question_id", "family_id", "question", "blocking_evidence_needed", "source_layer", "priority"]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "confidence_impact": "NONE",
    "trade_recommendation_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
}


def run_evidence_review_board(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_evidence_review_board(root=root, created_at=created_at)
    write_evidence_review_board(report, root=root)
    return report


def build_evidence_review_board(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    families = _family_review(sources)
    scorecard = _scorecard(sources, families)
    unresolved = _unresolved_questions(families, scorecard)
    counts = Counter(row["final_classification"] for row in families)
    strongest = _strongest_family(families)
    weakest = _weakest_family(families)
    final_classification = _overall_classification(counts)
    required_questions = {
        "is_a_signal_likely_present": _answer_signal_present(families),
        "is_the_signal_robust": _answer_robust(families),
        "is_the_signal_economically_meaningful": _answer_economic(families),
        "is_further_research_justified": _answer_research_justified(families, final_classification),
        "strongest_surviving_family": strongest,
    }
    final_report = {
        "overall_conclusion": final_classification,
        "strongest_family": strongest,
        "weakest_family": weakest,
        "confidence_statement": _confidence_statement(families, scorecard, final_classification),
        "recommended_next_phase": _recommended_next_phase(final_classification, unresolved),
    }
    report = {
        "schema_id": "atlas_v2_research_os_evidence_review_board",
        "schema_version": "1.0",
        "report_type": "EVIDENCE_REVIEW_BOARD",
        "build": "119",
        "created_at": created,
        "day": created[:10],
        "final_classification_values": list(FINAL_CLASSIFICATIONS),
        "source_inputs": _source_manifest(sources),
        "required_questions": required_questions,
        "final_report": final_report,
        "summary": {
            "families_reviewed": len(families),
            "classification_counts": dict(counts),
            "evidence_layers_reviewed": len(scorecard),
            "unresolved_question_count": len(unresolved),
            "confidence_impact": "NONE",
        },
        "evidence_scorecard": scorecard,
        "family_review": families,
        "unresolved_questions": unresolved,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Formal evidence review only.",
            "No consumer invents truth; this report reads existing Atlas evidence artifacts.",
            "Blocked or missing evidence is recorded as unresolved, not converted into confidence.",
            "No trading, broker, capital, sizing, candidate-promotion, or production-promotion authority.",
        ],
    }
    _validate_report(report)
    return report


def write_evidence_review_board(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "scorecard": out_dir / "evidence_scorecard.csv",
        "family": out_dir / "family_review.csv",
        "unresolved": out_dir / "unresolved_questions.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_evidence_review_board_summary(report), encoding="utf-8")
    _write_csv(paths["scorecard"], SCORECARD_COLUMNS, report.get("evidence_scorecard") or [])
    _write_csv(paths["family"], FAMILY_COLUMNS, report.get("family_review") or [])
    _write_csv(paths["unresolved"], QUESTION_COLUMNS, report.get("unresolved_questions") or [])
    return paths


def render_evidence_review_board_summary(report: dict[str, Any]) -> str:
    final = report.get("final_report") or {}
    questions = report.get("required_questions") or {}
    summary = report.get("summary") or {}
    lines = [
        "# Build 119 - Evidence Review Board",
        "",
        f"Created: {report.get('created_at')}",
        f"Overall conclusion: {final.get('overall_conclusion')}",
        f"Strongest family: {final.get('strongest_family')}",
        f"Weakest family: {final.get('weakest_family')}",
        f"Confidence statement: {final.get('confidence_statement')}",
        f"Recommended next phase: {final.get('recommended_next_phase')}",
        "",
        "## Required Questions",
        "",
        f"- Is a signal likely present? {questions.get('is_a_signal_likely_present')}",
        f"- Is the signal robust? {questions.get('is_the_signal_robust')}",
        f"- Is the signal economically meaningful? {questions.get('is_the_signal_economically_meaningful')}",
        f"- Is further research justified? {questions.get('is_further_research_justified')}",
        f"- Strongest surviving family: {questions.get('strongest_surviving_family')}",
        "",
        "## Classification Counts",
        "",
    ]
    counts = summary.get("classification_counts") or {}
    for classification in FINAL_CLASSIFICATIONS:
        lines.append(f"- {classification}: {counts.get(classification, 0)}")
    lines.extend(["", "## Family Review", ""])
    for row in report.get("family_review") or []:
        lines.append(
            f"- {row.get('family_id')}: {row.get('final_classification')} score={row.get('evidence_score')} "
            f"exact={row.get('exact_replay')} holdout={row.get('holdout_status')} net={row.get('net_of_cost')} stability={row.get('family_stability')}"
        )
    lines.extend(["", "## Unresolved Questions", ""])
    for row in report.get("unresolved_questions") or []:
        lines.append(f"- {row.get('priority')} {row.get('family_id')}: {row.get('question')}")
    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            "Research-only. No trading authority, candidate promotion, production promotion, confidence change, capital allocation, position sizing, or broker execution.",
            "",
        ]
    )
    return "\n".join(lines)


def classify_family_review(exact: str, holdout: str, net: str, stability: str, forward: str = "", lineage: str = "") -> str:
    score = _evidence_score(exact, holdout, net, stability, forward, lineage)
    signal = _signal_present(exact, holdout, stability, forward)
    robust = _robust(exact, holdout, stability)
    economic = _economic(net)
    rejected = _rejected(exact, holdout, net, stability)
    if rejected and not (signal and economic):
        return "RESEARCH_REJECTED"
    if robust and economic and score >= 7:
        return "RESEARCH_PROMISING"
    if signal or economic or score >= 3:
        return "RESEARCH_WEAK"
    if rejected:
        return "RESEARCH_REJECTED"
    return "RESEARCH_INCONCLUSIVE"


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "exact_replay": root / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost": root / "net_of_cost_evidence" / "latest.json",
        "forward_observation": root / "forward_observation_starter" / "latest.json",
        "holdout_status": root / "holdout_replay_validation" / "latest.json",
        "holdout_readiness": root / "holdout_readiness_audit" / "latest.json",
        "family_stability": root / "family_stability_analysis" / "latest.json",
        "lineage_graph": root / "evidence_lineage_graph" / "evidence_lineage_graph.json",
    }
    return {name: {"path": path.as_posix(), "exists": path.exists(), "payload": _read_json(path, {})} for name, path in paths.items()}


def _source_manifest(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        name: {
            "path": source["path"],
            "exists": source["exists"],
            "report_type": source["payload"].get("report_type"),
            "schema_id": source["payload"].get("schema_id"),
        }
        for name, source in sources.items()
    }


def _family_review(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    exact_by = {row.get("family_id", ""): row for row in (sources["exact_replay"]["payload"].get("family_repeatability") or [])}
    holdout_by = {row.get("family_id", ""): row for row in (sources["holdout_status"]["payload"].get("family_validations") or [])}
    net_by = _best_net_by_family(sources["net_of_cost"]["payload"].get("family_results") or [])
    stability_by = {
        row.get("family_id", ""): row
        for row in (
            sources["family_stability"]["payload"].get("family_stability_rankings")
            or sources["family_stability"]["payload"].get("family_rows")
            or []
        )
    }
    forward_by = _forward_by_family(sources["forward_observation"]["payload"])
    lineage_by = {
        row.get("family_id", ""): row for row in (sources["lineage_graph"]["payload"].get("families") or [])
    }
    family_ids = set()
    for mapping in (exact_by, holdout_by, net_by, stability_by, forward_by, lineage_by):
        family_ids.update(k for k in mapping if k)
    rows = []
    for family_id in sorted(family_ids):
        stability = stability_by.get(family_id, {})
        holdout = holdout_by.get(family_id, {})
        exact = exact_by.get(family_id, {})
        net = net_by.get(family_id, {})
        forward = forward_by.get(family_id, {})
        lineage = lineage_by.get(family_id, {})
        exact_class = str(exact.get("family_classification") or "")
        holdout_class = str(holdout.get("classification") or "")
        net_class = str(net.get("family_classification") or "")
        stability_class = str(stability.get("classification") or "")
        forward_status = str(forward.get("forward_observation_status") or "")
        lineage_status = _lineage_status(lineage, sources["lineage_graph"]["exists"])
        score = _evidence_score(exact_class, holdout_class, net_class, stability_class, forward_status, lineage_status)
        signal = _signal_present(exact_class, holdout_class, stability_class, forward_status)
        robust = _robust(exact_class, holdout_class, stability_class)
        economic = _economic(net_class)
        final = classify_family_review(exact_class, holdout_class, net_class, stability_class, forward_status, lineage_status)
        rows.append(
            {
                "family_id": family_id,
                "mechanism": str(stability.get("mechanism") or (holdout.get("family_definition") or {}).get("mechanism") or ""),
                "regime": str(stability.get("regime") or (holdout.get("family_definition") or {}).get("regime") or ""),
                "timeframe": str(stability.get("timeframe") or ",".join(holdout.get("family_definition", {}).get("timeframes") or []) or ""),
                "exact_replay": exact_class or "MISSING",
                "holdout_status": holdout_class or "MISSING",
                "net_of_cost": net_class or "MISSING",
                "family_stability": stability_class or "MISSING",
                "forward_observation": forward_status or "MISSING",
                "lineage_status": lineage_status,
                "evidence_score": score,
                "signal_likely_present": signal,
                "robust": robust,
                "economically_meaningful": economic,
                "further_research_justified": final in {"RESEARCH_PROMISING", "RESEARCH_WEAK", "RESEARCH_INCONCLUSIVE"} and not _hard_rejected(exact_class, holdout_class, net_class, stability_class),
                "final_classification": final,
                "review_notes": _review_notes(exact_class, holdout_class, net_class, stability_class, forward_status, lineage_status),
            }
        )
    return rows


def _best_net_by_family(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    order = {"NET_SURVIVES_STRONG": 0, "NET_SURVIVES_WEAK": 1, "COST_ERODED": 2, "NET_FAILED": 3, "NET_INSUFFICIENT_SAMPLE": 4, "NET_BLOCKED": 5}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        family_id = str(row.get("family_id") or "")
        if not family_id:
            continue
        current = out.get(family_id)
        if current is None or order.get(str(row.get("family_classification") or ""), 9) < order.get(str(current.get("family_classification") or ""), 9):
            out[family_id] = row
    return out


def _forward_by_family(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("forward_observation_status") or []:
        family_id = str(row.get("family_id") or "")
        if family_id:
            out[family_id] = {"forward_observation_status": row.get("status_summary") or ""}
    target = payload.get("target_family") or {}
    summary = payload.get("summary") or {}
    family_id = str(target.get("family_id") or summary.get("target_family") or "")
    if family_id and family_id not in out:
        out[family_id] = {"forward_observation_status": summary.get("target_status") or ""}
    return out


def _lineage_status(row: dict[str, Any], graph_exists: bool) -> str:
    if not graph_exists:
        return "MISSING"
    if not row:
        return "NO_FAMILY_NODE"
    density = _float(row.get("evidence_density"))
    if density is not None and density > 0:
        return "LINEAGE_PRESENT"
    return "LINEAGE_SPARSE"


def _scorecard(sources: dict[str, dict[str, Any]], families: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exact = sources["exact_replay"]["payload"]
    holdout = sources["holdout_status"]["payload"]
    net = sources["net_of_cost"]["payload"]
    forward = sources["forward_observation"]["payload"]
    readiness = sources["holdout_readiness"]["payload"]
    stability = sources["family_stability"]["payload"]
    lineage = sources["lineage_graph"]["payload"]
    return [
        _layer_row("exact_replay", sources["exact_replay"], exact.get("summary") or {}, families, "exact_replay"),
        _layer_row("net_of_cost", sources["net_of_cost"], net.get("summary") or {}, families, "net_of_cost"),
        _layer_row("forward_observation", sources["forward_observation"], forward.get("summary") or {}, families, "forward_observation"),
        _layer_row("holdout_status", sources["holdout_status"], holdout.get("summary") or {}, families, "holdout_status"),
        _layer_row("holdout_readiness", sources["holdout_readiness"], readiness.get("summary") or {}, families, "holdout_status"),
        _layer_row("family_stability", sources["family_stability"], stability.get("summary") or {}, families, "family_stability"),
        _layer_row("lineage_graph", sources["lineage_graph"], lineage.get("summary") or {}, families, "lineage_status"),
    ]


def _layer_row(layer: str, source: dict[str, Any], summary: dict[str, Any], families: list[dict[str, Any]], family_field: str) -> dict[str, Any]:
    values = [str(row.get(family_field) or "") for row in families]
    support = sum(_supporting_value(value) for value in values)
    weak = sum(_weak_value(value) for value in values)
    reject = sum(_rejecting_value(value) for value in values)
    blocked = sum(value in {"MISSING", "DATA_BLOCKED", "NET_BLOCKED", "EXACT_BLOCKED_INSUFFICIENT_DATA", "INSUFFICIENT_HOLDOUT_DATA", "NO_FAMILY_NODE"} for value in values)
    return {
        "evidence_layer": layer,
        "source_report": source["path"],
        "present": bool(source["exists"]),
        "status": "PRESENT" if source["exists"] else "MISSING",
        "family_count": len([value for value in values if value and value != "MISSING"]),
        "support_count": support,
        "weak_count": weak,
        "reject_count": reject,
        "blocked_count": blocked,
        "confidence_impact": str(summary.get("confidence_impact") or summary.get("methodology_confidence_impact") or "NONE"),
        "summary": json.dumps(summary, sort_keys=True),
    }


def _unresolved_questions(families: list[dict[str, Any]], scorecard: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    missing_layers = [row["evidence_layer"] for row in scorecard if not row["present"]]
    for layer in missing_layers:
        rows.append(
            {
                "question_id": f"q{len(rows) + 1:03d}",
                "family_id": "ALL",
                "question": f"Can the missing {layer} evidence artifact be produced from governed Atlas inputs?",
                "blocking_evidence_needed": layer,
                "source_layer": layer,
                "priority": "P0",
            }
        )
    for family in families:
        blockers = []
        if family["exact_replay"] in {"MISSING", "EXACT_BLOCKED_INSUFFICIENT_DATA"}:
            blockers.append(("exact_replay", "Can exact replay be completed without fallback for this family?"))
        if family["holdout_status"] in {"MISSING", "DATA_BLOCKED", "INSUFFICIENT_HOLDOUT_DATA"}:
            blockers.append(("holdout_status", "Can holdout event rows prove whether this family survives unseen periods?"))
        if family["net_of_cost"] in {"MISSING", "NET_BLOCKED", "NET_INSUFFICIENT_SAMPLE"}:
            blockers.append(("net_of_cost", "Can net-of-cost economics be measured with sufficient sample size?"))
        if family["forward_observation"] in {"MISSING", "READY_NO_SIGNALS_OBSERVED"}:
            blockers.append(("forward_observation", "Can forward observation collect mature outcomes for this family?"))
        if family["lineage_status"] in {"MISSING", "NO_FAMILY_NODE", "LINEAGE_SPARSE"}:
            blockers.append(("lineage_graph", "Can lineage reconstruct the family from upstream Atlas evidence?"))
        for layer, question in blockers:
            rows.append(
                {
                    "question_id": f"q{len(rows) + 1:03d}",
                    "family_id": str(family["family_id"]),
                    "question": question,
                    "blocking_evidence_needed": str(family.get(layer if layer != "lineage_graph" else "lineage_status") or layer),
                    "source_layer": layer,
                    "priority": "P1",
                }
            )
    return rows


def _evidence_score(exact: str, holdout: str, net: str, stability: str, forward: str, lineage: str) -> int:
    return (
        _score_exact(exact)
        + _score_holdout(holdout)
        + _score_net(net)
        + _score_stability(stability)
        + _score_forward(forward)
        + _score_lineage(lineage)
    )


def _score_exact(value: str) -> int:
    return {"EXACT_REPEATABLE_STRONG": 3, "EXACT_REPEATABLE_WEAK": 2, "EXACT_PARTIALLY_REPEATABLE": 1, "EXACT_NOT_REPEATABLE": -3}.get(value, 0)


def _score_holdout(value: str) -> int:
    return {"HOLDOUT_SURVIVED": 3, "SURVIVED_HOLDOUT": 3, "HOLDOUT_WEAKENED": -1, "DEGRADED": -1, "HOLDOUT_FAILED": -3, "FAILED_HOLDOUT": -3}.get(value, 0)


def _score_net(value: str) -> int:
    return {"NET_SURVIVES_STRONG": 3, "NET_SURVIVES_WEAK": 2, "COST_ERODED": -2, "NET_FAILED": -3}.get(value, 0)


def _score_stability(value: str) -> int:
    return {"STABLE_STRONG": 3, "STABLE_WEAK": 2, "MIXED": 1, "UNSTABLE": -3}.get(value, 0)


def _score_forward(value: str) -> int:
    return {"OBSERVATION_QUEUE_ACTIVE": 1, "COMPLETED": 1}.get(value, 0)


def _score_lineage(value: str) -> int:
    return {"LINEAGE_PRESENT": 1, "LINEAGE_SPARSE": 0, "NO_FAMILY_NODE": -1, "MISSING": -1}.get(value, 0)


def _signal_present(exact: str, holdout: str, stability: str, forward: str) -> bool:
    return _score_exact(exact) > 0 or _score_holdout(holdout) > 0 or _score_stability(stability) > 0 or _score_forward(forward) > 0


def _robust(exact: str, holdout: str, stability: str) -> bool:
    return _score_exact(exact) >= 2 and _score_holdout(holdout) >= 3 and _score_stability(stability) >= 2


def _economic(net: str) -> bool:
    return net in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}


def _rejected(exact: str, holdout: str, net: str, stability: str) -> bool:
    return _score_exact(exact) <= -3 or _score_holdout(holdout) <= -3 or _score_net(net) <= -3 or _score_stability(stability) <= -3 or net == "COST_ERODED"


def _hard_rejected(exact: str, holdout: str, net: str, stability: str) -> bool:
    return _score_exact(exact) <= -3 or _score_holdout(holdout) <= -3 or _score_net(net) <= -3 or _score_stability(stability) <= -3


def _supporting_value(value: str) -> bool:
    return value in {"EXACT_REPEATABLE_STRONG", "EXACT_REPEATABLE_WEAK", "HOLDOUT_SURVIVED", "SURVIVED_HOLDOUT", "NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK", "STABLE_STRONG", "STABLE_WEAK", "OBSERVATION_QUEUE_ACTIVE", "LINEAGE_PRESENT"}


def _weak_value(value: str) -> bool:
    return value in {"EXACT_PARTIALLY_REPEATABLE", "HOLDOUT_WEAKENED", "DEGRADED", "MIXED", "READY_NO_SIGNALS_OBSERVED", "LINEAGE_SPARSE"}


def _rejecting_value(value: str) -> bool:
    return value in {"EXACT_NOT_REPEATABLE", "HOLDOUT_FAILED", "FAILED_HOLDOUT", "NET_FAILED", "COST_ERODED", "UNSTABLE"}


def _strongest_family(rows: list[dict[str, Any]]) -> str:
    order = {"RESEARCH_PROMISING": 0, "RESEARCH_WEAK": 1, "RESEARCH_INCONCLUSIVE": 2, "RESEARCH_REJECTED": 3}
    return (sorted(rows, key=lambda row: (order.get(row["final_classification"], 9), -int(row["evidence_score"]), row["family_id"])) or [{"family_id": "NONE"}])[0]["family_id"]


def _weakest_family(rows: list[dict[str, Any]]) -> str:
    order = {"RESEARCH_REJECTED": 0, "RESEARCH_INCONCLUSIVE": 1, "RESEARCH_WEAK": 2, "RESEARCH_PROMISING": 3}
    return (sorted(rows, key=lambda row: (order.get(row["final_classification"], 9), int(row["evidence_score"]), row["family_id"])) or [{"family_id": "NONE"}])[0]["family_id"]


def _overall_classification(counts: Counter[str]) -> str:
    if counts["RESEARCH_PROMISING"]:
        return "RESEARCH_PROMISING"
    if counts["RESEARCH_WEAK"]:
        return "RESEARCH_WEAK"
    if counts["RESEARCH_REJECTED"] and not counts["RESEARCH_INCONCLUSIVE"]:
        return "RESEARCH_REJECTED"
    return "RESEARCH_INCONCLUSIVE"


def _answer_signal_present(rows: list[dict[str, Any]]) -> str:
    return "YES" if any(row["signal_likely_present"] for row in rows) else "NOT_PROVEN"


def _answer_robust(rows: list[dict[str, Any]]) -> str:
    return "YES" if any(row["robust"] for row in rows) else "NOT_PROVEN"


def _answer_economic(rows: list[dict[str, Any]]) -> str:
    return "YES" if any(row["economically_meaningful"] for row in rows) else "NOT_PROVEN"


def _answer_research_justified(rows: list[dict[str, Any]], final_classification: str) -> str:
    if final_classification == "RESEARCH_REJECTED":
        return "NO"
    return "YES" if rows else "NOT_PROVEN"


def _confidence_statement(families: list[dict[str, Any]], scorecard: list[dict[str, Any]], final: str) -> str:
    missing = [row["evidence_layer"] for row in scorecard if not row["present"]]
    if missing:
        return f"{final} with limited confidence because missing evidence layers remain: {', '.join(missing)}. Confidence impact remains NONE."
    if any(row["final_classification"] == "RESEARCH_PROMISING" for row in families):
        return "Research evidence is directionally supportive, but authority remains research-only and confidence impact remains NONE."
    return "Evidence does not yet justify confidence increase; confidence impact remains NONE."


def _recommended_next_phase(final: str, unresolved: list[dict[str, str]]) -> str:
    if final == "RESEARCH_PROMISING":
        return "Prospective observation and holdout completion, research-only."
    if final == "RESEARCH_WEAK":
        return "Resolve P0/P1 evidence gaps before any promotion discussion."
    if final == "RESEARCH_REJECTED":
        return "Archive or redesign rejected families before more evidence spend."
    if unresolved:
        return "Produce missing governed evidence artifacts, then rerun the review board."
    return "No next phase identified."


def _review_notes(exact: str, holdout: str, net: str, stability: str, forward: str, lineage: str) -> str:
    notes = []
    if _signal_present(exact, holdout, stability, forward):
        notes.append("signal evidence present")
    if _robust(exact, holdout, stability):
        notes.append("robust across exact/holdout/stability")
    if _economic(net):
        notes.append("net-of-cost survives")
    if _rejected(exact, holdout, net, stability):
        notes.append("rejecting evidence present")
    if lineage in {"MISSING", "NO_FAMILY_NODE", "LINEAGE_SPARSE"}:
        notes.append("lineage incomplete")
    return "; ".join(notes) or "insufficient evidence"


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


def _float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _validate_report(report: dict[str, Any]) -> None:
    final = (report.get("final_report") or {}).get("overall_conclusion")
    if final not in FINAL_CLASSIFICATIONS:
        raise ValueError(f"invalid final classification: {final}")
    for row in report.get("family_review") or []:
        if row.get("final_classification") not in FINAL_CLASSIFICATIONS:
            raise ValueError(f"invalid family classification: {row.get('final_classification')}")
    boundary = report.get("authority_boundary") or {}
    if boundary.get("confidence_impact") != "NONE" or boundary.get("trade_recommendation_authorized") is not False:
        raise ValueError("evidence review board must remain research-only with confidence impact NONE")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
