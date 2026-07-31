from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_replay_validation import (
    AUTHORITY_BOUNDARY as HOLDOUT_AUTHORITY_BOUNDARY,
    _extract_event_rows,
    _load_sources,
    _matching_rows,
    _read_json,
    _return_value,
    _select_target_families,
)
from .regime_vocabulary_bridge import NO_EXECUTABLE_REGIME_EQUIVALENT, map_research_regime_to_replay_regime

REPORT_DIRNAME = "holdout_readiness_audit"
READINESS_STATUSES = ("READY", "PARTIALLY_READY", "BLOCKED_DATA", "BLOCKED_SCHEMA", "BLOCKED_LOGIC")
MATRIX_COLUMNS = [
    "family_id",
    "candidate_count",
    "holdout_rows_present",
    "return_observed_present",
    "timestamp_present",
    "symbol_present",
    "regime_present",
    "event_id_present",
    "split_date_present",
    "parser_supported",
    "readiness_status",
]
BLOCKER_COLUMNS = [
    "priority",
    "blocker_id",
    "family_id",
    "readiness_status",
    "blocker_type",
    "reason",
    "evidence",
    "confidence_impact",
]

AUTHORITY_BOUNDARY = {
    **HOLDOUT_AUTHORITY_BOUNDARY,
    "holdout_replay_validation_allowed": False,
    "research_only": True,
    "confidence_impact": "NONE",
}


def run_holdout_readiness_audit(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_readiness_audit(root=root, created_at=created_at)
    write_holdout_readiness_audit(report, root=root)
    return report


def build_holdout_readiness_audit(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    families = _select_target_families(sources)
    event_sources = _load_event_sources(root_path)
    event_rows = [row for source in event_sources for row in source["rows"]]
    matrix = [_audit_family(family, event_rows) for family in families]
    blockers = _build_blockers(matrix, event_sources)
    summary = _summary(matrix, blockers, event_sources)
    report = {
        "schema_id": "atlas_v2_research_os_holdout_readiness_audit_v1",
        "schema_version": "1.0",
        "build": "092",
        "report_type": "HOLDOUT_READINESS_AUDIT",
        "created_at": created,
        "day": created[:10],
        "research_only": True,
        "confidence_impact": "NONE",
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "source_reports": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "event_sources": [
            {"name": source["name"], "path": source["path"], "exists": source["exists"], "row_count": len(source["rows"])}
            for source in event_sources
        ],
        "target_selection_policy": {
            "same_as_holdout_replay_validation": True,
            "family_definition_frozen": True,
            "holdout_validation_performed": False,
            "threshold_retuning_allowed": False,
            "promotion_authority_emitted": False,
        },
        "readiness_matrix": matrix,
        "blockers": blockers,
        "ranked_blocker_summary": _ranked_blocker_summary(blockers),
        "summary": summary,
        "repair_plan": _repair_plan(summary, blockers),
        "guardrails": [
            "Research-only readiness audit.",
            "Do not perform holdout validation.",
            "Do not modify candidate ranking, qualification, promotion, paper trading, or production systems.",
            "Confidence impact is always NONE.",
            "No promotion authority is emitted.",
        ],
    }
    _validate_report(report)
    return report


def write_holdout_readiness_audit(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "holdout_readiness_audit.json"
    summary_path = out_dir / "holdout_readiness_audit_summary.md"
    matrix_path = root_path / "holdout_readiness_matrix.csv"
    blockers_path = root_path / "holdout_blockers.csv"
    repair_path = root_path / "holdout_repair_plan.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"

    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_holdout_readiness_summary(report)
    repair_plan = render_holdout_repair_plan(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    _write_csv(matrix_path, MATRIX_COLUMNS, report.get("readiness_matrix") or [])
    _write_csv(blockers_path, BLOCKER_COLUMNS, report.get("blockers") or [])
    repair_path.write_text(repair_plan, encoding="utf-8")
    return {
        "json": json_path,
        "summary": summary_path,
        "latest_json": latest_json,
        "latest_summary": latest_summary,
        "matrix": matrix_path,
        "blockers": blockers_path,
        "repair_plan": repair_path,
    }


def render_holdout_readiness_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    counts = summary.get("readiness_counts", {})
    top_blockers = report.get("ranked_blocker_summary") or []
    lines = [
        "# Holdout Readiness Audit",
        "",
        f"Build: {report.get('build')}",
        f"Created: {report.get('created_at')}",
        f"Families audited: {summary.get('families_audited', 0)}",
        f"READY count: {counts.get('READY', 0)}",
        f"BLOCKED count: {summary.get('blocked_count', 0)}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        f"Promotion authority emitted: {str(summary.get('promotion_authority_emitted')).lower()}",
        "",
        "## Top Blockers",
        "",
    ]
    for row in top_blockers:
        lines.append(f"- {row.get('priority')}: {row.get('reason')} ({row.get('affected_family_count')} families)")
    lines.extend(["", "## Family Matrix", ""])
    for row in report.get("readiness_matrix") or []:
        lines.append(
            f"- {row.get('family_id')}: {row.get('readiness_status')} - {row.get('exact_blocker_reason')}"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Research-only.",
            "- Holdout validation was not performed.",
            "- No confidence increase.",
            "- No promotion, paper-trading, production, trade, broker, capital, or sizing authority.",
            "",
        ]
    )
    return "\n".join(lines)


def render_holdout_repair_plan(report: dict[str, Any]) -> str:
    lines = ["# Holdout Repair Plan", "", "Confidence impact: NONE", "", "## Ranked Repairs", ""]
    for item in report.get("repair_plan") or []:
        lines.append(f"- {item.get('priority')}: {item.get('recommended_repair')} Impact: {item.get('expected_effect')}")
    lines.extend(
        [
            "",
            "## Constraints",
            "",
            "- Repair data/schema only before rerunning this audit.",
            "- Do not alter candidate ranking, qualification, promotion, paper trading, or production systems.",
            "- Do not run holdout validation as part of repair.",
            "",
        ]
    )
    return "\n".join(lines)


def _audit_family(family: dict[str, Any], event_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = _matching_rows(family, event_rows)
    parser_supported = _parser_supported(family)
    flags = {
        "holdout_rows_present": bool(rows),
        "return_observed_present": any(_return_value(row) is not None for row in rows),
        "timestamp_present": any(_has_any(row, ("timestamp", "date", "event_date", "observed_at")) for row in rows),
        "symbol_present": any(_has_any(row, ("symbol", "ticker", "symbols")) for row in rows),
        "regime_present": any(_has_any(row, ("regime", "replay_regime", "regime_context")) for row in rows),
        "event_id_present": any(_has_any(row, ("event_id", "observation_id", "row_id", "replay_id")) for row in rows),
        "split_date_present": any(_has_any(row, ("split_date", "holdout_split_date")) for row in rows),
        "parser_supported": parser_supported,
    }
    status, reason = _classify_readiness(flags)
    return {
        "family_id": family.get("family_id"),
        "candidate_count": len(family.get("candidate_ids") or []),
        **flags,
        "readiness_status": status,
        "exact_blocker_reason": reason,
        "family_name": family.get("family_name"),
        "candidate_ids": list(family.get("candidate_ids") or []),
        "target_reasons": list(family.get("target_reasons") or []),
        "mechanism": family.get("mechanism"),
        "regime": family.get("regime"),
        "timeframes": list(family.get("timeframes") or []),
        "source_types": list(family.get("source_types") or []),
        "matched_event_row_count": len(rows),
        "confidence_impact": "NONE",
    }


def _classify_readiness(flags: dict[str, bool]) -> tuple[str, str]:
    if not flags["parser_supported"]:
        return "BLOCKED_LOGIC", "Frozen family definition cannot be mapped by the current holdout parser."
    if not flags["holdout_rows_present"]:
        return "BLOCKED_DATA", "No holdout event rows match this frozen family definition."
    required = [
        "return_observed_present",
        "timestamp_present",
        "symbol_present",
        "regime_present",
        "event_id_present",
    ]
    missing = [name for name in required if not flags[name]]
    if missing:
        return "BLOCKED_SCHEMA", "Matched holdout rows are missing required fields: " + ", ".join(missing) + "."
    if not flags["split_date_present"]:
        return "PARTIALLY_READY", "Matched rows have replay fields, but no explicit split_date; split must be derived from timestamps."
    return "READY", "Event-level holdout rows expose the required replay fields."


def _build_blockers(matrix: list[dict[str, Any]], event_sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    no_matching_rows = [row for row in matrix if not row["holdout_rows_present"]]
    total_rows = sum(len(source["rows"]) for source in event_sources)
    if no_matching_rows:
        blockers.append(
            {
                "priority": "P0",
                "blocker_id": "P0_NO_TARGETED_EVENT_LEVEL_HOLDOUT_ROWS",
                "family_id": "ALL" if len(no_matching_rows) == len(matrix) else "MULTIPLE",
                "readiness_status": "BLOCKED_DATA",
                "blocker_type": "DATA",
                "reason": "No targeted family has event-level holdout rows that match frozen family definitions.",
                "evidence": f"event_source_rows={total_rows}; families_without_rows={len(no_matching_rows)}",
                "confidence_impact": "NONE",
            }
        )
    for row in matrix:
        if row["readiness_status"] == "READY":
            continue
        if row["holdout_rows_present"]:
            missing = [column for column in MATRIX_COLUMNS if column.endswith("_present") and not row.get(column)]
            reason = row.get("exact_blocker_reason") or "Family is not holdout-ready."
            blockers.append(
                {
                    "priority": "P1",
                    "blocker_id": f"P1_{row['family_id']}_{row['readiness_status']}",
                    "family_id": row["family_id"],
                    "readiness_status": row["readiness_status"],
                    "blocker_type": "SCHEMA" if row["readiness_status"] == "BLOCKED_SCHEMA" else "DATA",
                    "reason": reason,
                    "evidence": "missing_fields=" + ",".join(missing),
                    "confidence_impact": "NONE",
                }
            )
        else:
            blockers.append(
                {
                    "priority": "P1",
                    "blocker_id": f"P1_{row['family_id']}_NO_MATCHING_ROWS",
                    "family_id": row["family_id"],
                    "readiness_status": row["readiness_status"],
                    "blocker_type": "DATA",
                    "reason": row.get("exact_blocker_reason"),
                    "evidence": f"candidate_count={row.get('candidate_count')}; matched_event_row_count=0",
                    "confidence_impact": "NONE",
                }
            )
        if row["readiness_status"] == "PARTIALLY_READY":
            blockers.append(
                {
                    "priority": "P2",
                    "blocker_id": f"P2_{row['family_id']}_SPLIT_DATE_DERIVED",
                    "family_id": row["family_id"],
                    "readiness_status": row["readiness_status"],
                    "blocker_type": "QUALITY",
                    "reason": "No explicit split_date weakens auditability of holdout partitioning.",
                    "evidence": "timestamp_present=true; split_date_present=false",
                    "confidence_impact": "NONE",
                }
            )
    return blockers


def _ranked_blocker_summary(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], set[str]] = {}
    for row in blockers:
        key = (str(row.get("priority")), str(row.get("reason")))
        grouped.setdefault(key, set()).add(str(row.get("family_id")))
    priority_order = {"P0": 0, "P1": 1, "P2": 2}
    return [
        {
            "priority": priority,
            "reason": reason,
            "affected_family_count": len({family for family in families if family not in {"ALL", "MULTIPLE"}}) or len(families),
            "confidence_impact": "NONE",
        }
        for (priority, reason), families in sorted(grouped.items(), key=lambda item: (priority_order.get(item[0][0], 9), item[0][1]))
    ]


def _repair_plan(summary: dict[str, Any], blockers: list[dict[str, Any]]) -> list[dict[str, str]]:
    plan = []
    if any(row.get("priority") == "P0" for row in blockers):
        plan.append(
            {
                "priority": "P0",
                "recommended_repair": "Create/import event-level holdout rows keyed to frozen family_id or candidate_id with dated return_observed outcomes.",
                "expected_effect": "Unblocks all-family holdout readiness checks without changing ranking, qualification, or promotion logic.",
                "confidence_impact": "NONE",
            }
        )
    if summary.get("blocked_count"):
        plan.append(
            {
                "priority": "P1",
                "recommended_repair": "Backfill required replay schema fields for each targeted family: return_observed, timestamp/date, symbol, regime, and event_id.",
                "expected_effect": "Converts family-level BLOCKED_DATA/BLOCKED_SCHEMA statuses toward READY.",
                "confidence_impact": "NONE",
            }
        )
    plan.append(
        {
            "priority": "P2",
            "recommended_repair": "Add explicit split_date or holdout_split_date to event rows after data exists.",
            "expected_effect": "Improves holdout partition auditability; does not increase confidence.",
            "confidence_impact": "NONE",
        }
    )
    return plan


def _summary(matrix: list[dict[str, Any]], blockers: list[dict[str, Any]], event_sources: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["readiness_status"] for row in matrix)
    return {
        "families_audited": len(matrix),
        "readiness_counts": {status: counts.get(status, 0) for status in READINESS_STATUSES},
        "ready_count": counts.get("READY", 0),
        "blocked_count": sum(counts.get(status, 0) for status in ("BLOCKED_DATA", "BLOCKED_SCHEMA", "BLOCKED_LOGIC")),
        "partially_ready_count": counts.get("PARTIALLY_READY", 0),
        "blocker_count": len(blockers),
        "p0_blocker_count": sum(row.get("priority") == "P0" for row in blockers),
        "p1_blocker_count": sum(row.get("priority") == "P1" for row in blockers),
        "p2_blocker_count": sum(row.get("priority") == "P2" for row in blockers),
        "event_source_rows": sum(len(source["rows"]) for source in event_sources),
        "confidence_impact": "NONE",
        "promotion_authority_emitted": False,
        "holdout_validation_performed": False,
    }


def _load_event_sources(root: Path) -> list[dict[str, Any]]:
    sources = []
    for dirname in ("holdout_replay_events", "family_holdout_replay_events", "historical_replay"):
        path = root / dirname / "latest.json"
        payload = _read_json(path, {}) if path.exists() else {}
        sources.append(
            {
                "name": dirname,
                "path": str(path),
                "exists": path.exists(),
                "rows": _extract_event_rows(payload),
            }
        )
    return sources


def _parser_supported(family: dict[str, Any]) -> bool:
    regime = str(family.get("regime") or "").upper()
    mapped_regime = map_research_regime_to_replay_regime(regime) if regime else ""
    return bool(
        family.get("family_id")
        and family.get("mechanism")
        and family.get("timeframes")
        and mapped_regime != NO_EXECUTABLE_REGIME_EQUIVALENT
    )


def _has_any(row: dict[str, Any], keys: tuple[str, ...]) -> bool:
    return any(row.get(key) not in (None, "", []) for key in keys)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_report(report: dict[str, Any]) -> None:
    if report.get("confidence_impact") != "NONE":
        raise ValueError("Holdout readiness audit confidence impact must be NONE.")
    if report.get("summary", {}).get("confidence_impact") != "NONE":
        raise ValueError("Holdout readiness audit summary confidence impact must be NONE.")
    if report.get("target_selection_policy", {}).get("holdout_validation_performed"):
        raise ValueError("Holdout readiness audit must not perform holdout validation.")
    authority = report.get("authority_boundary", {})
    forbidden = [
        "trade_recommendation_authorized",
        "capital_authorized",
        "position_sizing_authorized",
        "broker_execution_authorized",
        "automatic_paper_trade_placement_authorized",
        "candidate_production_promotion_authorized",
        "confidence_increase_from_search_selection_allowed",
    ]
    if any(authority.get(key) for key in forbidden):
        raise ValueError("Holdout readiness audit emitted forbidden authority.")
    invalid_statuses = [row.get("readiness_status") for row in report.get("readiness_matrix") or [] if row.get("readiness_status") not in READINESS_STATUSES]
    if invalid_statuses:
        raise ValueError(f"Invalid readiness statuses: {invalid_statuses}")
    if any(row.get("confidence_impact") != "NONE" for row in report.get("blockers") or []):
        raise ValueError("All blockers must have confidence impact NONE.")
