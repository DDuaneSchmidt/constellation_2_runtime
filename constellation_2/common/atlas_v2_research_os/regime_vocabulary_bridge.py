from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "regime_vocabulary_bridge"
NO_EXECUTABLE_REGIME_EQUIVALENT = "NO_EXECUTABLE_REGIME_EQUIVALENT"

RESEARCH_TO_REPLAY = {
    "CHOP": "RANGE_BOUND",
    "RANGE_BOUND": "RANGE_BOUND",
    "TRENDING": "TRENDING",
    "UNKNOWN": "UNKNOWN",
    "HIGH_VOL": "HIGH_VOLATILITY",
    "HIGH_VOLATILITY": "HIGH_VOLATILITY",
    "LOW_VOL": "LOW_VOLATILITY",
    "LOW_VOLATILITY": "LOW_VOLATILITY",
}

REPLAY_TO_RESEARCH = {
    "RANGE_BOUND": "CHOP",
    "TRENDING": "TRENDING",
    "UNKNOWN": "UNKNOWN",
    "HIGH_VOLATILITY": "HIGH_VOL",
    "LOW_VOLATILITY": "LOW_VOL",
}

DECLARED_RESEARCH_REGIMES = (
    "CHOP",
    "RANGE_BOUND",
    "TRENDING",
    "UNKNOWN",
    "HIGH_VOL",
    "LOW_VOL",
    "BULL",
    "BEAR",
    "RISK_ON",
    "RISK_OFF",
    "VOL_EXPANSION",
    "VOL_CONTRACTION",
)

EXECUTABLE_REPLAY_REGIMES = {"RANGE_BOUND", "TRENDING", "UNKNOWN", "HIGH_VOLATILITY", "LOW_VOLATILITY"}

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "regime_vocabulary_mapping_allowed": True,
    "filter_loosening_authorized": False,
    "trade_recommendation_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def map_research_regime_to_replay_regime(regime: Any) -> str:
    normalized = _norm(regime)
    mapped = RESEARCH_TO_REPLAY.get(normalized)
    if mapped and mapped in EXECUTABLE_REPLAY_REGIMES:
        return mapped
    return NO_EXECUTABLE_REGIME_EQUIVALENT


def map_replay_regime_to_research_regime(regime: Any) -> str:
    normalized = _norm(regime)
    return REPLAY_TO_RESEARCH.get(normalized, NO_EXECUTABLE_REGIME_EQUIVALENT)


def explain_regime_mapping(regime: Any, *, direction: str = "research_to_replay") -> dict[str, Any]:
    normalized = _norm(regime)
    if direction == "replay_to_research":
        mapped = map_replay_regime_to_research_regime(normalized)
    else:
        mapped = map_research_regime_to_replay_regime(normalized)
    accepted = mapped != NO_EXECUTABLE_REGIME_EQUIVALENT
    return {
        "direction": direction,
        "input_regime": normalized,
        "mapped_regime": mapped,
        "accepted": accepted,
        "status": "MAPPED" if accepted else NO_EXECUTABLE_REGIME_EQUIVALENT,
        "rationale": _rationale(normalized, mapped, direction=direction),
        "auditable": True,
    }


def validate_regime_mapping_is_auditable(mapping: dict[str, Any] | None = None) -> bool:
    row = mapping or {}
    required = {"direction", "input_regime", "mapped_regime", "accepted", "status", "rationale", "auditable"}
    return required.issubset(row) and bool(row.get("rationale")) and row.get("auditable") is True


def mapped_replay_regimes(regimes: list[Any]) -> tuple[list[str], list[dict[str, Any]]]:
    explanations = [explain_regime_mapping(regime) for regime in regimes]
    accepted = sorted({row["mapped_regime"] for row in explanations if row["accepted"]})
    return accepted, explanations


def run_regime_vocabulary_bridge_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_regime_vocabulary_bridge_report(root=root, created_at=created_at)
    write_regime_vocabulary_bridge_report(report, root=root)
    return report


def build_regime_vocabulary_bridge_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    before = _read_json(root_path / "direct_replay_zero_sample_diagnosis" / "latest.json", {})
    direct = _read_json(root_path / "direct_candidate_data_validation" / "latest.json", {})
    holdout = _read_json(root_path / "holdout_replay_validation" / "latest.json", {})
    candidate_rows = []
    for candidate_id in ("ptc_backtest_final_469607b8340421b7", "ptc_backtest_final_3a4ac24107c77136"):
        before_row = _find(before.get("candidate_diagnoses", []), candidate_id)
        direct_row = _find(direct.get("candidate_validations", []), candidate_id)
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                "before_sample_size": _pre_bridge_sample_size(before_row),
                "after_sample_size": (direct_row.get("direct_result") or {}).get("sample_size"),
                "before_first_failing_filter": _pre_bridge_first_failing_filter(before_row),
                "after_direct_validation_result": direct_row.get("classification"),
                "after_direct_result": direct_row.get("direct_result"),
                "regime_mapping": (direct_row.get("direct_result") or {}).get("regime_mapping") or direct_row.get("regime_mapping"),
            }
        )
    mapping_rows = [explain_regime_mapping(regime) for regime in DECLARED_RESEARCH_REGIMES]
    rejected = [row for row in mapping_rows if not row["accepted"]]
    applied = [row for row in mapping_rows if row["accepted"]]
    families_no_longer_data_blocked = _families_no_longer_data_blocked(root_path, direct)
    report = {
        "schema_id": "atlas_v2_research_os_regime_vocabulary_bridge",
        "schema_version": "1.0",
        "report_type": "REGIME_VOCABULARY_BRIDGE",
        "created_at": created,
        "day": created[:10],
        "mappings_applied": applied,
        "mappings_rejected": rejected,
        "candidate_impacts": candidate_rows,
        "holdout_validation_impact": {
            "classification_counts": (holdout.get("summary") or {}).get("classification_counts", {}),
            "methodology_confidence_impact": (holdout.get("summary") or {}).get("methodology_confidence_impact"),
            "families_data_blocked": (holdout.get("summary") or {}).get("families_data_blocked", []),
        },
        "families_no_longer_data_blocked": families_no_longer_data_blocked,
        "remaining_blockers": _remaining_blockers(candidate_rows, holdout),
        "summary": {
            "mappings_applied_count": len(applied),
            "mappings_rejected_count": len(rejected),
            "candidate_1_before_sample_size": candidate_rows[0]["before_sample_size"],
            "candidate_1_after_sample_size": candidate_rows[0]["after_sample_size"],
            "candidate_2_before_sample_size": candidate_rows[1]["before_sample_size"],
            "candidate_2_after_sample_size": candidate_rows[1]["after_sample_size"],
            "direct_validation_result_counts": dict(Counter(row.get("after_direct_validation_result") for row in candidate_rows)),
            "families_no_longer_data_blocked_count": len(families_no_longer_data_blocked),
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Explicit vocabulary bridge only.",
            "No blind filter loosening.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }
    return report


def write_regime_vocabulary_bridge_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "regime_vocabulary_bridge_report.json"
    summary_path = out_dir / "regime_vocabulary_bridge_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_regime_vocabulary_bridge_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_regime_vocabulary_bridge_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Regime Vocabulary Bridge",
        "",
        f"Mappings applied: {len(report.get('mappings_applied') or [])}",
        f"Mappings rejected: {len(report.get('mappings_rejected') or [])}",
        "",
        "## Candidate Impact",
    ]
    for row in report.get("candidate_impacts") or []:
        lines.append(
            f"- {row.get('candidate_id')}: sample_size {row.get('before_sample_size')} -> {row.get('after_sample_size')}; direct={row.get('after_direct_validation_result')}"
        )
    lines.extend(
        [
            "",
            "## Rejected Mappings",
        ]
    )
    for row in report.get("mappings_rejected") or []:
        lines.append(f"- {row.get('input_regime')}: {row.get('status')}")
    lines.extend(
        [
            "",
            f"Families no longer data-blocked: {', '.join(report.get('families_no_longer_data_blocked') or []) or 'none'}",
            f"Remaining blockers: {json.dumps(report.get('remaining_blockers') or [], sort_keys=True)}",
            "",
            "Authority: research-only vocabulary bridge; no live/capital/broker/position-sizing authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _rationale(normalized: str, mapped: str, *, direction: str) -> str:
    if mapped == NO_EXECUTABLE_REGIME_EQUIVALENT:
        return f"{normalized} has no explicit executable {direction} equivalent in the current bridge."
    if normalized == "CHOP" and mapped == "RANGE_BOUND":
        return "Research CHOP is treated as executable RANGE_BOUND because direct replay labels sideways regimes as RANGE_BOUND."
    if normalized == "RANGE_BOUND" and direction == "replay_to_research":
        return "Executable RANGE_BOUND maps back to research CHOP for reporting parity."
    return f"{normalized} has an explicit one-step bridge mapping to {mapped}."


def _families_no_longer_data_blocked(root: Path, direct: dict[str, Any]) -> list[str]:
    discovery = _read_json(root / "candidate_family_discovery" / "latest.json", {})
    family_by_candidate: dict[str, str] = {}
    for row in discovery.get("candidate_families", []) or discovery.get("families", []) or []:
        family_id = row.get("family_id")
        for candidate_id in row.get("candidate_ids") or row.get("top_candidate_ids") or []:
            if family_id:
                family_by_candidate[candidate_id] = family_id
        best = row.get("best_candidate") if isinstance(row.get("best_candidate"), dict) else {}
        if family_id and best.get("candidate_id"):
            family_by_candidate[best["candidate_id"]] = family_id
    families = []
    for row in direct.get("candidate_validations", []) or []:
        if row.get("classification") == "CONFIRMED":
            family_id = family_by_candidate.get(row.get("candidate_id"))
            if family_id:
                families.append(family_id)
    return sorted(set(families))



def _pre_bridge_sample_size(before_row: dict[str, Any]) -> Any:
    rule = before_row.get("candidate_rule_parsed") or {}
    raw_allowed = {str(value).upper() for value in rule.get("allowed_regimes") or []}
    observed = before_row.get("observed_regime_counts_before_regime_filter") or {}
    if raw_allowed and raw_allowed != {"UNKNOWN"}:
        return sum(int(count or 0) for regime, count in observed.items() if str(regime).upper() in raw_allowed)
    return before_row.get("final_sample_size")


def _pre_bridge_first_failing_filter(before_row: dict[str, Any]) -> str | None:
    trigger_count = ((before_row.get("step_counts") or {}).get("trigger_events_before_filters") or 0)
    if trigger_count and _pre_bridge_sample_size(before_row) == 0:
        return "regime_filter"
    return before_row.get("first_failing_filter")

def _remaining_blockers(candidate_rows: list[dict[str, Any]], holdout: dict[str, Any]) -> list[dict[str, Any]]:
    blockers = []
    for row in candidate_rows:
        result = row.get("after_direct_result") or {}
        if result.get("classification") != "BACKTEST_SUPPORTED":
            blockers.append({"candidate_id": row.get("candidate_id"), "blocker": result.get("missing_data") or "direct validation not confirmed"})
    for family_id in (holdout.get("summary") or {}).get("families_data_blocked", [])[:12]:
        blockers.append({"family_id": family_id, "blocker": "holdout remains data-blocked"})
    return blockers


def _find(rows: list[dict[str, Any]], candidate_id: str) -> dict[str, Any]:
    return next((row for row in rows if row.get("candidate_id") == candidate_id), {})


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
