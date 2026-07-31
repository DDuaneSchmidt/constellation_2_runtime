from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1

REPORT_FAMILY = "aegis_research_program_registry_v1"
REPORT_FILENAME = "research_program_registry.v1.json"
ALLOCATION_TYPE = "RESEARCH_ATTENTION_ONLY"
DISCLAIMER = "This is not trade sizing. This is not live capital allocation. This is research-priority guidance only."
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False, "allocation_instructions_allowed": False}


def research_program_registry_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_research_program_registry_v1(*, truth_root: Path | str, day_utc: str, portfolio: Mapping[str, Any] | None = None) -> dict[str, Any]:
    body = dict(portfolio or build_research_portfolio_v1(truth_root=truth_root, day_utc=day_utc))
    thesis_by_id = {text_v1(row.get("thesis_id")): row for row in body.get("theses") or [] if isinstance(row, Mapping)}
    hypotheses = [row for row in body.get("hypotheses") or [] if isinstance(row, Mapping)]
    sleeves = [row for row in body.get("sleeve_implementations") or [] if isinstance(row, Mapping)]
    programs = []
    for thesis_id in sorted({text_v1(row.get("thesis_id")) for row in hypotheses if text_v1(row.get("thesis_id"))}):
        thesis = thesis_by_id.get(thesis_id, {})
        linked_h = sorted(text_v1(row.get("hypothesis_id")) for row in hypotheses if text_v1(row.get("thesis_id")) == thesis_id)
        linked_s = sorted({text_v1(row.get("sleeve_id")) for row in sleeves if text_v1(row.get("thesis_id")) == thesis_id and text_v1(row.get("sleeve_id"))})
        current_units = _current_units(linked_h, linked_s)
        programs.append({
            "research_program_id": "PROGRAM_" + thesis_id.replace("THESIS_", ""),
            "name": thesis.get("name") or thesis_id,
            "description": thesis.get("description") or "Research program derived from thesis grouping.",
            "linked_thesis_ids": [thesis_id],
            "linked_hypothesis_ids": linked_h,
            "linked_sleeve_ids": linked_s,
            "status": "ACTIVE" if linked_h else "PROPOSED",
            "current_allocation_units": current_units,
            "allocation_recommendation": "PENDING_SCORING",
            "allocation_score": None,
            "reason_codes": [],
            "allocation_type": ALLOCATION_TYPE,
            "disclaimer": DISCLAIMER,
        })
    payload = {"schema_id": "aegis_research_program_registry", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": _now(), "allocation_type": ALLOCATION_TYPE, "disclaimer": DISCLAIMER, "programs": programs, "summary": {"research_program_count": len(programs), "active_program_count": sum(1 for p in programs if p["status"] == "ACTIVE")}, "source_artifact_paths": {"research_portfolio": str(report_path_v1(truth_root, "aegis_research_portfolio_v1", day_utc, "research_portfolio.v1.json"))}, "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_research_program_registry_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(research_program_registry_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_research_program_registry_v1(truth_root=truth_root, day_utc=day_utc))


def _current_units(hypotheses: list[str], sleeves: list[str]) -> int:
    return max(1, min(10, len(hypotheses) + len(sleeves)))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
