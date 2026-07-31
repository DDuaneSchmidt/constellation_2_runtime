from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1

REPORT_FAMILY = "aegis_research_portfolio_self_check_v1"
REPORT_FILENAME = "self_check.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def research_portfolio_self_check_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def research_portfolio_failures_v1(portfolio: Mapping[str, Any]) -> list[dict[str, Any]]:
    failures=[]
    thesis_ids={text_v1(row.get("thesis_id")) for row in portfolio.get("theses") or [] if isinstance(row, Mapping)}
    hypothesis_ids=[]
    hypothesis_to_thesis={}
    for row in portfolio.get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        hid=text_v1(row.get("hypothesis_id")); tid=text_v1(row.get("thesis_id"))
        hypothesis_ids.append(hid)
        if not hid:
            failures.append(_failure("orphan_hypothesis", "hypothesis missing hypothesis_id", row))
        if not tid or tid not in thesis_ids:
            failures.append(_failure("hypothesis_without_thesis", f"{hid} does not map to a known thesis", row))
        if hid in hypothesis_to_thesis and hypothesis_to_thesis[hid] != tid:
            failures.append(_failure("conflicting_thesis_mapping", f"{hid} maps to multiple thesis ids", row))
        hypothesis_to_thesis[hid]=tid
    seen=set()
    for hid in hypothesis_ids:
        if hid in seen:
            failures.append(_failure("duplicate_hypothesis_id", f"duplicate hypothesis_id {hid}", {}))
        seen.add(hid)
    for row in portfolio.get("sleeve_implementations") or []:
        if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")) not in hypothesis_to_thesis:
            failures.append(_failure("orphan_sleeve", f"sleeve {row.get('sleeve_id')} does not map to a hypothesis", row))
    for row in portfolio.get("candidate_hypothesis_links") or []:
        if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")) not in hypothesis_to_thesis:
            failures.append(_failure("orphan_candidate", f"candidate {row.get('candidate_id')} does not map to a hypothesis", row))
    for row in portfolio.get("paper_position_hypothesis_links") or []:
        if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")) not in hypothesis_to_thesis:
            failures.append(_failure("paper_position_missing_hypothesis", f"position {row.get('position_id')} does not inherit hypothesis linkage", row))
    for row in portfolio.get("research_allocation_recommendations") or []:
        if isinstance(row, Mapping) and not row.get("reason_codes"):
            failures.append(_failure("allocation_missing_reason_codes", f"allocation recommendation for {row.get('hypothesis_id')} lacks reason codes", row))
    return failures


def build_research_portfolio_self_check_v1(*, truth_root: Path | str, day_utc: str, portfolio: Mapping[str, Any] | None = None) -> dict[str, Any]:
    body=dict(portfolio or build_research_portfolio_v1(truth_root=truth_root, day_utc=day_utc))
    failures=research_portfolio_failures_v1(body)
    payload={"schema_id": "aegis_research_portfolio_self_check", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"), "ok": not failures, "failure_count": len(failures), "failures": failures, "checks": ["sleeve_maps_to_hypothesis", "hypothesis_maps_to_thesis", "candidate_maps_to_hypothesis", "paper_position_inherits_hypothesis", "validation_sample_maps_to_hypothesis", "no_orphan_hypotheses", "no_orphan_sleeves", "no_orphan_candidates", "no_conflicting_thesis_mappings", "no_duplicate_hypothesis_ids", "allocation_recommendations_have_reason_codes"], "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_research_portfolio_self_check_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(research_portfolio_self_check_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_research_portfolio_self_check_v1(truth_root=truth_root, day_utc=day_utc))


def _failure(code: str, message: str, row: Mapping[str, Any]) -> dict[str, Any]:
    return {"check_id": code, "message": message, "row_ref": {k: row.get(k) for k in ("thesis_id", "hypothesis_id", "sleeve_id", "candidate_id", "position_id") if row.get(k)}}
