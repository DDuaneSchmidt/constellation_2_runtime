from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.hypothesis_registry_v1 import build_hypothesis_registry_v1, write_hypothesis_registry_v1
from ops.aegis.hypothesis_state_machine_v1 import build_hypothesis_state_machine_v1, write_hypothesis_state_machine_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_allocation_score_v1 import build_research_allocation_v1, write_research_allocation_v1
from ops.aegis.intelligence_common_v1 import read_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1
from ops.aegis.research_thesis_registry_v1 import build_research_thesis_registry_v1, write_research_thesis_registry_v1

REPORT_FAMILY = "aegis_research_portfolio_v1"
REPORT_FILENAME = "research_portfolio.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False, "allocation_instructions_allowed": False}


def research_portfolio_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_research_portfolio_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    thesis_registry = build_research_thesis_registry_v1(truth_root=truth_root, day_utc=day_utc)
    hypothesis_registry = build_hypothesis_registry_v1(truth_root=truth_root, day_utc=day_utc)
    hypothesis_states = build_hypothesis_state_machine_v1(truth_root=truth_root, day_utc=day_utc, registry=hypothesis_registry)
    allocation = build_research_allocation_v1(truth_root=truth_root, day_utc=day_utc, registry=hypothesis_registry, states=hypothesis_states)
    state_by_h = {row.get("hypothesis_id"): row for row in hypothesis_states.get("hypothesis_states") or [] if isinstance(row, Mapping)}
    allocation_by_h = {row.get("hypothesis_id"): row for row in allocation.get("recommendations") or [] if isinstance(row, Mapping)}
    hypotheses=[]
    for row in hypothesis_registry.get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        hid=row.get("hypothesis_id")
        state_row=state_by_h.get(hid, {})
        allocation_row=allocation_by_h.get(hid, {})
        hypotheses.append({
            "hypothesis_id": hid,
            "thesis_id": row.get("thesis_id"),
            "name": row.get("name"),
            "state": state_row.get("state") or row.get("status"),
            "validation_state": row.get("validation_state"),
            "linked_sleeves": row.get("linked_sleeves") or [],
            "linked_candidates": row.get("linked_candidates") or [],
            "linked_paper_positions": row.get("linked_paper_positions") or [],
            "linked_outcomes": row.get("linked_outcomes") or [],
            "sample_count": len(row.get("linked_validation_samples") or []) + len(row.get("linked_validation_results") or []),
            "evidence_state": _evidence_state(row),
            "allocation_score": allocation_row.get("allocation_score"),
            "allocation_recommendation": allocation_row.get("allocation_recommendation"),
            "reason_codes": allocation_row.get("reason_codes") or [],
            "mapping_confidence": row.get("mapping_confidence"),
        })
    thesis_to_sleeves: dict[str, set[str]] = {}
    for row in hypothesis_registry.get("sleeve_implementations") or []:
        if isinstance(row, Mapping):
            thesis_to_sleeves.setdefault(str(row.get("thesis_id") or ""), set()).add(str(row.get("sleeve_id") or ""))
    summary = {
        "thesis_count": len(thesis_registry.get("theses") or []),
        "hypothesis_count": len(hypotheses),
        "validation_ready_hypotheses": sum(1 for row in hypotheses if row.get("state") == "VALIDATION_READY"),
        "validated_hypotheses": sum(1 for row in hypotheses if row.get("state") == "VALIDATED"),
        "degraded_hypotheses": sum(1 for row in hypotheses if row.get("state") == "DEGRADED"),
        "retired_hypotheses": sum(1 for row in hypotheses if row.get("state") == "RETIRED"),
        "active_hypotheses": sum(1 for row in hypotheses if row.get("state") not in {"RETIRED", "DISPROVEN"}),
    }
    outcome_maturity = _outcome_validation_maturity(truth_root, day_utc)
    research_capital_allocation = _research_capital_allocation(truth_root, day_utc)
    payload={
        "schema_id": "aegis_research_portfolio",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": _now(),
        "summary": summary,
        "active_theses": [row for row in thesis_registry.get("theses") or [] if row.get("status") in {"ACTIVE", "EXPANDING"}],
        "active_hypotheses": [row for row in hypotheses if row.get("state") not in {"RETIRED", "DISPROVEN"}],
        "validated_hypotheses": [row for row in hypotheses if row.get("state") == "VALIDATED"],
        "degraded_hypotheses": [row for row in hypotheses if row.get("state") == "DEGRADED"],
        "retired_hypotheses": [row for row in hypotheses if row.get("state") == "RETIRED"],
        "hypotheses": hypotheses,
        "theses": thesis_registry.get("theses") or [],
        "sleeve_implementations": hypothesis_registry.get("sleeve_implementations") or [],
        "hypothesis_to_sleeve_map": [{"hypothesis_id": row.get("hypothesis_id"), "thesis_id": row.get("thesis_id"), "linked_sleeves": row.get("linked_sleeves") or []} for row in hypotheses],
        "thesis_to_sleeve_map": [{"thesis_id": thesis_id, "linked_sleeves": sorted(s for s in sleeves if s)} for thesis_id, sleeves in sorted(thesis_to_sleeves.items()) if thesis_id],
        "research_allocation_recommendations": allocation.get("recommendations") or [],
        "outcome_validation_maturity": outcome_maturity,
        "research_capital_allocation": research_capital_allocation,
        "candidate_hypothesis_links": hypothesis_registry.get("candidate_hypothesis_links") or [],
        "paper_position_hypothesis_links": hypothesis_registry.get("paper_position_hypothesis_links") or [],
        "source_artifact_paths": {
            "thesis_registry": str(report_path_v1(truth_root, "aegis_research_thesis_registry_v1", day_utc, "thesis_registry.v1.json")),
            "hypothesis_registry": str(report_path_v1(truth_root, "aegis_hypothesis_registry_v1", day_utc, "hypothesis_registry.v1.json")),
            "hypothesis_states": str(report_path_v1(truth_root, "aegis_hypothesis_state_v1", day_utc, "hypothesis_states.v1.json")),
            "research_allocation": str(report_path_v1(truth_root, "aegis_research_allocation_v1", day_utc, "research_allocation.v1.json")),
            "outcome_registry": str(report_path_v1(truth_root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json")),
            "hypothesis_outcome_ledger": str(report_path_v1(truth_root, "aegis_hypothesis_outcome_ledger_v1", day_utc, "hypothesis_outcome_ledger.v1.json")),
            "validation_samples": str(report_path_v1(truth_root, "aegis_validation_samples_v1", day_utc, "validation_samples.v1.json")),
            "statistical_sufficiency": str(report_path_v1(truth_root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json")),
            "research_capital_allocation": str(report_path_v1(truth_root, "aegis_research_capital_allocation_v1", day_utc, "research_capital_allocation.v1.json")),
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_research_portfolio_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None, write_dependencies: bool = True) -> Path:
    if write_dependencies:
        thesis = build_research_thesis_registry_v1(truth_root=truth_root, day_utc=day_utc)
        write_research_thesis_registry_v1(truth_root=truth_root, day_utc=day_utc, payload=thesis)
        registry = build_hypothesis_registry_v1(truth_root=truth_root, day_utc=day_utc)
        write_hypothesis_registry_v1(truth_root=truth_root, day_utc=day_utc, payload=registry)
        states = build_hypothesis_state_machine_v1(truth_root=truth_root, day_utc=day_utc, registry=registry)
        write_hypothesis_state_machine_v1(truth_root=truth_root, day_utc=day_utc, payload=states)
        allocation = build_research_allocation_v1(truth_root=truth_root, day_utc=day_utc, registry=registry, states=states)
        write_research_allocation_v1(truth_root=truth_root, day_utc=day_utc, payload=allocation)
    body = payload or build_research_portfolio_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(research_portfolio_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def write_hypothesis_migration_report_v1(*, truth_root: Path | str, day_utc: str, output_path: Path | str = "docs/aegis_hypothesis_migration_report_v1.md") -> Path:
    portfolio = build_research_portfolio_v1(truth_root=truth_root, day_utc=day_utc)
    lines=["# Aegis Hypothesis Migration Report v1", "", f"Day UTC: `{day_utc}`", "", "Mappings are deterministic legacy inferences unless producer artifacts explicitly declare thesis/hypothesis IDs.", "", "## Migrated Sleeves", ""]
    for row in portfolio.get("sleeve_implementations") or []:
        lines.append(f"- `{row.get('sleeve_id')}` -> `{row.get('hypothesis_id')}` / `{row.get('thesis_id')}`; confidence: `{row.get('mapping_confidence')}`")
    lines += ["", "## Inferred Theses", ""]
    for row in portfolio.get("theses") or []:
        lines.append(f"- `{row.get('thesis_id')}`: {row.get('name')}")
    lines += ["", "## Inferred Hypotheses", ""]
    for row in portfolio.get("hypotheses") or []:
        lines.append(f"- `{row.get('hypothesis_id')}`: {row.get('name')}; sleeves: {', '.join(row.get('linked_sleeves') or []) or 'None'}; confidence: `{row.get('mapping_confidence')}`")
    lines += ["", "## Mappings Requiring Confirmation", "", "All `LEGACY_INFERRED` mappings should be source-declared by future sleeve/candidate/hypothesis producer artifacts before they are treated as fully authored research truth."]
    out=Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines).rstrip()+"\n", encoding="utf-8")
    return out



def _research_capital_allocation(truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    allocation = read_json_v1(report_path_v1(truth_root, "aegis_research_capital_allocation_v1", day_utc, "research_capital_allocation.v1.json"))
    score_by_program = {row.get("research_program_id"): row for row in allocation.get("scores") or [] if isinstance(row, Mapping)}
    rows = []
    for decision in allocation.get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        program_id = decision.get("research_program_id")
        score = score_by_program.get(program_id, {})
        components = score.get("score_components") or {}
        reasons = decision.get("reason_codes") or []
        primary_blocker = "None"
        for code in reasons:
            if code in {"INSUFFICIENT_CLOSED_OUTCOMES", "DATA_BLOCKED", "MARK_COVERAGE_BLOCKED", "LINEAGE_BLOCKED", "STATISTICAL_SUFFICIENCY_BLOCKED", "STALLED_RESEARCH"}:
                primary_blocker = code
                break
        rows.append({
            "research_program_id": program_id,
            "recommendation": decision.get("recommendation"),
            "allocation_score": decision.get("allocation_score"),
            "current_allocation_units": decision.get("prior_allocation_units"),
            "recommended_allocation_units": decision.get("recommended_allocation_units"),
            "allocation_delta": decision.get("allocation_delta"),
            "reason_codes": reasons,
            "primary_blocker": primary_blocker,
            "evidence_status": "STRONG" if float(components.get("evidence_quality_score") or 0) >= 15 else "WEAK_OR_MISSING",
            "validation_status": score.get("outcome_proof_status") or components.get("expected_value_signal_status") or "UNKNOWN",
            "allocation_type": decision.get("allocation_type"),
            "disclaimer": decision.get("disclaimer"),
            "linked_thesis_ids": [],
            "linked_hypothesis_ids": [],
            "linked_sleeve_ids": [],
        })
    program_by_id = {row.get("research_program_id"): row for row in allocation.get("programs") or [] if isinstance(row, Mapping)}
    for row in rows:
        program = program_by_id.get(row.get("research_program_id"), {})
        row["name"] = program.get("name")
        row["linked_thesis_ids"] = program.get("linked_thesis_ids") or []
        row["linked_hypothesis_ids"] = program.get("linked_hypothesis_ids") or []
        row["linked_sleeve_ids"] = program.get("linked_sleeve_ids") or []
    return {
        "available": bool(allocation),
        "summary": allocation.get("summary") or {},
        "allocation_type": allocation.get("allocation_type"),
        "disclaimer": allocation.get("disclaimer"),
        "programs": rows,
    }


def _outcome_validation_maturity(truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    outcome = read_json_v1(report_path_v1(truth_root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json"))
    ledger = read_json_v1(report_path_v1(truth_root, "aegis_hypothesis_outcome_ledger_v1", day_utc, "hypothesis_outcome_ledger.v1.json"))
    samples = read_json_v1(report_path_v1(truth_root, "aegis_validation_samples_v1", day_utc, "validation_samples.v1.json"))
    suff = read_json_v1(report_path_v1(truth_root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json"))
    ledger_by_h = {row.get("hypothesis_id"): row for row in ledger.get("hypotheses") or [] if isinstance(row, Mapping)}
    rows = []
    for row in suff.get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        lrow = ledger_by_h.get(row.get("hypothesis_id"), {})
        rows.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "thesis_id": row.get("thesis_id"),
            "closed_samples": row.get("usable_sample_count"),
            "required_samples": row.get("minimum_required_samples"),
            "validation_readiness_state": row.get("sufficiency_state"),
            "expectancy": row.get("expectancy"),
            "excess_return": row.get("excess_return"),
            "blocker_reasons": row.get("blocker_reasons") or [],
            "state_reason_codes": row.get("state_reason_codes") or [],
            "next_evidence_needed": row.get("next_evidence_needed"),
            "open_positions_count": lrow.get("open_positions_count"),
            "closed_positions_count": lrow.get("closed_positions_count"),
        })
    return {
        "available": bool(outcome or ledger or samples or suff),
        "summary": {
            "paper_position_count": (outcome.get("summary") or {}).get("paper_position_count", 0),
            "open_outcome_count": (outcome.get("summary") or {}).get("open_outcomes", 0),
            "closed_outcome_count": (outcome.get("summary") or {}).get("closed_outcomes", 0),
            "included_validation_sample_count": (samples.get("summary") or {}).get("included_samples", 0),
            "excluded_validation_sample_count": (samples.get("summary") or {}).get("excluded_samples", 0),
            "underpowered": (suff.get("summary") or {}).get("underpowered", 0),
            "accumulating": (suff.get("summary") or {}).get("accumulating", 0),
            "validation_ready": (suff.get("summary") or {}).get("validation_ready", 0),
            "validated": (suff.get("summary") or {}).get("validated", 0),
            "disproven": (suff.get("summary") or {}).get("disproven", 0),
        },
        "hypotheses": rows,
    }

def _evidence_state(row: Mapping[str, Any]) -> str:
    if row.get("linked_validation_results"):
        return "VALIDATION_RESULTS_PRESENT"
    if row.get("linked_validation_samples"):
        return "VALIDATION_SAMPLES_PRESENT"
    if row.get("linked_paper_positions"):
        return "PAPER_EVIDENCE_PRESENT"
    if row.get("linked_candidates"):
        return "CANDIDATE_EVIDENCE_PRESENT"
    return "NO_EVIDENCE_YET"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
