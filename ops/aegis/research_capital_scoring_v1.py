from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_program_registry_v1 import ALLOCATION_TYPE, DISCLAIMER, build_research_program_registry_v1

REPORT_FAMILY = "aegis_research_capital_scoring_v1"
REPORT_FILENAME = "research_capital_scoring.v1.json"
MODEL_VERSION = "RESEARCH_CAPITAL_ALLOCATION_MODEL_V1"
COMPONENTS = ["candidate_yield_score", "validation_progress_score", "evidence_quality_score", "expected_value_signal_score", "time_to_decision_score", "diversification_value_score", "resource_efficiency_score", "staleness_dormancy_penalty", "duplication_penalty", "risk_drawdown_penalty"]
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False, "allocation_instructions_allowed": False}


def research_capital_scoring_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_research_capital_scoring_v1(*, truth_root: Path | str, day_utc: str, registry: Mapping[str, Any] | None = None) -> dict[str, Any]:
    reg = dict(registry or build_research_program_registry_v1(truth_root=truth_root, day_utc=day_utc))
    portfolio = read_json_v1(report_path_v1(truth_root, "aegis_research_portfolio_v1", day_utc, "research_portfolio.v1.json"))
    suff = read_json_v1(report_path_v1(truth_root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json"))
    lineage = read_json_v1(report_path_v1(truth_root, "aegis_evidence_lineage_integrity_v1", day_utc, "evidence_lineage_integrity.v1.json"))
    hyp_by_id = {text_v1(row.get("hypothesis_id")): row for row in portfolio.get("hypotheses") or [] if isinstance(row, Mapping)}
    suff_by_id = {text_v1(row.get("hypothesis_id")): row for row in suff.get("hypotheses") or [] if isinstance(row, Mapping)}
    rows=[]
    for program in reg.get("programs") or []:
        if not isinstance(program, Mapping):
            continue
        score = score_program_v1(program, hyp_by_id, suff_by_id, lineage)
        rows.append({"research_program_id": program.get("research_program_id"), "allocation_type": ALLOCATION_TYPE, **score})
    rows.sort(key=lambda r: str(r.get("research_program_id")))
    payload={"schema_id":"aegis_research_capital_scoring","schema_version":"v1","artifact_id":REPORT_FAMILY,"day_utc":str(day_utc),"generated_at":_now(),"model_version":MODEL_VERSION,"allocation_type":ALLOCATION_TYPE,"disclaimer":DISCLAIMER,"scores":rows,"summary":{"program_count":len(rows)},"source_artifact_paths":{"research_program_registry":str(research_program_registry_path_guess(truth_root, day_utc)),"research_portfolio":str(report_path_v1(truth_root,"aegis_research_portfolio_v1",day_utc,"research_portfolio.v1.json")),"statistical_sufficiency":str(report_path_v1(truth_root,"aegis_statistical_sufficiency_v1",day_utc,"statistical_sufficiency.v1.json")),"evidence_lineage_integrity":str(report_path_v1(truth_root,"aegis_evidence_lineage_integrity_v1",day_utc,"evidence_lineage_integrity.v1.json"))},"safety":dict(SAFETY),**SAFETY}
    payload["content_hash"]=stable_hash_v1({**payload,"generated_at":"","content_hash":""})
    return payload


def score_program_v1(program: Mapping[str, Any], hyp_by_id: Mapping[str, Mapping[str, Any]], suff_by_id: Mapping[str, Mapping[str, Any]], lineage: Mapping[str, Any] | None = None) -> dict[str, Any]:
    hids = [text_v1(h) for h in program.get("linked_hypothesis_ids") or []]
    hyps = [hyp_by_id.get(h, {}) for h in hids]
    suffs = [suff_by_id.get(h, {}) for h in hids]
    candidates = sum(len(h.get("linked_candidates") or []) for h in hyps)
    positions = sum(len(h.get("linked_paper_positions") or []) for h in hyps)
    usable = sum(int(s.get("usable_sample_count") or 0) for s in suffs)
    next_needed = min([int(s.get("next_evidence_needed") or 30) for s in suffs] or [30])
    lineage_panel = (lineage or {}).get("evidence_coverage_panel") or {}
    lineage_ok = all(float(lineage_panel.get(k, 0) or 0) >= 100 for k in ("candidate_coverage_pct", "sleeve_attribution_pct", "mark_coverage_pct", "validation_coverage_pct")) if lineage_panel else False
    closed_missing = usable == 0
    expectancy_values = [s.get("expectancy") for s in suffs if s.get("expectancy") is not None]
    drawdowns = [s.get("max_drawdown") for s in suffs if s.get("max_drawdown") is not None]
    candidate_yield = min(20.0, candidates * 0.5)
    validation_progress = min(20.0, usable * 2.0)
    evidence_quality = 20.0 if lineage_ok else 8.0
    expected_value = 0.0 if closed_missing else min(15.0, max(0.0, sum(float(v) for v in expectancy_values) * 100))
    ev_status = "INSUFFICIENT_OUTCOMES" if closed_missing else "AVAILABLE"
    time_to_decision = max(0.0, 10.0 - min(10.0, next_needed / 3.0))
    diversification = 10.0 if len(program.get("linked_sleeve_ids") or []) <= 1 else 8.0
    efficiency = min(10.0, (candidates + usable * 3) / max(1, int(program.get("current_allocation_units") or 1)))
    staleness_penalty = 8.0 if candidates == 0 and usable == 0 else 0.0
    duplication_penalty = 4.0 if len(program.get("linked_hypothesis_ids") or []) > 1 else 0.0
    risk_penalty = 0.0 if not drawdowns else min(10.0, abs(min(float(x) for x in drawdowns)) * 100)
    total = round(max(0.0, candidate_yield + validation_progress + evidence_quality + expected_value + time_to_decision + diversification + efficiency - staleness_penalty - duplication_penalty - risk_penalty), 6)
    components={"candidate_yield_score":candidate_yield,"validation_progress_score":validation_progress,"evidence_quality_score":evidence_quality,"expected_value_signal_score":expected_value,"expected_value_signal_status":ev_status,"time_to_decision_score":time_to_decision,"diversification_value_score":diversification,"resource_efficiency_score":efficiency,"staleness_dormancy_penalty":staleness_penalty,"duplication_penalty":duplication_penalty,"risk_drawdown_penalty":risk_penalty}
    return {"allocation_score":total,"score_components":components,"outcome_proof_status":ev_status,"closed_sample_count":usable,"component_model_version":MODEL_VERSION}


def write_research_capital_scoring_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(research_capital_scoring_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_research_capital_scoring_v1(truth_root=truth_root, day_utc=day_utc))


def research_program_registry_path_guess(root: Path | str, day: str) -> Path:
    return report_path_v1(root,"aegis_research_program_registry_v1",day,"research_program_registry.v1.json")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
