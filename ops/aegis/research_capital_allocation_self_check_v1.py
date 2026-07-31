from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_allocation_decisions_v1 import VALID_RECOMMENDATIONS, build_research_allocation_decisions_v1
from ops.aegis.research_allocation_event_log_v1 import build_research_allocation_event_log_v1
from ops.aegis.research_capital_scoring_v1 import COMPONENTS, build_research_capital_scoring_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_program_registry_v1 import build_research_program_registry_v1

REPORT_FAMILY="aegis_research_capital_allocation_self_check_v1"
REPORT_FILENAME="self_check.v1.json"
SAFETY={"read_only":True,"trade_advice_allowed":False,"broker_execution_allowed":False,"autonomous_execution_allowed":False,"live_trading_allowed":False,"allocation_instructions_allowed":False}


def research_capital_allocation_self_check_path_v1(*, truth_root: Path|str, day_utc: str)->Path:
    return report_path_v1(truth_root,REPORT_FAMILY,day_utc,REPORT_FILENAME)


def research_capital_allocation_failures_v1(registry:Mapping[str,Any], scoring:Mapping[str,Any], decisions:Mapping[str,Any])->list[dict[str,Any]]:
    failures=[]; programs=[p for p in registry.get("programs") or [] if isinstance(p,Mapping)]
    ids=[text_v1(p.get("research_program_id")) for p in programs]
    for pid in ids:
        if ids.count(pid)>1: failures.append(_f("DUPLICATE_PROGRAM_ID",pid,{}))
    program_h={h for p in programs for h in (p.get("linked_hypothesis_ids") or [])}
    program_s={s for p in programs for s in (p.get("linked_sleeve_ids") or [])}
    if not program_h: failures.append(_f("MISSING_RESEARCH_PROGRAM_MAPPING","no hypotheses mapped",{}))
    if not program_s: failures.append(_f("MISSING_RESEARCH_PROGRAM_MAPPING","no sleeves mapped",{}))
    for score in scoring.get("scores") or []:
        if not isinstance(score,Mapping): continue
        comp=score.get("score_components") or {}
        for c in COMPONENTS:
            if c not in comp: failures.append(_f("MISSING_SCORE_COMPONENT",c,score))
        if score.get("closed_sample_count",0)==0 and comp.get("expected_value_signal_status")!="INSUFFICIENT_OUTCOMES": failures.append(_f("OUTCOME_PROOF_OVERCLAIM","missing insufficient outcome status",score))
        if score.get("closed_sample_count",0)==0 and float(comp.get("expected_value_signal_score") or 0)>0: failures.append(_f("OUTCOME_PROOF_OVERCLAIM","positive EV from missing outcomes",score))
    for d in decisions.get("decisions") or []:
        if not isinstance(d,Mapping): continue
        if not d.get("reason_codes"): failures.append(_f("MISSING_REASON_CODES","decision lacks reason codes",d))
        if not d.get("supporting_artifacts"): failures.append(_f("MISSING_SOURCE_ARTIFACTS","decision lacks supporting artifacts",d))
        if d.get("recommendation") not in VALID_RECOMMENDATIONS: failures.append(_f("INVALID_RECOMMENDATION",str(d.get("recommendation")),d))
        if "DATA_BLOCKED" in (d.get("reason_codes") or []) and d.get("recommendation")=="INCREASE": failures.append(_f("DATA_BLOCKED_PROGRAM_INCREASED","data blocked increased",d))
        if any(x in (d.get("reason_codes") or []) for x in ["RETIREMENT_THRESHOLD_MET","VALIDATION_FAILURE"]) and d.get("recommendation")=="INCREASE": failures.append(_f("RETIRED_PROGRAM_INCREASED","retired/disproven increased",d))
        if d.get("recommendation")=="INCREASE" and "INSUFFICIENT_CLOSED_OUTCOMES" in (d.get("reason_codes") or []): failures.append(_f("OUTCOME_PROOF_OVERCLAIM","increase with insufficient closed outcomes",d))
    return failures


def build_research_capital_allocation_self_check_v1(*, truth_root: Path|str, day_utc: str, registry:Mapping[str,Any]|None=None, scoring:Mapping[str,Any]|None=None, decisions:Mapping[str,Any]|None=None)->dict[str,Any]:
    reg=dict(registry or build_research_program_registry_v1(truth_root=truth_root,day_utc=day_utc)); score=dict(scoring or build_research_capital_scoring_v1(truth_root=truth_root,day_utc=day_utc,registry=reg)); dec=dict(decisions or build_research_allocation_decisions_v1(truth_root=truth_root,day_utc=day_utc,registry=reg,scoring=score))
    failures=research_capital_allocation_failures_v1(reg,score,dec)
    payload={"schema_id":"aegis_research_capital_allocation_self_check","schema_version":"v1","artifact_id":REPORT_FAMILY,"day_utc":str(day_utc),"generated_at":_now(),"ok":not failures,"failure_count":len(failures),"failures":failures,"checks":["program_mappings","reason_codes","source_artifacts","valid_recommendations","score_components","outcome_proof_guard","retired_no_increase","data_blocked_no_increase","deterministic_model","duplicate_program_ids","no_black_box_ai"],"safety":dict(SAFETY),**SAFETY}
    payload["content_hash"]=stable_hash_v1({**payload,"generated_at":"","content_hash":""})
    return payload


def write_research_capital_allocation_self_check_v1(*, truth_root: Path|str, day_utc: str, payload:dict[str,Any]|None=None)->Path:
    return write_json_v1(research_capital_allocation_self_check_path_v1(truth_root=truth_root,day_utc=day_utc), payload or build_research_capital_allocation_self_check_v1(truth_root=truth_root,day_utc=day_utc))


def _f(code:str,msg:str,row:Mapping[str,Any])->dict[str,Any]:
    return {"failure_code":code,"message":msg,"row_ref":{k:row.get(k) for k in ["research_program_id","allocation_decision_id"] if row.get(k)}}


def _now()->str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
