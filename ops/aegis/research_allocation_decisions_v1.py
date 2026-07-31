from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_capital_scoring_v1 import MODEL_VERSION, build_research_capital_scoring_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_program_registry_v1 import ALLOCATION_TYPE, DISCLAIMER, build_research_program_registry_v1

REPORT_FAMILY="aegis_research_allocation_decisions_v1"
REPORT_FILENAME="research_allocation_decisions.v1.json"
VALID_RECOMMENDATIONS={"INCREASE","MAINTAIN","REDUCE","PAUSE","RETIRE","INVESTIGATE_MORE"}
SAFETY={"read_only":True,"trade_advice_allowed":False,"broker_execution_allowed":False,"autonomous_execution_allowed":False,"live_trading_allowed":False,"allocation_instructions_allowed":False}


def research_allocation_decisions_path_v1(*, truth_root: Path|str, day_utc: str)->Path:
    return report_path_v1(truth_root,REPORT_FAMILY,day_utc,REPORT_FILENAME)


def build_research_allocation_decisions_v1(*, truth_root: Path|str, day_utc: str, registry: Mapping[str,Any]|None=None, scoring: Mapping[str,Any]|None=None)->dict[str,Any]:
    reg=dict(registry or build_research_program_registry_v1(truth_root=truth_root,day_utc=day_utc))
    score_payload=dict(scoring or build_research_capital_scoring_v1(truth_root=truth_root,day_utc=day_utc,registry=reg))
    program_by_id={p.get("research_program_id"):p for p in reg.get("programs") or [] if isinstance(p,Mapping)}
    decisions=[]
    source_paths={**(reg.get("source_artifact_paths") or {}), **(score_payload.get("source_artifact_paths") or {})}
    supporting_artifacts=sorted(set(str(v) for v in source_paths.values() if v))
    source_hashes=_source_hashes(supporting_artifacts)
    for score in score_payload.get("scores") or []:
        if not isinstance(score,Mapping): continue
        pid=score.get("research_program_id"); program=program_by_id.get(pid,{})
        rec,reasons=_recommend(program,score)
        prior=int(program.get("current_allocation_units") or 1)
        delta={"INCREASE":1,"MAINTAIN":0,"REDUCE":-1,"PAUSE":-prior,"RETIRE":-prior,"INVESTIGATE_MORE":1}.get(rec,0)
        new=max(0, prior+delta)
        decisions.append({"allocation_decision_id":"decision_"+stable_hash_v1({"day":day_utc,"program":pid,"rec":rec})[:24],"day_utc":str(day_utc),"research_program_id":pid,"prior_allocation_units":prior,"recommended_allocation_units":new,"allocation_delta":new-prior,"recommendation":rec,"reason_codes":reasons,"allocation_score":score.get("allocation_score"),"supporting_artifacts":supporting_artifacts,"source_hashes":source_hashes,"model_version":MODEL_VERSION,"allocation_type":ALLOCATION_TYPE,"disclaimer":DISCLAIMER,"generated_at":_now()})
    decisions.sort(key=lambda r:str(r.get("research_program_id")))
    payload={"schema_id":"aegis_research_allocation_decisions","schema_version":"v1","artifact_id":REPORT_FAMILY,"day_utc":str(day_utc),"generated_at":_now(),"model_version":MODEL_VERSION,"allocation_type":ALLOCATION_TYPE,"disclaimer":DISCLAIMER,"decisions":decisions,"summary":{r:sum(1 for d in decisions if d["recommendation"]==r) for r in sorted(VALID_RECOMMENDATIONS)},"source_artifact_paths":source_paths,"safety":dict(SAFETY),**SAFETY}
    payload["summary"]["decision_count"]=len(decisions)
    payload["content_hash"]=stable_hash_v1({**payload,"generated_at":"","content_hash":"","decisions":[{**d,"generated_at":""} for d in decisions]})
    return payload


def _recommend(program:Mapping[str,Any], score:Mapping[str,Any])->tuple[str,list[str]]:
    comp=score.get("score_components") or {}; total=float(score.get("allocation_score") or 0); reasons=[]
    insufficient=comp.get("expected_value_signal_status")=="INSUFFICIENT_OUTCOMES"
    if insufficient: reasons.append("INSUFFICIENT_CLOSED_OUTCOMES")
    if comp.get("candidate_yield_score",0)>=10: reasons.append("HIGH_CANDIDATE_YIELD")
    else: reasons.append("LOW_CANDIDATE_YIELD")
    if comp.get("validation_progress_score",0)>=10: reasons.append("VALIDATION_PROGRESS_GOOD")
    if comp.get("evidence_quality_score",0)>=15: reasons.append("STRONG_EVIDENCE_QUALITY")
    else: reasons.append("WEAK_EVIDENCE_QUALITY")
    if comp.get("time_to_decision_score",0)>=6: reasons.append("NEAR_VALIDATION_READY")
    if comp.get("staleness_dormancy_penalty",0)>0: reasons.append("STALLED_RESEARCH")
    if comp.get("duplication_penalty",0)>0: reasons.append("DUPLICATIVE_WITH_HIGHER_CONFIDENCE_PROGRAM")
    if insufficient and total>=25 and comp.get("candidate_yield_score",0)>=10:
        reasons.append("UNDERPOWERED_BUT_PROMISING"); return "INVESTIGATE_MORE", reasons
    if total>=55 and not insufficient: return "INCREASE", reasons
    if total>=35: return "MAINTAIN", reasons
    if total>=20: return "REDUCE", reasons
    if comp.get("staleness_dormancy_penalty",0)>0: return "PAUSE", reasons
    return "INVESTIGATE_MORE", reasons



def _source_hashes(paths: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for value in paths:
        path = Path(value)
        if path.exists() and path.is_file():
            hashes[value] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            hashes[value] = "MISSING"
    return hashes

def write_research_allocation_decisions_v1(*, truth_root: Path|str, day_utc: str, payload:dict[str,Any]|None=None)->Path:
    return write_json_v1(research_allocation_decisions_path_v1(truth_root=truth_root,day_utc=day_utc), payload or build_research_allocation_decisions_v1(truth_root=truth_root,day_utc=day_utc))


def _now()->str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
