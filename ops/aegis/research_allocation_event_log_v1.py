from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_allocation_decisions_v1 import build_research_allocation_decisions_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1
from ops.aegis.research_program_registry_v1 import ALLOCATION_TYPE, DISCLAIMER

REPORT_FAMILY="aegis_research_allocation_event_log_v1"
REPORT_FILENAME="research_allocation_event_log.v1.json"
SAFETY={"read_only":True,"trade_advice_allowed":False,"broker_execution_allowed":False,"autonomous_execution_allowed":False,"live_trading_allowed":False,"allocation_instructions_allowed":False}


def research_allocation_event_log_path_v1(*, truth_root: Path|str, day_utc: str)->Path:
    return report_path_v1(truth_root,REPORT_FAMILY,day_utc,REPORT_FILENAME)


def build_research_allocation_event_log_v1(*, truth_root: Path|str, day_utc: str, decisions: Mapping[str,Any]|None=None)->dict[str,Any]:
    d=dict(decisions or build_research_allocation_decisions_v1(truth_root=truth_root,day_utc=day_utc))
    events=[]
    for row in d.get("decisions") or []:
        if not isinstance(row,Mapping): continue
        events.append({"event_id":"event_"+stable_hash_v1({"day":day_utc,"decision":row.get("allocation_decision_id")})[:24],"allocation_decision_id":row.get("allocation_decision_id"),"research_program_id":row.get("research_program_id"),"prior_recommendation":"UNSET","new_recommendation":row.get("recommendation"),"prior_units":row.get("prior_allocation_units"),"new_units":row.get("recommended_allocation_units"),"reason_codes":row.get("reason_codes") or [],"evidence_delta":{"allocation_delta":row.get("allocation_delta"),"allocation_score":row.get("allocation_score")},"source_artifacts":row.get("supporting_artifacts") or [],"source_hashes":row.get("source_hashes") or {},"allocation_type":ALLOCATION_TYPE,"generated_at":row.get("generated_at")})
    payload={"schema_id":"aegis_research_allocation_event_log","schema_version":"v1","artifact_id":REPORT_FAMILY,"day_utc":str(day_utc),"generated_at":_now(),"allocation_type":ALLOCATION_TYPE,"disclaimer":DISCLAIMER,"events":events,"summary":{"event_count":len(events)},"safety":dict(SAFETY),**SAFETY}
    payload["content_hash"]=stable_hash_v1({**payload,"generated_at":"","content_hash":"","events":[{**e,"generated_at":""} for e in events]})
    return payload


def write_research_allocation_event_log_v1(*, truth_root: Path|str, day_utc: str, payload:dict[str,Any]|None=None)->Path:
    return write_json_v1(research_allocation_event_log_path_v1(truth_root=truth_root,day_utc=day_utc), payload or build_research_allocation_event_log_v1(truth_root=truth_root,day_utc=day_utc))


def _now()->str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
