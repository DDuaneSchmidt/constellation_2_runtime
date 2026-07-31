#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
REPO_ROOT=Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path: sys.path.insert(0,str(REPO_ROOT))
from ops.aegis.research_program_registry_v1 import build_research_program_registry_v1, write_research_program_registry_v1
from ops.aegis.research_capital_scoring_v1 import build_research_capital_scoring_v1, write_research_capital_scoring_v1
from ops.aegis.research_allocation_decisions_v1 import build_research_allocation_decisions_v1, write_research_allocation_decisions_v1
from ops.aegis.research_allocation_event_log_v1 import build_research_allocation_event_log_v1, write_research_allocation_event_log_v1
from ops.aegis.research_capital_allocation_self_check_v1 import build_research_capital_allocation_self_check_v1, write_research_capital_allocation_self_check_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1

def main():
 p=argparse.ArgumentParser(); p.add_argument('--truth-root','--truth_root',dest='truth_root',default='/home/node/constellation_runtime_data/truth'); p.add_argument('--day','--day-utc','--day_utc',dest='day',required=True); a=p.parse_args(); root=Path(a.truth_root); day=str(a.day)
 reg=build_research_program_registry_v1(truth_root=root,day_utc=day); rp=write_research_program_registry_v1(truth_root=root,day_utc=day,payload=reg)
 score=build_research_capital_scoring_v1(truth_root=root,day_utc=day,registry=reg); sp=write_research_capital_scoring_v1(truth_root=root,day_utc=day,payload=score)
 dec=build_research_allocation_decisions_v1(truth_root=root,day_utc=day,registry=reg,scoring=score); dp=write_research_allocation_decisions_v1(truth_root=root,day_utc=day,payload=dec)
 log=build_research_allocation_event_log_v1(truth_root=root,day_utc=day,decisions=dec); lp=write_research_allocation_event_log_v1(truth_root=root,day_utc=day,payload=log)
 check=build_research_capital_allocation_self_check_v1(truth_root=root,day_utc=day,registry=reg,scoring=score,decisions=dec); cp=write_research_capital_allocation_self_check_v1(truth_root=root,day_utc=day,payload=check)
 final={"schema_id":"aegis_research_capital_allocation","schema_version":"v1","artifact_id":"aegis_research_capital_allocation_v1","day_utc":day,"allocation_type":"RESEARCH_ATTENTION_ONLY","disclaimer":"This is not trade sizing. This is not live capital allocation. This is research-priority guidance only.","programs":reg.get('programs',[]),"scores":score.get('scores',[]),"decisions":dec.get('decisions',[]),"events":log.get('events',[]),"self_check":{"ok":check.get('ok'),"failure_count":check.get('failure_count')},"summary":dec.get('summary',{}),"source_artifact_paths":{"program_registry":str(rp),"scoring":str(sp),"decisions":str(dp),"event_log":str(lp),"self_check":str(cp)},"trade_advice_allowed":False,"broker_execution_allowed":False,"autonomous_execution_allowed":False,"allocation_instructions_allowed":False}
 final['content_hash']=stable_hash_v1({**final,'content_hash':'','decisions':[{**d,'generated_at':''} for d in final['decisions']],'events':[{**e,'generated_at':''} for e in final['events']]}); fp=write_json_v1(report_path_v1(root,'aegis_research_capital_allocation_v1',day,'research_capital_allocation.v1.json'), final)
 print(json.dumps({'ok':check.get('ok'),'day_utc':day,'path':str(fp),'program_count':reg['summary']['research_program_count'],'decision_count':dec['summary']['decision_count'],'summary':dec.get('summary')},sort_keys=True)); return 0 if check.get('ok') else 2
if __name__=='__main__': raise SystemExit(main())
