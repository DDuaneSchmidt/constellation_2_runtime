#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.hypothesis_outcome_ledger_v1 import build_hypothesis_outcome_ledger_v1, write_hypothesis_outcome_ledger_v1
from ops.aegis.hypothesis_state_machine_v1 import build_hypothesis_state_machine_v1, write_hypothesis_state_machine_v1
from ops.aegis.exit_recommendations_v1 import build_exit_recommendations_v1, write_exit_recommendations_v1
from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1, write_outcome_registry_v1
from ops.aegis.paper_outcome_auto_closure_v1 import build_paper_outcome_auto_closure_v1, write_paper_outcome_auto_closure_v1
from ops.aegis.outcome_validation_maturity_self_check_v1 import build_outcome_validation_maturity_self_check_v1, write_outcome_validation_maturity_self_check_v1
from ops.aegis.statistical_sufficiency_engine_v1 import build_statistical_sufficiency_v1, write_statistical_sufficiency_v1
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1, write_validation_samples_v1


def main() -> int:
    parser=argparse.ArgumentParser(prog="build_aegis_outcome_validation_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    args=parser.parse_args()
    root=Path(args.truth_root); day=str(args.day)
    exit_recs=build_exit_recommendations_v1(truth_root=root, day_utc=day); exit_rec_paths=write_exit_recommendations_v1(truth_root=root, day_utc=day, payload=exit_recs)
    auto_closure=build_paper_outcome_auto_closure_v1(truth_root=root, day_utc=day); auto_closure_path=write_paper_outcome_auto_closure_v1(truth_root=root, day_utc=day, payload=auto_closure)
    outcomes=build_outcome_registry_v1(truth_root=root, day_utc=day); outcome_path=write_outcome_registry_v1(truth_root=root, day_utc=day, payload=outcomes)
    samples=build_validation_samples_v1(truth_root=root, day_utc=day, outcome_registry=outcomes); sample_path=write_validation_samples_v1(truth_root=root, day_utc=day, payload=samples)
    ledger=build_hypothesis_outcome_ledger_v1(truth_root=root, day_utc=day, outcome_registry=outcomes, validation_samples=samples); ledger_path=write_hypothesis_outcome_ledger_v1(truth_root=root, day_utc=day, payload=ledger)
    suff=build_statistical_sufficiency_v1(truth_root=root, day_utc=day, ledger=ledger); suff_path=write_statistical_sufficiency_v1(truth_root=root, day_utc=day, payload=suff)
    states=build_hypothesis_state_machine_v1(truth_root=root, day_utc=day, sufficiency=suff); state_path=write_hypothesis_state_machine_v1(truth_root=root, day_utc=day, payload=states)
    check=build_outcome_validation_maturity_self_check_v1(truth_root=root, day_utc=day, outcomes=outcomes, samples=samples, ledger=ledger, sufficiency=suff, states=states); check_path=write_outcome_validation_maturity_self_check_v1(truth_root=root, day_utc=day, payload=check)
    print(json.dumps({"ok": True, "day_utc": day, "paths": {"exit_recommendations": str(exit_rec_paths.get("exit_recommendations", "")), "paper_outcome_auto_closure": str(auto_closure_path), "outcome_registry": str(outcome_path), "validation_samples": str(sample_path), "hypothesis_outcome_ledger": str(ledger_path), "statistical_sufficiency": str(suff_path), "hypothesis_states": str(state_path), "self_check": str(check_path)}, "summary": {"paper_position_count": outcomes["summary"]["paper_position_count"], "open_outcome_count": outcomes["summary"]["open_outcomes"], "closed_outcome_count": outcomes["summary"]["closed_outcomes"], "validation_sample_count": samples["summary"]["included_samples"], "self_check_ok": check["ok"]}, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0 if check.get("ok") else 2

if __name__ == "__main__":
    raise SystemExit(main())
