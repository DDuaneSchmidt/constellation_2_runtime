from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.advisory_storage_v1 import (
    advisory_authority_root_v1,
    write_immutable_json_v1,
)


def policy_snapshot_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "authorities" / "policy_snapshot_v1" / "households" / str(household_id)


def policy_snapshot_path_v1(output_root: str | Path, household_id: str, policy_snapshot_id: str) -> Path:
    return policy_snapshot_dir_v1(output_root, household_id) / f"{policy_snapshot_id}.policy_snapshot.v1.json"


def household_state_snapshot_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "authorities" / "household_state_snapshot_v1" / "households" / str(household_id)


def household_state_snapshot_path_v1(output_root: str | Path, household_id: str, household_state_snapshot_id: str) -> Path:
    return household_state_snapshot_dir_v1(output_root, household_id) / f"{household_state_snapshot_id}.household_state_snapshot.v1.json"


def compiled_constraints_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "artifacts" / "compiled_constraints_v1" / "households" / str(household_id)


def compiled_constraints_path_v1(output_root: str | Path, household_id: str, compiled_constraints_id: str) -> Path:
    return compiled_constraints_dir_v1(output_root, household_id) / f"{compiled_constraints_id}.compiled_constraints.v1.json"


def allocation_plan_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "artifacts" / "allocation_plan_v1" / "households" / str(household_id)


def allocation_plan_path_v1(output_root: str | Path, household_id: str, allocation_plan_id: str) -> Path:
    return allocation_plan_dir_v1(output_root, household_id) / f"{allocation_plan_id}.allocation_plan.v1.json"


def risk_envelope_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "artifacts" / "risk_envelope_v1" / "households" / str(household_id)


def risk_envelope_path_v1(output_root: str | Path, household_id: str, risk_envelope_id: str) -> Path:
    return risk_envelope_dir_v1(output_root, household_id) / f"{risk_envelope_id}.risk_envelope.v1.json"


def rebalance_candidates_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "artifacts" / "rebalance_candidates_v1" / "households" / str(household_id)


def rebalance_candidates_path_v1(output_root: str | Path, household_id: str, rebalance_candidates_id: str) -> Path:
    return rebalance_candidates_dir_v1(output_root, household_id) / f"{rebalance_candidates_id}.rebalance_candidates.v1.json"


def tax_adjudicated_rebalance_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "artifacts" / "tax_adjudicated_rebalance_v1" / "households" / str(household_id)


def tax_adjudicated_rebalance_path_v1(output_root: str | Path, household_id: str, tax_adjudicated_rebalance_id: str) -> Path:
    return tax_adjudicated_rebalance_dir_v1(output_root, household_id) / f"{tax_adjudicated_rebalance_id}.tax_adjudicated_rebalance.v1.json"


def portfolio_authorization_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "authorities" / "portfolio_authorization_v1" / "households" / str(household_id)


def portfolio_authorization_path_v1(output_root: str | Path, household_id: str, execution_intent_id: str) -> Path:
    return portfolio_authorization_dir_v1(output_root, household_id) / f"{execution_intent_id}.portfolio_authorization.v1.json"


def portfolio_decision_record_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / "artifacts" / "portfolio_decision_record_v1" / "households" / str(household_id)


def portfolio_decision_record_path_v1(output_root: str | Path, household_id: str, execution_intent_id: str) -> Path:
    return portfolio_decision_record_dir_v1(output_root, household_id) / f"{execution_intent_id}.portfolio_decision_record.v1.json"


def write_household_portfolio_artifact_v1(path: str | Path, obj: dict[str, object]) -> Path:
    return write_immutable_json_v1(path, obj)


def _read_json_obj(path: str | Path) -> dict[str, Any]:
    with Path(path).expanduser().resolve().open("r", encoding="utf-8") as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def load_portfolio_authorization_obj_v1(output_root: str | Path, household_id: str, execution_intent_id: str) -> dict[str, Any]:
    return _read_json_obj(portfolio_authorization_path_v1(output_root, household_id, execution_intent_id))


def find_execution_intent_any_household_v1(output_root: str | Path, execution_intent_id: str) -> tuple[ExecutionIntentV1, Path]:
    root = advisory_authority_root_v1(output_root) / "artifacts" / "execution_intent_v1" / "households"
    matches = sorted(root.glob(f"*/{execution_intent_id}.execution_intent.v1.json"))
    if not matches:
        raise FileNotFoundError(f"EXECUTION_INTENT_NOT_FOUND:{execution_intent_id}")
    if len(matches) != 1:
        raise ValueError(f"EXECUTION_INTENT_AMBIGUOUS:{execution_intent_id}")
    path = matches[0].resolve()
    return ExecutionIntentV1.load_file(path), path


def load_portfolio_authorization_for_execution_intent_v1(
    output_root: str | Path,
    execution_intent_id: str,
) -> tuple[ExecutionIntentV1, Path, dict[str, Any], Path]:
    execution_intent, execution_intent_path = find_execution_intent_any_household_v1(output_root, execution_intent_id)
    authorization_path = portfolio_authorization_path_v1(
        output_root,
        execution_intent.household_id,
        execution_intent.execution_intent_id,
    ).resolve()
    authorization_obj = _read_json_obj(authorization_path)
    return execution_intent, execution_intent_path, authorization_obj, authorization_path
