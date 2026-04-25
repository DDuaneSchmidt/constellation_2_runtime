from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1, canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.tax.constants_v1 import (
    TAX_ALGORITHM_VERSION_SET_V1,
    TAX_BASIS_QUANTUM_V1,
    TAX_GAIN_LOSS_QUANTUM_V1,
    TAX_MONEY_QUANTUM_V1,
    TAX_QUANTITY_QUANTUM_V1,
    TAX_ROUNDING_MODE_V1,
)


REPO_ROOT = Path(__file__).resolve().parents[3]

TAX_SCHEMA_RELPATHS_V1 = {
    "tax_scope_registry": "governance/04_DATA/SCHEMAS/C2/TAX/tax_scope_registry.v1.schema.json",
    "tax_scope_membership": "governance/04_DATA/SCHEMAS/C2/TAX/tax_scope_membership.v1.schema.json",
    "wash_enforcement_scope": "governance/04_DATA/SCHEMAS/C2/TAX/wash_enforcement_scope.v1.schema.json",
    "tax_observed_event": "governance/04_DATA/SCHEMAS/C2/TAX/tax_observed_event.v1.schema.json",
    "corporate_action_observed_event": "governance/04_DATA/SCHEMAS/C2/TAX/corporate_action_observed_event.v1.schema.json",
    "tax_fact_candidate": "governance/04_DATA/SCHEMAS/C2/TAX/tax_fact_candidate.v1.schema.json",
    "tax_fact_acceptance_decision": "governance/04_DATA/SCHEMAS/C2/TAX/tax_fact_acceptance_decision.v1.schema.json",
    "accepted_tax_fact": "governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact.v1.schema.json",
    "tax_fact_correction": "governance/04_DATA/SCHEMAS/C2/TAX/tax_fact_correction.v1.schema.json",
    "accepted_tax_fact_journal": "governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact_journal.v1.schema.json",
    "tax_lot_state": "governance/04_DATA/SCHEMAS/C2/TAX/tax_lot_state.v1.schema.json",
    "tax_wash_state": "governance/04_DATA/SCHEMAS/C2/TAX/tax_wash_state.v1.schema.json",
    "tax_data_completeness_state": "governance/04_DATA/SCHEMAS/C2/TAX/tax_data_completeness_state.v1.schema.json",
    "tax_snapshot_build_manifest": "governance/04_DATA/SCHEMAS/C2/TAX/tax_snapshot_build_manifest.v1.schema.json",
    "tax_position_snapshot": "governance/04_DATA/SCHEMAS/C2/TAX/tax_position_snapshot.v1.schema.json",
    "tax_decision_ranking_policy": "governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_ranking_policy.v1.schema.json",
    "tax_rounding_policy": "governance/04_DATA/SCHEMAS/C2/TAX/tax_rounding_policy.v1.schema.json",
    "tax_policy_registry": "governance/04_DATA/SCHEMAS/C2/TAX/tax_policy_registry.v1.schema.json",
    "resolved_tax_policy_set": "governance/04_DATA/SCHEMAS/C2/TAX/resolved_tax_policy_set.v1.schema.json",
    "tax_decision_dependency_fingerprint": "governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_dependency_fingerprint.v1.schema.json",
    "tax_execution_gate_result": "governance/04_DATA/SCHEMAS/C2/TAX/tax_execution_gate_result.v1.schema.json",
    "tax_correction_impact_index": "governance/04_DATA/SCHEMAS/C2/TAX/tax_correction_impact_index.v1.schema.json",
    "tax_decision_time_truth_view": "governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_time_truth_view.v1.schema.json",
    "tax_current_corrected_truth_view": "governance/04_DATA/SCHEMAS/C2/TAX/tax_current_corrected_truth_view.v1.schema.json",
    "sell_tax_decision": "governance/04_DATA/SCHEMAS/C2/TAX/sell_tax_decision.v1.schema.json",
    "buy_tax_decision": "governance/04_DATA/SCHEMAS/C2/TAX/buy_tax_decision.v1.schema.json",
    "account_routing_tax_decision": "governance/04_DATA/SCHEMAS/C2/TAX/account_routing_tax_decision.v1.schema.json",
    "harvest_candidate_decision": "governance/04_DATA/SCHEMAS/C2/TAX/harvest_candidate_decision.v1.schema.json",
    "tax_decision_journal": "governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_journal.v1.schema.json",
    "realized_tax_report": "governance/04_DATA/SCHEMAS/C2/TAX/realized_tax_report.v1.schema.json",
    "tax_advisory_explanation": "governance/04_DATA/SCHEMAS/C2/TAX/tax_advisory_explanation.v1.schema.json",
    "decision_replay_report": "governance/04_DATA/SCHEMAS/C2/TAX/decision_replay_report.v1.schema.json",
    "lot_reconciliation_report": "governance/04_DATA/SCHEMAS/C2/TAX/lot_reconciliation_report.v1.schema.json",
    "broker_tax_reconciliation_report": "governance/04_DATA/SCHEMAS/C2/TAX/broker_tax_reconciliation_report.v1.schema.json",
    "tax_corporate_action_candidate": "governance/04_DATA/SCHEMAS/C2/TAX/tax_corporate_action_candidate.v1.schema.json",
    "tax_corporate_action_acceptance_decision": "governance/04_DATA/SCHEMAS/C2/TAX/tax_corporate_action_acceptance_decision.v1.schema.json",
    "tax_corporate_action_state": "governance/04_DATA/SCHEMAS/C2/TAX/tax_corporate_action_state.v1.schema.json",
}


def now_utc_iso_v1() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def require_nonempty_str_v1(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"TAX_REQUIRED_FIELD_MISSING:{field_name}")
    return text


def require_list_of_strings_v1(values: Iterable[Any]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(sorted(normalized))


def canonical_hash_v1(payload: Any) -> str:
    return canonical_sha256_hex_v1(payload)


def canonical_payload_hashes_v1(payloads: Iterable[Any]) -> tuple[str, ...]:
    hashes = [canonical_hash_v1(payload) for payload in payloads]
    return tuple(sorted(hashes))


def canonical_json_sha256_v1(payload: Any) -> str:
    return canonical_hash_v1(canonical_json_bytes_v1(payload).decode("utf-8"))


def validate_tax_payload_v1(payload: dict[str, Any]) -> dict[str, Any]:
    schema_relpath = TAX_SCHEMA_RELPATHS_V1.get(str(payload.get("schema_id") or "").strip())
    if not schema_relpath:
        raise ValueError(f"TAX_SCHEMA_NOT_REGISTERED:{payload.get('schema_id')}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    return payload


def decimal_from_value_v1(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = str(value or "").strip()
    if not text:
        return Decimal("0")
    return Decimal(text)


def quantize_quantity_v1(value: Any) -> str:
    return format(decimal_from_value_v1(value).quantize(TAX_QUANTITY_QUANTUM_V1, rounding=TAX_ROUNDING_MODE_V1), "f")


def quantize_money_v1(value: Any) -> str:
    return format(decimal_from_value_v1(value).quantize(TAX_MONEY_QUANTUM_V1, rounding=TAX_ROUNDING_MODE_V1), "f")


def quantize_basis_v1(value: Any) -> str:
    return format(decimal_from_value_v1(value).quantize(TAX_BASIS_QUANTUM_V1, rounding=TAX_ROUNDING_MODE_V1), "f")


def quantize_gain_loss_v1(value: Any) -> str:
    return format(decimal_from_value_v1(value).quantize(TAX_GAIN_LOSS_QUANTUM_V1, rounding=TAX_ROUNDING_MODE_V1), "f")


def algorithm_version_v1(name: str) -> str:
    version = TAX_ALGORITHM_VERSION_SET_V1.get(name)
    if not version:
        raise ValueError(f"TAX_ALGORITHM_VERSION_UNKNOWN:{name}")
    return version
