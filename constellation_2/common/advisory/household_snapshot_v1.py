from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/household_snapshot.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class HouseholdSnapshotV1:
    schema_id: str
    schema_version: str
    record_id: str
    household_snapshot_id: str
    household_id: str
    parent_policy_id: str
    snapshot_version: str
    created_at: str
    effective_at: str
    actor_source: str
    timestamp_basis: str
    contract_version: str
    builder_version: str
    validation_status: str
    completeness_status: str
    freshness_status: str
    reconciliation_status: str
    parent_lineage_refs: tuple[str, ...]
    coverage_scope: dict[str, Any]
    account_registry_snapshot: dict[str, Any]
    investable_asset_summary: dict[str, Any]
    cash_liquidity_summary: dict[str, Any]
    holdings_by_account: tuple[dict[str, Any], ...]
    holdings_by_asset_class: tuple[dict[str, Any], ...]
    holdings_by_tax_treatment: tuple[dict[str, Any], ...]
    verified_components: tuple[dict[str, Any], ...]
    unverified_components: tuple[dict[str, Any], ...]
    missing_components: tuple[dict[str, Any], ...]
    input_record_refs: tuple[str, ...]
    classification_refs: tuple[str, ...]
    freshness_attestations: tuple[dict[str, Any], ...]
    value_basis: dict[str, Any]
    validity_tier: str
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'HouseholdSnapshotV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['household_snapshot_id']),
            str(obj['household_id']),
            str(obj['parent_policy_id']),
            str(obj['snapshot_version']),
            str(obj['created_at']),
            str(obj['effective_at']),
            str(obj['actor_source']),
            str(obj['timestamp_basis']),
            str(obj['contract_version']),
            str(obj['builder_version']),
            str(obj['validation_status']),
            str(obj['completeness_status']),
            str(obj['freshness_status']),
            str(obj['reconciliation_status']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            dict(obj['coverage_scope']),
            dict(obj['account_registry_snapshot']),
            dict(obj['investable_asset_summary']),
            dict(obj['cash_liquidity_summary']),
            tuple(dict(item) for item in obj['holdings_by_account']),
            tuple(dict(item) for item in obj['holdings_by_asset_class']),
            tuple(dict(item) for item in obj['holdings_by_tax_treatment']),
            tuple(dict(item) for item in obj['verified_components']),
            tuple(dict(item) for item in obj['unverified_components']),
            tuple(dict(item) for item in obj['missing_components']),
            tuple(str(item) for item in obj['input_record_refs']),
            tuple(str(item) for item in obj['classification_refs']),
            tuple(dict(item) for item in obj['freshness_attestations']),
            dict(obj['value_basis']),
            str(obj['validity_tier']),
            tuple(str(item) for item in obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'HouseholdSnapshotV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'household_snapshot_id': self.household_snapshot_id,
            'household_id': self.household_id,
            'parent_policy_id': self.parent_policy_id,
            'snapshot_version': self.snapshot_version,
            'created_at': self.created_at,
            'effective_at': self.effective_at,
            'actor_source': self.actor_source,
            'timestamp_basis': self.timestamp_basis,
            'contract_version': self.contract_version,
            'builder_version': self.builder_version,
            'validation_status': self.validation_status,
            'completeness_status': self.completeness_status,
            'freshness_status': self.freshness_status,
            'reconciliation_status': self.reconciliation_status,
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'coverage_scope': self.coverage_scope,
            'account_registry_snapshot': self.account_registry_snapshot,
            'investable_asset_summary': self.investable_asset_summary,
            'cash_liquidity_summary': self.cash_liquidity_summary,
            'holdings_by_account': [dict(item) for item in self.holdings_by_account],
            'holdings_by_asset_class': [dict(item) for item in self.holdings_by_asset_class],
            'holdings_by_tax_treatment': [dict(item) for item in self.holdings_by_tax_treatment],
            'verified_components': [dict(item) for item in self.verified_components],
            'unverified_components': [dict(item) for item in self.unverified_components],
            'missing_components': [dict(item) for item in self.missing_components],
            'input_record_refs': list(self.input_record_refs),
            'classification_refs': list(self.classification_refs),
            'freshness_attestations': [dict(item) for item in self.freshness_attestations],
            'value_basis': self.value_basis,
            'validity_tier': self.validity_tier,
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
