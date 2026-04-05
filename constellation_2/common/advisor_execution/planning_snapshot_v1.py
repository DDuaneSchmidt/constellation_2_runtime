from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/planning_snapshot.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class AccountsV1:
    taxable_account_id: str
    cash_reserve_account_id: str
    spending_account_id: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'AccountsV1':
        return cls(
            taxable_account_id=str(obj['taxable_account_id']),
            cash_reserve_account_id=str(obj['cash_reserve_account_id']),
            spending_account_id=str(obj['spending_account_id']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'taxable_account_id': self.taxable_account_id,
            'cash_reserve_account_id': self.cash_reserve_account_id,
            'spending_account_id': self.spending_account_id,
        }


@dataclass(frozen=True, slots=True)
class LiquidityV1:
    cash_cents: int

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'LiquidityV1':
        return cls(cash_cents=int(obj['cash_cents']))

    def to_dict(self) -> dict[str, Any]:
        return {'cash_cents': self.cash_cents}


@dataclass(frozen=True, slots=True)
class SpendingV1:
    minimum_monthly_spending_cents: int
    monthly_spending_cents: int

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'SpendingV1':
        return cls(
            minimum_monthly_spending_cents=int(obj['minimum_monthly_spending_cents']),
            monthly_spending_cents=int(obj['monthly_spending_cents']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'minimum_monthly_spending_cents': self.minimum_monthly_spending_cents,
            'monthly_spending_cents': self.monthly_spending_cents,
        }


@dataclass(frozen=True, slots=True)
class IncomeV1:
    guaranteed_monthly_income_cents: int

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'IncomeV1':
        return cls(guaranteed_monthly_income_cents=int(obj['guaranteed_monthly_income_cents']))

    def to_dict(self) -> dict[str, Any]:
        return {'guaranteed_monthly_income_cents': self.guaranteed_monthly_income_cents}


@dataclass(frozen=True, slots=True)
class TaxProfileV1:
    present: bool

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'TaxProfileV1':
        return cls(present=bool(obj['present']))

    def to_dict(self) -> dict[str, Any]:
        return {'present': self.present}


@dataclass(frozen=True, slots=True)
class AnnuityV1:
    annuity_id: str
    phase: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'AnnuityV1':
        return cls(annuity_id=str(obj['annuity_id']), phase=str(obj['phase']))

    def to_dict(self) -> dict[str, Any]:
        return {'annuity_id': self.annuity_id, 'phase': self.phase}


@dataclass(frozen=True, slots=True)
class PlanningSnapshotV1:
    planning_snapshot_id: str
    advisory_packet_id: str
    created_at: str
    version: str
    accounts: AccountsV1
    liquidity: LiquidityV1
    spending: SpendingV1
    income: IncomeV1
    tax_profile: TaxProfileV1
    annuities: tuple[AnnuityV1, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PlanningSnapshotV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            planning_snapshot_id=str(obj['planning_snapshot_id']),
            advisory_packet_id=str(obj['advisory_packet_id']),
            created_at=str(obj['created_at']),
            version=str(obj['version']),
            accounts=AccountsV1.from_dict(obj['accounts']),
            liquidity=LiquidityV1.from_dict(obj['liquidity']),
            spending=SpendingV1.from_dict(obj['spending']),
            income=IncomeV1.from_dict(obj['income']),
            tax_profile=TaxProfileV1.from_dict(obj['tax_profile']),
            annuities=tuple(AnnuityV1.from_dict(item) for item in obj['annuities']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PlanningSnapshotV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': 'planning_snapshot',
            'schema_version': 'v1',
            'planning_snapshot_id': self.planning_snapshot_id,
            'advisory_packet_id': self.advisory_packet_id,
            'created_at': self.created_at,
            'version': self.version,
            'accounts': self.accounts.to_dict(),
            'liquidity': self.liquidity.to_dict(),
            'spending': self.spending.to_dict(),
            'income': self.income.to_dict(),
            'tax_profile': self.tax_profile.to_dict(),
            'annuities': [item.to_dict() for item in self.annuities],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
