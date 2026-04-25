from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ExternalRealitySnapshot:
    external_snapshot_id: str
    source_type: str
    source_name: str
    captured_at: str
    schema_version: int
    content_hash: str
    account_refs: tuple[dict[str, Any], ...]
    position_records: tuple[dict[str, Any], ...]
    taxlot_records: tuple[dict[str, Any], ...]
    execution_records: tuple[dict[str, Any], ...]
    cash_records: tuple[dict[str, Any], ...]
    valuation_records: tuple[dict[str, Any], ...]
    pnl_records: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class InternalRealitySnapshot:
    internal_snapshot_id: str
    runtime_snapshot_id: str
    captured_at: str
    schema_version: int
    content_hash: str
    account_state: tuple[dict[str, Any], ...]
    position_state: tuple[dict[str, Any], ...]
    taxlot_state: tuple[dict[str, Any], ...]
    execution_state: tuple[dict[str, Any], ...]
    cash_state: tuple[dict[str, Any], ...]
    valuation_state: tuple[dict[str, Any], ...]
    pnl_state: tuple[dict[str, Any], ...]
    lineage_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PositionMismatch:
    account_id: str
    symbol: str
    internal_quantity: float | None
    external_quantity: float | None
    delta_quantity: float | None
    mismatch_type: str
    severity: str
    explanation: str


@dataclass(frozen=True, slots=True)
class TaxLotMismatch:
    account_id: str
    symbol: str
    lot_key: str
    internal_basis: float | None
    external_basis: float | None
    internal_quantity: float | None
    external_quantity: float | None
    mismatch_type: str
    severity: str
    explanation: str


@dataclass(frozen=True, slots=True)
class ExecutionMismatch:
    execution_key: str
    internal_execution: dict[str, Any] | None
    external_execution: dict[str, Any] | None
    mismatch_type: str
    severity: str
    explanation: str


@dataclass(frozen=True, slots=True)
class ValuationMismatch:
    account_id: str
    symbol: str
    internal_value: float | None
    external_value: float | None
    delta_value: float | None
    mismatch_type: str
    severity: str
    explanation: str


@dataclass(frozen=True, slots=True)
class PnLMismatch:
    account_id: str
    symbol_or_scope: str
    internal_pnl: float | None
    external_pnl: float | None
    delta_pnl: float | None
    mismatch_type: str
    severity: str
    explanation: str


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    reconciliation_id: str
    external_snapshot_ref: str
    internal_snapshot_ref: str
    position_mismatches: tuple[PositionMismatch, ...]
    taxlot_mismatches: tuple[TaxLotMismatch, ...]
    execution_mismatches: tuple[ExecutionMismatch, ...]
    valuation_mismatches: tuple[ValuationMismatch, ...]
    pnl_mismatches: tuple[PnLMismatch, ...]
    overall_severity: str
    recommended_actions: tuple[str, ...]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class DiscrepancyClassification:
    reconciliation_id: str
    mismatch_family: str
    classification: str
    severity: str
    operator_visibility: str
    freeze_required: bool
    review_required: bool
    explanation: str


@dataclass(frozen=True, slots=True)
class CorrectionRecommendation:
    reconciliation_id: str
    recommendation_id: str
    target_surface: str
    action_type: str
    action_priority: str
    requires_human_review: bool
    rationale: str


@dataclass(frozen=True, slots=True)
class ReconciliationBundle:
    reconciliation_id: str
    internal_snapshot_ref: str
    external_snapshot_ref: str
    result_ref: str
    discrepancy_refs: tuple[str, ...]
    recommendation_refs: tuple[str, ...]
    artifact_hash: str
