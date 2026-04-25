from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


REQUIRED_METADATA_FIELDS = (
    "view_name",
    "surface_kind",
    "entity_scope",
    "truth_state",
    "as_of_utc",
    "freshness_state",
    "source_authority",
    "contract_id",
    "contract_version",
    "provenance_refs",
    "degradation_codes",
)


@dataclass(frozen=True)
class ProjectionContract:
    projection_name: str
    source_authority: tuple[str, ...]
    upstream_artifacts: tuple[dict[str, Any], ...]
    entity_scope: str
    required_metadata_fields: tuple[str, ...]
    allowed_composition: bool
    notes_on_boundaries: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


PROJECTION_CONTRACTS: dict[str, ProjectionContract] = {
    "engine_readiness_projection": ProjectionContract(
        projection_name="engine_readiness_projection",
        source_authority=(
            "session_authority_status_v1",
            "replay_certification_gate_v1",
            "ib_api_handshake_latest_pointer_v1",
            "runtime_truth_integrity_result_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "session_authority_status_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/session_authority_status_v1/current.json",
                "provenance": "operations_read_model.py",
            },
            {
                "artifact_family": "replay_certification_gate_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/replay_certification_gate_v1/<DAY>/replay_certification_gate.v1.json",
                "provenance": "operations_read_model.py",
            },
            {
                "artifact_family": "ib_api_handshake_latest_pointer_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/execution_evidence_v1/ib_api_handshake/latest.json",
                "provenance": "operations_read_model.py",
            },
            {
                "artifact_family": "runtime_truth_integrity_result_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/runtime_truth_integrity_result_v1/<DAY>/runtime_truth_integrity_result.v1.json",
                "provenance": "operations_read_model.py",
            },
        ),
        entity_scope="system",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection defines operational readiness inputs only. Workspace composition may arrange "
            "these facts for Operations and shell summary, but may not reinterpret source authority."
        ),
    ),
    "order_lifecycle_projection": ProjectionContract(
        projection_name="order_lifecycle_projection",
        source_authority=(
            "execution_evidence_v1",
            "fill_ledger_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "execution_evidence_v1/submissions",
                "path_pattern": "SLEEVE_TRUTH_ROOT/execution_evidence_v1/submissions/<DAY>/*.json",
                "provenance": "orders_read_model.py",
            },
            {
                "artifact_family": "fill_ledger_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/fill_ledger_v1/<DAY>/fills.v1.json",
                "provenance": "orders_read_model.py",
            },
        ),
        entity_scope="orders",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns operator-facing order lifecycle state derived from canonical execution "
            "evidence and fill ledger artifacts. Frontend consumers must not derive alternate lifecycle truth."
        ),
    ),
    "position_state_projection": ProjectionContract(
        projection_name="position_state_projection",
        source_authority=("positions_snapshot_v2",),
        upstream_artifacts=(
            {
                "artifact_family": "positions_v1/snapshots",
                "path_pattern": "SLEEVE_TRUTH_ROOT/positions_v1/snapshots/<DAY>/positions_snapshot.v2.json",
                "provenance": "positions_read_model.py",
            },
        ),
        entity_scope="positions",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns holdings state only. Open-order linkage belongs in composition unless a separate "
            "governed contract is added for cross-lifecycle linkage."
        ),
    ),
    "reconciliation_status_projection": ProjectionContract(
        projection_name="reconciliation_status_projection",
        source_authority=(
            "reconciliation_report_v3",
            "execution_reconciliation_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "reconciliation_report_v3",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/reconciliation_report_v3/<DAY>/reconciliation_report.v3.json",
                "provenance": "reconciliation_read_model.py",
            },
            {
                "artifact_family": "execution_reconciliation_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/execution_reconciliation_v1/<DAY>/execution_reconciliation.v1.json",
                "provenance": "reconciliation_read_model.py",
            },
        ),
        entity_scope="reconciliation",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection may surface agreement, mismatch, and lineage evidence. It must not silently heal "
            "cross-surface disagreements or replace canonical reconciliation rules."
        ),
    ),
    "alert_projection": ProjectionContract(
        projection_name="alert_projection",
        source_authority=(
            "alerts_projection_v1",
            "session_authority_alert_v1",
            "current_system_projection_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "alerts_projection_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/alerts_projection_v1/<DAY>/alerts_projection.v1.json",
                "provenance": "alerts_projection_v1 contract",
            },
            {
                "artifact_family": "session_authority_alert_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/session_authority_alert_v1/current.json",
                "provenance": "alerts_read_model.py",
            },
            {
                "artifact_family": "current_system_projection_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/current_system_projection_v1/<DAY>/current_system_projection.v1.json",
                "provenance": "alerts_read_model.py",
            },
        ),
        entity_scope="alerts",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Governed alert projection should prefer the repo-governed alerts projection artifact where present. "
            "Workspace composition may merge alert and integrity display concerns, but must keep source families visible."
        ),
    ),
    "advisory_context_projection": ProjectionContract(
        projection_name="advisory_context_projection",
        source_authority=(
            "advisory_decision_state_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "advisory_decision_state_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/reports/advisory_decision_state_v1/<DAY>/<SCOPE>/<DECISION>/advisory_decision_state.v1.json",
                "provenance": "advisory_read_model.py",
            },
        ),
        entity_scope="advisory",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns advisory decision rendering only and must read governed advisory_decision_state_v1 "
            "artifacts rather than legacy recommendation packets or UI-local actionability overlays."
        ),
    ),
    "financial_state_v1": ProjectionContract(
        projection_name="financial_state_v1",
        source_authority=(
            "accounting_nav_v2",
            "cash_ledger_snapshot_v1",
            "positions_snapshot_v5",
            "exposure_net_v1",
            "portfolio_governance_snapshot_v1",
            "c2_ib_account_registry",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "accounting_v2/nav",
                "path_pattern": "GLOBAL_TRUTH_ROOT/accounting_v2/nav/<DAY>/nav.v2.json",
                "provenance": "financial_state_authority_v1.py",
            },
            {
                "artifact_family": "cash_ledger_v1/snapshots",
                "path_pattern": "GLOBAL_TRUTH_ROOT/cash_ledger_v1/snapshots/<DAY>/cash_ledger_snapshot.v1.json",
                "provenance": "financial_state_authority_v1.py",
            },
            {
                "artifact_family": "positions_v1/snapshots",
                "path_pattern": "SLEEVE_TRUTH_ROOT/positions_v1/snapshots/<DAY>/positions_snapshot.v5.json",
                "provenance": "financial_state_authority_v1.py",
            },
            {
                "artifact_family": "risk_v1/exposure_net_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/risk_v1/exposure_net_v1/<DAY>/exposure_net.v1.json",
                "provenance": "financial_state_authority_v1.py",
            },
            {
                "artifact_family": "risk_v1/portfolio_governance_snapshot_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/risk_v1/portfolio_governance_snapshot_v1/<DAY>/portfolio_governance_snapshot.v1.json",
                "provenance": "financial_state_authority_v1.py",
            },
            {
                "artifact_family": "c2_ib_account_registry",
                "path_pattern": "REPO_ROOT/governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
                "provenance": "financial_state_authority_v1.py",
            },
        ),
        entity_scope="financial_state",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns canonical Aegis financial-state presentation for investable totals, account rollups, "
            "liquidity, reserve, and exposure summaries. It must fail closed when required financial inputs cannot "
            "be proven. Frontend consumers must not restitch alternate household or portfolio totals from positions "
            "and orders once this projection is available."
        ),
    ),
    "capital_domain_projection": ProjectionContract(
        projection_name="capital_domain_projection",
        source_authority=(
            "capital_domain_sqlite_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "capital_v1/capital_domain.v1.sqlite3",
                "path_pattern": "RUNTIME_ROOT/capital_v1/capital_domain.v1.sqlite3",
                "provenance": "capital_read_model.py",
            },
        ),
        entity_scope="capital",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns household Capital domain rendering from append-only Capital bounded-domain storage. "
            "Frontend consumers must not query raw storage tables directly and must consume derived API surfaces only."
        ),
    ),
    "configuration_workflow_projection": ProjectionContract(
        projection_name="configuration_workflow_projection",
        source_authority=(
            "configuration_state_v1",
            "compiled_active_config_v1",
            "configuration_activation_transaction_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "configuration_state_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/configuration_state_v1/current.json",
                "provenance": "configuration_workflow_v1.py",
            },
            {
                "artifact_family": "compiled_active_config_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/compiled_active_config_v1/<COMPILED_CONFIG_ID>/compiled_active_config.v1.json",
                "provenance": "configuration_workflow_v1.py",
            },
            {
                "artifact_family": "configuration_activation_transaction_v1",
                "path_pattern": "GLOBAL_TRUTH_ROOT/reports/configuration_activation_transaction_v1/<ACTIVATION_ID>/configuration_activation_transaction.v1.json",
                "provenance": "configuration_workflow_v1.py",
            },
            {
                "artifact_family": "ui_configuration_drafts_v1",
                "path_pattern": "RUNTIME_ROOT/ui_configuration_drafts_v1/capital_cashflow/*.json",
                "provenance": "configuration_workflow_v1.py",
            },
        ),
        entity_scope="configuration",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=False,
        notes_on_boundaries=(
            "UI can write non-authoritative drafts only. Activation must run through configuration_activation_authority_v1 "
            "to produce audited activation artifacts and advance configuration_state_v1/current.json. Locked safety fields "
            "remain non-editable from this projection surface."
        ),
    ),
    "sleeve_evaluation_projection": ProjectionContract(
        projection_name="sleeve_evaluation_projection",
        source_authority=(
            "C2_CAPITAL_AUTHORITY_POLICY_V1",
            "capital_authority_allocation_v1",
            "evaluation_input_manifest_v1",
            "sleeve_edge_measurement_snapshot_v1",
            "allocation_governance_snapshot_v1",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "C2_CAPITAL_AUTHORITY_POLICY_V1",
                "path_pattern": "REPO_ROOT/governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json",
                "provenance": "sleeve_evaluation_read_model.py",
            },
            {
                "artifact_family": "allocation_v1/capital_authority_allocation_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json",
                "provenance": "sleeve_evaluation_read_model.py",
            },
            {
                "artifact_family": "reports/evaluation_input_manifest_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/evaluation_input_manifest_v1/<DAY>/<SLEEVE_ID>/evaluation_input_manifest.v1.json",
                "provenance": "sleeve_evaluation_read_model.py",
            },
            {
                "artifact_family": "reports/sleeve_edge_measurement_snapshot_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/sleeve_edge_measurement_snapshot_v1/<DAY>/<SLEEVE_ID>/sleeve_edge_measurement_snapshot.v1.json",
                "provenance": "sleeve_evaluation_read_model.py",
            },
            {
                "artifact_family": "reports/allocation_governance_snapshot_v1",
                "path_pattern": "SLEEVE_TRUTH_ROOT/reports/allocation_governance_snapshot_v1/<DAY>/<SLEEVE_ID>/allocation_governance_snapshot.v1.json",
                "provenance": "sleeve_evaluation_read_model.py",
            },
        ),
        entity_scope="sleeves",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns the Aegis sleeves domain read model. It may surface canonical economic sleeve ids, "
            "allocation facts, evaluation evidence, and governance recommendation states, but it must not invent "
            "frontend sleeve scores, target allocations, or tax-efficiency outputs where backend authorities are missing."
        ),
    ),
    "tax_state_projection": ProjectionContract(
        projection_name="tax_state_projection",
        source_authority=(
            "accounting_nav_v2",
            "cash_ledger_snapshot_v1",
            "positions_snapshot_v5",
            "c2_ib_account_registry",
        ),
        upstream_artifacts=(
            {
                "artifact_family": "accounting_v2/nav",
                "path_pattern": "GLOBAL_TRUTH_ROOT/accounting_v2/nav/<DAY>/nav.v2.json",
                "provenance": "tax_state_read_model.py",
            },
            {
                "artifact_family": "cash_ledger_v1/snapshots",
                "path_pattern": "GLOBAL_TRUTH_ROOT/cash_ledger_v1/snapshots/<DAY>/cash_ledger_snapshot.v1.json",
                "provenance": "tax_state_read_model.py",
            },
            {
                "artifact_family": "positions_v1/snapshots",
                "path_pattern": "SLEEVE_TRUTH_ROOT/positions_v1/snapshots/<DAY>/positions_snapshot.v5.json",
                "provenance": "tax_state_read_model.py",
            },
            {
                "artifact_family": "c2_ib_account_registry",
                "path_pattern": "REPO_ROOT/governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
                "provenance": "tax_state_read_model.py",
            },
        ),
        entity_scope="tax",
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        allowed_composition=True,
        notes_on_boundaries=(
            "Projection owns the Aegis tax domain read model for the current seam. Until canonical tax lot, basis, "
            "harvest, and tax-report artifacts are materialized on this seam, the backend must surface explicit "
            "degradation and proxy-only summaries, and frontend consumers must not infer tax truth locally."
        ),
    ),
}


def get_projection_contract(name: str) -> ProjectionContract | None:
    return PROJECTION_CONTRACTS.get(name)


def projection_names() -> tuple[str, ...]:
    return tuple(PROJECTION_CONTRACTS.keys())


def projection_registry() -> dict[str, dict[str, Any]]:
    return {name: contract.to_dict() for name, contract in PROJECTION_CONTRACTS.items()}
