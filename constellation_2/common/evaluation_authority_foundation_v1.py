from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_frozen_decision_input_bundle_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.day_open_attempt_v1 import read_day_open_attempt_runtime_lifecycle_ref_v1
from constellation_2.common.governed_evaluation_v1 import write_evaluation_policy_snapshot_v1
from constellation_2.common.operator_intervention_state_v1 import OPERATOR_INTERVENTION_STATE_SCHEMA_RELPATH
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.common.sleeve_edge_measurement_v1 import read_sleeve_edge_snapshot_for_day_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.evaluation_authority_foundation_v1"
POLICY_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json"

INPUT_MANIFEST_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/evaluation_input_manifest.v1.schema.json"
OUTCOME_ATTRIBUTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/outcome_attribution_snapshot.v1.schema.json"
EDGE_MEASUREMENT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_edge_measurement_snapshot.v1.schema.json"
ALLOCATION_GOVERNANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/allocation_governance_snapshot.v1.schema.json"
OPERATIVE_CONTROL_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_operative_control_state.v1.schema.json"


@dataclass(frozen=True)
class EvidenceInputsV1:
    canonical_truth_root: Path
    execution_truth_root: Path
    day_open_attempt_path: Path
    execution_reconciliation_path: Path
    reconciliation_report_path: Path
    sleeve_edge_snapshot_path: Path
    sleeve_edge_fact_ledger_path: Path
    capital_authority_allocation_path: Path
    sleeve_governance_action_path: Path | None
    operator_intervention_state_path: Path | None
    runtime_lifecycle_ref: Dict[str, Any] | None


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_day(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise ValueError(f"BAD_DAY_UTC:{day!r}")
    return day


def _read_json(path: Path) -> Dict[str, Any]:
    return read_json_object_v1(path.resolve())


def _policy_registry() -> Dict[str, Any]:
    return _read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve())


def _resolve_execution_truth_root(*, truth_root: Path, execution_sleeve_id: str, mode: str) -> Path:
    registry = _read_json((REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve())
    for row in registry.get("sleeves") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("sleeve_id") or "").strip().upper() != str(execution_sleeve_id).strip().upper():
            continue
        if str(row.get("mode") or "").strip().upper() != str(mode).strip().upper():
            continue
        partition = str(row.get("truth_partition") or "").strip()
        if partition:
            root = (truth_root.resolve().parent / partition).resolve()
            if root.exists() and root.is_dir():
                return root
    candidate = (
        truth_root.resolve().parent
        / "truth_sleeves"
        / str(execution_sleeve_id).strip().upper()
        / str(mode).strip().upper()
    ).resolve()
    return candidate


def _require_file(path: Path, code: str) -> Path:
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_file():
        raise ValueError(f"{code}:{resolved}")
    return resolved


def _governed_ref(artifact_id: str, path: Path) -> Dict[str, Any]:
    return build_governed_dependency_ref_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        path=str(path.resolve()),
        sha256=sha256_file_v1(path.resolve()),
        finality_state=FINALITY_FINALIZED,
    )


def _plain_ref(artifact_id: str, path: Path) -> Dict[str, Any]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "path": str(path.resolve()),
        "sha256": sha256_file_v1(path.resolve()),
    }


def _write_governed(*, artifact_id: str, schema_relpath: str, path: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    validate_governed_artifact_payload_v1(repo_root=REPO_ROOT, artifact_id=artifact_id, payload=payload)
    return atomic_write_idempotent_validated_json_v1(
        path=path.resolve(),
        payload=payload,
        schema_relpath=schema_relpath,
        volatile_field_names=("produced_utc",),
    )


def _input_paths(*, execution_truth_root: Path, day_utc: str, sleeve_id: str) -> Dict[str, Path]:
    day = _parse_day(day_utc)
    base = execution_truth_root.resolve()
    return {
        "reconciliation_report": (base / "reports" / "reconciliation_report_v3" / day / "reconciliation_report.v3.json").resolve(),
        "capital_authority_allocation": (base / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json").resolve(),
        "sleeve_governance_action": (base / "reports" / "sleeve_governance_action_state_v1" / day / sleeve_id / "sleeve_governance_action_state.v1.json").resolve(),
        "evaluation_input_manifest": (base / "reports" / "evaluation_input_manifest_v1" / day / sleeve_id / "evaluation_input_manifest.v1.json").resolve(),
        "outcome_attribution": (base / "reports" / "outcome_attribution_snapshot_v1" / day / sleeve_id / "outcome_attribution_snapshot.v1.json").resolve(),
        "edge_measurement": (base / "reports" / "sleeve_edge_measurement_snapshot_v1" / day / sleeve_id / "sleeve_edge_measurement_snapshot.v1.json").resolve(),
        "allocation_governance": (base / "reports" / "allocation_governance_snapshot_v1" / day / sleeve_id / "allocation_governance_snapshot.v1.json").resolve(),
        "operative_control": (base / "reports" / "sleeve_operative_control_state_v1" / day / sleeve_id / "sleeve_operative_control_state.v1.json").resolve(),
    }


def _load_inputs(
    *,
    truth_root: Path,
    execution_sleeve_id: str,
    mode: str,
    day_utc: str,
    sleeve_id: str,
    operator_intervention_state_path: str | Path | None,
) -> EvidenceInputsV1:
    canonical_truth_root = truth_root.resolve()
    execution_truth_root = _resolve_execution_truth_root(
        truth_root=canonical_truth_root,
        execution_sleeve_id=execution_sleeve_id,
        mode=mode,
    )
    if not execution_truth_root.exists() or not execution_truth_root.is_dir():
        raise ValueError(f"MISSING_EXECUTION_TRUTH_ROOT:{execution_truth_root}")

    day = _parse_day(day_utc)
    policy_ref = write_evaluation_policy_snapshot_v1(truth_root=canonical_truth_root, day_utc=day)
    policy = policy_ref.payload
    snapshot = read_sleeve_edge_snapshot_for_day_v1(
        truth_root=execution_truth_root,
        sleeve_id=sleeve_id,
        day_utc=day,
        expected_policy_version=str(policy.get("policy_version") or "").strip(),
    )
    snapshot_path = _require_file(
        Path(str(snapshot.get("artifact_path") or "")),
        "SLEEVE_EDGE_SNAPSHOT_PATH_MISSING",
    )
    fact_ref = snapshot.get("fact_ledger_ref") if isinstance(snapshot.get("fact_ledger_ref"), dict) else {}
    fact_ledger_path = _require_file(
        Path(str(fact_ref.get("artifact_path") or "")),
        "SLEEVE_EDGE_FACT_LEDGER_PATH_MISSING",
    )

    day_open_attempt_path = _require_file(
        canonical_truth_root / "reports" / "day_open_attempt_v1" / day / "day_open_attempt.v1.json",
        "DAY_OPEN_ATTEMPT_MISSING",
    )
    execution_reconciliation_path = _require_file(
        canonical_truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json",
        "EXECUTION_RECONCILIATION_MISSING",
    )
    reconciliation_report_path = _require_file(
        execution_truth_root / "reports" / "reconciliation_report_v3" / day / "reconciliation_report.v3.json",
        "RECONCILIATION_REPORT_MISSING",
    )
    capital_authority_allocation_path = _require_file(
        execution_truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json",
        "CAPITAL_AUTHORITY_ALLOCATION_MISSING",
    )
    sleeve_governance_action_path = (execution_truth_root / "reports" / "sleeve_governance_action_state_v1" / day / sleeve_id / "sleeve_governance_action_state.v1.json").resolve()
    if not sleeve_governance_action_path.exists() or not sleeve_governance_action_path.is_file():
        sleeve_governance_action_path = None

    runtime_lifecycle_ref = read_day_open_attempt_runtime_lifecycle_ref_v1(
        truth_root=canonical_truth_root,
        day_utc=day,
    )[1]

    override_path: Path | None = None
    if operator_intervention_state_path:
        override_path = _require_file(Path(operator_intervention_state_path), "OPERATOR_INTERVENTION_STATE_MISSING")
        validate_against_repo_schema_v1(
            _read_json(override_path),
            REPO_ROOT,
            OPERATOR_INTERVENTION_STATE_SCHEMA_RELPATH,
        )

    return EvidenceInputsV1(
        canonical_truth_root=canonical_truth_root,
        execution_truth_root=execution_truth_root,
        day_open_attempt_path=day_open_attempt_path,
        execution_reconciliation_path=execution_reconciliation_path,
        reconciliation_report_path=reconciliation_report_path,
        sleeve_edge_snapshot_path=snapshot_path,
        sleeve_edge_fact_ledger_path=fact_ledger_path,
        capital_authority_allocation_path=capital_authority_allocation_path,
        sleeve_governance_action_path=sleeve_governance_action_path,
        operator_intervention_state_path=override_path,
        runtime_lifecycle_ref=runtime_lifecycle_ref,
    )


def _build_manifest_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    sleeve_id: str,
    inputs: EvidenceInputsV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "evaluation_input_manifest_v1", WRITER_ID)
    day = _parse_day(day_utc)
    produced_utc = _now_utc()
    snapshot = _read_json(inputs.sleeve_edge_snapshot_path)
    source_refs = [
        {
            **_plain_ref("day_open_attempt_v1", inputs.day_open_attempt_path),
            "root_type": "canonical_truth_root",
            "scope_kind": "day",
        },
        {
            **_plain_ref("execution_reconciliation_v1", inputs.execution_reconciliation_path),
            "root_type": "canonical_truth_root",
            "scope_kind": "day",
        },
        {
            **_plain_ref("reconciliation_report_v3", inputs.reconciliation_report_path),
            "root_type": "execution_truth_root",
            "scope_kind": "day",
        },
        {
            **_plain_ref("sleeve_edge_fact_ledger_v1", inputs.sleeve_edge_fact_ledger_path),
            "root_type": "execution_truth_root",
            "scope_kind": "sleeve",
        },
        {
            **_plain_ref("sleeve_edge_snapshot_v1", inputs.sleeve_edge_snapshot_path),
            "root_type": "execution_truth_root",
            "scope_kind": "sleeve",
        },
        {
            **_plain_ref("capital_authority_allocation_v1", inputs.capital_authority_allocation_path),
            "root_type": "execution_truth_root",
            "scope_kind": "day",
        },
    ]
    if inputs.sleeve_governance_action_path is not None:
        source_refs.append(
            {
                **_plain_ref("sleeve_governance_action_state_v1", inputs.sleeve_governance_action_path),
                "root_type": "execution_truth_root",
                "scope_kind": "sleeve",
            }
        )
    if inputs.operator_intervention_state_path is not None:
        source_refs.append(
            {
                **_plain_ref("operator_intervention_state_v1", inputs.operator_intervention_state_path),
                "root_type": "execution_truth_root",
                "scope_kind": "sleeve",
            }
        )

    policy_ref_row = _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)
    dependency_refs = [
        _governed_ref("reconciliation_report_v3", inputs.reconciliation_report_path),
        _governed_ref("sleeve_edge_fact_ledger_v1", inputs.sleeve_edge_fact_ledger_path),
        _governed_ref("sleeve_edge_snapshot_v1", inputs.sleeve_edge_snapshot_path),
        _governed_ref("capital_authority_allocation_v1", inputs.capital_authority_allocation_path),
        policy_ref_row,
    ]
    bundle = build_frozen_decision_input_bundle_v1(
        artifact_type="evaluation_input_manifest_v1",
        authority_id="evaluation_input_manifest_v1",
        generated_at_utc=produced_utc,
        effective_at_utc=f"{day}T00:00:00Z",
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        run_id=f"evaluation_input_manifest:{day}:{sleeve_id}",
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="evaluation_input_manifest_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="evaluation_input_manifest_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="evaluation_input_manifest_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="evaluation_input_manifest_v1",
        producer_id=WRITER_ID,
        generated_at_utc=produced_utc,
        effective_at_utc=f"{day}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        code_version=repo_git_sha_v1(),
        run_id=f"evaluation_input_manifest:{day}:{sleeve_id}",
    )
    payload = {
        "schema_id": "C2_EVALUATION_INPUT_MANIFEST_V1",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": str(sleeve_id).strip(),
        "measurement_window": {
            "window_kind": str(policy_ref.payload.get("window_policy", {}).get("window_kind") or "WEEKLY"),
            "sample_basis": str(policy_ref.payload.get("window_policy", {}).get("sample_basis") or "CLOSED_TRADES_ONLY"),
            "source_as_of_ts": str(snapshot.get("as_of_ts") or produced_utc),
        },
        "evidence_summary": {
            "included_trade_count": len(snapshot.get("included_trade_ids") or []),
            "excluded_trade_count": len(snapshot.get("excluded_trade_ids") or []),
            "sample_trade_count": int((snapshot.get("factual_metrics") or {}).get("sample_count") or 0),
            "source_artifact_count": len(source_refs),
        },
        "source_evidence_refs": source_refs,
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "frozen_input_bundle": bundle,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }
    if inputs.runtime_lifecycle_ref is not None:
        payload["runtime_lifecycle_ref"] = dict(inputs.runtime_lifecycle_ref)
    return payload


def _build_attribution_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    sleeve_id: str,
    inputs: EvidenceInputsV1,
    manifest_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "outcome_attribution_snapshot_v1", WRITER_ID)
    day = _parse_day(day_utc)
    produced_utc = _now_utc()
    snapshot = _read_json(inputs.sleeve_edge_snapshot_path)
    factual_metrics = snapshot.get("factual_metrics") if isinstance(snapshot.get("factual_metrics"), dict) else {}
    attribution_policy = dict(policy_ref.payload.get("outcome_attribution_policy") or {})
    sample_count = int(factual_metrics.get("sample_count") or 0)
    unknown_count = int(factual_metrics.get("unknown_attribution_count") or 0)
    high_sample = int(attribution_policy.get("high_confidence_min_sample_count") or 5)
    medium_sample = int(attribution_policy.get("medium_confidence_min_sample_count") or 2)
    high_unknown = int(attribution_policy.get("max_unknown_attribution_for_high_confidence") or 0)
    medium_unknown = int(attribution_policy.get("max_unknown_attribution_for_medium_confidence") or 1)
    execution_state = str((factual_metrics.get("execution_data_completeness") or {}).get("state") or "").strip().upper()
    invalidity = [str(code).strip() for code in factual_metrics.get("invalidity_reasons") or [] if str(code).strip()]

    if invalidity or execution_state not in {"COMPLETE", ""}:
        confidence_state = "INVALID"
    elif sample_count >= high_sample and unknown_count <= high_unknown:
        confidence_state = "HIGH"
    elif sample_count >= medium_sample and unknown_count <= medium_unknown:
        confidence_state = "MEDIUM"
    else:
        confidence_state = "LOW"

    included_count = len(snapshot.get("included_trade_ids") or [])
    if sample_count == 0:
        residual_state = "NOT_ENOUGH_EVIDENCE"
    elif unknown_count == 0:
        residual_state = "FULLY_EXPLAINED"
    elif unknown_count < included_count:
        residual_state = "PARTIALLY_EXPLAINED"
    else:
        residual_state = "UNEXPLAINED"

    reason_codes = list(invalidity)
    if unknown_count > 0:
        reason_codes.append("OUTCOME_ATTRIBUTION_UNKNOWN_COMPONENT_PRESENT")
    if residual_state == "FULLY_EXPLAINED":
        reason_codes.append("OUTCOME_ATTRIBUTION_FULLY_EXPLAINED")
    if confidence_state == "INVALID":
        reason_codes.append("OUTCOME_ATTRIBUTION_CONFIDENCE_INVALID")

    dependency_refs = [
        _governed_ref("evaluation_input_manifest_v1", manifest_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        _governed_ref("sleeve_edge_snapshot_v1", inputs.sleeve_edge_snapshot_path),
        _governed_ref("sleeve_edge_fact_ledger_v1", inputs.sleeve_edge_fact_ledger_path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="outcome_attribution_snapshot_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="outcome_attribution_snapshot_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="outcome_attribution_snapshot_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="outcome_attribution_snapshot_v1",
        producer_id=WRITER_ID,
        generated_at_utc=produced_utc,
        effective_at_utc=f"{day}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"outcome_attribution_snapshot:{day}:{sleeve_id}",
    )
    return {
        "schema_id": "C2_OUTCOME_ATTRIBUTION_SNAPSHOT_V1",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": str(sleeve_id).strip(),
        "evaluation_input_manifest_ref": _plain_ref("evaluation_input_manifest_v1", manifest_ref.path),
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "measurement_summary": {
            "sample_trade_count": sample_count,
            "included_trade_count": included_count,
            "excluded_trade_count": len(snapshot.get("excluded_trade_ids") or []),
        },
        "attribution_components": {
            "native_entry_net_pnl": str(factual_metrics.get("native_net_pnl") or "0"),
            "adopted_management_net_pnl": str(factual_metrics.get("adopted_net_pnl") or "0"),
            "fee_drag": str(factual_metrics.get("fee_drag") or "0"),
            "measured_slippage_drag": dict(factual_metrics.get("measured_slippage_drag") or {}),
            "unknown_attribution_count": unknown_count,
        },
        "confidence_state": confidence_state,
        "residual_state": residual_state,
        "reason_codes": sorted(set(reason_codes)),
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_edge_measurement_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    sleeve_id: str,
    inputs: EvidenceInputsV1,
    manifest_ref: SurfaceRefV1,
    attribution_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_edge_measurement_snapshot_v1", WRITER_ID)
    day = _parse_day(day_utc)
    produced_utc = _now_utc()
    snapshot = _read_json(inputs.sleeve_edge_snapshot_path)
    qualification = snapshot.get("qualification") if isinstance(snapshot.get("qualification"), dict) else {}
    factual_metrics = snapshot.get("factual_metrics") if isinstance(snapshot.get("factual_metrics"), dict) else {}
    measurement_policy = dict(policy_ref.payload.get("sleeve_edge_measurement_policy") or {})
    stability_map = dict(measurement_policy.get("stability_from_drift_band") or {})
    confidence_map = dict(measurement_policy.get("confidence_from_sample_band") or {})
    drift_band = str(qualification.get("drift_band") or "UNKNOWN").strip().upper()
    sample_band = str(qualification.get("sample_sufficiency_band") or "INSUFFICIENT").strip().upper()
    stability_state = str(stability_map.get(drift_band) or "unstable")
    confidence_state = str(confidence_map.get(sample_band) or "LOW").strip().upper()
    if str(attribution_ref.payload.get("confidence_state") or "").strip().upper() == "INVALID":
        confidence_state = "INVALID"

    dependency_refs = [
        _governed_ref("evaluation_input_manifest_v1", manifest_ref.path),
        _governed_ref("outcome_attribution_snapshot_v1", attribution_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        _governed_ref("sleeve_edge_snapshot_v1", inputs.sleeve_edge_snapshot_path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_edge_measurement_snapshot_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_edge_measurement_snapshot_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_edge_measurement_snapshot_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_edge_measurement_snapshot_v1",
        producer_id=WRITER_ID,
        generated_at_utc=produced_utc,
        effective_at_utc=f"{day}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_edge_measurement_snapshot:{day}:{sleeve_id}",
    )
    return {
        "schema_id": "C2_SLEEVE_EDGE_MEASUREMENT_SNAPSHOT_V1",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": str(sleeve_id).strip(),
        "evaluation_input_manifest_ref": _plain_ref("evaluation_input_manifest_v1", manifest_ref.path),
        "outcome_attribution_ref": _plain_ref("outcome_attribution_snapshot_v1", attribution_ref.path),
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "legacy_measurement_ref": _plain_ref("sleeve_edge_snapshot_v1", inputs.sleeve_edge_snapshot_path),
        "measurement_window": dict(snapshot.get("metric_window") or {}),
        "sample_trade_count": int(factual_metrics.get("sample_count") or 0),
        "edge_measurement": {
            "native_net_expectancy": str(factual_metrics.get("native_net_expectancy") or "0"),
            "adopted_management_expectancy": str(factual_metrics.get("adopted_management_expectancy") or "0"),
            "qualification_state": str(qualification.get("qualification_state") or ""),
            "edge_band": str(qualification.get("edge_band") or ""),
        },
        "stability_measurement": {
            "drift_band": str(qualification.get("drift_band") or ""),
            "stability_state": stability_state,
        },
        "confidence_measurement": {
            "confidence_state": confidence_state,
            "sample_sufficiency_band": str(qualification.get("sample_sufficiency_band") or ""),
            "execution_health_band": str(qualification.get("execution_health_band") or ""),
        },
        "reason_codes": sorted(
            set(
                [str(code).strip() for code in qualification.get("reason_codes") or [] if str(code).strip()]
                + [str(code).strip() for code in attribution_ref.payload.get("reason_codes") or [] if str(code).strip()]
            )
        ),
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _sleeve_control_row(allocation_payload: Mapping[str, Any], sleeve_id: str) -> Dict[str, Any] | None:
    control_state = allocation_payload.get("governed_evaluation_control_state")
    if not isinstance(control_state, dict):
        return None
    for row in control_state.get("sleeve_controls") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("scope_id") or "").strip() == str(sleeve_id).strip():
            return dict(row)
    return None


def _build_allocation_governance_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    sleeve_id: str,
    inputs: EvidenceInputsV1,
    manifest_ref: SurfaceRefV1,
    measurement_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "allocation_governance_snapshot_v1", WRITER_ID)
    day = _parse_day(day_utc)
    produced_utc = _now_utc()
    allocation_payload = _read_json(inputs.capital_authority_allocation_path)
    control_row = _sleeve_control_row(allocation_payload, sleeve_id)
    measurement_policy = dict(policy_ref.payload.get("allocation_governance_policy") or {})
    review_states = {str(item).strip() for item in measurement_policy.get("review_required_measurement_states") or [] if str(item).strip()}
    restrictive_states = {str(item).strip() for item in measurement_policy.get("restrictive_measurement_states") or [] if str(item).strip()}
    allow_actions = {str(item).strip() for item in measurement_policy.get("allow_alignment_action_states") or [] if str(item).strip()}
    restrictive_actions = {str(item).strip() for item in measurement_policy.get("restrictive_action_states") or [] if str(item).strip()}
    quality_state = str(measurement_ref.payload.get("edge_measurement", {}).get("qualification_state") or "").strip()
    confidence_state = str(measurement_ref.payload.get("confidence_measurement", {}).get("confidence_state") or "").strip().upper()
    if control_row is None:
        current_action = "no_conclusion"
        current_control_context = {
            "adoption_state": "UNAVAILABLE",
            "action_state": current_action,
            "control_state": "unavailable",
            "headroom_multiplier_bp": 0,
            "diagnostic": "CAPITAL_AUTHORITY_GOVERNED_CONTROL_STATE_UNAVAILABLE",
        }
        recommended_action = "no_conclusion"
        alignment_state = "CONTROL_GAP"
        recommendation_reason_codes = [
            "ALLOCATION_GOVERNANCE_CONTROL_CONTEXT_UNAVAILABLE",
            "ALLOCATION_GOVERNANCE_RECOMMENDED_NO_CONCLUSION",
        ]
    else:
        current_action = str(control_row.get("action_state") or "").strip()
        current_control_context = {
            "adoption_state": str(control_row.get("adoption_state") or ""),
            "action_state": current_action,
            "control_state": str(control_row.get("control_state") or ""),
            "headroom_multiplier_bp": int(control_row.get("headroom_multiplier_bp") or 0),
            "diagnostic": str(control_row.get("diagnostic") or ""),
        }
        if quality_state in restrictive_states:
            recommended_action = "pause" if quality_state == "DISABLED" else "reduce"
        elif quality_state in review_states or confidence_state in {"LOW", "INVALID"}:
            recommended_action = "watch"
        else:
            recommended_action = "continue"
        if recommended_action == "continue" and current_action in allow_actions:
            alignment_state = "ALIGNED"
        elif recommended_action != "continue" and current_action in restrictive_actions.union({"watch"}):
            alignment_state = "ALIGNED"
        elif recommended_action == "watch":
            alignment_state = "REVIEW_REQUIRED"
        else:
            alignment_state = "CONTROL_GAP"
        recommendation_reason_codes = [
            f"ALLOCATION_GOVERNANCE_ALIGNMENT_{alignment_state}",
            f"ALLOCATION_GOVERNANCE_RECOMMENDED_{recommended_action.upper()}",
        ]
    state_class_map = dict((policy_ref.payload.get("operative_control_policy") or {}).get("state_class_from_action") or {})
    recommended_state_class = str(state_class_map.get(recommended_action) or "REVIEW_REQUIRED")
    dependency_refs = [
        _governed_ref("evaluation_input_manifest_v1", manifest_ref.path),
        _governed_ref("sleeve_edge_measurement_snapshot_v1", measurement_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        _governed_ref("capital_authority_allocation_v1", inputs.capital_authority_allocation_path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="allocation_governance_snapshot_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="allocation_governance_snapshot_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="allocation_governance_snapshot_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="allocation_governance_snapshot_v1",
        producer_id=WRITER_ID,
        generated_at_utc=produced_utc,
        effective_at_utc=f"{day}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"allocation_governance_snapshot:{day}:{sleeve_id}",
    )
    return {
        "schema_id": "C2_ALLOCATION_GOVERNANCE_SNAPSHOT_V1",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": str(sleeve_id).strip(),
        "evaluation_input_manifest_ref": _plain_ref("evaluation_input_manifest_v1", manifest_ref.path),
        "measurement_ref": _plain_ref("sleeve_edge_measurement_snapshot_v1", measurement_ref.path),
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "capital_authority_allocation_ref": _plain_ref("capital_authority_allocation_v1", inputs.capital_authority_allocation_path),
        "current_control_context": current_control_context,
        "allocation_alignment_state": alignment_state,
        "recommended_action_state": recommended_action,
        "recommended_state_class": recommended_state_class,
        "recommendation_reason_codes": recommendation_reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _load_override_ref(path: Path | None) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
    if path is None:
        return None, None
    payload = _read_json(path)
    return _plain_ref("operator_intervention_state_v1", path), payload


def _build_operative_control_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    sleeve_id: str,
    inputs: EvidenceInputsV1,
    measurement_ref: SurfaceRefV1,
    allocation_governance_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_operative_control_state_v1", WRITER_ID)
    day = _parse_day(day_utc)
    produced_utc = _now_utc()
    operative_policy = dict(policy_ref.payload.get("operative_control_policy") or {})
    allocation_payload = allocation_governance_ref.payload
    recommended_action = str(allocation_payload.get("recommended_action_state") or "watch").strip()
    state_class_map = dict(operative_policy.get("state_class_from_action") or {})
    override_ref, override_payload = _load_override_ref(inputs.operator_intervention_state_path)
    if override_payload is not None and str(override_payload.get("override_status") or "").strip().upper() == "ACTIVE":
        state_class = str(operative_policy.get("override_active_state_class") or "OVERRIDE")
        reason_codes = ["OPERATIVE_CONTROL_OPERATOR_OVERRIDE_ACTIVE"]
    else:
        state_class = str(state_class_map.get(recommended_action) or "REVIEW_REQUIRED")
        reason_codes = [f"OPERATIVE_CONTROL_RECOMMENDED_{recommended_action.upper()}"]
    governance_action_ref = (
        None
        if inputs.sleeve_governance_action_path is None
        else _plain_ref("sleeve_governance_action_state_v1", inputs.sleeve_governance_action_path)
    )
    dependency_refs = [
        _governed_ref("allocation_governance_snapshot_v1", allocation_governance_ref.path),
        _governed_ref("sleeve_edge_measurement_snapshot_v1", measurement_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_operative_control_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_operative_control_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_operative_control_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_operative_control_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=produced_utc,
        effective_at_utc=f"{day}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_operative_control_state:{day}:{sleeve_id}",
    )
    return {
        "schema_id": "C2_SLEEVE_OPERATIVE_CONTROL_STATE_V1",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": str(sleeve_id).strip(),
        "state_mode": str(operative_policy.get("state_mode") or "PROPOSED_ONLY"),
        "state_class": state_class,
        "binding_state": str(operative_policy.get("binding_state_default") or "NOT_BOUND_LIVE"),
        "effective_from": f"{day}T00:00:00Z",
        "superseded_by": None,
        "allocation_governance_ref": _plain_ref("allocation_governance_snapshot_v1", allocation_governance_ref.path),
        "measurement_ref": _plain_ref("sleeve_edge_measurement_snapshot_v1", measurement_ref.path),
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "governance_action_ref": governance_action_ref,
        "operator_override_ref": override_ref,
        "reason_codes": reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def materialize_evaluation_authority_slice_v1(
    *,
    truth_root: Path,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    sleeve_id: str,
    operator_intervention_state_path: str | Path | None = None,
) -> Dict[str, Any]:
    canonical_truth_root = Path(truth_root).resolve()
    day = _parse_day(day_utc)
    inputs = _load_inputs(
        truth_root=canonical_truth_root,
        execution_sleeve_id=execution_sleeve_id,
        mode=mode,
        day_utc=day,
        sleeve_id=sleeve_id,
        operator_intervention_state_path=operator_intervention_state_path,
    )
    policy_ref = write_evaluation_policy_snapshot_v1(truth_root=canonical_truth_root, day_utc=day)
    paths = _input_paths(
        execution_truth_root=inputs.execution_truth_root,
        day_utc=day,
        sleeve_id=sleeve_id,
    )
    manifest_ref = _write_governed(
        artifact_id="evaluation_input_manifest_v1",
        schema_relpath=INPUT_MANIFEST_SCHEMA,
        path=paths["evaluation_input_manifest"],
        payload=_build_manifest_payload(
            day_utc=day,
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
            sleeve_id=sleeve_id,
            inputs=inputs,
            policy_ref=policy_ref,
        ),
    )
    attribution_ref = _write_governed(
        artifact_id="outcome_attribution_snapshot_v1",
        schema_relpath=OUTCOME_ATTRIBUTION_SCHEMA,
        path=paths["outcome_attribution"],
        payload=_build_attribution_payload(
            day_utc=day,
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
            sleeve_id=sleeve_id,
            inputs=inputs,
            manifest_ref=manifest_ref,
            policy_ref=policy_ref,
        ),
    )
    measurement_ref = _write_governed(
        artifact_id="sleeve_edge_measurement_snapshot_v1",
        schema_relpath=EDGE_MEASUREMENT_SCHEMA,
        path=paths["edge_measurement"],
        payload=_build_edge_measurement_payload(
            day_utc=day,
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
            sleeve_id=sleeve_id,
            inputs=inputs,
            manifest_ref=manifest_ref,
            attribution_ref=attribution_ref,
            policy_ref=policy_ref,
        ),
    )
    allocation_governance_ref = _write_governed(
        artifact_id="allocation_governance_snapshot_v1",
        schema_relpath=ALLOCATION_GOVERNANCE_SCHEMA,
        path=paths["allocation_governance"],
        payload=_build_allocation_governance_payload(
            day_utc=day,
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
            sleeve_id=sleeve_id,
            inputs=inputs,
            manifest_ref=manifest_ref,
            measurement_ref=measurement_ref,
            policy_ref=policy_ref,
        ),
    )
    operative_control_ref = _write_governed(
        artifact_id="sleeve_operative_control_state_v1",
        schema_relpath=OPERATIVE_CONTROL_SCHEMA,
        path=paths["operative_control"],
        payload=_build_operative_control_payload(
            day_utc=day,
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
            sleeve_id=sleeve_id,
            inputs=inputs,
            measurement_ref=measurement_ref,
            allocation_governance_ref=allocation_governance_ref,
            policy_ref=policy_ref,
        ),
    )
    return {
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "evaluation_input_manifest_ref": _plain_ref("evaluation_input_manifest_v1", manifest_ref.path),
        "outcome_attribution_ref": _plain_ref("outcome_attribution_snapshot_v1", attribution_ref.path),
        "sleeve_edge_measurement_ref": _plain_ref("sleeve_edge_measurement_snapshot_v1", measurement_ref.path),
        "allocation_governance_ref": _plain_ref("allocation_governance_snapshot_v1", allocation_governance_ref.path),
        "operative_control_ref": _plain_ref("sleeve_operative_control_state_v1", operative_control_ref.path),
    }


__all__ = ["materialize_evaluation_authority_slice_v1"]
