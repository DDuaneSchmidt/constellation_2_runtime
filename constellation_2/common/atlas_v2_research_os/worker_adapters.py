from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ops.atlas.v2_cheap_experiment_generator import build_spec_payload, normalize_research_hypothesis
from ops.atlas.v2_claim_idea_generator import build_generated_claim_payload
from ops.atlas.v2_claim_to_hypothesis_generator import GENERATED_STATUS, build_hypothesis_payload
from ops.atlas.v2_external_strategy_claim_extractor import short_hash
from ops.atlas.v2_learning_estimator import attention_priority_score

from .artifact_models import ArtifactType, EvidenceLevel, LifecycleState
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .worker_connection_rules import is_worker_connection_allowed
from .worker_execution_records import record_from_worker_result, write_worker_execution_record
from .worker_governance import (
    audit_worker_outputs,
    validate_connected_worker_no_authority_escalation,
    validate_connected_worker_outputs,
    validate_worker_connection,
)
from .worker_interfaces import (
    AttentionWorker,
    ClaimWorker,
    EvaluationWorker,
    ExperimentDesignWorker,
    ExperimentExecutionWorker,
    HypothesisWorker,
    LearningWorker,
    MemoryCuratorWorker,
    ResearchOSWorker,
)
from .worker_models import WorkerRunResult, WorkerRunStatus
from .worker_registry import register_worker


class ResearchOSWorkerAdapter(ResearchOSWorker):
    adapter_status = WorkerRunStatus.NOT_CONNECTED.value
    source_component = ""

    def validate_inputs(self, input_artifacts: list[dict[str, Any]]) -> tuple[bool, list[str]]:
        allowed = set(self.supported_input_artifact_types)
        failures = []
        if not input_artifacts:
            failures.append("at least one input artifact is required")
        for artifact in input_artifacts:
            artifact_type = str(artifact.get("artifact_type", ""))
            if artifact_type not in allowed:
                failures.append(f"unsupported input artifact type: {artifact_type}")
            if not artifact.get("artifact_id"):
                failures.append("input artifact missing artifact_id")
        return not failures, failures

    def validate_outputs(self, output_artifacts: list[dict[str, Any]], input_artifacts: list[dict[str, Any]] | None = None) -> tuple[bool, list[str]]:
        allowed = set(self.supported_output_artifact_types)
        failures = []
        for artifact in output_artifacts:
            artifact_type = str(artifact.get("artifact_type", ""))
            if artifact_type not in allowed:
                failures.append(f"unsupported output artifact type: {artifact_type}")
        governance = validate_connected_worker_outputs(output_artifacts, input_artifacts or [])
        failures.extend(governance.violations)
        authority = validate_connected_worker_no_authority_escalation(output_artifacts)
        failures.extend(authority.violations)
        return not failures, failures

    def dry_run(self, input_artifacts: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None) -> WorkerRunResult:
        if not input_artifacts and (metadata or {}).get("report_mode") == "worker_interface_audit":
            return self._result(input_artifacts, [], WorkerRunStatus.DRY_RUN.value, metadata=metadata)
        if self.adapter_status in {WorkerRunStatus.CONNECTED_READ_ONLY.value, WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value}:
            return self._connected_execute(input_artifacts, dry_run=True, metadata=metadata)
        return self._result(input_artifacts, [], WorkerRunStatus.DRY_RUN.value, metadata=metadata)

    def run(self, input_artifacts: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None) -> WorkerRunResult:
        if self.adapter_status in {WorkerRunStatus.CONNECTED_READ_ONLY.value, WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value}:
            return self._connected_execute(input_artifacts, dry_run=False, metadata=metadata)
        ok, failures = self.validate_inputs(input_artifacts)
        if not ok:
            return self._result(input_artifacts, [], WorkerRunStatus.VALIDATION_FAILED.value, errors=failures, metadata=metadata)
        return self._result(input_artifacts, [], self.adapter_status, metadata=metadata)

    def _connected_execute(self, input_artifacts: list[dict[str, Any]], *, dry_run: bool, metadata: dict[str, Any] | None = None) -> WorkerRunResult:
        run_metadata = dict(metadata or {})
        ok, failures = self.validate_inputs(input_artifacts)
        if not ok:
            result = self._result(input_artifacts, [], WorkerRunStatus.VALIDATION_FAILED.value, errors=failures, metadata=run_metadata)
            self._write_execution_record(result, run_metadata)
            return result
        connection_result = validate_worker_connection(self)
        if connection_result.status != "PASS" or not is_worker_connection_allowed(self):
            result = self._result(input_artifacts, [], WorkerRunStatus.VALIDATION_FAILED.value, errors=connection_result.violations or ["worker connection disallowed"], metadata=run_metadata)
            self._write_execution_record(result, run_metadata)
            return result
        started_at = _now()
        worker_run_id = f"{self.worker_id}:{started_at}"
        try:
            output_specs = self._build_output_specs(input_artifacts, started_at=started_at, worker_run_id=worker_run_id, metadata=run_metadata)
        except Exception as exc:
            result = self._result(input_artifacts, [], WorkerRunStatus.VALIDATION_FAILED.value, errors=[f"{exc.__class__.__name__}: {exc}"], metadata=run_metadata, started_at=started_at, worker_run_id=worker_run_id)
            self._write_execution_record(result, run_metadata)
            return result
        valid, output_failures = self.validate_outputs(output_specs, input_artifacts)
        if not valid:
            result = self._result(input_artifacts, output_specs, WorkerRunStatus.VALIDATION_FAILED.value, errors=output_failures, metadata=run_metadata, started_at=started_at, worker_run_id=worker_run_id)
            self._write_execution_record(result, run_metadata)
            return result
        if dry_run:
            preview_metadata = dict(run_metadata)
            preview_metadata["dry_run_output_preview"] = output_specs
            result = self._result(input_artifacts, output_specs, WorkerRunStatus.DRY_RUN.value, metadata=preview_metadata, started_at=started_at, worker_run_id=worker_run_id)
            self._write_execution_record(result, preview_metadata)
            return result
        store = ArtifactStore(Path(run_metadata.get("root") or run_metadata.get("artifact_store_root") or DEFAULT_STORE_ROOT))
        created = []
        for spec in output_specs:
            payload = dict(spec)
            payload["artifact_id"] = _unique_artifact_id(store, str(payload["artifact_id"]))
            artifact = store.create_artifact(
                artifact_id=payload["artifact_id"],
                artifact_type=payload["artifact_type"],
                created_at=payload["created_at"],
                created_by=payload["created_by"],
                source_artifact_ids=list(payload.get("source_artifact_ids") or []),
                supersedes_artifact_ids=list(payload.get("supersedes_artifact_ids") or []),
                confidence=float(payload.get("confidence") or 0.0),
                evidence_level=str(payload.get("evidence_level") or EvidenceLevel.GENERATED_ONLY.value),
                lifecycle_state=str(payload.get("lifecycle_state") or LifecycleState.NEW.value),
                labels=list(payload.get("labels") or []),
                metadata=dict(payload.get("metadata") or {}),
                is_root=False,
            )
            created.append(artifact.to_dict())
        result = self._result(input_artifacts, created, WorkerRunStatus.COMPLETED.value, metadata=run_metadata, started_at=started_at, worker_run_id=worker_run_id)
        self._write_execution_record(result, run_metadata)
        return result

    def _build_output_specs(self, input_artifacts: list[dict[str, Any]], *, started_at: str, worker_run_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        return []

    def _result(
        self,
        input_artifacts: list[dict[str, Any]],
        output_artifacts: list[dict[str, Any]],
        status: str,
        *,
        errors: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        started_at: str | None = None,
        worker_run_id: str | None = None,
    ) -> WorkerRunResult:
        timestamp = started_at or _now()
        completed_at = _now()
        governance_result, lineage_result = audit_worker_outputs(input_artifacts, output_artifacts)
        run_metadata = {
            "adapter_status": self.adapter_status,
            "source_component": self.source_component,
            "design_status": WorkerRunStatus.DESIGN_READY.value,
            "research_connected": self.adapter_status == WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value,
            "read_only_connected": self.adapter_status == WorkerRunStatus.CONNECTED_READ_ONLY.value,
            "behavior_changed": False,
        }
        run_metadata.update(metadata or {})
        return WorkerRunResult(
            worker_run_id=worker_run_id or f"{self.worker_id}:{timestamp}",
            worker_id=self.worker_id,
            started_at=timestamp,
            completed_at=completed_at,
            status=status,
            input_artifact_ids=[str(item.get("artifact_id", "")) for item in input_artifacts if item.get("artifact_id")],
            output_artifact_ids=[str(item.get("artifact_id", "")) for item in output_artifacts if item.get("artifact_id")],
            errors=errors or [],
            warnings=[],
            governance_result=governance_result.to_dict(),
            lineage_result=lineage_result.to_dict(),
            metadata=run_metadata,
        )

    def _write_execution_record(self, result: WorkerRunResult, metadata: dict[str, Any]) -> None:
        root = Path(metadata.get("root") or metadata.get("artifact_store_root") or DEFAULT_STORE_ROOT)
        write_worker_execution_record(record_from_worker_result(result, worker_type=self.worker_type), root)


class AtlasClaimWorkerAdapter(ResearchOSWorkerAdapter, ClaimWorker):
    worker_id = "atlas_v2_claim_worker_adapter"
    adapter_status = WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value
    supported_input_artifact_types = [ArtifactType.QUESTION.value, ArtifactType.EXPERIENCE_EVENT.value]
    supported_output_artifact_types = [ArtifactType.GENERATED_RESEARCH_CLAIM.value]
    source_component = "ops/atlas/v2_claim_idea_generator.py"

    def _build_output_specs(self, input_artifacts: list[dict[str, Any]], *, started_at: str, worker_run_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        source = _claim_source_from_artifact(input_artifacts[0])
        payload = build_generated_claim_payload(source, created_at=started_at)
        input_ids = _input_ids(input_artifacts)
        return [_artifact_spec(
            artifact_id=f"ros-{payload['generated_claim_id']}-{short_hash(worker_run_id)}",
            artifact_type=ArtifactType.GENERATED_RESEARCH_CLAIM.value,
            created_at=started_at,
            created_by=self.worker_id,
            source_artifact_ids=input_ids,
            confidence=float(input_artifacts[0].get("confidence") or payload.get("uncertainty_score") or 0.1),
            evidence_level=_output_evidence(input_artifacts),
            labels=_output_labels(input_artifacts, "claim_worker_output"),
            metadata={
                "atlas_component_payload": payload,
                "source_worker_id": self.worker_id,
                "source_component": self.source_component,
                "source_evidence_levels": _source_evidence_levels(input_artifacts),
                "lineage_preserved": True,
                "evidence_labels_preserved": True,
                "governance_preserved": True,
                "lifecycle_metadata_preserved": True,
            },
        )]


class AtlasHypothesisWorkerAdapter(ResearchOSWorkerAdapter, HypothesisWorker):
    worker_id = "atlas_v2_hypothesis_worker_adapter"
    adapter_status = WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value
    supported_input_artifact_types = [ArtifactType.GENERATED_RESEARCH_CLAIM.value]
    supported_output_artifact_types = [ArtifactType.RESEARCH_HYPOTHESIS.value]
    source_component = "ops/atlas/v2_claim_to_hypothesis_generator.py"

    def _build_output_specs(self, input_artifacts: list[dict[str, Any]], *, started_at: str, worker_run_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        claim = _external_claim_from_artifact(input_artifacts[0])
        mechanism = _mechanism_from_claim_artifact(input_artifacts[0], claim)
        contrarian = {
            "contrarian_id": f"ctr-{short_hash(claim['claim_id'])}",
            "claim_id": claim["claim_id"],
            "mechanism_id": mechanism["mechanism_id"],
            "primary_failure_modes": ["INSUFFICIENT_SAMPLE", "UNOBSERVED_REGIME"],
            "required_falsification_tests": ["Replay against matched baseline observations."],
            "status": "GENERATED",
        }
        registry = {"mechanism_family": mechanism["mechanism_family"], "claim_count": 1}
        payload = build_hypothesis_payload(claim, mechanism, contrarian, registry, status=GENERATED_STATUS, created_at=started_at)
        return [_artifact_spec(
            artifact_id=f"ros-{payload['hypothesis_id']}-{short_hash(worker_run_id)}",
            artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value,
            created_at=started_at,
            created_by=self.worker_id,
            source_artifact_ids=_input_ids(input_artifacts),
            confidence=float(payload.get("confidence") or input_artifacts[0].get("confidence") or 0.1),
            evidence_level=_output_evidence(input_artifacts),
            labels=_output_labels(input_artifacts, "hypothesis_worker_output"),
            metadata={
                "atlas_component_payload": payload,
                "source_claim_artifact_id": input_artifacts[0]["artifact_id"],
                "source_worker_id": self.worker_id,
                "source_component": self.source_component,
                "source_evidence_levels": _source_evidence_levels(input_artifacts),
                "evidence_maturity_required": True,
                "lineage_preserved": True,
                "governance_preserved": True,
            },
        )]


class AtlasExperimentDesignWorkerAdapter(ResearchOSWorkerAdapter, ExperimentDesignWorker):
    worker_id = "atlas_v2_experiment_design_worker_adapter"
    adapter_status = WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value
    supported_input_artifact_types = [ArtifactType.RESEARCH_HYPOTHESIS.value, ArtifactType.QUESTION.value]
    supported_output_artifact_types = [ArtifactType.CHEAP_EXPERIMENT_SPEC.value]
    source_component = "ops/atlas/v2_cheap_experiment_generator.py"

    def _build_output_specs(self, input_artifacts: list[dict[str, Any]], *, started_at: str, worker_run_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        hypothesis = _hypothesis_payload_from_artifact(input_artifacts[0])
        payload = build_spec_payload(normalize_research_hypothesis(hypothesis), index=1, created_at=started_at)
        return [_artifact_spec(
            artifact_id=f"ros-{payload['experiment_spec_id']}-{short_hash(worker_run_id)}",
            artifact_type=ArtifactType.CHEAP_EXPERIMENT_SPEC.value,
            created_at=started_at,
            created_by=self.worker_id,
            source_artifact_ids=_input_ids(input_artifacts),
            confidence=float(input_artifacts[0].get("confidence") or 0.1),
            evidence_level=_output_evidence(input_artifacts),
            labels=_output_labels(input_artifacts, "experiment_design_worker_output"),
            metadata={
                "atlas_component_payload": payload,
                "source_hypothesis_artifact_id": input_artifacts[0]["artifact_id"],
                "execution_requested": False,
                "source_worker_id": self.worker_id,
                "source_component": self.source_component,
                "source_evidence_levels": _source_evidence_levels(input_artifacts),
                "lineage_preserved": True,
                "evidence_labels_preserved": True,
            },
        )]


class AtlasExperimentExecutionWorkerAdapter(ResearchOSWorkerAdapter, ExperimentExecutionWorker):
    worker_id = "atlas_v2_experiment_execution_worker_adapter"
    supported_input_artifact_types = [ArtifactType.CHEAP_EXPERIMENT_SPEC.value]
    supported_output_artifact_types = [ArtifactType.EXPERIMENT_RESULT.value, ArtifactType.EXPERIENCE_EVENT.value]
    source_component = "ops/atlas/v2_cheap_experiment_executor.py"


class AtlasLearningWorkerAdapter(ResearchOSWorkerAdapter, LearningWorker):
    worker_id = "atlas_v2_learning_worker_adapter"
    adapter_status = WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value
    supported_input_artifact_types = [ArtifactType.EXPERIMENT_RESULT.value, ArtifactType.EXPERIENCE_EVENT.value]
    supported_output_artifact_types = [ArtifactType.LEARNING_ESTIMATE.value]
    source_component = "ops/atlas/v2_learning_estimator.py"

    def _build_output_specs(self, input_artifacts: list[dict[str, Any]], *, started_at: str, worker_run_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        source = input_artifacts[0]
        estimate = _learning_estimate_payload(source, started_at=started_at, worker_run_id=worker_run_id)
        return [_artifact_spec(
            artifact_id=f"ros-{estimate['estimate_id']}-{short_hash(worker_run_id)}",
            artifact_type=ArtifactType.LEARNING_ESTIMATE.value,
            created_at=started_at,
            created_by=self.worker_id,
            source_artifact_ids=_input_ids(input_artifacts),
            confidence=float(source.get("confidence") or estimate["expected_learning_value"]),
            evidence_level=_output_evidence(input_artifacts),
            labels=_output_labels(input_artifacts, "learning_worker_output"),
            metadata={
                "atlas_component_payload": estimate,
                "source_worker_id": self.worker_id,
                "source_component": self.source_component,
                "source_evidence_levels": _source_evidence_levels(input_artifacts),
                "evidence_maturity_level": _output_evidence(input_artifacts),
                "lineage_preserved": True,
                "lifecycle_metadata_preserved": True,
            },
        )]


class AtlasEvaluationWorkerAdapter(ResearchOSWorkerAdapter, EvaluationWorker):
    worker_id = "atlas_v2_evaluation_worker_adapter"
    adapter_status = WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value
    supported_input_artifact_types = [ArtifactType.LEARNING_ESTIMATE.value, ArtifactType.EXPERIMENT_RESULT.value]
    supported_output_artifact_types = [ArtifactType.LEARNING_ESTIMATE_EVALUATION.value]
    source_component = "ops/atlas/v2_learning_estimator.py"

    def _build_output_specs(self, input_artifacts: list[dict[str, Any]], *, started_at: str, worker_run_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        estimate_artifact = next((item for item in input_artifacts if item.get("artifact_type") == ArtifactType.LEARNING_ESTIMATE.value), input_artifacts[0])
        source_artifact = next((item for item in input_artifacts if item is not estimate_artifact), estimate_artifact)
        evaluation = _learning_evaluation_payload(estimate_artifact, source_artifact, started_at=started_at, worker_run_id=worker_run_id)
        return [_artifact_spec(
            artifact_id=f"ros-{evaluation['evaluation_id']}-{short_hash(worker_run_id)}",
            artifact_type=ArtifactType.LEARNING_ESTIMATE_EVALUATION.value,
            created_at=started_at,
            created_by=self.worker_id,
            source_artifact_ids=_input_ids(input_artifacts),
            confidence=float(estimate_artifact.get("confidence") or 0.1),
            evidence_level=_output_evidence(input_artifacts),
            labels=_output_labels(input_artifacts, "evaluation_worker_output"),
            metadata={
                "atlas_component_payload": evaluation,
                "source_estimate_artifact_id": estimate_artifact["artifact_id"],
                "source_worker_id": self.worker_id,
                "source_component": self.source_component,
                "source_evidence_levels": _source_evidence_levels(input_artifacts),
                "lineage_preserved": True,
                "auditability_preserved": True,
                "evidence_boundaries_preserved": True,
            },
        )]


class AtlasAttentionWorkerAdapter(ResearchOSWorkerAdapter, AttentionWorker):
    worker_id = "atlas_v2_attention_worker_adapter"
    supported_input_artifact_types = [ArtifactType.LEARNING_ESTIMATE_EVALUATION.value, ArtifactType.BACKLOG_ITEM.value]
    supported_output_artifact_types = [ArtifactType.ATTENTION_SIGNAL.value]
    source_component = "constellation_2/common/atlas_v2_research_os/priority_engine.py"


class AtlasMemoryCuratorWorkerAdapter(ResearchOSWorkerAdapter, MemoryCuratorWorker):
    worker_id = "atlas_v2_memory_curator_worker_adapter"
    supported_input_artifact_types = [ArtifactType.GENERATED_RESEARCH_CLAIM.value, ArtifactType.RESEARCH_HYPOTHESIS.value, ArtifactType.LEARNING_ESTIMATE.value]
    supported_output_artifact_types = [ArtifactType.LINEAGE_RECORD.value, ArtifactType.BACKLOG_ITEM.value]
    source_component = "constellation_2/common/atlas_v2_research_os/memory_index.py"


def create_default_worker_adapters() -> list[ResearchOSWorkerAdapter]:
    return [
        AtlasClaimWorkerAdapter(),
        AtlasHypothesisWorkerAdapter(),
        AtlasExperimentDesignWorkerAdapter(),
        AtlasExperimentExecutionWorkerAdapter(),
        AtlasLearningWorkerAdapter(),
        AtlasEvaluationWorkerAdapter(),
        AtlasAttentionWorkerAdapter(),
        AtlasMemoryCuratorWorkerAdapter(),
    ]


def register_default_worker_adapters(*, replace: bool = True) -> list[ResearchOSWorkerAdapter]:
    adapters = create_default_worker_adapters()
    for adapter in adapters:
        register_worker(adapter, replace=replace)
    return adapters


def _artifact_spec(**kwargs: Any) -> dict[str, Any]:
    payload = {
        "supersedes_artifact_ids": [],
        "lifecycle_state": LifecycleState.NEW.value,
        **kwargs,
    }
    return payload


def _claim_source_from_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    metadata = artifact.get("metadata", {}) or {}
    text = str(metadata.get("question") or metadata.get("description") or artifact.get("artifact_id") or "bounded research question")
    record = {
        "record_id": artifact["artifact_id"],
        "summary": text,
        "experience_quality_score": float(metadata.get("experience_quality_score") or 0.5),
        "status": "CONVERTED",
        "mechanism_family": str(metadata.get("mechanism_family") or "MEAN_REVERSION"),
        "entry_rule": str(metadata.get("entry_rule") or "source-defined setup is observed"),
        "exit_rule": str(metadata.get("exit_rule") or "source-defined follow-through appears"),
        "risk_rule": str(metadata.get("risk_rule") or "source-defined invalidation appears"),
    }
    source_type = "PRIOR_EXPERIENCE_EVENT" if artifact.get("artifact_type") == ArtifactType.EXPERIENCE_EVENT.value else "HISTORICAL_RESEARCH_OUTCOME"
    source_object_type = "ExperienceEvent" if source_type == "PRIOR_EXPERIENCE_EVENT" else "HistoricalExperienceRecord"
    return {
        "source_type": source_type,
        "source_reference": f"ResearchOSArtifact:{artifact['artifact_id']}",
        "source_object_type": source_object_type,
        "source_record": record,
        "source_text": text,
    }


def _external_claim_from_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    payload = (artifact.get("metadata", {}) or {}).get("atlas_component_payload", {}) or {}
    claim_text = str(payload.get("claim_text") or (artifact.get("metadata", {}) or {}).get("claim_text") or "source-defined setup produces observable follow-through")
    return {
        "claim_id": str(payload.get("generated_claim_id") or artifact["artifact_id"]),
        "created_at": str(payload.get("created_at") or artifact.get("created_at") or _now()),
        "source_id": artifact["artifact_id"],
        "claim_text": claim_text,
        "entry_rule": str(payload.get("entry_rule") or "source-defined setup is observed"),
        "exit_rule": str(payload.get("exit_rule") or "source-defined follow-through appears"),
        "risk_rule": str(payload.get("risk_rule") or "source-defined invalidation appears"),
        "filter_rule": str(payload.get("filter_rule") or "bounded historical replay inputs only"),
        "confidence": float(artifact.get("confidence") or 0.5),
        "extraction_status": "EXTRACTED",
    }


def _mechanism_from_claim_artifact(artifact: dict[str, Any], claim: dict[str, Any]) -> dict[str, Any]:
    payload = (artifact.get("metadata", {}) or {}).get("atlas_component_payload", {}) or {}
    family = str(payload.get("mechanism_family") or "MEAN_REVERSION")
    return {
        "mechanism_id": f"mech-{short_hash(claim['claim_id'] + ':' + family)}",
        "claim_id": claim["claim_id"],
        "mechanism_family": family,
        "classification_confidence": float(claim.get("confidence") or 0.5),
    }


def _hypothesis_payload_from_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    payload = (artifact.get("metadata", {}) or {}).get("atlas_component_payload", {}) or {}
    text = str(payload.get("hypothesis_text") or (artifact.get("metadata", {}) or {}).get("hypothesis_text") or "Replay source-defined setup against matched baseline observations.")
    return {
        "hypothesis_id": str(payload.get("hypothesis_id") or artifact["artifact_id"]),
        "created_at": str(payload.get("created_at") or artifact.get("created_at") or _now()),
        "mechanism_id": str(payload.get("mechanism_id") or "mechanism-unassigned"),
        "mechanism_family": str(payload.get("mechanism_family") or "MEAN_REVERSION"),
        "hypothesis_text": text,
        "testable_condition": str(payload.get("testable_condition") or text),
        "expected_direction": str(payload.get("expected_direction") or "EFFECT_SIZE_ABOVE_BASELINE"),
        "baseline_comparison": str(payload.get("baseline_comparison") or "matched baseline observations"),
        "required_data": list(payload.get("required_data") or ["timestamped_observations", "matched_baseline_observations"]),
        "falsification_criteria": payload.get("falsification_criteria") or ["Falsify if the setup does not separate from the baseline."],
        "confidence": float(artifact.get("confidence") or payload.get("confidence") or 0.5),
        "tier": "TIER_1_SANITY",
        "status": str(payload.get("status") or "GENERATED"),
    }


def _learning_estimate_payload(source: dict[str, Any], *, started_at: str, worker_run_id: str) -> dict[str, Any]:
    metadata = source.get("metadata", {}) or {}
    expected = _bounded_float(metadata.get("actual_learning_value", metadata.get("expected_learning_value", source.get("confidence", 0.4))))
    importance = _bounded_float(metadata.get("importance_score", 0.5))
    regret = _bounded_float(metadata.get("expected_regret_if_ignored", 0.25))
    cost = _bounded_float(metadata.get("attention_cost_estimate", 0.1))
    priority = attention_priority_score(expected_learning_value=expected, importance_score=importance, expected_regret_if_ignored=regret, attention_cost_estimate=cost)
    return {
        "estimate_id": f"le-{short_hash(source['artifact_id'] + ':' + worker_run_id)}",
        "created_at": started_at,
        "estimator_run_id": f"ler-{short_hash(worker_run_id)}",
        "source_object_id": source["artifact_id"],
        "source_object_type": source["artifact_type"],
        "expected_learning_value": expected,
        "importance_score": importance,
        "expected_regret_if_ignored": regret,
        "attention_cost_estimate": cost,
        "estimated_attention_priority": priority,
        "estimator_version": "atlas_v2_learning_estimator_v1",
        "status": "estimated",
    }


def _learning_evaluation_payload(estimate_artifact: dict[str, Any], source_artifact: dict[str, Any], *, started_at: str, worker_run_id: str) -> dict[str, Any]:
    estimate = (estimate_artifact.get("metadata", {}) or {}).get("atlas_component_payload", {}) or {}
    source_meta = source_artifact.get("metadata", {}) or {}
    expected = _bounded_float(estimate.get("expected_learning_value", estimate_artifact.get("confidence", 0.4)))
    actual = _bounded_float(source_meta.get("actual_learning_value", expected))
    importance = _bounded_float(estimate.get("importance_score", 0.5))
    prediction_error = round(actual - expected, 6)
    return {
        "evaluation_id": f"lee-{short_hash(estimate_artifact['artifact_id'] + ':' + source_artifact['artifact_id'] + ':' + worker_run_id)}",
        "created_at": started_at,
        "estimator_run_id": str(estimate.get("estimator_run_id") or f"ler-{short_hash(worker_run_id)}"),
        "estimate_id": str(estimate.get("estimate_id") or estimate_artifact["artifact_id"]),
        "source_object_id": source_artifact["artifact_id"],
        "source_object_type": source_artifact["artifact_type"],
        "expected_learning_value": expected,
        "importance_score": importance,
        "actual_learning_value": actual,
        "actual_regret": _bounded_float(source_meta.get("actual_regret", 0.0)),
        "behavior_change_observed": bool(source_meta.get("behavior_change_observed", False)),
        "calibration_error": _bounded_float(source_meta.get("calibration_error", abs(prediction_error))),
        "learning_prediction_error": prediction_error,
        "importance_weighted_learning_error": round(importance * prediction_error, 6),
        "evaluation_reason": "observed learning compared with expected learning without maturity escalation",
        "evaluated_at": started_at,
    }


def _input_ids(input_artifacts: list[dict[str, Any]]) -> list[str]:
    return [str(item["artifact_id"]) for item in input_artifacts if item.get("artifact_id")]


def _source_evidence_levels(input_artifacts: list[dict[str, Any]]) -> list[str]:
    return [str(item.get("evidence_level") or EvidenceLevel.GENERATED_ONLY.value) for item in input_artifacts]


def _output_evidence(input_artifacts: list[dict[str, Any]]) -> str:
    return _source_evidence_levels(input_artifacts)[0] if input_artifacts else EvidenceLevel.GENERATED_ONLY.value


def _output_labels(input_artifacts: list[dict[str, Any]], worker_label: str) -> list[str]:
    labels = []
    for item in input_artifacts:
        labels.extend(str(label) for label in item.get("labels", []))
    labels.extend(["research_os", "connected_worker_output", worker_label])
    return sorted(set(labels))


def _unique_artifact_id(store: ArtifactStore, base: str) -> str:
    candidate = base
    counter = 1
    while store.exists(candidate):
        counter += 1
        candidate = f"{base}-{counter}"
    return candidate


def _bounded_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = 0.0
    return round(max(0.0, min(1.0, number)), 6)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
