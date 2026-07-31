import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import FORBIDDEN_ARTIFACT_TYPES
from constellation_2.common.atlas_v2_research_os.worker_adapters import AtlasClaimWorkerAdapter, register_default_worker_adapters
from constellation_2.common.atlas_v2_research_os.worker_registry import (
    WorkerRegistryError,
    clear_worker_registry_for_tests,
    get_worker,
    list_workers,
    register_worker,
    validate_worker_contract,
)


def setup_function():
    clear_worker_registry_for_tests()


def test_register_get_and_list_worker():
    worker = AtlasClaimWorkerAdapter()
    register_worker(worker)
    assert get_worker(worker.worker_id) is worker
    listed = list_workers()
    assert listed == [
        {
            "worker_id": worker.worker_id,
            "worker_type": worker.worker_type,
            "supported_input_artifact_types": worker.supported_input_artifact_types,
            "supported_output_artifact_types": worker.supported_output_artifact_types,
            "adapter_status": "CONNECTED_RESEARCH_ONLY",
            "source_component": "ops/atlas/v2_claim_idea_generator.py",
        }
    ]


def test_validate_worker_contract_rejects_forbidden_outputs():
    worker = AtlasClaimWorkerAdapter()
    worker.supported_output_artifact_types = [next(iter(FORBIDDEN_ARTIFACT_TYPES))]
    ok, failures = validate_worker_contract(worker)
    assert not ok
    assert "forbidden output" in failures[0]


def test_register_default_worker_adapters_covers_all_worker_types():
    register_default_worker_adapters()
    worker_types = {row["worker_type"] for row in list_workers()}
    assert worker_types == {
        "ClaimWorker",
        "HypothesisWorker",
        "ExperimentDesignWorker",
        "ExperimentExecutionWorker",
        "LearningWorker",
        "EvaluationWorker",
        "AttentionWorker",
        "MemoryCuratorWorker",
    }


def test_get_unknown_worker_raises():
    with pytest.raises(WorkerRegistryError):
        get_worker("missing")
