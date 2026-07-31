from constellation_2.common.atlas_v2_research_os.worker_adapters import AtlasClaimWorkerAdapter
from constellation_2.common.atlas_v2_research_os.worker_interfaces import ClaimWorker, ResearchOSWorker
from constellation_2.common.atlas_v2_research_os.worker_models import WorkerRunResult, WorkerType


def test_worker_interfaces_expose_standard_contract():
    worker = AtlasClaimWorkerAdapter()
    assert isinstance(worker, ResearchOSWorker)
    assert isinstance(worker, ClaimWorker)
    assert worker.worker_id == "atlas_v2_claim_worker_adapter"
    assert worker.worker_type == WorkerType.CLAIM.value
    for attr in ("supported_input_artifact_types", "supported_output_artifact_types"):
        assert getattr(worker, attr)
    for method in ("run", "dry_run", "validate_inputs", "validate_outputs"):
        assert callable(getattr(worker, method))


def test_worker_run_result_contains_required_fields():
    worker = AtlasClaimWorkerAdapter()
    result = worker.dry_run([])
    assert isinstance(result, WorkerRunResult)
    payload = result.to_dict()
    for field in (
        "worker_run_id",
        "worker_id",
        "started_at",
        "completed_at",
        "status",
        "input_artifact_ids",
        "output_artifact_ids",
        "errors",
        "warnings",
        "governance_result",
        "lineage_result",
        "metadata",
    ):
        assert field in payload
