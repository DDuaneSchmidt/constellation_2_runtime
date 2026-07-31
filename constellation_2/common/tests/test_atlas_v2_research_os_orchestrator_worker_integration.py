import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.orchestrator import run_once
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog
from constellation_2.common.atlas_v2_research_os.worker_execution_records import read_worker_run_index

NOW = "2026-06-05T00:00:00Z"


def _write_ready_truth(root: Path):
    truth = root / "reports" / "aegis_runtime_truth_kernel_v1" / "2026-06-05"
    graph = root / "reports" / "aegis_verified_runtime_graph_v1" / "2026-06-05"
    truth.mkdir(parents=True)
    graph.mkdir(parents=True)
    (truth / "runtime_truth_kernel.v1.json").write_text(json.dumps({
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }), encoding="utf-8")
    (graph / "verified_runtime_graph.v1.json").write_text(json.dumps({"graph_status": "READY"}), encoding="utf-8")


def test_orchestrator_run_once_invokes_connected_worker_and_records_run(tmp_path: Path):
    root = tmp_path / "research_os"
    truth_root = tmp_path / "truth"
    _write_ready_truth(truth_root)
    store = ArtifactStore(root)
    store.create_artifact(
        artifact_id="q1",
        artifact_type=ArtifactType.QUESTION.value,
        created_at=NOW,
        created_by="test",
        confidence=0.5,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"question": "When source-defined setup is observed, does follow-through separate from baseline?", "mechanism_family": "MEAN_REVERSION"},
        is_root=True,
    )
    ResearchBacklog(root).create_backlog_item(
        backlog_item_id="bi1",
        item_type="RESEARCH_QUESTION",
        title="Question",
        description="Desc",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=["q1"],
        expected_learning_value=1.0,
    )
    record = run_once(root, truth_root=truth_root, day="2026-06-05", seed=1)
    assert record["status"] == "COMPLETED"
    assert record["workers_invoked"][0]["worker_id"] == "atlas_v2_claim_worker_adapter"
    assert record["artifacts_created"][0]["artifact_type"] == "GeneratedResearchClaim"
    index = read_worker_run_index(root)
    assert index["worker_runs"][0]["worker_type"] == "ClaimWorker"
