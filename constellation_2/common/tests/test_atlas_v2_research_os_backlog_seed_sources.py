from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.backlog_seed_sources import collect_seed_sources, source_inventory_summary
from constellation_2.common.atlas_v2_research_os.failure_patterns import increment_failure_repetition, record_failure_pattern
from constellation_2.common.atlas_v2_research_os.memory_index import add_memory_object
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryType, create_memory_object
from constellation_2.common.atlas_v2_research_os.regime_context import create_regime_context

NOW = "2026-06-05T00:00:00Z"


def _seed_local_sources(root: Path) -> None:
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-1", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    store.create_artifact(artifact_id="claim-1", artifact_type=ArtifactType.GENERATED_RESEARCH_CLAIM.value, created_at=NOW, created_by="test", source_artifact_ids=["q-1"], confidence=0.4, is_root=False)
    create_regime_context(root=root, regime_context_id="regime-1", labels=["UNKNOWN"], source_artifact_ids=["q-1"], created_at=NOW)
    add_memory_object(create_memory_object(memory_id="mem-1", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["q-1"], mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-1"], metadata={"canonical_text": "Opening range follow-through"}), root, artifact_store=store)
    failure = record_failure_pattern(root=root, failure_id="failure-1", failure_type="REGIME_MISMATCH", source_artifact_ids=["q-1"], mechanism_tags=["OPENING_RANGE"], reason="test", evidence_level="GENERATED_ONLY", regime_context_ids=["regime-1"], first_seen_at=NOW)
    increment_failure_repetition(root, failure["failure_id"], seen_at=NOW)


def test_collect_seed_sources_discovers_local_research_os_inputs(tmp_path: Path):
    _seed_local_sources(tmp_path)
    sources = collect_seed_sources(tmp_path)
    summary = source_inventory_summary(sources)
    assert summary["research_memory"] == 1
    assert summary["repeated_failures"] == 1
    assert summary["regime_contexts"] == 1
    assert summary["generated_research_claims"] == 1
