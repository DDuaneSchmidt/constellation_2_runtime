from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog
from constellation_2.common.atlas_v2_research_os.throughput_review import cleanup_incompatible_ready_backlog, ensure_research_effectiveness_latest, run_throughput_repair_and_review

NOW = "2026-06-05T00:00:00Z"


def test_cleanup_blocks_hypothesis_validation_without_claim(tmp_path: Path):
    store = ArtifactStore(tmp_path)
    store.create_artifact(artifact_id="q1", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", confidence=0.1, evidence_level="GENERATED_ONLY", labels=[], metadata={"question": "demo"}, is_root=True)
    ResearchBacklog(tmp_path).create_backlog_item(backlog_item_id="bad-hv", item_type="HYPOTHESIS_VALIDATION", title="bad", description="bad", created_at=NOW, created_by="test", source_artifact_ids=["q1"], state="READY")
    cleanup = cleanup_incompatible_ready_backlog(tmp_path)
    assert cleanup["blocked_count"] == 1
    assert ResearchBacklog(tmp_path).get("bad-hv")["blocked_reason"] == "MISSING_GENERATED_RESEARCH_CLAIM_INPUT"


def test_throughput_review_regenerates_effectiveness_latest_and_seeds_claim_work(tmp_path: Path):
    report = run_throughput_repair_and_review(tmp_path, day="2026-06-05")
    assert (tmp_path / "research_effectiveness" / "latest.json").exists()
    assert report["seed_result"]["created_count"] >= 10
    assert report["metrics"]["ready_backlog_count"] >= 10
    assert report["guardrails"]["live_trading_added"] is False
