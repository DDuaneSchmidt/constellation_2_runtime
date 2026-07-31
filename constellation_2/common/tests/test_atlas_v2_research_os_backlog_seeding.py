from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.backlog_seeding import (
    deduplicate_seed_items,
    generate_duplicate_review_items,
    generate_edge_qualification_review_items,
    generate_failure_analysis_items,
    generate_mechanism_variation_items,
    generate_regime_gap_items,
    generate_seed_backlog_items,
)
from constellation_2.common.atlas_v2_research_os.backlog_seeding_models import BacklogSeedProfile
from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog


def test_generate_mechanism_variation_items():
    items = generate_mechanism_variation_items({}, limit=12)
    assert len(items) == 12
    assert {item.item_type for item in items} == {"MECHANISM_VARIATION"}
    assert all(item.mechanism_tags for item in items)


def test_generate_failure_analysis_items_from_repeated_failures():
    sources = {"repeated_failures": [{"failure_id": "f1", "failure_type": "REGIME_MISMATCH", "repetition_count": 3, "source_artifact_ids": ["q1"], "mechanism_tags": ["OPENING_RANGE"]}]}
    items = generate_failure_analysis_items(sources)
    assert len(items) == 1
    assert items[0].item_type == "FAILURE_ANALYSIS"
    assert items[0].failure_reduction_score > 0


def test_generate_regime_gap_items():
    sources = {
        "regime_contexts": [{"regime_context_id": "regime-1", "labels": ["UNKNOWN"]}],
        "research_memory": [{"memory_id": "mem-1", "source_artifact_ids": ["q1"], "mechanism_tags": ["OPENING_RANGE"], "regime_context_ids": ["regime-1"]}],
    }
    items = generate_regime_gap_items(sources)
    assert len(items) == 1
    assert items[0].item_type == "REGIME_GAP"


def test_generate_duplicate_review_items():
    sources = {"potential_duplicates": [{"left_memory_id": "mem-a", "right_memory_id": "mem-b", "duplicate_score": 0.9, "status": "LIKELY_DUPLICATE"}]}
    items = generate_duplicate_review_items(sources)
    assert len(items) == 1
    assert items[0].item_type == "DUPLICATE_REVIEW"
    assert items[0].duplicate_risk == 0.9


def test_generate_edge_qualification_review_items_when_pipeline_data_exists():
    sources = {
        "paper_trade_candidates": [{"candidate_id": "ptc-1", "source_artifact_ids": ["q1"], "mechanism_tags": ["OPENING_RANGE"]}],
        "paper_trading_queue": [{"queue_item_id": "queue-1", "test_plan": {"mechanism_tags": ["OPENING_RANGE"]}}],
    }
    items = generate_edge_qualification_review_items(sources)
    assert {item.item_type for item in items} == {"PAPER_TRADE_CANDIDATE_REVIEW", "EDGE_QUALIFICATION_REVIEW"}


def test_seed_small_review_writes_ready_backlog_items(tmp_path: Path):
    result = generate_seed_backlog_items(tmp_path, profile=BacklogSeedProfile.SMALL_REVIEW.value, day="2026-06-05")
    assert result["generated_count"] >= 25
    assert result["written_count"] == 25
    assert result["governance_result"]["status"] == "PASS"
    ready = ResearchBacklog(tmp_path).get_ready_items()
    assert len(ready) == 25
    assert all(row["state"] == "READY" for row in ready)
    assert all(row.get("mechanism_tags") for row in ready)
    assert all(ArtifactStore(tmp_path).exists(row["source_artifact_ids"][-1]) for row in ready)


def test_seed_overnight_review_writes_100_items(tmp_path: Path):
    result = generate_seed_backlog_items(tmp_path, profile=BacklogSeedProfile.OVERNIGHT_RESEARCH.value, day="2026-06-05")
    assert result["requested_limit"] == 100
    assert result["written_count"] == 100
    assert result["ready_count_after_seeding"] == 100


def test_deduplicates_existing_unresolved_backlog_items(tmp_path: Path):
    first = generate_seed_backlog_items(tmp_path, profile=BacklogSeedProfile.SMALL_REVIEW.value, day="2026-06-05")
    second = generate_seed_backlog_items(tmp_path, profile=BacklogSeedProfile.SMALL_REVIEW.value, day="2026-06-05")
    assert first["written_count"] == 25
    assert second["written_count"] == 0
    assert second["duplicate_count"] >= 25
    assert len(ResearchBacklog(tmp_path).get_ready_items()) == 25


def test_cli_seed_and_report_commands(tmp_path: Path):
    assert main(["--root", str(tmp_path), "--seed-research-backlog-small"]) == 0
    assert main(["--root", str(tmp_path), "--backlog-seeding-report"]) == 0
    assert (tmp_path / "backlog_seeding" / "latest.json").exists()
