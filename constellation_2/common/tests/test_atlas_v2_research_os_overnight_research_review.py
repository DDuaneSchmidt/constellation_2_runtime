from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.overnight_research_review import (
    OvernightResearchReviewProfile,
    builds_013_015_present,
    default_overnight_research_review_profile,
    dry_run_overnight_research_review,
    run_overnight_research_review,
)
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-05T00:00:00Z"


def _seed_question_backlog(root: Path, *, item_id: str = "bi1") -> None:
    store = ArtifactStore(root)
    artifact_id = f"q-{item_id}"
    store.create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.QUESTION.value,
        created_at=NOW,
        created_by="test",
        confidence=0.4,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"question": "Does source-defined follow-through separate from baseline?"},
        is_root=True,
    )
    ResearchBacklog(root).create_backlog_item(
        backlog_item_id=item_id,
        item_type="CLAIM_INVESTIGATION",
        title="Claim work",
        description="Desc",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=[artifact_id],
        expected_learning_value=1.0,
    )


def test_default_profile_is_safe_and_detects_candidate_pipeline_modules(tmp_path: Path):
    profile = default_overnight_research_review_profile(tmp_path)
    assert profile.name == "OVERNIGHT_RESEARCH_REVIEW"
    assert profile.cadence_minutes == 30
    assert profile.max_runs == 16
    assert profile.max_backlog_items_per_run == 3
    assert profile.max_worker_executions_per_run == 3
    assert profile.max_artifacts_per_run == 10
    assert profile.max_total_artifacts_per_session == 50
    assert profile.stop_on_governance_failure is True
    assert profile.stop_on_certification_failure is True
    assert profile.stop_on_forbidden_artifact_attempt is True
    assert profile.stop_on_consecutive_failures == 2
    assert profile.research_only is True
    assert profile.paper_trade_candidate_generation_allowed is True
    assert profile.live_trading_allowed is False
    assert profile.capital_authority_allowed is False
    assert profile.broker_execution_allowed is False
    assert profile.candidate_promotion_allowed is False


def test_builds_013_015_enable_paper_trade_candidate_generation_flag_only(tmp_path: Path):
    builds = tmp_path / "builds"
    builds.mkdir()
    for build_id in ("013", "014", "015"):
        (builds / f"{build_id}.present").write_text("present\n", encoding="utf-8")
    assert builds_013_015_present(tmp_path) is True
    assert default_overnight_research_review_profile(tmp_path).paper_trade_candidate_generation_allowed is True


def test_overnight_review_runs_bounded_research_and_writes_summary(tmp_path: Path):
    _seed_question_backlog(tmp_path)
    profile = OvernightResearchReviewProfile(max_runs=2)
    report = run_overnight_research_review(tmp_path, day="2026-06-05", profile=profile)
    assert report["profile_name"] == "OVERNIGHT_RESEARCH_REVIEW"
    assert report["dry_run"] is False
    assert report["metrics"]["runs_attempted"] >= 1
    assert report["metrics"]["runs_attempted"] <= profile.max_runs
    assert report["metrics"]["runs_completed"] >= 1
    assert report["metrics"]["backlog_items_processed"] == 2
    assert report["metrics"]["backlog_items_processed"] <= profile.max_runs
    assert report["metrics"]["claims_generated"] == 1
    assert report["metrics"]["hypotheses_generated"] == 1
    assert report["metrics"]["paper_trade_candidates_created"] == 0
    assert report["profile"]["live_trading_allowed"] is False
    assert (tmp_path / "overnight_review" / "2026-06-05" / "overnight_research_review.json").exists()
    summary = tmp_path / "overnight_review" / "2026-06-05" / "overnight_research_review_summary.md"
    assert "Runs attempted:" in summary.read_text(encoding="utf-8")


def test_overnight_dry_run_creates_no_research_artifacts(tmp_path: Path):
    _seed_question_backlog(tmp_path)
    before = len(ArtifactStore(tmp_path).list_artifacts())
    report = dry_run_overnight_research_review(tmp_path, day="2026-06-05")
    after = len(ArtifactStore(tmp_path).list_artifacts())
    assert after == before
    assert report["dry_run"] is True
    assert report["metrics"]["claims_generated"] >= 1
    assert ResearchBacklog(tmp_path).get("bi1")["state"] == "READY"


def test_cli_overnight_flags(tmp_path: Path):
    _seed_question_backlog(tmp_path)
    assert main(["--root", str(tmp_path), "--overnight-research-dry-run"]) == 0
    assert main(["--root", str(tmp_path), "--overnight-research-summary"]) == 0
    assert (tmp_path / "overnight_review" / "latest.json").exists()
