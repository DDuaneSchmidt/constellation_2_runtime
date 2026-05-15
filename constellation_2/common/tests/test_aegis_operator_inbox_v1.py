from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_operator_inbox_v1 import (  # noqa: E402
    build_operator_inbox_item_v1,
    build_operator_inbox_review_report_v1,
    capture_operator_inbox_item_v1,
    load_operator_inbox_item_v1,
    promote_operator_inbox_to_research_idea_v1,
    transition_operator_inbox_item_v1,
    validate_operator_inbox_artifact_v1,
    write_operator_inbox_artifact_v1,
)
from ops.tools.aegis_operator_inbox_capture_v1 import main as capture_main  # noqa: E402
from ops.tools.aegis_operator_inbox_review_v1 import main as review_main  # noqa: E402


NOW = "2026-05-15T18:00:00Z"
DAY = "2026-05-15"


def test_capture_operator_inbox_item(tmp_path: Path) -> None:
    item, path = capture_operator_inbox_item_v1(
        truth_root=tmp_path,
        title="Try alternate breadth threshold",
        description="Look at breadth collapse thresholds during panic sessions.",
        category="RESEARCH_IDEA",
        created_at=NOW,
        priority="HIGH",
        tags="breadth,panic",
    )

    assert path.exists()
    assert item["status"] == "CAPTURED"
    assert item["operator_inbox_only"] is True
    assert item["research_task_creation_allowed"] is False
    assert item["trade_creation_allowed"] is False
    assert item["sleeve_creation_allowed"] is False
    validate_operator_inbox_artifact_v1(item)


def test_capture_cli_requires_explicit_truth_root() -> None:
    with pytest.raises(SystemExit):
        capture_main(["--title", "x", "--description", "y", "--category", "OTHER"])


def test_invalid_category_and_status_rejected() -> None:
    with pytest.raises(ValueError, match="INVALID_OPERATOR_INBOX_CATEGORY"):
        build_operator_inbox_item_v1(
            inbox_item_id="bad",
            title="bad",
            description="bad",
            category="NOT_A_CATEGORY",
            created_at=NOW,
        )
    with pytest.raises(ValueError, match="INVALID_OPERATOR_INBOX_STATUS"):
        build_operator_inbox_item_v1(
            inbox_item_id="bad",
            title="bad",
            description="bad",
            category="OTHER",
            created_at=NOW,
            status="not-valid",
        )


def test_review_archive_reject_transitions(tmp_path: Path) -> None:
    item, _path = capture_operator_inbox_item_v1(
        truth_root=tmp_path,
        title="UI receipt panel",
        description="Add a lightweight receipt status panel.",
        category="UI_IMPROVEMENT",
        created_at=NOW,
    )
    reviewed = transition_operator_inbox_item_v1(item=item, status="REVIEWED", updated_at=NOW)
    assert reviewed["status"] == "REVIEWED"
    archived = transition_operator_inbox_item_v1(item=reviewed, status="ARCHIVED", updated_at=NOW)
    assert archived["status"] == "ARCHIVED"

    item2, _path2 = capture_operator_inbox_item_v1(
        truth_root=tmp_path,
        title="Bad idea",
        description="Reject me.",
        category="OTHER",
        created_at="2026-05-15T18:01:00Z",
    )
    rejected = transition_operator_inbox_item_v1(item=item2, status="REJECTED", updated_at=NOW)
    assert rejected["status"] == "REJECTED"


def test_promote_to_research_idea_preserves_lineage_and_creates_no_tasks(tmp_path: Path) -> None:
    item, _path = capture_operator_inbox_item_v1(
        truth_root=tmp_path,
        title="Panic breadth idea",
        description="Capture as a raw research idea.",
        category="RESEARCH_IDEA",
        created_at=NOW,
    )
    reviewed = transition_operator_inbox_item_v1(item=item, status="REVIEWED", updated_at=NOW)
    write_operator_inbox_artifact_v1(truth_root=tmp_path, payload=reviewed)

    updated, idea, idea_path = promote_operator_inbox_to_research_idea_v1(
        truth_root=tmp_path,
        item=reviewed,
        day_utc=DAY,
        updated_at=NOW,
    )

    assert updated["status"] == "PROMOTED_TO_IDEA"
    assert updated["promoted_to_research_idea_id"] == idea["inbox_id"]
    assert f"operator_inbox.v1:{item['inbox_item_id']}" in idea["notes"]
    assert idea_path.exists()
    assert not (tmp_path / "research_lab" / "research_task_queue_v1").exists()
    assert not (tmp_path / "reports" / "manual_trade_packet_v1").exists()
    assert not (tmp_path / "reports" / "promoted_sleeve_library_v1").exists()


def test_promote_to_hypothesis_only_records_existing_intake_lineage(tmp_path: Path) -> None:
    item, _path = capture_operator_inbox_item_v1(
        truth_root=tmp_path,
        title="Hypothesis seed",
        description="This should first go through Research Lab intake.",
        category="HYPOTHESIS_SEED",
        created_at=NOW,
    )
    reviewed = transition_operator_inbox_item_v1(item=item, status="REVIEWED", updated_at=NOW)
    updated = transition_operator_inbox_item_v1(
        item=reviewed,
        status="PROMOTED_TO_HYPOTHESIS",
        updated_at=NOW,
        promoted_to_hypothesis_id="rh-existing-001",
    )

    assert updated["status"] == "PROMOTED_TO_HYPOTHESIS"
    assert updated["promoted_to_hypothesis_id"] == "rh-existing-001"
    assert not (tmp_path / "research_lab" / "research_task_queue_v1").exists()


def test_review_report_has_deterministic_ordering_and_lineage_gap_detection(tmp_path: Path) -> None:
    first = build_operator_inbox_item_v1(
        inbox_item_id="item-b",
        title="B",
        description="B",
        category="OTHER",
        created_at="2026-05-01T00:00:00Z",
        priority="HIGH",
    )
    second = build_operator_inbox_item_v1(
        inbox_item_id="item-a",
        title="A",
        description="A",
        category="OTHER",
        created_at="2026-05-02T00:00:00Z",
        status="PROMOTED_TO_IDEA",
    )

    report = build_operator_inbox_review_report_v1(
        truth_root=tmp_path,
        generated_at_utc=NOW,
        items=[second, first],
    )

    validate_operator_inbox_artifact_v1(report)
    assert report["item_counts"]["total"] == 2
    assert report["captured_items"][0]["inbox_item_id"] == "item-b"
    assert report["stale_captured_items"][0]["inbox_item_id"] == "item-b"
    assert report["high_priority_open_items"][0]["inbox_item_id"] == "item-b"
    assert report["lineage_gaps"][0]["inbox_item_id"] == "item-a"
    assert report["research_task_creation_allowed"] is False
    assert report["trade_creation_allowed"] is False
    assert report["sleeve_creation_allowed"] is False


def test_review_cli_lists_and_updates_without_tasks_trades_or_sleeves(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = capture_main(
        [
            "--truth_root",
            str(tmp_path),
            "--title",
            "Dataset gap",
            "--description",
            "Need breadth data.",
            "--category",
            "DATASET_GAP",
            "--created_at",
            NOW,
        ]
    )
    assert rc == 0
    captured = json.loads(capsys.readouterr().out)
    item_id = captured["inbox_item_id"]

    assert review_main(["--truth_root", str(tmp_path), "--action", "mark-reviewed", "--inbox_item_id", item_id, "--updated_at", NOW]) == 0
    reviewed = load_operator_inbox_item_v1(truth_root=tmp_path, inbox_item_id=item_id)
    assert reviewed["status"] == "REVIEWED"
    assert review_main(["--truth_root", str(tmp_path), "--action", "list"]) == 0
    listed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert listed["count"] == 1
    assert listed["research_task_created"] is False
    assert not (tmp_path / "research_lab" / "research_task_queue_v1").exists()
    assert not (tmp_path / "reports" / "manual_trade_packet_v1").exists()
    assert not (tmp_path / "reports" / "promoted_sleeve_library_v1").exists()
