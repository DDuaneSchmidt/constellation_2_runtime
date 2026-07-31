from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.failure_patterns import can_failure_influence_priority, get_failure_patterns_by_mechanism, get_repeated_failures, increment_failure_repetition, list_failure_patterns, record_failure_pattern, reopen_failure_pattern, retire_failure_pattern

NOW = "2026-06-04T00:00:00Z"


def _failure(root: Path):
    return record_failure_pattern(root=root, failure_id="failure-1", failure_type="REGIME_MISMATCH", source_artifact_ids=["a-1"], mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-1"], reason="failed outside opening context", evidence_level="GENERATED_ONLY", first_seen_at=NOW)


def test_records_failure(tmp_path: Path) -> None:
    row = _failure(tmp_path / "research_os")
    assert row["repetition_count"] == 1
    assert row["evidence_level"] == "GENERATED_ONLY"


def test_increments_and_retrieves_repeated_failures(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _failure(root)
    increment_failure_repetition(root, "failure-1", source_artifact_id="a-2")
    repeated = get_repeated_failures(root, 2)
    assert repeated[0]["repetition_count"] == 2


def test_filters_failures_by_mechanism(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _failure(root)
    assert get_failure_patterns_by_mechanism(root, "OPENING_RANGE")[0]["failure_id"] == "failure-1"
    assert len(list_failure_patterns(root)) == 1


def test_retires_and_reopens_failure_pattern(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _failure(root)
    retired = retire_failure_pattern(root, "failure-1", reason="do not repeat")
    assert retired["retirement_status"] == "RETIRED_DO_NOT_REPEAT"
    assert can_failure_influence_priority(retired) is False
    reopened = reopen_failure_pattern(root, "failure-1", reason="new explicit regime")
    assert reopened["retirement_status"] == "REOPENED_LIMITED_SCOPE"
    assert can_failure_influence_priority(reopened) is True
