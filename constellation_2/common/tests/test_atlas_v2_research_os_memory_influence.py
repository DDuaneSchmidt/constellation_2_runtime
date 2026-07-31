from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.memory_index import add_memory_object
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryLifecycleState, MemoryType, create_memory_object
from constellation_2.common.atlas_v2_research_os.research_memory import can_influence_backlog_creation, can_influence_candidate_generation, can_influence_lifecycle_transition, can_influence_priority, can_influence_production_interpretation

NOW = "2026-06-04T00:00:00Z"


def _add(root: Path, memory_id: str, **kwargs):
    return add_memory_object(create_memory_object(memory_id=memory_id, memory_type=MemoryType.LEARNING_NODE.value, created_at=NOW, is_root=True, **kwargs), root)


def test_generated_only_memory_can_influence_priority_only_with_label(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _add(root, "mem-labeled", evidence_level="GENERATED_ONLY", labels=["generated_only"])
    _add(root, "mem-unlabeled", evidence_level="GENERATED_ONLY", labels=[])
    assert can_influence_priority("mem-labeled", root) is True
    assert can_influence_priority("mem-unlabeled", root) is False


def test_mock_only_memory_cannot_influence_production_interpretation(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _add(root, "mem-mock", evidence_level="MOCK_ONLY")
    assert can_influence_production_interpretation("mem-mock", root) is False


def test_retired_memory_cannot_influence_priority(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _add(root, "mem-retired", lifecycle_state=MemoryLifecycleState.RETIRED.value, labels=["generated_only"])
    assert can_influence_priority("mem-retired", root) is False


def test_quarantined_memory_cannot_influence_anything_except_audit_review(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _add(root, "mem-quarantine", lifecycle_state=MemoryLifecycleState.QUARANTINED.value, labels=["generated_only"])
    assert can_influence_priority("mem-quarantine", root) is False
    assert can_influence_backlog_creation("mem-quarantine", root) is False
    assert can_influence_lifecycle_transition("mem-quarantine", root) is False


def test_no_memory_can_influence_candidate_generation_before_future_certification(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _add(root, "mem-candidate", labels=["generated_only"])
    assert can_influence_candidate_generation("mem-candidate", root) is False
