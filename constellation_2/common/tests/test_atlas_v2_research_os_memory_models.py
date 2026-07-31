from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import EvidenceLevel
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType, create_memory_object

NOW = "2026-06-04T00:00:00Z"


def test_creates_each_memory_object_type() -> None:
    for item in MemoryType:
        obj = create_memory_object(memory_id=f"mem-{item.name.lower()}", memory_type=item.value, created_at=NOW, source_artifact_ids=["a-1"], mechanism_tags=["OPENING_RANGE"], confidence=0.5)
        assert obj.memory_type == item.value
        assert obj.updated_at == NOW


def test_required_metadata_exists() -> None:
    obj = create_memory_object(memory_id="mem-001", memory_type=MemoryType.LEARNING_NODE.value, created_at=NOW, source_artifact_ids=["a-1"], mechanism_tags=["MEAN_REVERSION"], regime_context_ids=["regime-1"], labels=["generated_only"], metadata={"lesson": "test"})
    payload = obj.to_dict()
    for field in ["memory_id", "memory_type", "created_at", "updated_at", "source_artifact_ids", "mechanism_tags", "regime_context_ids", "evidence_level", "lifecycle_state", "confidence", "labels", "metadata"]:
        assert field in payload


def test_evidence_maturity_is_separate_from_lifecycle_state() -> None:
    obj = create_memory_object(memory_id="mem-002", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["a-1"], evidence_level=MemoryEvidenceMaturity.MOCK_ONLY.value, lifecycle_state=MemoryLifecycleState.FALSIFIED.value)
    assert obj.evidence_level == "MOCK_ONLY"
    assert obj.lifecycle_state == "FALSIFIED"


def test_operator_approved_is_not_memory_evidence_maturity() -> None:
    assert EvidenceLevel.OPERATOR_APPROVED.value == "OPERATOR_APPROVED"
    with pytest.raises(ValueError, match="memory evidence"):
        create_memory_object(memory_id="mem-003", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["a-1"], evidence_level="OPERATOR_APPROVED")
