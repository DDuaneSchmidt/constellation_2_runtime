from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.governance import GovernanceError, validate_artifact_allowed, validate_no_forbidden_artifacts

NOW = "2026-06-04T00:00:00Z"


def _artifact(**overrides):
    payload = {"artifact_id": "a-001", "artifact_type": ArtifactType.QUESTION.value, "created_at": NOW, "created_by": "test", "source_artifact_ids": [], "supersedes_artifact_ids": [], "confidence": 0.5, "evidence_level": "GENERATED_ONLY", "lifecycle_state": "NEW", "labels": ["generated"], "metadata": {}, "is_root": True}
    payload.update(overrides)
    return payload


def test_rejects_forbidden_artifact_types() -> None:
    with pytest.raises(GovernanceError, match="forbidden"):
        validate_artifact_allowed(_artifact(artifact_type="LiveTrade"))


def test_passes_allowed_research_artifacts() -> None:
    assert validate_artifact_allowed(_artifact()) is True


def test_detects_generated_artifact_mislabeled_as_validated() -> None:
    with pytest.raises(GovernanceError, match="mislabeled"):
        validate_artifact_allowed(_artifact(evidence_level="EXTERNALLY_VALIDATED"))


def test_detects_mock_artifact_promotion() -> None:
    with pytest.raises(GovernanceError, match="mock artifacts"):
        validate_artifact_allowed(_artifact(evidence_level="HISTORICAL_REPLAY", labels=[], metadata={"source_evidence_levels": ["MOCK_ONLY"]}))


def test_confirms_no_capital_trade_sleeve_artifacts_are_emitted(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "research_os")
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    ok, failures = validate_no_forbidden_artifacts(store.root)
    assert ok is True
    assert failures == []


from constellation_2.common.atlas_v2_research_os.governance import validate_memory_allowed, validate_memory_candidate_isolation, validate_memory_evidence_labels, validate_memory_no_authority_escalation
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryType, create_memory_object


def _memory(**overrides):
    payload = create_memory_object(memory_id="mem-gov", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, is_root=True, labels=["generated_only"]).to_dict()
    payload.update(overrides)
    return payload


def test_memory_rejects_forbidden_artifact_types() -> None:
    with pytest.raises(GovernanceError, match="forbidden"):
        validate_memory_allowed(_memory(artifact_type="LiveTrade"))


def test_memory_detects_authority_escalation() -> None:
    with pytest.raises(GovernanceError, match="authority"):
        validate_memory_no_authority_escalation(_memory(metadata={"capital_authority": True}))


def test_memory_rejects_operator_approved_as_evidence() -> None:
    with pytest.raises(GovernanceError, match="OPERATOR_APPROVED"):
        validate_memory_evidence_labels(_memory(evidence_level="OPERATOR_APPROVED"))


def test_memory_candidate_isolation() -> None:
    with pytest.raises(GovernanceError, match="candidate influence"):
        validate_memory_candidate_isolation(_memory(metadata={"can_influence_candidate_generation": True}))


def test_memory_confirms_no_trade_capital_sleeve_candidate_artifacts() -> None:
    assert validate_memory_allowed(_memory()) is True
