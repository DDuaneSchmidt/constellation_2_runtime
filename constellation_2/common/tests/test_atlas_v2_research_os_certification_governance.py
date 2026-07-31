from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.certification_governance import (
    authority_boundary_metadata,
    scan_authority_boundary,
    validate_artifact_governance,
    validate_evidence_maturity_boundaries,
)

NOW = "2026-06-04T00:00:00Z"


def test_authority_boundary_metadata_denies_trading_capital_and_candidate_promotion() -> None:
    metadata = authority_boundary_metadata()
    assert metadata["certifies_trading"] is False
    assert metadata["certifies_capital"] is False
    assert metadata["certifies_candidate_promotion"] is False
    assert metadata["certifies_live_readiness"] is False


def test_scan_authority_boundary_detects_forbidden_marker(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    root.mkdir()
    (root / "bad.json").write_text('{"note": "capital allocation"}\n', encoding="utf-8")
    ok, failures = scan_authority_boundary(root)
    assert ok is False
    assert "capital allocation" in failures[0]


def test_scan_authority_boundary_allows_explicit_denial_text(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    root.mkdir()
    (root / "safe_report.json").write_text(
        '{"limitations": ["No live trading, no broker execution, no capital allocation, no candidate promotion, and no position sizing authority."], "broker_execution_allowed": false}\n',
        encoding="utf-8",
    )
    ok, failures = scan_authority_boundary(root)
    assert ok is True
    assert failures == []


def test_scan_authority_boundary_rejects_true_authority_flag(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    root.mkdir()
    (root / "bad_flag.json").write_text('{"broker_execution_allowed": true}\n', encoding="utf-8")
    ok, failures = scan_authority_boundary(root)
    assert ok is False
    assert "broker_execution_allowed=true" in failures[0]


def test_artifact_governance_passes_allowed_store(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "research_os")
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    ok, failures = validate_artifact_governance(store)
    assert ok is True
    assert failures == []


def test_evidence_maturity_boundary_rejects_operator_disposition_as_evidence(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "research_os")
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True, evidence_level="OPERATOR_APPROVED")
    ok, failures = validate_evidence_maturity_boundaries(store, store.root)
    assert ok is False
    assert "operator disposition" in failures[0]
