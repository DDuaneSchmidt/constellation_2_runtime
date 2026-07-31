from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.certification_runner import run_research_os_certification
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-04T00:00:00Z"


def _seed(root: Path) -> None:
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    ResearchBacklog(root).create_backlog_item(
        backlog_item_id="bi-001",
        item_type="RESEARCH_QUESTION",
        title="Question",
        description="Desc",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=["q-001"],
        expected_learning_value=0.5,
        novelty_score=0.2,
    )


def test_runner_emits_required_checks(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    report = run_research_os_certification(root, day="2026-06-04").to_dict()
    check_ids = {check["check_id"] for check in report["checks"]}
    assert {
        "artifact_store_integrity",
        "lineage_integrity",
        "memory_integrity",
        "backlog_integrity",
        "priority_engine_deterministic_behavior",
        "lifecycle_transition_validity",
        "forbidden_artifact_absence",
        "authority_boundary_preservation",
        "evidence_maturity_boundaries",
        "operator_disposition_not_evidence_level",
        "connected_worker_contract_validity",
        "connected_worker_lineage_evidence_governance_preservation",
        "worker_run_record_integrity",
    } <= check_ids
    assert report["authority_boundary"]["certifies_trading"] is False


def test_runner_fails_on_forbidden_artifact_marker(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    (root / "forbidden.json").write_text('{"artifact_type": "LiveTrade"}\n', encoding="utf-8")
    report = run_research_os_certification(root, day="2026-06-04").to_dict()
    assert report["status"] == "FAIL"
    assert any(check["check_id"] == "forbidden_artifact_absence" and check["status"] == "FAIL" for check in report["checks"])


def test_runner_detects_non_deterministic_backlog_score_storage(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    path = root / "research_backlog.json"
    text = path.read_text(encoding="utf-8").replace('"priority_score": 0.7', '"priority_score": 9.9')
    path.write_text(text, encoding="utf-8")
    report = run_research_os_certification(root, day="2026-06-04").to_dict()
    assert any(check["check_id"] == "backlog_integrity" and check["status"] == "FAIL" for check in report["checks"])


def test_worker_connection_certification_fails_invalid_connected_worker(monkeypatch):
    import constellation_2.common.atlas_v2_research_os.certification_runner as runner
    from constellation_2.common.atlas_v2_research_os.worker_adapters import AtlasClaimWorkerAdapter

    worker = AtlasClaimWorkerAdapter()
    worker.supported_output_artifact_types = ["LiveTrade"]
    monkeypatch.setattr(runner, "create_default_worker_adapters", lambda: [worker])
    check = runner._check_worker_connection_contracts()
    assert check.status == "FAIL"
    assert check.certification_type == "WORKER_CONNECTION_CERTIFICATION"
