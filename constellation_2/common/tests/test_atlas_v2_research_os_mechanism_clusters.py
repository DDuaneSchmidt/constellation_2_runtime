from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.mechanism_clusters import assign_artifact_to_mechanism_cluster, create_mechanism_cluster, get_cluster_artifacts, get_related_mechanisms, list_mechanism_clusters

NOW = "2026-06-04T00:00:00Z"


def test_creates_mechanism_cluster(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    cluster = create_mechanism_cluster(root=root, cluster_id="cluster-1", mechanism_tags=["OPENING_RANGE"], name="Opening Range", created_at=NOW)
    assert cluster["validation_status"] == "NOT_VALIDATED"


def test_assigns_artifact_to_cluster_and_preserves_lineage(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    create_mechanism_cluster(root=root, cluster_id="cluster-2", mechanism_tags=["OPENING_RANGE"], name="Opening Range", source_artifact_ids=["a-1"], created_at=NOW)
    cluster = assign_artifact_to_mechanism_cluster(root, "cluster-2", "a-2")
    assert get_cluster_artifacts(root, "cluster-2") == ["a-1", "a-2"]
    assert "a-2" in cluster["source_artifact_ids"]


def test_supports_multiple_mechanism_tags(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    create_mechanism_cluster(root=root, cluster_id="cluster-3", mechanism_tags=["OPENING_RANGE", "VOLATILITY_EXPANSION"], name="OR Vol", created_at=NOW)
    assert get_related_mechanisms(root, "VOLATILITY_EXPANSION")[0]["cluster_id"] == "cluster-3"


def test_clustering_does_not_imply_validation_or_authority(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    cluster = create_mechanism_cluster(root=root, cluster_id="cluster-4", mechanism_tags=["REVERSAL"], name="Reversal", created_at=NOW)
    assert cluster["validation_status"] == "NOT_VALIDATED"
    assert cluster["candidate_generation_authority"] is False
    assert cluster["capital_authority"] is False
    assert len(list_mechanism_clusters(root)) == 1
