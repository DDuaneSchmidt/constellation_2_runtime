from __future__ import annotations

from constellation_2.common.control_plane_read_boundary_v1 import (
    CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
    NON_GOVERNED_DIRECT_READ_BYPASS_PATHS,
    READ_BOUNDARY_CLASSIFICATIONS_V1,
    READ_DOMINANCE_ACTIVE_PATHS,
    classify_read_boundary_path_v1,
)


def test_non_governed_bypass_paths_are_explicitly_classified() -> None:
    assert NON_GOVERNED_DIRECT_READ_BYPASS_PATHS
    for relpath in NON_GOVERNED_DIRECT_READ_BYPASS_PATHS:
        row = classify_read_boundary_path_v1(relpath)
        assert row is not None
        assert row["classification"] == CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL


def test_non_governed_bypass_paths_are_not_active_read_dominance_paths() -> None:
    overlap = set(NON_GOVERNED_DIRECT_READ_BYPASS_PATHS) & set(READ_DOMINANCE_ACTIVE_PATHS)
    assert not overlap


def test_boundary_classification_rows_have_reason_and_action() -> None:
    for relpath, row in READ_BOUNDARY_CLASSIFICATIONS_V1.items():
        assert relpath
        assert row["classification"]
        assert row["reason"]
        assert row["action"]
