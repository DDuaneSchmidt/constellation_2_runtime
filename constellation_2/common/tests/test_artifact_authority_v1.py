from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.artifact_authority_v1 import (  # noqa: E402
    ArtifactAuthorityError,
    assert_artifact_consumer_allowed_v1,
    get_artifact_contract_v1,
    get_artifact_mirror_contract_v1,
    load_artifact_authority_registry_v1,
    resolve_artifact_authority_path_v1,
)


def test_artifact_authority_registry_declares_core_domains_and_artifacts() -> None:
    registry = load_artifact_authority_registry_v1(SOURCE_ROOT)
    domain_ids = {str(row.get("domain_id") or "").strip() for row in registry.get("domains") or []}
    assert domain_ids == {"canonical_portfolio", "execution", "economic", "authority"}

    positions = get_artifact_contract_v1(SOURCE_ROOT, "positions_snapshot_v5")
    assert positions["authoritative_domain"] == "canonical_portfolio"
    assert positions["artifact_class"] == "compiled_state"
    assert positions["root_policy"] == "mirrored"

    capauth = get_artifact_contract_v1(SOURCE_ROOT, "capital_authority_allocation_v1")
    assert capauth["artifact_class"] == "decision_proposal"
    assert capauth["authoritative_domain"] == "execution"
    assert capauth["authoritative_root_type"] == "execution_truth_root"

    econ_package = get_artifact_contract_v1(SOURCE_ROOT, "economic_state_package_v1")
    assert econ_package["artifact_class"] == "outcome_record"
    assert econ_package["authoritative_domain"] == "economic"
    assert econ_package["authoritative_root_type"] == "execution_truth_root"


def test_positions_snapshot_v5_bridge_contract_is_explicit_and_resolvable() -> None:
    mirror = get_artifact_mirror_contract_v1(
        SOURCE_ROOT,
        "positions_snapshot_v5",
        "execution_positions_snapshot_v5",
    )
    assert mirror["authoritative_bridge"] == "ops/tools/run_execution_positions_snapshot_v5_bridge_v1.py"

    canonical = resolve_artifact_authority_path_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="positions_snapshot_v5",
        day_utc="2026-04-17",
        canonical_truth_root=Path("/tmp/canonical"),
        execution_truth_root=Path("/tmp/execution"),
        path_role="authoritative",
    )
    execution = resolve_artifact_authority_path_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="positions_snapshot_v5",
        day_utc="2026-04-17",
        canonical_truth_root=Path("/tmp/canonical"),
        execution_truth_root=Path("/tmp/execution"),
        path_role="mirror",
        mirror_id="execution_positions_snapshot_v5",
    )
    assert str(canonical).endswith("/tmp/canonical/positions_v1/snapshots/2026-04-17/positions_snapshot.v5.json")
    assert str(execution).endswith("/tmp/execution/positions_v1/snapshots/2026-04-17/positions_snapshot.v5.json")


def test_consumer_authority_is_explicit_for_targeted_runtime_paths() -> None:
    assert_artifact_consumer_allowed_v1(SOURCE_ROOT, "economic_state_package_v1", "trade_submit_readiness_c2_v1")
    assert_artifact_consumer_allowed_v1(SOURCE_ROOT, "economic_state_build_v1", "session_authority_v1")

    with pytest.raises(ArtifactAuthorityError):
        assert_artifact_consumer_allowed_v1(SOURCE_ROOT, "positions_snapshot_v5", "random_unlisted_consumer")
