from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import pytest

from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def test_metadata_envelope_run_id_is_deterministic() -> None:
    env_a = metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=['decision_plan_id:dp1', 'planning_snapshot_id:ps1'],
        artifact_family='decision_chain_v1',
    )
    env_b = metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=['planning_snapshot_id:ps1', 'decision_plan_id:dp1'],
        artifact_family='decision_chain_v1',
    )
    env_c = metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=['planning_snapshot_id:ps1', 'decision_plan_id:dp1'],
        artifact_family='decision_chain_v1',
        selection_basis='manual_review',
    )

    assert env_a.run_id() == env_b.run_id()
    assert env_a.run_id() != env_c.run_id()


def test_metadata_envelope_fails_closed_on_missing_required_values() -> None:
    with pytest.raises(ValueError, match='PRODUCED_UTC_REQUIRED'):
        metadata_envelope_v1(
            produced_utc='',
            day_utc='2026-04-05',
            mode='paper',
            source_artifact_refs=['planning_snapshot_id:ps1'],
            artifact_family='authority_registry_v1',
        )

    with pytest.raises(ValueError, match='ARTIFACT_FAMILY_REQUIRED'):
        metadata_envelope_v1(
            produced_utc='2026-04-05T12:00:00Z',
            day_utc='2026-04-05',
            mode='paper',
            source_artifact_refs=['planning_snapshot_id:ps1'],
            artifact_family='',
        )
