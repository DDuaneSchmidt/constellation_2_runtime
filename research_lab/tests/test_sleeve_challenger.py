from __future__ import annotations

from research_lab.sleeves.sleeve_challenger import build_sleeve_challenge, store_sleeve_challenge
from research_lab.sleeves.sleeve_health import build_sleeve_health_snapshot, store_sleeve_health_snapshot
from research_lab.tests.test_sleeve_health_snapshot import _write_health_fixture


def test_sleeve_challenge_recommends_research_for_underperformance(tmp_path) -> None:
    store = tmp_path / "store"
    sleeve_id, version_id = _write_health_fixture(store)
    health = build_sleeve_health_snapshot(sleeve_id=sleeve_id, sleeve_version_id=version_id, as_of_date="2024-01-02", store_root=store, created_at="2024-01-02T00:00:00Z")
    store_sleeve_health_snapshot(health, store_root=store)

    challenge = build_sleeve_challenge(sleeve_id=sleeve_id, sleeve_version_id=version_id, sleeve_health_snapshot_id=health["sleeve_health_snapshot_id"], store_root=store, created_at="2024-01-02T00:00:00Z")
    store_sleeve_challenge(challenge, store_root=store)

    assert challenge["challenge_type"] == "underperformance"
    assert challenge["recommended_action"] == "continue_research"

