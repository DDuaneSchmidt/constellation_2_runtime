from __future__ import annotations

import pytest

from research_lab.sleeves.sleeve_challenger import build_sleeve_challenge, store_sleeve_challenge
from research_lab.sleeves.sleeve_health import build_sleeve_health_snapshot, store_sleeve_health_snapshot
from research_lab.sleeves.sleeve_review import build_sleeve_review, sleeve_summary, store_sleeve_review
from research_lab.tests.test_sleeve_health_snapshot import _write_health_fixture


def _challenge(store):
    sleeve_id, version_id = _write_health_fixture(store)
    health = build_sleeve_health_snapshot(sleeve_id=sleeve_id, sleeve_version_id=version_id, as_of_date="2024-01-02", store_root=store, created_at="2024-01-02T00:00:00Z")
    store_sleeve_health_snapshot(health, store_root=store)
    challenge = build_sleeve_challenge(sleeve_id=sleeve_id, sleeve_version_id=version_id, sleeve_health_snapshot_id=health["sleeve_health_snapshot_id"], store_root=store, created_at="2024-01-02T00:00:00Z")
    store_sleeve_challenge(challenge, store_root=store)
    return sleeve_id, version_id, challenge["sleeve_challenge_id"]


def test_sleeve_review_append_only_and_summary(tmp_path) -> None:
    store = tmp_path / "store"
    sleeve_id, version_id, challenge_id = _challenge(store)
    review = build_sleeve_review(sleeve_id=sleeve_id, sleeve_version_id=version_id, sleeve_challenge_id=challenge_id, review_decision="continue", review_reason="Keep collecting research observations.", reviewed_by="operator", reviewed_at="2024-01-03T00:00:00Z", store_root=store)
    store_sleeve_review(review, store_root=store, actor="operator")

    summary = sleeve_summary(sleeve_id, store_root=store)
    assert summary["latest_review"]["review_decision"] == "continue"
    assert summary["versions"]
    assert summary["challenges"]


def test_invalid_review_decision_fails(tmp_path) -> None:
    store = tmp_path / "store"
    sleeve_id, version_id, challenge_id = _challenge(store)

    with pytest.raises(RuntimeError, match="Invalid sleeve review decision"):
        build_sleeve_review(sleeve_id=sleeve_id, sleeve_version_id=version_id, sleeve_challenge_id=challenge_id, review_decision="activate", review_reason="bad", reviewed_by="operator", store_root=store)

