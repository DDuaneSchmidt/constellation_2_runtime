from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import alerts_projection_v1 as alerts_projection


def test_alert_deduplication_by_first_true_blocker() -> None:
    payload = alerts_projection.build_alerts_projection_v1(
        current_projection_payload={
            "authority_scope": "DERIVED_ONLY_CURRENT_SYSTEM_PROJECTION",
            "day_utc": "2026-04-08",
            "projection_id": "current-001",
            "projection_ref": "/tmp/current.json",
            "git_sha": "a" * 40,
            "first_true_blocker_code": "AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED",
            "first_true_blocker_source": "/tmp/deploy.json",
            "operator_action_summary": "Resolve first true blocker: AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED.",
            "contradiction_status": "PRESENT",
            "contradictions": [
                {
                    "contradiction_code": "AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED",
                    "summary": "duplicate root cause",
                    "source_paths": ["/tmp/deploy.json"],
                }
            ],
            "generated_at_utc": "2026-04-08T13:00:00Z",
        },
        generated_at_utc="2026-04-08T13:01:00Z",
        producer_module="test.module",
    )
    assert payload["first_actionable_alert"]["alert_code"] == "AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED"
    assert payload["active_alerts"][0]["root_cause_family"] == "DEPLOYMENT_BLOCK"
    assert len(payload["suppressed_or_secondary_alerts"]) == 1


def test_alerts_projection_rejects_malformed_current_projection() -> None:
    with pytest.raises(ValueError, match="AUTHORITY_SCOPE_INVALID|REQUIRED_FIELD_MISSING"):
        alerts_projection.build_alerts_projection_v1(
            current_projection_payload={
                "day_utc": "2026-04-08",
                "projection_id": "current-001",
                "projection_ref": "/tmp/current.json",
                "generated_at_utc": "2026-04-08T13:00:00Z",
                "git_sha": "a" * 40,
            },
            generated_at_utc="2026-04-08T13:01:00Z",
            producer_module="test.module",
        )
