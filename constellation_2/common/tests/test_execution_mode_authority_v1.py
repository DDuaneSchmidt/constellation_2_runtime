from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_mode_authority_v1 import evaluate_execution_mode_authority_v1


DAY = "2026-04-27"
SID = "a" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_submission(root: Path, *, dry_run: bool, order_id=None, perm_id=None) -> None:
    subdir = root / "execution_evidence_v1" / "submissions" / DAY / SID
    _write_json(subdir / "broker_submit_attempt_v1.json", {"submission_id": SID, "dry_run": dry_run})
    broker = {"submission_id": SID, "broker_ids": {"order_id": order_id, "perm_id": perm_id}}
    if dry_run:
        broker["error"] = {"code": "DRY_RUN_NO_BROKER_ID"}
    _write_json(subdir / "broker_submission_record.v2.json", broker)


def test_dry_run_evidence_without_broker_ids_is_diagnostic(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=True)

    payload = evaluate_execution_mode_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, env={})

    assert payload["mode_state"] == "DRY_RUN_LOCKED"
    assert payload["broker_transmit_enabled"] is False
    assert payload["missing_broker_ids_diagnostic"] is True
    assert payload["missing_broker_ids_blocker"] is False


def test_transmit_enabled_missing_broker_ids_sets_failure_expectation(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=False)

    payload = evaluate_execution_mode_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, env={})

    assert payload["mode_state"] == "PAPER_TRANSMIT_ENABLED"
    assert payload["broker_transmit_enabled"] is True
    assert payload["missing_broker_ids_blocker"] is True
    assert payload["post_submit_lineage_gap_policy"] == "BLOCKER"


def test_conflicting_env_and_artifact_mode_is_conflict(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=False)

    payload = evaluate_execution_mode_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert payload["mode_state"] == "SUBMIT_BLOCKED"
    assert payload["status"] == "FAIL"


def test_submission_index_can_set_dry_run_mode(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "submission_index_v1" / DAY / "submission_index.v1.json",
        {"status": "PASS", "attempts": [{"attempt_id": SID, "submit_mode_status": "DRY_RUN_COMPLETE", "broker_transmit_enabled": False}]},
    )

    payload = evaluate_execution_mode_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, env={})

    assert payload["mode_state"] == "DRY_RUN_LOCKED"
    assert payload["broker_transmit_enabled"] is False


def test_no_mode_evidence_defaults_to_locked_dry_run_not_unknown(tmp_path: Path) -> None:
    payload = evaluate_execution_mode_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, env={})

    assert payload["mode_state"] == "DRY_RUN_LOCKED"
    assert payload["mode_state"] != "UNKNOWN"
    assert payload["status"] == "PASS"
