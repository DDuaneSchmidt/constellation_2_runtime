from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.trade_lineage_graph_v1 import evaluate_trade_lineage_graph_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DAY = "2026-04-27"
INTENT_HASH = "1" * 64
OTHER_HASH = "2" * 64
SID = "a" * 64
SID2 = "b" * 64
INTENT_ID = "intent_alpha"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_intent(root: Path, *, intent_hash: str = INTENT_HASH, intent_id: str = INTENT_ID) -> Path:
    path = root / "intents_v1" / "snapshots" / DAY / f"{intent_hash}.exposure_intent.v1.json"
    _write_json(path, {"schema_id": "exposure_intent", "intent_id": intent_id, "underlying": {"symbol": "SPY"}})
    return path


def _write_phasec(root: Path, *, intent_hash: str = INTENT_HASH, attempt: str = "attempt_A0001") -> Path:
    path = root / "phaseC_preflight_v1" / DAY / attempt / intent_hash / "submit_preflight_decision.v1.json"
    _write_json(path, {"decision": "ALLOW", "intent_hash": intent_hash, "intent_id": INTENT_ID})
    return path


def _write_auth(root: Path, *, intent_hash: str = INTENT_HASH) -> Path:
    path = root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json"
    _write_json(path, {"status": "AUTHORIZED", "intent_hash": intent_hash, "intent_id": INTENT_ID})
    return path


def _write_build(root: Path, *, submission_id: str = SID, intent_id: str = INTENT_ID) -> Path:
    path = root / "reports" / "execution_build_v1" / DAY / submission_id / "execution_build.v1.json"
    _write_json(path, {"closure_status": "COMPLETE", "submission_id": submission_id, "intent_id": intent_id})
    return path


def _write_package(root: Path, *, submission_id: str = SID, intent_hash: str = INTENT_HASH, intent_id: str = INTENT_ID) -> Path:
    path = root / "execution_package_v1" / DAY / submission_id / "execution_package.v1.json"
    _write_json(
        path,
        {
            "submission_id": submission_id,
            "intent_hash": intent_hash,
            "intent_id": intent_id,
            "canonical_json_hash": "c" * 64,
        },
    )
    return path


def _write_submit_trace(root: Path, *, submission_id: str = SID, intent_hash: str = INTENT_HASH, intent_id: str = INTENT_ID, name: str = "trace") -> Path:
    path = root / "reports" / "submit_decision_trace_v1" / DAY / "ENGINE" / intent_id / name / "submit_decision_trace.v1.json"
    _write_json(
        path,
        {
            "intent_id": intent_id,
            "evidence_refs": [
                {
                    "artifact_type": "broker_submission_record_v2",
                    "artifact_path": str((root / "execution_evidence_v1" / "submissions" / DAY / submission_id / "broker_submission_record.v2.json").resolve()),
                },
                {
                    "artifact_type": "exposure_intent_v1",
                    "artifact_path": str((root / "intents_v1" / "snapshots" / DAY / f"{intent_hash}.exposure_intent.v1.json").resolve()),
                    "artifact_sha256": intent_hash,
                },
            ],
        },
    )
    return path


def _write_submission(
    root: Path,
    *,
    submission_id: str = SID,
    intent_hash: str = INTENT_HASH,
    dry_run: bool = True,
    order_id=None,
    perm_id=None,
    include_package_ref: bool = True,
) -> None:
    subdir = root / "execution_evidence_v1" / "submissions" / DAY / submission_id
    package_ref = root / "execution_package_v1" / DAY / submission_id / "execution_package.v1.json"
    phasec_ref = root / "phaseC_preflight_v1" / DAY / "attempt_A0001" / intent_hash / "submit_preflight_decision.v1.json"
    evidence = [str(phasec_ref.resolve())]
    if include_package_ref:
        evidence.append(str(package_ref.resolve()))
    _write_json(
        subdir / "broker_submit_attempt_v1.json",
        {
            "submission_id": submission_id,
            "dry_run": dry_run,
            "reason_codes": ["DRY_RUN_SUBMIT_ATTEMPT" if dry_run else "REAL_SUBMIT_ATTEMPT"],
            "evidence_artifacts": evidence,
        },
    )
    broker_record = {
        "submission_id": submission_id,
        "status": "PENDINGSUBMIT" if dry_run else "SUBMITTED",
        "binding_hash": submission_id,
        "broker_ids": {"order_id": order_id, "perm_id": perm_id},
    }
    if dry_run:
        broker_record["error"] = {"code": "DRY_RUN_NO_BROKER_ID", "message": "dry run"}
    _write_json(subdir / "broker_submission_record.v2.json", broker_record)


def _write_lifecycle(root: Path, *, submission_id: str = SID, state: str = "DRY_RUN_COMPLETE") -> None:
    _write_json(
        root / "reports" / "execution_lifecycle_authority_v1" / DAY / "execution_lifecycle_authority.v1.json",
        {"status": "PASS", "current_lifecycle_state": state, "submissions": [{"submission_id": submission_id, "current_lifecycle_state": state}]},
    )


def _write_fill(root: Path, *, submission_id: str = SID, fill_submission_id: str | None = None) -> None:
    _write_json(
        root / "fill_ledger_v1" / DAY / f"{submission_id}.fill_ledger.v1.json",
        {"status": "OK", "submission_id": fill_submission_id or submission_id, "filled_qty": 1, "order_qty": 1, "lifecycle_status": "FILLED"},
    )


def _write_recon(root: Path) -> None:
    _write_json(
        root / "reports" / "execution_reconciliation_v1" / DAY / "execution_reconciliation.v1.json",
        {"status": "PASS", "semantic_status": "FULLY_OBSERVED_AND_CONFIRMED"},
    )


def _write_full_path(root: Path, *, dry_run: bool = True, order_id=None, perm_id=None, write_trace: bool = True) -> None:
    _write_intent(root)
    _write_phasec(root)
    _write_auth(root)
    _write_build(root)
    _write_package(root)
    if write_trace:
        _write_submit_trace(root)
    _write_submission(root, dry_run=dry_run, order_id=order_id, perm_id=perm_id)
    _write_lifecycle(root, state="DRY_RUN_COMPLETE" if dry_run else "ACKNOWLEDGED_OPEN")


def _eval(root: Path) -> dict:
    return evaluate_trade_lineage_graph_v1(
        day_utc=DAY,
        truth_root=root,
        execution_root=root,
        produced_utc=f"{DAY}T15:00:00Z",
    )


def test_dry_run_full_path_produces_complete_lineage_without_broker_ids(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=True)

    payload = _eval(tmp_path)
    row = payload["lineages"][0]

    assert payload["identity_state"] == "DRY_RUN_COMPLETE"
    assert row["dry_run"] is True
    assert row["broker_order_id"] is None
    assert row["broker_perm_id"] is None


def test_transmitted_path_with_broker_ids_is_broker_identified(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=False, order_id=101, perm_id=202)

    payload = _eval(tmp_path)

    assert payload["identity_state"] == "BROKER_IDENTIFIED"
    assert payload["lineages"][0]["broker_order_id"] == 101
    assert payload["lineages"][0]["broker_perm_id"] == 202


def test_intent_hash_wrong_for_submission_is_identity_conflict(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=True, write_trace=False)
    _write_submit_trace(tmp_path, intent_hash=OTHER_HASH, name="wrong")

    payload = _eval(tmp_path)

    assert payload["identity_state"] == "IDENTITY_CONFLICT"
    assert payload["lineages"][0]["first_broken_edge"]["blocker_code"] == "IDENTITY_CONFLICT"


def test_same_submission_under_two_intents_is_duplicate_identity(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=True)
    _write_submit_trace(tmp_path, intent_hash=OTHER_HASH, name="other_intent")
    _write_submit_trace(tmp_path, intent_hash="3" * 64, name="third_intent")

    payload = _eval(tmp_path)

    assert payload["identity_state"] == "DUPLICATE_IDENTITY"
    assert payload["lineages"][0]["first_broken_edge"]["blocker_code"] == "DUPLICATE_IDENTITY"


def test_missing_execution_package_after_build_is_lineage_gap(tmp_path: Path) -> None:
    _write_intent(tmp_path)
    _write_phasec(tmp_path)
    _write_auth(tmp_path)
    _write_build(tmp_path)
    _write_submit_trace(tmp_path)
    _write_submission(tmp_path, include_package_ref=False)

    payload = _eval(tmp_path)

    assert payload["identity_state"] == "LINEAGE_GAP"
    assert payload["lineages"][0]["first_broken_edge"]["blocker_code"] == "EXECUTION_PACKAGE_MISSING_AFTER_BUILD"


def test_stale_current_head_cannot_override_trade_lineage_graph(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=True)
    _write_json(
        tmp_path / "execution_evidence_v1" / "current_head" / DAY / "current_head.v1.json",
        {"status": "PASS", "selected_attempt_id": SID2},
    )

    payload = _eval(tmp_path)

    assert payload["identity_state"] == "DRY_RUN_COMPLETE"
    assert payload["projection_consistency"][0]["status"] == "STALE_PROJECTION"


def test_fill_ledger_connects_to_correct_submission_identity(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=False, order_id=101, perm_id=202)
    _write_fill(tmp_path)

    payload = _eval(tmp_path)

    assert payload["lineages"][0]["identity_state"] == "FILLED"
    assert any(edge["target_node_id"].startswith("fill:") and edge["status"] == "PASS" for edge in payload["lineages"][0]["edges"])


def test_reconciliation_connects_to_submission_and_fill_identity(tmp_path: Path) -> None:
    _write_full_path(tmp_path, dry_run=False, order_id=101, perm_id=202)
    _write_fill(tmp_path)
    _write_recon(tmp_path)

    payload = _eval(tmp_path)

    assert payload["identity_state"] == "RECONCILED"
    assert payload["lineages"][0]["reconciliation_complete"] is True
    validate_against_repo_schema_v1(
        payload,
        SOURCE_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_lineage_graph.v1.schema.json",
    )
