from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

from ops.tools import run_execution_stream_snapshot_day_v1 as module
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding="utf-8")


def _write_bound_submission(
    day_root: Path,
    submission_id: str,
    *,
    order_id: int,
    perm_id: int,
    symbol: str = "SPY",
    action: str = "BUY",
    qty: int = 1,
) -> None:
    subdir = day_root / submission_id
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "submission_id": submission_id,
            "binding_hash": f"binding-{submission_id}",
            "broker": {"environment": "PAPER"},
            "status": "PRESUBMITTED",
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_version": "v2",
            "engine_id": "ENGINE",
            "source_intent_id": f"intent-{submission_id}",
            "intent_sha256": f"sha-{submission_id}",
            "symbol": symbol,
            "action": action,
            "qty_shares": qty,
        },
    )


def _write_orphan_submission(
    day_root: Path,
    submission_id: str,
    *,
    symbol: str = "SPY",
    action: str = "BUY",
    qty: int = 1,
    eligible_post_handoff: bool,
) -> None:
    subdir = day_root / submission_id
    _write_json(
        subdir / "binding_record.v2.json",
        {
            "submission_id": submission_id,
            "canonical_json_hash": f"binding-{submission_id}",
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_version": "v2",
            "engine_id": "ENGINE",
            "source_intent_id": f"intent-{submission_id}",
            "intent_sha256": f"sha-{submission_id}",
            "symbol": symbol,
            "action": action,
            "qty_shares": qty,
        },
    )
    if eligible_post_handoff:
        reason_code = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
        reason_detail = (
            "SUBMIT_FAILURE: SchemaValidationError("
            "\"SCHEMA_VALIDATION_FAILED: constellation_2/schemas/broker_submission_record.v2.schema.json\")"
        )
    else:
        reason_code = "C2_RISK_BUDGET_EXCEEDED"
        reason_detail = "Projected margin 9999 exceeds cap 2500"
    _write_json(
        subdir / "veto_record.v1.json",
        {
            "reason_code": reason_code,
            "reason_detail": reason_detail,
        },
    )


def _write_submission_index(
    truth_root: Path,
    day: str,
    *,
    attempts: list[dict],
    status: str = "PASS",
) -> Path:
    path = truth_root / "submission_index_v1" / day / "submission_index.v1.json"
    _write_json(
        path,
        {
            "schema_version": "submission_index.v1",
            "day": day,
            "sleeve": "PRIMARY",
            "environment": "PAPER",
            "status": status,
            "attempts": attempts,
            "blocking_evidence": [],
            "generated_at_utc": "2026-04-24T00:00:00Z",
        },
    )
    return path


def _set_day_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, day: str) -> Path:
    root = tmp_path / "submissions"
    day_root = root / day
    day_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(module, "SUBMISSIONS_DAY_ROOT", root)
    return day_root


def test_resolve_prefers_exact_perm_id_match() -> None:
    idx = {
        "perm_id:1870974300": {
            "submission_id": "f6",
            "binding_hash": "binding-f6",
            "engine_id": "ENGINE",
            "source_intent_id": "intent-f6",
            "intent_sha256": "sha-f6",
            "broker_env": "PAPER",
        }
    }
    meta, reasons, attribution = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=idx,
        orphan_fallback_idx={},
        orphan_claims={},
        perm_id_bridge_candidates={},
        perm_id_bridge_error="",
        order_id=75,
        perm_id=1870974300,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )
    assert meta["submission_id"] == "f6"
    assert reasons == []
    assert attribution["attribution_method"] == "DIRECT_BROKER_IDS"


def test_resolve_uses_exact_order_id_when_perm_id_misses() -> None:
    idx = {
        "order_id:75": {
            "submission_id": "f6",
            "binding_hash": "binding-f6",
            "engine_id": "ENGINE",
            "source_intent_id": "intent-f6",
            "intent_sha256": "sha-f6",
            "broker_env": "PAPER",
        }
    }
    meta, reasons, attribution = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=idx,
        orphan_fallback_idx={},
        orphan_claims={},
        perm_id_bridge_candidates={},
        perm_id_bridge_error="",
        order_id=75,
        perm_id=999,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )
    assert meta["submission_id"] == "f6"
    assert reasons == []
    assert attribution["attribution_method"] == "DIRECT_BROKER_IDS"


def test_symbol_quantity_only_candidate_is_rejected_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-13"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    _write_orphan_submission(day_root, "d50", eligible_post_handoff=True)
    with pytest.raises(RuntimeError, match="UNATTRIBUTABLE_BROKER_EVENT"):
        module._resolve_submission_meta_for_event(
            event_type="ORDER_STATUS",
            idx=module._build_orderid_index(day),
            orphan_fallback_idx=module._build_orphan_fallback_index(day),
            orphan_claims={},
            perm_id_bridge_candidates={},
            perm_id_bridge_error="",
            order_id=70,
            perm_id=1870974295,
            symbol="SPY",
            action="BUY",
            order_qty=1,
        )


def test_symbol_quantity_timestamp_only_signal_does_not_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-13"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    _write_orphan_submission(day_root, "d50", eligible_post_handoff=True)
    with pytest.raises(RuntimeError, match="UNATTRIBUTABLE_BROKER_EVENT"):
        module._resolve_submission_meta_for_event(
            event_type="ORDER_STATUS",
            idx=module._build_orderid_index(day),
            orphan_fallback_idx=module._build_orphan_fallback_index(day),
            orphan_claims={},
            perm_id_bridge_candidates={},
            perm_id_bridge_error="",
            order_id=70,
            perm_id=1870974295,
            symbol="SPY",  # Symbol/action/qty alone are forbidden attribution signals.
            action="BUY",
            order_qty=1,
        )


def test_unknown_event_remains_unattributable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-13"
    _set_day_root(monkeypatch, tmp_path, day)

    with pytest.raises(RuntimeError, match="UNATTRIBUTABLE_BROKER_EVENT"):
        module._resolve_submission_meta_for_event(
            event_type="ORDER_STATUS",
            idx={},
            orphan_fallback_idx={},
            orphan_claims={},
            perm_id_bridge_candidates={},
            perm_id_bridge_error="",
            order_id=999,
            perm_id=888,
            symbol="IWM",
            action="SELL",
            order_qty=2,
        )


def test_perm_id_bridge_attributes_order_id_zero_with_single_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-24"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    submission_id = "f6" * 32
    _write_bound_submission(day_root, submission_id, order_id=90, perm_id=764621016)
    submission_record_path = (
        day_root / submission_id / "broker_submission_record.v2.json"
    ).resolve()
    truth_root = tmp_path / "truth"
    _write_submission_index(
        truth_root,
        day,
        attempts=[
            {
                "attempt_id": submission_id,
                "submission_record_path": str(submission_record_path),
                "execution_stream_path": "/tmp/exec.json",
                "fill_ledger_path": "/tmp/fill.json",
                "broker_order_id": 90,
                "broker_perm_id": 764621016,
                "lineage_status": "PASS",
                "blocking_evidence": [],
            }
        ],
    )
    rows = module._submission_meta_rows(day)
    bridge_candidates, bridge_error = module._build_perm_id_bridge_candidates(
        day=day,
        truth_root=truth_root,
        submission_meta_rows=rows,
    )
    assert bridge_error == ""
    meta, reasons, attribution = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx={},
        orphan_fallback_idx={},
        orphan_claims={},
        perm_id_bridge_candidates=bridge_candidates,
        perm_id_bridge_error=bridge_error,
        order_id=0,
        perm_id=764621016,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )
    assert meta["submission_id"] == submission_id
    assert reasons == ["ATTRIBUTED_BY_PERM_ID_BRIDGE"]
    assert attribution["attribution_method"] == "PERM_ID_BRIDGE"
    assert attribution["attribution_confidence"] == "EXACT_SINGLE_MATCH"
    assert attribution["raw_order_id"] == 0
    assert attribution["raw_perm_id"] == 764621016
    assert attribution["matched_attempt_id"] == submission_id


def test_perm_id_bridge_order_id_zero_fails_when_no_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-24"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    submission_id = "ab" * 32
    _write_bound_submission(day_root, submission_id, order_id=91, perm_id=555)
    truth_root = tmp_path / "truth"
    _write_submission_index(
        truth_root,
        day,
        attempts=[],
    )
    rows = module._submission_meta_rows(day)
    bridge_candidates, bridge_error = module._build_perm_id_bridge_candidates(
        day=day,
        truth_root=truth_root,
        submission_meta_rows=rows,
    )
    with pytest.raises(RuntimeError, match="UNATTRIBUTABLE_BROKER_EVENT"):
        module._resolve_submission_meta_for_event(
            event_type="ORDER_STATUS",
            idx={},
            orphan_fallback_idx={},
            orphan_claims={},
            perm_id_bridge_candidates=bridge_candidates,
            perm_id_bridge_error=bridge_error,
            order_id=0,
            perm_id=764621016,
            symbol="SPY",
            action="BUY",
            order_qty=1,
        )


def test_perm_id_bridge_order_id_zero_fails_when_multiple_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-24"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    submission_id_a = "f6" * 32
    submission_id_b = "d5" * 32
    _write_bound_submission(day_root, submission_id_a, order_id=90, perm_id=764621016)
    _write_bound_submission(day_root, submission_id_b, order_id=91, perm_id=764621016)
    submission_record_path_a = (
        day_root / submission_id_a / "broker_submission_record.v2.json"
    ).resolve()
    submission_record_path_b = (
        day_root / submission_id_b / "broker_submission_record.v2.json"
    ).resolve()
    truth_root = tmp_path / "truth"
    _write_submission_index(
        truth_root,
        day,
        attempts=[
            {
                "attempt_id": submission_id_a,
                "submission_record_path": str(submission_record_path_a),
                "execution_stream_path": "/tmp/exec-a.json",
                "fill_ledger_path": "/tmp/fill-a.json",
                "broker_order_id": 90,
                "broker_perm_id": 764621016,
                "lineage_status": "PASS",
                "blocking_evidence": [],
            },
            {
                "attempt_id": submission_id_b,
                "submission_record_path": str(submission_record_path_b),
                "execution_stream_path": "/tmp/exec-b.json",
                "fill_ledger_path": "/tmp/fill-b.json",
                "broker_order_id": 91,
                "broker_perm_id": 764621016,
                "lineage_status": "PASS",
                "blocking_evidence": [],
            },
        ],
    )
    rows = module._submission_meta_rows(day)
    bridge_candidates, bridge_error = module._build_perm_id_bridge_candidates(
        day=day,
        truth_root=truth_root,
        submission_meta_rows=rows,
    )
    with pytest.raises(RuntimeError, match="AMBIGUOUS_BROKER_EVENT_ATTRIBUTION"):
        module._resolve_submission_meta_for_event(
            event_type="ORDER_STATUS",
            idx={},
            orphan_fallback_idx={},
            orphan_claims={},
            perm_id_bridge_candidates=bridge_candidates,
            perm_id_bridge_error=bridge_error,
            order_id=0,
            perm_id=764621016,
            symbol="SPY",
            action="BUY",
            order_qty=1,
        )


def test_write_immutable_refuses_overwrite_different_bytes(tmp_path: Path) -> None:
    target = tmp_path / "immutable.json"
    module._write_immutable(target, b'{"a":1}\n')
    with pytest.raises(RuntimeError, match="REFUSE_OVERWRITE_DIFFERENT_BYTES"):
        module._write_immutable(target, b'{"a":2}\n')


def test_governed_replay_mode_writes_new_attempt_without_mutating_history(tmp_path: Path) -> None:
    day = "2026-04-24"
    truth_root = tmp_path / "truth"
    submission_dir = truth_root / "execution_evidence_v1" / "submissions" / day / ("a" * 64)
    submission_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": "a" * 64,
            "binding_hash": "b" * 64,
            "status": "PENDINGSUBMIT",
            "broker": {"environment": "PAPER"},
            "broker_ids": {},
        },
    )
    _write_json(
        submission_dir / "equity_order_plan.v2.json",
        {
            "schema_version": "v2",
            "engine_id": "ENGINE",
            "source_intent_id": "intent-source-id-0001",
            "intent_sha256": "c" * 64,
            "symbol": "SPY",
            "action": "BUY",
            "qty_shares": 1,
        },
    )

    immutable_failure = truth_root / "execution_stream_v1" / "failures" / day / "failure.json"
    immutable_failure.parent.mkdir(parents=True, exist_ok=True)
    immutable_failure.write_text('{"status":"FAIL_SCHEMA_VIOLATION","legacy":true}\n', encoding="utf-8")
    before_sha = hashlib.sha256(immutable_failure.read_bytes()).hexdigest()

    with pytest.MonkeyPatch.context() as m:
        m.setattr(
            sys,
            "argv",
            [
                "run_execution_stream_snapshot_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth_root),
                "--replay_mode",
                "governed_new_attempt",
            ],
        )
        rc = module.main()
    assert rc == 0

    replay_root = truth_root / "execution_stream_v1" / "replays" / day
    replay_dirs = [p for p in replay_root.iterdir() if p.is_dir()]
    assert replay_dirs
    replay_dir = sorted(replay_dirs)[-1]

    replay_artifact = replay_dir / "execution_stream_snapshot.v1.json"
    replay_payload = json.loads(replay_artifact.read_text(encoding="utf-8"))
    assert replay_payload["status"] == "PASS"
    assert replay_payload["immutable_history_preserved"] is True
    assert replay_payload["raw_broker_status"] == "PENDINGSUBMIT"
    assert replay_payload["normalized_lifecycle_status"] == "OPEN_PENDING_SUBMIT"
    assert replay_payload["source_failure_artifact_path"] == str(immutable_failure.resolve())

    stream_records = sorted(replay_dir.glob("*.execution_event_stream_record.v1.json"))
    assert stream_records
    record = json.loads(stream_records[0].read_text(encoding="utf-8"))
    assert record["order_state"]["raw_broker_status"] == "PENDINGSUBMIT"
    assert record["order_state"]["normalized_lifecycle_status"] == "OPEN_PENDING_SUBMIT"
    validate_against_repo_schema_v1(
        record,
        Path(__file__).resolve().parents[3],
        "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json",
    )

    after_sha = hashlib.sha256(immutable_failure.read_bytes()).hexdigest()
    assert after_sha == before_sha
