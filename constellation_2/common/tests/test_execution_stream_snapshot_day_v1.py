from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops.tools import run_execution_stream_snapshot_day_v1 as module


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
    meta, reasons = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=idx,
        orphan_fallback_idx={},
        orphan_claims={},
        order_id=75,
        perm_id=1870974300,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )
    assert meta["submission_id"] == "f6"
    assert reasons == []


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
    meta, reasons = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=idx,
        orphan_fallback_idx={},
        orphan_claims={},
        order_id=75,
        perm_id=999,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )
    assert meta["submission_id"] == "f6"
    assert reasons == []


def test_post_handoff_orphan_fallback_excludes_pre_broker_vetoes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-13"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    _write_orphan_submission(day_root, "d50", eligible_post_handoff=True)
    _write_orphan_submission(day_root, "6b8", eligible_post_handoff=False)

    idx = module._build_orderid_index(day)
    orphan_idx = module._build_orphan_fallback_index(day)
    claims: dict[str, tuple[int | None, int | None]] = {}

    meta, reasons = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=idx,
        orphan_fallback_idx=orphan_idx,
        orphan_claims=claims,
        order_id=70,
        perm_id=1870974295,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )

    assert meta["submission_id"] == "d50"
    assert reasons == ["ATTRIBUTED_BY_POST_HANDOFF_ORPHAN_FALLBACK"]
    assert idx["perm_id:1870974295"]["submission_id"] == "d50"
    assert idx["order_id:70"]["submission_id"] == "d50"


def test_ambiguous_orphan_fallback_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-13"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    _write_orphan_submission(day_root, "d50", eligible_post_handoff=True)
    _write_orphan_submission(day_root, "abc", eligible_post_handoff=True)

    with pytest.raises(RuntimeError, match="AMBIGUOUS_BROKER_EVENT"):
        module._resolve_submission_meta_for_event(
            event_type="ORDER_STATUS",
            idx=module._build_orderid_index(day),
            orphan_fallback_idx=module._build_orphan_fallback_index(day),
            orphan_claims={},
            order_id=70,
            perm_id=1870974295,
            symbol="SPY",
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
            order_id=999,
            perm_id=888,
            symbol="IWM",
            action="SELL",
            order_qty=2,
        )


def test_rerun_preserves_same_attribution_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day = "2026-04-13"
    day_root = _set_day_root(monkeypatch, tmp_path, day)
    _write_orphan_submission(day_root, "d50", eligible_post_handoff=True)
    _write_bound_submission(day_root, "f6", order_id=75, perm_id=1870974300)

    first = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=module._build_orderid_index(day),
        orphan_fallback_idx=module._build_orphan_fallback_index(day),
        orphan_claims={},
        order_id=70,
        perm_id=1870974295,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )[0]["submission_id"]
    second = module._resolve_submission_meta_for_event(
        event_type="ORDER_STATUS",
        idx=module._build_orderid_index(day),
        orphan_fallback_idx=module._build_orphan_fallback_index(day),
        orphan_claims={},
        order_id=70,
        perm_id=1870974295,
        symbol="SPY",
        action="BUY",
        order_qty=1,
    )[0]["submission_id"]

    assert first == "d50"
    assert second == "d50"
