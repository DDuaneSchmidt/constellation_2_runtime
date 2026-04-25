from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_kernel.execution_submission_record_v1 import (  # noqa: E402
    build_execution_submission_record_from_execution_package_v1,
    write_execution_submission_record_from_execution_package_v1,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _package_obj(*, package_path: Path) -> dict:
    return {
        "schema_id": "execution_package",
        "schema_version": "v1",
        "submission_id": "a" * 64,
        "canonical_json_hash": "b" * 64,
        "candidate_ref": {
            "phasec_out_dir": str((package_path.parent / "candidate").resolve()),
            "environment": "PAPER",
            "sleeve_id": "PRIMARY",
        },
        "selected_order_plan_ref": {
            "path": str((package_path.parent / "equity_order_plan.v2.json").resolve()),
            "sha256": "c" * 64,
        },
        "build_ref": {
            "path": str((package_path.parent / "execution_build.v1.json").resolve()),
            "sha256": "d" * 64,
        },
        "intent_id": "c2_trend_eq_spy_2026-04-23_v1",
        "trade_instance_id": "e" * 64,
    }


def test_build_execution_submission_record_from_execution_package_bridge() -> None:
    package_path = Path("/tmp/execution_package.v1.json")
    record = build_execution_submission_record_from_execution_package_v1(
        execution_package_obj=_package_obj(package_path=package_path),
        execution_package_path=package_path,
        day_utc="2026-04-23",
        produced_utc="2026-04-23T00:00:00Z",
        plan_obj={"source_intent_id": "c2_trend_eq_spy_2026-04-23_v1", "intent_sha256": "f" * 64},
        binding_obj={"binding_id": "binding-1", "intent_hash": "1" * 64},
    )

    assert record.schema_id == "execution_submission_record"
    assert record.builder_version == "execution_submission_record_package_bridge_v1"
    assert record.status == "READY_TO_SUBMIT"
    assert record.submission_id == "a" * 64
    assert record.execution_intent_id == "c2_trend_eq_spy_2026-04-23_v1"
    assert record.idempotency_key == "f" * 64
    assert record.execution_package_ref["sha256"] == "b" * 64


def test_write_execution_submission_record_from_execution_package_bridge_is_idempotent(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    package_path = (
        tmp_path / "execution_package_v1" / "2026-04-23" / ("a" * 64) / "execution_package.v1.json"
    ).resolve()
    package = _package_obj(package_path=package_path)
    _write_json(Path(package["selected_order_plan_ref"]["path"]), {"schema_id": "equity_order_plan", "schema_version": "v2"})
    _write_json(package_path, package)

    _, written_path, action = write_execution_submission_record_from_execution_package_v1(
        execution_package_path=package_path,
        day_utc="2026-04-23",
        produced_utc="2026-04-23T00:00:00Z",
        truth_root=truth_root,
        plan_obj={"source_intent_id": "c2_trend_eq_spy_2026-04-23_v1", "intent_sha256": "f" * 64},
        binding_obj={"binding_id": "binding-1", "intent_hash": "1" * 64},
    )
    assert action == "WROTE"
    assert Path(written_path).exists()

    _, second_path, second_action = write_execution_submission_record_from_execution_package_v1(
        execution_package_path=package_path,
        day_utc="2026-04-23",
        produced_utc="2026-04-23T00:00:00Z",
        truth_root=truth_root,
        plan_obj={"source_intent_id": "c2_trend_eq_spy_2026-04-23_v1", "intent_sha256": "f" * 64},
        binding_obj={"binding_id": "binding-1", "intent_hash": "1" * 64},
    )
    assert Path(second_path).resolve() == Path(written_path).resolve()
    assert second_action == "SKIP_IDENTICAL"


def test_build_execution_submission_record_from_execution_package_bridge_fails_closed_on_invalid_package() -> None:
    package_path = Path("/tmp/execution_package.v1.json")
    bad_package = _package_obj(package_path=package_path)
    bad_package["selected_order_plan_ref"] = {}

    with pytest.raises(ValueError, match="EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_REF_INVALID"):
        build_execution_submission_record_from_execution_package_v1(
            execution_package_obj=bad_package,
            execution_package_path=package_path,
            day_utc="2026-04-23",
            produced_utc="2026-04-23T00:00:00Z",
            plan_obj={"source_intent_id": "c2_trend_eq_spy_2026-04-23_v1", "intent_sha256": "f" * 64},
            binding_obj={"binding_id": "binding-1", "intent_hash": "1" * 64},
        )
