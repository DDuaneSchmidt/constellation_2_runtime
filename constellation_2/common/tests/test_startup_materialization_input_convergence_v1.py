from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.startup_materialization_input_convergence_v1 import (
    derive_startup_materialization_input_convergence_payload_v1,
)
import ops.tools.run_startup_materialization_v1 as startup_materialization_module
import ops.tools.run_startup_materialization_input_convergence_v1 as startup_materialization_input_module


DAY = "2026-04-14"


def _row(artifact_id: str, *, ready: bool, observed_status: str, blocker_code: str = "") -> dict:
    return {
        "artifact_id": artifact_id,
        "required": True,
        "artifact_path": f"/tmp/{artifact_id}.json",
        "schema_id": artifact_id,
        "target_day_expected": DAY,
        "target_day_observed": DAY if ready or blocker_code else "",
        "observed_status": observed_status,
        "ready": ready,
        "reason_codes": [blocker_code] if blocker_code else [],
        "blocker_code": blocker_code,
        "summary": blocker_code or observed_status,
    }


def test_startup_materialization_input_convergence_blocks_when_nav_missing(tmp_path: Path) -> None:
    payload = derive_startup_materialization_input_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        required_inputs=["cash_ledger_snapshot_v1", "accounting_nav_v2", "startup_materialization_inputs_prep_v1"],
        artifact_results=[
            _row("cash_ledger_snapshot_v1", ready=True, observed_status="OK"),
            _row(
                "accounting_nav_v2",
                ready=False,
                observed_status="MISSING",
                blocker_code="ACCOUNTING_NAV_V2_MISSING",
            ),
            _row(
                "startup_materialization_inputs_prep_v1",
                ready=False,
                observed_status="BLOCKED_VALID",
                blocker_code="LIQPOL_MISSING_NAV",
            ),
        ],
        source_refs=[],
    )
    assert payload["convergence_status"] == "BLOCKED"
    assert payload["blocker_chain"][0]["artifact_id"] == "accounting_nav_v2"


def test_startup_materialization_input_convergence_succeeds_when_inputs_present(tmp_path: Path) -> None:
    payload = derive_startup_materialization_input_convergence_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        required_inputs=["operator_statement_v1", "cash_ledger_snapshot_v1", "accounting_nav_v2", "startup_materialization_inputs_prep_v1"],
        artifact_results=[
            _row("operator_statement_v1", ready=True, observed_status="OK"),
            _row("cash_ledger_snapshot_v1", ready=True, observed_status="OK"),
            _row("accounting_nav_v2", ready=True, observed_status="OK"),
            _row("startup_materialization_inputs_prep_v1", ready=True, observed_status="PASS"),
        ],
        source_refs=[],
    )
    assert payload["convergence_status"] == "SUCCESS"
    assert payload["blocker_chain"] == []


def test_startup_materialization_reads_inputs_prep_payload_when_present() -> None:
    with tempfile.TemporaryDirectory() as td:
        payload_path = Path(td) / "startup_materialization_inputs_prep.v1.json"
        payload_path.write_text(
            json.dumps(
                {
                    "schema_id": "startup_materialization_inputs_prep",
                    "day_utc": "2026-04-10",
                    "status": "PASS",
                    "default_equity_reference_price": "650.00",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        payload = startup_materialization_module._load_inputs_prep_payload(payload_path)
        assert payload is not None
        assert payload["status"] == "PASS"


def test_operator_statement_truth_root_points_above_operator_inputs() -> None:
    assert startup_materialization_input_module._operator_statement_truth_root() == Path("/home/node/constellation/constellation_2")
    assert startup_materialization_input_module._operator_input_root() == Path("/home/node/constellation/constellation_2/operator_inputs")


def test_startup_materialization_critical_path_python_uses_current_execution_python(tmp_path: Path, monkeypatch) -> None:
    current_python = tmp_path / "python-current"
    current_python.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setattr(startup_materialization_input_module.sys, "executable", str(current_python))
    assert startup_materialization_input_module._critical_path_python() == current_python.resolve()
