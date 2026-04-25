from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

import ops.tools.run_c2_capital_risk_envelope_gate_v2 as gate


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _make_inputs(tmp_path: Path, *, nav_total) -> gate.Inputs:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    day = "2026-04-23"
    alloc_path = truth_root / "allocation_v1" / "summary" / day / "summary.json"
    nav_path = truth_root / "accounting_v2" / "nav" / day / "nav.v2.json"
    pos_path = truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"

    _write_json(alloc_path, {"schema_id": "allocation_summary", "schema_version": "v1"})
    _write_json(
        nav_path,
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": day,
            "nav": {"nav_total": nav_total},
            "history": {"peak_nav": 100000, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
        },
    )
    _write_json(
        pos_path,
        {
            "positions": {"items": []},
        },
    )
    return gate.Inputs(
        alloc_path=alloc_path,
        nav_path=nav_path,
        pos_path=pos_path,
        pos_schema=gate.SCHEMA_POS_V2,
        truth_root=truth_root,
    )


def test_compute_fails_when_nav_total_missing_or_null() -> None:
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(gate, "_validate_against_repo_schema", return_value=None):
            with mock.patch.object(gate, "_git_sha", return_value="0" * 40):
                inp = _make_inputs(Path(td), nav_total=None)
                out = gate._compute(out_day="2026-04-23", produced_utc="2026-04-23T00:00:00Z", inp=inp)
    assert out["status"] == "FAIL"
    assert "B2_NAV_TOTAL_MISSING_OR_INVALID" in out["reason_codes"]
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" in out["reason_codes"]
    assert out["checks"]["nav_present"] is False
    assert out["envelope"]["nav_total_cents"] == 0


def test_compute_fails_when_nav_total_non_positive() -> None:
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(gate, "_validate_against_repo_schema", return_value=None):
            with mock.patch.object(gate, "_git_sha", return_value="0" * 40):
                inp = _make_inputs(Path(td), nav_total=0)
                out = gate._compute(out_day="2026-04-23", produced_utc="2026-04-23T00:00:00Z", inp=inp)
    assert out["status"] == "FAIL"
    assert "B2_NAV_TOTAL_MISSING_OR_INVALID" in out["reason_codes"]
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" in out["reason_codes"]
    assert out["checks"]["nav_present"] is False
    assert out["envelope"]["nav_total_cents"] == 0


def test_compute_passes_with_valid_positive_nav() -> None:
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(gate, "_validate_against_repo_schema", return_value=None):
            with mock.patch.object(gate, "_git_sha", return_value="0" * 40):
                inp = _make_inputs(Path(td), nav_total=100000)
                out = gate._compute(out_day="2026-04-23", produced_utc="2026-04-23T00:00:00Z", inp=inp)
    assert out["status"] == "PASS"
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" not in out["reason_codes"]
    assert out["checks"]["nav_present"] is True
    assert out["envelope"]["nav_total_cents"] == 10000000
