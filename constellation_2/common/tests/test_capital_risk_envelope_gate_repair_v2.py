from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

import ops.tools.run_c2_capital_risk_envelope_gate_v2 as gate


DAY = "2026-04-14"
INPUT_DAY = "2026-04-13"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _report_obj(
    *,
    positions_sha: str,
    positions: list[dict],
    status: str,
    reason_codes: list[str],
    nav_total: int = 100000,
    nav_total_cents: int = 10000000,
    allowed_capital_at_risk_cents: int = 200000,
    portfolio_capital_at_risk_cents: int = 0,
    headroom_cents: int = 200000,
    nav_present: bool = True,
) -> dict:
    out = {
        "schema_id": "capital_risk_envelope",
        "schema_version": "v2",
        "day_utc": DAY,
        "produced_utc": f"{DAY}T00:00:00Z",
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
            "git_sha": "0" * 40,
        },
        "status": status,
        "reason_codes": reason_codes,
        "notes": [],
        "input_manifest": [
            {"type": "allocation_summary", "path": "/tmp/alloc.json", "sha256": "a" * 64},
            {"type": "accounting_nav", "path": "/tmp/nav.json", "sha256": "b" * 64},
            {
                "type": "positions_snapshot",
                "path": "/tmp/positions_snapshot.v2.json",
                "sha256": positions_sha,
            },
            {"type": "drawdown_contract", "path": "/tmp/drawdown.md", "sha256": "c" * 64},
            {"type": "capital_risk_envelope_contract_v2", "path": "/tmp/capital.md", "sha256": "d" * 64},
            {"type": "output_schema", "path": "/tmp/schema.json", "sha256": "e" * 64},
        ],
        "checks": {
            "allocation_summary_present": True,
            "nav_present": nav_present,
            "positions_present": True,
            "drawdown_present": True,
            "positions_all_have_max_loss": status == "PASS",
            "portfolio_within_envelope": status == "PASS",
        },
        "envelope": {
            "contracts": {
                "drawdown_contract": {"path": "/tmp/drawdown.md", "sha256": "c" * 64},
                "capital_risk_envelope_contract": {"path": "/tmp/capital.md", "sha256": "d" * 64},
            },
            "drawdown_multiplier_table": gate._table(),
            "base_envelope_pct": "0.020000",
            "nav_total": nav_total,
            "nav_total_cents": nav_total_cents,
            "peak_nav": 100000,
            "drawdown_abs": 0,
            "drawdown_pct": "0.000000",
            "multiplier": "1.00",
            "allowed_capital_at_risk_cents": allowed_capital_at_risk_cents,
            "portfolio_capital_at_risk_cents": portfolio_capital_at_risk_cents,
            "headroom_cents": headroom_cents,
            "positions": positions,
        },
    }
    return gate._finalize_constitutional_capital_risk_report(out)


def test_safe_positions_carry_forward_repair_updates_existing_day_report(tmp_path: Path, capsys) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    out_path = truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"
    existing = _report_obj(positions_sha="1" * 64, positions=[], status="PASS", reason_codes=[])
    candidate = _report_obj(
        positions_sha="2" * 64,
        positions=[
            {
                "position_id": "ed8bf1",
                "engine_id": "unknown",
                "market_exposure_type": "UNDEFINED_RISK",
                "status": "OPEN",
                "max_loss_cents": None,
                "included_in_risk_sum": False,
            }
        ],
        status="FAIL",
        reason_codes=["B2_OPEN_POSITION_MISSING_MAX_LOSS_FAILCLOSED"],
    )
    _write_json(out_path, existing)

    with (
        mock.patch.object(gate, "_resolve_inputs", return_value=mock.Mock()),
        mock.patch.object(gate, "_compute", return_value=candidate),
        mock.patch.object(sys, "argv", [
            "run_c2_capital_risk_envelope_gate_v2.py",
            "--out_day_utc", DAY,
            "--input_day_utc", INPUT_DAY,
            "--truth_root", str(truth_root),
            "--produced_utc", f"{DAY}T00:00:00Z",
        ]),
    ):
        rc = gate.main()

    assert rc == 2
    repaired = json.loads(out_path.read_text(encoding="utf-8"))
    assert repaired["input_manifest"][2]["sha256"] == "2" * 64
    assert repaired["envelope"]["positions"]
    captured = capsys.readouterr()
    assert "action=BACKFILL_REPAIRED" in captured.out


def test_non_safe_existing_report_remains_unchanged(tmp_path: Path, capsys) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    out_path = truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"
    existing = _report_obj(
        positions_sha="1" * 64,
        positions=[
            {
                "position_id": "existing",
                "engine_id": "unknown",
                "market_exposure_type": "UNDEFINED_RISK",
                "status": "OPEN",
                "max_loss_cents": 100,
                "included_in_risk_sum": True,
            }
        ],
        status="PASS",
        reason_codes=[],
    )
    candidate = _report_obj(
        positions_sha="2" * 64,
        positions=[
            {
                "position_id": "different",
                "engine_id": "unknown",
                "market_exposure_type": "UNDEFINED_RISK",
                "status": "OPEN",
                "max_loss_cents": None,
                "included_in_risk_sum": False,
            }
        ],
        status="FAIL",
        reason_codes=["B2_OPEN_POSITION_MISSING_MAX_LOSS_FAILCLOSED"],
    )
    _write_json(out_path, existing)

    with (
        mock.patch.object(gate, "_resolve_inputs", return_value=mock.Mock()),
        mock.patch.object(gate, "_compute", return_value=candidate),
        mock.patch.object(sys, "argv", [
            "run_c2_capital_risk_envelope_gate_v2.py",
            "--out_day_utc", DAY,
            "--input_day_utc", INPUT_DAY,
            "--truth_root", str(truth_root),
            "--produced_utc", f"{DAY}T00:00:00Z",
        ]),
    ):
        rc = gate.main()

    assert rc == 0
    preserved = json.loads(out_path.read_text(encoding="utf-8"))
    assert preserved["input_manifest"][2]["sha256"] == "1" * 64
    assert preserved["envelope"]["positions"][0]["position_id"] == "existing"
    captured = capsys.readouterr()
    assert "action=EXISTS" in captured.out


def test_safe_nav_validation_recovery_updates_stale_zero_nav_pass_report(tmp_path: Path, capsys) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    out_path = truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"
    existing = _report_obj(
        positions_sha="1" * 64,
        positions=[],
        status="PASS",
        reason_codes=[],
        nav_total=0,
        nav_total_cents=0,
        allowed_capital_at_risk_cents=0,
        portfolio_capital_at_risk_cents=0,
        headroom_cents=0,
    )
    candidate = _report_obj(
        positions_sha="1" * 64,
        positions=[],
        status="PASS",
        reason_codes=[],
        nav_total=100000,
        nav_total_cents=10000000,
        allowed_capital_at_risk_cents=200000,
        portfolio_capital_at_risk_cents=0,
        headroom_cents=200000,
    )
    _write_json(out_path, existing)

    with (
        mock.patch.object(gate, "_resolve_inputs", return_value=mock.Mock()),
        mock.patch.object(gate, "_compute", return_value=candidate),
        mock.patch.object(sys, "argv", [
            "run_c2_capital_risk_envelope_gate_v2.py",
            "--out_day_utc", DAY,
            "--input_day_utc", INPUT_DAY,
            "--truth_root", str(truth_root),
            "--produced_utc", f"{DAY}T00:00:00Z",
        ]),
    ):
        rc = gate.main()

    assert rc == 0
    repaired = json.loads(out_path.read_text(encoding="utf-8"))
    assert repaired["envelope"]["nav_total_cents"] == 10000000
    assert repaired["envelope"]["headroom_cents"] == 200000
    captured = capsys.readouterr()
    assert "action=BACKFILL_REPAIRED" in captured.out


def test_safe_nav_validation_recovery_replaces_stale_zero_nav_pass_with_fail(tmp_path: Path, capsys) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    out_path = truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"
    existing = _report_obj(
        positions_sha="1" * 64,
        positions=[],
        status="PASS",
        reason_codes=[],
        nav_total=0,
        nav_total_cents=0,
        allowed_capital_at_risk_cents=0,
        portfolio_capital_at_risk_cents=0,
        headroom_cents=0,
    )
    candidate = _report_obj(
        positions_sha="1" * 64,
        positions=[],
        status="FAIL",
        reason_codes=[
            "B2_NAV_TOTAL_MISSING_OR_INVALID",
            "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS",
        ],
        nav_total=0,
        nav_total_cents=0,
        allowed_capital_at_risk_cents=0,
        portfolio_capital_at_risk_cents=0,
        headroom_cents=0,
        nav_present=False,
    )
    _write_json(out_path, existing)

    with (
        mock.patch.object(gate, "_resolve_inputs", return_value=mock.Mock()),
        mock.patch.object(gate, "_compute", return_value=candidate),
        mock.patch.object(sys, "argv", [
            "run_c2_capital_risk_envelope_gate_v2.py",
            "--out_day_utc", DAY,
            "--input_day_utc", INPUT_DAY,
            "--truth_root", str(truth_root),
            "--produced_utc", f"{DAY}T00:00:00Z",
        ]),
    ):
        rc = gate.main()

    assert rc == 2
    repaired = json.loads(out_path.read_text(encoding="utf-8"))
    assert repaired["status"] == "FAIL"
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" in repaired["reason_codes"]
    captured = capsys.readouterr()
    assert "action=BACKFILL_REPAIRED" in captured.out
