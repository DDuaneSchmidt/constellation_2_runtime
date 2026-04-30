from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.aegis_chatgpt_packet as packet_module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_stale_operator_gate_is_diagnostic_only_when_day_authority_is_newer(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-27"
    truth_root = (tmp_path / "truth").resolve()
    state_root = (tmp_path / "state").resolve()

    market_calendar_path = (truth_root / "market_calendar_v1" / "NYSE" / "2026.jsonl").resolve()
    market_calendar_path.parent.mkdir(parents=True, exist_ok=True)
    market_calendar_path.write_text(
        json.dumps({"day_utc": day_utc, "is_trading_session": True}) + "\n",
        encoding="utf-8",
    )

    _write_json(
        truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / day_utc
        / "paper_trading_day_authority.v1.json",
        {
            "state": "PREFLIGHT_BLOCKED",
            "can_submit_paper_orders": False,
            "canonical_blocker": "C2_KILL_SWITCH_ACTIVE",
            "reason_codes": ["C2_KILL_SWITCH_ACTIVE"],
            "produced_at_utc": "2026-04-27T12:00:00Z",
        },
    )

    _write_json(
        state_root / f"operator_gate_{day_utc}.v1.json",
        {
            "status": "FAIL",
            "session_day_blocker": "SESSION_AUTHORITY_MISSING",
            "reason_codes": ["SESSION_AUTHORITY_MISSING"],
            "produced_utc": "2026-04-27T09:00:00Z",
        },
    )

    monkeypatch.setattr(packet_module, "_today_utc_day", lambda: day_utc)
    monkeypatch.setattr(packet_module, "OPERATOR_GATE_STATE_ROOT", state_root)

    roots = packet_module.RootResolution(
        canonical_truth_root=truth_root,
        runtime_truth_root=truth_root,
        truth_sleeves_root=None,
        authority_source="test",
        evidence="test",
        error="",
    )

    status = packet_module._build_current_calendar_day_runtime_status(roots)

    assert status.status == "NOT_READY"
    assert status.canonical_blocker == "C2_KILL_SWITCH_ACTIVE"
    assert "operator_gate=STALE_DIAGNOSTIC_ONLY/IGNORED_CANONICAL_DAY_AUTHORITY_NEWER" in status.service_status_summary


def test_current_day_packet_uses_day_run_as_canonical_authority(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-30"
    truth_root = (tmp_path / "truth").resolve()

    market_calendar_path = (truth_root / "market_calendar_v1" / "NYSE" / "2026.jsonl").resolve()
    market_calendar_path.parent.mkdir(parents=True, exist_ok=True)
    market_calendar_path.write_text(
        json.dumps({"day_utc": day_utc, "is_trading_session": True}) + "\n",
        encoding="utf-8",
    )

    _write_json(
        truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / day_utc
        / "paper_trading_day_authority.v1.json",
        {
            "state": "PREFLIGHT_BLOCKED",
            "can_submit_paper_orders": False,
            "canonical_blocker": "OPTIONS_SNAPSHOT_SYMBOL_MISSING",
            "reason_codes": ["OPTIONS_SNAPSHOT_SYMBOL_MISSING"],
            "produced_at_utc": "2026-04-30T21:00:00Z",
        },
    )
    _write_json(
        truth_root
        / "reports"
        / "aegis_day_run_v1"
        / day_utc
        / "day_run.v1.json",
        {
            "day_utc": day_utc,
            "environment": "PAPER",
            "final_status": "NOT_READY",
            "canonical_phase": "MARKET_OPEN_DATA_GATE",
            "canonical_blocker": "MARKET_CLOSED",
            "updated_at_utc": "2026-04-30T21:15:00Z",
        },
    )

    monkeypatch.setattr(packet_module, "_today_utc_day", lambda: day_utc)

    roots = packet_module.RootResolution(
        canonical_truth_root=truth_root,
        runtime_truth_root=truth_root,
        truth_sleeves_root=None,
        authority_source="test",
        evidence="test",
        error="",
    )

    status = packet_module._build_current_calendar_day_runtime_status(roots)

    assert status.status == "NOT_READY"
    assert status.canonical_blocker == "MARKET_CLOSED"
    assert "day_run=NOT_READY/MARKET_CLOSED" in status.service_status_summary
    assert "OPTIONS_SNAPSHOT_SYMBOL_MISSING" not in status.service_status_summary
