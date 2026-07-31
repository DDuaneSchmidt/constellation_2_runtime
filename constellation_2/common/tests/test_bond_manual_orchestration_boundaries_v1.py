from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import ops.tools.run_bond_manual_monitor_v1 as bond_monitor
import ops.tools.run_c2_multi_sleeve_orchestrator_v1 as multi_orchestrator


DAY = "2026-04-16"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _bond_registry_row() -> dict:
    return {
        "sleeve_id": "BOND",
        "enabled": True,
        "mode": "PAPER",
        "execution_mode": "MANUAL",
        "status": "MANUAL_PRODUCTION",
        "asset_class": "FIXED_INCOME",
        "sleeve_type": "BOND",
        "display_name": "Bond Sleeve",
        "ui_visible": True,
        "allocator_visible": True,
        "automated_execution_enabled": False,
        "broker_execution_allowed": False,
        "advisory_only": True,
        "ib_account": "DUO847203",
        "symbols": ["BND", "TLT"],
        "truth_partition": "truth_sleeves/BOND/PAPER",
    }


def test_bond_manual_monitor_writes_durable_manual_recommendation(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"

    rc = bond_monitor.main(["--day_utc", DAY, "--truth_root", str(truth_root)])

    artifact_path = truth_root / "reports" / "bond_sleeve_recommendation_v2" / DAY / "bond_sleeve_recommendation.v2.json"
    display_head_path = artifact_path.parent / "display_head_pointer.v1.json"
    pointer_index_path = artifact_path.parent / "canonical_pointer_index.v1.jsonl"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    display_head = json.loads(display_head_path.read_text(encoding="utf-8"))

    assert rc == 0
    assert artifact["sleeve_id"] == "BOND"
    assert artifact["display_name"] == "Bond Sleeve"
    assert artifact["execution_mode"] == "MANUAL"
    assert artifact["manual_execution_only"] is True
    assert artifact["advisory_only"] is True
    assert artifact["broker_execution_allowed"] is False
    assert artifact["automated_execution_allowed"] is False
    assert artifact["manual_status"] == "ADVISORY_ONLY"
    assert display_head["points_to"] == str(artifact_path)
    assert pointer_index_path.read_text(encoding="utf-8").strip()


def test_bond_registry_row_is_manual_visible_and_not_automated_paper() -> None:
    row = _bond_registry_row()

    assert multi_orchestrator.sleeve_is_automated_paper_execution(row) is False
    manual_rollup = multi_orchestrator.manual_sleeve_rollup_entry(row)
    assert manual_rollup == {
        "sleeve_id": "BOND",
        "enabled": True,
        "status": "SKIP_MANUAL_ADVISORY",
        "mode": "PAPER",
        "reason_code": "SLEEVE_MANUAL_ADVISORY_NOT_AUTOMATED_PAPER",
    }


def test_multi_sleeve_orchestrator_skips_bond_without_automated_artifacts(monkeypatch, tmp_path: Path) -> None:
    registry_path = tmp_path / "C2_SLEEVE_REGISTRY_V1.json"
    _write_json(
        registry_path,
        {
            "schema_id": "c2_sleeve_registry",
            "schema_version": "v1",
            "sleeves": [_bond_registry_row()],
        },
    )
    captured: dict[str, object] = {}
    orchestrator_calls: list[tuple] = []

    def fake_write_rollup(day: str, payload: dict) -> Path:
        captured["day"] = day
        captured["payload"] = payload
        out_path = tmp_path / "rollup" / day / "sleeve_rollup.v1.json"
        _write_json(out_path, payload)
        return out_path

    monkeypatch.setattr(multi_orchestrator, "REGISTRY_PATH", registry_path)
    monkeypatch.setattr(multi_orchestrator, "ROLLOUP_ROOT", tmp_path / "rollup")
    monkeypatch.setattr(multi_orchestrator, "write_rollup", fake_write_rollup)
    monkeypatch.setattr(
        multi_orchestrator,
        "assert_paper_session_ledger_open_ready_v1",
        lambda **_kwargs: SimpleNamespace(
            session_id="session-1",
            ledger_id="ledger-1",
            control_state={"authority_status": "GRANTED"},
        ),
    )
    monkeypatch.setattr(multi_orchestrator, "run_orchestrator_v2", lambda *args, **kwargs: orchestrator_calls.append((args, kwargs)))
    monkeypatch.setattr(
        multi_orchestrator,
        "resolve_sleeve_truth_root",
        lambda sleeve: (_ for _ in ()).throw(AssertionError("manual BOND must not resolve automated truth root")),
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_c2_multi_sleeve_orchestrator_v1.py",
            "--day_utc",
            DAY,
            "--paper_session_ledger_path",
            str(tmp_path / "paper_session.json"),
        ],
    )

    rc = multi_orchestrator.main()

    payload = captured["payload"]
    bond = payload["sleeves"][0]
    assert rc == 0
    assert orchestrator_calls == []
    assert payload["status"] == "PASS"
    assert bond["sleeve_id"] == "BOND"
    assert bond["status"] == "SKIP_MANUAL_ADVISORY"
    assert bond["reason_code"] == "SLEEVE_MANUAL_ADVISORY_NOT_AUTOMATED_PAPER"
    assert "truth_root" not in bond
    assert "cmd" not in bond
