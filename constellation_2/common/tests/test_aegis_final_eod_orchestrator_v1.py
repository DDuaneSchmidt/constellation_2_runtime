from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis import final_eod_orchestrator_v1 as orch

DAY = "2026-05-22"


def _write(path: Path, payload: dict | None = None) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload or {"ok": True}, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _patch_successful_stages(monkeypatch, tmp_path: Path) -> None:
    final_artifact = tmp_path / "final_eod.json"
    final_manifest = tmp_path / "final_eod_manifest.json"
    _write(final_artifact, {"schema_id": "final_eod_market_data_v1", "symbols": {"SPY": {}}})
    _write(final_manifest, {"schema_id": "final_eod_market_data_current_manifest.v1", "artifact_path": str(final_artifact)})

    monkeypatch.setattr(orch, "certify_tiingo_provider_v1", lambda **_: {
        "ok": True,
        "result_status": "CERTIFIED",
        "build_result": {"artifact_path": str(final_artifact), "current_manifest_json": str(final_manifest)},
    })
    monkeypatch.setattr(orch, "build_market_data_inputs_v1", lambda **_: {
        "status": "READY",
        "downstream_certification_invariant": {"status": "PASS"},
    })
    monkeypatch.setattr(orch, "write_market_data_inputs_v1", lambda **_: {"market_data_inputs_json": _write(tmp_path / "market_data_inputs.json")})
    monkeypatch.setattr(orch, "emit_market_data_inputs_events_v1", lambda **_: None)
    monkeypatch.setattr(orch, "build_runtime_symbol_universe_v1", lambda **_: {"requested_symbols": ["SPY"]})
    monkeypatch.setattr(orch, "build_data_registry_v1", lambda **_: {"missing_items": []})
    monkeypatch.setattr(orch, "write_data_registry_v1", lambda **_: {"data_registry_json": _write(tmp_path / "data_registry.json")})
    monkeypatch.setattr(orch, "build_sleeve_readiness_v1", lambda **_: {"blocked_count": 0})
    monkeypatch.setattr(orch, "write_sleeve_readiness_v1", lambda **_: {"sleeve_readiness_json": _write(tmp_path / "sleeve_readiness.json")})
    monkeypatch.setattr(orch, "build_domain_certification_report_v1", lambda **_: {"summary": {"CERTIFIED": 1}})
    monkeypatch.setattr(orch, "write_domain_certification_report_v1", lambda **_: {"domain_certification_json": _write(tmp_path / "domain_certification.json")})


def _ledger_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_final_eod_orchestrator_records_single_run_ledger(tmp_path: Path, monkeypatch) -> None:
    _patch_successful_stages(monkeypatch, tmp_path)

    result = orch.run_final_eod_certification_pipeline_v1(
        truth_root=tmp_path / "truth",
        repo_root=tmp_path,
        day_utc=DAY,
        run_id="unit-run-success",
        run_repair=False,
    )

    rows = _ledger_rows(Path(result["ledger_jsonl"]))
    assert result["status"] == "SUCCEEDED"
    assert [row["stage"] for row in rows] == [
        "EOD_SOURCE_CERTIFY",
        "MARKET_DATA_INPUTS",
        "DATA_REGISTRY",
        "SLEEVE_READINESS",
        "DOMAIN_CERTIFICATION",
    ]
    assert {row["run_id"] for row in rows} == {"unit-run-success"}
    assert rows[0]["output_hashes"]["final_eod_artifact"]
    assert result["broker_execution_allowed"] is False
    assert result["autonomous_execution_allowed"] is False
    assert Path(result["paths"]["json"]).exists()


def test_final_eod_orchestrator_interruption_after_mid_run_stage_is_ledgored(tmp_path: Path, monkeypatch) -> None:
    _patch_successful_stages(monkeypatch, tmp_path)

    result = orch.run_final_eod_certification_pipeline_v1(
        truth_root=tmp_path / "truth",
        repo_root=tmp_path,
        day_utc=DAY,
        run_id="unit-run-interrupted",
        run_repair=False,
        stop_after_stage="MARKET_DATA_INPUTS",
    )

    rows = _ledger_rows(Path(result["ledger_jsonl"]))
    assert result["status"] == "INTERRUPTED"
    assert result["failure_reason"] == "Interrupted after MARKET_DATA_INPUTS by stop_after_stage."
    assert [row["stage"] for row in rows] == ["EOD_SOURCE_CERTIFY", "MARKET_DATA_INPUTS"]
    assert all(row["status"] == "SUCCEEDED" for row in rows)


def test_final_eod_orchestrator_records_provider_rate_limit_failure(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(orch, "certify_tiingo_provider_v1", lambda **_: {
        "ok": False,
        "status": "FAILED",
        "failure_reason": "TIINGO_RATE_LIMIT_429",
        "provider_attempts": [{"provider": "TIINGO", "status": "RATE_LIMITED", "http_status": 429}],
        "build_result": {},
    })

    result = orch.run_final_eod_certification_pipeline_v1(
        truth_root=tmp_path / "truth",
        repo_root=tmp_path,
        day_utc=DAY,
        run_id="unit-run-429",
        run_repair=False,
    )

    rows = _ledger_rows(Path(result["ledger_jsonl"]))
    assert result["status"] == "FAILED"
    assert "TIINGO_RATE_LIMIT_429" in result["failure_reason"]
    assert [row["stage"] for row in rows] == ["EOD_SOURCE_CERTIFY"]
    assert rows[0]["status"] == "FAILED"
    assert rows[0]["failure_reason"] == "TIINGO_RATE_LIMIT_429"
