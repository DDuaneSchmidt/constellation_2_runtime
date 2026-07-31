from __future__ import annotations

import json
from pathlib import Path

from ops.aegis import breadth_drop_validation_v1 as breadth_drop
from ops.aegis import vix_drop_validation_v1 as vix_drop
from ops.tools import create_aegis_breadth_template_v1 as create_template
from ops.tools import create_aegis_vix_template_v1 as create_vix_template
from ops.tools import ingest_aegis_breadth_drop_v1 as ingest_breadth
from ops.tools import ingest_aegis_vix_drop_v1 as ingest_vix
from ops.tools import repair_aegis_context_readiness_v1 as repair_context
from ops.tools import validate_aegis_vix_drop_v1 as validate_vix
from ops.tools import verify_aegis_vix_source_v1 as verify_vix


DAY = "2026-05-26"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _seed_vix_truth_root(tmp_path: Path, *, session_date: str = DAY, registry_status: str = "CURRENT") -> Path:
    root = tmp_path / "truth"
    _write_json(
        root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "day_utc": DAY,
            "provider_results": [
                {"provider": "CBOE", "request_status": "SUCCESS" if registry_status == "CURRENT" else "FAILED", "timestamp_utc": f"{DAY}T20:55:58Z", "returned_data_date": session_date if registry_status == "CURRENT" else ""},
                {"provider": "FRED", "request_status": "SOURCE_UNAVAILABLE", "timestamp_utc": f"{DAY}T20:55:59Z", "returned_data_date": ""},
            ],
            "symbols": {
                "VIX": {
                    "last_price": 18.25,
                    "close": 18.25,
                    "market_session_date": session_date,
                    "freshness_status": registry_status,
                    "provider": "CBOE",
                    "data_timestamp_utc": f"{session_date}T20:55:58Z",
                }
            },
        },
    )
    _write_json(
        root / "reports" / "aegis_data_registry_v1" / DAY / "data_registry.v1.json",
        {
            "data_items": [
                {
                    "data_item_id": "market.volatility.VIX",
                    "status": registry_status,
                    "provider": "CBOE",
                    "market_session_date": session_date,
                    "data_timestamp_utc": f"{session_date}T20:55:58Z",
                    "source_artifact_path": str(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"),
                    "source_hash": "vixhash",
                    "value": 18.25,
                }
            ]
        },
    )
    _write_json(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {"day_utc": DAY})
    _write_jsonl(
        root / "market_data_snapshot_v1" / "VIX" / "2026.jsonl",
        [
            {"symbol": "VIX", "timestamp_utc": "2026-05-22T20:55:00Z", "close": 17.50},
            {"symbol": "VIX", "timestamp_utc": f"{DAY}T20:55:00Z", "close": 18.25},
        ],
    )
    return root



def test_template_creation(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(breadth_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    result = create_template.main(["--truth_root", str(tmp_path / "truth"), "--day", DAY])
    assert result == 0
    template_path = breadth_drop.expected_breadth_drop_path_v1(day_utc=DAY).with_name("breadth.csv.template")
    assert template_path.exists()
    text = template_path.read_text(encoding="utf-8")
    assert "day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc" in text
    assert f"{DAY},1234,42.7,MANUAL_OPERATOR_DROP,{DAY}T20:00:00Z" in text


def test_vix_template_creation(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    result = create_vix_template.main(["--truth_root", str(tmp_path / "truth"), "--day", DAY])
    assert result == 0
    template_path = vix_drop.expected_vix_drop_path_v1(day_utc=DAY).with_name("vix.csv.template")
    assert template_path.exists()
    text = template_path.read_text(encoding="utf-8")
    assert "day_utc,vix_level,source,timestamp_utc" in text
    assert f"{DAY},18.25,MANUAL_OPERATOR_DROP,{DAY}T20:00:00Z" in text


def test_valid_breadth_csv_certifies(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(breadth_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = breadth_drop.expected_breadth_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc\n"
        f"{DAY},125,41.5,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )

    payload = breadth_drop.build_breadth_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert payload["file_found"] is True
    assert payload["schema_valid"] is True
    assert payload["certification_status"] == "CERTIFIED"
    assert payload["parsed_row"]["breadth_down_pct"] == "41.5"
    assert payload["file_hash"]



def test_ingest_valid_breadth_drop(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr(breadth_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = breadth_drop.expected_breadth_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc\n"
        f"{DAY},125,41.5,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )
    code = ingest_breadth.main(["--truth_root", str(tmp_path / "truth"), "--day", DAY])
    assert code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ingest_status"] == "INGESTED"
    assert output["certification_status"] == "CERTIFIED"
    assert output["file_hash"]


def test_valid_vix_csv_certifies(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = vix_drop.expected_vix_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,vix_level,source,timestamp_utc\n"
        f"{DAY},18.25,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )

    payload = vix_drop.build_vix_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert payload["file_found"] is True
    assert payload["schema_valid"] is True
    assert payload["certification_status"] == "CERTIFIED"
    assert payload["parsed_row"]["vix_level"] == "18.25"
    assert payload["file_hash"]


def test_ingest_valid_vix_drop(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = vix_drop.expected_vix_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,vix_level,source,timestamp_utc\n"
        f"{DAY},18.25,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )
    code = ingest_vix.main(["--truth_root", str(tmp_path / "truth"), "--day", DAY])
    assert code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ingest_status"] == "INGESTED"
    assert output["certification_status"] == "CERTIFIED"
    assert output["file_hash"]


def test_missing_breadth_csv_reports_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(breadth_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")

    payload = breadth_drop.build_breadth_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert payload["file_found"] is False
    assert payload["certification_status"] == "BLOCKED"
    assert payload["failure_reason"].startswith("BREADTH_DROP_MISSING")


def test_invalid_breadth_down_pct_rejects(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(breadth_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = breadth_drop.expected_breadth_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc\n"
        f"{DAY},125,140.0,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )

    payload = breadth_drop.build_breadth_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert payload["certification_status"] == "BLOCKED"
    assert payload["failure_reason"].startswith("BREADTH_DROP_BREADTH_DOWN_PCT_RANGE_INVALID")


def test_mismatched_day_utc_rejects(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(breadth_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = breadth_drop.expected_breadth_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc\n"
        "2026-05-25,125,41.5,MANUAL_CSV_DROP,2026-05-25T20:56:00Z\n",
        encoding="utf-8",
    )

    payload = breadth_drop.build_breadth_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert payload["certification_status"] == "BLOCKED"
    assert payload["failure_reason"].startswith("BREADTH_DROP_DAY_MISMATCH")


def test_current_vix_certifies(tmp_path: Path) -> None:
    root = _seed_vix_truth_root(tmp_path)

    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)

    assert payload["status"] == "CERTIFIED"
    assert payload["current_certified_provider"] == "CBOE"
    assert payload["vix_change_pct"] != ""
    cboe = next(row for row in payload["providers"] if row["provider"] == "CBOE")
    assert cboe["certification_status"] == "CERTIFIED"


def test_missing_vix_csv_reports_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    payload = vix_drop.build_vix_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    assert payload["file_found"] is False
    assert payload["certification_status"] == "BLOCKED"
    assert payload["failure_reason"].startswith("VIX_DROP_MISSING")


def test_invalid_vix_level_rejects(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = vix_drop.expected_vix_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,vix_level,source,timestamp_utc\n"
        f"{DAY},-1,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )
    payload = vix_drop.build_vix_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    assert payload["certification_status"] == "BLOCKED"
    assert payload["failure_reason"].startswith("VIX_DROP_LEVEL_RANGE_INVALID")


def test_vix_day_mismatch_rejects(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    drop_path = vix_drop.expected_vix_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,vix_level,source,timestamp_utc\n"
        "2026-05-25,18.25,MANUAL_CSV_DROP,2026-05-25T20:56:00Z\n",
        encoding="utf-8",
    )
    payload = vix_drop.build_vix_drop_validation_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    assert payload["certification_status"] == "BLOCKED"
    assert payload["failure_reason"].startswith("VIX_DROP_DAY_MISMATCH")


def test_prior_day_vix_is_certified_reference_for_intraday_advisory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "INTRADAY_ADVISORY")
    root = _seed_vix_truth_root(tmp_path, session_date="2026-05-22", registry_status="STALE")
    _write_jsonl(
        root / "market_calendar_v1" / "NYSE" / "2026.jsonl",
        [
            {"date": "2026-05-25", "is_trading_session": False},
            {"date": "2026-05-26", "is_trading_session": True},
        ],
    )

    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)

    assert payload["status"] == "CERTIFIED_REFERENCE"
    cboe = next(row for row in payload["providers"] if row["provider"] == "CBOE")
    assert cboe["freshness_status"] == "PRIOR_TRADING_DAY_ALLOWED"
    assert cboe["certification_status"] == "CERTIFIED_REFERENCE"
    assert cboe["source_label"] == "VIX_EOD_REFERENCE"


def test_prior_day_vix_rejected_for_strict_current_session(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "STRICT_CURRENT_SESSION")
    root = _seed_vix_truth_root(tmp_path, session_date="2026-05-22", registry_status="STALE")
    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)
    assert payload["status"] == "BLOCKED"
    cboe = next(row for row in payload["providers"] if row["provider"] == "CBOE")
    assert cboe["certification_status"] == "BLOCKED"


def test_too_old_vix_rejected_even_for_intraday_advisory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "INTRADAY_ADVISORY")
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_MAX_PRIOR_TRADING_DAYS_ALLOWED", "1")
    root = _seed_vix_truth_root(tmp_path, session_date="2026-05-20", registry_status="STALE")
    _write_jsonl(
        root / "market_data_snapshot_v1" / "VIX" / "2026.jsonl",
        [{"symbol": "VIX", "provider": "CBOE", "timestamp_utc": "2026-05-20T20:55:00Z", "close": 17.50}],
    )
    _write_jsonl(
        root / "market_calendar_v1" / "NYSE" / "2026.jsonl",
        [
            {"date": "2026-05-21", "is_trading_session": True},
            {"date": "2026-05-22", "is_trading_session": True},
            {"date": "2026-05-25", "is_trading_session": False},
            {"date": "2026-05-26", "is_trading_session": True},
        ],
    )
    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)
    assert payload["status"] == "BLOCKED"


def test_same_day_vix_accepted_for_eod_advisory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "EOD_ADVISORY")
    root = _seed_vix_truth_root(tmp_path)
    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)
    assert payload["status"] == "CERTIFIED"
    assert payload["current_certified_provider"] == "CBOE"



def test_manual_vix_current_certifies(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(vix_drop, "MANUAL_DROP_ROOT", tmp_path / "manual_drops")
    root = _seed_vix_truth_root(tmp_path, session_date="2026-05-22", registry_status="STALE")
    drop_path = vix_drop.expected_vix_drop_path_v1(day_utc=DAY)
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    drop_path.write_text(
        "day_utc,vix_level,source,timestamp_utc\n"
        f"{DAY},18.25,MANUAL_CSV_DROP,{DAY}T20:56:00Z\n",
        encoding="utf-8",
    )
    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)
    assert payload["status"] == "CERTIFIED"
    assert payload["current_certified_provider"] == "MANUAL_CSV_DROP"
    manual = next(row for row in payload["providers"] if row["provider"] == "MANUAL_CSV_DROP")
    assert manual["certification_status"] == "CERTIFIED"
    assert manual["next_repair_option"] == "npm run aegis:validate-vix-drop"


def test_vix_unavailable_reports_explicit_failure_code(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "STRICT_CURRENT_SESSION")
    root = _seed_vix_truth_root(tmp_path, session_date="2026-05-20", registry_status="STALE")
    payload = verify_vix.build_report_v1(truth_root=root, day_utc=DAY)
    assert payload["failure_code"] == "VIX_CURRENT_SOURCE_UNAVAILABLE"
    assert payload["expected_session_date"] == DAY
    next_actions = {row["provider"]: row["next_repair_option"] for row in payload["providers"]}
    assert next_actions["MANUAL_CSV_DROP"] == "npm run aegis:validate-vix-drop"
    assert next_actions["CBOE"] == "npm run aegis:refresh-market-data"
    assert next_actions["FRED"] == "Retry after final close publication or provide a manual VIX drop."


def test_repair_context_readiness_preserves_safety_gates(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"

    def fake_run(cmd: list[str], *, env: dict[str, str]) -> dict[str, object]:
        command = " ".join(cmd)
        if "validate_aegis_breadth_drop_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_breadth_drop_validation_v1" / DAY / "breadth_drop_validation.v1.json", {"expected_path": "/tmp/breadth.csv", "file_found": False, "schema_valid": False, "certification_status": "BLOCKED", "failure_reason": "BREADTH_DROP_MISSING"})
        elif "verify_aegis_vix_source_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_vix_source_verification_v1" / DAY / "vix_source_verification.v1.json", {"status": "BLOCKED", "providers": [], "current_certified_provider": "", "current_certified_value": ""})
        elif "build_aegis_market_context_provider_health_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_market_context_provider_health_v1" / DAY / "provider_health.v1.json", {"day_utc": DAY})
        elif "build_aegis_market_context_demand_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_market_context_demand_v1" / DAY / "market_context_demand.v1.json", {"day_utc": DAY})
        elif "build_event_market_snapshot_v1.py" in command:
            _write_json(truth_root / "reports" / "event_market_snapshot_v1" / DAY / "event_market_snapshot.v1.json", {"day_utc": DAY, "market_context_overall_status": "CONTEXT_BLOCKED", "market_context_items": [{"context_item_id": "vix_level", "fulfillment_status": "CONTEXT_STALE", "failure_reason": "VIX stale"}]})
        elif "run_aegis_runtime_truth_kernel_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json", {"day_utc": DAY, "runtime_truth_classification": "PARTIAL_CONTEXT", "trade_advice_allowed": False})
        elif "build_aegis_verified_runtime_graph_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_verified_runtime_graph_v1" / DAY / "verified_runtime_graph.v1.json", {"day_utc": DAY})
        elif "build_aegis_audit_handoff_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_audit_handoff_v1" / DAY / "aegis_audit_handoff.v1.json", {"day_utc": DAY})
        return {"command": command, "exit_code": 0, "stdout": "", "stderr": "", "started_at_utc": DAY, "completed_at_utc": DAY}

    monkeypatch.setattr(repair_context, "_run", fake_run)
    payload = repair_context.build_context_readiness_repair_v1(truth_root=truth_root, day_utc=DAY)

    assert payload["breadth_status"] == "BLOCKED"
    assert payload["vix_status"] == "BLOCKED"
    assert payload["closeout_summary"]["trade_advice_allowed"] is False
    assert payload["event_market_snapshot_status"] == "CONTEXT_BLOCKED"
    assert payload["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert payload["trade_advice_allowed"] is False
    assert payload["safety"]["trade_advice_allowed"] is False
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False
