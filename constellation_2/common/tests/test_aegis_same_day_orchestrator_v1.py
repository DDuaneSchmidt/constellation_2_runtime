from __future__ import annotations

import json
import os
import time
from pathlib import Path

from ops.tools import run_aegis_same_day_orchestrator_v1 as orchestrator


DAY = "2026-05-04"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows) + "\n", encoding="utf-8")


def _calendar(root: Path, *, trading: bool = True) -> None:
    _write_json(
        root / "market_calendar_v1" / "dataset_manifest.json",
        {"files": [{"year": 2026, "file": "NYSE/2026.jsonl"}]},
    )
    row = {
        "dataset_version": "v1",
        "day_utc": DAY,
        "exchange": "NYSE",
        "ingested_utc": "2026-05-01T00:00:00Z",
        "is_trading_session": trading,
        "source_hash": "a" * 64,
        "source_name": "test",
    }
    path = root / "market_calendar_v1" / "NYSE" / "2026.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _ctx(tmp_path: Path, *, phase: str = "PRE_SUBMIT") -> orchestrator.OrchestratorContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "execution"
    runtime = tmp_path / "runtime"
    _calendar(truth)
    return orchestrator.OrchestratorContext(
        day_utc=DAY,
        phase=phase,
        truth_root=truth.resolve(),
        execution_root=execution.resolve(),
        runtime_root=runtime.resolve(),
        environment="PAPER",
        ib_account="DU123",
        intent_id="intent-1",
    )


def _simple_dag(path: Path, *, first_output: str = "{truth_root}/reports/n1/{day_utc}/n1.json") -> Path:
    payload = {
        "producer_nodes": [
            {
                "producer_id": "n1",
                "command": "run n1",
                "execution_mode": "ONE_SHOT",
                "inputs": [],
                "outputs": [
                    {
                        "artifact_id": "n1",
                        "path": first_output,
                        "artifact_type": "json",
                        "schema_path": "",
                        "required": True,
                        "target_day_required": True,
                        "truth_root_required": "truth_root",
                        "freshness_seconds": None,
                    }
                ],
                "recovery_command": "recover n1",
            },
            {
                "producer_id": "n2",
                "command": "run n2",
                "execution_mode": "ONE_SHOT",
                "inputs": ["n1"],
                "outputs": [
                    {
                        "artifact_id": "n2",
                        "path": "{truth_root}/reports/n2/{day_utc}/n2.json",
                        "artifact_type": "json",
                        "schema_path": "",
                        "required": True,
                        "target_day_required": True,
                        "truth_root_required": "truth_root",
                        "freshness_seconds": None,
                    }
                ],
                "recovery_command": "recover n2",
            },
        ],
        "phase_scheduler": {
            "PRE_SUBMIT": {
                "required_producers": ["n1", "n2"],
                "allowed_producers": ["n1", "n2"],
                "forbidden_producers": [],
                "freshness_thresholds": {},
                "stop_conditions": ["PRODUCER_FAILED", "ARTIFACT_INVALID"],
            }
        },
    }
    _write_json(path, payload)
    return path


def test_default_pre_submit_dag_runs_in_dependency_order() -> None:
    dag = orchestrator.load_producer_dag_v1()
    ids = [node["producer_id"] for node in orchestrator.phase_nodes_v1(dag, "PRE_SUBMIT")]

    assert ids == [
        "broker_event_observer",
        "broker_supply_v1",
        "runtime_resilience_authority_v1",
        "session_readiness_refresh_v1",
        "day_authority_decision_v1",
        "target_day_admission_v1",
        "day_activation_package_v1",
        "global_context_package_v1",
        "execution_build_v1",
        "execution_package_v1",
    ]


def test_missing_artifact_blocks_and_downstream_is_not_run(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    dag_path = _simple_dag(tmp_path / "dag.json")
    calls: list[str] = []

    def runner(command: str) -> dict:
        calls.append(command)
        return {"returncode": 0, "stdout": "", "stderr": ""}

    payload = orchestrator.run_orchestration_v1(ctx=ctx, dag_path=dag_path, command_runner=runner)

    assert payload["final_status"] == "BLOCKED"
    assert payload["blockers"] == ["N1_MISSING"]
    assert calls == ["run n1"]
    assert [row["producer_id"] for row in payload["producer_results"]] == ["n1"]
    assert payload["readiness_effect"] == "NONE"
    assert payload["submit_effect"] == "NONE"


def test_failed_producer_stops_downstream_producers(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    dag_path = _simple_dag(tmp_path / "dag.json")
    calls: list[str] = []

    def runner(command: str) -> dict:
        calls.append(command)
        return {"returncode": 2, "stdout": "", "stderr": "failed"}

    payload = orchestrator.run_orchestration_v1(ctx=ctx, dag_path=dag_path, command_runner=runner)

    assert payload["final_status"] == "BLOCKED"
    assert payload["blockers"] == ["N1_PRODUCER_FAILED"]
    assert calls == ["run n1"]


def test_wrong_day_artifact_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    dag_path = _simple_dag(tmp_path / "dag.json")

    def runner(_command: str) -> dict:
        _write_json(ctx.truth_root / "reports" / "n1" / DAY / "n1.json", {"day_utc": "2026-05-05"})
        return {"returncode": 0, "stdout": "", "stderr": ""}

    payload = orchestrator.run_orchestration_v1(ctx=ctx, dag_path=dag_path, command_runner=runner)

    assert payload["final_status"] == "BLOCKED"
    assert payload["artifact_results"][0]["target_day_status"] == "FAIL"
    assert payload["blockers"] == ["N1_WRONG_DAY"]


def test_wrong_root_artifact_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    wrong = tmp_path / "wrong" / "n1.json"
    dag_path = _simple_dag(tmp_path / "dag.json", first_output=str(wrong))

    def runner(_command: str) -> dict:
        _write_json(wrong, {"day_utc": DAY})
        return {"returncode": 0, "stdout": "", "stderr": ""}

    payload = orchestrator.run_orchestration_v1(ctx=ctx, dag_path=dag_path, command_runner=runner)

    assert payload["final_status"] == "BLOCKED"
    assert payload["artifact_results"][0]["truth_root_status"] == "FAIL"
    assert payload["blockers"] == ["N1_WRONG_ROOT"]


def test_stale_broker_event_artifact_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path, phase="MARKET_OPEN")
    event_log = ctx.execution_root / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl"
    _write_jsonl(event_log, [{"event_type": "nextValidId", "received_utc": f"{DAY}T14:00:00Z"}])
    old = time.time() - 1000
    os.utime(event_log, (old, old))

    payload = orchestrator.run_orchestration_v1(ctx=ctx, command_runner=lambda _command: {"returncode": 0})

    assert payload["final_status"] == "BLOCKED"
    assert payload["artifact_results"][0]["freshness_status"] == "FAIL"
    assert payload["blockers"] == ["BROKER_EVENT_LOG_V1_STALE"]


def test_non_trading_day_skips_trading_producers(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path, phase="PRE_SUBMIT")
    _calendar(ctx.truth_root, trading=False)
    payload = orchestrator.run_orchestration_v1(ctx=ctx, command_runner=lambda _command: {"returncode": 1})

    assert payload["final_status"] == "SKIPPED_NON_TRADING_DAY"
    assert payload["producer_results"] == []
    assert payload["artifact_results"] == []


def test_orchestrator_cannot_set_readiness_or_submit_permission(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    dag_path = _simple_dag(tmp_path / "dag.json")

    def runner(command: str) -> dict:
        producer = command.split()[-1]
        _write_json(ctx.truth_root / "reports" / producer / DAY / f"{producer}.json", {"day_utc": DAY})
        return {"returncode": 0, "stdout": "", "stderr": ""}

    payload = orchestrator.run_orchestration_v1(ctx=ctx, dag_path=dag_path, command_runner=runner)

    assert payload["final_status"] == "PASS"
    assert payload["readiness_authority"] == "aegis_control_plane_v1"
    assert payload["submit_authority"] == "aegis_submit_enforcement_v1"
    assert payload["readiness_effect"] == "NONE"
    assert payload["submit_effect"] == "NONE"
    assert "submit_allowed" not in payload
    assert payload["final_status"] != "READY"


def test_ledger_schema_validation_and_write(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    dag_path = _simple_dag(tmp_path / "dag.json")

    def runner(command: str) -> dict:
        producer = command.split()[-1]
        _write_json(ctx.truth_root / "reports" / producer / DAY / f"{producer}.json", {"day_utc": DAY})
        return {"returncode": 0, "stdout": "", "stderr": ""}

    payload = orchestrator.run_orchestration_v1(ctx=ctx, dag_path=dag_path, command_runner=runner)
    path = orchestrator.write_ledger_v1(ctx=ctx, payload=payload, command="test")

    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["schema_id"] == "orchestration_run_v1"
    assert written["producer_contract_v1"]["producer_name"] == orchestrator.PRODUCER
