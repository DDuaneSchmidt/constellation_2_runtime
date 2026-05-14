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


def test_operator_statement_defaults_to_active_release_operator_inputs_for_paper() -> None:
    truth_root = Path("/home/node/constellation_runtime_data/truth")
    operator_input_root = startup_materialization_input_module._runtime_operator_input_root(
        truth_root=truth_root,
        environment="PAPER",
    )
    assert operator_input_root == Path("/home/node/constellation/constellation_2")
    assert startup_materialization_input_module._operator_statement_path(
        operator_input_root=operator_input_root,
        day_utc=DAY,
    ) == Path(
        "/home/node/constellation/constellation_2/operator_inputs/cash_ledger_operator_statements/2026-04-14/operator_statement.v1.json"
    )


def test_operator_statement_non_paper_default_preserves_runtime_parent_root() -> None:
    truth_root = Path("/home/node/constellation_runtime_data/truth")
    operator_input_root = startup_materialization_input_module._runtime_operator_input_root(
        truth_root=truth_root,
        environment="LIVE",
    )
    assert operator_input_root == Path("/home/node/constellation_runtime_data")


def test_startup_materialization_critical_path_python_uses_current_execution_python(tmp_path: Path, monkeypatch) -> None:
    current_python = tmp_path / "python-current"
    current_python.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setattr(startup_materialization_input_module.sys, "executable", str(current_python))
    assert startup_materialization_input_module._critical_path_python() == current_python.resolve()


def test_startup_materialization_v1_passes_operator_and_execution_roots_to_input_convergence(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "runtime" / "truth"
    operator_input_root = tmp_path / "release" / "constellation_2"
    execution_truth_root = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    captured: dict[str, object] = {}

    class Result:
        returncode = 0
        stdout = "OK"
        stderr = ""

    def fake_run(cmd, cwd=None, capture_output=False, text=False, check=False):
        captured["cmd"] = list(cmd)
        return Result()

    monkeypatch.setattr(startup_materialization_module.subprocess, "run", fake_run)

    result = startup_materialization_module._run_input_convergence(
        day_utc=DAY,
        truth_root=truth_root,
        operator_input_root=operator_input_root,
        execution_truth_root=execution_truth_root,
        ib_account="DUO847203",
    )

    cmd = captured["cmd"]
    assert result["returncode"] == 0
    assert "--operator_input_root" in cmd
    assert cmd[cmd.index("--operator_input_root") + 1] == str(operator_input_root)
    assert "--execution_truth_root" in cmd
    assert cmd[cmd.index("--execution_truth_root") + 1] == str(execution_truth_root)
    assert "--environment" in cmd
    assert cmd[cmd.index("--environment") + 1] == "PAPER"
    assert "--ib_account" in cmd
    assert cmd[cmd.index("--ib_account") + 1] == "DUO847203"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _operator_statement_path(runtime_root: Path, day_utc: str) -> Path:
    return runtime_root / "operator_inputs" / "cash_ledger_operator_statements" / day_utc / "operator_statement.v1.json"


def _cash_ledger_snapshot_path(execution_truth_root: Path, day_utc: str) -> Path:
    return execution_truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"


def _nav_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json"


def _inputs_prep_path(truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "startup_materialization_inputs_prep_v1"
        / day_utc
        / "startup_materialization_inputs_prep.v1.json"
    )


def _seed_startup_inputs(
    runtime_root: Path,
    truth_root: Path,
    execution_truth_root: Path,
    day_utc: str,
    *,
    include_operator: bool = True,
    include_cash: bool = True,
) -> None:
    if include_operator:
        _write_json(
            _operator_statement_path(runtime_root, day_utc),
            {
                "observed_at_utc": f"{day_utc}T00:00:00Z",
                "currency": "USD",
                "cash_total": "0.00",
                "nlv_total": "0.00",
                "account_id": "DUO847203",
                "notes": ["test governed runtime operator statement"],
            },
        )
    if include_cash:
        _write_json(_cash_ledger_snapshot_path(execution_truth_root, day_utc), {"day_utc": day_utc, "status": "OK"})
    _write_json(_nav_path(truth_root, day_utc), {"day_utc": day_utc, "status": "OK"})
    _write_json(_inputs_prep_path(truth_root, day_utc), {"day_utc": day_utc, "status": "PASS"})


def _stub_convergence_runtime(monkeypatch, truth_root: Path) -> None:
    monkeypatch.setattr(startup_materialization_input_module, "resolve_decision_truth_root_v1", lambda raw, repo_root=None: truth_root)
    monkeypatch.setattr(startup_materialization_input_module, "resolve_single_paper_ib_account_from_sleeve_registry", lambda repo_root: "DUO847203")
    monkeypatch.setattr(startup_materialization_input_module, "_git_sha", lambda: "a" * 40)
    monkeypatch.setattr(
        startup_materialization_input_module,
        "_tool_result",
        lambda command, env_overrides=None, logical_name="": {
            "script": logical_name,
            "command": command,
            "return_code": 0,
            "stdout": "stubbed",
            "stderr": "",
        },
    )


def _read_convergence(truth_root: Path, day_utc: str) -> dict:
    path = (
        truth_root
        / "reports"
        / "startup_materialization_input_convergence_v1"
        / day_utc
        / "startup_materialization_input_convergence.v1.json"
    )
    return json.loads(path.read_text(encoding="utf-8"))


def test_startup_materialization_accepts_runtime_operator_statement_without_repo_local_inputs(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "constellation_runtime_data"
    truth_root = runtime_root / "truth"
    execution_truth_root = runtime_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    _seed_startup_inputs(runtime_root, truth_root, execution_truth_root, DAY)
    _stub_convergence_runtime(monkeypatch, truth_root)

    rc = startup_materialization_input_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--operator_input_root",
            str(runtime_root),
            "--execution_truth_root",
            str(execution_truth_root),
        ]
    )

    payload = _read_convergence(truth_root, DAY)
    operator_row = next(row for row in payload["artifact_results"] if row["artifact_id"] == "operator_statement_v1")
    cash_row = next(row for row in payload["artifact_results"] if row["artifact_id"] == "cash_ledger_snapshot_v1")
    assert rc == 0
    assert payload["convergence_status"] == "SUCCESS"
    assert operator_row["ready"] is True
    assert operator_row["artifact_path"] == str(_operator_statement_path(runtime_root, DAY).resolve())
    assert cash_row["ready"] is True
    assert cash_row["artifact_path"] == str(_cash_ledger_snapshot_path(execution_truth_root, DAY).resolve())
    assert "/constellation_2/operator_inputs/" not in operator_row["artifact_path"]


def test_startup_materialization_blocks_when_runtime_operator_statement_missing(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "constellation_runtime_data"
    truth_root = runtime_root / "truth"
    execution_truth_root = runtime_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    _seed_startup_inputs(runtime_root, truth_root, execution_truth_root, DAY, include_operator=False)
    _stub_convergence_runtime(monkeypatch, truth_root)

    rc = startup_materialization_input_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--operator_input_root",
            str(runtime_root),
            "--execution_truth_root",
            str(execution_truth_root),
        ]
    )

    payload = _read_convergence(truth_root, DAY)
    operator_row = next(row for row in payload["artifact_results"] if row["artifact_id"] == "operator_statement_v1")
    assert rc == 2
    assert payload["convergence_status"] == "BLOCKED"
    assert operator_row["blocker_code"] == "OPERATOR_STATEMENT_V1_MISSING"


def test_startup_materialization_requires_cash_ledger_snapshot_separately(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "constellation_runtime_data"
    truth_root = runtime_root / "truth"
    execution_truth_root = runtime_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    _seed_startup_inputs(runtime_root, truth_root, execution_truth_root, DAY, include_cash=False)
    _stub_convergence_runtime(monkeypatch, truth_root)

    rc = startup_materialization_input_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--operator_input_root",
            str(runtime_root),
            "--execution_truth_root",
            str(execution_truth_root),
        ]
    )

    payload = _read_convergence(truth_root, DAY)
    artifact_ids = {row["artifact_id"] for row in payload["artifact_results"]}
    cash_row = next(row for row in payload["artifact_results"] if row["artifact_id"] == "cash_ledger_snapshot_v1")
    assert rc == 2
    assert {"operator_statement_v1", "cash_ledger_snapshot_v1"}.issubset(artifact_ids)
    assert cash_row["blocker_code"] == "CASH_LEDGER_SNAPSHOT_V1_MISSING"


def test_startup_materialization_ignores_cash_snapshot_only_under_wrong_canonical_root(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "constellation_runtime_data"
    truth_root = runtime_root / "truth"
    execution_truth_root = runtime_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    _seed_startup_inputs(runtime_root, truth_root, execution_truth_root, DAY, include_cash=False)
    _write_json(_cash_ledger_snapshot_path(truth_root, DAY), {"day_utc": DAY, "status": "OK"})
    _stub_convergence_runtime(monkeypatch, truth_root)

    rc = startup_materialization_input_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--operator_input_root",
            str(runtime_root),
            "--execution_truth_root",
            str(execution_truth_root),
        ]
    )

    payload = _read_convergence(truth_root, DAY)
    cash_row = next(row for row in payload["artifact_results"] if row["artifact_id"] == "cash_ledger_snapshot_v1")
    assert rc == 2
    assert payload["convergence_status"] == "BLOCKED"
    assert cash_row["blocker_code"] == "CASH_LEDGER_SNAPSHOT_V1_MISSING"
    assert cash_row["artifact_path"] == str(_cash_ledger_snapshot_path(execution_truth_root, DAY).resolve())


def test_startup_materialization_ignores_operator_statement_only_under_wrong_runtime_root(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "constellation_runtime_data"
    active_operator_root = tmp_path / "active_release" / "constellation_2"
    truth_root = runtime_root / "truth"
    execution_truth_root = runtime_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    _seed_startup_inputs(runtime_root, truth_root, execution_truth_root, DAY)
    _stub_convergence_runtime(monkeypatch, truth_root)

    rc = startup_materialization_input_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--operator_input_root",
            str(active_operator_root),
            "--execution_truth_root",
            str(execution_truth_root),
        ]
    )

    payload = _read_convergence(truth_root, DAY)
    operator_row = next(row for row in payload["artifact_results"] if row["artifact_id"] == "operator_statement_v1")
    assert rc == 2
    assert payload["convergence_status"] == "BLOCKED"
    assert operator_row["blocker_code"] == "OPERATOR_STATEMENT_V1_MISSING"
    assert operator_row["artifact_path"] == str(_operator_statement_path(active_operator_root, DAY).resolve())
