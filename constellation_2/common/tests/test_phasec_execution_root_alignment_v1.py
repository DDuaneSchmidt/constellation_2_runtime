from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_phasec_identity_materializer_day_v1 as materializer_module  # noqa: E402
import ops.tools.run_startup_materialization_v1 as startup_module  # noqa: E402


def test_phasec_materializer_defaults_execution_truth_root_to_truth_root(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)

    resolved = materializer_module._resolve_execution_truth_root("", read_truth_root=truth_root)

    assert resolved == truth_root


def test_startup_materialization_invokes_phasec_materializer_with_execution_truth_root(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return Result()

    with patch.object(startup_module.subprocess, "run", side_effect=fake_run):
        startup_module._run_phasec_materializer(
            day_utc="2026-04-14",
            truth_root=tmp_path / "truth",
            execution_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
            default_equity_reference_price="679.91",
            equity_reference_prices_by_symbol={"SPY": "679.91", "DBC": "23.45"},
        )

    assert calls
    cmd = calls[0]
    assert "--truth_root" in cmd
    assert cmd[cmd.index("--truth_root") + 1] == str(tmp_path / "truth")
    assert "--execution_truth_root" in cmd
    assert cmd[cmd.index("--execution_truth_root") + 1] == str(tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER")
    assert "--equity_reference_prices_by_symbol_json" in cmd
    per_symbol_prices = json.loads(cmd[cmd.index("--equity_reference_prices_by_symbol_json") + 1])
    assert per_symbol_prices == {"DBC": "23.45", "SPY": "679.91"}


def test_phasec_materializer_help_prefers_authoritative_repo_over_shadow_pythonpath() -> None:
    script_path = SOURCE_ROOT / "ops/tools/run_phasec_identity_materializer_day_v1.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = "/home/node/constellation_2_runtime"

    proc = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        cwd=str(SOURCE_ROOT),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert proc.returncode == 0
    assert "run_phasec_identity_materializer_day_v1" in proc.stdout


def test_accounting_nav_bridge_proof_mode_bootstraps_authoritative_repo_without_pythonpath(tmp_path: Path) -> None:
    script_path = SOURCE_ROOT / "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py"
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)

    proc = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--day_utc",
            "2026-04-14",
            "--truth_root",
            str(truth_root),
            "--proof-mode",
            "import_only",
        ],
        cwd=str(SOURCE_ROOT),
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        check=False,
    )

    assert proc.returncode == 0
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["module"] == "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py"
    assert payload["repo_root"] == str(SOURCE_ROOT)
    assert payload["pythonpath"] == ""


def test_phasec_entry_transformer_uses_execution_truth_root(tmp_path: Path) -> None:
    read_truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    read_truth_root.mkdir(parents=True)
    execution_truth_root.mkdir(parents=True)

    day_utc = "2026-04-16"
    attempt_day_dir = execution_truth_root / "phaseC_preflight_v1" / day_utc / "attempt_A0001"
    attempt_day_dir.mkdir(parents=True, exist_ok=True)
    intent_path = tmp_path / "spy.exposure_intent.v1.json"
    intent_path.write_text(json.dumps({"schema_id": "exposure_intent", "target_notional_pct": "0.10"}) + "\n", encoding="utf-8")

    calls: list[list[str]] = []

    def fake_run(cmd, *, cwd):
        calls.append(cmd)
        out_dir = Path(cmd[cmd.index("--out_dir") + 1])
        out_dir.mkdir(parents=True, exist_ok=True)
        tool_name = Path(cmd[1]).name
        if tool_name == "c2_risk_transformer_offline_v1.py":
            (out_dir / "equity_intent.v1.json").write_text(
                json.dumps(
                    {
                        "intent_id": "intent-1",
                        "engine": {"engine_id": "swing_mean_reversion"},
                        "underlying": {"symbol": "SPY", "currency": "USD"},
                    }
                ) + "\n",
                encoding="utf-8",
            )
            (out_dir / "lineage_envelope.v1.json").write_text(json.dumps({"ok": True}) + "\n", encoding="utf-8")
            (out_dir / "equity_order_plan.v2.json").write_text(json.dumps({"plan_id": "plan-1", "qty_shares": 1}) + "\n", encoding="utf-8")
        else:
            (out_dir / "submit_preflight_decision.v1.json").write_text(json.dumps({"decision": "ALLOW"}) + "\n", encoding="utf-8")
            (out_dir / "mapping_ledger_record.v2.json").write_text(json.dumps({"ok": True}) + "\n", encoding="utf-8")
            (out_dir / "binding_record.v2.json").write_text(json.dumps({"submission_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}) + "\n", encoding="utf-8")
            (out_dir / "equity_order_plan.v2.json").write_text(json.dumps({"plan_id": "plan-1", "qty_shares": 1}) + "\n", encoding="utf-8")

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return Result()

    with patch.object(materializer_module, "_run", side_effect=fake_run), patch.object(
        materializer_module, "validate_against_repo_schema_v1", return_value=None
    ):
        status, out_path = materializer_module._materialize_equity_intent(
            truth_root=read_truth_root,
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
            eval_time_utc=f"{day_utc}T00:00:00Z",
            attempt_id="A0001",
            prior_active_attempt_id=None,
            attempt_day_dir=attempt_day_dir,
            intent_path=intent_path,
            intent_obj={
                "schema_id": "exposure_intent",
                "target_notional_pct": "0.10",
            },
            intent_hash="a" * 64,
            intent_sha256="b" * 64,
            default_equity_reference_price="655.83",
        )

    assert status == "RELEASED"
    assert Path(out_path).exists()
    assert calls
    transformer_cmd = calls[0]
    assert "--truth_root" in transformer_cmd
    assert transformer_cmd[transformer_cmd.index("--truth_root") + 1] == str(execution_truth_root)


def test_phasec_materializer_selects_symbol_reference_price(tmp_path: Path) -> None:
    read_truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    read_truth_root.mkdir(parents=True)
    execution_truth_root.mkdir(parents=True)

    day_utc = "2026-04-16"
    attempt_day_dir = execution_truth_root / "phaseC_preflight_v1" / day_utc / "attempt_A0001"
    attempt_day_dir.mkdir(parents=True, exist_ok=True)
    intent_path = tmp_path / "dbc.exposure_intent.v1.json"
    intent_path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "target_notional_pct": "0.10",
                "underlying": {"symbol": "DBC"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    calls: list[list[str]] = []

    def fake_run(cmd, *, cwd):
        calls.append(cmd)
        out_dir = Path(cmd[cmd.index("--out_dir") + 1])
        out_dir.mkdir(parents=True, exist_ok=True)
        tool_name = Path(cmd[1]).name
        if tool_name == "c2_risk_transformer_offline_v1.py":
            (out_dir / "equity_intent.v1.json").write_text(
                json.dumps(
                    {
                        "intent_id": "intent-1",
                        "engine": {"engine_id": "cross_asset_trend"},
                        "underlying": {"symbol": "DBC", "currency": "USD"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (out_dir / "lineage_envelope.v1.json").write_text(json.dumps({"ok": True}) + "\n", encoding="utf-8")
            (out_dir / "equity_order_plan.v2.json").write_text(json.dumps({"plan_id": "plan-1", "qty_shares": 1}) + "\n", encoding="utf-8")
        else:
            (out_dir / "submit_preflight_decision.v1.json").write_text(json.dumps({"decision": "ALLOW"}) + "\n", encoding="utf-8")
            (out_dir / "mapping_ledger_record.v2.json").write_text(json.dumps({"ok": True}) + "\n", encoding="utf-8")
            (out_dir / "binding_record.v2.json").write_text(
                json.dumps({"submission_id": "a" * 64}) + "\n",
                encoding="utf-8",
            )
            (out_dir / "equity_order_plan.v2.json").write_text(json.dumps({"plan_id": "plan-1", "qty_shares": 1}) + "\n", encoding="utf-8")

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return Result()

    with patch.object(materializer_module, "_run", side_effect=fake_run), patch.object(
        materializer_module, "validate_against_repo_schema_v1", return_value=None
    ):
        status, out_path = materializer_module._materialize_equity_intent(
            truth_root=read_truth_root,
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
            eval_time_utc=f"{day_utc}T00:00:00Z",
            attempt_id="A0001",
            prior_active_attempt_id=None,
            attempt_day_dir=attempt_day_dir,
            intent_path=intent_path,
            intent_obj={
                "schema_id": "exposure_intent",
                "target_notional_pct": "0.10",
            },
            intent_hash="a" * 64,
            intent_sha256="b" * 64,
            default_equity_reference_price="23.45",
        )

    assert status == "RELEASED"
    assert Path(out_path).exists()
    transformer_cmd = calls[0]
    assert "--equity_reference_price" in transformer_cmd
    assert transformer_cmd[transformer_cmd.index("--equity_reference_price") + 1] == "23.45"
