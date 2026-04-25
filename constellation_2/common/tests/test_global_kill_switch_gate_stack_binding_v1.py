from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_global_kill_switch_v1 as kill_switch_module
from constellation_2.common.kill_switch_authority_v1 import resolve_kill_switch_authority_v1


DAY = "2026-04-14"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def test_load_inputs_uses_primary_sleeve_authorization_truth_root() -> None:
    with tempfile.TemporaryDirectory() as td:
        canonical_truth = Path(td) / "truth"
        sleeve_truth = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        verdict_path = sleeve_truth / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
        _write_json(
            verdict_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )

        with patch.object(kill_switch_module, "TRUTH", canonical_truth), patch.object(
            kill_switch_module,
            "resolve_governed_sleeve_truth_bindings",
            return_value=(SimpleNamespace(truth_root=sleeve_truth),),
        ):
            manifest, reason_codes, decisions = kill_switch_module._load_inputs(DAY)

        assert reason_codes == []
        assert decisions["authorization_verdict_status"] == "PASS"
        assert manifest[0]["path"] == str(verdict_path.resolve())

def test_load_inputs_fail_closed_when_sleeve_binding_unresolved() -> None:
    with tempfile.TemporaryDirectory() as td:
        canonical_truth = Path(td) / "truth"
        with patch.object(kill_switch_module, "TRUTH", canonical_truth), patch.object(
            kill_switch_module,
            "resolve_governed_sleeve_truth_bindings",
            side_effect=ValueError("SLEEVE_TRUTH_BINDING_NOT_FOUND"),
        ):
            manifest, reason_codes, decisions = kill_switch_module._load_inputs(DAY)

        assert kill_switch_module.RC_MISSING_INPUTS in reason_codes
        assert decisions["authorization_verdict_resolution_error"] == "SLEEVE_TRUTH_BINDING_NOT_FOUND"
        assert manifest[0]["type"] == "authorization_gate_verdict_v1_missing"


def test_load_inputs_marks_invalid_when_authorization_schema_is_wrong() -> None:
    with tempfile.TemporaryDirectory() as td:
        canonical_truth = Path(td) / "truth"
        sleeve_truth = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        verdict_path = sleeve_truth / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
        _write_json(
            verdict_path,
            {
                "schema_id": "wrong_schema",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )

        with patch.object(kill_switch_module, "TRUTH", canonical_truth), patch.object(
            kill_switch_module,
            "resolve_governed_sleeve_truth_bindings",
            return_value=(SimpleNamespace(truth_root=sleeve_truth),),
        ):
            manifest, reason_codes, decisions = kill_switch_module._load_inputs(DAY)

        assert kill_switch_module.RC_INPUT_INVALID in reason_codes
        assert "AUTHORIZATION_VERDICT_SCHEMA_ID_INVALID" in decisions["authorization_verdict_parse_error"]
        assert manifest[0]["type"] == "authorization_gate_verdict_v1"


def test_main_allows_entries_when_authorization_verdict_passes() -> None:
    with tempfile.TemporaryDirectory() as td:
        canonical_truth = Path(td) / "truth"
        sleeve_truth = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        verdict_path = sleeve_truth / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
        _write_json(
            verdict_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )

        with patch.object(kill_switch_module, "TRUTH", canonical_truth), patch.object(
            kill_switch_module,
            "OUT_ROOT",
            (canonical_truth / "risk_v1" / "kill_switch_v1").resolve(),
        ), patch.object(
            kill_switch_module,
            "resolve_governed_sleeve_truth_bindings",
            return_value=(SimpleNamespace(truth_root=sleeve_truth),),
        ), patch.object(
            kill_switch_module,
            "validate_against_repo_schema_v1",
            return_value=None,
        ), patch.object(
            kill_switch_module,
            "_git_sha",
            return_value="a" * 40,
        ), patch.object(
            sys,
            "argv",
            ["run_global_kill_switch_v1.py", "--day_utc", DAY],
        ):
            rc = kill_switch_module.main()

        payload = json.loads(
            (canonical_truth / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 0
        assert payload["state"] == "INACTIVE"
        assert payload["allow_entries"] is True
        assert payload["input_manifest"][0]["type"] == "authorization_gate_verdict_v1"


def test_main_blocks_entries_when_authorization_verdict_fails() -> None:
    with tempfile.TemporaryDirectory() as td:
        canonical_truth = Path(td) / "truth"
        sleeve_truth = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        verdict_path = sleeve_truth / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
        _write_json(
            verdict_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "FAIL",
            },
        )

        with patch.object(kill_switch_module, "TRUTH", canonical_truth), patch.object(
            kill_switch_module,
            "OUT_ROOT",
            (canonical_truth / "risk_v1" / "kill_switch_v1").resolve(),
        ), patch.object(
            kill_switch_module,
            "resolve_governed_sleeve_truth_bindings",
            return_value=(SimpleNamespace(truth_root=sleeve_truth),),
        ), patch.object(
            kill_switch_module,
            "validate_against_repo_schema_v1",
            return_value=None,
        ), patch.object(
            kill_switch_module,
            "_git_sha",
            return_value="a" * 40,
        ), patch.object(
            sys,
            "argv",
            ["run_global_kill_switch_v1.py", "--day_utc", DAY],
        ):
            rc = kill_switch_module.main()

        payload = json.loads(
            (canonical_truth / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 0
        assert payload["state"] == "ACTIVE"
        assert payload["allow_entries"] is False
        assert "C2_KILL_SWITCH_ACTIVE" in payload["reason_codes"]


def test_main_refreshes_conflicting_sleeve_kill_switch_copy_from_canonical() -> None:
    with tempfile.TemporaryDirectory() as td:
        canonical_truth = Path(td) / "truth"
        truth_sleeves_root = Path(td) / "truth_sleeves"
        sleeve_truth = truth_sleeves_root / "PRIMARY" / "PAPER"
        verdict_path = sleeve_truth / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
        stale_sleeve_kill_switch = sleeve_truth / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json"
        _write_json(
            verdict_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )
        _write_json(
            stale_sleeve_kill_switch,
            {
                "schema_id": "global_kill_switch_state",
                "schema_version": "v1",
                "day_utc": DAY,
                "produced_utc": f"{DAY}T00:00:00Z",
                "producer": {
                    "repo": "constellation_2_runtime",
                    "module": "ops/tools/run_global_kill_switch_v1.py",
                    "git_sha": "a" * 40,
                },
                "state": "ACTIVE",
                "allow_entries": False,
                "allow_exits": True,
                "forced_mode": "FLATTEN_ONLY",
                "reason_codes": ["C2_KILL_SWITCH_ACTIVE"],
                "input_manifest": [
                    {
                        "type": "authorization_gate_verdict_v1",
                        "path": "/tmp/fake/truth_sleeves/PRIMARY/PAPER/reports/authorization_gate_verdict_v1/2026-04-14/authorization_gate_verdict.v1.json",
                        "sha256": "0" * 64,
                    }
                ],
                "state_sha256": "0" * 64,
            },
        )

        with patch.object(kill_switch_module, "TRUTH", canonical_truth), patch.object(
            kill_switch_module,
            "OUT_ROOT",
            (canonical_truth / "risk_v1" / "kill_switch_v1").resolve(),
        ), patch.object(
            kill_switch_module,
            "resolve_governed_sleeve_truth_bindings",
            return_value=(SimpleNamespace(truth_root=sleeve_truth),),
        ), patch.object(
            kill_switch_module,
            "resolve_truth_sleeves_root",
            return_value=truth_sleeves_root,
        ), patch.object(
            kill_switch_module,
            "validate_against_repo_schema_v1",
            return_value=None,
        ), patch.object(
            kill_switch_module,
            "_git_sha",
            return_value="b" * 40,
        ), patch.object(
            sys,
            "argv",
            ["run_global_kill_switch_v1.py", "--day_utc", DAY],
        ):
            rc = kill_switch_module.main()

        assert rc == 0
        assert stale_sleeve_kill_switch.exists()
        quarantine_dir = stale_sleeve_kill_switch.parent / "__quarantine__"
        quarantined = list(quarantine_dir.glob("global_kill_switch_state.v1.json.INVALID_*.json"))
        assert quarantined
        canonical_payload = json.loads(
            (canonical_truth / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json").read_text(encoding="utf-8")
        )
        sleeve_payload = json.loads(stale_sleeve_kill_switch.read_text(encoding="utf-8"))
        assert sleeve_payload == canonical_payload

        result = resolve_kill_switch_authority_v1(
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=DAY,
        )
        assert result.status == "PASS"
