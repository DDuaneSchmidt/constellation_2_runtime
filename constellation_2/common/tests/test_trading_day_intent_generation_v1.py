from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_intents_day_completeness_v1 as completeness_module
import ops.tools.run_no_intents_day_marker_v1 as no_intents_module
import ops.tools.run_trading_day_intent_generation_v1 as generation_module
import ops.tools.run_trading_day_state_machine_v1 as state_machine_module


def _sample_intent_bytes() -> bytes:
    sample_path = (
        SOURCE_ROOT
        / "constellation_2"
        / "phaseH"
        / "acceptance"
        / "samples"
        / "sample_exposure_intent_long_equity.v1.json"
    )
    return sample_path.read_bytes()


def _registry_payload() -> dict:
    return {
        "schema_id": "engine_model_registry",
        "schema_version": "v1",
        "engines": [],
    }


def _producer_spec(engine_id: str, script_relpath: str) -> generation_module.ProducerSpec:
    script_path = (SOURCE_ROOT / script_relpath).resolve()
    return generation_module.ProducerSpec(
        engine_id=engine_id,
        script_path=script_path,
        registry_runner_sha256=hashlib.sha256(script_path.read_bytes()).hexdigest(),
    )


def test_active_alpha_writers_no_longer_runtime_hardcode_runtime_copy() -> None:
    active_runner_paths = [
        SOURCE_ROOT / "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
        SOURCE_ROOT / "constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py",
        SOURCE_ROOT / "constellation_2/phaseI/vol_income_defined_risk/run/run_vol_income_defined_risk_intents_day_v1.py",
        SOURCE_ROOT / "constellation_2/phaseI/event_dislocation/run/run_event_dislocation_intents_day_v1.py",
        SOURCE_ROOT / "constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py",
    ]
    for path in active_runner_paths:
        text = path.read_text(encoding="utf-8")
        assert "/home/node/constellation_2_runtime" not in text, path
        assert "--truth_root" in text, path
        assert "resolve_fact_plane_truth_root_v1" in text, path


def test_source_authoritative_no_intents_marker_is_accepted_by_completeness(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    rc = no_intents_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    assert rc == 0

    completeness_rc = completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    assert completeness_rc == 2

    payload = json.loads(
        (
            truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["completeness_status"] == "NO_INTENTS_DECLARED"
    assert "INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR" not in payload["blocking_codes"]


def test_generator_runs_required_producers_in_fixed_order_and_writes_valid_zero(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    order: list[str] = []
    specs = [
        _producer_spec(
            "C2_MEAN_REVERSION_EQ_V1",
            "constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py",
        ),
        _producer_spec(
            "C2_TREND_EQ_PRIMARY_V1",
            "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
        ),
    ]

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        order.append(tool_name)
        if tool_name in {
            "run_mean_reversion_intents_day_v1.py",
            "run_trend_eq_primary_intents_day_v1.py",
        }:
            return {"return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": ""}
        if tool_name == "run_no_intents_day_marker_v1.py":
            rc = no_intents_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            return {"return_code": rc, "stdout": "", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(generation_module, "_load_registry", return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64)):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [{"engine_id": "C2_INTENT_SIMULATOR_V1", "reason_code": "ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED"}])):
            with patch.object(generation_module, "_run", side_effect=fake_run):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    assert order == [
        "run_mean_reversion_intents_day_v1.py",
        "run_trend_eq_primary_intents_day_v1.py",
        "run_no_intents_day_marker_v1.py",
    ]
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_status"] == "VALID_ZERO"
    assert payload["producer_results"][0]["status"] == "NO_INTENT"
    assert payload["producer_results"][1]["status"] == "NO_INTENT"
    assert payload["producer_results"][2]["status"] == "VALID_ZERO_WRITTEN"
    assert payload["skipped_active_engines"] == [
        {"engine_id": "C2_INTENT_SIMULATOR_V1", "reason_code": "ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED"}
    ]


def test_generator_fails_closed_when_required_producer_returns_zero_without_output(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    specs = [
        _producer_spec(
            "C2_TREND_EQ_PRIMARY_V1",
            "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
        ),
    ]

    with patch.object(generation_module, "_load_registry", return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64)):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [])):
            with patch.object(generation_module, "_run", return_value={"return_code": 0, "stdout": "{}", "stderr": ""}):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_status"] == "BLOCKED_BY_DEFECT"
    assert payload["first_blocker_code"] == "PRODUCER_ZERO_RC_WITHOUT_CANONICAL_OUTPUT"


def test_generator_fail_closed_on_contradictory_snapshot_and_marker(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    day_dir = truth_root / "intents_v1" / "snapshots" / day_utc
    day_dir.mkdir(parents=True, exist_ok=True)
    payload = _sample_intent_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    (day_dir / f"{digest}.exposure_intent.v1.json").write_bytes(payload)
    marker_path = day_dir / "no_intents_day.v1.json"
    marker_path.write_text(
        json.dumps(
            {
                "schema_id": "C2_NO_INTENTS_DAY_V1",
                "schema_version": 1,
                "produced_utc": f"{day_utc}T00:00:00Z",
                "day_utc": day_utc,
                "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
                "status": "OK",
                "reason_codes": ["NO_INTENTS_FOR_DAY"],
                "input_manifest": [],
                "intents_dir": str(day_dir),
                "intents_json_count": 0,
                "intents_listing_sha256": hashlib.sha256(b"").hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    with patch.object(generation_module, "_load_registry", return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64)):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=([], [])):
            rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_status"] == "BLOCKED_BY_DEFECT"
    assert payload["first_blocker_code"] == "TRADING_DAY_INTENT_GENERATION_CONTRADICTORY_EXISTING_INTENTS_AND_MARKER"


def test_generator_writes_canonical_intents_and_completeness_no_longer_missing_day_dir(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    specs = [
        _producer_spec(
            "C2_TREND_EQ_PRIMARY_V1",
            "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
        ),
    ]

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trend_eq_primary_intents_day_v1.py":
            payload = _sample_intent_bytes()
            digest = hashlib.sha256(payload).hexdigest()
            out_path = truth_root / "intents_v1" / "snapshots" / day_utc / f"{digest}.exposure_intent.v1.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(payload)
            return {"return_code": 0, "stdout": "OK: TREND_INTENT_WRITTEN", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(generation_module, "_load_registry", return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64)):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [])):
            with patch.object(generation_module, "_run", side_effect=fake_run):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    completeness_rc = completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    assert completeness_rc == 0
    completeness_payload = json.loads(
        (
            truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert completeness_payload["completeness_status"] == "COMPLETE"
    assert "INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR" not in completeness_payload["blocking_codes"]


def test_state_machine_generator_defect_becomes_first_true_blocker_not_missing_day_dir(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    generation_report_path = (
        truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json"
    )
    generation_report_path.parent.mkdir(parents=True, exist_ok=True)
    generation_report_path.write_text(
        json.dumps(
            {
                "schema_id": "trading_day_intent_generation",
                "schema_version": "v1",
                "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
                "day_utc": day_utc,
                "generation_run_id": "g",
                "produced_at_utc": f"{day_utc}T00:00:00Z",
                "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
                "truth_root": str(truth_root),
                "active_engine_registry_path": "/tmp/registry.json",
                "active_engine_registry_sha256": "b" * 64,
                "producer_topology": [],
                "skipped_active_engines": [],
                "producer_results": [],
                "final_status": "BLOCKED_BY_DEFECT",
                "first_blocker_code": "ENGINE_RUNNER_SHA256_MISMATCH",
                "first_blocker_artifact_path": "/tmp/writer.py",
                "canonical_outputs": {
                    "intents_dir": str(truth_root / "intents_v1" / "snapshots" / day_utc),
                    "intent_output_paths": [],
                    "no_intents_marker_path": "",
                    "output_count": 0,
                },
                "blocking_codes": ["ENGINE_RUNNER_SHA256_MISMATCH"],
                "human_readable_summary": "blocked"
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    with patch.object(
        state_machine_module,
        "_run",
        return_value={"return_code": 3, "stdout": "{}", "stderr": "generation failed"},
    ):
        with patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root.resolve()):
            rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["first_true_blocker"]["first_true_blocker_code"] == "ENGINE_RUNNER_SHA256_MISMATCH"
    assert payload["first_true_blocker"]["first_true_blocker_code"] != "INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR"


def test_authoritative_active_engine_registry_code_lock_matches_runner_bytes() -> None:
    registry_path = SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    active_rows = [row for row in payload["engines"] if row.get("activation_status") == "ACTIVE"]
    assert active_rows

    for row in active_rows:
        runner_path = (SOURCE_ROOT / row["engine_runner_path"]).resolve()
        assert runner_path.is_file(), row["engine_id"]
        actual_sha = hashlib.sha256(runner_path.read_bytes()).hexdigest()
        assert row["engine_runner_sha256"] == actual_sha, row["engine_id"]


def test_generator_still_fails_closed_on_true_runner_sha_mismatch(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    script_relpath = "constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py"
    script_path = (SOURCE_ROOT / script_relpath).resolve()
    specs = [
        generation_module.ProducerSpec(
            engine_id="C2_DEFENSIVE_TAIL_V1",
            script_path=script_path,
            registry_runner_sha256="0" * 64,
        ),
    ]

    with patch.object(
        generation_module,
        "_load_registry",
        return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64),
    ):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [])):
            with patch.object(generation_module, "_run", side_effect=AssertionError("producer should not run")):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_status"] == "BLOCKED_BY_DEFECT"
    assert payload["first_blocker_code"] == "ENGINE_RUNNER_SHA256_MISMATCH"
    assert payload["first_blocker_artifact_path"] == str(script_path)
    assert payload["producer_results"][0]["script_sha256"] == hashlib.sha256(script_path.read_bytes()).hexdigest()
    assert payload["producer_results"][0]["registry_runner_sha256"] == "0" * 64


def test_generator_nonzero_rc_uses_structured_failure_code_when_present(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    specs = [
        _producer_spec(
            "C2_DEFENSIVE_TAIL_V1",
            "constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py",
        ),
    ]

    with patch.object(
        generation_module,
        "_load_registry",
        return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64),
    ):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [])):
            with patch.object(
                generation_module,
                "_run",
                return_value={
                    "return_code": 1,
                    "stdout": "",
                    "stderr": "FAIL: MISSING_REQUIRED_INPUTS: /tmp/missing.json",
                },
            ):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["first_blocker_code"] == "MISSING_REQUIRED_INPUTS"
    assert payload["blocking_codes"] == ["MISSING_REQUIRED_INPUTS"]
    assert payload["producer_results"][0]["reason_codes"] == ["MISSING_REQUIRED_INPUTS"]


def test_generator_nonzero_rc_maps_missing_market_data_manifest_phrase(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    specs = [
        _producer_spec(
            "C2_EVENT_DISLOCATION_V1",
            "constellation_2/phaseI/event_dislocation/run/run_event_dislocation_intents_day_v1.py",
        ),
    ]

    with patch.object(
        generation_module,
        "_load_registry",
        return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64),
    ):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [])):
            with patch.object(
                generation_module,
                "_run",
                return_value={
                    "return_code": 1,
                    "stdout": "",
                    "stderr": "FAIL: Missing market data manifest: /tmp/dataset_manifest.json",
                },
            ):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["first_blocker_code"] == "MARKET_DATA_MANIFEST_MISSING"
    assert payload["blocking_codes"] == ["MARKET_DATA_MANIFEST_MISSING"]
    assert payload["producer_results"][0]["reason_codes"] == ["MARKET_DATA_MANIFEST_MISSING"]


def test_generator_nonzero_rc_still_falls_back_when_failure_is_unstructured(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    specs = [
        _producer_spec(
            "C2_DEFENSIVE_TAIL_V1",
            "constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py",
        ),
    ]

    with patch.object(
        generation_module,
        "_load_registry",
        return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64),
    ):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=(specs, [])):
            with patch.object(
                generation_module,
                "_run",
                return_value={
                    "return_code": 1,
                    "stdout": "",
                    "stderr": "plain runtime failure",
                },
            ):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_intent_generation_v1"
            / day_utc
            / "trading_day_intent_generation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["first_blocker_code"] == "PRODUCER_NONZERO_RC"
    assert payload["blocking_codes"] == ["PRODUCER_NONZERO_RC"]
    assert payload["producer_results"][0]["reason_codes"] == ["PRODUCER_NONZERO_RC"]


def test_generator_uses_resolved_paper_intent_truth_root_when_preexisting_intent_exists(tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    day_utc = "2026-04-24"
    payload = _sample_intent_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    sleeve_day = sleeve_truth_root / "intents_v1" / "snapshots" / day_utc
    sleeve_day.mkdir(parents=True, exist_ok=True)
    intent_path = sleeve_day / f"{digest}.exposure_intent.v1.json"
    intent_path.write_bytes(payload)

    with patch.object(
        generation_module,
        "_load_registry",
        return_value=(_registry_payload(), SOURCE_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", "a" * 64),
    ):
        with patch.object(generation_module, "_load_required_producer_specs", return_value=([], [])):
            with patch.object(
                generation_module,
                "resolve_paper_intent_truth_root_v1",
                return_value=sleeve_truth_root.resolve(),
            ):
                rc = generation_module.main(["--day_utc", day_utc, "--truth_root", str(canonical_truth_root)])

    assert rc == 0
    report_path = (
        canonical_truth_root
        / "reports"
        / "trading_day_intent_generation_v1"
        / day_utc
        / "trading_day_intent_generation.v1.json"
    )
    payload_obj = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload_obj["final_status"] == "INTENTS_PRESENT"
    assert payload_obj["canonical_outputs"]["intents_dir"] == str(sleeve_day.resolve())
    assert payload_obj["canonical_outputs"]["intent_output_paths"] == [str(intent_path.resolve())]
