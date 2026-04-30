from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_requirement_graph_v1 as requirement_graph
import ops.tools.run_intent_arbitration_v1 as arbitration
import ops.tools.run_market_session_intent_engine_v1 as market_session
import ops.tools.run_portfolio_activation_gate_v1 as portfolio_gate
import ops.tools.run_portfolio_scoring_v1 as portfolio_scoring
import ops.tools.run_portfolio_state_v1 as portfolio_state
import ops.tools.run_sleeve_evaluation_kernel_v1 as sleeve_kernel
import ops.tools.run_structure_decision_supply_v1 as structure_supply
from ops.tools import run_aegis_bod_prepare_v1 as bod


def _script() -> Path:
    return SOURCE_ROOT / "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py"


def _registry(*, active: list[dict], inactive: list[dict] | None = None) -> dict:
    rows = []
    for row in active:
        rows.append(
            {
                "engine_id": row["engine_id"],
                "activation_status": "ACTIVE",
                "allowed_symbols": row.get("allowed_symbols", ["SPY"]),
                "engine_runner_path": str(_script().relative_to(SOURCE_ROOT)),
                "engine_runner_sha256": hashlib.sha256(_script().read_bytes()).hexdigest(),
            }
        )
    for row in inactive or []:
        rows.append(
            {
                "engine_id": row["engine_id"],
                "activation_status": "INACTIVE",
                "allowed_symbols": row.get("allowed_symbols", ["SPY"]),
                "engine_runner_path": str(_script().relative_to(SOURCE_ROOT)),
                "engine_runner_sha256": hashlib.sha256(_script().read_bytes()).hexdigest(),
            }
        )
    return {"schema_id": "engine_model_registry", "schema_version": "v1", "engines": rows}


def _registry_with_simulator(*, active: list[dict], inactive: list[dict] | None = None) -> dict:
    registry = _registry(active=active, inactive=inactive)
    registry["engines"].insert(
        0,
        {
            "engine_id": sleeve_kernel.SIMULATOR_ENGINE_ID,
            "activation_status": "ACTIVE",
            "allowed_symbols": ["SPY"],
            "engine_runner_path": str(_script().relative_to(SOURCE_ROOT)),
            "engine_runner_sha256": hashlib.sha256(_script().read_bytes()).hexdigest(),
        },
    )
    return registry


def _write_intent(root: Path, day: str, *, engine_id: str, symbol: str = "SPY", suffix: str = "intent") -> Path:
    path = root / "intents_v1" / "snapshots" / day / f"{suffix}.exposure_intent.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "intent_id": f"{engine_id.lower()}_{symbol.lower()}",
                "engine": {"engine_id": engine_id},
                "underlying": {"symbol": symbol, "currency": "USD"},
                "exposure_type": "SHORT_VOL_DEFINED",
                "risk_class": "VOL_INCOME_DEFINED",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _write_manifest(root: Path, symbols: list[str]) -> Path:
    md_root = root / "market_data_snapshot_v1"
    files = []
    for symbol in symbols:
        sym = symbol.strip().upper()
        path = md_root / sym / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"symbol":"%s","timestamp_utc":"2026-04-30T00:00:00Z","close":100}\n' % sym, encoding="utf-8")
        files.append({"symbol": sym, "file": f"{sym}/2026.jsonl", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "year": 2026})
    manifest = md_root / "dataset_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({"dataset_version": "v1", "symbols": sorted({item["symbol"] for item in files}), "files": files}, sort_keys=True), encoding="utf-8")
    return manifest


def _ctx(tmp_path: Path, day: str = "2026-04-30") -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=day,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DU123456",
    )


def test_existing_intent_file_does_not_suppress_other_active_sleeves(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY", suffix="a")
    called: list[str] = []

    def fake_run(row: dict, *, day_utc: str, intent_truth_root: Path) -> dict:
        called.append(row["engine_id"])
        return {
            "command": ["fake", row["engine_id"]],
            "return_code": 0,
            "stdout": '{"status":"NO_INTENT"}',
            "stderr": "",
            "started_at_utc": "2026-04-30T14:00:00Z",
            "completed_at_utc": "2026-04-30T14:00:01Z",
        }

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A"}, {"engine_id": "ENGINE_B"}])):
        with patch.object(sleeve_kernel, "_run_engine", side_effect=fake_run):
            payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    statuses = {row["engine_id"]: row["status"] for row in payload["outcomes"]}
    assert statuses["ENGINE_A"] == "INTENT_CREATED"
    assert statuses["ENGINE_B"] == "NO_INTENT"
    assert called == ["ENGINE_B"]


def test_every_active_sleeve_has_outcome_and_inactive_is_disabled(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A"}], inactive=[{"engine_id": "ENGINE_OFF"}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    statuses = {row["engine_id"]: row["status"] for row in payload["outcomes"]}
    assert statuses == {"ENGINE_A": "NO_INTENT", "ENGINE_OFF": "DISABLED"}
    assert (truth / "reports/sleeve_evaluation_kernel_v1/2026-04-30/ENGINE_A/sleeve_evaluation.v1.json").is_file()


def test_disallowed_symbol_existing_intent_blocks_that_sleeve(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["IWM"])
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A", "allowed_symbols": ["IWM"]}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    outcome = payload["outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "ALLOWED_SYMBOL_MISMATCH"
    assert outcome["registry_allowed_symbols"] == ["IWM"]
    assert outcome["producer_requested_symbol"] == "IWM"
    assert outcome["intent_symbol"] == "SPY"
    assert outcome["rejected_intents"][0]["symbol"] == "SPY"
    assert outcome["stale_artifact_detected"] is True
    assert outcome["artifact_source"] == "PREEXISTING_INTENT_SNAPSHOT"
    assert outcome["artifact_symbol"] == "SPY"
    assert outcome["active_symbol_universe"] == ["IWM"]


def test_producer_receives_single_registry_allowed_symbol(tmp_path: Path) -> None:
    captured: dict[str, list[str]] = {}

    class FakeProc:
        returncode = 0
        stdout = '{"status":"NO_INTENT"}'
        stderr = ""

    def fake_run(cmd: list[str], **_: object) -> FakeProc:
        captured["cmd"] = cmd
        return FakeProc()

    row = _registry(active=[{"engine_id": "ENGINE_A", "allowed_symbols": ["IWM"]}])["engines"][0]
    with patch.object(sleeve_kernel.subprocess, "run", side_effect=fake_run):
        result = sleeve_kernel._run_engine(row, day_utc="2026-04-30", intent_truth_root=tmp_path / "truth")

    assert result["return_code"] == 0
    assert captured["cmd"][-2:] == ["--symbol", "IWM"]


def test_stale_spy_intent_does_not_become_arbitration_candidate_for_iwm_sleeve(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["IWM"])
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="stale_spy")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "allowed_symbols": ["IWM"]}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "ALLOWED_SYMBOL_MISMATCH"
    assert outcome["rejected_intents"][0]["symbol"] == "SPY"
    assert outcome["stale_artifact_detected"] is True
    assert outcome["artifact_source"] == "PREEXISTING_INTENT_SNAPSHOT"
    assert outcome["artifact_symbol"] == "SPY"
    assert outcome["active_symbol_universe"] == ["IWM"]
    assert payload["arbitration"]["candidate_intents"] == []
    assert payload["arbitration"]["selected_intent"] == {}


def test_iwm_manifest_and_iwm_intent_can_be_selected_for_iwm_sleeve(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["IWM", "SPY"])
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM", suffix="iwm")
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="spy")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "allowed_symbols": ["IWM"]}])):
        payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    assert outcome["status"] == "INTENT_CREATED"
    assert outcome["output_intents"][0]["symbol"] == "IWM"
    assert [row["symbol"] for row in outcome["rejected_intents"]] == ["SPY"]
    assert outcome["stale_artifact_detected"] is True
    assert outcome["artifact_source"] == "PREEXISTING_INTENT_SNAPSHOT"
    assert outcome["artifact_symbol"] == "SPY"
    assert outcome["active_symbol_universe"] == ["IWM"]
    assert payload["arbitration"]["selected_intent"]["symbol"] == "IWM"


def test_missing_sleeve_manifest_but_canonical_has_symbol_is_aligned_from_canonical(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    canonical = tmp_path / "canonical"
    canonical_manifest = _write_manifest(canonical, ["IWM"])

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=canonical):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "allowed_symbols": ["IWM"]}])):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    assert outcome["status"] == "NO_INTENT"
    assert outcome["canonical_blocker"] == ""
    assert outcome["market_data_manifest_check"]["missing_in_local_truth_root"] == []
    local_manifest = json.loads((truth / "market_data_snapshot_v1/dataset_manifest.json").read_text())
    canonical_payload = json.loads(canonical_manifest.read_text())
    assert "IWM" in local_manifest["symbols"]
    assert local_manifest["files"][0]["sha256"] == canonical_payload["files"][0]["sha256"]
    assert payload["arbitration"]["candidate_intents"] == []


def test_missing_iwm_manifest_blocks_without_executable_candidate(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=tmp_path / "empty_canonical"):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "allowed_symbols": ["IWM"]}])):
            payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "MARKET_DATA_MANIFEST_SYMBOL_MISSING"
    assert outcome["output_intents"] == []
    assert payload["arbitration"]["candidate_intents"] == []


def test_spy_canonical_manifest_is_not_fallback_for_iwm_only_sleeve(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    canonical = tmp_path / "canonical"
    _write_manifest(canonical, ["SPY"])

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=canonical):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "allowed_symbols": ["IWM"]}])):
            payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "MARKET_DATA_MANIFEST_SYMBOL_MISSING"
    assert outcome["market_data_manifest_check"]["missing_in_both_truth_roots"] == ["IWM"]
    assert "SPY" not in [row.get("symbol") for row in outcome["output_intents"]]


def test_defensive_tail_mismatched_symbol_cannot_become_output_intent(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["TLT"])
    _write_intent(truth, day, engine_id="C2_DEFENSIVE_TAIL_V1", symbol="SPY", suffix="stale_spy_tail")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_DEFENSIVE_TAIL_V1", "allowed_symbols": ["TLT"]}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 1, "stdout": "", "stderr": "FAIL: MISSING_REQUIRED_INPUTS", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["output_intents"] == []
    assert outcome["rejected_intents"][0]["symbol"] == "SPY"
    assert payload["arbitration"]["candidate_intents"] == []


def test_large_registry_allowed_symbol_list_aligns_from_canonical(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    canonical = tmp_path / "canonical"
    symbols = [f"SYM{idx:03d}" for idx in range(200)]
    _write_manifest(canonical, symbols)

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=canonical):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A", "allowed_symbols": symbols}])):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    local_manifest = json.loads((truth / "market_data_snapshot_v1/dataset_manifest.json").read_text())
    assert outcome["status"] == "NO_INTENT"
    assert outcome["market_data_manifest_check"]["requested_symbols"] == symbols
    assert local_manifest["symbols"] == symbols
    assert len(local_manifest["files"]) == 200


def test_registry_symbol_alignment_replaces_stale_local_hash_with_canonical(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    canonical = tmp_path / "canonical"
    _write_manifest(truth, ["SPY"])
    canonical_manifest = _write_manifest(canonical, ["SPY"])
    stale_file = truth / "market_data_snapshot_v1/SPY/2026.jsonl"
    stale_file.write_text('{"symbol":"SPY","timestamp_utc":"2026-04-29T00:00:00Z","close":1}\n', encoding="utf-8")

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=canonical):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A", "allowed_symbols": ["SPY"]}])):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    outcome = payload["sleeve_outcomes"][0]
    local_manifest = json.loads((truth / "market_data_snapshot_v1/dataset_manifest.json").read_text())
    canonical_payload = json.loads(canonical_manifest.read_text())
    assert outcome["status"] == "NO_INTENT"
    assert hashlib.sha256(stale_file.read_bytes()).hexdigest() == canonical_payload["files"][0]["sha256"]
    assert local_manifest["files"][0]["sha256"] == canonical_payload["files"][0]["sha256"]


def test_multi_symbol_registry_runs_producer_once_per_allowed_symbol_when_no_symbols_arg(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    class FakeProc:
        returncode = 0
        stdout = '{"status":"NO_INTENT"}'
        stderr = ""

    def fake_run(cmd: list[str], **_: object) -> FakeProc:
        calls.append(cmd)
        return FakeProc()

    row = _registry(active=[{"engine_id": "ENGINE_A", "allowed_symbols": ["IWM", "SPY"]}])["engines"][0]
    with patch.object(sleeve_kernel, "_runner_supports_symbols_arg", return_value=False):
        with patch.object(sleeve_kernel.subprocess, "run", side_effect=fake_run):
            result = sleeve_kernel._run_engine(row, day_utc="2026-04-30", intent_truth_root=tmp_path / "truth")

    assert result["return_code"] == 0
    assert [cmd[-2:] for cmd in calls] == [["--symbol", "IWM"], ["--symbol", "SPY"]]


def test_multi_symbol_registry_uses_symbols_arg_when_supported(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    class FakeProc:
        returncode = 0
        stdout = '{"status":"NO_INTENT"}'
        stderr = ""

    def fake_run(cmd: list[str], **_: object) -> FakeProc:
        calls.append(cmd)
        return FakeProc()

    row = _registry(active=[{"engine_id": "ENGINE_A", "allowed_symbols": ["IWM", "SPY"]}])["engines"][0]
    with patch.object(sleeve_kernel, "_runner_supports_symbols_arg", return_value=True):
        with patch.object(sleeve_kernel.subprocess, "run", side_effect=fake_run):
            result = sleeve_kernel._run_engine(row, day_utc="2026-04-30", intent_truth_root=tmp_path / "truth")

    assert result["return_code"] == 0
    assert len(calls) == 1
    assert calls[0][-2:] == ["--symbols", "IWM,SPY"]


def test_no_intent_is_auditable_and_does_not_block_arbitration(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    day = "2026-04-30"
    rollup = {
        "schema_id": "sleeve_evaluation_kernel_rollup",
        "schema_version": "v1",
        "day_utc": day,
        "environment": "PAPER",
        "status": "PASS",
        "outcomes": [{"engine_id": "ENGINE_A", "sleeve_id": "ENGINE_A", "status": "NO_INTENT", "reason_codes": ["NO_INTENT_DECLARED"]}],
    }
    path = sleeve_kernel.sleeve_evaluation_rollup_path(truth_root=truth, day_utc=day)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(rollup), encoding="utf-8")

    payload = arbitration.build_intent_arbitration(day_utc=day, truth_root=truth, environment="PAPER")

    assert payload["status"] == "NO_EXECUTABLE_INTENT"
    assert payload["canonical_blocker"] == "NO_EXECUTABLE_INTENT"


def test_multiple_valid_intents_are_deterministically_arbitrated(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    day = "2026-04-30"
    first = _write_intent(truth, day, engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", suffix="trend")
    second = _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="vol")
    rollup = {
        "schema_id": "sleeve_evaluation_kernel_rollup",
        "schema_version": "v1",
        "day_utc": day,
        "environment": "PAPER",
        "status": "PASS",
        "outcomes": [
            {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "trend", "intent_path": str(first), "intent_hash": "b", "symbol": "SPY"}]},
            {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "vol", "intent_path": str(second), "intent_hash": "a", "symbol": "SPY"}]},
        ],
    }
    path = sleeve_kernel.sleeve_evaluation_rollup_path(truth_root=truth, day_utc=day)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(rollup), encoding="utf-8")

    state_path = portfolio_state.portfolio_state_path(truth_root=truth, day_utc=day)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"schema_id": "portfolio_state", "day_utc": day, "status": "PASS", "regime": "TREND", "trend_strength": "HIGH", "volatility_regime": "NORMAL", "artifact_path": str(state_path)}),
        encoding="utf-8",
    )
    gate = portfolio_gate.build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, environment="PAPER", source_rollup_path=path)
    scoring = portfolio_scoring.build_portfolio_scoring_v1(day_utc=day, truth_root=truth, environment="PAPER", source_rollup_path=path, portfolio_gate_path_arg=Path(gate["artifact_path"]))

    payload = arbitration.build_intent_arbitration(
        day_utc=day,
        truth_root=truth,
        environment="PAPER",
        portfolio_gate_path=Path(gate["artifact_path"]),
        portfolio_scoring_path_arg=Path(scoring["artifact_path"]),
    )

    assert payload["status"] == "SELECTED"
    assert payload["selected_intent"]["intent_id"] == "trend"
    assert payload["selected_intent_rank"] == 1
    assert Path(payload["selected_intent_pointer_path"]).is_file()


def test_requirement_graph_fails_closed_when_arbitration_missing(tmp_path: Path) -> None:
    payload = requirement_graph.build_requirement_graph(_ctx(tmp_path))

    assert payload["status"] == "BLOCKED"
    assert any(node.get("blocker") == "INTENT_ARBITRATION_MISSING" for node in payload["requirements"])


def test_requirement_graph_consumes_only_selected_intent(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx.execution_root, ctx.day_utc, engine_id="ENGINE_A", symbol="SPY", suffix="selected")
    _write_intent(ctx.execution_root, ctx.day_utc, engine_id="ENGINE_B", symbol="QQQ", suffix="raw")
    selected_path = ctx.execution_root / "intents_v1/snapshots" / ctx.day_utc / "selected.exposure_intent.v1.json"
    pointer = {
        "schema_id": "selected_intent_pointer",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "environment": "PAPER",
        "status": "SELECTED",
        "selected_intent": {"intent_id": "selected", "intent_path": str(selected_path), "intent_hash": "selected", "engine_id": "ENGINE_A"},
    }
    p = arbitration.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(pointer), encoding="utf-8")
    a = arbitration.intent_arbitration_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    a.parent.mkdir(parents=True)
    a.write_text(json.dumps({"status": "SELECTED"}), encoding="utf-8")

    payload = requirement_graph.build_requirement_graph(ctx)

    assert [row["intent_id"] for row in payload["active_intents"]] == ["engine_a_spy"]


def test_structure_supply_blocks_when_arbitration_missing(tmp_path: Path) -> None:
    payload = structure_supply.build_structure_decision_supply_v1(_ctx(tmp_path))

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "INTENT_ARBITRATION_MISSING"


def test_structure_supply_preserves_blocked_arbitration_pointer(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    p = arbitration.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    p.parent.mkdir(parents=True)
    p.write_text(
        json.dumps(
            {
                "schema_id": "selected_intent_pointer",
                "schema_version": "v1",
                "day_utc": ctx.day_utc,
                "environment": "PAPER",
                "status": "BLOCKED",
                "canonical_blocker": "ALLOWED_SYMBOL_MISMATCH",
                "selected_intent": {},
            }
        ),
        encoding="utf-8",
    )

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "ALLOWED_SYMBOL_MISMATCH"


def test_identical_consecutive_cycles_do_not_duplicate_intents(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A"}])):
        first = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")
        second = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    assert first["outcomes"][0]["status"] == "INTENT_CREATED"
    assert first["outcomes"][0]["signal_state"] == {"state": "ACTIVE", "duration_cycles": 1}
    assert second["outcomes"][0]["status"] == "NO_INTENT"
    assert "UNCHANGED_SIGNAL" in second["outcomes"][0]["reason_codes"]
    assert second["outcomes"][0]["output_intents"] == []
    assert second["outcomes"][0]["signal_state"] == {"state": "ACTIVE", "duration_cycles": 2}


def test_state_transitions_are_recorded(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A"}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            first = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")
        _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY")
        second = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    assert first["outcomes"][0]["previous_status"] == ""
    assert first["outcomes"][0]["current_status"] == "NO_INTENT"
    assert second["outcomes"][0]["previous_status"] == "NO_INTENT"
    assert second["outcomes"][0]["current_status"] == "INTENT_CREATED"
    assert second["outcomes"][0]["changed"] is True


def test_state_memory_uses_deterministic_intent_signature(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A"}])):
        first = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")
        second = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    assert first["outcomes"][0]["intent_signature"] == second["outcomes"][0]["intent_signature"]


def test_market_session_scan_rollup_contains_all_seven_sleeves(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    active = [{"engine_id": f"ENGINE_{idx}"} for idx in range(5)]
    inactive = [{"engine_id": f"ENGINE_OFF_{idx}"} for idx in range(2)]

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=active, inactive=inactive)):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.build_market_session_intent_engine(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    rollup = json.loads(Path(payload["sleeve_scan_rollup_path"]).read_text())
    assert rollup["summary"]["configured_sleeve_count"] == 7
    assert len(rollup["sleeve_outcomes"]) == 7
    assert sleeve_kernel.SIMULATOR_ENGINE_ID not in {row["engine_id"] for row in rollup["sleeve_outcomes"]}


def test_market_session_once_writes_cycle_manifest_and_operator_status(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    active = [{"engine_id": "ENGINE_A", "allowed_symbols": ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]}]
    _write_manifest(truth, ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=active)):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    manifest = json.loads(Path(payload["cycle_manifest_path"]).read_text())
    status = json.loads(Path(payload["operator_status_path"]).read_text())
    assert manifest["sleeves_expected"] == 1
    assert len(manifest["registry_hash"]) == 64
    assert manifest["allowed_symbols_by_engine"]["ENGINE_A"] == ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    assert manifest["symbol_policy_source"] == "ENGINE_MODEL_REGISTRY_V1.allowed_symbols"
    assert manifest["deprecated_symbol_universe_used"] is False
    assert manifest["submit_enabled"] is False
    assert status["submit_enabled"] is False
    assert status["execution_path_touched"] is False
    assert status["next_scan_at_utc"] is None


def test_market_session_scan_ledger_appends_and_preserves_cycle_directories(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A"}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            first = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")
            second = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    assert first["cycle_id"] == "cycle_a"
    assert second["cycle_id"] == "cycle_a_001"
    assert Path(first["artifact_root"]).is_dir()
    assert Path(second["artifact_root"]).is_dir()
    rows = [json.loads(line) for line in Path(second["ledger_path"]).read_text().splitlines()]
    assert [row["cycle_id"] for row in rows] == ["cycle_a", "cycle_a_001"]
    assert all(row["submit_enabled"] is False for row in rows)


def test_latest_scan_pointer_updates_to_newest_cycle(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A"}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")
            second = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_b")

    pointer = json.loads(Path(second["latest_scan_cycle_pointer_path"]).read_text())
    assert pointer["cycle_id"] == "cycle_b"
    assert pointer["artifact_root"] == second["artifact_root"]


def test_selected_intent_pointer_lives_under_pointers_and_has_cycle_provenance(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="vol")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}])):
        payload = market_session.run_scan_cycle_v1(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    pointer_path = Path(payload["selected_intent_pointer_path"])
    pointer = json.loads(pointer_path.read_text())
    assert pointer_path == truth / "pointers" / "selected_intent_pointer.v1.json"
    assert pointer["cycle_id"] == "cycle_a"
    assert pointer["selected_intent"]["cycle_id"] == "cycle_a"
    assert pointer["source_arbitration_path"] == payload["arbitration_result_path"]


def test_loop_wrapper_calls_scan_cycle_repeatedly_without_changing_scan_logic(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    calls: list[str] = []

    def fake_cycle(*, day_utc: str, truth_root: Path, environment: str = "PAPER", cycle_id: str = "") -> dict:
        calls.append(day_utc)
        idx = len(calls)
        return {
            "status": "BLOCKED",
            "canonical_blocker": "NO_EXECUTABLE_INTENT",
            "cycle_id": f"cycle_{idx}",
            "artifact_root": str(truth / f"cycle_{idx}"),
            "ledger_path": str(truth / "reports/scan_ledger_v1/2026-04-30.scan_ledger.v1.jsonl"),
            "operator_status_path": str(truth / f"cycle_{idx}/operator_status.v1.json"),
            "sleeve_scan_rollup_path": str(truth / f"cycle_{idx}/scan_rollup.v1.json"),
            "intent_arbitration_path": str(truth / f"cycle_{idx}/arbitration_result.v1.json"),
            "latest_scan_cycle_pointer_path": str(truth / "pointers/latest_scan_cycle_pointer.v1.json"),
            "selected_intent_pointer_path": str(truth / "pointers/selected_intent_pointer.v1.json"),
            "operator_status": {
                "cycle_id": f"cycle_{idx}",
                "submit_enabled": False,
                "execution_path_touched": False,
            },
        }

    with patch.object(market_session, "run_scan_cycle_v1", side_effect=fake_cycle):
        payload = market_session.run_loop_v1(
            day_utc="2026-04-30",
            truth_root=truth,
            environment="PAPER",
            cadence_seconds=0,
            max_cycles=3,
            stop_at_market_close=False,
        )

    assert calls == ["2026-04-30", "2026-04-30", "2026-04-30"]
    assert payload["cycles_run"] == 3
    assert payload["cycle_ids"] == ["cycle_1", "cycle_2", "cycle_3"]
    assert payload["status"] == "BLOCKED"
    first_status = json.loads((truth / "cycle_1/operator_status.v1.json").read_text())
    final_status = json.loads((truth / "cycle_3/operator_status.v1.json").read_text())
    assert first_status["next_scan_at_utc"] is not None
    assert final_status["next_scan_at_utc"] is None


def test_market_session_cli_requires_exactly_one_mode() -> None:
    with patch.object(market_session, "resolve_fact_plane_truth_root_v1", return_value=Path("/tmp/unused")):
        try:
            market_session.main(["--day_utc", "2026-04-30"])
        except SystemExit as exc:
            assert "Specify exactly one" in str(exc)
        else:
            raise AssertionError("missing mode should fail closed")

        try:
            market_session.main(["--day_utc", "2026-04-30", "--once", "--loop"])
        except SystemExit as exc:
            assert "Specify exactly one" in str(exc)
        else:
            raise AssertionError("conflicting modes should fail closed")


def test_market_session_cli_rejects_loop_options_with_once() -> None:
    with patch.object(market_session, "resolve_fact_plane_truth_root_v1", return_value=Path("/tmp/unused")):
        try:
            market_session.main(["--day_utc", "2026-04-30", "--once", "--cadence_seconds", "0"])
        except SystemExit as exc:
            assert "apply only with --loop" in str(exc)
        else:
            raise AssertionError("loop cadence should not apply to --once")


def test_loop_stop_at_market_close_exits_without_scan(tmp_path: Path) -> None:
    with patch.object(market_session, "_is_after_market_close_now", return_value=True):
        with patch.object(market_session, "run_scan_cycle_v1") as run_scan:
            payload = market_session.run_loop_v1(
                day_utc="2026-04-30",
                truth_root=tmp_path / "truth",
                environment="PAPER",
                cadence_seconds=0,
                max_cycles=2,
                stop_at_market_close=True,
            )

    run_scan.assert_not_called()
    assert payload["status"] == "STOPPED_MARKET_CLOSED"
    assert payload["cycles_run"] == 0


def test_loop_real_cycles_write_required_artifacts_and_latest_pointers(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A"}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.run_loop_v1(
                day_utc="2026-04-30",
                truth_root=truth,
                environment="PAPER",
                cadence_seconds=0,
                max_cycles=2,
                stop_at_market_close=False,
            )

    assert payload["cycles_run"] == 2
    first_id, second_id = payload["cycle_ids"]
    assert first_id != second_id
    day_root = truth / "reports/sleeve_scan_session_v1/2026-04-30"
    for cycle_id in payload["cycle_ids"]:
        root = day_root / cycle_id
        assert (root / "cycle_manifest.v1.json").is_file()
        assert (root / "scan_rollup.v1.json").is_file()
        assert (root / "arbitration_result.v1.json").is_file()
        assert (root / "operator_status.v1.json").is_file()
        assert (root / "preflight_readiness_matrix.v1.json").is_file()
        assert (root / "operator_readiness_summary.v1.json").is_file()
    ledger = truth / "reports/scan_ledger_v1/2026-04-30.scan_ledger.v1.jsonl"
    rows = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert [row["cycle_id"] for row in rows] == [first_id, second_id]
    latest = json.loads((truth / "pointers/latest_scan_cycle_pointer.v1.json").read_text())
    selected = json.loads((truth / "pointers/selected_intent_pointer.v1.json").read_text())
    first_status = json.loads((day_root / first_id / "operator_status.v1.json").read_text())
    second_status = json.loads((day_root / second_id / "operator_status.v1.json").read_text())
    assert latest["cycle_id"] == second_id
    assert selected["cycle_id"] == second_id
    assert first_status["next_scan_at_utc"] is not None
    assert second_status["next_scan_at_utc"] is None
    assert first_status["submit_enabled"] is False
    assert second_status["submit_enabled"] is False
    assert first_status["execution_path_touched"] is False
    assert second_status["execution_path_touched"] is False
    assert first_status["operator_readiness_summary_path"].endswith("operator_readiness_summary.v1.json")
    assert second_status["preflight_readiness_matrix_path"].endswith("preflight_readiness_matrix.v1.json")


def test_readiness_matrix_contains_every_scan_registry_sleeve(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A"}], inactive=[{"engine_id": "ENGINE_OFF"}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    matrix = json.loads(Path(payload["preflight_readiness_matrix_path"]).read_text())
    assert [row["engine_id"] for row in matrix["rows"]] == ["ENGINE_A", "ENGINE_OFF"]
    assert all(row["diagnostic_only"] is True for row in matrix["rows"])
    assert all(row["affects_arbitration"] is False for row in matrix["rows"])
    assert all(row["affects_execution"] is False for row in matrix["rows"])


def test_readiness_disabled_sleeve_is_disabled_not_blocked(tmp_path: Path) -> None:
    truth = tmp_path / "truth"

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[], inactive=[{"engine_id": "C2_CROSS_ASSET_TREND_V1"}])):
        payload = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    matrix = json.loads(Path(payload["preflight_readiness_matrix_path"]).read_text())
    row = matrix["rows"][0]
    assert row["engine_id"] == "C2_CROSS_ASSET_TREND_V1"
    assert row["readiness_status"] == "DISABLED"
    assert row["readiness_reason"] == "ENGINE_INACTIVE"


def test_readiness_missing_required_input_is_blocked(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["TLT"])

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=truth):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_DEFENSIVE_TAIL_V1", "allowed_symbols": ["TLT"]}])):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 1, "stdout": "", "stderr": "FAIL: MISSING_REQUIRED_INPUTS", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = market_session.run_scan_cycle_v1(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    matrix = json.loads(Path(payload["preflight_readiness_matrix_path"]).read_text())
    row = matrix["rows"][0]
    assert row["readiness_status"] == "BLOCKED"
    assert row["readiness_reason"] == "REQUIRED_INPUT_MISSING"
    assert row["missing_inputs"][0]["name"] == "market_data_daily_snapshot"
    assert row["missing_inputs"][0]["path"].endswith("market_data_snapshot_v1/snapshots/2026-04-30/TLT.market_data_snapshot.v1.json")


def test_readiness_unknown_contract_is_unknown_not_ready(tmp_path: Path) -> None:
    truth = tmp_path / "truth"

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_WITHOUT_CONTRACT", "allowed_symbols": []}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.run_scan_cycle_v1(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    matrix = json.loads(Path(payload["preflight_readiness_matrix_path"]).read_text())
    assert matrix["rows"][0]["readiness_status"] == "UNKNOWN"
    assert matrix["rows"][0]["readiness_reason"] == "SLEEVE_CONTRACT_MISSING"


def test_operator_readiness_summary_groups_blockers(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["TLT"])

    with patch.object(sleeve_kernel, "_canonical_truth_root", return_value=truth):
        with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_DEFENSIVE_TAIL_V1", "allowed_symbols": ["TLT"]}])):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 1, "stdout": "", "stderr": "FAIL: MISSING_REQUIRED_INPUTS", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = market_session.run_scan_cycle_v1(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    summary = json.loads(Path(payload["operator_readiness_summary_path"]).read_text())
    assert summary["diagnostic_only"] is True
    assert summary["blocked_count"] == 1
    blockers = {row["blocker"]: row for row in summary["top_blockers"]}
    assert blockers["market_data_daily_snapshot"]["sleeves"] == ["C2_DEFENSIVE_TAIL_V1"]


def test_readiness_does_not_change_arbitration_selected_intent(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", suffix="trend")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_TREND_EQ_PRIMARY_V1"}])):
        payload = market_session.run_scan_cycle_v1(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    pointer = json.loads(Path(payload["selected_intent_pointer_path"]).read_text())
    matrix = json.loads(Path(payload["preflight_readiness_matrix_path"]).read_text())
    assert payload["arbitration"]["selected_intent"]["intent_id"] == "c2_trend_eq_primary_v1_spy"
    assert pointer["selected_intent"]["intent_id"] == "c2_trend_eq_primary_v1_spy"
    assert all(row["affects_arbitration"] is False for row in matrix["rows"])


def test_readiness_supports_large_allowed_symbol_lists_without_deprecated_universe(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    symbols = [f"SYM{idx:03d}" for idx in range(200)]
    _write_manifest(truth, symbols)

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_TREND_EQ_PRIMARY_V1", "allowed_symbols": symbols}])):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            payload = market_session.run_scan_cycle_v1(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    matrix = json.loads(Path(payload["preflight_readiness_matrix_path"]).read_text())
    manifest = json.loads(Path(payload["cycle_manifest_path"]).read_text())
    row = matrix["rows"][0]
    assert row["allowed_symbols"] == symbols
    assert row["readiness_status"] == "READY"
    assert manifest["deprecated_symbol_universe_used"] is False


def test_readiness_artifacts_do_not_mutate_producer_truth_inputs(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "sleeve"
    (truth / "reports").mkdir(parents=True)
    before = sorted(str(path.relative_to(sleeve_truth)) for path in sleeve_truth.rglob("*"))
    matrix = market_session.build_preflight_readiness_matrix_v1(
        day_utc="2026-04-30",
        environment="PAPER",
        truth_root=truth,
        sleeve_truth_root=sleeve_truth,
        cycle_id="cycle_a",
        rows=[{"engine_id": "ENGINE_WITHOUT_CONTRACT", "activation_status": "ACTIVE", "allowed_symbols": []}],
        outcomes=[],
    )
    market_session.build_operator_readiness_summary_v1(
        day_utc="2026-04-30",
        environment="PAPER",
        truth_root=truth,
        cycle_id="cycle_a",
        matrix=matrix,
    )
    after = sorted(str(path.relative_to(sleeve_truth)) for path in sleeve_truth.rglob("*"))
    assert before == after


def test_market_session_scan_does_not_import_submit_broker_ib_or_order_tools(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    before = set(sys.modules)

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[], inactive=[{"engine_id": "ENGINE_OFF"}])):
        market_session.build_market_session_intent_engine(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    imported = set(sys.modules) - before
    forbidden_fragments = (
        "run_aegis_paper_submit",
        "run_ib_",
        "ibapi",
        "broker_preview",
        "broker_submit",
        "execution_observer",
        "order_submit",
    )
    assert not any(any(fragment in name for fragment in forbidden_fragments) for name in imported)


def test_market_session_scan_evaluates_active_even_when_prior_intent_exists(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY", suffix="a")
    called: list[str] = []

    def fake_run(row: dict, *, day_utc: str, intent_truth_root: Path) -> dict:
        called.append(row["engine_id"])
        return {
            "command": ["fake", row["engine_id"]],
            "return_code": 0,
            "stdout": '{"status":"NO_INTENT"}',
            "stderr": "",
            "started_at_utc": "",
            "completed_at_utc": "",
        }

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A"}, {"engine_id": "ENGINE_B"}])):
        with patch.object(sleeve_kernel, "_run_engine", side_effect=fake_run):
            payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    statuses = {row["engine_id"]: row["status"] for row in payload["sleeve_outcomes"]}
    assert statuses["ENGINE_A"] == "INTENT_CREATED"
    assert statuses["ENGINE_B"] == "NO_INTENT"
    assert called == ["ENGINE_B"]


def test_market_session_scan_records_inactive_sleeves_disabled(tmp_path: Path) -> None:
    truth = tmp_path / "truth"

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[], inactive=[{"engine_id": "ENGINE_OFF"}])):
        payload = market_session.build_market_session_intent_engine(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    assert payload["sleeve_outcomes"][0]["status"] == "DISABLED"
    assert (truth / "reports/sleeve_scan_session_v1/2026-04-30/cycle_a/sleeves/ENGINE_OFF/sleeve_outcome.v1.json").is_file()


def test_market_session_selected_pointer_includes_cycle_id(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="vol")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}])):
        payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    pointer = json.loads(Path(payload["selected_intent_pointer_path"]).read_text())
    assert pointer["cycle_id"] == "cycle_a"
    assert pointer["selected_intent"]["cycle_id"] == "cycle_a"
    assert pointer["selected_intent"]["arbitration_reason"] == "HIGHEST_PORTFOLIO_SCORE_V1"
    assert pointer["portfolio_scoring_path"]


def test_market_session_arbitration_is_deterministic_with_multiple_candidates(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", suffix="trend")
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="vol")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_TREND_EQ_PRIMARY_V1"}, {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}])):
        payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    assert payload["arbitration"]["selected_intent"]["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert payload["arbitration"]["selected_intent"]["portfolio_score_rank"] == 1
    assert payload["arbitration"]["rejected_or_filtered_intents"][0]["engine_id"] == "C2_VOL_INCOME_DEFINED_RISK_V1"
    assert payload["arbitration"]["rejected_or_filtered_intents"][0]["rejection_reason"] == "NOT_SELECTED_BY_PORTFOLIO_SCORE_V1"


def test_market_session_identical_consecutive_scan_does_not_duplicate_executable_intent(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "ENGINE_A"}])):
        first = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")
        second = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_b")

    assert first["sleeve_outcomes"][0]["status"] == "INTENT_CREATED"
    assert second["sleeve_outcomes"][0]["status"] == "NO_INTENT"
    assert "UNCHANGED_SIGNAL" in second["sleeve_outcomes"][0]["reason_codes"]
    assert second["arbitration"]["status"] == "NO_EXECUTABLE_INTENT"


def test_market_session_scan_never_touches_broker_or_submit_evidence(tmp_path: Path) -> None:
    truth = tmp_path / "truth"

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[], inactive=[{"engine_id": "ENGINE_OFF"}])):
        market_session.build_market_session_intent_engine(day_utc="2026-04-30", truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    written = {str(path.relative_to(truth)) for path in truth.rglob("*") if path.is_file()}
    assert not any("broker_submission" in path for path in written)
    assert not any("execution_evidence" in path for path in written)
    assert not any("submit_boundary" in path for path in written)
