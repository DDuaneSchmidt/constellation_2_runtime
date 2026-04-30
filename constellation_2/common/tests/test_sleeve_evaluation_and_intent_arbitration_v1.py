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
    _write_intent(truth, day, engine_id="ENGINE_A", symbol="SPY")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(active=[{"engine_id": "ENGINE_A", "allowed_symbols": ["IWM"]}])):
        payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=day, truth_root=truth, environment="PAPER")

    outcome = payload["outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "ALLOWED_SYMBOL_MISMATCH"


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

    payload = arbitration.build_intent_arbitration(day_utc=day, truth_root=truth, environment="PAPER")

    assert payload["status"] == "SELECTED"
    assert payload["selected_intent"]["intent_id"] == "vol"
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


def test_market_session_scan_evaluates_active_even_when_prior_intent_exists(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
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
    assert (truth / "reports/sleeve_scan_session_v1/2026-04-30/cycle_a/ENGINE_OFF/sleeve_outcome.v1.json").is_file()


def test_market_session_selected_pointer_includes_cycle_id(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="vol")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}])):
        payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    pointer = json.loads(Path(payload["selected_intent_pointer_path"]).read_text())
    assert pointer["cycle_id"] == "cycle_a"
    assert pointer["selected_intent"]["cycle_id"] == "cycle_a"
    assert pointer["selected_intent"]["arbitration_reason"] == "FIRST_BY_INTENT_ARBITRATION_PRIORITY_V1"


def test_market_session_arbitration_is_deterministic_with_multiple_candidates(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_intent(truth, day, engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", suffix="trend")
    _write_intent(truth, day, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="SPY", suffix="vol")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry_with_simulator(active=[{"engine_id": "C2_TREND_EQ_PRIMARY_V1"}, {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}])):
        payload = market_session.build_market_session_intent_engine(day_utc=day, truth_root=truth, environment="PAPER", cycle_id="cycle_a")

    assert payload["arbitration"]["selected_intent"]["engine_id"] == "C2_VOL_INCOME_DEFINED_RISK_V1"
    assert payload["arbitration"]["rejected_or_filtered_intents"][0]["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"


def test_market_session_identical_consecutive_scan_does_not_duplicate_executable_intent(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
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
