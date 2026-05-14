from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = REPO_ROOT / "ops/tools/run_aegis_paper_blocker_forecast_v1.py"
SPEC = importlib.util.spec_from_file_location("run_aegis_paper_blocker_forecast_v1", TOOL_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

build_aegis_paper_blocker_forecast_v1 = MODULE.build_aegis_paper_blocker_forecast_v1


DAY = "2026-05-14"
SLEEVE = "PRIMARY"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _files(root: Path) -> set[str]:
    if not root.exists():
        return set()
    return {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()}


def _forecast(truth: Path, canonical: Path) -> dict:
    return build_aegis_paper_blocker_forecast_v1(
        day_utc=DAY,
        sleeve=SLEEVE,
        environment="PAPER",
        truth_root=truth,
        canonical_truth_root=canonical,
        generated_at_utc="2026-05-14T13:30:00Z",
    )


def _row(payload: dict, stage_id: str) -> dict:
    for domain in payload["domains"]:
        for row in domain["stages"]:
            if row["stage_id"] == stage_id:
                return row
    raise AssertionError(f"missing stage row: {stage_id}")


def _write_startup_pass(truth: Path, canonical: Path) -> None:
    _write_json(truth / f"reports/broker_supply_v1/{DAY}/broker_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:30:00Z"})
    _write_json(truth / f"cash_ledger_v1/snapshots/{DAY}/cash_ledger_snapshot.v1.json", {"day_utc": DAY, "status": "OK", "cash_total_cents": 100_000, "nlv_total_cents": 100_000, "generated_at_utc": f"{DAY}T13:30:01Z"})
    _write_json(truth / f"positions_v1/snapshots/{DAY}/positions_snapshot.v5.json", {"day_utc": DAY, "status": "OK", "generated_at_utc": f"{DAY}T13:30:02Z"})
    _write_json(truth / f"accounting_v2/nav/{DAY}/nav.v2.json", {"day_utc": DAY, "status": "ACTIVE", "nav": {"nav_total_cents": 100_000}, "generated_at_utc": f"{DAY}T13:30:03Z"})
    _write_json(truth / f"accounting_compat_v1/nav/{DAY}/nav_snapshot.v1.json", {"day_utc": DAY, "status": "OK", "generated_at_utc": f"{DAY}T13:30:04Z"})
    _write_json(truth / f"reports/capital_supply_v1/{DAY}/capital_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:30:05Z"})
    _write_json(
        canonical / f"reports/paper_startup_intent_input_convergence_v1/{DAY}/paper_startup_intent_input_convergence.v1.json",
        {
            "target_day": DAY,
            "convergence_status": "SUCCESS",
            "generated_utc": f"{DAY}T13:30:06Z",
            "symbol_diagnostics": {
                "required_snapshot_symbols": ["SPY"],
                "materialized_snapshot_symbols": ["SPY"],
                "missing_snapshot_symbols": [],
                "bridge_symbol_used_for_market_snapshot": False,
            },
        },
    )


def _write_intent_pass(truth: Path) -> None:
    selected = {"intent_id": "intent-spy", "instrument": "SPY"}
    _write_json(truth / f"reports/trading_day_intent_generation_v1/{DAY}/trading_day_intent_generation.v1.json", {"day_utc": DAY, "final_status": "INTENTS_PRESENT", "generated_at_utc": f"{DAY}T13:31:00Z"})
    _write_json(truth / f"reports/portfolio_activation_gate_v1/{DAY}/portfolio_activation_gate.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:31:01Z"})
    _write_json(truth / f"reports/portfolio_scoring_v1/{DAY}/portfolio_scoring.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:31:02Z"})
    _write_json(truth / f"reports/intent_arbitration_v1/{DAY}/intent_arbitration.v1.json", {"day_utc": DAY, "status": "SELECTED", "selected_intent": selected, "generated_at_utc": f"{DAY}T13:31:03Z"})
    _write_json(truth / "pointers/selected_intent_pointer.v1.json", {"day_utc": DAY, "status": "SELECTED", "selected_intent": selected, "created_at_utc": f"{DAY}T13:31:04Z"})


def _write_requirement_graph_pass(truth: Path) -> None:
    _write_json(
        truth / f"reports/aegis_requirement_graph_v1/{DAY}/requirement_graph.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "truth_root": str(truth.resolve()),
            "generated_at_utc": f"{DAY}T13:32:00Z",
            "active_intents": [{"intent_id": "intent-spy", "instrument": "SPY"}],
            "requirements": [
                {
                    "owner_phase": "MARKET_DATA",
                    "source_type": "ACTIVE_INTENT",
                    "source_id": "intent-spy",
                    "instrument": "SPY",
                }
            ],
        },
    )


def _write_market_and_structure_pass(truth: Path) -> None:
    _write_json(truth / f"market_data_snapshot_v1/snapshots/{DAY}/SPY.market_data_snapshot.v1.json", {"day_utc": DAY, "status": "PASS", "symbol": "SPY", "quote_as_of_utc": f"{DAY}T13:32:01Z"})
    _write_json(truth / f"reports/risk_budget_supply_v1/{DAY}/risk_budget_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:32:02Z"})
    _write_json(truth / f"reports/market_open_data_gate_v1/{DAY}/market_open_data_gate.v1.json", {"day_utc": DAY, "status": "PASS", "capture_attempted_by_gate": True, "quote_completeness_status": "PASS", "generated_at_utc": f"{DAY}T13:32:03Z"})
    _write_json(truth / f"reports/market_data_supply_v1/{DAY}/market_data_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:32:04Z"})
    _write_json(truth / f"reports/structure_decision_supply_v1/{DAY}/structure_decision_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:32:05Z"})


def _write_authority_and_allocation_pass(truth: Path, canonical: Path, *, kill_switch: dict | None = None) -> None:
    verdict = truth / f"reports/authorization_gate_verdict_v1/{DAY}/authorization_gate_verdict.v1.json"
    _write_json(verdict, {"schema_id": "authorization_gate_verdict_v1", "schema_version": "v1", "day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:33:00Z"})
    _write_json(
        truth / "run_pointer_v2/canonical_authority_head.v1.json",
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "PASS",
            "authoritative": True,
            "points_to": str(verdict),
            "produced_utc": f"{DAY}T13:33:01Z",
        },
    )
    _write_json(truth / f"risk_v1/exposure_net_v1/{DAY}/exposure_net.v1.json", {"day_utc": DAY, "status": "OK", "generated_at_utc": f"{DAY}T13:33:02Z"})
    _write_json(truth / f"allocation_v1/capital_authority_allocation_v1/{DAY}/capital_authority_allocation.v1.json", {"day_utc": DAY, "status": "OK", "generated_at_utc": f"{DAY}T13:33:03Z"})
    (truth / f"phaseC_preflight_v1/{DAY}").mkdir(parents=True, exist_ok=True)
    _write_json(truth / f"phaseC_preflight_v1/{DAY}/identity.json", {"day_utc": DAY, "status": "OK"})
    _write_json(truth / f"engine_activity_v1/authorization_v1/{DAY}/auth.json", {"day_utc": DAY, "status": "AUTHORIZED"})
    _write_json(truth / f"reports/authorization_supply_v1/{DAY}/authorization_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:33:04Z"})
    kill = kill_switch or {"day_utc": DAY, "state": "INACTIVE", "allow_entries": True, "produced_utc": f"{DAY}T13:33:05Z"}
    _write_json(canonical / f"risk_v1/kill_switch_v1/{DAY}/global_kill_switch_state.v1.json", kill)


def test_no_writes_occur(tmp_path: Path) -> None:
    truth = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    canonical = tmp_path / "truth"
    before = _files(tmp_path)

    payload = _forecast(truth, canonical)

    assert _files(tmp_path) == before
    assert payload["read_only"] is True
    assert payload["runtime_mutation"] is False
    assert payload["producers_called"] is False
    assert payload["paper_ready_run"] is False
    assert payload["fake_pass_evidence_used"] is False


def test_missing_equity_snapshot_forecasts_market_data_blocker(tmp_path: Path) -> None:
    truth = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    canonical = tmp_path / "truth"
    _write_startup_pass(truth, canonical)
    _write_intent_pass(truth)
    _write_requirement_graph_pass(truth)

    payload = _forecast(truth, canonical)

    first = payload["first_canonical_blocker"]
    assert first["stage_id"] == "equity_market_data_snapshot"
    assert first["domain"] == "market data"
    assert first["status"] == "MISSING"
    assert first["blocker_code"] == "EQUITY_MARKET_DATA_SNAPSHOT_MISSING"


def test_missing_sleeve_edge_forecasts_allocation_blocker(tmp_path: Path) -> None:
    truth = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    canonical = tmp_path / "truth"
    _write_startup_pass(truth, canonical)
    _write_intent_pass(truth)
    _write_requirement_graph_pass(truth)
    _write_market_and_structure_pass(truth)
    _write_authority_and_allocation_pass(truth, canonical)

    payload = _forecast(truth, canonical)

    first = payload["first_canonical_blocker"]
    assert first["stage_id"] == "sleeve_edge_snapshot"
    assert first["domain"] == "exposure/risk/allocation"
    assert first["blocker_code"] == "SLEEVE_EDGE_SNAPSHOT_MISSING"


def test_stale_kill_switch_forecasts_submit_blocker(tmp_path: Path) -> None:
    truth = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    canonical = tmp_path / "truth"
    _write_startup_pass(truth, canonical)
    _write_intent_pass(truth)
    _write_requirement_graph_pass(truth)
    _write_market_and_structure_pass(truth)
    _write_json(truth / f"reports/sleeve_edge_snapshot_v1/{DAY}/{SLEEVE}/r1/sleeve_edge_snapshot.v1.json", {"day_utc": DAY, "status": "QUALIFIED", "generated_at_utc": f"{DAY}T13:32:06Z"})
    _write_authority_and_allocation_pass(
        truth,
        canonical,
        kill_switch={"day_utc": DAY, "state": "INACTIVE", "allow_entries": True, "produced_utc": "2026-05-13T20:00:00Z"},
    )

    payload = _forecast(truth, canonical)

    first = payload["first_canonical_blocker"]
    assert first["stage_id"] == "global_kill_switch"
    assert first["domain"] == "submit boundary"
    assert first["status"] == "STALE"
    assert first["blocker_code"] == "GLOBAL_KILL_SWITCH_STALE"
    assert payload["stale_artifact_suspects"][0]["stage_id"] == "global_kill_switch"


def test_wrong_root_artifact_is_reported_as_root_mismatch(tmp_path: Path) -> None:
    truth = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    canonical = tmp_path / "truth"
    _write_json(canonical / f"reports/broker_supply_v1/{DAY}/broker_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": f"{DAY}T13:30:00Z"})

    payload = _forecast(truth, canonical)

    first = payload["first_canonical_blocker"]
    assert first["stage_id"] == "broker_supply"
    assert first["blocker_code"] == "ROOT_PATH_MISMATCH"
    assert payload["root_path_mismatches"]
    assert payload["root_path_mismatches"][0]["observed_wrong_root_path"].endswith("broker_supply.v1.json")


def test_valid_path_progresses_to_next_domain(tmp_path: Path) -> None:
    truth = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    canonical = tmp_path / "truth"
    _write_startup_pass(truth, canonical)

    payload = _forecast(truth, canonical)

    first = payload["first_canonical_blocker"]
    assert first["stage_id"] == "trading_day_intent_generation"
    assert first["domain"] == "intent/arbitration"
    assert first["status"] == "MISSING"
    assert _row(payload, "broker_supply")["status"] == "PASS"
    assert _row(payload, "paper_startup_intent_input_convergence")["status"] == "PASS"
