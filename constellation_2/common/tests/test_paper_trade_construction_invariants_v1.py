from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_operator_state_snapshot_v1
from ops.aegis.operator_state.manual_capture_record_v1 import append_manual_capture_record_v1
from ops.aegis.runtime_evaluation_v1 import finalize_runtime_evaluation_v1, stable_json_bytes_v1
from ops.aegis.runtime_truth_kernel_v1 import runtime_evaluation_path_v1
from ops.aegis.submit_boundary_precheck_v1 import build_and_write_submit_boundary_precheck_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_paper_trade_construction_v1
from ops.aegis.trade_ticket_lineage_v1 import read_trade_ticket_lineage_v1, ticket_id_v1, write_ticket_evidence_set_v1


DAY = "2026-05-20"
INTENT_ID = "c2_trend_eq_amt_2026-05-20_v1"
INTENT_HASH = "a" * 64


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")



def _seed_candidate_packet_and_queue(root: Path, *, symbol: str = "QQQ") -> None:
    candidate = {
        "candidate_id": "paper-candidate-1",
        "symbol": symbol,
        "direction": "LONG",
        "paper_trade_eligible": True,
        "live_trade_eligible": False,
        "entry_reference_price": "500.00",
        "entry_reference_price_source": "market_data_inputs_v1",
    }
    _write(root / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json", {
        "schema_id": "candidate_review_packet",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T14:00:00Z",
        "candidate_count": 1,
        "review_candidates": [candidate],
        "safety": {"paper_only": True, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
    })
    _write(root / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json", {
        "schema_id": "paper_review_queue",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T14:00:00Z",
        "rows": [{**candidate, "status": "AWAITING_REVIEW"}],
        "safety": {"paper_only": True, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
    })


def _seed_market_data_inputs(root: Path, *, symbol: str = "QQQ", value: str = "500.00", day: str = DAY) -> None:
    _write(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {
        "schema_id": "market_data_inputs",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T14:00:00Z",
        "validation_status": "VALID",
        "input_records": [{"symbol": symbol, "value": value, "day_utc": day, "validation_status": "VALID", "source_path": "/tmp/market-data.json"}],
    })


def test_candidate_paper_construction_uses_canonical_market_data_inputs(tmp_path: Path) -> None:
    _seed_candidate_packet_and_queue(tmp_path)
    _seed_market_data_inputs(tmp_path)

    construction = build_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T14:00:00Z")

    assert construction["trade_construction_status"] == "complete"
    assert construction["paper_submit_created"] is True
    assert construction["constructed_paper_trade_count"] == 1
    assert construction["constructed_paper_trades"][0]["symbol"] == "QQQ"
    assert construction["constructed_paper_trades"][0]["live_trading_allowed"] is False
    assert construction["constructed_paper_trades"][0]["order_routing_allowed"] is False
    assert construction["broker_execution_allowed"] is False
    assert construction["live_trading_allowed"] is False
    assert construction["order_routing_allowed"] is False


def test_candidate_paper_construction_reports_precise_missing_market_data(tmp_path: Path) -> None:
    _seed_candidate_packet_and_queue(tmp_path, symbol="QQQ")

    construction = build_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T14:00:00Z")

    assert construction["trade_construction_status"] == "blocked_missing_market_data"
    assert construction["paper_submit_created"] is False
    assert construction["constructed_paper_trade_count"] == 0
    diag = construction["market_data_diagnostics"][0]
    assert diag["symbol"] == "QQQ"
    assert diag["missing_field"] == "market_data.current_price"
    assert diag["expected_source_artifact"] == "market_data_inputs_v1"
    assert diag["status"] == "ABSENT"
    assert diag["artifact_path_checked"].endswith(f"/reports/market_data_inputs_v1/{DAY}/market_data_inputs.v1.json")


def test_candidate_paper_construction_rejects_stale_review_packet_when_contracts_are_newer(tmp_path: Path) -> None:
    stale_candidate = {
        "candidate_id": "stale-candidate",
        "symbol": "OLD",
        "direction": "LONG",
        "paper_trade_eligible": True,
        "entry_reference_price": "10.00",
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
    }
    current_candidates = [
        {
            "candidate_id": "candidate-current-1",
            "raw_signal_id": "raw-current-1",
            "symbol": "QQQ",
            "direction": "LONG",
            "paper_trade_eligible": True,
            "entry_reference_price": "500.00",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "status": "AWAITING_REVIEW",
        },
        {
            "candidate_id": "candidate-current-2",
            "raw_signal_id": "raw-current-2",
            "symbol": "SPY",
            "direction": "LONG",
            "paper_trade_eligible": True,
            "entry_reference_price": "600.00",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "status": "AWAITING_REVIEW",
        },
        {
            "candidate_id": "candidate-already-open",
            "raw_signal_id": "raw-already-open",
            "symbol": "DIA",
            "direction": "LONG",
            "paper_trade_eligible": True,
            "entry_reference_price": "400.00",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "status": "PAPER_POSITION_OPEN",
        },
    ]
    _write(tmp_path / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json", {
        "schema_id": "candidate_review_packet",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T14:00:00Z",
        "candidate_count": 1,
        "review_candidates": [stale_candidate],
        "safety": {"paper_only": True, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
    })
    _write(tmp_path / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json", {
        "schema_id": "candidate_contracts",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T15:00:00Z",
        "candidate_contracts": [{**row, "contract_validation_status": "VALID"} for row in current_candidates[:2]],
    })
    _write(tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json", {
        "schema_id": "paper_review_queue",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T15:01:00Z",
        "rows": current_candidates,
        "safety": {"paper_only": True, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
    })
    _write(tmp_path / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {
        "schema_id": "market_data_inputs",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": f"{DAY}T15:01:00Z",
        "validation_status": "VALID",
        "input_records": [
            {"symbol": "QQQ", "value": "500.00", "day_utc": DAY, "validation_status": "VALID"},
            {"symbol": "SPY", "value": "600.00", "day_utc": DAY, "validation_status": "VALID"},
            {"symbol": "DIA", "value": "400.00", "day_utc": DAY, "validation_status": "VALID"},
        ],
    })

    construction = build_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:02:00Z")

    summary = construction["candidate_construction_summary"]
    assert summary["candidate_source"] == "aegis_paper_review_queue_v1"
    assert summary["candidate_count"] == 2
    assert summary["constructed_count"] == 2
    constructed_ids = {row["candidate_id"] for row in construction["constructed_paper_trades"]}
    assert constructed_ids == {"candidate-current-1", "candidate-current-2"}
    assert "stale-candidate" not in constructed_ids
    assert "candidate-already-open" not in constructed_ids
    assert summary["stale_authority_rejections"] == [{
        "artifact_id": "aegis_candidate_review_packet_v1",
        "path": str(tmp_path / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json"),
        "status": "REJECTED_STALE_AUTHORITY",
        "reason_code": "STALE_REVIEW_PACKET_NEWER_CANDIDATE_CONTRACTS",
        "artifact_generated_at_utc": f"{DAY}T14:00:00Z",
        "authoritative_artifact_id": "aegis_candidate_contracts_v1",
        "authoritative_path": str(tmp_path / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json"),
        "authoritative_generated_at_utc": f"{DAY}T15:00:00Z",
        "authoritative_valid_candidate_count": 2,
    }]

def _seed_runtime(root: Path) -> str:
    caps = {
        "TRADE_ADVICE_ALLOWED": {"allowed": False, "reason": "Trade advice disabled for test."},
        "MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": True, "reason": "Manual capture infrastructure allowed for governed test."},
        "AUTONOMOUS_EXECUTION_ALLOWED": {"allowed": False, "reason": "Autonomous execution disabled by design."},
        "BROKER_SUBMIT_TRANSMIT": {"allowed": False, "reason": "Broker submit/transmit disabled by design."},
    }
    evaluation = finalize_runtime_evaluation_v1({
        "run_id": "runtime-test", "parent_run_id": "", "day_utc": DAY, "generated_at_utc": f"{DAY}T14:00:00Z", "git_sha": "TEST",
        "evaluator_version": "aegis_runtime_evaluator.v1", "policy_version": "test", "dag_version": "test", "schema_version": "v1",
        "runtime_truth_classification": "REAL_RUNTIME", "highest_readiness_layer": "ADVISORY_READY", "capabilities": caps,
        "blockers": [], "decision_trace": [], "source_evidence_refs": [], "root_blockers": [], "blocker_state": [], "repairability": {},
        "producer_contract_ref": {}, "next_safe_action": {}, "producer_contract_registry_version": "", "producer_contract_registry_hash": "",
        "warnings": [], "errors": [], "deterministic_output_hash": "",
    })
    path = runtime_evaluation_path_v1(truth_root=root, day_utc=DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(stable_json_bytes_v1(evaluation) + b"\n")
    return str(evaluation["deterministic_output_hash"])


def _prepare_submit_boundary(root: Path, construction: dict) -> None:
    write_ticket_evidence_set_v1(truth_root=root, construction=construction, generated_at_utc=f"{DAY}T14:01:00Z")
    build_and_write_submit_boundary_precheck_v1(truth_root=root, construction=construction, generated_at_utc=f"{DAY}T14:02:00Z")


def _seed(
    root: Path,
    *,
    symbol: str = "AMT",
    observed_session: str = DAY,
    target_notional_pct: str = "0.01",
    max_risk_pct: str = "0.01",
    stop_loss_bps: int | None = 1000,
    capital: bool = True,
    authorized_quantity: int | None = 3,
    submit_status: str = "PASS",
    enable_defaults: bool = True,
    write_policy: bool = True,
    stale_qqq_converter: bool = False,
    engine_id: str = "C2_TREND_EQ_PRIMARY_V1",
    direction: str = "LONG",
    exposure_type: str | None = None,
) -> None:
    constraints: dict[str, object] = {}
    if max_risk_pct:
        constraints["max_risk_pct"] = max_risk_pct
    if stop_loss_bps is not None:
        constraints["stop_loss_bps"] = stop_loss_bps
    intent_path = root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json"
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": INTENT_ID,
        "engine": {"engine_id": engine_id, "mode": "PAPER"},
        "exposure_type": exposure_type or ("SHORT_EQUITY" if direction.upper() == "SHORT" else "LONG_EQUITY"),
        "underlying": {"symbol": symbol, "currency": "USD"},
        "constraints": constraints,
    }
    if target_notional_pct:
        intent["target_notional_pct"] = target_notional_pct
    _write(intent_path, intent)
    _write(
        root / "reports" / "portfolio_gate_candidate_report_v1" / DAY / "portfolio_gate_candidate_report.v1.json",
        {
            "schema_id": "portfolio_gate_candidate_report",
            "schema_version": "portfolio_gate_candidate_report.v1",
            "day_utc": DAY,
            "selected_candidate_id": INTENT_ID,
            "candidate_rows": [
                {
                    "candidate_id": INTENT_ID,
                    "symbol": symbol,
                    "sleeve_id": engine_id,
                    "engine_id": engine_id,
                    "direction": direction.upper(),
                    "selected_by_gate": "YES",
                    "portfolio_gate_decision": "ALLOW",
                    "evidence_paths": [str(intent_path.resolve())],
                }
            ],
        },
    )
    conv_symbol = "QQQ" if stale_qqq_converter else symbol
    conv_id = "stale_qqq" if stale_qqq_converter else INTENT_ID
    _write(
        root / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json",
        {"schema_id": "exposure_intent_paper_submission_package", "schema_version": "v1", "day_utc": DAY, "status": ("BLOCKED" if stale_qqq_converter else "PASS"), "exposure_intent_id": conv_id, "symbol": conv_symbol, "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION" if stale_qqq_converter else ""},
    )
    _write_jsonl(
        root / "market_data_snapshot_v1" / symbol / "2026.jsonl",
        [
            {"schema_id": "market_data_snapshot_v1", "symbol": symbol, "timestamp_utc": f"{observed_session}T00:00:00Z", "close": "180.25", "open": "180.00", "high": "181.00", "low": "179.00", "volume": 1000000}
        ],
    )
    _write(root / "market_data_snapshot_v1" / "dataset_manifest.json", {"schema_id": "market_data_snapshot_manifest", "symbols": [symbol], "files": [{"symbol": symbol, "file": f"{symbol}/2026.jsonl"}]})
    if capital:
        row = {"intent_id": INTENT_ID, "authorization_outcome": "APPROVED"}
        if authorized_quantity is not None:
            row["authorized_quantity"] = authorized_quantity
        _write(root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json", {"schema_id": "capital_authority_allocation", "decision_chain": {"authorized_trade_intents": [row]}})
    _write(root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json", {"schema_id": "submit_boundary_status", "schema_version": "v1", "status": submit_status})
    if write_policy:
        _write(
            root / "config" / "paper_trade_construction_policy.v1.json",
            {
                "schema_id": "paper_trade_construction_policy",
                "schema_version": "v1",
                "enabled": enable_defaults,
                "paper_test_capital_base": "100000",
                "max_notional_per_trade": "5000",
                "max_risk_per_trade": "1000",
                "entry_reference_rule": "latest_close",
                "quantity_rounding_rule": "floor",
                "default_stop_policy": {"stop_loss_bps": 1000},
            },
        )


def test_selected_exposure_always_produces_construction_artifact(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    snapshot = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    construction = snapshot["paper_trade_construction_v1"]
    assert construction["schema_id"] == "paper_trade_construction"
    assert construction["selected_exposure_intent_id"] == INTENT_ID
    assert (root / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json").exists()


def test_complete_fixture_produces_entry_qty_notional_stop_risk(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["trade_construction_status"] == "complete"
    assert construction["entry_reference_price"] == "180.25"
    assert construction["suggested_quantity"] == 3
    assert construction["suggested_notional"] == "540.75"
    assert construction["stop_price"] == "162.23"
    assert construction["risk_per_share"] == "18.02"
    assert construction["max_loss_estimate"] == "54.06"
    assert construction["manual_capture_ready"] is True


def test_stale_data_blocks_construction(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, observed_session="2026-05-19")
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["trade_construction_status"] == "blocked_missing_market_data"
    assert "MISSING_CURRENT_MARKET_DATA" in construction["blocker_codes"]


def test_missing_capital_authority_blocks_construction(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, capital=False)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["trade_construction_status"] == "blocked_missing_capital_authority"
    assert "MISSING_CAPITAL_AUTHORITY" in construction["blocker_codes"]


def test_existing_paper_capital_authority_is_resolved_from_allocation_decision(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, capital=False)
    _write(
        root / "allocation_v1" / "decisions" / DAY / f"{INTENT_HASH}.allocation_decision.v1.json",
        {
            "schema_id": "allocation_decision",
            "schema_version": "v1",
            "status": "BLOCK",
            "decision": {"intent_id": INTENT_ID, "contracts_allowed": 0},
            "binding_constraints": ["G_BLOCK_ACCOUNTING_NOT_OK"],
        },
    )
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["capital_authority_source"] == "allocation_decision_v1"
    assert "MISSING_CAPITAL_AUTHORITY" not in construction["blocker_codes"]
    assert "CAPITAL_AUTHORITY_BLOCKED" in construction["blocker_codes"]


def test_existing_entry_policy_is_resolved_from_current_market_close(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, write_policy=False)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["entry_reference_price"] == "180.25"
    assert construction["entry_reference_source"] == "market_data_snapshot_v1.latest_close"
    assert "MISSING_ENTRY_POLICY" not in construction["blocker_codes"]


def test_existing_stop_policy_is_resolved_from_risk_policy_registry(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stop_loss_bps=None, write_policy=False)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["stop_price"] == "162.23"
    assert construction["stop_policy_source"] == "C2_RISK_POLICY_REGISTRY_V1:C2_TREND_EQ_PRIMARY_V1.stop_loss_bps_default"
    assert "MISSING_STOP_POLICY" not in construction["blocker_codes"]


def test_qqq_construction_uses_existing_target_notional_pct_and_stop_loss_bps(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, symbol="QQQ", target_notional_pct="0.10", stop_loss_bps=1000, write_policy=False)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["symbol"] == "QQQ"
    assert construction["allocation_percent"] == "0.1"
    assert construction["stop_price"] == "162.23"
    assert construction["stop_policy_source"] == "exposure_intent.constraints.stop_loss_bps"


def test_cross_asset_trend_stop_policy_registry_is_governed_and_not_symbol_specific() -> None:
    registry = json.loads((ROOT / "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json").read_text(encoding="utf-8"))
    policy = registry["policies"]["C2_CROSS_ASSET_TREND_V1"]
    contract_path = ROOT / policy["contract_ref"]

    assert policy["policy_id"] == "CROSS_ASSET_TREND_STOP_POLICY_V1"
    assert policy["stop_loss_bps_default"] == 1000
    assert policy["expected_holding_days_default"] == 60
    assert contract_path.exists()
    assert "QQQ" not in contract_path.read_text(encoding="utf-8")
    assert policy["broker_execution_allowed"] is False
    assert policy["paper_submit_allowed"] is False
    validate_against_repo_schema_v1(registry, ROOT, "governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json")


def test_cross_asset_trend_long_stop_and_risk_from_registry(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, symbol="QQQ", engine_id="C2_CROSS_ASSET_TREND_V1", stop_loss_bps=None, target_notional_pct="0.10", write_policy=False)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)

    assert construction["trade_construction_status"] == "complete"
    assert construction["stop_policy_id"] == "CROSS_ASSET_TREND_STOP_POLICY_V1"
    assert construction["stop_policy_source"] == "C2_RISK_POLICY_REGISTRY_V1:C2_CROSS_ASSET_TREND_V1.stop_loss_bps_default"
    assert construction["stop_loss_bps"] == "1000"
    assert construction["stop_price"] == "162.23"
    assert construction["risk_per_share"] == "18.02"
    assert construction["max_loss_estimate"] == "54.06"
    assert construction["estimated_notional_risk_pct"] == "0.099972"
    assert construction["manual_capture_ready"] is True


def test_cross_asset_trend_short_stop_and_risk_from_registry(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, symbol="TLT", engine_id="C2_CROSS_ASSET_TREND_V1", stop_loss_bps=None, target_notional_pct="0.10", write_policy=False, direction="SHORT")
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)

    assert construction["direction"] == "SHORT"
    assert construction["stop_price"] == "198.28"
    assert construction["risk_per_share"] == "18.03"
    assert construction["max_loss_estimate"] == "54.09"
    assert construction["manual_capture_ready"] is True


def test_construction_does_not_create_duplicate_policy_files(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, write_policy=False)
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    assert not policy_path.exists()
    build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert not policy_path.exists()


def test_missing_stop_policy_blocks_construction(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stop_loss_bps=None, enable_defaults=True, engine_id="C2_NO_STOP_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy = json.loads(policy_path.read_text())
    policy["default_stop_policy"] = {}
    _write(policy_path, policy)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["trade_construction_status"] == "blocked_missing_stop_policy"
    assert "MISSING_STOP_POLICY" in construction["blocker_codes"]


def test_missing_sizing_policy_blocks_construction(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, authorized_quantity=None, enable_defaults=True, engine_id="C2_NO_SIZING_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy = json.loads(policy_path.read_text())
    policy["paper_test_capital_base"] = ""
    _write(policy_path, policy)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["trade_construction_status"] == "blocked_missing_sizing_policy"
    assert "MISSING_SUGGESTED_QUANTITY" in construction["blocker_codes"]


def test_missing_risk_policy_blocks_construction(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy = json.loads(policy_path.read_text())
    policy["max_risk_per_trade"] = "1"
    _write(policy_path, policy)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["trade_construction_status"] == "blocked_missing_risk_policy"
    assert "MISSING_RISK_POLICY" in construction["blocker_codes"]


def test_captured_manually_cannot_save_unless_construction_complete(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root, stop_loss_bps=None, engine_id="C2_NO_STOP_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy_path.write_text('{"schema_id":"paper_trade_construction_policy","schema_version":"v1","enabled":true,"paper_test_capital_base":"100000","max_notional_per_trade":"5000","max_risk_per_trade":"1000","entry_reference_rule":"latest_close","quantity_rounding_rule":"floor","default_stop_policy":{}}\n', encoding="utf-8")
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={"selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "quantity": "3", "fill_price": "180.50", "fill_time": f"{DAY}T15:30:00Z", "stop_price": "162.23", "operator_id": "test"})
    except ValueError as exc:
        assert "STALE_RUNTIME" in str(exc) or "MISSING_SUBMIT_BOUNDARY" in str(exc) or "NOT_CAPTURE_READY" in str(exc)
    else:
        raise AssertionError("incomplete construction allowed captured_manually")


def test_missing_operator_id_blocks_manual_capture_record(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={"selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "quantity": "3", "fill_price": "180.50", "fill_time": f"{DAY}T15:30:00Z", "stop_price": "162.23"})
    except ValueError as exc:
        assert "OPERATOR_ID_MISSING" in str(exc)
    else:
        raise AssertionError("manual capture succeeded without operator_id")


def _lineage_bound_payload(root: Path, construction: dict, *, capture_status: str = "not_captured") -> dict:
    ticket_id = ticket_id_v1(construction)
    lineage = read_trade_ticket_lineage_v1(truth_root=root, day_utc=DAY, ticket_id=ticket_id)
    assert lineage["lineage_status"] == "ACTIVE_CURRENT"
    return {
        "ticket_id": ticket_id,
        "selected_exposure_intent_id": INTENT_ID,
        "capture_status": capture_status,
        "operator_id": "test",
        "runtime_evaluation_hash": lineage["runtime_evaluation_hash"],
        "ticket_lineage_hash": lineage["lineage_hash"],
        "submit_boundary_hash": lineage["submit_boundary_hash"],
        "construction_contract_hash": lineage["construction_contract_hash"],
    }


def test_lineage_bound_manual_capture_save_succeeds(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = {
        **_lineage_bound_payload(root, construction, capture_status="captured_manually"),
        "quantity": "3",
        "fill_price": "180.50",
        "fill_time_utc": f"{DAY}T15:30:00Z",
        "stop_price": "162.23",
    }
    record = append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    assert record["capture_status"] == "captured_manually"
    assert record["ticket_id"] == payload["ticket_id"]
    assert record["event_ids"]
    assert record["broker_execution_allowed"] is False
    assert record["paper_submit_created"] is False


def test_operator_capture_status_and_local_fill_time_are_normalized(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = {
        **_lineage_bound_payload(root, construction, capture_status="captured"),
        "quantity": "3",
        "fill_price": "180.50",
        "fill_time_local": f"{DAY}T15:30",
        "stop_price": "162.23",
    }
    record = append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    assert record["capture_status"] == "captured_manually"
    assert record["fill_time"].startswith(f"{DAY}T15:30")
    assert record["broker_execution_allowed"] is False
    assert record["paper_submit_created"] is False


def test_invalid_fill_time_returns_received_value(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = {
        **_lineage_bound_payload(root, construction, capture_status="captured_manually"),
        "quantity": "3",
        "fill_price": "180.50",
        "fill_time_utc": "not-a-time",
        "stop_price": "162.23",
    }
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    except ValueError as exc:
        text = str(exc)
        assert "INVALID_FILL_TIME" in text
        assert "Invalid fill time format" in text
        assert '"received_value": "not-a-time"' in text
    else:
        raise AssertionError("invalid fill time saved")


def test_missing_fill_time_returns_empty_received_value(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = {
        **_lineage_bound_payload(root, construction, capture_status="captured_manually"),
        "quantity": "3",
        "fill_price": "180.50",
        "stop_price": "162.23",
    }
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    except ValueError as exc:
        text = str(exc)
        assert "INVALID_FILL_TIME" in text
        assert "Fill time is required." in text
        assert '"received_value": ""' in text
    else:
        raise AssertionError("missing fill time saved")


def test_stale_ticket_lineage_hash_returns_refresh_required(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = _lineage_bound_payload(root, construction)
    payload["ticket_lineage_hash"] = "0" * 64
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    except ValueError as exc:
        assert "REFRESH_REQUIRED" in str(exc)
        assert "STALE_TICKET_LINEAGE" in str(exc)
        assert payload["ticket_id"] in str(exc)
    else:
        raise AssertionError("stale ticket lineage hash saved")


def test_stale_construction_id_cannot_override_ticket_lineage(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = _lineage_bound_payload(root, construction)
    payload["paper_trade_construction_id"] = "paper-trade-construction:stale"
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    except ValueError as exc:
        assert "CONSTRUCTION_CONTRACT_MISMATCH" in str(exc)
        assert "paper_trade_construction_id does not match ticket lineage" in str(exc)
    else:
        raise AssertionError("stale construction id saved")


def test_complete_construction_allows_manual_capture_record(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    record = append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={"paper_trade_construction_id": construction["construction_id"], "selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "quantity": "3", "fill_price": "180.50", "fill_time": f"{DAY}T15:30:00Z", "stop_price": "162.23", "operator_id": "test"})
    assert record["capture_status"] == "captured_manually"
    assert record["paper_trade_construction_id"] == construction["construction_id"]
    assert record["broker_execution_allowed"] is False
    assert record["paper_submit_created"] is False


def test_ib_handshake_missing_does_not_affect_manual_capture_save(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)

    ib_path = root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
    assert not ib_path.exists()

    record = append_manual_capture_record_v1(
        truth_root=root,
        day_utc=DAY,
        request_payload={
            "paper_trade_construction_id": construction["construction_id"],
            "selected_exposure_intent_id": INTENT_ID,
            "capture_status": "captured_manually",
            "quantity": "3",
            "fill_price": "180.50",
            "fill_time": f"{DAY}T15:30:00Z",
            "stop_price": "162.23",
            "operator_id": "test",
        },
    )

    assert record["capture_status"] == "captured_manually"
    assert record["manual_capture_only"] is True
    assert record["manual_capture_status"] == "READY"
    assert record["submit_boundary_status"] == "VALIDATED"
    assert record["broker_submit_status"] == "DISABLED"
    assert record["ib_api_handshake_required"] is False
    assert "IB handshake NOT REQUIRED FOR MANUAL CAPTURE" in record["readiness_summary"]
    assert record["broker_execution_allowed"] is False
    assert record["order_routing_allowed"] is False
    assert record["autonomous_execution_allowed"] is False


def test_ui_always_shows_trade_fields_or_blockers_and_reads_construction_contract() -> None:
    text = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    assert "trade_ticket_projection_v1" in text
    assert "Entry:" in text
    assert "Qty:" in text
    assert "Stop:" in text
    assert "Risk:" in text
    assert "Manual capture only" in text
    assert "No frontend may calculate trade readiness" not in text



def test_conversion_evidence_binds_selected_candidate_and_exposes_stale_converter(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stale_qqq_converter=True)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)

    result = write_ticket_evidence_set_v1(truth_root=root, construction=construction, generated_at_utc=f"{DAY}T14:01:00Z")
    conversion = result["evidences"]["conversion"]
    selected_ref = next(row for row in construction["source_artifacts"] if row["artifact_id"] == "selected_exposure_intent")
    conversion_ref = next(row for row in construction["source_artifacts"] if row["artifact_id"] == "exposure_intent_conversion")

    assert conversion["validation_status"] == "INVALID"
    assert "CONVERSION_MISMATCH" in conversion["blocker_codes"]
    assert conversion["source_candidate_artifact_hash"] == selected_ref["sha256"]
    assert conversion["conversion_artifact_hash"] == conversion_ref["sha256"]
    assert conversion["candidate_to_ticket_conversion_trace"]["selected_exposure_intent_id"] == INTENT_ID
    assert conversion["candidate_to_ticket_conversion_trace"]["conversion_exposure_intent_id"] == "stale_qqq"
    assert conversion["risk_quantity_notional_trace"]["suggested_quantity"] == construction["suggested_quantity"]
    assert conversion["broker_execution_allowed"] is False
    assert conversion["autonomous_execution_allowed"] is False

def test_current_selected_exposure_cannot_regress_to_stale_qqq(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stale_qqq_converter=True)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["symbol"] == "AMT"
    assert construction["selected_exposure_intent_id"] == INTENT_ID


def test_api_routes_return_safe_degraded_payloads_and_no_side_effect_strings() -> None:
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    assert "/api/aegis/operator/paper-trade-construction/latest" in server
    assert "/api/aegis/operator/paper-trade-construction/" in server
    assert "/api/aegis/operator/manual-capture/latest" in server
    assert "paper_trade_construction_view_v1" in server
    assert "paper_submit_created" in server


def test_no_broker_order_allocation_side_effects(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    assert construction["broker_execution_allowed"] is False
    assert construction["order_routing_allowed"] is False
    assert construction["capital_allocation_allowed"] is False
    assert construction["paper_submit_created"] is False


def _write_paper_envelope(root: Path, *, status: str = "PASS", headroom_cents: int = 600000) -> None:
    _write(
        root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        {
            "schema_id": "capital_risk_envelope",
            "schema_version": "v2",
            "status": status,
            "envelope": {
                "nav_total": "300000.00",
                "nav_total_cents": 30000000,
                "headroom_cents": headroom_cents,
                "allowed_capital_at_risk_cents": 600000,
                "portfolio_capital_at_risk_cents": 0,
            },
        },
    )


def test_dow_mean_reversion_resolves_capital_stop_quantity_and_risk_from_governed_sources(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(
        root,
        symbol="DOW",
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        stop_loss_bps=None,
        target_notional_pct="0.20",
        max_risk_pct="0.02",
        capital=False,
        write_policy=False,
    )
    _write_paper_envelope(root)

    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)

    assert construction["symbol"] == "DOW"
    assert construction["sleeve_id"] == "C2_MEAN_REVERSION_EQ_V1"
    assert construction["trade_construction_status"] == "complete"
    assert construction["capital_authority_source"] == "capital_policy_and_risk_envelope_v1"
    assert construction["stop_policy_source"] == "C2_RISK_POLICY_REGISTRY_V1:C2_MEAN_REVERSION_EQ_V1.stop_loss_bps_default"
    assert construction["entry_reference_price"] == "180.25"
    assert construction["suggested_quantity"] == 33
    assert construction["suggested_notional"] == "5948.25"
    assert construction["stop_price"] == "171.24"
    assert construction["risk_per_share"] == "9.01"
    assert construction["max_loss_estimate"] == "297.33"
    assert construction["manual_capture_ready"] is True
    assert "MISSING_CAPITAL_AUTHORITY" not in construction["blocker_codes"]
    assert "MISSING_STOP_POLICY" not in construction["blocker_codes"]
    assert "MISSING_SUGGESTED_QUANTITY" not in construction["blocker_codes"]
    assert "MISSING_RISK_ESTIMATE" not in construction["blocker_codes"]


def test_active_paper_sleeves_have_capital_and_stop_policy_coverage() -> None:
    required_engines = {
        "C2_MEAN_REVERSION_EQ_V1",
        "C2_TREND_EQ_PRIMARY_V1",
        "C2_EVENT_DISLOCATION_V1",
        "C2_CROSS_ASSET_TREND_V1",
        "C2_VOL_INCOME_DEFINED_RISK_V1",
        "C2_MARKET_NEUTRAL_SPREAD_V1",
        "C2_DEFENSIVE_TAIL_V1",
    }
    risk_registry = json.loads((ROOT / "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json").read_text(encoding="utf-8"))
    capital_registry = json.loads((ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").read_text(encoding="utf-8"))

    risk_policies = risk_registry["policies"]
    assert required_engines <= set(risk_policies)
    for engine_id in required_engines:
        policy = risk_policies[engine_id]
        assert policy["stop_loss_bps_default"] > 0 or policy.get("invalidation_policy")
        assert policy["allow_entry_only_paper_test"] is False
        assert policy["broker_execution_allowed"] is False
        assert policy["paper_submit_allowed"] is False
        assert (ROOT / policy["contract_ref"]).exists()

    capital_by_engine = {}
    for row in capital_registry["sleeves"]:
        for engine_id in row.get("engine_ids", []):
            capital_by_engine[engine_id] = row
    assert required_engines <= set(capital_by_engine)
    for engine_id in required_engines:
        limits = capital_by_engine[engine_id]["limits"]
        assert limits["paper_enabled"] is True
        assert float(limits["paper_test_capital_base"]) > 0
        assert float(limits["max_notional_per_trade"]) > 0
        assert float(limits["max_risk_per_trade"]) > 0

    validate_against_repo_schema_v1(risk_registry, ROOT, "governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json")


def test_missing_policy_blocks_only_when_truly_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(
        root,
        symbol="DOW",
        engine_id="C2_UNKNOWN_ACTIVELESS_ENGINE_V1",
        stop_loss_bps=None,
        capital=False,
        write_policy=False,
    )
    _write_paper_envelope(root)

    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)

    assert construction["trade_construction_status"] == "blocked_missing_capital_authority"
    assert "MISSING_CAPITAL_AUTHORITY" in construction["blocker_codes"]
    assert "MISSING_STOP_POLICY" in construction["blocker_codes"]



def test_captured_ticket_becomes_historical_and_skips_post_capture_submit_boundary(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = {
        **_lineage_bound_payload(root, construction, capture_status="captured"),
        "quantity": "3",
        "fill_price": "180.50",
        "fill_time_local": f"{DAY}T15:30",
        "stop_price": "162.23",
    }
    record = append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)

    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=DAY)
    runtime_payload = json.loads(runtime_path.read_text(encoding="utf-8"))
    runtime_payload["deterministic_output_hash"] = "b" * 64
    _write(runtime_path, runtime_payload)

    precheck, _path, lineage = build_and_write_submit_boundary_precheck_v1(truth_root=root, construction=construction)

    assert record["record_id"]
    assert lineage["lineage_status"] == "CAPTURED_HISTORICAL"
    assert "STALE_RUNTIME" not in lineage["blocker_codes"]
    assert precheck["validation_status"] == "NOT_APPLICABLE"
    assert precheck["allowed_boundary"] == "NONE_POST_CAPTURE_HISTORICAL"
    assert precheck["submit_boundary_revalidation_required"] is False
    assert lineage["editable"] is False
    assert lineage["read_only"] is True


def test_duplicate_manual_capture_append_is_blocked_before_lineage_revalidation(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    payload = {
        **_lineage_bound_payload(root, construction, capture_status="captured"),
        "quantity": "3",
        "fill_price": "180.50",
        "fill_time_local": f"{DAY}T15:30",
        "stop_price": "162.23",
    }
    append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=payload)
    except ValueError as exc:
        assert "ALREADY_CAPTURED" in str(exc)
    else:
        raise AssertionError("duplicate manual capture append succeeded")
