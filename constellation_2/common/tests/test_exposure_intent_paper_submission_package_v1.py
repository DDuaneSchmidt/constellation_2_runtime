from __future__ import annotations

import json
from pathlib import Path

import importlib.util
import sys
import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

_spec = importlib.util.spec_from_file_location("run_exposure_intent_paper_submission_package_v1", SOURCE_ROOT / "ops/tools/run_exposure_intent_paper_submission_package_v1.py")
assert _spec is not None and _spec.loader is not None
tool = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tool)

from ops.aegis.capital_authority_allocation_v1 import build_and_write_capital_authority_allocation_v1
from ops.aegis.risk_definition_contract_v1 import build_and_write_risk_definition_contract_v1
from ops.aegis.candidate_identity_set_v1 import build_and_write_candidate_identity_set_v1
from ops.aegis.runtime_evaluation_v1 import finalize_runtime_evaluation_v1, stable_json_bytes_v1
from ops.aegis.runtime_truth_kernel_v1 import runtime_evaluation_path_v1

DAY = "2026-05-20"
INTENT_ID = "c2_trend_eq_spy_2026-05-20_v1"
INTENT_HASH = "a" * 64
ENGINE_ID = "C2_TREND_EQ_PRIMARY_V1"


@pytest.fixture(autouse=True)
def _canonical_symbol_authority_fixture(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    if request.node.name == "test_symbol_authority_deprecated_fallback_is_not_used":
        return

    class Resolution:
        symbols = ["SPY", "QQQ", "IWM"]
        source = "engine_universe_candidate_basis_v1"
        source_path = "/tmp/engine_universe_candidate_basis.v1.json"
        symbol_count = 3
        deprecated_fallback_used = False

    monkeypatch.setattr(tool, "resolve_canonical_symbol_universe_v1", lambda **_: Resolution())


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_selected(root: Path, *, symbol: str = "SPY") -> Path:
    intent_path = root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json"
    _write(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": INTENT_ID,
            "engine": {"engine_id": ENGINE_ID, "mode": "PAPER"},
            "exposure_type": "LONG_EQUITY",
            "underlying": {"symbol": symbol, "currency": "USD"},
            "target_notional_pct": "0.01",
            "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
        },
    )
    pointer = root / "pointers" / "selected_intent_pointer.v1.json"
    _write(
        pointer,
        {
            "schema_id": "selected_intent_pointer",
            "schema_version": "v1",
            "status": "SELECTED",
            "day_utc": DAY,
            "selected_intent": {
                "intent_id": INTENT_ID,
                "intent_hash": INTENT_HASH,
                "intent_path": str(intent_path.resolve()),
                "engine_id": ENGINE_ID,
                "sleeve_id": ENGINE_ID,
                "symbol": symbol,
                "executable_eligible": True,
            },
        },
    )
    return pointer


def _seed_symbol_authority(root: Path, *, symbol: str = "SPY") -> None:
    symbols = sorted({symbol, *[f"T{i:03d}" for i in range(120)]})
    _write(root / "market_data_snapshot_v1" / "dataset_manifest.json", {"schema_id": "market_data_snapshot_manifest", "symbols": symbols, "files": []})


def _seed_market(root: Path, *, symbol: str = "SPY", day: str = DAY) -> None:
    _write(
        root / "market_data_snapshot_v1" / "snapshots" / DAY / f"{symbol}.market_data_snapshot.v1.json",
        {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "day_utc": day, "symbol": symbol, "close": "500.00"},
    )


def _seed_capital(root: Path, *, authorized: bool = True) -> None:
    _write(
        root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {
            "decision_chain": {
                "authorized_trade_intents": [
                    {
                        "intent_id": INTENT_ID,
                        "intent_hash": INTENT_HASH,
                        "authorization_outcome": "APPROVED" if authorized else "REJECTED",
                        "authorized_quantity": 1 if authorized else 0,
                    }
                ]
            }
        },
    )



def _seed_runtime(root: Path, *, hash_override: str = "") -> str:
    caps = {
        "TRADE_ADVICE_ALLOWED": {"allowed": False, "reason": "Trade advice disabled."},
        "MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": True, "reason": "Manual capture allowed."},
        "AUTONOMOUS_EXECUTION_ALLOWED": {"allowed": False, "reason": "Autonomous execution disabled."},
        "BROKER_SUBMIT_TRANSMIT": {"allowed": False, "reason": "Broker submit disabled."},
    }
    evaluation = finalize_runtime_evaluation_v1({
        "run_id": "runtime-test", "parent_run_id": "", "day_utc": DAY, "generated_at_utc": f"{DAY}T14:00:00Z", "git_sha": "TEST",
        "evaluator_version": "aegis_runtime_evaluator.v1", "policy_version": "test", "dag_version": "test", "schema_version": "v1",
        "runtime_truth_classification": "REAL_RUNTIME", "highest_readiness_layer": "MANUAL_TRADE_CAPTURE_ALLOWED", "capabilities": caps,
        "blockers": [], "decision_trace": [], "source_evidence_refs": [], "root_blockers": [], "blocker_state": [], "repairability": {},
        "producer_contract_ref": {}, "next_safe_action": {}, "producer_contract_registry_version": "", "producer_contract_registry_hash": "",
        "warnings": [], "errors": [], "deterministic_output_hash": "",
    })
    if hash_override:
        evaluation["deterministic_output_hash"] = hash_override
    path = runtime_evaluation_path_v1(truth_root=root, day_utc=DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(stable_json_bytes_v1(evaluation) + b"\n")
    return str(evaluation["deterministic_output_hash"])


def _seed_construction(root: Path, runtime_hash: str, *, engine_id: str = ENGINE_ID, quantity: int = 1, notional: str = "500.00", risk: str = "50.00") -> None:
    _write(
        root / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json",
        {
            "schema_id": "paper_trade_construction",
            "schema_version": "v1",
            "source_day": DAY,
            "selected_exposure_intent_id": INTENT_ID,
            "selected_exposure_intent_hash": INTENT_HASH,
            "sleeve_id": engine_id,
            "engine_id": engine_id,
            "symbol": "SPY",
            "direction": "LONG",
            "suggested_quantity": quantity,
            "suggested_notional": notional,
            "max_loss_estimate": risk,
            "runtime_evaluation_hash": runtime_hash,
            "construction_contract_hash": "3" * 64,
            "trade_construction_status": "complete",
            "source_artifacts": [],
        },
    )

def _seed_risk(root: Path) -> None:
    _write(root / "risk_definition_contract_v1" / DAY / INTENT_HASH / "risk_definition_contract.v1.json", {"validation_status": "PASS"})


def _seed_valid_governed_risk(root: Path) -> dict:
    payload, _path = build_and_write_risk_definition_contract_v1(
        truth_root=root,
        day_utc=DAY,
        intent_hash=INTENT_HASH,
        generated_at_utc=f"{DAY}T14:10:00Z",
        emit_events=False,
    )
    return payload


def _seed_valid_candidate_identity(root: Path) -> dict:
    payload, _path = build_and_write_candidate_identity_set_v1(
        truth_root=root,
        day_utc=DAY,
        intent_hash=INTENT_HASH,
        generated_at_utc=f"{DAY}T14:11:00Z",
        emit_events=False,
    )
    return payload


def _seed_fake_execution_package(root: Path) -> Path:
    package_path = root / "execution_package_v1" / DAY / ("b" * 64) / "execution_package.v1.json"
    plan_path = package_path.parent / "equity_order_plan.v2.json"
    candidate = package_path.parent / "candidate"
    _write(plan_path, {"source_intent_id": INTENT_ID, "intent_hash": INTENT_HASH, "intent_sha256": INTENT_HASH})
    _write(candidate / "binding_record.v2.json", {"binding_id": "binding-1", "intent_hash": INTENT_HASH})
    _write(package_path, {
        "schema_id": "execution_package",
        "schema_version": "v1",
        "submission_id": "b" * 64,
        "canonical_json_hash": "c" * 64,
        "candidate_ref": {"phasec_out_dir": str(candidate.resolve()), "environment": "PAPER", "sleeve_id": "PRIMARY"},
        "selected_order_plan_ref": {"path": str(plan_path.resolve()), "sha256": "d" * 64},
        "build_ref": {"path": str((package_path.parent / "execution_build.v1.json").resolve()), "sha256": "e" * 64},
        "intent_id": INTENT_ID,
        "trade_instance_id": "f" * 64,
        "advisory_submission": {"execution_intent_id": INTENT_ID, "promotion_record_id": "capital-authority", "household_id": "AegisPaper", "promotion_idempotency_key": INTENT_HASH},
    })
    return package_path


def test_stale_market_data_blocks_before_package_creation(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root, day="2026-05-19")

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["status"] == "BLOCKED"
    assert result["blocker_code"] == "STALE_MARKET_DATA_BLOCKS_CONVERSION"
    assert result["execution_package_created"] is False
    assert result["paper_submit_attempted"] is False
    assert Path(result["artifact_path"]).exists()


def test_missing_capital_authority_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["status"] == "BLOCKED"
    assert result["blocker_code"] == "CAPITAL_AUTHORITY_ALLOCATION_MISSING"
    assert result["submission_record_created"] is False



def test_policy_derived_capital_allocation_authorizes_valid_selected_intent(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    payload, allocation_path = build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, generated_at_utc=f"{DAY}T14:05:00Z", emit_events=False)
    _seed_valid_governed_risk(root)
    _seed_valid_candidate_identity(root)
    package_path = root / "execution_package_v1" / DAY / ("b" * 64) / "execution_package.v1.json"
    plan_path = package_path.parent / "equity_order_plan.v2.json"
    candidate = package_path.parent / "candidate"
    _write(plan_path, {"source_intent_id": INTENT_ID, "intent_hash": INTENT_HASH, "intent_sha256": INTENT_HASH})
    _write(candidate / "binding_record.v2.json", {"binding_id": "binding-1", "intent_hash": INTENT_HASH})
    _write(package_path, {"schema_id": "execution_package", "schema_version": "v1", "submission_id": "b" * 64, "canonical_json_hash": "c" * 64, "candidate_ref": {"phasec_out_dir": str(candidate.resolve()), "environment": "PAPER", "sleeve_id": "PRIMARY"}, "selected_order_plan_ref": {"path": str(plan_path.resolve()), "sha256": "d" * 64}, "build_ref": {"path": str((package_path.parent / "execution_build.v1.json").resolve()), "sha256": "e" * 64}, "intent_id": INTENT_ID, "trade_instance_id": "f" * 64, "advisory_submission": {"execution_intent_id": INTENT_ID, "promotion_record_id": "capital-authority", "household_id": "AegisPaper", "promotion_idempotency_key": INTENT_HASH}})
    monkeypatch.setattr(tool, "build_execution_package_from_authorized_intent_v1", lambda **kwargs: {"status": "PASS", "package_path": str(package_path), "build_path": str(package_path.parent / "execution_build.v1.json")})

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert payload["validation_status"] == "VALID"
    assert Path(allocation_path).exists()
    assert result["status"] == "PASS"
    assert result["capital_authority_hash"]
    assert result["allocation_source"] == "POLICY_DERIVED"


def test_stale_capital_allocation_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_capital(root)
    path = root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    payload = json.loads(path.read_text())
    payload.update({"schema_id": "capital_authority_allocation", "schema_version": "v1", "day_utc": "2026-05-19", "runtime_evaluation_hash": runtime_hash, "validation_status": "VALID"})
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "CAPITAL_AUTHORITY_STALE"


def test_wrong_runtime_hash_capital_allocation_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    _seed_runtime(root)
    _seed_capital(root)
    path = root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    payload = json.loads(path.read_text())
    payload.update({"schema_id": "capital_authority_allocation", "schema_version": "v1", "day_utc": DAY, "runtime_evaluation_hash": "0" * 64, "validation_status": "VALID"})
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "RUNTIME_HASH_MISMATCH"


def test_allocation_builder_rejects_unauthorized_sleeve_limit_exceeded_and_manual_required(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash, engine_id="C2_UNKNOWN_ENGINE_V1")
    unauthorized = build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)[0]
    assert unauthorized["validation_status"] == "REJECTED"
    assert "SLEEVE_NOT_AUTHORIZED" in unauthorized["reason_codes"]

    _seed_construction(root, runtime_hash, notional="999999.00")
    exceeded = build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)[0]
    assert exceeded["validation_status"] == "REJECTED"
    assert "ALLOCATION_LIMIT_EXCEEDED" in exceeded["reason_codes"]

    empty_root = tmp_path / "empty_truth"
    _seed_runtime(empty_root)
    manual_required = build_and_write_capital_authority_allocation_v1(truth_root=empty_root, day_utc=DAY, emit_events=False)[0]
    assert manual_required["validation_status"] == "MANUAL_REQUIRED"
    assert manual_required["decision_chain"]["authorized_trade_intents"] == []
    assert "build_capital_authority_allocation_v1.py" in manual_required["manual_required_command"]

def test_converter_creates_package_and_submission_record_when_governed_inputs_exist(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    _seed_valid_candidate_identity(root)
    package_path = root / "execution_package_v1" / DAY / ("b" * 64) / "execution_package.v1.json"
    plan_path = package_path.parent / "equity_order_plan.v2.json"
    candidate = package_path.parent / "candidate"
    binding_path = candidate / "binding_record.v2.json"
    plan = {"source_intent_id": INTENT_ID, "intent_hash": INTENT_HASH, "intent_sha256": INTENT_HASH}
    binding = {"binding_id": "binding-1", "intent_hash": INTENT_HASH}
    _write(plan_path, plan)
    _write(binding_path, binding)
    package = {
        "schema_id": "execution_package",
        "schema_version": "v1",
        "submission_id": "b" * 64,
        "canonical_json_hash": "c" * 64,
        "candidate_ref": {"phasec_out_dir": str(candidate.resolve()), "environment": "PAPER", "sleeve_id": "PRIMARY"},
        "selected_order_plan_ref": {"path": str(plan_path.resolve()), "sha256": "d" * 64},
        "build_ref": {"path": str((package_path.parent / "execution_build.v1.json").resolve()), "sha256": "e" * 64},
        "intent_id": INTENT_ID,
        "trade_instance_id": "f" * 64,
        "advisory_submission": {"execution_intent_id": INTENT_ID, "promotion_record_id": "capital-authority", "household_id": "AegisPaper", "promotion_idempotency_key": INTENT_HASH},
    }
    _write(package_path, package)

    def fake_build(**kwargs):
        assert kwargs["intent_id"] == INTENT_ID
        return {"status": "PASS", "package_path": str(package_path), "build_path": str(package_path.parent / "execution_build.v1.json")}

    monkeypatch.setattr(tool, "build_execution_package_from_authorized_intent_v1", fake_build)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["status"] == "PASS"
    assert result["execution_package_created"] is True
    assert result["submission_record_created"] is True
    assert result["paper_submit_attempted"] is False
    record = json.loads(Path(result["submission_record_path"]).read_text(encoding="utf-8"))
    assert record["schema_id"] == "execution_submission_record"
    assert record["status"] == "READY_TO_SUBMIT"
    assert record["execution_package_ref"]["path"] == str(package_path.resolve())



def test_missing_risk_contract_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "RISK_DEFINITION_CONTRACT_MISSING"


def test_valid_risk_contract_clears_conversion_blocker(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    risk = _seed_valid_governed_risk(root)
    _seed_valid_candidate_identity(root)
    package_path = _seed_fake_execution_package(root)
    monkeypatch.setattr(tool, "build_execution_package_from_authorized_intent_v1", lambda **kwargs: {"status": "PASS", "package_path": str(package_path), "build_path": str(package_path.parent / "execution_build.v1.json")})

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert risk["validation_status"] == "PASS"
    assert result["status"] == "PASS"
    assert result["risk_contract_status"] == "PASS"
    assert result["risk_contract_hash"]
    assert result["risk_measure"] == "MAX_STOP_LOSS_ESTIMATE_USD"



def test_missing_candidate_identity_set_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "CANDIDATE_IDENTITY_SET_MISSING"


def test_wrong_runtime_hash_candidate_identity_set_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    _seed_valid_candidate_identity(root)
    path = root / "reports" / "candidate_identity_set_v1" / DAY / INTENT_HASH / "candidate_identity_set.v1.json"
    payload = json.loads(path.read_text())
    payload["validation_status"] = "REJECTED"
    payload["blocker_codes"] = ["RUNTIME_HASH_MISMATCH"]
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "CANDIDATE_IDENTITY_SET_MISMATCH"


def test_stale_selected_intent_pointer_candidate_identity_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    _seed_valid_candidate_identity(root)
    path = root / "reports" / "candidate_identity_set_v1" / DAY / INTENT_HASH / "candidate_identity_set.v1.json"
    payload = json.loads(path.read_text())
    payload["validation_status"] = "REJECTED"
    payload["blocker_codes"] = ["SELECTED_INTENT_POINTER_MISMATCH"]
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "SELECTED_INTENT_POINTER_MISMATCH"

def test_wrong_runtime_hash_risk_contract_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    path = root / "risk_definition_contract_v1" / DAY / INTENT_HASH / "risk_definition_contract.v1.json"
    payload = json.loads(path.read_text())
    payload["runtime_evaluation_hash"] = "0" * 64
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "RISK_DEFINITION_CONTRACT_INVALID"
    assert result["risk_contract_status"] == "INVALID"


def test_wrong_construction_hash_risk_contract_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    path = root / "risk_definition_contract_v1" / DAY / INTENT_HASH / "risk_definition_contract.v1.json"
    payload = json.loads(path.read_text())
    payload["construction_contract_hash"] = "1" * 64
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "RISK_DEFINITION_CONTRACT_INVALID"


def test_wrong_allocation_hash_risk_contract_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    path = root / "risk_definition_contract_v1" / DAY / INTENT_HASH / "risk_definition_contract.v1.json"
    payload = json.loads(path.read_text())
    payload["capital_allocation_hash"] = "2" * 64
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "RISK_DEFINITION_CONTRACT_INVALID"


def test_risk_limit_exceeded_contract_blocks_conversion(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    runtime_hash = _seed_runtime(root)
    _seed_construction(root, runtime_hash)
    build_and_write_capital_authority_allocation_v1(truth_root=root, day_utc=DAY, emit_events=False)
    _seed_valid_governed_risk(root)
    path = root / "risk_definition_contract_v1" / DAY / INTENT_HASH / "risk_definition_contract.v1.json"
    payload = json.loads(path.read_text())
    payload["validation_status"] = "FAIL"
    payload["blockers"] = ["RISK_LIMIT_EXCEEDED"]
    payload["requested_risk"] = "999.00"
    payload["risk_measure_definition"]["requested_risk"] = "999.00"
    payload["risk_measure_definition"]["validation_status"] = "BLOCKED"
    _write(path, payload)

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["blocker_code"] == "RISK_LIMIT_EXCEEDED"

def test_raw_exposure_intent_is_not_submitted(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _seed_symbol_authority(root)
    _seed_market(root)
    calls = []

    def fake_submit(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("submit should not be called without package and submission record")

    monkeypatch.setattr(tool.subprocess, "run", fake_submit)
    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=True)

    assert result["status"] == "BLOCKED"
    assert result["blocker_code"] == "CAPITAL_AUTHORITY_ALLOCATION_MISSING"
    assert result["paper_submit_attempted"] is False
    assert calls == []


def test_symbol_authority_deprecated_fallback_is_not_used(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_selected(root)
    _write(root / "market_data_snapshot_v1" / "dataset_manifest.json", {"symbols": ["SPY", "QQQ", "IWM"]})

    result = tool.convert_selected_exposure_intent_to_paper_submission_v1(day_utc=DAY, truth_root=root, attempt_paper_submit=False)

    assert result["status"] == "BLOCKED"
    assert result["blocker_code"] == "SYMBOL_AUTHORITY_EVIDENCE_MISSING"
