from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.candidate_observability_v1 import (  # noqa: E402
    candidate_generation_manifest_path_v1,
    sleeve_invocation_ledger_path_v1,
)
from ops.tools import run_intent_arbitration_v1 as arbitration  # noqa: E402
from ops.tools import run_portfolio_activation_gate_v1 as portfolio_gate  # noqa: E402
from ops.tools import run_portfolio_scoring_v1 as portfolio_scoring  # noqa: E402
from ops.tools import run_portfolio_state_v1 as portfolio_state  # noqa: E402
from ops.tools import run_sleeve_evaluation_kernel_v1 as sleeve_kernel  # noqa: E402


DAY = "2026-05-12"
ENGINES = [
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
]


def _script() -> Path:
    return REPO_ROOT / "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py"


def _registry() -> dict[str, object]:
    script = _script()
    return {
        "schema_id": "engine_model_registry",
        "schema_version": "v1",
        "engines": [
            {
                "engine_id": engine_id,
                "activation_status": "ACTIVE",
                "allowed_symbols": ["SPY"],
                "engine_runner_path": str(script.relative_to(REPO_ROOT)),
                "engine_runner_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
            }
            for engine_id in ENGINES
        ],
    }


def _write_manifest(root: Path, symbols: list[str]) -> None:
    md_root = root / "market_data_snapshot_v1"
    files = []
    for symbol in symbols:
        sym = symbol.strip().upper()
        path = md_root / sym / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"symbol":"%s","timestamp_utc":"2026-05-12T14:00:00Z","close":100}\n' % sym, encoding="utf-8")
        files.append({"symbol": sym, "file": f"{sym}/2026.jsonl", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "year": 2026})
    manifest = md_root / "dataset_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({"dataset_version": "v1", "symbols": sorted({item["symbol"] for item in files}), "files": files}, sort_keys=True), encoding="utf-8")


def _write_intent(root: Path, *, engine_id: str, symbol: str, suffix: str) -> Path:
    path = root / "intents_v1" / "snapshots" / DAY / f"{suffix}.exposure_intent.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": f"{engine_id.lower()}_{symbol.lower()}",
        "engine": {"engine_id": engine_id},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "LONG_EQUITY",
        "risk_class": "TREND",
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _write_portfolio_state(root: Path) -> Path:
    path = portfolio_state.portfolio_state_path(truth_root=root, day_utc=DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "portfolio_state",
                "day_utc": DAY,
                "status": "PASS",
                "regime": "TREND",
                "trend_strength": "HIGH",
                "volatility_regime": "NORMAL",
                "equity_beta_state": "HIGH",
                "artifact_path": str(path),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def test_sleeve_invocation_ledger_contains_all_seven_sleeves(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry()):
        with patch.object(
            sleeve_kernel,
            "_run_engine",
            return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
        ):
            sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=DAY, truth_root=truth, environment="PAPER")

    path = sleeve_invocation_ledger_path_v1(truth_root=truth, day_utc=DAY, run_id=f"sleeve_evaluation_kernel_v1:{DAY}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert {row["engine_id"] for row in payload["invocations"]} == set(ENGINES)
    assert all(row["symbol_or_pair"] == "SPY" for row in payload["invocations"])
    assert payload["execution_authority_granted"] is False
    assert payload["order_submission_attempted"] is False
    assert payload["trading_behavior_changed"] is False


def test_candidate_manifest_retains_generated_no_signal_blocked_and_suppressed_rows(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY", "QQQ"])
    trend = _write_intent(truth, engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", suffix="trend")
    cross = _write_intent(truth, engine_id="C2_CROSS_ASSET_TREND_V1", symbol="QQQ", suffix="cross")
    rollup = {
        "schema_id": "sleeve_evaluation_kernel_rollup",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "status": "BLOCKED",
        "canonical_blocker": "ENGINE_BLOCKED",
        "outcomes": [
            {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "trend", "intent_path": str(trend), "intent_hash": "b" * 64, "symbol": "SPY"}], "reason_codes": ["INTENT_OUTPUT_CREATED"]},
            {"engine_id": "C2_CROSS_ASSET_TREND_V1", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "cross", "intent_path": str(cross), "intent_hash": "a" * 64, "symbol": "QQQ"}], "reason_codes": ["INTENT_OUTPUT_CREATED"]},
            {"engine_id": "C2_MEAN_REVERSION_EQ_V1", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "status": "NO_INTENT", "reason_codes": ["NO_INTENT_DECLARED"], "producer_requested_symbols": ["SPY"]},
            {"engine_id": "C2_EVENT_DISLOCATION_V1", "sleeve_id": "C2_EVENT_DISLOCATION_V1", "status": "BLOCKED", "canonical_blocker": "MISSING_INPUTS", "reason_codes": ["MISSING_INPUTS"], "producer_requested_symbols": ["SPY"]},
        ],
    }
    rollup_path = sleeve_kernel.sleeve_evaluation_rollup_path(truth_root=truth, day_utc=DAY)
    rollup_path.parent.mkdir(parents=True, exist_ok=True)
    rollup_path.write_text(json.dumps(rollup, sort_keys=True), encoding="utf-8")
    _write_portfolio_state(truth)

    gate = portfolio_gate.build_portfolio_activation_gate_v1(day_utc=DAY, truth_root=truth, environment="PAPER", source_rollup_path=rollup_path)

    path = candidate_generation_manifest_path_v1(truth_root=truth, day_utc=DAY, run_id=f"sleeve_evaluation_kernel_v1:{DAY}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    statuses = {row["engine_id"]: row["status"] for row in payload["candidate_rows"]}
    assert statuses["C2_TREND_EQ_PRIMARY_V1"] == "CANDIDATE_CREATED"
    assert statuses["C2_CROSS_ASSET_TREND_V1"] == "SUPPRESSED"
    assert statuses["C2_MEAN_REVERSION_EQ_V1"] == "NO_SIGNAL"
    assert statuses["C2_EVENT_DISLOCATION_V1"] == "BLOCKED"
    assert any(row["rejection_reason"] == "PORTFOLIO_GATE_SUPPRESSED" for row in payload["candidate_rows"])
    assert gate["suppressed_or_signal_only_intents"]
    assert payload["selected_intent_pointer_authoritative"] is True
    assert payload["execution_authority_granted"] is False


def test_signal_only_candidate_is_retained(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY"])
    pair = _write_intent(truth, engine_id="C2_MARKET_NEUTRAL_SPREAD_V1", symbol="SPY", suffix="pair")
    rollup = {
        "schema_id": "sleeve_evaluation_kernel_rollup",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "status": "PASS",
        "outcomes": [
            {"engine_id": "C2_MARKET_NEUTRAL_SPREAD_V1", "sleeve_id": "C2_MARKET_NEUTRAL_SPREAD_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "pair", "intent_path": str(pair), "intent_hash": "c" * 64, "symbol": "SPY"}], "reason_codes": ["INTENT_OUTPUT_CREATED"]},
        ],
    }
    rollup_path = sleeve_kernel.sleeve_evaluation_rollup_path(truth_root=truth, day_utc=DAY)
    rollup_path.parent.mkdir(parents=True, exist_ok=True)
    rollup_path.write_text(json.dumps(rollup, sort_keys=True), encoding="utf-8")
    _write_portfolio_state(truth)

    portfolio_gate.build_portfolio_activation_gate_v1(day_utc=DAY, truth_root=truth, environment="PAPER", source_rollup_path=rollup_path)

    manifest = json.loads(candidate_generation_manifest_path_v1(truth_root=truth, day_utc=DAY, run_id=f"sleeve_evaluation_kernel_v1:{DAY}").read_text(encoding="utf-8"))
    assert manifest["candidate_rows"][0]["status"] == "SIGNAL_ONLY"
    assert manifest["candidate_rows"][0]["rejection_reason"] == "PORTFOLIO_GATE_SIGNAL_ONLY"


def test_arbitration_winner_and_selected_pointer_are_unchanged_by_observability(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_manifest(truth, ["SPY", "IWM"])
    trend = _write_intent(truth, engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", suffix="trend")
    vol = _write_intent(truth, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM", suffix="vol")
    rollup = {
        "schema_id": "sleeve_evaluation_kernel_rollup",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "status": "PASS",
        "outcomes": [
            {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "trend", "intent_path": str(trend), "intent_hash": "b" * 64, "symbol": "SPY"}]},
            {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "vol", "intent_path": str(vol), "intent_hash": "a" * 64, "symbol": "IWM"}]},
        ],
    }
    rollup_path = sleeve_kernel.sleeve_evaluation_rollup_path(truth_root=truth, day_utc=DAY)
    rollup_path.parent.mkdir(parents=True, exist_ok=True)
    rollup_path.write_text(json.dumps(rollup, sort_keys=True), encoding="utf-8")
    _write_portfolio_state(truth)
    gate = portfolio_gate.build_portfolio_activation_gate_v1(day_utc=DAY, truth_root=truth, environment="PAPER", source_rollup_path=rollup_path)
    scoring = portfolio_scoring.build_portfolio_scoring_v1(day_utc=DAY, truth_root=truth, environment="PAPER", source_rollup_path=rollup_path, portfolio_gate_path_arg=Path(gate["artifact_path"]))

    result = arbitration.build_intent_arbitration(day_utc=DAY, truth_root=truth, environment="PAPER", portfolio_gate_path=Path(gate["artifact_path"]), portfolio_scoring_path_arg=Path(scoring["artifact_path"]))
    pointer = json.loads(Path(result["selected_intent_pointer_path"]).read_text(encoding="utf-8"))
    manifest = json.loads(candidate_generation_manifest_path_v1(truth_root=truth, day_utc=DAY, run_id=f"sleeve_evaluation_kernel_v1:{DAY}").read_text(encoding="utf-8"))

    assert result["selected_intent"]["intent_id"] == pointer["selected_intent"]["intent_id"]
    assert result["selected_intent"]["intent_id"] == "trend"
    assert manifest["order_submission_attempted"] is False
    assert manifest["execution_authority_granted"] is False


def test_missing_observability_artifacts_do_not_create_execution_authority(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    ledger_path = sleeve_invocation_ledger_path_v1(truth_root=truth, day_utc=DAY, run_id=f"sleeve_evaluation_kernel_v1:{DAY}")
    manifest_path = candidate_generation_manifest_path_v1(truth_root=truth, day_utc=DAY, run_id=f"sleeve_evaluation_kernel_v1:{DAY}")

    assert not ledger_path.exists()
    assert not manifest_path.exists()
    assert not any(truth.rglob("broker_submission_record*.json"))
    assert not any(truth.rglob("submit_decision_trace.v1.json"))
