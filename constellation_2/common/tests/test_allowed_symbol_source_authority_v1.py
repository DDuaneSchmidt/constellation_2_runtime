from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_sleeve_evaluation_kernel_v1 as sleeve_kernel
from ops.aegis.universe.canonical_symbol_universe_resolver_v1 import CanonicalSymbolUniverseError, SymbolUniverseResolution

DAY = "2026-04-30"


def _script() -> Path:
    return Path("/home/node/constellation/constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py")


def _registry(symbols: list[str]) -> dict:
    return {
        "schema_id": "engine_model_registry",
        "schema_version": "v1",
        "engines": [
            {
                "engine_id": "ENGINE_A",
                "activation_status": "ACTIVE",
                "allowed_symbols": symbols,
                "engine_runner_path": str(_script().relative_to(Path("/home/node/constellation"))),
                "engine_runner_sha256": hashlib.sha256(_script().read_bytes()).hexdigest(),
            }
        ],
    }


def _resolution(symbols: list[str], source_path: Path) -> SymbolUniverseResolution:
    return SymbolUniverseResolution(
        engine_id="ENGINE_A",
        day_utc=DAY,
        symbols=symbols,
        symbol_count=len(symbols),
        source="engine_universe_candidate_basis_v1",
        source_path=str(source_path),
        source_rank="A",
        deprecated_fallback_used=False,
        deprecated_fallback_warning="",
        policy_universe_mode="LIQUIDITY_RANKED_SYMBOLS",
        policy_target_symbol_count=len(symbols),
        blockers=[],
        source_artifacts=[str(source_path)],
    )


def _write_manifest(root: Path, symbols: list[str]) -> None:
    path = root / "market_data_snapshot_v1" / "dataset_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"symbols": symbols, "files": [{"symbol": symbol} for symbol in symbols]}, sort_keys=True), encoding="utf-8")


def _write_intent(root: Path, symbol: str) -> None:
    path = root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "intent_id": f"engine_a_{symbol.lower()}",
                "engine": {"engine_id": "ENGINE_A"},
                "underlying": {"symbol": symbol, "currency": "USD"},
                "exposure_type": "LONG_EQUITY",
                "risk_class": "TEST",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_canonical_universe_valid_deprecated_registry_mismatch_warns_only(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    source_path = truth / "reports/engine_universe_candidate_basis_v1" / DAY / "ENGINE_A" / "engine_universe_candidate_basis.v1.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps({"day_utc": DAY, "candidate_symbols": ["IWM"], "status": "PASS"}), encoding="utf-8")
    _write_manifest(truth, ["IWM"])

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(["SPY"])):
        with patch.object(sleeve_kernel, "resolve_canonical_symbol_universe_v1", return_value=_resolution(["IWM"], source_path)):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=DAY, truth_root=truth, environment="PAPER", allow_deprecated_symbol_fallback=True)

    outcome = payload["outcomes"][0]
    assert payload["status"] == "PASS"
    assert outcome["allowed_symbols"] == ["IWM"]
    assert outcome["symbol_source"] == "engine_universe_candidate_basis_v1"
    assert outcome["deprecated_symbol_source_classification"] == "WARNING_ONLY"
    assert outcome["deprecated_registry_symbol_diff"]["symbols_only_in_deprecated_registry"] == ["SPY"]
    assert outcome["deprecated_registry_symbol_diff"]["symbols_missing_from_deprecated_registry"] == ["IWM"]
    report = json.loads(Path(payload["allowed_symbol_source_report_path"]).read_text())
    assert report["status"] == "PASS"
    assert report["deprecated_warning_sleeves"] == ["ENGINE_A"]


def test_canonical_universe_missing_blocks_without_deprecated_fallback(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(["SPY"])):
        with patch.object(sleeve_kernel, "resolve_canonical_symbol_universe_v1", side_effect=CanonicalSymbolUniverseError("missing canonical universe")):
            payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=DAY, truth_root=truth, environment="PAPER", allow_deprecated_symbol_fallback=True)

    outcome = payload["outcomes"][0]
    assert payload["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE"
    assert outcome["allowed_symbols"] == []
    assert outcome["deprecated_symbol_fallback_used"] is False
    report = json.loads(Path(payload["allowed_symbol_source_report_path"]).read_text())
    assert report["canonical_unavailable_sleeves"] == ["ENGINE_A"]


def test_stale_canonical_universe_blocks_precisely(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    err = CanonicalSymbolUniverseError('{"error":"CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE","blockers":[{"blocker_type":"engine_candidate_basis_unusable","detail":"basis_day=2026-04-29"}]}')
    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(["SPY"])):
        with patch.object(sleeve_kernel, "resolve_canonical_symbol_universe_v1", side_effect=err):
            payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=DAY, truth_root=truth, environment="PAPER")
    outcome = payload["outcomes"][0]
    assert outcome["canonical_blocker"] == "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE"
    assert "basis_day=2026-04-29" in outcome["market_data_manifest_check"]["details"]


def test_unexpected_symbol_in_output_blocks_against_canonical_universe(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    source_path = truth / "reports/engine_universe_candidate_basis_v1" / DAY / "ENGINE_A" / "engine_universe_candidate_basis.v1.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps({"day_utc": DAY, "candidate_symbols": ["IWM"], "status": "PASS"}), encoding="utf-8")
    _write_manifest(truth, ["IWM"])
    _write_intent(truth, "SPY")

    with patch.object(sleeve_kernel, "_load_engine_registry", return_value=_registry(["SPY"])):
        with patch.object(sleeve_kernel, "resolve_canonical_symbol_universe_v1", return_value=_resolution(["IWM"], source_path)):
            with patch.object(
                sleeve_kernel,
                "_run_engine",
                return_value={"command": [], "return_code": 0, "stdout": '{"status":"NO_INTENT"}', "stderr": "", "started_at_utc": "", "completed_at_utc": ""},
            ):
                payload = sleeve_kernel.build_sleeve_evaluation_kernel(day_utc=DAY, truth_root=truth, environment="PAPER")

    outcome = payload["outcomes"][0]
    assert outcome["status"] == "BLOCKED"
    assert outcome["canonical_blocker"] == "ALLOWED_SYMBOL_MISMATCH"
    assert outcome["rejected_intents"][0]["symbol"] == "SPY"
    report = json.loads(Path(payload["allowed_symbol_source_report_path"]).read_text())
    assert report["unexpected_output_symbol_sleeves"] == ["ENGINE_A"]
