from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis import sleeve_universe_validation_v1 as suv


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _base_truth(tmp_path: Path, monkeypatch) -> Path:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    engine_registry = repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
    policy_registry = repo / "governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json"
    _write_json(
        engine_registry,
        {
            "engines": [
                {"engine_id": "CURATED", "activation_status": "ACTIVE", "allowed_symbols": ["SPY"]},
                {"engine_id": "DYNAMIC", "activation_status": "ACTIVE", "allowed_symbols": ["QQQ"]},
            ]
        },
    )
    _write_json(
        policy_registry,
        {
            "policies": [
                {
                    "engine_id": "CURATED",
                    "universe_mode": "CURATED_SYMBOLS",
                    "symbol_source_class": "GOVERNED_CURATED",
                    "same_day_symbol_basis_required": False,
                    "curated_symbols": ["SPY", "QQQ"],
                },
                {
                    "engine_id": "DYNAMIC",
                    "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                    "symbol_source_class": "DYNAMIC_SAME_DAY",
                    "same_day_symbol_basis_required": True,
                    "curated_symbols": [],
                },
            ]
        },
    )
    monkeypatch.setattr(suv, "ENGINE_REGISTRY_PATH", engine_registry)
    monkeypatch.setattr(suv, "ENGINE_UNIVERSE_POLICY_PATH", policy_registry)
    _write_json(
        truth / "reports/aegis_runtime_truth_kernel_v1/2026-05-20/runtime_evaluation.v1.json",
        {
            "day_utc": "2026-05-20",
            "deterministic_output_hash": "hash",
            "runtime_truth_classification": "REAL_RUNTIME",
            "highest_readiness_layer": "DATA_READY",
            "capabilities": {"TRADE_ADVICE_ALLOWED": False, "AUTONOMOUS_EXECUTION_ALLOWED": False},
        },
    )
    _write_json(
        truth / "reports/aegis_sleeve_readiness_v1/2026-05-20/sleeve_readiness.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "CURATED", "readiness": "READY", "blocking_inputs": [], "warning_inputs": []},
                {"sleeve_id": "DYNAMIC", "readiness": "READY", "blocking_inputs": [], "warning_inputs": []},
            ]
        },
    )
    _write_json(
        truth / "reports/engine_universe_candidate_basis_v1/2026-05-20/DYNAMIC/engine_universe_candidate_basis.v1.json",
        {"schema_id": "engine_universe_candidate_basis_v1", "day_utc": "2026-05-20", "candidate_symbols": ["AAPL", "MSFT"]},
    )
    return truth


def test_sleeve_universe_validation_accepts_curated_and_dynamic_sources(tmp_path, monkeypatch) -> None:
    truth = _base_truth(tmp_path, monkeypatch)
    dynamic_path = truth / "reports/engine_universe_candidate_basis_v1/2026-05-20/DYNAMIC/engine_universe_candidate_basis.v1.json"
    _write_json(
        truth / "reports/sleeve_evaluation_kernel_v1/2026-05-20/sleeve_evaluation_rollup.v1.json",
        {
            "status": "READY",
            "canonical_blocker": "",
            "outcomes": [
                {
                    "engine_id": "CURATED",
                    "status": "NO_INTENT",
                    "symbol_source": "ENGINE_UNIVERSE_POLICY_V1.curated_symbols",
                    "symbol_source_path": str(suv.ENGINE_UNIVERSE_POLICY_PATH),
                    "allowed_symbols": ["QQQ", "SPY", "SPY"],
                    "deprecated_symbol_fallback_used": False,
                    "stale_artifact_detected": False,
                },
                {
                    "engine_id": "DYNAMIC",
                    "status": "NO_INTENT",
                    "symbol_source": "engine_universe_candidate_basis_v1",
                    "symbol_source_path": str(dynamic_path),
                    "allowed_symbols": ["MSFT", "AAPL"],
                    "deprecated_symbol_fallback_used": False,
                    "stale_artifact_detected": False,
                },
            ],
        },
    )

    payload = suv.build_sleeve_universe_validation_v1(truth_root=truth, day_utc="2026-05-20", generated_at_utc="2026-05-20T12:00:00Z")

    assert payload["overall_status"] == "PASS"
    assert payload["fallback_or_default_usage_count"] == 0
    assert payload["stale_source_count"] == 0
    assert {row["engine_id"]: row["validation_status"] for row in payload["sleeves"]} == {"CURATED": "PASS", "DYNAMIC": "PASS"}


def test_sleeve_universe_validation_rejects_default_fallback(tmp_path, monkeypatch) -> None:
    truth = _base_truth(tmp_path, monkeypatch)
    _write_json(
        truth / "reports/sleeve_evaluation_kernel_v1/2026-05-20/sleeve_evaluation_rollup.v1.json",
        {
            "status": "READY",
            "canonical_blocker": "",
            "outcomes": [
                {
                    "engine_id": "CURATED",
                    "status": "NO_INTENT",
                    "symbol_source": "ENGINE_MODEL_REGISTRY_V1.allowed_symbols",
                    "symbol_source_path": str(suv.ENGINE_REGISTRY_PATH),
                    "allowed_symbols": ["SPY"],
                    "deprecated_symbol_fallback_used": False,
                    "stale_artifact_detected": False,
                }
            ],
        },
    )

    payload = suv.build_sleeve_universe_validation_v1(truth_root=truth, day_utc="2026-05-20", generated_at_utc="2026-05-20T12:00:00Z")

    assert payload["overall_status"] == "FAIL"
    assert payload["fallback_or_default_usage_count"] == 1
    assert payload["sleeves"][0]["validation_status"] == "FAIL"
