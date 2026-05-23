from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.engine_activity_authorization_v1 import build_and_write_engine_activity_authorization_v1


DAY = "2026-05-20"
RUNTIME_HASH = "a" * 64
INTENT_HASH = "4f60b5008c648dbf1df2524cd06f78be5cbf7e80e5ce1ef7ccae40bc9cc50960"
ENGINE_ID = "C2_MEAN_REVERSION_EQ_V1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed(root: Path, repo_root: Path, *, identity_runtime_hash: str = RUNTIME_HASH, engine_active: bool = True) -> Path:
    repo_root.mkdir(parents=True, exist_ok=True)
    runner = repo_root / "runner.py"
    runner.write_text("print('runner')\n", encoding="utf-8")
    runner_hash = hashlib.sha256(runner.read_bytes()).hexdigest()
    registry = repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
    _write(registry, {
        "engines": [{
            "engine_id": ENGINE_ID,
            "activation_status": "ACTIVE" if engine_active else "DISABLED",
            "engine_runner_path": "runner.py",
            "engine_runner_sha256": runner_hash,
        }]
    })
    runtime = root / f"reports/aegis_runtime_truth_kernel_v1/{DAY}/runtime_evaluation.v1.json"
    _write(runtime, {"deterministic_output_hash": RUNTIME_HASH})
    intent = root.parent / f"truth_sleeves/PRIMARY/PAPER/intents_v1/snapshots/{DAY}/{INTENT_HASH}.exposure_intent.v1.json"
    _write(intent, {"intent_id": "intent-1", "engine_id": ENGINE_ID, "symbol": "DOW"})
    _write(root / "pointers/selected_intent_pointer.v1.json", {
        "day_utc": DAY,
        "status": "SELECTED",
        "selected_intent": {
            "intent_hash": INTENT_HASH,
            "intent_id": "intent-1",
            "intent_path": str(intent),
            "engine_id": ENGINE_ID,
            "symbol": "DOW",
        },
    })
    _write(root / f"reports/candidate_identity_set_v1/{DAY}/{INTENT_HASH}/candidate_identity_set.v1.json", {
        "day_utc": DAY,
        "validation_status": "VALID",
        "runtime_evaluation_hash": identity_runtime_hash,
        "intent_hash": INTENT_HASH,
        "candidate_id": "intent-1",
        "exposure_id": "intent-1",
        "sleeve_id": ENGINE_ID,
        "symbol": "DOW",
        "side": "BUY",
    })
    _write(root / f"reports/aegis_sleeve_readiness_v1/{DAY}/sleeve_readiness.v1.json", {
        "day_utc": DAY,
        "sleeves": [{"sleeve_id": ENGINE_ID, "readiness": "READY_WITH_WARNINGS"}],
    })
    _write(root / f"reports/market_data_inputs_v1/{DAY}/market_data_inputs.v1.json", {
        "day_utc": DAY,
        "validation_status": "VALID",
    })
    return registry


def test_valid_authorization_clears_gate_without_execution_permissions(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo_root = tmp_path / "repo"
    registry = _seed(root, repo_root)

    payload, path = build_and_write_engine_activity_authorization_v1(
        truth_root=root,
        day_utc=DAY,
        intent_hash=INTENT_HASH,
        repo_root=repo_root,
        engine_registry_path=registry,
        emit_events=False,
    )

    assert path.exists()
    assert payload["validation_status"] == "VALID"
    assert payload["authorization_status"] == "AUTHORIZED"
    assert payload["broker_submit_transmit_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False


def test_stale_candidate_identity_runtime_hash_blocks(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo_root = tmp_path / "repo"
    registry = _seed(root, repo_root, identity_runtime_hash="b" * 64)

    payload, _path = build_and_write_engine_activity_authorization_v1(
        truth_root=root,
        day_utc=DAY,
        intent_hash=INTENT_HASH,
        repo_root=repo_root,
        engine_registry_path=registry,
        emit_events=False,
    )

    assert payload["validation_status"] == "REJECTED"
    assert payload["authorization_status"] == "STALE"
    assert "RUNTIME_HASH_MISMATCH" in payload["blocker_codes"]


def test_disabled_engine_blocks(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo_root = tmp_path / "repo"
    registry = _seed(root, repo_root, engine_active=False)

    payload, _path = build_and_write_engine_activity_authorization_v1(
        truth_root=root,
        day_utc=DAY,
        intent_hash=INTENT_HASH,
        repo_root=repo_root,
        engine_registry_path=registry,
        emit_events=False,
    )

    assert payload["validation_status"] == "REJECTED"
    assert payload["authorization_status"] == "POLICY_DISABLED"
    assert "ENGINE_POLICY_DISABLED" in payload["blocker_codes"]
