from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.event_dislocation_candidate_suppression_diagnostics_v1 import (
    build_event_dislocation_candidate_suppression_diagnostics_v1,
    write_event_dislocation_candidate_suppression_diagnostics_v1,
)

DAY = "2026-06-02"
SLEEVE = "C2_EVENT_DISLOCATION_V1"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _repo(tmp_path: Path, *, policy: bool = True) -> Path:
    repo = tmp_path / "repo"
    if policy:
        _write_json(repo / "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json", {"sleeves": [{ "sleeve_id": SLEEVE }]})
        _write_json(repo / "governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json", {"sleeves": [{ "sleeve_id": SLEEVE }]})
        _write_json(repo / "governance/02_REGISTRIES/C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json", {"sleeves": [{ "sleeve_id": SLEEVE }]})
    return repo


def _seed_signal(root: Path, *, signal_id: str = "sig-1", symbol: str = "CIFR", extra: dict | None = None) -> dict:
    row = {
        "raw_signal_id": signal_id,
        "sleeve_id": SLEEVE,
        "symbol": symbol,
        "signal_type": "exposure_intent",
        "evidence_path": str(root / "intent.json"),
        "intent_hash": "abc",
    }
    row.update(extra or {})
    _write_json(root / "reports/aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json", {"signals": [row]})
    return row


def _seed_manifest(root: Path, *, status: str = "CANDIDATE_CREATED", reason: str = "", signal_id: str = "sig-1") -> None:
    _write_json(
        root / "reports/candidate_generation_manifest_v1" / DAY / "run-1" / "candidate_generation_manifest.v1.json",
        {
            "candidate_rows": [
                {
                    "engine_id": SLEEVE,
                    "candidate_id": "attempt-1",
                    "status": status,
                    "raw_intent_id": signal_id,
                    "symbol_or_pair": "CIFR",
                    "rejection_reason": reason,
                    "reason_codes": [reason] if reason else [],
                }
            ]
        },
    )


def _seed_contract_rejection(root: Path, *, reason: str = "INSTRUMENT_TYPE_NOT_GOVERNED") -> None:
    _write_json(
        root / "reports/aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {
            "candidate_contracts": [],
            "rejected_raw_signals": [
                {
                    "sleeve_id": SLEEVE,
                    "raw_signal_id": "sig-1",
                    "candidate_id": "contract-1",
                    "symbol": "CIFR",
                    "contract_validation_status": "REJECTED",
                    "rejection_reason": reason,
                    "missing_contract_fields": ["candidate.direction", "candidate.instrument_type", "candidate.governance_status"],
                    "risk_policy_source_path": "/repo/governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json",
                }
            ],
        },
    )


def _build(root: Path, repo: Path) -> dict:
    return build_event_dislocation_candidate_suppression_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)


def test_signals_present_but_candidate_builder_not_invoked(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)

    payload = _build(root, repo)

    assert payload["raw_signal_count"] == 1
    assert payload["candidate_generation_invoked"] is False
    assert payload["suppression_code"] == "SIGNALS_PRESENT_BUT_CANDIDATE_BUILDER_NOT_INVOKED"


def test_signal_schema_invalid(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _write_json(root / "reports/aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json", {"signals": [{"sleeve_id": SLEEVE, "symbol": "CIFR"}]})

    payload = _build(root, repo)

    assert payload["signal_validation_status"] == "SCHEMA_INVALID"
    assert payload["suppression_code"] == "SIGNAL_SCHEMA_INVALID"
    assert "raw_signal_id" in payload["missing_fields"] or "raw_signal_id" in payload["suppression_reason"]


def test_signal_confidence_below_minimum(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root, extra={"confidence": 0.2, "minimum_confidence": 0.5})

    payload = _build(root, repo)

    assert payload["suppression_code"] == "SIGNAL_CONFIDENCE_BELOW_MINIMUM"
    assert payload["rejected_by_confidence_gate"] is True


def test_candidate_construction_policy_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, policy=False)
    _seed_signal(root)
    _seed_manifest(root)

    payload = _build(root, repo)

    assert payload["candidate_generation_invoked"] is True
    assert payload["suppression_code"] == "CANDIDATE_CONSTRUCTION_POLICY_MISSING"
    assert payload["post_diagnostic_status"] == "BLOCKED_BY_POLICY"


def test_candidate_construction_fails_with_precise_reason(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root, status="FAILED", reason="CANDIDATE_BUILD_EXCEPTION")

    payload = _build(root, repo)

    assert payload["suppression_code"] == "CANDIDATE_CONSTRUCTION_FAILED"
    assert "CANDIDATE_BUILD_EXCEPTION" in payload["suppression_reason"]


def test_candidate_contract_rejected_with_precise_reason(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root)
    _seed_contract_rejection(root)

    payload = _build(root, repo)

    assert payload["candidate_generation_status"] == "CONTRACT_REJECTED"
    assert payload["suppression_code"] == "CANDIDATE_CONTRACT_REJECTED"
    assert payload["rejected_by_contract"] is True
    assert "candidate.instrument_type" in payload["missing_fields"]


def test_duplicate_suppression(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root, status="BLOCKED", reason="DUPLICATE_SUPPRESSED")

    payload = _build(root, repo)

    assert payload["suppression_code"] == "DUPLICATE_SUPPRESSED"
    assert payload["rejected_by_duplicate_control"] is True


def test_cooldown_suppression(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root, status="BLOCKED", reason="COOLDOWN_ACTIVE")

    payload = _build(root, repo)

    assert payload["suppression_code"] == "COOLDOWN_ACTIVE"
    assert payload["rejected_by_cooldown"] is True


def test_exposure_limit_suppression(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root, status="BLOCKED", reason="EXPOSURE_LIMIT_REACHED")

    payload = _build(root, repo)

    assert payload["suppression_code"] == "EXPOSURE_LIMIT_REACHED"
    assert payload["rejected_by_exposure_limit"] is True


def test_data_quality_gate_suppression(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root, status="BLOCKED", reason="DATA_QUALITY_GATE_FAILED")

    payload = _build(root, repo)

    assert payload["suppression_code"] == "DATA_QUALITY_GATE_FAILED"
    assert payload["rejected_by_data_quality_gate"] is True


def test_successful_diagnostic_does_not_fabricate_candidates(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_manifest(root)
    path = write_event_dislocation_candidate_suppression_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["candidate_count"] == 0
    assert payload["candidate_ids"] == []
    assert payload["no_candidate_fabrication"] is True


def test_strategy_thresholds_and_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_signal(root)
    payload = _build(root, repo)

    assert payload["no_strategy_logic_mutation"] is True
    assert payload["no_threshold_mutation"] is True
    assert payload["no_candidate_scoring_mutation"] is True
    assert payload["no_risk_policy_mutation"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["live_trading_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
