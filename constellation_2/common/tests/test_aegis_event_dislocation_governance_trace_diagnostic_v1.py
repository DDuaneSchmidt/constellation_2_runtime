from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.event_dislocation_governance_trace_diagnostic_v1 import (
    build_event_dislocation_governance_trace_diagnostic_v1,
    write_event_dislocation_governance_trace_diagnostic_v1,
)

DAY = "2026-06-02"
SLEEVE = "C2_EVENT_DISLOCATION_V1"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _repo(tmp_path: Path, *, event_policy: bool = False, allowed_symbols: list[str] | None = None) -> Path:
    repo = tmp_path / "repo"
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {"engines": [{"engine_id": SLEEVE, "allowed_symbols": allowed_symbols if allowed_symbols is not None else ["CIFR"]}]},
    )
    policies = []
    if event_policy:
        policies.append(
            {
                "engine_id": SLEEVE,
                "structure_template": {"structure_type": "EQUITY_SPOT", "allowed_action": "BUY"},
                "exposure_requirements": {"exposure_type": "LONG_EQUITY"},
            }
        )
    _write_json(repo / "governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json", {"engine_policies": policies})
    _write_json(repo / "governance/02_REGISTRIES/C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json", {"engine_policies": []})
    _write_json(repo / "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json", {"policies": {SLEEVE: {"stop_loss_bps_default": 100}}})
    return repo


def _seed(
    root: Path,
    *,
    raw_fields: dict | None = None,
    signal_fields: dict | None = None,
    contract_fields: dict | None = None,
    missing: list[str] | None = None,
    reason: str = "INSTRUMENT_TYPE_NOT_GOVERNED",
) -> Path:
    raw_path = _write_json(
        root / "truth_sleeves/PRIMARY/PAPER/intents_v1/snapshots" / DAY / "intent.json",
        {
            "intent_id": "sig-1",
            "sleeve_id": SLEEVE,
            "symbol": "CIFR",
            **(raw_fields or {}),
        },
    )
    signal = {
        "raw_signal_id": "sig-1",
        "intent_id": "sig-1",
        "sleeve_id": SLEEVE,
        "symbol": "CIFR",
        "signal_type": "exposure_intent",
        "evidence_path": str(raw_path),
        "direction": "",
        "instrument_type": "",
        "governance_status": "",
        "missing_candidate_fields": missing if missing is not None else ["candidate.direction", "candidate.instrument_type", "candidate.governance_status"],
        **(signal_fields or {}),
    }
    contract = {
        "raw_signal_id": "sig-1",
        "intent_id": "sig-1",
        "candidate_id": "candidate-1",
        "sleeve_id": SLEEVE,
        "symbol": "CIFR",
        "direction": "",
        "instrument_type": "",
        "governance_status": "",
        "contract_validation_status": "REJECTED",
        "missing_contract_fields": missing if missing is not None else ["candidate.direction", "candidate.instrument_type", "candidate.governance_status"],
        "rejection_reason": reason,
        **(contract_fields or {}),
    }
    _write_json(root / "reports/aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json", {"signals": [signal]})
    return _write_json(root / "reports/aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json", {"candidate_contracts": [], "rejected_raw_signals": [contract]})


def _build(root: Path, repo: Path) -> dict:
    return build_event_dislocation_governance_trace_diagnostic_v1(truth_root=root, repo_root=repo, day_utc=DAY)


def test_missing_signal_metadata_is_classified_as_field_missing_from_signal(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed(root)

    payload = _build(root, repo)

    assert payload["classification"] == "FIELD_MISSING_FROM_SIGNAL"
    trace = payload["candidate_traces"][0]
    assert trace["candidate_instrument_type"] == ""
    assert "candidate.instrument_type" in trace["missing_fields"]


def test_present_signal_metadata_not_mapped_to_candidate_is_classified(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, event_policy=True)
    _seed(
        root,
        raw_fields={"direction": "LONG", "instrument_type": "LONG_EQUITY", "governance_status": "GOVERNED"},
        signal_fields={"direction": "LONG", "instrument_type": "LONG_EQUITY", "governance_status": "GOVERNED"},
    )

    payload = _build(root, repo)

    assert payload["classification"] == "FIELD_PRESENT_IN_SIGNAL_NOT_MAPPED_TO_CANDIDATE"


def test_invalid_instrument_type_is_classified(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, event_policy=True)
    _seed(
        root,
        signal_fields={"direction": "LONG", "instrument_type": "CRYPTO_SWAP", "governance_status": "GOVERNED", "missing_candidate_fields": []},
        contract_fields={"direction": "LONG", "instrument_type": "CRYPTO_SWAP", "governance_status": "GOVERNED", "missing_contract_fields": []},
        missing=[],
    )

    payload = _build(root, repo)

    assert payload["classification"] == "INSTRUMENT_TYPE_VALUE_NOT_GOVERNED"
    assert "candidate.instrument_type" in payload["candidate_traces"][0]["invalid_fields"]


def test_instrument_absent_from_governed_registry_is_classified(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, event_policy=True, allowed_symbols=["GLD"])
    _seed(
        root,
        signal_fields={"direction": "LONG", "instrument_type": "LONG_EQUITY", "governance_status": "GOVERNED", "missing_candidate_fields": []},
        contract_fields={"direction": "LONG", "instrument_type": "LONG_EQUITY", "governance_status": "GOVERNED", "missing_contract_fields": []},
        missing=[],
    )

    payload = _build(root, repo)

    assert payload["classification"] == "INSTRUMENT_NOT_IN_GOVERNED_REGISTRY"
    assert payload["candidate_traces"][0]["instrument_registry_lookup_result"]["symbol_in_engine_allowed_symbols"] is False


def test_candidate_contract_schema_mismatch_is_classified(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed(root, contract_fields={"missing_contract_fields": ["raw_signal_id"]}, missing=["raw_signal_id"])

    payload = _build(root, repo)

    assert payload["classification"] == "CONTRACT_SCHEMA_MISMATCH"


def test_diagnostic_does_not_repair_or_mutate_candidates(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    contracts_path = _seed(root)
    before = contracts_path.read_text(encoding="utf-8")

    write_event_dislocation_governance_trace_diagnostic_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    after = contracts_path.read_text(encoding="utf-8")

    assert before == after


def test_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed(root)

    payload = _build(root, repo)

    assert payload["no_repair_performed"] is True
    assert payload["no_candidate_mutation"] is True
    assert payload["no_strategy_logic_mutation"] is True
    assert payload["no_threshold_mutation"] is True
    assert payload["no_candidate_scoring_mutation"] is True
    assert payload["no_risk_policy_mutation"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["live_trading_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
