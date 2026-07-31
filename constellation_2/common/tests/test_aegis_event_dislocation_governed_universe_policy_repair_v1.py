from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.candidate_contracts_v1 import REQUIRED_FIELDS, _is_pre_contract_suppressed_signal
from ops.aegis.event_dislocation_governed_universe_policy_repair_v1 import (
    UNGOVERNED_SYMBOL_SUPPRESSED,
    build_event_dislocation_governed_universe_policy_repair_v1,
    write_event_dislocation_governed_universe_policy_repair_v1,
)

DAY = "2026-06-02"
SLEEVE = "C2_EVENT_DISLOCATION_V1"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _repo(tmp_path: Path, *, allowed_symbols: list[str] | None = None, event_policy: bool = True) -> Path:
    repo = tmp_path / "repo"
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {"engines": [{"engine_id": SLEEVE, "allowed_symbols": allowed_symbols if allowed_symbols is not None else ["GLD"]}]},
    )
    policies = []
    if event_policy:
        policies.append(
            {
                "engine_id": SLEEVE,
                "exposure_requirements": {"allowed_symbols": ["GLD"], "exposure_type": "LONG_EQUITY"},
                "structure_template": {"structure_type": "EQUITY_SPOT", "allowed_action": "BUY"},
            }
        )
    _write_json(repo / "governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json", {"engine_policies": policies})
    return repo


def _seed_t07a(root: Path, *, policy_before: bool = False) -> None:
    _write_json(
        root / "reports/aegis_event_dislocation_governance_trace_diagnostic_v1" / DAY / "event_dislocation_governance_trace_diagnostic.v1.json",
        {"representative_rejected_candidate": {"instrument_registry_lookup_result": {"structure_policy_found": policy_before}}},
    )


def _seed_runtime(root: Path, *, symbol: str = "CIFR", suppressed: bool = True, created: bool = False, rejected: bool = False) -> None:
    signal = {
        "raw_signal_id": f"sig-{symbol}",
        "intent_id": f"sig-{symbol}",
        "sleeve_id": SLEEVE,
        "symbol": symbol,
        "direction": "LONG" if symbol == "GLD" and not suppressed else "",
        "instrument_type": "LONG_EQUITY" if symbol == "GLD" and not suppressed else "",
        "governance_status": "GOVERNED" if symbol == "GLD" and not suppressed else "",
        "candidate_contract_status": "SUPPRESSED" if suppressed else "VALID",
        "pre_contract_suppression_reason": UNGOVERNED_SYMBOL_SUPPRESSED if suppressed else "",
        "rejection_stage": "PRE_CONTRACT_GOVERNED_UNIVERSE" if suppressed else "",
        "symbol_governance_status": "UNGOVERNED" if suppressed else "GOVERNED",
        "governed_symbols": ["GLD"],
        "governed_universe_source": "ENGINE_MODEL_REGISTRY_V1:C2_EVENT_DISLOCATION_V1.allowed_symbols",
        "evidence_path": str(root / "intent.json"),
        "missing_candidate_fields": [],
    }
    contracts = []
    rejected_rows = []
    suppressed_rows = []
    if created:
        contracts.append(
            {
                "raw_signal_id": f"sig-{symbol}",
                "intent_id": f"sig-{symbol}",
                "candidate_id": "candidate-gld",
                "sleeve_id": SLEEVE,
                "symbol": symbol,
                "direction": "LONG",
                "instrument_type": "LONG_EQUITY",
                "governance_status": "GOVERNED",
                "contract_validation_status": "VALID",
            }
        )
    if rejected:
        rejected_rows.append(
            {
                "raw_signal_id": f"sig-{symbol}",
                "intent_id": f"sig-{symbol}",
                "candidate_id": "candidate-rejected",
                "sleeve_id": SLEEVE,
                "symbol": symbol,
                "direction": "",
                "instrument_type": "",
                "governance_status": "",
                "contract_validation_status": "REJECTED",
                "rejection_reason": "INSTRUMENT_TYPE_NOT_GOVERNED",
            }
        )
    if suppressed:
        suppressed_rows.append(
            {
                "raw_signal_id": f"sig-{symbol}",
                "intent_id": f"sig-{symbol}",
                "sleeve_id": SLEEVE,
                "symbol": symbol,
                "contract_validation_status": "NOT_SUBMITTED_PRE_CONTRACT_SUPPRESSION",
                "pre_contract_suppression_reason": UNGOVERNED_SYMBOL_SUPPRESSED,
                "rejection_stage": "PRE_CONTRACT_GOVERNED_UNIVERSE",
                "candidate_contract_input_created": False,
            }
        )
    _write_json(root / "reports/aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json", {"signals": [signal]})
    _write_json(
        root / "reports/aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {"candidate_contracts": contracts, "rejected_raw_signals": rejected_rows, "pre_contract_suppressed_raw_signals": suppressed_rows},
    )


def _build(root: Path, repo: Path) -> dict:
    return build_event_dislocation_governed_universe_policy_repair_v1(
        truth_root=root,
        repo_root=repo,
        day_utc=DAY,
        rebuild_runtime_evidence=False,
    )


def test_non_governed_signal_symbol_is_suppressed_before_candidate_contract_validation(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    payload = _build(root, repo)

    assert payload["repair_status"] == "REPAIRED"
    assert payload["candidate_filtering_applied"] is True
    assert payload["suppressed_ungoverned_signal_count"] == 1
    assert payload["candidate_attempt_count_after_repair"] == 0
    assert payload["candidate_rejection_count_after_repair"] == 0


def test_governed_signal_symbol_reaches_candidate_contract_validation(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="GLD", suppressed=False, created=True)

    payload = _build(root, repo)

    assert payload["candidate_attempt_count_after_repair"] == 1
    assert payload["candidate_count_after_repair"] == 1
    assert payload["candidate_ids_after_repair"] == ["candidate-gld"]
    assert payload["post_repair_t06_status"] == "CANDIDATE_CONTRACT_VALID"


def test_candidate_instrument_type_is_derived_from_governed_instrument_metadata(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="GLD", suppressed=False, created=True)

    payload = _build(root, repo)

    assert payload["normalized_instrument_type_source"] == "C2_EQUITY_STRUCTURE_POLICY_V1:C2_EVENT_DISLOCATION_V1.exposure_requirements.exposure_type"


def test_candidate_direction_is_derived_from_event_dislocation_policy(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="GLD", suppressed=False, created=True)

    payload = _build(root, repo)

    assert payload["normalized_direction_source"] == "C2_EQUITY_STRUCTURE_POLICY_V1:C2_EVENT_DISLOCATION_V1.structure_template.allowed_action"


def test_candidate_governance_status_is_derived_from_governed_policy(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="GLD", suppressed=False, created=True)

    payload = _build(root, repo)

    assert payload["normalized_governance_status_source"] == "C2_EQUITY_STRUCTURE_POLICY_V1:C2_EVENT_DISLOCATION_V1"


def test_ungoverned_cifr_like_symbol_is_not_allowed_by_broad_allowlist(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path, allowed_symbols=["GLD"])
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    payload = _build(root, repo)

    assert "CIFR" in payload["ungoverned_symbols_before_repair"]
    assert "CIFR" not in payload["governed_symbols_before_repair"]
    assert payload["broad_allowlist_introduced"] is False


def test_no_candidate_is_created_for_ungoverned_symbol(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    payload = _build(root, repo)

    assert payload["candidate_count_after_repair"] == 0
    assert payload["candidate_ids_after_repair"] == []


def test_instrument_not_in_governed_registry_is_eliminated_when_suppression_is_correct(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    payload = _build(root, repo)

    assert payload["rejection_reasons_after_repair"] == []
    assert payload["post_repair_t07a_status"] == "NO_CONTRACT_REJECTIONS"


def test_contract_strictness_remains_unchanged() -> None:
    assert "instrument_type" in REQUIRED_FIELDS
    assert "direction" in REQUIRED_FIELDS
    assert "governance_status" in REQUIRED_FIELDS


def test_strategy_thresholds_and_scoring_remain_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    payload = _build(root, repo)

    assert payload["strategy_thresholds_changed"] is False
    assert payload["candidate_scoring_changed"] is False
    assert payload["risk_policy_changed"] is False


def test_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    payload = _build(root, repo)

    assert payload["safety_gates_unchanged"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["live_trading_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_candidate_contract_suppression_marker_is_recognized() -> None:
    assert _is_pre_contract_suppressed_signal(
        {
            "candidate_contract_status": "SUPPRESSED",
            "pre_contract_suppression_reason": UNGOVERNED_SYMBOL_SUPPRESSED,
            "rejection_stage": "PRE_CONTRACT_GOVERNED_UNIVERSE",
        }
    )


def test_repair_artifact_writer_writes_required_path(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _seed_t07a(root)
    _seed_runtime(root, symbol="CIFR", suppressed=True)

    path = write_event_dislocation_governed_universe_policy_repair_v1(
        truth_root=root,
        repo_root=repo,
        day_utc=DAY,
        payload=_build(root, repo),
    )

    assert path.exists()
    assert path.name == "event_dislocation_governed_universe_policy_repair.v1.json"
