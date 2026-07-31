from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.operator_state.trade_candidate_contract_v1 import content_hash_file_v1
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1

DAY = "2026-05-26"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _write_json(
        repo / "governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json",
        {
            "engine_policies": [
                {
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "exposure_requirements": {"exposure_type": "LONG_EQUITY"},
                    "structure_template": {"allowed_action": "BUY", "structure_type": "EQUITY_SPOT"},
                }
            ]
        },
    )
    return repo


def _seed_signal(root: Path, *, intent_id: str = "intent_aapl") -> None:
    intent_path = root / "truth_sleeves/PRIMARY/PAPER/intents_v1/snapshots" / DAY / "hash-aapl.exposure_intent.v1.json"
    _write_json(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "intent_id": intent_id,
            "intent_hash": "hash-aapl",
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "suite": "C2_HYBRID_V1"},
            "exposure_type": "LONG_EQUITY",
            "constraints": {"max_risk_pct": "0.01"},
            "underlying": {"symbol": "AAPL"},
        },
    )
    sleeve_path = root / "reports/sleeve_evaluation_kernel_v1" / DAY / "C2_TREND_EQ_PRIMARY_V1" / "sleeve_evaluation.v1.json"
    payload = {
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "status": "BLOCKED",
        "current_status": "BLOCKED",
        "output_count": 1,
        "artifact_path": str(sleeve_path),
        "reason_codes": ["POSITION_STATE_STALE"],
        "lifecycle_reason_codes": ["UNCHANGED_SIGNAL"],
        "signal_state": {"state": "ACTIVE"},
        "exposure_intent_batch": {
            "output_intents": [
                {
                    "intent_id": intent_id,
                    "intent_hash": "hash-aapl",
                    "intent_path": str(intent_path),
                    "schema_id": "exposure_intent",
                    "symbol": "AAPL",
                }
            ]
        },
    }
    _write_json(sleeve_path, payload)
    _write_json(root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"day_utc": DAY, "outcomes": [payload]})


def _seed_registry(root: Path, *, include_registry: bool = True, validation_status: str = "VALID", input_validation_status: str = "VALID") -> None:
    market_inputs = root / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"
    _write_json(
        market_inputs,
        {
            "day_utc": DAY,
            "input_records": [
                {
                    "data_item_id": "market.price.AAPL",
                    "symbol": "AAPL",
                    "value": 201.25,
                    "field_type": "last_price",
                    "day_utc": DAY,
                    "source_timestamp_utc": "2026-05-26T16:00:00Z",
                    "source_vendor": "LOCAL_CACHE",
                    "validation_status": input_validation_status,
                }
            ],
        },
    )
    items = []
    if include_registry:
        items.append(
            {
                "data_item_id": "market.price.AAPL",
                "symbol": "AAPL",
                "status": "CURRENT",
                "market_session_date": DAY,
                "value": 201.25,
                "source_artifact_path": str(market_inputs),
                "source_hash": content_hash_file_v1(market_inputs),
                "provider": "LOCAL_CACHE",
                "data_timestamp_utc": "2026-05-26T16:00:00Z",
                "market_data_validation_status": validation_status,
                "field": "last_price",
            }
        )
    _write_json(root / "reports/aegis_data_registry_v1" / DAY / "data_registry.v1.json", {"day_utc": DAY, "data_items": items})


def test_signal_evidence_graph_certifies_current_entry_price() -> None:
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        root = tmp_path / "truth"
        repo = _repo(tmp_path)
        _seed_signal(root)
        _seed_registry(root)
        payload = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
        signal = payload["signals"][0]
        price = next(item for item in signal["required_evidence"] if item["purpose"] == "ENTRY_REFERENCE_PRICE")
        assert price["demanded"] is True
        assert price["fetched"] is True
        assert price["certified"] is True
        assert price["consumed"] is True
        assert signal["candidate_contract_status"] == "VALID"


def test_signal_evidence_graph_reports_not_certified_when_registry_missing() -> None:
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        root = tmp_path / "truth"
        repo = _repo(tmp_path)
        _seed_signal(root)
        _seed_registry(root, include_registry=False)
        payload = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
        signal = payload["signals"][0]
        price = next(item for item in signal["required_evidence"] if item["purpose"] == "ENTRY_REFERENCE_PRICE")
        assert price["fetched"] is True
        assert price["certified"] is False
        assert price["failure_reason"] == "EVIDENCE_NOT_CERTIFIED"
        assert signal["rejection_stage"] == "CANDIDATE_CONVERSION"


def test_signal_evidence_graph_reports_not_fetched_when_price_absent() -> None:
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        root = tmp_path / "truth"
        repo = _repo(tmp_path)
        _seed_signal(root)
        market_inputs = root / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"
        _write_json(market_inputs, {"day_utc": DAY, "input_records": []})
        _write_json(root / "reports/aegis_data_registry_v1" / DAY / "data_registry.v1.json", {"day_utc": DAY, "data_items": []})
        payload = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
        signal = payload["signals"][0]
        price = next(item for item in signal["required_evidence"] if item["purpose"] == "ENTRY_REFERENCE_PRICE")
        assert price["fetched"] is False
        assert price["certified"] is False
        assert price["failure_reason"] == "EVIDENCE_NOT_FETCHED"
        assert signal["rejection_reason"] == "EVIDENCE_NOT_FETCHED"



def _repo_with_repaired_candidate_policies(tmp_path: Path) -> Path:
    repo = tmp_path / "repo_repaired"
    _write_json(
        repo / "governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json",
        {
            "engine_policies": [
                {
                    "engine_id": "C2_MEAN_REVERSION_EQ_V1",
                    "exposure_requirements": {"exposure_type": "LONG_EQUITY"},
                    "structure_template": {"allowed_action": "BUY", "structure_type": "EQUITY_SPOT"},
                }
            ]
        },
    )
    _write_json(
        repo / "governance/02_REGISTRIES/C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json",
        {
            "engine_policies": [
                {
                    "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "exposure_requirements": {
                        "exposure_type": "SHORT_VOL_DEFINED",
                        "required_option_direction": "SELL",
                    },
                    "options_template": {
                        "strategy": {"structure": "VERTICAL_SPREAD", "right": "PUT", "direction": "CREDIT"}
                    },
                }
            ]
        },
    )
    return repo


def _seed_custom_signal(root: Path, *, engine_id: str, exposure_type: str, symbol: str, intent_id: str) -> None:
    intent_path = root / "truth_sleeves/PRIMARY/PAPER/intents_v1/snapshots" / DAY / f"{intent_id}.exposure_intent.v1.json"
    _write_json(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "intent_id": intent_id,
            "intent_hash": f"hash-{intent_id}",
            "engine": {"engine_id": engine_id, "suite": "C2_HYBRID_V1"},
            "exposure_type": exposure_type,
            "constraints": {"max_risk_pct": "0.01"},
            "underlying": {"symbol": symbol},
        },
    )
    sleeve_path = root / "reports/sleeve_evaluation_kernel_v1" / DAY / engine_id / "sleeve_evaluation.v1.json"
    payload = {
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "status": "INTENT_CREATED",
        "current_status": "INTENT_CREATED",
        "output_count": 1,
        "artifact_path": str(sleeve_path),
        "reason_codes": ["SIGNAL_CHANGED"],
        "exposure_intent_batch": {
            "output_intents": [
                {
                    "intent_id": intent_id,
                    "intent_hash": f"hash-{intent_id}",
                    "intent_path": str(intent_path),
                    "schema_id": "exposure_intent",
                    "symbol": symbol,
                }
            ]
        },
    }
    _write_json(sleeve_path, payload)
    _write_json(root / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"day_utc": DAY, "outcomes": [payload]})


def _seed_symbol_price(root: Path, *, symbol: str) -> None:
    market_inputs = root / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"
    _write_json(
        market_inputs,
        {
            "day_utc": DAY,
            "input_records": [
                {
                    "data_item_id": f"market.price.{symbol}",
                    "symbol": symbol,
                    "value": 101.25,
                    "field_type": "last_price",
                    "day_utc": DAY,
                    "source_timestamp_utc": "2026-05-26T16:00:00Z",
                    "source_vendor": "LOCAL_CACHE",
                    "validation_status": "VALID",
                }
            ],
        },
    )
    _write_json(
        root / "reports/aegis_data_registry_v1" / DAY / "data_registry.v1.json",
        {
            "day_utc": DAY,
            "data_items": [
                {
                    "data_item_id": f"market.price.{symbol}",
                    "symbol": symbol,
                    "status": "CURRENT",
                    "market_session_date": DAY,
                    "value": 101.25,
                    "source_artifact_path": str(market_inputs),
                    "source_hash": content_hash_file_v1(market_inputs),
                    "provider": "LOCAL_CACHE",
                    "data_timestamp_utc": "2026-05-26T16:00:00Z",
                    "market_data_validation_status": "VALID",
                    "field": "last_price",
                }
            ],
        },
    )


def test_mean_reversion_candidate_fields_are_derived_from_equity_policy(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo_with_repaired_candidate_policies(tmp_path)
    _seed_custom_signal(root, engine_id="C2_MEAN_REVERSION_EQ_V1", exposure_type="LONG_EQUITY", symbol="IMO", intent_id="mr-imo")
    _seed_symbol_price(root, symbol="IMO")

    payload = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
    signal = payload["signals"][0]

    assert signal["direction"] == "LONG"
    assert signal["instrument_type"] == "LONG_EQUITY"
    assert signal["governance_status"] == "GOVERNED"
    assert signal["missing_candidate_fields"] == []
    assert signal["candidate_contract_status"] == "VALID"


def test_vol_income_candidate_fields_are_derived_from_options_policy(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo_with_repaired_candidate_policies(tmp_path)
    _seed_custom_signal(root, engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", exposure_type="SHORT_VOL_DEFINED", symbol="GLD", intent_id="vol-gld")
    _seed_symbol_price(root, symbol="GLD")

    payload = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
    signal = payload["signals"][0]

    assert signal["direction"] == "SELL"
    assert signal["instrument_type"] == "SHORT_VOL_DEFINED"
    assert signal["governance_status"] == "GOVERNED"
    assert "candidate.direction" not in signal["missing_candidate_fields"]
    assert "candidate.instrument_type" not in signal["missing_candidate_fields"]
    assert "candidate.governance_status" not in signal["missing_candidate_fields"]
