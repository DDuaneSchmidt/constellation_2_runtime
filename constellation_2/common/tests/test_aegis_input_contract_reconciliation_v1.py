from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.hash_lineage_v1 import build_hash_lineage_v1
from ops.aegis.input_contract_reconciliation_v1 import build_input_contract_reconciliation_v1, write_input_contract_reconciliation_v1
from ops.aegis.sleeve_input_contracts_v1 import build_sleeve_input_contracts_v1, write_sleeve_input_contracts_v1
from ops.aegis.sleeve_readiness_v1 import build_sleeve_readiness_v1

DAY = "2026-05-27"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_sleeve_contract_uses_canonical_symbol_universe_and_vix_profile(monkeypatch, tmp_path: Path) -> None:
    import ops.aegis.sleeve_input_contracts_v1 as contracts_mod

    def fake_inventory(*, repo_root: Path) -> dict:
        return {"registry_path": "/registry.json", "enabled_sleeves": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "enabled": True, "allowed_symbols": ["SPY"]}, {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "enabled": True, "allowed_symbols": ["IWM"]}]}

    def fake_resolver(**kwargs):
        engine_id = kwargs["engine_id"]
        symbols = ["AAPL", "MSFT"] if engine_id == "C2_TREND_EQ_PRIMARY_V1" else ["IWM"]
        return SimpleNamespace(symbols=symbols, source="governed_universe", source_path=str(tmp_path / "basis.json"), deprecated_fallback_used=False)

    (tmp_path / "basis.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(contracts_mod, "_authoritative_sleeve_inventory", fake_inventory)
    monkeypatch.setattr(contracts_mod, "resolve_canonical_symbol_universe_v1", fake_resolver)
    payload = build_sleeve_input_contracts_v1(repo_root=tmp_path, truth_root=tmp_path, day_utc=DAY)
    trend = next(row for row in payload["contracts"] if row["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1")
    vol = next(row for row in payload["contracts"] if row["sleeve_id"] == "C2_VOL_INCOME_DEFINED_RISK_V1")

    assert trend["allowed_symbols"] == ["AAPL", "MSFT"]
    assert trend["registry_allowed_symbols"] == ["SPY"]
    assert trend["symbol_resolution"]["deprecated_registry_fallback_used"] is False
    assert [row["data_item_id"] for row in trend["required_inputs"]] == ["market.price.AAPL", "market.price.MSFT"]
    assert not [row for row in vol["required_inputs"] if row["data_item_id"] == "market.volatility.VIX"]
    vix_optional = next(row for row in vol["optional_inputs"] if row["data_item_id"] == "market.volatility.VIX")
    assert vix_optional["blocker_severity"] == "NON_BLOCKING"
    assert vix_optional["freshness_requirement"] == "PRIOR_EOD_REFERENCE_ALLOWED"


def test_vix_nonblocking_contract_does_not_block_readiness(tmp_path: Path) -> None:
    contract_payload = {
        "schema_id": "aegis_sleeve_input_contracts",
        "day_utc": DAY,
        "contracts": [{
            "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
            "contract_status": "OK",
            "required_inputs": [{"data_item_id": "market.price.IWM", "required": True, "block_if_missing": True, "freshness_requirement": "CURRENT"}],
            "optional_inputs": [{"data_item_id": "market.volatility.VIX", "required": False, "block_if_missing": False, "block_if_stale": False, "warn_if_missing": True, "freshness_requirement": "PRIOR_EOD_REFERENCE_ALLOWED"}],
        }],
    }
    registry_payload = {"schema_id": "aegis_data_registry", "day_utc": DAY, "data_items": [{"data_item_id": "market.price.IWM", "status": "CURRENT"}, {"data_item_id": "market.volatility.VIX", "status": "STALE"}]}
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", contract_payload)
    _write_json(tmp_path / "reports/aegis_data_registry_v1" / DAY / "data_registry.v1.json", registry_payload)

    readiness = build_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    sleeve = readiness["sleeves"][0]
    assert sleeve["readiness"] == "READY_WITH_WARNINGS"
    assert "market.volatility.VIX" not in sleeve["blocking_inputs"]
    assert "market.volatility.VIX" in sleeve["warning_inputs"]


def test_input_contract_reconciliation_detects_hash_symbols_and_stale_intent(tmp_path: Path) -> None:
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {"contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "contract_status": "OK", "allowed_symbols": ["AAPL", "MSFT"], "registry_allowed_symbols": ["SPY"], "symbol_universe_hash": "universe-hash"}]})
    _write_json(tmp_path / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json", {"sleeves": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "blocking_inputs": []}]})
    _write_json(tmp_path / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"outcomes": [{"engine_id": "C2_TREND_EQ_PRIMARY_V1", "status": "BLOCKED", "canonical_blocker": "MARKET_DATA_SHA_MISMATCH", "reason_codes": ["MARKET_DATA_SHA_MISMATCH", "STALE_MISMATCHED_INTENT_REJECTED"], "producer_requested_symbols": ["SPY"], "stderr_summary": "FAIL: MARKET_DATA_SHA_MISMATCH: file=SPY/2026.jsonl expected=abc123abc123abc123abc123abc123ab got=def456def456def456def456def456de", "rejected_intents": [{"symbol": "COF", "intent_path": "/truth/stale.json"}]}]})
    _write_json(tmp_path / "reports/aegis_context_requirement_profile_v1" / DAY / "context_requirement_profile.v1.json", {"active_profile": {"requirements": [{"required_evidence_item": "market.volatility.VIX", "blocker_severity": "NON_BLOCKING"}]}})

    payload = build_input_contract_reconciliation_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_input_contract_reconciliation_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)["json"]
    row = payload["rows"][0]
    assert Path(path).exists()
    assert row["allowed_symbol_status"] == "MISMATCH"
    assert row["market_data_hash_status"] == "MISMATCH"
    assert row["stale_intent_status"] == "INVALIDATED_OR_REJECTED"
    assert row["stale_artifact_path"] == "/truth/stale.json"
    assert row["blocking_current_day_valid_candidates"] is True
    assert row["repair_action"] == f"TARGET_DAY={DAY} npm run aegis:repair-input-contracts"



def test_hash_lineage_marks_sleeve_contracts_stale_after_market_data_hash_change(tmp_path: Path) -> None:
    market_path = tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json"
    _write_json(market_path, {"artifact_id": "aegis_market_data_v1", "day_utc": DAY, "generated_at_utc": "2026-05-27T14:00:00Z", "market_data_snapshot_hash": "new-market-hash"})
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {
        "artifact_id": "aegis_sleeve_input_contracts_v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-05-27T13:55:00Z",
        "contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "market_data_hash": "old-market-hash", "market_data_artifact_path": str(market_path), "generated_at_utc": "2026-05-27T13:55:00Z"}],
    })

    lineage = build_hash_lineage_v1(truth_root=tmp_path, day_utc=DAY)

    assert lineage["status"] == "STALE_DOWNSTREAM_ARTIFACTS"
    assert lineage["market_data_hash"] == "new-market-hash"
    assert lineage["stale_downstream_artifacts"][0]["stale_sleeve_ids"] == ["C2_TREND_EQ_PRIMARY_V1"]
    assert lineage["required_regeneration_actions"] == [f"TARGET_DAY={DAY} npm run aegis:repair-input-contracts"]


def test_repair_regenerated_contract_hashes_clear_hash_lineage_staleness(tmp_path: Path) -> None:
    market_path = tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json"
    _write_json(market_path, {"artifact_id": "aegis_market_data_v1", "day_utc": DAY, "generated_at_utc": "2026-05-27T14:00:00Z", "market_data_snapshot_hash": "current-market-hash"})
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {
        "artifact_id": "aegis_sleeve_input_contracts_v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-05-27T14:01:00Z",
        "contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "market_data_hash": "current-market-hash", "market_data_artifact_path": str(market_path), "generated_at_utc": "2026-05-27T14:01:00Z"}],
    })

    lineage = build_hash_lineage_v1(truth_root=tmp_path, day_utc=DAY)

    assert lineage["status"] == "PASS"
    assert lineage["stale_downstream_artifacts"] == []
    assert lineage["required_regeneration_actions"] == []


def test_input_reconciliation_explains_coverage_ready_but_hash_mismatch(tmp_path: Path) -> None:
    market_path = tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json"
    _write_json(market_path, {"artifact_id": "aegis_market_data_v1", "day_utc": DAY, "generated_at_utc": "2026-05-27T14:00:00Z", "market_data_snapshot_hash": "new-market-hash"})
    _write_json(tmp_path / "reports/aegis_market_data_coverage_v1" / DAY / "market_data_coverage.v1.json", {"status": "READY", "coverage_pct": 100.0})
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {"contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "contract_status": "OK", "allowed_symbols": ["SPY"], "market_data_hash": "old-market-hash", "market_data_artifact_path": str(market_path)}]})
    _write_json(tmp_path / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json", {"sleeves": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "blocking_inputs": []}]})
    _write_json(tmp_path / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"outcomes": [{"engine_id": "C2_TREND_EQ_PRIMARY_V1", "status": "BLOCKED", "reason_codes": [], "producer_requested_symbols": ["SPY"]}]})

    payload = build_input_contract_reconciliation_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["rows"][0]

    assert row["market_data_hash_status"] == "MISMATCH"
    assert row["expected_market_data_hash"] == "old-market-hash"
    assert row["actual_market_data_hash"] == "new-market-hash"
    assert payload["coverage_ready_hash_mismatch_message"] == "Coverage is ready, but downstream sleeve contracts were generated against an older market-data hash. Run repair-input-contracts."



def test_candidate_diagnostics_guard_blocks_stale_expected_market_hash(tmp_path: Path) -> None:
    from ops.tools.write_aegis_candidate_generation_diagnostics_v1 import main as diagnostics_main

    _write_json(tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json", {"artifact_id": "aegis_market_data_v1", "day_utc": DAY, "generated_at_utc": "2026-05-27T14:00:00Z", "market_data_snapshot_hash": "new-market-hash"})
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {
        "artifact_id": "aegis_sleeve_input_contracts_v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-05-27T13:55:00Z",
        "contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "market_data_hash": "old-market-hash"}],
    })

    rc = diagnostics_main(["--truth_root", str(tmp_path), "--day", DAY, "--skip-self-heal"])

    assert rc == 2
    lineage_path = tmp_path / "reports/aegis_hash_lineage_v1" / DAY / "hash_lineage.v1.json"
    assert lineage_path.exists()
    assert json.loads(lineage_path.read_text(encoding="utf-8"))["status"] == "STALE_DOWNSTREAM_ARTIFACTS"



def test_market_data_manifest_sync_records_actual_file_hash_without_bypass(tmp_path: Path) -> None:
    from ops.tools.refresh_aegis_market_data_v1 import sync_market_data_snapshot_manifest_v1
    import hashlib

    data_path = tmp_path / "market_data_snapshot_v1/SPY/2026.jsonl"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_text('{"symbol":"SPY","timestamp_utc":"2026-05-27T14:00:00Z"}\n', encoding="utf-8")
    _write_json(tmp_path / "market_data_snapshot_v1/dataset_manifest.json", {"files": [{"symbol": "SPY", "file": "SPY/2026.jsonl", "year": 2026, "sha256": "stale"}]})

    result = sync_market_data_snapshot_manifest_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"], generated_at_utc="2026-05-27T14:00:00Z")
    manifest = json.loads((tmp_path / "market_data_snapshot_v1/dataset_manifest.json").read_text(encoding="utf-8"))

    assert result["updated_file_count"] == 1
    assert manifest["files"][0]["sha256"] == hashlib.sha256(data_path.read_bytes()).hexdigest()
    assert manifest["global_hash"]


def test_missing_contract_market_hash_is_stale_when_market_data_exists(tmp_path: Path) -> None:
    market_path = tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json"
    _write_json(market_path, {"artifact_id": "aegis_market_data_v1", "day_utc": DAY, "generated_at_utc": "2026-05-27T14:00:00Z", "market_data_snapshot_hash": "current-market-hash"})
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {"contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "contract_status": "OK", "allowed_symbols": ["SPY"]}]})
    _write_json(tmp_path / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json", {"sleeves": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "blocking_inputs": []}]})
    _write_json(tmp_path / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"outcomes": [{"engine_id": "C2_TREND_EQ_PRIMARY_V1", "status": "BLOCKED", "reason_codes": [], "producer_requested_symbols": ["SPY"]}]})

    lineage = build_hash_lineage_v1(truth_root=tmp_path, day_utc=DAY)
    reconciliation = build_input_contract_reconciliation_v1(truth_root=tmp_path, day_utc=DAY)
    row = reconciliation["rows"][0]

    assert lineage["status"] == "STALE_DOWNSTREAM_ARTIFACTS"
    assert lineage["diagnostics_hash"] == ""
    assert lineage["generated_at_timestamps"]["market_data"] == "2026-05-27T14:00:00Z"
    assert lineage["stale_downstream_artifacts"][0]["artifact_family"] == "aegis_sleeve_input_contracts_v1"
    assert row["market_data_hash_status"] == "MISMATCH"
    assert row["expected_market_data_hash"] == ""
    assert row["actual_market_data_hash"] == "current-market-hash"
    assert row["blocking_current_day_valid_candidates"] is True


def test_hash_lineage_reports_stale_sleeve_evaluation_rollup(tmp_path: Path) -> None:
    _write_json(tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json", {"artifact_id": "aegis_market_data_v1", "day_utc": DAY, "generated_at_utc": "2026-05-27T14:00:00Z", "market_data_snapshot_hash": "current-market-hash"})
    _write_json(tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json", {"contracts": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "market_data_hash": "current-market-hash"}]})
    _write_json(tmp_path / "reports/sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"generated_at_utc": "2026-05-27T14:01:00Z", "outcomes": [{"engine_id": "C2_TREND_EQ_PRIMARY_V1", "market_data_manifest_check": {"canonical_blocker": "MARKET_DATA_SHA_MISMATCH", "hash_mismatches": [{"symbol": "SPY", "file": "SPY/2026.jsonl", "expected_sha256": "old", "actual_sha256": "new", "path": "/truth/SPY/2026.jsonl"}]}}]})

    lineage = build_hash_lineage_v1(truth_root=tmp_path, day_utc=DAY)

    assert lineage["status"] == "STALE_DOWNSTREAM_ARTIFACTS"
    stale = lineage["stale_downstream_artifacts"][0]
    assert stale["artifact_family"] == "sleeve_evaluation_kernel_v1"
    assert stale["reason"] == "SLEEVE_EVALUATION_ROLLUP_REFERENCES_STALE_MARKET_DATA_MANIFEST_HASHES"
    assert stale["hash_mismatches"][0]["symbol"] == "SPY"
    assert f"TARGET_DAY={DAY} npm run aegis:repair-input-contracts" in lineage["required_regeneration_actions"]


def test_repair_input_contracts_orders_hash_lineage_before_reconciliation_and_diagnostics() -> None:
    source = (ROOT / "ops/tools/repair_aegis_input_contracts_v1.py").read_text(encoding="utf-8")
    assert source.index('"aegis:sleeve-input-contracts"') < source.index('"aegis:hash-lineage"')
    assert source.index('"aegis:hash-lineage"') < source.index('"aegis:input-contract-reconciliation"')
    assert source.index('"aegis:input-contract-reconciliation"') < source.index('"aegis:candidate-diagnostics"')
