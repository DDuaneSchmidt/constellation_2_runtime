from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

from ops.aegis.missing_market_data_repair_v1 import build_missing_market_data_repair_v1


DAY = "2026-06-02"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _seed_t03(root: Path) -> None:
    _write_json(
        root / "reports" / "aegis_missing_market_data_requirement_resolver_v1" / DAY / "missing_market_data_requirement_resolver.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "C2_DEFENSIVE_TAIL_V1", "sleeve_name": "Defensive Tail", "resolution_type": "SOURCE_AVAILABLE_ROUTING_MISSING"},
                {"sleeve_id": "C2_EVENT_DISLOCATION_V1", "sleeve_name": "Event Dislocation", "resolution_type": "SOURCE_AVAILABLE_BUT_INCOMPLETE"},
            ]
        },
    )


def _seed_tlt_sources(root: Path) -> dict:
    row = {
        "close": 85.73,
        "day_utc": DAY,
        "high": 85.8112,
        "low": 85.55,
        "open": 85.8,
        "provider": "STOOQ",
        "source": "STOOQ",
        "symbol": "TLT",
        "timestamp_utc": f"{DAY}T17:47:26Z",
        "value": 85.73,
        "volume": 6758859,
        "synthetic_data": False,
    }
    _write_text(root / "market_data_snapshot_v1" / "TLT" / "2026.jsonl", json.dumps(row, sort_keys=True) + "\n")
    sleeve = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_json(
        sleeve / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "day_utc": DAY,
            "history": {"drawdown_pct": "0.000000"},
            "source_type": "STATIC_RISK_BUDGET_BOOTSTRAP",
            "status": "BOOTSTRAP",
        },
    )
    _write_json(
        sleeve / "positions_v1" / "failures" / DAY / "positions_snapshot.v5.failure.json",
        {"day_utc": DAY, "reason_codes": ["DAY0_BOOTSTRAP_MISSING_CASH_OR_POSITIONS_ALLOWED"]},
    )
    return row


def _seed_gld_manifest(root: Path) -> Path:
    gld = root / "market_data_snapshot_v1" / "GLD" / "2026.jsonl"
    _write_text(gld, json.dumps({"day_utc": "2026-05-29", "symbol": "GLD", "close": 400.0}, sort_keys=True) + "\n")
    _write_json(
        root / "market_data_snapshot_v1" / "dataset_manifest.json",
        {"files": [{"file": "GLD/2026.jsonl", "sha256": sha256(gld.read_bytes()).hexdigest(), "symbol": "GLD", "year": 2026}]},
    )
    return gld


def _seed_post_diagnostics(root: Path) -> None:
    _write_json(root / "reports" / "aegis_sleeve_throughput_diagnostics_v1" / DAY / "sleeve_throughput_diagnostics.v1.json", {"sleeves": []})
    _write_json(root / "reports" / "aegis_dormant_sleeve_signal_generation_diagnostics_v1" / DAY / "dormant_sleeve_signal_generation_diagnostics.v1.json", {"dormant_sleeves": []})


def test_tlt_source_exists_but_sleeve_routing_missing_is_repaired(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    source_row = _seed_tlt_sources(root)
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)
    row = next(item for item in payload["sleeves"] if item["sleeve_id"] == "C2_DEFENSIVE_TAIL_V1")

    routed = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "market_data_snapshot_v1" / "snapshots" / DAY / "TLT.market_data_snapshot.v1.json"
    nav = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "accounting_v1" / "nav" / DAY / "nav_snapshot.v1.json"
    pos = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "positions_snapshot_v2" / "snapshots" / DAY / "positions_snapshot.v2.json"

    assert row["repair_status"] == "REPAIRED"
    assert row["repair_type"] == "ROUTING_REPAIR"
    assert routed.exists() and nav.exists() and pos.exists()
    assert json.loads(routed.read_text())["bars"][0] == source_row
    assert json.loads(nav.read_text())["history"]["drawdown_pct"] == "0.000000"
    assert json.loads(pos.read_text())["positions"]["items"] == []
    assert row["david_action_required"] is False


def test_gld_historical_bar_exists_under_alternate_source_and_is_repaired(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    gld = _seed_gld_manifest(root)
    _write_text(
        root / "reports" / "aegis_market_data_v1" / DAY / "raw" / "STOOQ" / "GLD.quote.csv",
        "Symbol,Date,Time,Open,High,Low,Close,Volume\nGLD.US,2026-06-02,17:45:02,414.08,414.3999,412.14,413.25,1090615\n",
    )
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)
    row = next(item for item in payload["sleeves"] if item["sleeve_id"] == "C2_EVENT_DISLOCATION_V1")
    manifest = json.loads((root / "market_data_snapshot_v1" / "dataset_manifest.json").read_text())
    manifest_hash = next(item["sha256"] for item in manifest["files"] if item["file"] == "GLD/2026.jsonl")

    bars = [json.loads(line) for line in gld.read_text().splitlines()]
    repaired = next(item for item in bars if item["day_utc"] == DAY)
    assert row["repair_status"] == "REPAIRED"
    assert row["repair_type"] == "INGESTION_COMPLETENESS_REPAIR"
    assert repaired["close"] == 413.25
    assert repaired["synthetic_data"] is False
    assert manifest_hash == sha256(gld.read_bytes()).hexdigest()


def test_gld_historical_bar_truly_missing_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    gld = _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)
    row = next(item for item in payload["sleeves"] if item["sleeve_id"] == "C2_EVENT_DISLOCATION_V1")

    assert row["repair_status"] == "NOT_REPAIRED_SOURCE_TRULY_MISSING"
    assert row["repair_type"] == "NO_REPAIR"
    assert DAY not in gld.read_text()
    assert row["david_action_required"] is False


def test_t04_preserves_strategy_threshold_and_safety_boundaries(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["no_strategy_logic_mutation"] is True
    assert payload["no_threshold_mutation"] is True
    assert payload["no_candidate_scoring_mutation"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["live_trading_allowed"] is False


def test_no_fabricated_market_data_is_created_for_tlt(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    source_row = _seed_tlt_sources(root)
    source_file = root / "market_data_snapshot_v1" / "TLT" / "2026.jsonl"
    before_hash = sha256(source_file.read_bytes()).hexdigest()
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)

    assert sha256(source_file.read_bytes()).hexdigest() == before_hash
    routed = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "market_data_snapshot_v1" / "snapshots" / DAY / "TLT.market_data_snapshot.v1.json"
    assert json.loads(routed.read_text())["bars"][0] == source_row


def test_original_t03_evidence_remains_traceable(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)

    defensive = next(item for item in payload["sleeves"] if item["sleeve_id"] == "C2_DEFENSIVE_TAIL_V1")
    event = next(item for item in payload["sleeves"] if item["sleeve_id"] == "C2_EVENT_DISLOCATION_V1")
    assert defensive["original_t03_resolution_type"] == "SOURCE_AVAILABLE_ROUTING_MISSING"
    assert event["original_t03_resolution_type"] == "SOURCE_AVAILABLE_BUT_INCOMPLETE"
    assert defensive["original_t03_link"]["artifact_id"] == "aegis_missing_market_data_requirement_resolver_v1"


def test_tlt_repair_is_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    first = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)
    second = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)

    first_defensive = next(item for item in first["sleeves"] if item["sleeve_id"] == "C2_DEFENSIVE_TAIL_V1")
    second_defensive = next(item for item in second["sleeves"] if item["sleeve_id"] == "C2_DEFENSIVE_TAIL_V1")
    assert first_defensive["repair_status"] == "REPAIRED"
    assert second_defensive["repair_status"] == "NO_REPAIR_REQUIRED"


def test_aegis_owned_system_routing_requirement_has_no_david_action(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)
    defensive = next(item for item in payload["sleeves"] if item["sleeve_id"] == "C2_DEFENSIVE_TAIL_V1")

    assert defensive["david_action_required"] is False
    assert defensive["repair_type"] == "ROUTING_REPAIR"


def test_safety_gate_fields_remain_false_on_rows(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    _seed_gld_manifest(root)
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)

    for row in payload["sleeves"]:
        assert row["broker_execution_allowed"] is False
        assert row["trade_advice_allowed"] is False
        assert row["live_trading_allowed"] is False
        assert row["autonomous_execution_allowed"] is False


def test_post_repair_t03_no_action_required_when_routes_and_bar_exist(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t03(root)
    _seed_tlt_sources(root)
    _write_text(
        root / "market_data_snapshot_v1" / "GLD" / "2026.jsonl",
        json.dumps({"day_utc": DAY, "symbol": "GLD", "close": 413.25}, sort_keys=True) + "\n",
    )
    _write_json(root / "market_data_snapshot_v1" / "dataset_manifest.json", {"files": []})
    _seed_post_diagnostics(root)

    payload = build_missing_market_data_repair_v1(truth_root=root, day_utc=DAY)

    for row in payload["sleeves"]:
        assert row["remaining_blocker"] == "NONE"
        assert row["david_action_required"] is False
