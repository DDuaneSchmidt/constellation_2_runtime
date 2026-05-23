from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_eod_artifact_contract_v1 import _write_immutable_json
from ops.tools.run_aegis_lite_eod_pipeline_v1 import (
    build_aegis_lite_eod_pipeline_v1,
    prepare_operational_input_payload_v1,
)


DAY = "2026-05-15"
GENERATED = "2026-05-15T19:50:03Z"


def test_raw_candidates_upstream_without_promotion_becomes_precise_advisory_noop(tmp_path: Path) -> None:
    _write_market_snapshot(tmp_path, DAY, ["SPY", "QQQ", "IWM", "GLD"])
    _write_upstream_candidate_manifest(tmp_path, DAY, [_raw_candidate(idx) for idx in range(1, 5)])
    payload = prepare_operational_input_payload_v1(candidate_input_path="", promoted_sleeve_library_path="", manual_only=True)

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="missing-candidate-input",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["report_status"] == "ADVISORY_ONLY"
    assert report["eod_outcome_status"] == "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES"
    assert report["selected_trade_candidates"] == []
    assert report["manual_execution_status"] == "NOT_READY"
    assert report["run_receipt"]["broker_transmit_control_touched"] is False
    assert report["run_receipt"]["ib_submit_automation_invoked"] is False
    assert "CANDIDATE_INPUT_MISSING" not in report["eod_input_contract"]["blockers"]
    assert "RAW_CANDIDATES_EXISTED_NOT_CONSUMED" not in report["eod_input_contract"]["blockers"]
    assert "NO_PROMOTABLE_CANDIDATES" in report["do_not_trade_blockers"]
    assert "MISSING_PROMOTION_APPROVAL" in report["do_not_trade_blockers"]
    assert "PROMOTED_SLEEVE_LIBRARY_REQUIRED" in report["do_not_trade_blockers"]
    assert report["empty_section_reasons"]["executable_manual_trades"] == "raw candidates existed but none passed promotion governance"
    assert len(report["blocked_advisory_candidates"]) == 4

    audit = json.loads(Path(report["candidate_consumption_audit_artifact_path"]).read_text(encoding="utf-8"))
    assert audit["normal_no_op"] is True
    assert audit["raw_candidate_count"] == 4
    assert audit["promoted_candidate_count"] == 0
    assert audit["excluded_candidate_count"] == 4
    assert sum(audit["consumption_counts"].values()) == 4
    assert set(audit["consumption_counts"]).issubset({"EXCLUDED_LOW_SCORE", "EXCLUDED_POLICY", "EXCLUDED_UNCOVERED_SYMBOL", "EXCLUDED_MISSING_CERTIFIED_DATA"})

    lineage = json.loads(Path(report["candidate_lineage_artifact_path"]).read_text(encoding="utf-8"))
    assert lineage["candidate_count"] == 4
    assert {row["final_state"] for row in lineage["lineage_rows"]} == {"NOT_CONSUMED_BY_EOD"}
    assert all(row["consumed_by_eod"] is False for row in lineage["lineage_rows"])

    manifest = json.loads(Path(report["eod_run_manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["overall_status"] in {"ADVISORY_ONLY", "RELEASE_MISMATCH"}
    assert "NO_PROMOTABLE_CANDIDATES" in manifest["blockers"]
    assert "CANDIDATE_INPUT_MISSING" not in manifest["blockers"]
    assert "PROMOTED_SLEEVE_LIBRARY_REQUIRED" in manifest["blockers"]
    assert manifest["broker_transmit_control_touched"] is False
    assert manifest["ib_submit_automation_invoked"] is False


def test_promoted_sleeve_manifest_missing_blocks_execution_but_preserves_raw_visibility(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.json"
    candidates_path.write_text(json.dumps({"candidates": [_candidate_input()]}), encoding="utf-8")
    payload = prepare_operational_input_payload_v1(
        candidate_input_path=str(candidates_path),
        promoted_sleeve_library_path="",
        manual_only=True,
    )

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="missing-promoted-library",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["report_status"] == "ADVISORY_ONLY"
    assert report["eod_outcome_status"] == "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES"
    assert report["selected_trade_candidates"] == []
    assert "NO_PROMOTABLE_CANDIDATES" in report["do_not_trade_blockers"]
    assert "MISSING_PROMOTION_APPROVAL" in report["do_not_trade_blockers"]
    assert "PROMOTED_SLEEVE_LIBRARY_REQUIRED" in report["do_not_trade_blockers"]
    assert report["blocked_advisory_candidates"]
    assert report["blocked_advisory_candidates"][0]["symbol"] == "SPY"


def test_empty_sections_distinguish_clean_no_signal_from_missing_candidate_input(tmp_path: Path) -> None:
    candidates_path = tmp_path / "empty-candidates.json"
    library_path = tmp_path / "promoted.json"
    candidates_path.write_text(json.dumps({"candidates": []}), encoding="utf-8")
    library_path.write_text(json.dumps({"promoted_sleeves": [_promoted_sleeve()]}), encoding="utf-8")
    payload = prepare_operational_input_payload_v1(
        candidate_input_path=str(candidates_path),
        promoted_sleeve_library_path=str(library_path),
        manual_only=True,
    )

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="clean-no-signal",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["report_status"] == "ADVISORY_ONLY"
    assert "CANDIDATE_INPUT_MISSING" not in report["do_not_trade_blockers"]
    assert report["empty_section_reasons"]["executable_manual_trades"] == "no raw candidates generated"
    assert report["empty_section_reasons"]["blocked_advisory_candidates"] == "no raw candidates generated"


def test_stale_market_snapshot_blocks_promoted_candidates_with_visible_symbols(tmp_path: Path) -> None:
    _write_market_snapshot(tmp_path, "2026-05-14", ["SPY"])
    candidates_path = tmp_path / "candidates.json"
    library_path = tmp_path / "promoted.json"
    candidates_path.write_text(json.dumps({"candidates": [_candidate_input()]}), encoding="utf-8")
    library_path.write_text(json.dumps({"promoted_sleeves": [_promoted_sleeve()]}), encoding="utf-8")
    payload = prepare_operational_input_payload_v1(candidate_input_path=str(candidates_path), promoted_sleeve_library_path=str(library_path), manual_only=True)

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="stale-market-snapshot",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["eod_outcome_status"] == "BLOCKED_UNCERTIFIED_SYMBOLS"
    assert "MARKET_SNAPSHOT_PARTIAL" in report["do_not_trade_blockers"]
    assert report["selected_trade_candidates"]
    assert report["selected_trade_candidates"][0]["symbol"] == "SPY"
    snapshot = json.loads(Path(report["market_snapshot_authority_path"]).read_text(encoding="utf-8"))
    assert snapshot["snapshot_status"] == "PARTIAL"
    assert snapshot["stale_symbols"] == ["SPY"]


def test_broad_universe_raw_candidates_do_not_make_report_market_snapshot_partial(tmp_path: Path) -> None:
    _write_market_snapshot(tmp_path, DAY, ["SPY"])
    rows = [_raw_candidate(1), {**_raw_candidate(2), "candidate_id": "raw-uncertified", "symbol_or_pair": "UNCERTIFIED"}]
    _write_upstream_candidate_manifest(tmp_path, DAY, rows)
    payload = prepare_operational_input_payload_v1(candidate_input_path="", promoted_sleeve_library_path="", manual_only=True)

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="broad-raw-universe",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["report_status"] == "ADVISORY_ONLY"
    assert report["eod_outcome_status"] == "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES"
    assert "MARKET_SNAPSHOT_DATA_NOT_READY" not in report["do_not_trade_blockers"]
    assert "MARKET_SNAPSHOT_PARTIAL" not in report["do_not_trade_blockers"]
    assert "NO_PROMOTABLE_CANDIDATES" in report["do_not_trade_blockers"]
    snapshot = json.loads(Path(report["market_snapshot_authority_path"]).read_text(encoding="utf-8"))
    assert snapshot["required_symbols"] == []
    assert snapshot["snapshot_status"] == "COHERENT"
    audit = json.loads(Path(report["candidate_consumption_audit_artifact_path"]).read_text(encoding="utf-8"))
    assert audit["consumption_counts"]["EXCLUDED_UNCOVERED_SYMBOL"] == 1
    uncovered = [row for row in audit["candidate_rows"] if row["consumption_category"] == "EXCLUDED_UNCOVERED_SYMBOL"]
    assert uncovered[0]["symbol"] == "UNCERTIFIED"


def test_promoted_library_consumes_covered_candidate_in_audit(tmp_path: Path) -> None:
    _write_market_snapshot(tmp_path, DAY, ["SPY"])
    candidates_path = tmp_path / "candidates.json"
    library_path = tmp_path / "promoted.json"
    candidates_path.write_text(json.dumps({"candidates": [_candidate_input()]}), encoding="utf-8")
    library_path.write_text(json.dumps({"promoted_sleeves": [_promoted_sleeve()]}), encoding="utf-8")
    payload = prepare_operational_input_payload_v1(
        candidate_input_path=str(candidates_path),
        promoted_sleeve_library_path=str(library_path),
        manual_only=True,
    )

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="promoted-audit",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["report_status"] in {"READY", "READY_WITH_WARNINGS", "BLOCKED"}
    assert report["eod_outcome_status"] == "READY_WITH_PROMOTED_CANDIDATES"
    audit = json.loads(Path(report["candidate_consumption_audit_artifact_path"]).read_text(encoding="utf-8"))
    assert audit["promoted_candidate_count"] == 1
    assert audit["consumption_counts"] == {"PROMOTED": 1}
    assert audit["candidate_rows"][0]["consumption_category"] == "PROMOTED"


def _write_upstream_candidate_manifest(root: Path, day: str, rows: list[dict[str, object]]) -> None:
    rollup_path = root / "reports" / "sleeve_evaluation_kernel_v1" / day / "sleeve_evaluation_rollup.v1.json"
    rollup_path.parent.mkdir(parents=True, exist_ok=True)
    rollup_path.write_text(json.dumps({"day_utc": day}), encoding="utf-8")
    manifest_path = (
        root
        / "reports"
        / "candidate_generation_manifest_v1"
        / day
        / f"sleeve_evaluation_kernel_v1:{day}"
        / "candidate_generation_manifest.v1.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps({"day_utc": day, "source_rollup_path": str(rollup_path), "candidate_rows": rows}),
        encoding="utf-8",
    )


def _write_market_snapshot(root: Path, day: str, symbols: list[str]) -> None:
    clean_symbols = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    market_root = root / "market_data_snapshot_v1"
    market_root.mkdir(parents=True, exist_ok=True)
    (market_root / "dataset_manifest.json").write_text(json.dumps({"day_utc": day}), encoding="utf-8")
    for symbol in clean_symbols:
        path = market_root / symbol / f"{DAY[:4]}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"timestamp_utc": f"{day}T20:00:00Z", "close": 100}) + "\n", encoding="utf-8")
    final_eod_path = root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json"
    final_eod_path.parent.mkdir(parents=True, exist_ok=True)
    final_eod_path.write_text(json.dumps({"schema_id": "final_eod_market_data", "day_utc": day, "final_eod_symbols": clean_symbols}), encoding="utf-8")


def _raw_candidate(idx: int) -> dict[str, object]:
    symbol = ["SPY", "QQQ", "IWM", "GLD"][idx - 1]
    return {
        "candidate_id": f"raw-{idx}",
        "raw_intent_id": f"intent-{idx}",
        "engine_id": "C2_TREND_EQ_PRIMARY",
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
        "symbol_or_pair": symbol,
        "status": "CANDIDATE_CREATED",
        "reason_codes": ["SIGNAL_CREATED"],
    }


def _candidate_input() -> dict[str, object]:
    return {
        "candidate_id": "raw-spy",
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
        "symbol": "SPY",
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": "520.10",
        "suggested_quantity": 1,
        "sizing_guidance": "Buy 1 share.",
        "stop_price": "514.90",
        "stop_logic": "STOP_BASED",
        "risk_per_trade": "5.20",
        "confidence": "MEDIUM",
        "conviction": "MEDIUM",
        "edge_family": "TREND",
        "thesis_id": "SPY_TREND",
    }


def _promoted_sleeve() -> dict[str, object]:
    return {
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
        "promotion_status": "promoted",
        "approved_by_human": True,
        "approved_for_lite_implementation": True,
        "archived": False,
    }


def test_lite_eod_cli_default_run_id_is_time_versioned_to_surface_duplicate_runs() -> None:
    source = (REPO_ROOT / "ops/tools/run_aegis_lite_eod_pipeline_v1.py").read_text(encoding="utf-8")

    assert "default_run_suffix = generated_at_utc" in source
    assert "aegis_lite_eod_v1:{day_utc}:{default_run_suffix}" in source
    assert "aegis_lite_eod_v1:{day_utc}\")" not in source


def test_eod_writer_versions_conflicting_scheduler_reruns(tmp_path: Path) -> None:
    path = tmp_path / "reports" / "eod_run_manifest_v1" / DAY / "scheduled-1450" / "eod_run_manifest.v1.json"
    first = {"schema_id": "x", "trading_date": DAY, "run_id": "scheduled-1450", "value": 1}
    second = {"schema_id": "x", "trading_date": DAY, "run_id": "scheduled-1450", "value": 2}

    first_path = _write_immutable_json(path, first)
    same_path = _write_immutable_json(path, first)
    second_path = _write_immutable_json(path, second)

    assert first_path == path
    assert same_path == path
    assert second_path != path
    assert second_path.parent.name.startswith("scheduled-1450__")
    assert path.exists()
    assert second_path.exists()
