from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import direct_replay_attrition_audit as audit


def _candidate(candidate_id="ptc_backtest_final_469607b8340421b7", *, symbol="SPY", timeframe="5m", regime="CHOP", mechanism="BREAKOUT"):
    candidate = {"candidate_id": candidate_id, "mechanism": mechanism, "regime": regime, "candidate_symbols": [symbol], "candidate_timeframes": [timeframe]}
    plan = {"candidate_id": candidate_id, "mechanism": mechanism, "regime": regime, "minimum_sample_size": 3}
    attribution = {"candidate_id": candidate_id, "candidate_symbols": [symbol], "candidate_timeframes": [timeframe], "mechanism": mechanism, "regime": regime}
    return candidate, plan, attribution


def _touch_data(root: Path, symbol="SPY", timeframe="5m", *, raw=True, tf=True):
    if raw:
        path = root / "data" / "manual_intraday_import" / f"{symbol}_1m.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")
    if tf:
        path = root / "data" / "cache" / f"{symbol}_{timeframe}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")


def _patch_rows(monkeypatch, rows):
    monkeypatch.setattr(audit, "_load_rows", lambda path, symbol, timeframe: rows if timeframe != "1m" else rows[:2])
    monkeypatch.setattr(audit, "_compute_features", lambda rows_in: rows_in)
    monkeypatch.setattr(audit, "_has_required_features", lambda row, mechanism: True)
    monkeypatch.setattr(audit, "_mechanism_trigger", lambda row, mechanism: bool(row.get("trigger")))


def _rows(regimes, triggers):
    return [
        {"timestamp": f"2023-01-{i+1:02d}T09:30:00", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100, "regime": regime, "trigger": trigger}
        for i, (regime, trigger) in enumerate(zip(regimes, triggers))
    ]


def test_build_089_includes_priority_and_comparator(monkeypatch, tmp_path):
    root = tmp_path / "reports" / "atlas_v2_research_os"
    root.mkdir(parents=True)
    ids = audit.AUDITED_CANDIDATE_IDS
    candidates = []
    plans = []
    attributions = []
    for idx, candidate_id in enumerate(ids):
        c, p, a = _candidate(candidate_id, timeframe="5m" if idx != 0 else "30m", regime="TRENDING", mechanism="BREAKOUT")
        candidates.append(c)
        plans.append(p)
        attributions.append(a)
        _touch_data(tmp_path, "SPY", c["candidate_timeframes"][0])
    (root / "focused_observation_campaign").mkdir()
    (root / "candidate_data_validation_plan").mkdir()
    (root / "candidate_symbol_attribution").mkdir()
    (root / "direct_candidate_data_validation").mkdir()
    (root / "focused_observation_campaign" / "latest.json").write_text(json.dumps({"campaign_candidates": candidates}), encoding="utf-8")
    (root / "candidate_data_validation_plan" / "latest.json").write_text(json.dumps({"candidate_data_validation_plans": plans}), encoding="utf-8")
    (root / "candidate_symbol_attribution" / "latest.json").write_text(json.dumps({"candidate_symbol_attributions": attributions}), encoding="utf-8")
    (root / "direct_candidate_data_validation" / "latest.json").write_text(json.dumps({"candidate_validations": []}), encoding="utf-8")
    _patch_rows(monkeypatch, _rows(["TRENDING"] * 8, [True] * 8))
    monkeypatch.chdir(tmp_path)
    report = audit.run_direct_replay_attrition_audit(root=root, created_at="2026-06-06T00:00:00Z")
    found = {row["candidate_id"] for row in report["candidate_audits"]}
    assert ids[0] in found
    assert ids[1] in found
    assert audit.COMPARATOR_CANDIDATE_ID in found


def test_missing_csv_diagnostics(monkeypatch, tmp_path):
    c, p, a = _candidate()
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "DATA_FILE_MISSING"
    _touch_data(tmp_path, raw=True, tf=False)
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "TIMEFRAME_FILE_MISSING"


def test_no_date_overlap(monkeypatch, tmp_path):
    c, p, a = _candidate()
    c["candidate_expected_start"] = "2024-01-01"
    c["candidate_expected_end"] = "2024-01-31"
    _touch_data(tmp_path)
    _patch_rows(monkeypatch, _rows(["RANGE_BOUND"] * 3, [True] * 3))
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "DATE_RANGE_NO_OVERLAP"


def test_trigger_and_regime_diagnostics(monkeypatch, tmp_path):
    c, p, a = _candidate(regime="CHOP")
    _touch_data(tmp_path)
    _patch_rows(monkeypatch, _rows(["RANGE_BOUND"] * 8, [False] * 8))
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "NO_TRIGGER_MATCH"
    _patch_rows(monkeypatch, _rows(["RANGE_BOUND", "RANGE_BOUND", "TRENDING", "TRENDING", "TRENDING", "TRENDING", "TRENDING", "TRENDING", "TRENDING", "TRENDING", "TRENDING", "TRENDING"], [False, False, True, True, True, True, True, True, True, True, True, True]))
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "TRIGGER_MATCH_REGIME_BLOCKED"


def test_unknown_regime_and_insufficient_sample(monkeypatch, tmp_path):
    c, p, a = _candidate(regime="BULL")
    _touch_data(tmp_path)
    _patch_rows(monkeypatch, _rows(["RANGE_BOUND"] * 8, [True] * 8))
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "VOCABULARY_MISMATCH"
    c, p, a = _candidate(regime="CHOP")
    p["minimum_sample_size"] = 10
    row = audit.audit_candidate_attrition(c["candidate_id"], c, p, a, repo_root=tmp_path)
    assert row["diagnostic"] == "INSUFFICIENT_SAMPLE"


def test_reports_columns_confidence_and_forbidden_fields(monkeypatch, tmp_path):
    root = tmp_path / "reports" / "atlas_v2_research_os"
    for name in ["focused_observation_campaign", "candidate_data_validation_plan", "candidate_symbol_attribution", "direct_candidate_data_validation"]:
        (root / name).mkdir(parents=True, exist_ok=True)
    c, p, a = _candidate(audit.AUDITED_CANDIDATE_IDS[0], regime="CHOP")
    records = [c]
    plans = [p]
    attrs = [a]
    for candidate_id in audit.AUDITED_CANDIDATE_IDS[1:]:
        c2, p2, a2 = _candidate(candidate_id, regime="CHOP")
        records.append(c2); plans.append(p2); attrs.append(a2)
    (root / "focused_observation_campaign" / "latest.json").write_text(json.dumps({"campaign_candidates": records}), encoding="utf-8")
    (root / "candidate_data_validation_plan" / "latest.json").write_text(json.dumps({"candidate_data_validation_plans": plans}), encoding="utf-8")
    (root / "candidate_symbol_attribution" / "latest.json").write_text(json.dumps({"candidate_symbol_attributions": attrs}), encoding="utf-8")
    (root / "direct_candidate_data_validation" / "latest.json").write_text(json.dumps({"candidate_validations": []}), encoding="utf-8")
    _touch_data(tmp_path)
    _patch_rows(monkeypatch, _rows(["RANGE_BOUND"] * 8, [False] * 8))
    monkeypatch.chdir(tmp_path)
    report = audit.run_direct_replay_attrition_audit(root=root, created_at="2026-06-06T00:00:00Z")
    assert report["confidence_impact"] in {"NONE", "DECREASE"}
    assert report["confidence_impact"] != "INCREASE"
    assert all(row["diagnostic"] != "INSUFFICIENT_DATA" for row in report["candidate_audits"])
    out = root / audit.REPORT_DIRNAME
    assert (out / "latest.json").exists()
    assert (out / "latest_summary.md").exists()
    assert (out / "attrition_matrix.csv").exists()
    assert (out / "candidate_stage_details.csv").exists()
    with (out / "attrition_matrix.csv").open(newline="", encoding="utf-8") as handle:
        assert set(audit.ATTRITION_COLUMNS).issubset(csv.DictReader(handle).fieldnames or [])
    with (out / "candidate_stage_details.csv").open(newline="", encoding="utf-8") as handle:
        assert {"candidate_id", "stage_name", "stage_count", "stage_status", "notes"}.issubset(csv.DictReader(handle).fieldnames or [])
    forbidden = {"live_trading", "broker_execution", "capital", "position_sizing", "paper_placement", "production_promotion", "candidate_promotion"}
    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                assert key not in forbidden
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    for row in report["candidate_audits"]:
        walk(row)
