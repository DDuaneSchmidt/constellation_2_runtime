from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.manual_intraday_data_sourcing_pack import (
    build_manual_intraday_data_sourcing_pack,
    write_manual_intraday_data_sourcing_pack,
)


def test_build_manual_intraday_data_sourcing_pack_lists_priority1_files() -> None:
    report = build_manual_intraday_data_sourcing_pack(created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["required_symbols"] == ["DIA", "QQQ", "SPY"]
    assert report["summary"]["accepted_1m_alternatives"] == ["DIA_1m.csv", "QQQ_1m.csv", "SPY_1m.csv"]
    assert len(report["priority1_required_files"]) == 6
    assert {row["target_drop_path"] for row in report["priority1_required_files"]} == {
        "data/manual_intraday_import/DIA_30m.csv",
        "data/manual_intraday_import/DIA_5m.csv",
        "data/manual_intraday_import/QQQ_30m.csv",
        "data/manual_intraday_import/QQQ_5m.csv",
        "data/manual_intraday_import/SPY_30m.csv",
        "data/manual_intraday_import/SPY_5m.csv",
    }
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_write_manual_intraday_data_sourcing_pack_outputs_all_support_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "reports" / "atlas_v2_research_os"

    paths = write_manual_intraday_data_sourcing_pack(root, created_at="2026-06-05T00:00:00Z")

    for key in [
        "json",
        "summary",
        "priority1_required_files",
        "vendor_request_checklist",
        "csv_schema_template",
        "post_drop_validation_commands",
        "latest_json",
        "latest_summary",
    ]:
        assert paths[key].exists()
    report = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert report["summary"]["status"] == "MANUAL_DATA_REQUIRED"
    with paths["priority1_required_files"].open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6
    assert rows[0]["accepted_alternative"] == "DIA_1m.csv"
    assert "timestamp,open,high,low,close,volume" in paths["csv_schema_template"].read_text(encoding="utf-8")
    commands = paths["post_drop_validation_commands"].read_text(encoding="utf-8")
    assert "--manual-intraday-csv-intake" in commands
    assert "--direct-candidate-data-validation" in commands
    assert (tmp_path / "data" / "manual_intraday_import").is_dir()


def test_vendor_checklist_excludes_trading_authority(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    paths = write_manual_intraday_data_sourcing_pack(tmp_path / "reports" / "atlas_v2_research_os", created_at="2026-06-05T00:00:00Z")

    checklist = paths["vendor_request_checklist"].read_text(encoding="utf-8")

    assert "No trade execution data needed." in checklist
    assert "No account/broker connection needed." in checklist
    assert "Required columns: timestamp, open, high, low, close, volume." in checklist
