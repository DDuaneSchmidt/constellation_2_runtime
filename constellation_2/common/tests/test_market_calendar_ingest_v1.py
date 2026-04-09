from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.phaseJ.tools.market_calendar_ingest_v1 as ingest_module
from constellation_2.common.paper_session_fact_plane_v1 import resolve_market_calendar_record_v1


def _write_csv(path: Path, rows: list[tuple[str, str]]) -> str:
    body = ["day_utc,is_trading_session"]
    body.extend(f"{day},{flag}" for day, flag in rows)
    payload = ("\n".join(body) + "\n").encode("utf-8")
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _run_ingest(*, truth_root: Path, csv_path: Path, source_hash: str) -> int:
    return ingest_module.main(
        [
            "--dataset_version",
            "v1",
            "--run_utc",
            "2026-04-09T21:43:49Z",
            "--exchange",
            "NYSE",
            "--csv",
            str(csv_path),
            "--source_name",
            "governed_source_dataset_v1",
            "--source_hash",
            source_hash,
            "--truth_root",
            str(truth_root),
        ]
    )


def test_market_calendar_ingest_appends_current_day_to_selected_truth_root(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    seed_csv = tmp_path / "seed.csv"
    append_csv = tmp_path / "append.csv"

    seed_hash = _write_csv(seed_csv, [("2026-04-07", "true"), ("2026-04-08", "true")])
    append_hash = _write_csv(append_csv, [("2026-04-09", "true")])

    assert _run_ingest(truth_root=truth_root, csv_path=seed_csv, source_hash=seed_hash) == 0
    assert _run_ingest(truth_root=truth_root, csv_path=append_csv, source_hash=append_hash) == 0

    manifest_path = truth_root / "market_calendar_v1" / "dataset_manifest.json"
    year_path = truth_root / "market_calendar_v1" / "NYSE" / "2026.jsonl"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = [json.loads(line) for line in year_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    assert manifest["date_range"] == {"start": "2026-04-07", "end": "2026-04-09"}
    assert [row["day_utc"] for row in rows] == ["2026-04-07", "2026-04-08", "2026-04-09"]
    assert rows[-1]["is_trading_session"] is True

    resolved = resolve_market_calendar_record_v1(truth_root=truth_root, day_utc="2026-04-09")
    assert resolved["status"] == "OK"
    assert resolved["record"]["is_trading_session"] is True


def test_market_calendar_record_missing_day_stays_fail_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    seed_csv = tmp_path / "seed.csv"
    seed_hash = _write_csv(seed_csv, [("2026-04-07", "true"), ("2026-04-08", "true")])

    assert _run_ingest(truth_root=truth_root, csv_path=seed_csv, source_hash=seed_hash) == 0

    resolved = resolve_market_calendar_record_v1(truth_root=truth_root, day_utc="2026-04-09")
    assert resolved["status"] == "MISSING"
    assert resolved["reason_code"] == "MARKET_CALENDAR_DAY_MISSING"
