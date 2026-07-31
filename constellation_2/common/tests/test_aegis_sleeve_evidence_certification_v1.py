from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.sleeve_evidence_certification_v1 import (  # noqa: E402
    build_sleeve_evidence_certification_v1,
    write_sleeve_evidence_certification_v1,
)

DAY = "2026-06-03"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(
    root: Path,
    *,
    sleeve_id: str = "SLEEVE_A",
    active: int = 1,
    unrealized_pnl: str = "10",
    realized_pnl: str = "0",
    closed_pnls: list[str] | None = None,
    benchmark_return: str | None = "0.01",
) -> None:
    closed_pnls = closed_pnls or []
    _write(
        root / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json",
        {
            "schema_id": "aegis_sleeve_performance_truth",
            "schema_version": "v1",
            "day_utc": DAY,
            "sleeves": [
                {
                    "sleeve_id": sleeve_id,
                    "open_paper_position_count": active,
                    "closed_paper_position_count": len(closed_pnls),
                    "realized_pnl": realized_pnl,
                    "unrealized_pnl": unrealized_pnl,
                    "unrealized_pnl_status": "AVAILABLE",
                    "certified_entry_notional": "1000",
                    "mark_coverage_by_position_pct": 100,
                    "factory_classification": "TECHNICAL_STRATEGY",
                }
            ],
        },
    )
    _write(
        root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "open_positions": [
                {
                    "position_id": "open-1",
                    "sleeve_id": sleeve_id,
                    "symbol": "AAA",
                    "quantity": "1",
                    "entry_price": "100",
                    "unrealized_pnl": unrealized_pnl,
                }
            ],
            "closed_positions": [
                {
                    "position_id": f"closed-{idx}",
                    "sleeve_id": sleeve_id,
                    "symbol": "AAA",
                    "entry_time": f"{DAY}T14:00:00Z",
                    "exit_time": f"{DAY}T20:00:00Z",
                    "realized_pnl": pnl,
                }
                for idx, pnl in enumerate(closed_pnls)
            ],
        },
    )
    _write(
        root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json",
        {"schema_id": "aegis_paper_pnl_report", "day_utc": DAY, "overview": {"data_quality": "PASS"}},
    )
    market_row = {"canonical_symbol": "SPY", "open": 100, "close": 101, "freshness_status": "CURRENT"}
    if benchmark_return is not None:
        market_row["return"] = benchmark_return
    else:
        market_row = {"canonical_symbol": "QQQ", "open": 100, "close": 101, "freshness_status": "CURRENT"}
    _write(
        root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
        {"schema_id": "aegis_market_data", "day_utc": DAY, "normalized_records": [market_row]},
    )


def _row(payload: dict, sleeve_id: str = "SLEEVE_A") -> dict:
    return {row["sleeve_id"]: row for row in payload["sleeves"]}[sleeve_id]


def test_zero_closed_positions_are_underpowered_and_not_working(tmp_path: Path) -> None:
    _seed(tmp_path, unrealized_pnl="25", closed_pnls=[])
    row = _row(build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["sample_status"] == "ZERO_SAMPLE"
    assert row["evidence_status"] == "UNDERPOWERED"
    assert "ZERO_CLOSED_POSITIONS" in row["blocking_reasons"]


def test_positive_unrealized_pnl_alone_does_not_certify_evidence(tmp_path: Path) -> None:
    _seed(tmp_path, unrealized_pnl="100", closed_pnls=[])
    row = _row(build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["net_pnl"] == "100"
    assert row["evidence_status"] != "POSITIVE_EVIDENCE"
    assert "POSITIVE_UNREALIZED_PNL_IS_MARK_TO_MARKET_ONLY" in row["blocking_reasons"]


def test_benchmark_failure_blocks_positive_evidence_with_sufficient_sample(tmp_path: Path) -> None:
    closed = ["1"] * 20
    _seed(tmp_path, unrealized_pnl="1", realized_pnl="20", closed_pnls=closed, benchmark_return="0.10")
    row = _row(build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["sample_status"] == "SUFFICIENT_SAMPLE"
    assert row["net_pnl"] == "21"
    assert row["evidence_status"] == "BASELINE_FAIL"
    assert "BENCHMARK_UNDERPERFORMANCE" in row["blocking_reasons"]


def test_missing_benchmark_produces_explicit_not_evaluable_status(tmp_path: Path) -> None:
    closed = ["2"] * 20
    _seed(tmp_path, unrealized_pnl="0", realized_pnl="40", closed_pnls=closed, benchmark_return=None)
    row = _row(build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["sample_status"] == "SUFFICIENT_SAMPLE"
    assert row["benchmark_return"] == "NOT_EVALUABLE"
    assert row["benchmark_excess_return"] == "NOT_EVALUABLE"
    assert row["evidence_status"] == "NOT_EVALUABLE"
    assert "BENCHMARK_RETURN_NOT_EVALUABLE" in row["blocking_reasons"]


def test_writes_json_and_markdown_artifacts(tmp_path: Path) -> None:
    _seed(tmp_path)
    paths = write_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert paths["json"] == tmp_path / "reports" / "aegis_sleeve_evidence_certification_v1" / DAY / "sleeve_evidence_certification.v1.json"
    assert paths["markdown"] == tmp_path / "reports" / "aegis_sleeve_evidence_certification_v1" / DAY / "sleeve_evidence_certification_summary.md"
    assert json.loads(paths["json"].read_text(encoding="utf-8"))["schema_id"] == "aegis_sleeve_evidence_certification"
    summary = paths["markdown"].read_text(encoding="utf-8")
    assert "makes no trade recommendations" in summary
    assert "no investable-edge claims" in summary
