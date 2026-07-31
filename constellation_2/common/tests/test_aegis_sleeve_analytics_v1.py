from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.sleeve_analytics_v1 import build_sleeve_analytics_v1, write_sleeve_analytics_v1  # noqa: E402
from ops.tools.run_aegis_sleeve_analytics_self_check_v1 import main as self_check_main  # noqa: E402

DAY = "2026-05-28"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path) -> None:
    _write(
        root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json",
        {
            "schema_id": "aegis_paper_pnl_report",
            "day_utc": DAY,
            "generated_at_utc": f"{DAY}T21:00:00Z",
            "mark_coverage": {"open_position_count": 2, "marked_position_count": 2, "missing_mark_position_count": 0, "mark_coverage_by_position_pct": 100.0},
            "open_positions": [
                {"position_id": "pos-core", "symbol": "AAA", "sleeve_id": "CORE", "quantity": "10", "entry_price": "10", "current_certified_mark": "12.5", "unrealized_pnl": "25"},
                {"position_id": "pos-growth", "symbol": "BBB", "sleeve_id": "GROWTH", "quantity": "5", "entry_price": "20", "current_certified_mark": "18", "unrealized_pnl": "-10"},
            ],
            "closed_positions": [],
        },
    )
    _write(
        root / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json",
        {
            "schema_id": "aegis_sleeve_performance_truth",
            "generated_at_utc": f"{DAY}T21:10:00Z",
            "data_quality_status": "PASS",
            "sleeves": [
                {"sleeve_id": "CORE", "realized_pnl": "0", "unrealized_pnl": "25", "open_paper_position_count": 1, "closed_paper_position_count": 0, "mark_coverage_by_position_pct": 100, "data_quality_status": "PASS"},
                {"sleeve_id": "GROWTH", "realized_pnl": "0", "unrealized_pnl": "-10", "open_paper_position_count": 1, "closed_paper_position_count": 0, "mark_coverage_by_position_pct": 100, "data_quality_status": "PASS"},
            ],
        },
    )
    _write(root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {"schema_id": "aegis_paper_position_ledger", "open_positions": []})
    _write(root / "reports" / "aegis_daily_paper_performance_v1" / DAY / "daily_paper_performance.v1.json", {"schema_id": "aegis_daily_paper_performance", "generated_at_utc": f"{DAY}T21:15:00Z"})


def test_sleeve_analytics_builds_phase1_canonical_artifact(tmp_path: Path) -> None:
    _seed(tmp_path)

    payload = build_sleeve_analytics_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_sleeve_analytics"
    assert payload["artifact_id"] == "aegis_sleeve_analytics_v1"
    assert payload["status"] == "CANONICAL"
    assert payload["summary"]["total_sleeves"] == 2
    assert payload["summary"]["active_sleeves"] == 2
    assert payload["summary"]["total_unrealized_pnl"] == 15.0
    assert payload["summary"]["total_pnl"] == 15.0
    assert payload["summary"]["mark_coverage_pct"] == 100.0
    assert payload["summary"]["sleeve_attribution_coverage_pct"] == 100.0
    assert payload["summary"]["best_sleeve"]["sleeve_id"] == "CORE"
    assert payload["summary"]["worst_sleeve"]["sleeve_id"] == "GROWTH"
    assert payload["composite_sleeve_score_included"] is False
    assert payload["trade_advice_allowed"] is False


def test_sleeve_scorecard_formula_and_data_quality_are_deterministic(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_analytics_v1(truth_root=tmp_path, day_utc=DAY)
    rows = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert rows["CORE"]["total_pnl"] == rows["CORE"]["realized_pnl"] + rows["CORE"]["unrealized_pnl"]
    assert rows["CORE"]["open_positions"] == 1
    assert rows["CORE"]["market_value"] == 125.0
    assert rows["CORE"]["mark_coverage_pct"] == 100.0
    assert rows["CORE"]["data_quality_status"] == "PASS"
    assert rows["CORE"]["profit_factor"] is None
    assert rows["CORE"]["null_reasons"]["profit_factor"] == "PHASE_2_CLOSED_TRADE_ANALYTICS_NOT_IMPLEMENTED"


def test_unknown_sleeve_bucket_requires_diagnostics(tmp_path: Path) -> None:
    _seed(tmp_path)
    pnl_path = tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json"
    payload = json.loads(pnl_path.read_text(encoding="utf-8"))
    payload["open_positions"].append({"position_id": "pos-unknown", "symbol": "UNK", "sleeve_id": "UNKNOWN", "quantity": "1", "entry_price": "10", "current_certified_mark": "10", "unrealized_pnl": "0"})
    _write(pnl_path, payload)

    report = build_sleeve_analytics_v1(truth_root=tmp_path, day_utc=DAY)

    assert any(row["sleeve_id"] == "UNKNOWN" for row in report["sleeves"])
    assert report["data_quality"]["missing_sleeve_assignment_count"] == 1
    assert any(row["label"] == "Missing sleeve attribution" for row in report["diagnostics"])
    reconciliation = {row["position_id"]: row for row in report["sleeve_assignment_reconciliation"]}
    assert reconciliation["pos-unknown"]["sleeve_assignment_source"] == "unresolved"
    assert reconciliation["pos-unknown"]["recovered_sleeve"] is None
    assert reconciliation["pos-unknown"]["unknown_reason"] is None
    assert report["status"] == "PARTIAL"
    assert report["summary"]["data_quality_status"] == "PARTIAL"


def test_paper_pnl_recovers_unknown_sleeve_from_candidate_lineage(tmp_path: Path) -> None:
    from ops.aegis.paper_pnl_report_v1 import build_paper_pnl_report_v1

    _write(
        tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "open_positions": [
                {
                    "position_id": "pos-recovered",
                    "candidate_id": "cand-recovered",
                    "symbol": "AAA",
                    "sleeve_id": "UNKNOWN",
                    "candidate_lineage": {"sleeve_id": "CORE"},
                    "quantity": "10",
                    "entry_price": "10",
                    "current_certified_mark": "12",
                    "unrealized_pnl_status": "AVAILABLE",
                    "unrealized_pnl": "20",
                }
            ],
            "closed_positions": [],
        },
    )

    payload = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)

    row = payload["open_positions"][0]
    assert row["position_id"] == "pos-recovered"
    assert row["sleeve_id"] == "CORE"
    assert row["sleeve_assignment_source"] == "candidate_lineage.sleeve_id"
    assert row["recovered_sleeve"] == "CORE"
    assert row["unknown_reason"] is None
    assert payload["pnl_by_sleeve"][0]["sleeve_id"] == "CORE"


def test_write_and_self_check_pass(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = write_sleeve_analytics_v1(truth_root=tmp_path, day_utc=DAY)

    assert path == tmp_path / "reports" / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json"
    assert self_check_main(["--truth-root", str(tmp_path), "--day", DAY]) == 0


def test_sleeve_attribution_self_check_recovers_guarded_lineage_without_symbol_only(tmp_path: Path) -> None:
    from ops.tools.run_aegis_sleeve_attribution_self_check_v1 import build_report

    _write(
        tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "open_positions": [
                {
                    "position_id": "pos-guarded",
                    "candidate_id": "cand-guarded",
                    "symbol": "AAA",
                    "sleeve_id": "UNKNOWN",
                    "candidate_lineage": {"candidate_id": "cand-guarded", "paper_session_id": "PAPER-2026-05-28-0950"},
                    "source_receipt": {"candidate_id": "cand-guarded", "paper_session_id": "PAPER-2026-05-28-0950", "symbol": "AAA"},
                    "quantity": "1",
                    "entry_price": "10",
                    "current_certified_mark": "11",
                    "unrealized_pnl_status": "AVAILABLE",
                    "unrealized_pnl": "1",
                }
            ],
            "closed_positions": [],
        },
    )
    _write(
        tmp_path / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {
            "current_session_candidates": [
                {
                    "candidate_id": "cand-guarded",
                    "candidate_contract_id": "cand-guarded",
                    "paper_session_id": "PAPER-2026-05-28-0950",
                    "symbol": "AAA",
                    "sleeve_id": "CORE",
                }
            ]
        },
    )

    report = build_report(truth_root=tmp_path, day_utc=DAY)

    assert report["ok"] is True
    assert report["before_unknown_open_position_count"] == 0
    assert report["after_unknown_open_position_count"] == 0
    assert report["unsafe_symbol_only_recovery_count"] == 0
    assert report["performance_sleeve_counts"] == {"CORE": 1}
    row = report["reconciliation"][0]
    assert row["recovered_sleeve_id"] == "CORE"
    assert row["symbol_only_match_used"] is False


def test_not_canonical_summary_never_reports_pass(tmp_path: Path) -> None:
    _seed(tmp_path)
    (tmp_path / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json").unlink()

    report = build_sleeve_analytics_v1(truth_root=tmp_path, day_utc=DAY)

    assert report["status"] == "NOT_CANONICAL"
    assert report["summary"]["data_quality_status"] != "PASS"
    assert report["summary"]["data_quality_status"] == "NOT_CANONICAL"
