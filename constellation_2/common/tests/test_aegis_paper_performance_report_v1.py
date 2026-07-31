from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.advisor_benchmark_v1 import AdvisorBenchmarkValidationError, write_advisor_benchmark_snapshot_v1  # noqa: E402
from ops.aegis.paper_performance_report_v1 import build_paper_performance_report_v1  # noqa: E402
from ops.tools.run_aegis_performance_self_check_v1 import main as performance_self_check_main  # noqa: E402

DAY = "2026-05-28"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path) -> None:
    _write(
        root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json",
        {
            "schema_id": "aegis_paper_pnl_report",
            "generated_at_utc": f"{DAY}T21:00:00Z",
            "open_position_count": 1,
            "closed_position_count": 1,
            "realized_pnl": "75",
            "unrealized_pnl": "25",
            "total_paper_pnl": "100",
            "data_quality_status": "PASS",
            "open_positions": [
                {
                    "position_id": "pos-open",
                    "symbol": "AAA",
                    "sleeve_id": "CORE",
                    "quantity": "10",
                    "entry_price": "10",
                    "current_certified_mark": "12.5",
                    "unrealized_pnl": "25",
                    "unrealized_pnl_status": "AVAILABLE",
                }
            ],
            "closed_positions": [
                {
                    "position_id": "pos-closed",
                    "symbol": "BBB",
                    "sleeve_id": "GROWTH",
                    "quantity": "5",
                    "entry_price": "20",
                    "exit_price": "35",
                    "realized_pnl": "75",
                }
            ],
        },
    )
    _write(
        root / "reports" / "aegis_daily_paper_performance_v1" / DAY / "daily_paper_performance.v1.json",
        {
            "schema_id": "aegis_daily_paper_performance",
            "generated_at_utc": f"{DAY}T21:05:00Z",
            "total_open_positions": 1,
            "total_closed_positions": 1,
            "realized_pnl": "75",
            "unrealized_pnl": "25",
            "total_paper_pnl": "100",
            "data_quality_status": "PASS",
            "best_paper_positions": [{"symbol": "BBB", "total_pnl": "75"}],
            "worst_paper_positions": [{"symbol": "AAA", "total_pnl": "25"}],
        },
    )
    _write(
        root / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json",
        {
            "schema_id": "aegis_sleeve_performance_truth",
            "generated_at_utc": f"{DAY}T21:10:00Z",
            "data_quality_status": "PASS",
            "totals": {
                "realized_pnl": "75",
                "unrealized_pnl": "25",
                "total_paper_pnl": "100",
                "open_paper_position_count": 1,
                "closed_paper_position_count": 1,
                "win_rate": 1.0,
            },
            "sleeves": [
                {
                    "sleeve_id": "CORE",
                    "realized_pnl": "0",
                    "unrealized_pnl": "25",
                    "total_paper_pnl": "25",
                    "open_paper_position_count": 1,
                    "closed_paper_position_count": 0,
                    "win_rate": 0,
                    "data_quality_status": "PASS",
                },
                {
                    "sleeve_id": "GROWTH",
                    "realized_pnl": "75",
                    "unrealized_pnl": "0",
                    "total_paper_pnl": "75",
                    "open_paper_position_count": 0,
                    "closed_paper_position_count": 1,
                    "win_rate": 1.0,
                    "data_quality_status": "PASS",
                },
            ],
        },
    )
    _write(
        root / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {
            "schema_id": "aegis_candidate_lifecycle_projection",
            "generated_at_utc": f"{DAY}T21:15:00Z",
            "current_session_candidates": [
                {"candidate_id": "cand-approved", "candidate_lifecycle_state": "APPROVED_FOR_PAPER"},
                {"candidate_id": "cand-rejected", "candidate_lifecycle_state": "REJECTED"},
                {"candidate_id": "cand-deferred", "candidate_lifecycle_state": "DEFERRED"},
            ],
        },
    )
    _write(
        root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {"schema_id": "aegis_paper_position_ledger", "paper_position_ledger_mismatches": []},
    )


def test_performance_report_normalizes_existing_read_models(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "ready"
    assert payload["rebuilt_on_request"] is False
    assert "paper_pnl_report_v1" in payload["source_read_models"]
    assert payload["overview"]["total_pnl"] == 100.0
    assert payload["overview"]["realized_pnl"] == 75.0
    assert payload["overview"]["unrealized_pnl"] == 25.0
    assert payload["overview"]["win_rate"] == 1.0
    assert {row["name"] for row in payload["sleeves"]} == {"CORE", "GROWTH"}
    assert [row["symbol"] for row in payload["position_attribution"]] == ["AAA", "BBB"]
    assert payload["candidate_outcomes"]["available"] is True
    assert payload["candidate_outcomes"]["approved"]["count"] == 1
    assert payload["benchmarks"]["advisor"]["available"] is False
    assert payload["benchmarks"]["advisor"]["status"] == "not_connected"
    assert all(row["classification"] != "ENGINEERING" for row in payload["diagnostics"])
    assert payload["benchmarks"]["aegis_paper"]["status"] == "ready"
    assert payload["benchmarks"]["aegis_paper"]["capital_basis"] == 200.0
    assert payload["benchmarks"]["aegis_paper"]["realized_return_pct"] == 37.5
    assert payload["benchmarks"]["aegis_paper"]["unrealized_return_pct"] == 12.5
    assert payload["benchmarks"]["aegis_paper"]["total_return_pct"] == 50.0
    assert payload["benchmarks"]["aegis_paper"]["return_pct"] == 50.0
    assert payload["position_attribution"][0]["current_or_exit_price"] == 12.5
    assert payload["safety"]["read_only"] is True
    assert payload["safety"]["no_accounting_engine_created"] is True
    assert payload["safety"]["broker_execution_allowed"] is False


def test_performance_report_degrades_when_read_models_are_missing(tmp_path: Path) -> None:
    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "unavailable"
    assert payload["overview"]["total_pnl"] is None
    assert payload["sleeves"] == []
    assert payload["position_attribution"] == []
    assert payload["candidate_outcomes"]["available"] is False
    assert any(row["label"] == "paper_pnl_report_v1 unavailable" for row in payload["diagnostics"])
    assert any(row["label"] == "Advisor benchmark not configured" for row in payload["configuration_diagnostics"])
    assert all(row["classification"] in {"PERFORMANCE_CRITICAL", "PERFORMANCE_WARNING"} for row in payload["diagnostics"])


def test_performance_report_does_not_mutate_truth_root(tmp_path: Path) -> None:
    _seed(tmp_path)
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*") if path.is_file())

    build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*") if path.is_file())
    assert after == before


def test_performance_report_uses_read_model_values_without_recomputing_position_totals(tmp_path: Path) -> None:
    _seed(tmp_path)
    pnl_path = tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json"
    payload = json.loads(pnl_path.read_text(encoding="utf-8"))
    payload["total_paper_pnl"] = "123.45"
    payload["open_positions"][0]["unrealized_pnl"] = "999"
    _write(pnl_path, payload)
    daily_path = tmp_path / "reports" / "aegis_daily_paper_performance_v1" / DAY / "daily_paper_performance.v1.json"
    daily = json.loads(daily_path.read_text(encoding="utf-8"))
    daily["total_paper_pnl"] = "123.45"
    _write(daily_path, daily)

    report = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert report["overview"]["total_pnl"] == 123.45
    assert report["position_attribution"][0]["unrealized_pnl"] == 999.0
    assert report["position_attribution"][0]["total_pnl"] == 999.0
    assert report["position_attribution"][0]["return_pct"] == 999.0


def test_performance_report_is_partial_when_one_read_model_is_missing(tmp_path: Path) -> None:
    _seed(tmp_path)
    missing = tmp_path / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json"
    missing.unlink()

    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "partial"
    assert payload["rebuilt_on_request"] is False
    assert any(row["label"] == "sleeve_performance_truth_v1 unavailable" for row in payload["diagnostics"])


def test_advisor_benchmark_accepts_valid_ytd_snapshot_and_performance_latest_reads_it(tmp_path: Path) -> None:
    _seed(tmp_path)
    result = write_advisor_benchmark_snapshot_v1(
        truth_root=tmp_path,
        payload={
            "as_of_date": "2026-03-31",
            "period_type": "YTD",
            "return_pct": 4.7,
            "source": "Advisor Q1 statement",
            "notes": "Manual advisor statement input.",
        },
    )

    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert result["ok"] is True
    advisor = payload["benchmarks"]["advisor"]
    assert advisor["available"] is True
    assert advisor["return_pct"] == 4.7
    assert advisor["period_type"] == "YTD"
    assert advisor["as_of_date"] == "2026-03-31"
    assert advisor["source"] == "Advisor Q1 statement"
    assert advisor["notes"] == "Manual advisor statement input."
    assert advisor["stale"] is False
    assert advisor["status"] == "ready"


def test_advisor_benchmark_accepts_negative_return(tmp_path: Path) -> None:
    result = write_advisor_benchmark_snapshot_v1(
        truth_root=tmp_path,
        payload={"as_of_date": "2026-03-31", "period_type": "QTD", "return_pct": -2.5, "source": "Advisor", "notes": ""},
    )

    assert result["snapshot"]["return_pct"] == -2.5


def test_advisor_benchmark_rejects_invalid_date_period_and_return(tmp_path: Path) -> None:
    for payload, field in [
        ({"as_of_date": "2026-99-99", "period_type": "YTD", "return_pct": 1}, "as_of_date"),
        ({"as_of_date": "2026-03-31", "period_type": "MTD", "return_pct": 1}, "period_type"),
        ({"as_of_date": "2026-03-31", "period_type": "YTD", "return_pct": "abc"}, "return_pct"),
    ]:
        try:
            write_advisor_benchmark_snapshot_v1(truth_root=tmp_path, payload=payload)
        except AdvisorBenchmarkValidationError as exc:
            assert field in exc.field_errors
        else:
            raise AssertionError(f"expected validation error for {field}")


def test_advisor_benchmark_upserts_same_date_period_and_appends_different_date(tmp_path: Path) -> None:
    first = write_advisor_benchmark_snapshot_v1(
        truth_root=tmp_path,
        payload={"as_of_date": "2026-03-31", "period_type": "YTD", "return_pct": 1.0, "source": "First", "notes": ""},
    )
    second = write_advisor_benchmark_snapshot_v1(
        truth_root=tmp_path,
        payload={"as_of_date": "2026-03-31", "period_type": "YTD", "return_pct": 2.0, "source": "Second", "notes": "updated"},
    )
    third = write_advisor_benchmark_snapshot_v1(
        truth_root=tmp_path,
        payload={"as_of_date": "2026-04-30", "period_type": "YTD", "return_pct": 3.0, "source": "Third", "notes": "new date"},
    )

    same_day_payload = json.loads(Path(second["artifact_path"]).read_text(encoding="utf-8"))
    assert len(same_day_payload["snapshots"]) == 1
    assert same_day_payload["snapshots"][0]["return_pct"] == 2.0
    assert same_day_payload["snapshots"][0]["source"] == "Second"
    assert Path(first["artifact_path"]) == Path(second["artifact_path"])
    assert Path(third["artifact_path"]) != Path(second["artifact_path"])


def test_performance_report_has_missing_and_stale_advisor_diagnostics(tmp_path: Path) -> None:
    _seed(tmp_path)
    missing = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)
    assert any(row["label"] == "Advisor benchmark not configured" for row in missing["configuration_diagnostics"])

    write_advisor_benchmark_snapshot_v1(
        truth_root=tmp_path,
        payload={"as_of_date": "2025-12-31", "period_type": "Annual", "return_pct": 6.0, "source": "Old advisor statement", "notes": ""},
    )
    stale = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert stale["benchmarks"]["advisor"]["available"] is True
    assert stale["benchmarks"]["advisor"]["stale"] is True
    assert stale["benchmarks"]["advisor"]["status"] == "unavailable"
    stale_diag = next(row for row in stale["diagnostics"] if row["label"] == "Advisor benchmark stale")
    assert stale_diag["classification"] == "PERFORMANCE_WARNING"
    assert stale_diag["diagnostic_type"] == "stale_benchmark"


def test_performance_report_exposes_partial_certified_mark_coverage(tmp_path: Path) -> None:
    _seed(tmp_path)
    pnl_path = tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json"
    pnl = json.loads(pnl_path.read_text(encoding="utf-8"))
    pnl["unrealized_pnl"] = "NOT_CANONICAL"
    pnl["total_paper_pnl"] = "NOT_CANONICAL"
    pnl["full_portfolio_pnl_status"] = "NOT_CANONICAL"
    pnl["certified_unrealized_pnl"] = "25"
    pnl["certified_total_paper_pnl"] = "100"
    pnl["missing_mark_count"] = 1
    pnl["missing_mark_symbols"] = ["ZZZ"]
    pnl["mark_coverage"] = {"open_position_count": 2, "marked_position_count": 1, "missing_mark_position_count": 1, "mark_coverage_by_position_pct": 50.0, "mark_coverage_by_entry_notional_pct": 40.0, "missing_symbols": ["ZZZ"]}
    pnl["data_quality_explanation"] = "Full portfolio P&L is not canonical because one or more open positions lack certified current marks."
    _write(pnl_path, pnl)

    report = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert report["overview"]["total_pnl"] is None
    assert report["overview"]["full_portfolio_pnl_status"] == "NOT_CANONICAL"
    assert report["overview"]["certified_unrealized_pnl"] == 25.0
    assert report["overview"]["mark_coverage"]["mark_coverage_by_position_pct"] == 50.0
    assert report["overview"]["missing_mark_symbols"] == ["ZZZ"]
    mark_diag = next(row for row in report["diagnostics"] if row["label"] == "Missing marks")
    assert mark_diag["classification"] == "PERFORMANCE_CRITICAL"
    assert mark_diag["diagnostic_type"] == "missing_marks"



def test_runtime_engineering_diagnostics_are_runtime_health_not_performance(tmp_path: Path) -> None:
    _seed(tmp_path)
    _write(
        tmp_path / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json",
        {"runtime_truth_classification": "PARTIAL_CONTEXT", "highest_readiness_layer": "BLOCKED"},
    )
    _write(
        tmp_path / "reports" / "aegis_verified_runtime_graph_v1" / DAY / "verified_runtime_graph.v1.json",
        {"graph_status": "BLOCKED"},
    )

    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert not any(row.get("classification") == "ENGINEERING" for row in payload["diagnostics"])
    runtime_rows = payload["runtime_health"]["diagnostics"]
    assert {row["diagnostic_type"] for row in runtime_rows} >= {"runtime_truth_partial", "readiness_layer_blocked", "verified_runtime_graph_not_ready"}
    assert all(row["classification"] == "ENGINEERING" for row in runtime_rows)


def test_missing_sleeve_assignment_is_performance_critical(tmp_path: Path) -> None:
    _seed(tmp_path)
    pnl_path = tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json"
    pnl = json.loads(pnl_path.read_text(encoding="utf-8"))
    pnl["open_positions"][0]["sleeve_id"] = "UNKNOWN"
    _write(pnl_path, pnl)

    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)

    diag = next(row for row in payload["diagnostics"] if row["diagnostic_type"] == "missing_sleeve_assignments")
    assert diag["classification"] == "PERFORMANCE_CRITICAL"
    assert diag["severity"] == "error"

def test_performance_self_check_passes_consistent_mark_contract(tmp_path: Path) -> None:
    _seed(tmp_path)
    _write(
        tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "open_positions": [
                {"position_id": "pos-open", "symbol": "AAA", "mark_certification_status": "CERTIFIED"}
            ],
            "paper_position_ledger_mismatches": [],
        },
    )
    pnl_path = tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json"
    pnl = json.loads(pnl_path.read_text(encoding="utf-8"))
    pnl["full_portfolio_pnl_status"] = "CANONICAL"
    pnl["certified_unrealized_pnl"] = "25"
    pnl["certified_total_paper_pnl"] = "100"
    pnl["mark_coverage"] = {"open_position_count": 1, "marked_position_count": 1, "missing_mark_position_count": 0, "mark_coverage_by_position_pct": 100.0, "mark_coverage_by_entry_notional_pct": 100.0, "missing_symbols": []}
    pnl["missing_mark_count"] = 0
    pnl["data_quality_explanation"] = "Full portfolio P&L is canonical because every open position has a certified current mark."
    _write(pnl_path, pnl)

    assert performance_self_check_main(["--truth-root", str(tmp_path), "--day", DAY]) == 0


def test_performance_report_computes_position_totals_and_returns(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_paper_performance_report_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["position_attribution"][0]
    assert row["unrealized_pnl"] == 25.0
    assert row["certified_unrealized_pnl"] == 25.0
    assert row["total_pnl"] == 25.0
    assert row["return_pct"] == 25.0
    assert row["mark_coverage_by_position_pct"] == 100.0
