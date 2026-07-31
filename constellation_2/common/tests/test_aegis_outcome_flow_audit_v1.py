from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.outcome_flow_audit_v1 import build_outcome_flow_audit_v1, write_outcome_flow_audit_v1  # noqa: E402

DAY = "2026-06-03"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(
    root: Path,
    *,
    sleeve_id: str = "SLEEVE_A",
    entry_day: str = "2026-06-02",
    open_count: int = 1,
    closed_count: int = 0,
    exit_policy: dict | None = None,
    exit_recommendation: str = "HOLD",
    auto_closed_count: int = 0,
) -> None:
    exit_policy = exit_policy if exit_policy is not None else {"stop_loss_pct": 0.05, "take_profit_pct": 0.1, "max_hold_days": 20}
    open_positions = [
        {
            "position_id": f"open-{idx}",
            "sleeve_id": sleeve_id,
            "symbol": "AAA",
            "entry_time": f"{entry_day}T14:00:00Z",
            "originating_day": entry_day,
            "current_status": "OPEN",
        }
        for idx in range(open_count)
    ]
    closed_positions = [
        {
            "position_id": f"closed-{idx}",
            "sleeve_id": sleeve_id,
            "symbol": "AAA",
            "entry_time": "2026-05-01T14:00:00Z",
            "exit_time": "2026-05-10T14:00:00Z",
            "current_status": "CLOSED",
        }
        for idx in range(closed_count)
    ]
    _write(
        root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "positions": open_positions + closed_positions,
            "open_positions": open_positions,
            "closed_positions": closed_positions,
        },
    )
    _write(
        root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json",
        {"schema_id": "aegis_paper_pnl_report", "day_utc": DAY},
    )
    _write(
        root / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json",
        {
            "schema_id": "aegis_sleeve_performance_truth",
            "day_utc": DAY,
            "sleeves": [
                {
                    "sleeve_id": sleeve_id,
                    "factory_classification": "TECHNICAL_STRATEGY",
                    "open_paper_position_count": open_count,
                    "closed_paper_position_count": closed_count,
                }
            ],
        },
    )
    _write(
        root / "reports" / "aegis_sleeve_evidence_certification_v1" / DAY / "sleeve_evidence_certification.v1.json",
        {
            "schema_id": "aegis_sleeve_evidence_certification",
            "day_utc": DAY,
            "sleeves": [{"sleeve_id": sleeve_id, "factory_classification": "TECHNICAL_STRATEGY", "closed_position_count": closed_count}],
        },
    )
    _write(
        root / "reports" / "aegis_exit_recommendations_v1" / DAY / "exit_recommendations.v1.json",
        {
            "schema_id": "aegis_exit_recommendations",
            "day_utc": DAY,
            "rows": [
                {
                    "position_id": row["position_id"],
                    "sleeve_id": sleeve_id,
                    "exit_recommendation": exit_recommendation,
                    "exit_trigger": "NO_EXIT_RULE_TRIGGERED" if exit_recommendation == "HOLD" else "TAKE_PROFIT_THRESHOLD_REACHED",
                    "policy": exit_policy,
                }
                for row in open_positions
            ],
        },
    )
    _write(
        root / "reports" / "aegis_paper_outcome_auto_closure_v1" / DAY / "paper_outcome_auto_closure.v1.json",
        {
            "schema_id": "aegis_paper_outcome_auto_closure",
            "day_utc": DAY,
            "rows": [
                {
                    "position_id": f"auto-{idx}",
                    "sleeve_id": sleeve_id,
                    "auto_closure_state": "AUTO_CLOSED_PAPER_OUTCOME",
                    "exit_trigger": "TAKE_PROFIT_THRESHOLD_REACHED",
                    "exit_recommendation": "EXIT_TAKE_PROFIT",
                }
                for idx in range(auto_closed_count)
            ],
        },
    )
    _write(
        root / "reports" / "aegis_outcome_validation_v1" / DAY / "outcome_validation.v1.json",
        {"schema_id": "aegis_outcome_validation", "day_utc": DAY, "summary": {"closed_outcome_count": auto_closed_count + closed_count}},
    )


def _row(payload: dict) -> dict:
    return payload["sleeves"][0]


def test_recent_open_positions_produce_early_observation(tmp_path: Path) -> None:
    _seed(tmp_path, entry_day="2026-06-02")
    row = _row(build_outcome_flow_audit_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["evidence_accumulation_status"] == "EARLY_OBSERVATION"
    assert "OPEN_POSITIONS_TOO_RECENT_FOR_OUTCOME_FLOW" in row["blocking_reasons"]


def test_old_open_positions_with_no_exits_produce_no_outcome_flow_when_exit_logic_missing(tmp_path: Path) -> None:
    _seed(tmp_path, entry_day="2026-05-01", exit_policy={})
    row = _row(build_outcome_flow_audit_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["evidence_accumulation_status"] == "NO_OUTCOME_FLOW"
    assert "MISSING_EXIT_LOGIC" in row["blocking_reasons"]


def test_exit_records_missing_from_evidence_produce_pipeline_broken(tmp_path: Path) -> None:
    _seed(tmp_path, entry_day="2026-05-01", exit_recommendation="EXIT_TAKE_PROFIT", auto_closed_count=1)
    row = _row(build_outcome_flow_audit_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["evidence_accumulation_status"] == "EXIT_PIPELINE_BROKEN"
    assert row["closure_records_not_in_evidence_certification"] > 0
    assert "EXIT_RECORDS_EXIST_BUT_LEDGER_OR_EVIDENCE_HAS_ZERO_CLOSED_SAMPLES" in row["blocking_reasons"]


def test_intentional_long_holding_period_produces_long_holding_period_expected(tmp_path: Path) -> None:
    _seed(tmp_path, entry_day="2026-05-25", exit_policy={"max_hold_days": 90, "minimum_holding_period": 30})
    row = _row(build_outcome_flow_audit_v1(truth_root=tmp_path, day_utc=DAY))

    assert row["evidence_accumulation_status"] == "LONG_HOLDING_PERIOD_EXPECTED"
    assert "HOLDING_PERIOD_POLICY_EXCEEDS_CURRENT_OBSERVATION_AGE" in row["blocking_reasons"]


def test_writes_json_and_markdown_artifacts(tmp_path: Path) -> None:
    _seed(tmp_path)
    paths = write_outcome_flow_audit_v1(truth_root=tmp_path, day_utc=DAY)

    assert paths["json"] == tmp_path / "reports" / "aegis_outcome_flow_audit_v1" / DAY / "outcome_flow_audit.v1.json"
    assert paths["markdown"] == tmp_path / "reports" / "aegis_outcome_flow_audit_v1" / DAY / "outcome_flow_audit_summary.md"
    assert json.loads(paths["json"].read_text(encoding="utf-8"))["schema_id"] == "aegis_outcome_flow_audit"
    assert "no investable-edge claims" in paths["markdown"].read_text(encoding="utf-8")
