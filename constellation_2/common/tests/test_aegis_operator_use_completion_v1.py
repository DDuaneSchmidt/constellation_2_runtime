from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_manual_trade_packet_v1,
    build_promotion_review_v1,
    build_promoted_sleeve_library_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)
from ops.tools import audit_research_dataset_bindings_v1 as dataset_cli  # noqa: E402
from ops.tools import build_advisor_benchmark_v1 as advisor_cli  # noqa: E402
from ops.tools import build_aegis_operator_status_v1 as status_cli  # noqa: E402
from ops.tools import build_and_print_sleeve_performance_report_v1 as print_perf_cli  # noqa: E402
from ops.tools import promote_validated_hypothesis_to_sleeve_v1 as promote_cli  # noqa: E402
from ops.tools import record_manual_execution_receipt_v1 as receipt_cli  # noqa: E402
from ops.tools import record_trade_outcome_v1 as outcome_cli  # noqa: E402


DAY = "2026-05-15"
GENERATED = "2026-05-15T19:50:00Z"


def _candidate(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "candidate_id": "panic-spy-1",
        "sleeve_id": "PANIC_EXHAUSTION_SLEEVE",
        "source_hypothesis_id": "research-hyp-panic-exhaustion",
        "symbol": "SPY",
        "side": "BUY",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": "500.00",
        "quantity_or_sizing_guidance": "Buy 1 share only if supervised manual paper smoke is approved.",
        "order_type_suggestion": "LIMIT",
        "stop_price": "495.00",
        "stop_logic": "Stop below panic stabilization reference.",
        "risk_per_trade": "5.00",
        "edge_family": "PANIC_EXHAUSTION",
        "confidence": "MEDIUM",
        "inclusion_reason": "Promoted sleeve deterministic test candidate.",
        "exclusion_reason": "",
        "governance_notes": "Manual paper only; no broker automation.",
        "edge_overlap_result": "NO_BLOCKING_OVERLAP",
    }
    row.update(overrides)
    return row


def _promoted_library() -> dict[str, object]:
    return build_promoted_sleeve_library_v1(
        generated_at_utc=GENERATED,
        sleeves=[
            {
                "sleeve_id": "PANIC_EXHAUSTION_SLEEVE",
                "sleeve_name": "Panic Exhaustion Sleeve",
                "promotion_status": "promoted",
                "approved_by_human": True,
                "approved_for_lite_implementation": True,
                "approved_edge_families": ["PANIC_EXHAUSTION"],
                "approved_trade_classes": ["LONG_EQUITY"],
                "expected_regimes": ["PANIC"],
                "source_hypothesis_id": "research-hyp-panic-exhaustion",
                "edge_family": "PANIC_EXHAUSTION",
                "behavioral_thesis": "Panic selling exhaustion can produce short-term reversal.",
                "regime_fit": ["PANIC"],
                "instrument_universe": ["SPY"],
                "entry_logic": "Enter only from governed manual packet.",
                "exit_logic": "Exit through manual receipt and outcome ledger.",
                "stop_logic": "Protective stop below stabilization reference.",
                "sizing_logic": "One-share supervised manual paper smoke.",
                "invalidation_logic": "Block if stop/risk/entry/sizing is missing.",
                "known_failure_modes": ["volatility expansion"],
                "overlap_tags": ["US_EQUITY_BETA"],
                "promotion_evidence_path": "/offline/research/evidence/panic.json",
                "production_status": "manual_paper_only",
                "created_at": GENERATED,
                "updated_at": GENERATED,
                "archived": False,
            }
        ],
    )


def _write_packet(root: Path, *, candidate: dict[str, object] | None = None) -> dict[str, object]:
    library = _promoted_library()
    packet = build_manual_trade_packet_v1(
        packet_id="packet:operator-proof",
        run_id="operator-proof",
        date=DAY,
        generated_at_utc=GENERATED,
        regime_state="PANIC",
        trade_candidates=[candidate or _candidate()],
        promoted_sleeve_library=library,
    )
    validate_research_lab_artifact_v1(packet)
    write_research_lab_artifact_v1(truth_root=root, day_utc=DAY, payload=packet)
    write_research_lab_artifact_v1(truth_root=root, day_utc=DAY, payload=library)
    return packet


def test_manual_receipt_cli_writes_valid_receipt_without_broker(tmp_path: Path) -> None:
    packet = _write_packet(tmp_path)
    trade_id = packet["trade_candidates"][0]["recommended_trade_id"]

    receipt_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--source_packet_id",
            trade_id,
            "--symbol",
            "SPY",
            "--side",
            "BUY",
            "--quantity",
            "1",
            "--fill_price",
            "500.25",
            "--fill_timestamp_utc",
            "2026-05-15T19:55:00Z",
            "--stop_entered",
            "yes",
            "--stop_price",
            "495.00",
            "--notes",
            "supervised paper smoke receipt",
        ]
    )

    receipt_path = next(tmp_path.rglob("manual_execution_receipt.v1.json"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["recommended_trade_id"] == trade_id
    assert receipt["stop_order_entered"] is True
    assert receipt["broker_submit_required"] is False


def test_outcome_cli_writes_valid_outcome_and_performance_summary(tmp_path: Path, capsys) -> None:
    packet = _write_packet(tmp_path)
    trade_id = packet["trade_candidates"][0]["recommended_trade_id"]
    receipt_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--source_packet_id",
            trade_id,
            "--symbol",
            "SPY",
            "--side",
            "BUY",
            "--quantity",
            "1",
            "--fill_price",
            "500.25",
            "--fill_timestamp_utc",
            "2026-05-15T19:55:00Z",
            "--stop_entered",
            "yes",
            "--stop_price",
            "495.00",
        ]
    )
    outcome_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--trade_id",
            trade_id,
            "--exit_price",
            "505.25",
            "--exit_timestamp_utc",
            "2026-05-15T20:30:00Z",
            "--outcome_status",
            "WIN",
            "--notes",
            "closed manually",
        ]
    )

    outcome_path = next(tmp_path.rglob("outcome_ledger.v1.json"))
    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    assert outcome["outcomes"][0]["return_pct"] == "0.9995"
    assert outcome["runtime_mutation_allowed"] is False

    print_perf_cli.main(["--truth_root", str(tmp_path), "--day", DAY])
    printed = capsys.readouterr().out
    assert "AEGIS SLEEVE PERFORMANCE REPORT" in printed
    assert "PANIC_EXHAUSTION_SLEEVE" in printed
    assert "Missing receipts: 0" in printed
    assert "Missing outcomes: 0" in printed
    assert "Realized return: 0.9995" in printed


def test_demo_or_dry_run_packet_cannot_be_actionable(tmp_path: Path) -> None:
    packet = _write_packet(tmp_path, candidate=_candidate(demo_mode=True, dry_run_only=True))
    trade = packet["trade_candidates"][0]

    assert packet["runtime_truth_classification"] == "DEMO_ONLY"
    assert trade["runtime_truth_classification"] == "DEMO_ONLY"
    assert trade["actionable"] is False
    assert "DEMO_ONLY_NOT_ACTIONABLE" in trade["do_not_trade_blockers"]
    assert "DRY_RUN_ONLY_NOT_ACTIONABLE" in trade["do_not_trade_blockers"]


def test_dataset_gap_and_operator_status_artifacts_are_operator_readable(tmp_path: Path) -> None:
    _write_packet(tmp_path)
    dataset_cli.main(["--truth_root", str(tmp_path), "--day_utc", DAY])
    status_cli.main(["--truth_root", str(tmp_path), "--day_utc", DAY])

    gap_path = next(tmp_path.rglob("research_dataset_gap.v1.json"))
    gaps = json.loads(gap_path.read_text(encoding="utf-8"))
    assert {row["dataset_name"] for row in gaps["dataset_gaps"]} >= {"price_data", "volatility_data", "breadth_data"}
    assert all(row["current_status"] == "MISSING" for row in gaps["dataset_gaps"])
    assert gaps["broker_submit_required"] is False

    status_path = next(tmp_path.rglob("aegis_operator_status.v1.json"))
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["manual_execution_only"] is True
    assert status["broker_submit_required"] is False
    assert status["dataset_blockers"]
    assert status["event_monitor_schedule_status"]["source_configured"] is True
    assert status["event_monitor_schedule_status"]["broker_submit_required"] is False
    assert status["last_event_data_freshness_status"] == "UNKNOWN"
    assert status["last_triggered_event_count"] == 0
    assert status["last_blocked_event_count"] == 0
    assert "Review manual trade packet" in status["next_operator_action"]


def test_advisor_benchmark_comparison_is_calculated(tmp_path: Path) -> None:
    advisor_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--benchmark_name",
            "Fee Advisor",
            "--period_start",
            "2026-05-01",
            "--period_end",
            "2026-05-15",
            "--gross_return",
            "1.50",
            "--fee_rate",
            "0.25",
            "--aegis_return",
            "2.00",
        ]
    )

    path = next(tmp_path.rglob("advisor_benchmark.v1.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["net_return"] == "1.25"
    assert payload["difference"] == "0.75"
    assert payload["fee_drag"] == "0.25"
    assert payload["broker_submit_required"] is False


def test_promotion_cli_writes_promoted_sleeve_library_without_trade_creation(tmp_path: Path) -> None:
    promotion = build_promotion_review_v1(
        promotion_id="promotion:panic",
        hypothesis_id="research-hyp-panic-exhaustion",
        generated_at_utc=GENERATED,
        test_plan={
            "completed_stages": [
                "definition_check",
                "duplicate_overlap_check",
                "exploratory_backtest",
                "regime_segmentation",
                "robustness_check",
                "transaction_friction_check",
                "out_of_sample_check",
                "failure_mode_review",
                "edge_overlap_review",
                "promotion_review",
            ]
        },
        friction_adjusted_result="positive",
        out_of_sample_support="supported",
        regime_notes="PANIC regime only.",
        failure_mode_notes="Fails during continued volatility expansion.",
        edge_overlap_review="No blocking overlap.",
        approved_by_human=True,
        approval_reason_codes=["HUMAN_APPROVED"],
    )
    promotion_path = write_research_lab_artifact_v1(truth_root=tmp_path, day_utc=DAY, payload=promotion)

    promote_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--day_utc",
            DAY,
            "--promotion_json",
            str(promotion_path),
            "--sleeve_id",
            "PANIC_EXHAUSTION_SLEEVE",
            "--edge_family",
            "PANIC_EXHAUSTION",
            "--trade_class",
            "LONG_EQUITY",
            "--expected_regimes",
            "PANIC",
            "--instrument_universe",
            "SPY,QQQ",
        ]
    )

    library_path = next(tmp_path.rglob("promoted_sleeve_library.v1.json"))
    library = json.loads(library_path.read_text(encoding="utf-8"))
    sleeve = library["promoted_sleeves"][0]
    assert sleeve["sleeve_id"] == "PANIC_EXHAUSTION_SLEEVE"
    assert sleeve["approved_by_human"] is True
    assert sleeve["approved_for_lite_implementation"] is True
    assert library["broker_submit_required"] is False
    assert not (tmp_path / "reports" / "manual_trade_packet_v1").exists()
