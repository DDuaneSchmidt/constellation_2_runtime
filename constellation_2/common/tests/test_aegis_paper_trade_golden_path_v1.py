from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.paper_trade_golden_path_v1 import run_paper_trade_golden_path_v1
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1
from ops.aegis.candidate_generation_diagnostics_v1 import build_candidate_generation_diagnostics_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DAY = "2026-05-26"


def _read(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_paper_trade_golden_path_writes_full_simulated_chain(tmp_path: Path) -> None:
    report = run_paper_trade_golden_path_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-26T14:30:00Z")

    assert report["mode"] == "PAPER_REHEARSAL"
    assert report["receipt_type"] == "SIMULATED_PAPER"
    assert report["paper_rehearsal_lifecycle_proven"] is True
    assert report["raw_signal_count"] == 1
    assert report["candidate_count"] == 1
    assert [row["stage"] for row in report["chain"]] == [
        "raw_signal",
        "candidate",
        "operator_execution_queue",
        "manual_trade_packet",
        "manual_execution_receipt",
        "outcome",
    ]

    paths = report["artifact_paths"]
    promoted = _read(paths["promoted_candidate_set"])
    queue = _read(paths["operator_execution_queue"])
    packet = _read(paths["manual_trade_packet"])
    receipt = _read(paths["manual_execution_receipt"])
    attribution = _read(paths["trade_outcome_attribution"])

    validate_against_repo_schema_v1(promoted, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/promoted_candidate_set.v1.schema.json")
    validate_against_repo_schema_v1(queue, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_execution_queue.v1.schema.json")
    validate_against_repo_schema_v1(packet, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json")
    validate_against_repo_schema_v1(receipt, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json")
    validate_against_repo_schema_v1(attribution, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_outcome_attribution.v1.schema.json")

    assert promoted["promoted_candidate_count"] == 1
    assert queue["execution_queue"][0]["queue_status"] == "READY_FOR_MANUAL_ENTRY"
    assert packet["runtime_truth_classification"] == "DRY_RUN_ONLY"
    assert packet["broker_submit_required"] is False
    assert receipt["receipt_type"] == "SIMULATED_PAPER"
    assert receipt["result"] == "SIMULATED_PAPER_RECEIPT_VALID"
    assert receipt["broker_submission_by_aegis"] is False
    assert receipt["autonomous_execution"] is False
    assert attribution["attributions"][0]["realized_pnl"] == "0.00"


def test_paper_rehearsal_candidate_is_not_counted_as_real_candidate_generation(tmp_path: Path) -> None:
    run_paper_trade_golden_path_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-26T14:30:00Z")
    diagnostics = build_candidate_generation_diagnostics_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert diagnostics["total_candidates_generated"] == 0
    assert diagnostics["paper_rehearsal_candidate_count"] == 1
    assert diagnostics["paper_rehearsal_candidates_excluded_from_generation_totals"] is True


def test_paper_trade_golden_path_updates_lifecycle_and_outcome(tmp_path: Path) -> None:
    report = run_paper_trade_golden_path_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-26T14:30:00Z")
    lifecycle = _read(report["artifact_paths"]["candidate_lifecycle"])

    candidate = lifecycle["candidates"][0]
    assert candidate["raw_signal_id"].startswith("raw_signal:paper_rehearsal")
    assert candidate["current_operator_decision"] == "TRADED_MANUALLY"
    assert candidate["manual_trade_receipt_id"].startswith("simulated-paper:")
    assert candidate["outcome_status"] == "OUTCOME_FLAT"
    assert candidate["safety"]["broker_execution_allowed"] is False
    assert candidate["safety"]["autonomous_execution_allowed"] is False


def test_simulated_paper_receipt_does_not_enable_unsafe_runtime_capabilities(tmp_path: Path) -> None:
    run_paper_trade_golden_path_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-26T14:30:00Z")
    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY)
    caps = kernel["dependency_graph"]

    assert kernel["runtime_truth_classification"] == "DRY_RUN_ONLY"
    assert caps["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert caps["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"] is False
    assert caps["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False
    assert caps["BROKER_SUBMIT_TRANSMIT"]["allowed"] is False
    assert caps["LIVE_TRADE_READY"]["allowed"] is False


def test_portal_static_ui_exposes_paper_rehearsal_chain() -> None:
    js = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    assert "renderPaperTradeGoldenPathSection" in js
    assert "PAPER_REHEARSAL / SIMULATED only" in js
    assert "paper_trade_golden_path_v1.chain" in js
