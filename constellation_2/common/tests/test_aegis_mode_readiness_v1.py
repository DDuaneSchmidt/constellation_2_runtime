from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.mode_readiness_v1 import build_mode_readiness_v1, write_mode_readiness_v1
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1

DAY = "2026-05-27"
NOW = "2026-05-27T14:00:00Z"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _artifact(artifact_id: str, **extra) -> dict:
    return {"schema_id": artifact_id, "artifact_id": artifact_id, "day_utc": DAY, "generated_at_utc": NOW, **extra}


def _seed_graph(root: Path, status: str = "READY") -> None:
    _write(root / f"reports/aegis_verified_runtime_graph_v1/{DAY}/verified_runtime_graph.v1.json", _artifact("aegis_verified_runtime_graph_v1", graph_status=status))


def _seed_paper_mode(root: Path, *, candidate_contracts: bool = True, queue_count: int = 1, paper_ledger: bool = True) -> None:
    _seed_graph(root)
    if candidate_contracts:
        _write(root / f"reports/aegis_candidate_contracts_v1/{DAY}/candidate_contracts.v1.json", _artifact("aegis_candidate_contracts_v1", candidate_contracts=[{"candidate_id": "c1"}], candidates_created=1))
    _write(root / f"reports/aegis_paper_review_queue_v1/{DAY}/paper_review_queue.v1.json", _artifact("aegis_paper_review_queue_v1", candidate_count=queue_count, status_counts={"AWAITING_REVIEW": queue_count}))
    if paper_ledger:
        _write(root / f"reports/aegis_paper_position_ledger_v1/{DAY}/paper_position_ledger.v1.json", _artifact("aegis_paper_position_ledger_v1", open_position_count=0, positions=[]))
        events = root / f"reports/aegis_paper_position_events_v1/{DAY}/paper_position_events.v1.jsonl"
        events.parent.mkdir(parents=True, exist_ok=True)
        events.write_text("", encoding="utf-8")
    _write(root / f"reports/aegis_paper_trade_outcomes_v1/{DAY}/paper_trade_outcomes.v1.json", _artifact("aegis_paper_trade_outcomes_v1", outcomes=[]))
    _write(root / f"reports/aegis_exit_recommendations_v1/{DAY}/exit_recommendations.v1.json", _artifact("aegis_exit_recommendations_v1", recommendations=[]))
    _write(root / f"reports/aegis_context_requirement_profile_v1/{DAY}/context_requirement_profile.v1.json", _artifact("aegis_context_requirement_profile_v1"))
    _write(root / f"reports/aegis_market_data_coverage_v1/{DAY}/market_data_coverage.v1.json", _artifact("aegis_market_data_coverage_v1", status="READY"))
    _write(root / f"reports/aegis_canonical_operator_state_v1/{DAY}/canonical_operator_state.v1.json", _artifact("aegis_canonical_operator_state_v1"))
    _write(root / f"reports/aegis_sleeve_performance_truth_v1/{DAY}/sleeve_performance_truth.v1.json", _artifact("aegis_sleeve_performance_truth_v1", sleeves=[]))


def _active(payload: dict) -> dict:
    return payload["modes_by_id"]["HUMAN_REVIEWED_PAPER_MODE"]


def test_graph_ready_full_platform_blocked_paper_mode_ready_is_valid(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path)
    payload = build_mode_readiness_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        generated_at_utc=NOW,
        global_missing_or_stale_sources=[{"artifact_id": "research_task_queue", "status": "MISSING", "expected_path": "research_lab/..."}],
    )

    assert payload["verified_graph_status"] == "READY"
    assert payload["active_mode_readiness_status"] == "READY"
    assert payload["modes_by_id"]["FULL_PLATFORM_MODE"]["readiness_status"] == "BLOCKED"
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_submit_transmit_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_missing_research_task_queue_and_manual_trade_packet_do_not_block_paper_mode(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path)
    payload = build_mode_readiness_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    blockers = {row["artifact_id"] for row in _active(payload)["blocking_requirements"]}
    assert "research_task_queue" not in blockers
    assert "manual_trade_packet" not in blockers
    assert _active(payload)["readiness_status"] == "READY"


def test_missing_paper_position_ledger_blocks_paper_mode(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path, paper_ledger=False)
    payload = build_mode_readiness_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert _active(payload)["readiness_status"] == "BLOCKED"
    assert "paper_position_ledger" in {row["artifact_id"] for row in _active(payload)["blocking_requirements"]}


def test_missing_candidate_contracts_block_only_without_carried_forward_reviewable_candidates(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path, candidate_contracts=False, queue_count=2)
    carried = build_mode_readiness_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    assert _active(carried)["readiness_status"] == "READY"

    root2 = tmp_path / "empty"
    _seed_paper_mode(root2, candidate_contracts=False, queue_count=0)
    blocked = build_mode_readiness_v1(truth_root=root2, day_utc=DAY, generated_at_utc=NOW)
    assert _active(blocked)["readiness_status"] == "BLOCKED"
    assert "candidate_contracts" in {row["artifact_id"] for row in _active(blocked)["blocking_requirements"]}


def test_runtime_truth_preserves_global_classification_and_adds_active_mode(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path)
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=payload)

    assert payload["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert payload["active_mode"] == "HUMAN_REVIEWED_PAPER_MODE"
    assert payload["active_mode_readiness_status"] == "READY"
    assert payload["trade_advice_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert Path(paths["mode_readiness"]).exists()


def test_full_platform_mode_still_blocks_on_full_artifact_set(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path)
    payload = build_mode_readiness_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, global_missing_or_stale_sources=[{"artifact_id": "alert_transport_proof", "status": "MISSING", "expected_path": "reports/alert_transport_proof_v1/..."}])

    full = payload["modes_by_id"]["FULL_PLATFORM_MODE"]
    assert full["readiness_status"] == "BLOCKED"
    assert full["blocking_requirements"][0]["artifact_id"] == "alert_transport_proof"


def test_write_mode_readiness_artifact(tmp_path: Path) -> None:
    _seed_paper_mode(tmp_path)
    payload = build_mode_readiness_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    paths = write_mode_readiness_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()
