from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.lite_migration_equivalence_v1 import build_lite_migration_equivalence_v1
from ops.aegis.lite_migration_equivalence_v1 import validate_lite_migration_equivalence_v1
from ops.aegis.research_eod_summary_v1 import build_and_write_research_eod_summary_v1
from ops.aegis.strategic_operating_status_v1 import build_and_write_strategic_operating_status_v1
from ops.aegis.runtime_truth_kernel_v1 import _evaluate_capabilities, registry_payload_v1

DAY = "2026-05-30"
GEN = "2026-05-30T12:00:00Z"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_lite(root: Path) -> None:
    _write(
        root / "reports/aegis_lite_operating_status_v1" / DAY / "aegis_lite_operating_status.v1.json",
        {
            "schema_id": "aegis_lite_operating_status",
            "artifact_id": "aegis_lite_operating_status_v1",
            "day_utc": DAY,
            "generated_at_utc": GEN,
            "readiness_classification": "ADVISORY_ONLY",
            "current_blockers": ["CANDIDATE_INPUT_MISSING"],
            "manual_execution_only": True,
            "broker_submit_required": False,
        },
    )
    _write(
        root / "reports/aegis_lite_eod_report_v1" / DAY / "run" / "aegis_lite_eod_report.v1.json",
        {
            "schema_id": "aegis_lite_eod_report",
            "artifact_id": "aegis_lite_eod_report_v1",
            "day_utc": DAY,
            "generated_at_utc": GEN,
            "run_id": "run",
            "report_status": "INPUT_CONTRACT_FAILED",
            "manual_execution_status": "NOT_READY",
            "readiness_classification": "ADVISORY_ONLY",
            "selected_trade_candidates": [],
            "do_not_trade_blockers": ["CANDIDATE_INPUT_MISSING"],
            "source_artifact_lineage": [],
        },
    )


def test_strategic_operating_status_generation_preserves_safety(tmp_path: Path) -> None:
    payload, path = build_and_write_strategic_operating_status_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    assert path.exists()
    assert payload["schema_id"] == "aegis_strategic_operating_status"
    assert payload["strategic_system_of_record"] == "Paper Trading + Hypothesis Validation Architecture"
    assert payload["legacy_compatibility_layer"] == "Aegis Lite"
    assert payload["policy_state"]["trade_advice_allowed"] is False
    assert payload["policy_state"]["manual_trade_capture_allowed"] is False
    assert payload["policy_state"]["broker_submit_transmit_allowed"] is False


def test_research_eod_summary_generation_preserves_paper_only_policy(tmp_path: Path) -> None:
    payload, path = build_and_write_research_eod_summary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    assert path.exists()
    assert payload["schema_id"] == "aegis_research_eod_summary"
    assert payload["policy_state"]["trade_advice_allowed"] is False
    assert payload["paper_monitoring_summary"]["paper_only"] is True


def test_lite_migration_equivalence_passes_when_strategic_coverage_exists(tmp_path: Path) -> None:
    _seed_lite(tmp_path)
    build_and_write_strategic_operating_status_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    build_and_write_research_eod_summary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    payload = build_lite_migration_equivalence_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    assert validate_lite_migration_equivalence_v1(payload) == []
    assert payload["runtime_truth_migration_safe"] is True
    assert payload["blocking_comparison_count"] == 0


def test_lite_migration_equivalence_fails_when_strategic_evidence_missing(tmp_path: Path) -> None:
    _seed_lite(tmp_path)
    payload = build_lite_migration_equivalence_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    assert payload["runtime_truth_migration_safe"] is False
    assert payload["blocking_comparison_count"] > 0


def test_runtime_truth_blocks_when_strategic_evidence_missing() -> None:
    graph = _evaluate_capabilities(
        {
            "operator_execution_queue": {"status": "OK"},
            "event_market_snapshot": {"status": "OK"},
        }
    )
    assert graph["DATA_READY"]["allowed"] is False
    assert "aegis_strategic_operating_status" in graph["DATA_READY"]["missing_or_blocking_artifacts"]
    assert "aegis_research_eod_summary" in graph["DATA_READY"]["missing_or_blocking_artifacts"]


def test_lite_status_and_report_remain_registered_as_compatibility_artifacts() -> None:
    registry = {row["artifact_id"]: row for row in registry_payload_v1()}
    assert "aegis_lite_operating_status" in registry
    assert "aegis_lite_eod_report" in registry
    data_ready = _evaluate_capabilities(
        {
            "aegis_strategic_operating_status": {"status": "OK"},
            "aegis_research_eod_summary": {"status": "OK"},
            "operator_execution_queue": {"status": "OK"},
            "event_market_snapshot": {"status": "OK"},
        }
    )["DATA_READY"]
    assert data_ready["allowed"] is True
    assert "aegis_lite_operating_status" not in data_ready["depends_on_artifacts"]
    assert "aegis_lite_eod_report" not in data_ready["depends_on_artifacts"]


def test_projection_rerun_is_stable_with_fixed_timestamp(tmp_path: Path) -> None:
    first, _ = build_and_write_strategic_operating_status_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    second, _ = build_and_write_strategic_operating_status_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GEN)
    assert first["canonical_json_hash"] == second["canonical_json_hash"]
