from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.diagnostic_foundation_v1 import (  # noqa: E402
    write_activity_flow_diagnostics,
    write_batch1_diagnostics,
    write_runtime_regression_analytics,
)
from constellation_2.common.operator_control_plane_v1 import (  # noqa: E402
    build_operator_home_bundle,
    build_operator_query_bundle,
    write_batch3_operator_home,
)
from constellation_2.common.tests.test_visibility_foundation_v1 import (  # noqa: E402
    _prepare_current_day,
    _write_json,
)
from constellation_2.common.visibility_foundation_v1 import write_batch2_visibility  # noqa: E402


def _prepare_batch3_day(
    truth_root: Path,
    day: str,
    *,
    verdict_status: str,
    intents: int,
    submitted: int,
    filled: int,
    rejected: int,
    vetoed: int,
    gate_status: str,
    blocking_class: str,
    prior_day: str | None = None,
    prior_day_intents: int = 3,
    prior_day_submitted: int = 2,
    prior_day_filled: int = 1,
    prior_day_rejected: int = 1,
    prior_day_vetoed: int = 0,
) -> None:
    if prior_day:
        _prepare_current_day(
            truth_root,
            prior_day,
            verdict_status="PASS",
            intents=prior_day_intents,
            submitted=prior_day_submitted,
            filled=prior_day_filled,
            rejected=prior_day_rejected,
            vetoed=prior_day_vetoed,
            gate_status="PASS",
            blocking_class="NONE",
        )
        write_runtime_regression_analytics(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=prior_day,
            produced_utc=f"{prior_day}T00:00:00Z",
        )
        write_batch2_visibility(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=prior_day,
            produced_utc=f"{prior_day}T00:00:00Z",
        )

    _prepare_current_day(
        truth_root,
        day,
        verdict_status=verdict_status,
        intents=intents,
        submitted=submitted,
        filled=filled,
        rejected=rejected,
        vetoed=vetoed,
        gate_status=gate_status,
        blocking_class=blocking_class,
    )
    write_runtime_regression_analytics(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day,
        produced_utc=f"{day}T00:00:00Z",
    )
    write_batch2_visibility(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day,
        produced_utc=f"{day}T00:00:00Z",
    )
    _write_operator_finalization_truth(
        truth_root,
        day,
        lifecycle_state="POST_TRADE_ECONOMIC" if gate_status == "PASS" and verdict_status == "PASS" else "PRE_TRADE",
        economic_status="ECONOMIC_FINALIZATION_COMPLETE" if gate_status == "PASS" and verdict_status == "PASS" else "NOT_READY_FOR_ECONOMIC_FINALIZATION",
    )




def _write_operator_finalization_truth(
    truth_root: Path,
    day: str,
    *,
    lifecycle_state: str,
    economic_status: str,
) -> None:
    run_scope = {
        "day_utc": day,
        "mode": "PAPER",
        "sleeve_id": "PRIMARY",
        "engine_id": None,
        "attempt_id": None,
        "attempt_seq": None,
        "truth_partition": str(truth_root),
    }
    _write_json(
        truth_root / "reports" / "lifecycle_state_authority_v1" / day / "lifecycle_state_authority.v1.json",
        {
            "schema_id": "lifecycle_state_authority_v1",
            "schema_version": 1,
            "lifecycle_policy_id": "C2_LIFECYCLE_STATE_POLICY_V1",
            "lifecycle_policy_version": 1,
            "day_utc": day,
            "mode": "PAPER",
            "run_scope": {
                "day_utc": day,
                "mode": "PAPER",
                "truth_partition": str(truth_root),
            },
            "produced_utc": f"{day}T00:00:00Z",
            "lifecycle_state": lifecycle_state,
            "derivation_inputs": [
                {
                    "artifact_family": "gate_stack_verdict_v1",
                    "path": str(truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json"),
                    "present": True,
                }
            ],
            "derivation_reasons": [f"lifecycle_state={lifecycle_state}"],
            "evidence_refs": [str(truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json")],
        },
    )
    blocked_stage_status = "COMPLETE" if economic_status == "ECONOMIC_FINALIZATION_COMPLETE" else "BLOCKED_UPSTREAM"
    blocked_reasons = [] if economic_status == "ECONOMIC_FINALIZATION_COMPLETE" else [economic_status]
    integrity_status = "OK" if economic_status == "ECONOMIC_FINALIZATION_COMPLETE" else "ECONOMIC_FINALIZATION_INCOMPLETE"
    execution_truth_status = "EXECUTION_TRUTH_COMPLETE" if economic_status == "ECONOMIC_FINALIZATION_COMPLETE" else "EXECUTION_TRUTH_INCOMPLETE"
    _write_json(
        truth_root / "reports" / "economic_finalization_status_v1" / day / "economic_finalization_status.v1.json",
        {
            "schema_id": "economic_finalization_status_v1",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": f"{day}T00:00:00Z",
            "execution_completion_ruleset_id": "EXECUTION_COMPLETION_RULESET_V1",
            "execution_completion_ruleset_version": 1,
            "lifecycle_completeness_policy_id": "C2_LIFECYCLE_COMPLETENESS_POLICY_V1",
            "lifecycle_completeness_policy_version": 1,
            "run_scope": run_scope,
            "execution_truth_status": execution_truth_status,
            "economic_finalization_status": economic_status,
            "stage_statuses": [
                {"stage_name": "POSITIONS", "stage_status": blocked_stage_status, "blocking_reasons": blocked_reasons, "blocked_writers": []},
                {"stage_name": "CASH", "stage_status": blocked_stage_status, "blocking_reasons": blocked_reasons, "blocked_writers": []},
                {"stage_name": "MARKS", "stage_status": blocked_stage_status, "blocking_reasons": blocked_reasons, "blocked_writers": []},
                {"stage_name": "ACCOUNTING", "stage_status": blocked_stage_status, "blocking_reasons": blocked_reasons, "blocked_writers": []},
                {"stage_name": "EXIT_RECONCILIATION", "stage_status": blocked_stage_status, "blocking_reasons": blocked_reasons, "blocked_writers": []},
            ],
            "integrity_status": integrity_status,
            "evidence_refs": [str(truth_root / "reports" / "daily_summary_v1" / day / "daily_summary.v1.json")],
        },
    )

def test_home_view_for_blocked_finalized_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
    )
    bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03")
    assert bundle["home_view"]["home_status_classification"] == "READINESS_BLOCKED"
    assert bundle["trust_panel"]["finalization_state"] == "IN_FLIGHT_BLOCKED"
    assert bundle["trust_panel"]["trust_classification"] == "LIMITED"
    assert bundle["home_view"]["partiality_status"] == "BOUNDED_PARTIAL"




def test_home_view_for_bootstrap_day_is_not_trusted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    _write_operator_finalization_truth(
        truth_root,
        "2026-04-03",
        lifecycle_state="PRE_TRADE",
        economic_status="NOT_READY_FOR_ECONOMIC_FINALIZATION",
    )
    bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03")
    assert bundle["trust_panel"]["finalization_state"] == "IN_FLIGHT_BLOCKED"
    assert bundle["trust_panel"]["trust_classification"] == "LIMITED"
    assert bundle["trust_panel"]["exactness_classification"] == "BOUNDED_PARTIAL"


def test_home_view_for_economic_not_ready_day_is_not_trusted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    _write_operator_finalization_truth(
        truth_root,
        "2026-04-03",
        lifecycle_state="POST_TRADE_ECONOMIC",
        economic_status="NOT_READY_FOR_ECONOMIC_FINALIZATION",
    )
    bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03")
    assert bundle["trust_panel"]["finalization_state"] == "IN_FLIGHT_BLOCKED"
    assert bundle["trust_panel"]["trust_classification"] == "LIMITED"
    assert bundle["trust_panel"]["exactness_classification"] == "BOUNDED_PARTIAL"

def test_home_view_for_integrity_constrained_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=2,
        submitted=1,
        filled=1,
        rejected=1,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    flow_path = truth_root / "reports" / "activity_flow_diagnostics_v1" / "2026-04-03" / "activity_flow_diagnostics.v1.json"
    flow_doc = json.loads(flow_path.read_text(encoding="utf-8"))
    flow_doc["integrity_status"] = "EVIDENCE_INCONSISTENT"
    _write_json(flow_path, flow_doc)
    for relpath in [
        "reports/daily_summary_v1/2026-04-03/daily_summary.v1.json",
        "reports/funnel_metrics_v1/2026-04-03/funnel_metrics.v1.json",
    ]:
        artifact_path = truth_root / relpath
        artifact_doc = json.loads(artifact_path.read_text(encoding="utf-8"))
        artifact_doc["integrity_status"] = "EVIDENCE_INCONSISTENT"
        _write_json(artifact_path, artifact_doc)
    bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03")
    assert bundle["trust_panel"]["exactness_classification"] == "INTEGRITY_CONSTRAINED"
    assert bundle["home_view"]["integrity_status"] == "EVIDENCE_INCONSISTENT"


def test_supported_readiness_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="what is the readiness status today",
    )
    assert bundle["query_response"]["query_class_id"] == "readiness_state_query_v1"
    assert bundle["query_response"]["response_status"] == "EXACT"


def test_supported_blocker_reason_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="why blocked today",
    )
    assert bundle["query_response"]["query_class_id"] == "blocker_reason_query_v1"
    assert "blocking_class=CLASS1_SYSTEM_HARD_STOP" in bundle["query_response"]["answer_blocks"][0]["lines"]


def test_supported_root_cause_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="show root cause for today",
    )
    assert bundle["query_response"]["query_class_id"] == "root_cause_query_v1"
    assert any("first_break_cause=" in line for line in bundle["query_response"]["answer_blocks"][0]["lines"])


def test_supported_no_intents_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="why no intents today",
    )
    assert bundle["query_response"]["query_class_id"] == "no_intents_query_v1"
    assert any("zero_intent_classification=" in line for line in bundle["query_response"]["answer_blocks"][0]["lines"])


def test_supported_daily_summary_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="daily summary",
    )
    assert bundle["query_response"]["query_class_id"] == "daily_summary_query_v1"
    assert any("summary_classification=" in line for line in bundle["query_response"]["answer_blocks"][0]["lines"])


def test_supported_funnel_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=3,
        submitted=2,
        filled=1,
        rejected=1,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="show funnel stage counts",
    )
    assert bundle["query_response"]["query_class_id"] == "funnel_query_v1"
    assert any("INTENTS:" in line for line in bundle["query_response"]["answer_blocks"][0]["lines"])


def test_supported_drift_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
        prior_day="2026-04-02",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="show drift vs prior day",
    )
    assert bundle["query_response"]["query_class_id"] == "drift_query_v1"
    assert any("comparison_mode=PRIOR_DAY" in line for line in bundle["query_response"]["answer_blocks"][0]["lines"])


def test_supported_trust_explanation_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="can i trust this",
    )
    assert bundle["query_response"]["query_class_id"] == "trust_explanation_query_v1"
    assert any("trust_classification=" in line for line in bundle["query_response"]["answer_blocks"][0]["lines"])


def test_unsupported_query(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="what should we trade tomorrow",
    )
    assert bundle["query_response"]["query_class_id"] == "unsupported_query_v1"
    assert bundle["query_response"]["response_status"] == "UNSUPPORTED"


def test_query_with_missing_source_artifact(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    summary_path = truth_root / "reports" / "daily_summary_v1" / "2026-04-03" / "daily_summary.v1.json"
    summary_path.unlink()
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="status",
    )
    assert bundle["query_response"]["response_status"] == "BOUNDED_PARTIAL"
    assert any(row["reason"] == "MISSING_REQUIRED_INPUT" for row in bundle["retrieval_manifest"]["rejected_artifacts"])


def test_query_with_incompatible_scope_request(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="readiness in live mode on 2026-04-03",
    )
    assert bundle["query_response"]["response_status"] == "UNAVAILABLE"
    assert any(row["reason"] == "SCOPE_MISMATCH" for row in bundle["retrieval_manifest"]["rejected_artifacts"])


def test_in_flight_data_request_blocked_or_downgraded(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    gate_path = truth_root / "reports" / "gate_stack_verdict_v1" / "2026-04-03" / "gate_stack_verdict.v1.json"
    gate_path.unlink()
    bundle = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="readiness",
    )
    assert bundle["trust_panel"]["finalization_state"] == "UNAVAILABLE"
    assert bundle["query_response"]["response_status"] in {"BOUNDED_PARTIAL", "UNAVAILABLE"}


def test_replay_same_query_same_inputs_identical_response_object(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
        prior_day="2026-04-02",
    )
    first = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="show drift vs prior day",
    )
    second = build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="show drift vs prior day",
    )
    assert first["query_response"] == second["query_response"]
    assert first["retrieval_manifest"] == second["retrieval_manifest"]
    assert first["trust_panel"] == second["trust_panel"]


def test_batch3_internal_failure_path(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    import constellation_2.common.operator_control_plane_v1 as ocp

    monkeypatch.setattr(ocp, "_render_query_answer_blocks", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    bundle = ocp.build_operator_query_bundle(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        query_text="readiness",
    )
    assert bundle["query_response"]["integrity_status"] == "INTERNAL_FAILURE"
    assert bundle["trust_panel"]["integrity_state"] == "INTERNAL_FAILURE"


def test_trust_gate_suppression_downgrade_behavior(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
        prior_day="2026-04-02",
    )
    bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03")
    funnel_panel = next(panel for panel in bundle["home_view"]["rendered_panels"] if panel["panel_id"] == "funnel_panel")
    assert funnel_panel["suppressed"] is True
    assert "funnel_panel:BLOCKED_STATUS" in bundle["trust_panel"]["suppression_rules_applied"]


def test_home_view_composition_precedence(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="FAIL",
        intents=0,
        submitted=0,
        filled=0,
        rejected=0,
        vetoed=0,
        gate_status="FAIL",
        blocking_class="CLASS1_SYSTEM_HARD_STOP",
    )
    bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03")
    panel_ids = [panel["panel_id"] for panel in bundle["home_view"]["rendered_panels"]]
    assert panel_ids.index("readiness_panel") < panel_ids.index("diagnostic_panel")
    assert panel_ids.index("diagnostic_panel") < panel_ids.index("summary_panel")


def test_write_batch3_operator_home(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_batch3_day(
        truth_root,
        "2026-04-03",
        verdict_status="PASS",
        intents=1,
        submitted=1,
        filled=1,
        rejected=0,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    writes = write_batch3_operator_home(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert writes["home_view"].action in {"WROTE", "SKIP_IDENTICAL", "REFRESHED"}
    assert (truth_root / "reports" / "operator_home_view_v1" / "2026-04-03" / "operator_home_view.v1.json").exists()
