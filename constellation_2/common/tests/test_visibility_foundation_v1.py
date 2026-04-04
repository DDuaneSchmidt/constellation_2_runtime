from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_clean").resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.diagnostic_foundation_v1 import (  # noqa: E402
    write_activity_flow_diagnostics,
    write_batch1_diagnostics,
    write_runtime_regression_analytics,
)
from constellation_2.common.tests.test_diagnostic_foundation_v1 import _write_day_fixture  # noqa: E402
from constellation_2.common.visibility_foundation_v1 import (  # noqa: E402
    build_batch2_visibility_docs,
    write_batch2_visibility,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_gate_stack_verdict(truth_root: Path, day: str, *, status: str, blocking_class: str) -> None:
    gate_doc = {
        "schema_id": "gate_stack_verdict",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": f"{day}T00:00:00Z",
        "producer": {"repo": "constellation_2_runtime", "module": "fixture", "git_sha": "fixture"},
        "status": status,
        "blocking_class": blocking_class,
        "reason_codes": [] if status == "PASS" else [f"GATE_REQUIRED_NOT_PASS:fixture:{status}"],
        "input_manifest": [],
        "gates": [
            {
                "gate_id": "fixture_gate",
                "gate_class": "CLASS1_SYSTEM_HARD_STOP" if status != "PASS" else "NONE",
                "required": True,
                "blocking": True,
                "status": status,
                "artifact_path": str((truth_root / "reports" / "fixture_gate_v1" / day / "fixture.json").resolve()),
                "artifact_sha256": "fixture",
                "reason_codes": [] if status == "PASS" else ["FIXTURE_FAIL"],
            }
        ],
    }
    _write_json(truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json", gate_doc)


def _prepare_current_day(
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
) -> None:
    _write_day_fixture(
        truth_root,
        day,
        attempt_id=f"{day}__A0001",
        verdict_status=verdict_status,
        intents=intents,
        submitted=submitted,
        filled=filled,
        rejected=rejected,
        vetoed=vetoed,
    )
    write_activity_flow_diagnostics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc=day, produced_utc=f"{day}T00:00:00Z")
    write_batch1_diagnostics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc=day, produced_utc=f"{day}T00:00:00Z")
    _write_gate_stack_verdict(truth_root, day, status=gate_status, blocking_class=blocking_class)


def test_happy_path_funnel_from_activity_flow(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["funnel"]["total_intent_count"] == 3
    assert len(docs["funnel"]["stage_metrics"]) == 5
    assert docs["funnel"]["integrity_status"] == "OK"


def test_funnel_with_contradictory_counts(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    flow_path = truth_root / "reports" / "activity_flow_diagnostics_v1" / "2026-04-03" / "activity_flow_diagnostics.v1.json"
    flow = json.loads(flow_path.read_text(encoding="utf-8"))
    flow["integrity_status"] = "EVIDENCE_INCONSISTENT"
    flow["counts"]["submitted"] = 5
    _write_json(flow_path, flow)
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["funnel"]["integrity_status"] == "EVIDENCE_INCONSISTENT"
    assert docs["summary"]["summary_classification"] == "INTEGRITY_FAILED"


def test_drift_with_prior_day_comparison_available(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
        truth_root,
        "2026-04-02",
        verdict_status="PASS",
        intents=3,
        submitted=2,
        filled=1,
        rejected=1,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    _prepare_current_day(
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
    write_runtime_regression_analytics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["drift"]["comparison_window_definition"]["comparison_mode"] == "PRIOR_DAY"
    assert any(row["metric_id"] == "intents" and row["comparability_status"] == "LEGITIMATE" for row in docs["drift"]["compared_metrics"])


def test_drift_with_missing_prior_day_comparison(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    write_runtime_regression_analytics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["drift"]["integrity_status"] == "COMPARISON_UNAVAILABLE"
    assert "PRIOR_DAY_UNAVAILABLE" in docs["drift"]["drift_classifications"]


def test_drift_with_scope_mismatch(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
        truth_root,
        "2026-04-02",
        verdict_status="PASS",
        intents=3,
        submitted=2,
        filled=1,
        rejected=1,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    _prepare_current_day(
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
    write_runtime_regression_analytics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    regression_path = truth_root / "reports" / "runtime_regression_analytics_v1" / "2026-04-03" / "runtime_regression_analytics.v1.json"
    regression_doc = json.loads(regression_path.read_text(encoding="utf-8"))
    regression_doc["current_activity_flow_ref"] = str((truth_root / "reports" / "activity_flow_diagnostics_v1" / "2099-01-01" / "activity_flow_diagnostics.v1.json").resolve())
    _write_json(regression_path, regression_doc)
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    first = docs["drift"]["compared_metrics"][0]
    assert first["comparability_status"] == "SCOPE_MISMATCH"
    assert first["comparison_status"] == "NOT_COMPUTED"


def test_blocked_day_summary_uses_gate_stack_verdict(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["summary"]["blocked_status"] == "BLOCKED"
    assert docs["summary"]["summary_classification"] == "READINESS_BLOCKED"


def test_ready_day_summary_uses_gate_stack_verdict(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["summary"]["readiness_status"] == "PASS"
    assert docs["summary"]["summary_classification"] == "READY_STABLE"


def test_integrity_failed_day_summary(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    flow = json.loads(flow_path.read_text(encoding="utf-8"))
    flow["integrity_status"] = "EVIDENCE_INCONSISTENT"
    _write_json(flow_path, flow)
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["summary"]["integrity_status"] == "EVIDENCE_INCONSISTENT"
    assert docs["summary"]["summary_classification"] == "INTEGRITY_FAILED"


def test_partial_visibility_blocked_by_eligibility_gate(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
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
    flow_path = truth_root / "reports" / "activity_flow_diagnostics_v1" / "2026-04-03" / "activity_flow_diagnostics.v1.json"
    flow = json.loads(flow_path.read_text(encoding="utf-8"))
    flow["terminal_state"] = "PARTIAL_OR_UNKNOWN"
    _write_json(flow_path, flow)
    docs = build_batch2_visibility_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["snapshot"]["integrity_status"] == "BOUNDED_INCOMPLETE"
    assert docs["summary"]["summary_classification"] == "INTEGRITY_FAILED"


def test_replay_same_inputs_produce_identical_outputs(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_current_day(
        truth_root,
        "2026-04-02",
        verdict_status="PASS",
        intents=3,
        submitted=2,
        filled=1,
        rejected=1,
        vetoed=0,
        gate_status="PASS",
        blocking_class="NONE",
    )
    _prepare_current_day(
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
    write_runtime_regression_analytics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    docs_a = build_batch2_visibility_docs(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    docs_b = build_batch2_visibility_docs(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    assert canonical_json_bytes_v1(docs_a) == canonical_json_bytes_v1(docs_b)


def test_batch2_self_failure_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    writes = write_batch2_visibility(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
        force_internal_failure=True,
    )
    assert writes["summary"].action == "WROTE"
    summary = json.loads((truth_root / "reports" / "daily_summary_v1" / "2026-04-03" / "daily_summary.v1.json").read_text(encoding="utf-8"))
    assert summary["integrity_status"] == "INTERNAL_FAILURE"
    assert summary["summary_classification"] == "INTERNAL_FAILURE"
