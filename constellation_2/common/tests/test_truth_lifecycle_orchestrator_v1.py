from __future__ import annotations

from pathlib import Path
from typing import Callable
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_lifecycle_orchestrator_v1 import (
    LifecycleRunContextV1,
    PhaseRunResultV1,
    load_truth_lifecycle_phase_graph_v1,
    run_truth_lifecycle_orchestrator_v1,
)

DAY = "2026-04-21"
PRODUCED_UTC = "2026-04-21T00:00:00Z"


def _phase_ids() -> list[str]:
    graph = load_truth_lifecycle_phase_graph_v1(repo_root=REPO_ROOT)
    return [str(row["phase_id"]).strip().upper() for row in graph["phases"]]


def _runner_map(factory: Callable[[str], PhaseRunResultV1]) -> dict[str, Callable[[LifecycleRunContextV1, dict], PhaseRunResultV1]]:
    return {phase_id: (lambda _ctx, _phase, pid=phase_id: factory(pid)) for phase_id in _phase_ids()}


def test_steady_state_phase_order_and_no_bootstrap(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )
    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=_runner_map(lambda _pid: PhaseRunResultV1(status="PASS", reason_codes=("OK",))),
    )
    ledger = orchestration["run_ledger"]
    assert ledger["overall_status"] == "PASS"
    assert ledger["bootstrap_applied"] is False
    assert [row["phase_id"] for row in ledger["phase_results"]] == _phase_ids()
    assert all(row["status"] == "PASS" for row in ledger["phase_results"])


def test_genesis_bootstrap_recorded_once(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )

    def _factory(phase_id: str) -> PhaseRunResultV1:
        if phase_id == "GENESIS_BOOTSTRAP":
            return PhaseRunResultV1(
                status="BOOTSTRAP",
                bootstrap_applied=True,
                bootstrap_reason="NO_PRIOR_DAY_CHAIN",
                reason_codes=("GENESIS_BOOTSTRAP_APPLIED",),
            )
        return PhaseRunResultV1(status="PASS", reason_codes=("OK",))

    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=_runner_map(_factory),
    )
    ledger = orchestration["run_ledger"]
    phase_rows = {row["phase_id"]: row for row in ledger["phase_results"]}
    assert ledger["bootstrap_applied"] is True
    assert phase_rows["GENESIS_BOOTSTRAP"]["status"] == "BOOTSTRAP"
    assert phase_rows["GENESIS_BOOTSTRAP"]["bootstrap_applied"] is True
    assert sum(1 for row in ledger["phase_results"] if row["bootstrap_applied"]) == 1


def test_missing_dependency_reports_first_blocker_and_skips_downstream(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )

    def _factory(phase_id: str) -> PhaseRunResultV1:
        if phase_id == "CONTEXT_AUTHORITY":
            return PhaseRunResultV1(
                status="BLOCKED",
                first_blocker_code="GLOBAL_CONTEXT_PACKAGE_MISSING",
                first_blocker_artifact_path=str(
                    (truth_root / "reports" / "global_context_build_v1" / DAY / "missing.json").resolve()
                ),
                upstream_dependency_id="global_context_package_v1",
                upstream_dependency_artifact_path=str(
                    (truth_root / "reports" / "global_context_build_v1" / DAY / "missing.json").resolve()
                ),
                reason_codes=("GLOBAL_CONTEXT_PACKAGE_MISSING",),
            )
        return PhaseRunResultV1(status="PASS", reason_codes=("OK",))

    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=_runner_map(_factory),
    )
    ledger = orchestration["run_ledger"]
    rows = {row["phase_id"]: row for row in ledger["phase_results"]}
    assert ledger["overall_status"] == "BLOCKED"
    assert ledger["first_blocker_code"] == "GLOBAL_CONTEXT_PACKAGE_MISSING"
    assert rows["CONTEXT_AUTHORITY"]["status"] == "BLOCKED"
    assert rows["GATE_INPUTS"]["status"] == "SKIPPED"


def test_truth_root_execution_does_not_count_sleeve_side_effects(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    sleeve_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_gate_path = (
        sleeve_root / "reports" / "feed_attestation_gate_v1" / DAY / "feed_attestation_gate.v1.json"
    ).resolve()
    sleeve_gate_path.parent.mkdir(parents=True, exist_ok=True)
    sleeve_gate_path.write_text('{"schema_id":"feed_attestation_gate","status":"PASS"}\n', encoding="utf-8")

    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )
    truth_gate_path = (
        truth_root / "reports" / "feed_attestation_gate_v1" / DAY / "feed_attestation_gate.v1.json"
    ).resolve()

    def _factory(phase_id: str) -> PhaseRunResultV1:
        if phase_id == "GATE_INPUTS":
            if not truth_gate_path.exists():
                return PhaseRunResultV1(
                    status="BLOCKED",
                    first_blocker_code="CANONICAL_TRUTH_GATE_INPUT_MISSING",
                    first_blocker_artifact_path=str(truth_gate_path),
                    upstream_dependency_id="feed_attestation_gate_v1",
                    upstream_dependency_artifact_path=str(truth_gate_path),
                    reason_codes=("CANONICAL_TRUTH_GATE_INPUT_MISSING",),
                )
        return PhaseRunResultV1(status="PASS", reason_codes=("OK",))

    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=_runner_map(_factory),
    )
    ledger = orchestration["run_ledger"]
    assert ledger["overall_status"] == "BLOCKED"
    assert ledger["first_blocker_code"] == "CANONICAL_TRUTH_GATE_INPUT_MISSING"
    assert ledger["first_blocker_artifact_path"] == str(truth_gate_path)
    assert str(sleeve_gate_path) != ledger["first_blocker_artifact_path"]


def test_ledger_blocks_when_target_day_admission_is_not_admit(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    _write_json = lambda p, o: (p.parent.mkdir(parents=True, exist_ok=True), p.write_text(__import__("json").dumps(o), encoding="utf-8"))
    _write_json(
        truth_root / "target_day_build_v1" / f"{DAY}.json",
        {"build_status": "COMPLETE", "closure_status": "CLOSED"},
    )
    _write_json(
        truth_root / "target_day_admission_v1" / f"{DAY}.json",
        {"admission_status": "BLOCKED"},
    )
    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )
    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=_runner_map(lambda _pid: PhaseRunResultV1(status="PASS", reason_codes=("OK",))),
    )
    ledger = orchestration["run_ledger"]
    assert ledger["overall_status"] == "BLOCKED"
    assert ledger["first_blocker_code"] == "TARGET_DAY_ADMISSION_NOT_ADMIT"
    assert ledger["structural_readiness_reached"] is False


def test_ledger_blocks_on_truth_sleeve_continuity_divergence(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    sleeve_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_recon = (
        sleeve_root / "reports" / "reconciliation_report_v3" / DAY / "reconciliation_report.v3.json"
    ).resolve()
    sleeve_recon.parent.mkdir(parents=True, exist_ok=True)
    sleeve_recon.write_text('{"schema_id":"reconciliation_report_v3","day_utc":"2026-04-21"}\n', encoding="utf-8")
    _write_json = lambda p, o: (p.parent.mkdir(parents=True, exist_ok=True), p.write_text(__import__("json").dumps(o), encoding="utf-8"))
    _write_json(
        truth_root / "target_day_build_v1" / f"{DAY}.json",
        {"build_status": "COMPLETE", "closure_status": "CLOSED"},
    )
    _write_json(
        truth_root / "target_day_admission_v1" / f"{DAY}.json",
        {"admission_status": "ADMIT"},
    )
    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )
    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=_runner_map(lambda _pid: PhaseRunResultV1(status="PASS", reason_codes=("OK",))),
    )
    ledger = orchestration["run_ledger"]
    assert ledger["overall_status"] == "BLOCKED"
    assert ledger["first_blocker_code"] == "TRUTH_SLEEVE_DIVERGENCE_RECONCILIATION_REPORT_V3"
