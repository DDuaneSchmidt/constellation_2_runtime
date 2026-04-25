from __future__ import annotations

import json
from pathlib import Path
from typing import Callable
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_lifecycle_orchestrator_v1 import (  # noqa: E402
    LifecycleRunContextV1,
    PhaseRunResultV1,
    load_truth_lifecycle_phase_graph_v1,
    run_truth_lifecycle_orchestrator_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402


DAY = "2026-04-21"
PRODUCED_UTC = "2026-04-21T00:00:00Z"


def _phase_ids() -> list[str]:
    graph = load_truth_lifecycle_phase_graph_v1(repo_root=REPO_ROOT)
    return [str(row["phase_id"]).strip().upper() for row in graph["phases"]]


def _runner_map(
    factory: Callable[[str], PhaseRunResultV1],
) -> dict[str, Callable[[LifecycleRunContextV1, dict], PhaseRunResultV1]]:
    return {phase_id: (lambda _ctx, _phase, pid=phase_id: factory(pid)) for phase_id in _phase_ids()}


def test_truth_lifecycle_phase_result_schema_has_load_bearing_required_fields() -> None:
    schema_path = (
        REPO_ROOT
        / "governance/04_DATA/SCHEMAS/C2/REPORTS/truth_lifecycle_phase_result.v1.schema.json"
    ).resolve()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    required = set(schema.get("required") or [])
    assert schema.get("additionalProperties") is False
    assert {
        "status",
        "first_blocker_code",
        "first_blocker_artifact_path",
        "upstream_dependency_id",
        "upstream_dependency_artifact_path",
        "reason_codes",
        "bootstrap_applied",
        "bootstrap_reason",
        "producer_invocations",
        "phase_payload",
    }.issubset(required)


def test_truth_day_run_ledger_schema_has_load_bearing_required_fields() -> None:
    schema_path = (
        REPO_ROOT
        / "governance/04_DATA/SCHEMAS/C2/REPORTS/truth_day_run_ledger.v1.schema.json"
    ).resolve()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    required = set(schema.get("required") or [])
    assert schema.get("additionalProperties") is False
    assert {
        "overall_status",
        "first_blocker_code",
        "first_blocker_artifact_path",
        "upstream_dependency_id",
        "upstream_dependency_artifact_path",
        "bootstrap_applied",
        "phase_results",
        "producer_invocations",
        "structural_readiness_reached",
        "business_no_trade",
        "execution_readiness_reached",
        "execution_reached",
        "day_close_reached",
        "run_ledger_path",
    }.issubset(required)


def test_truth_day_run_ledger_validation_fails_when_run_ledger_path_missing(tmp_path: Path) -> None:
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
    ledger = dict(orchestration["run_ledger"])
    validate_against_repo_schema_v1(
        ledger,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/truth_day_run_ledger.v1.schema.json",
    )
    ledger.pop("run_ledger_path", None)
    try:
        validate_against_repo_schema_v1(
            ledger,
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/REPORTS/truth_day_run_ledger.v1.schema.json",
        )
    except Exception as exc:  # noqa: BLE001
        assert "run_ledger_path" in str(exc)
    else:
        raise AssertionError("expected schema validation to fail when run_ledger_path is missing")
