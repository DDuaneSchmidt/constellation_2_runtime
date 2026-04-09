#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.testing_evidence_plane_v1 import (
    INTEGRATION_TEST_RESULT_SCHEMA,
    PAPER_OPEN_READINESS_SCHEMA,
    PAPER_WORKFLOW_RESULT_SCHEMA,
    REPLAY_TEST_RESULT_SCHEMA,
    REQUIRED_SUBSYSTEMS,
    RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA,
    SCENARIO_CATALOG_SCHEMA,
    SCENARIO_TEST_RESULT_SCHEMA,
    SUBSYSTEM_READINESS_REPORT_SCHEMA,
    WORKFLOW_RESTART_RESULT_SCHEMA,
    build_scenario_catalog_v1,
    build_subsystem_readiness_report_v1,
    build_suite_result_artifact_v1,
    canonical_testing_evidence_report_path,
    read_testing_evidence_report_v1,
    write_testing_evidence_report_v1,
)
from constellation_2.common.paper_open_readiness_v1 import (
    build_paper_open_readiness_v1 as build_governed_paper_open_readiness_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_paper_session_ledger_ref_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_ledger_path,
    resolve_repo_truth_root,
)


TRUTH_ROOT = resolve_repo_truth_root(REPO_ROOT)


@dataclass(frozen=True)
class SuiteConfig:
    suite_id: str
    subsystem_name: str
    test_paths: tuple[str, ...]
    semantic_gaps: tuple[str, ...] = ()


RESULT_GROUPS: dict[str, tuple[str, tuple[SuiteConfig, ...]]] = {
    "scenario_test_result_v1": (
        SCENARIO_TEST_RESULT_SCHEMA,
        (
            SuiteConfig(
                suite_id="bond_scenario_matrix_v1",
                subsystem_name="bond",
                test_paths=("constellation_2/common/tests/test_bond_scenario_matrix_v1.py",),
            ),
            SuiteConfig(
                suite_id="bond_precedence_pack_v1",
                subsystem_name="bond",
                test_paths=("constellation_2/common/tests/test_bond_precedence_pack_v1.py",),
            ),
            SuiteConfig(
                suite_id="bond_invariants_v1",
                subsystem_name="bond",
                test_paths=("constellation_2/common/tests/test_bond_invariants_v1.py",),
            ),
            SuiteConfig(
                suite_id="signal_artifact_builders_v1",
                subsystem_name="signal",
                test_paths=("constellation_2/common/tests/test_signal_artifact_builders_v1.py",),
            ),
            SuiteConfig(
                suite_id="legacy_holdings_artifact_builders_v1",
                subsystem_name="legacy_holdings",
                test_paths=("constellation_2/common/tests/test_legacy_holdings_artifact_builders_v1.py",),
            ),
        ),
    ),
    "replay_test_result_v1": (
        REPLAY_TEST_RESULT_SCHEMA,
        (
            SuiteConfig(
                suite_id="bug_replay_ledger_v1",
                subsystem_name="lifecycle",
                test_paths=("constellation_2/common/tests/test_bug_replay_ledger_v1.py",),
            ),
            SuiteConfig(
                suite_id="runtime_replay_day_v1",
                subsystem_name="operator_session_replay",
                test_paths=("constellation_2/common/tests/test_runtime_replay_day_v1.py",),
            ),
            SuiteConfig(
                suite_id="lifecycle_fail_closed_v1",
                subsystem_name="lifecycle",
                test_paths=(
                    "constellation_2/common/tests/test_change_control_lifecycle_fail_closed_v1.py",
                    "constellation_2/common/tests/test_shared_authority_promotion_fail_closed_v1.py",
                ),
            ),
        ),
    ),
    "integration_test_result_v1": (
        INTEGRATION_TEST_RESULT_SCHEMA,
        (
            SuiteConfig(
                suite_id="advisory_integration_v1",
                subsystem_name="advisory",
                test_paths=(
                    "constellation_2/common/tests/test_advisory_integration_v1.py",
                    "constellation_2/common/tests/test_advisory_promotion_readiness_v1.py",
                    "constellation_2/common/tests/test_advisory_invariants_v1.py",
                ),
            ),
            SuiteConfig(
                suite_id="authority_constraints_v1",
                subsystem_name="governance",
                test_paths=("constellation_2/common/tests/test_artifact_driven_authority_constraints_v1.py",),
            ),
        ),
    ),
    "runtime_truth_integrity_result_v1": (
        RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA,
        (
            SuiteConfig(
                suite_id="governance_registry_integrity_v1",
                subsystem_name="governance",
                test_paths=("constellation_2/common/tests/test_governance_registry_integrity_v1.py",),
            ),
            SuiteConfig(
                suite_id="truth_surface_mapping_integrity_v1",
                subsystem_name="runtime_truth",
                test_paths=("constellation_2/common/tests/test_truth_surface_mapping_integrity_v1.py",),
            ),
            SuiteConfig(
                suite_id="runtime_artifact_integrity_v1",
                subsystem_name="runtime_truth",
                test_paths=("constellation_2/common/tests/test_runtime_artifact_integrity_v1.py",),
            ),
        ),
    ),
    "workflow_restart_result_v1": (
        WORKFLOW_RESTART_RESULT_SCHEMA,
        (
            SuiteConfig(
                suite_id="bond_restart_recovery_v1",
                subsystem_name="bond",
                test_paths=("constellation_2/common/tests/test_bond_restart_recovery_v1.py",),
            ),
            SuiteConfig(
                suite_id="session_readiness_repair_v1",
                subsystem_name="operator_session_replay",
                test_paths=("constellation_2/common/tests/test_session_readiness_repair_v1.py",),
            ),
            SuiteConfig(
                suite_id="system_restart_replay_v1",
                subsystem_name="operator_session_replay",
                test_paths=("constellation_2/common/tests/test_system_restart_replay_v1.py",),
            ),
        ),
    ),
    "paper_workflow_result_v1": (
        PAPER_WORKFLOW_RESULT_SCHEMA,
        (
            SuiteConfig(
                suite_id="paper_workflow_discovery_v1",
                subsystem_name="operator_session_replay",
                test_paths=("constellation_2/common/tests/test_paper_workflow_discovery_v1.py",),
            ),
            SuiteConfig(
                suite_id="operator_control_plane_v1",
                subsystem_name="operator_session_replay",
                test_paths=("constellation_2/common/tests/test_operator_control_plane_v1.py",),
            ),
            SuiteConfig(
                suite_id="evidence_plane_contracts_v1",
                subsystem_name="governance",
                test_paths=(
                    "constellation_2/common/tests/test_testing_evidence_plane_v1.py",
                    "constellation_2/common/tests/test_subsystem_readiness_report_v1.py",
                    "constellation_2/common/tests/test_paper_open_readiness_v1.py",
                ),
            ),
        ),
    ),
}


class _PytestCapturePlugin:
    def __init__(self) -> None:
        self.collected_nodeids: list[str] = []
        self.outcomes: dict[str, str] = {}

    def pytest_collection_modifyitems(self, session: Any, config: Any, items: list[Any]) -> None:
        self.collected_nodeids = [str(item.nodeid) for item in items]

    def pytest_runtest_logreport(self, report: Any) -> None:
        nodeid = str(report.nodeid)
        if report.when == "call":
            self.outcomes[nodeid] = str(report.outcome)
        elif report.when == "setup" and report.skipped and nodeid not in self.outcomes:
            self.outcomes[nodeid] = "skipped"


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_suite(config: SuiteConfig) -> dict[str, Any]:
    plugin = _PytestCapturePlugin()
    args = ["-q", *config.test_paths]
    exit_code = int(pytest.main(args, plugins=[plugin]))
    executed_at = _now_utc()
    scenario_ids = list(plugin.collected_nodeids)
    pass_count = 0
    fail_count = 0
    skip_count = 0
    failure_refs: list[str] = []
    for nodeid in scenario_ids:
        outcome = plugin.outcomes.get(nodeid, "failed" if exit_code != 0 else "passed")
        if outcome == "passed":
            pass_count += 1
        elif outcome == "skipped":
            skip_count += 1
        else:
            fail_count += 1
            failure_refs.append(nodeid)
    if fail_count > 0:
        status = "FAIL"
    elif config.semantic_gaps:
        status = "SEMANTIC_GAP"
    else:
        status = "PASS"
    return {
        "suite_id": config.suite_id,
        "subsystem_name": config.subsystem_name,
        "executed_at": executed_at,
        "scenario_ids": scenario_ids,
        "test_refs": [str((REPO_ROOT / path).resolve()) for path in config.test_paths],
        "status": status,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "skip_count": skip_count,
        "failure_refs": failure_refs,
        "semantic_gaps": list(config.semantic_gaps),
    }


def _build_scenario_catalog_rows(group_rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    catalog_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for artifact_family, rows in group_rows.items():
        for row in rows:
            for scenario_id in row["scenario_ids"]:
                if scenario_id in seen_ids:
                    continue
                seen_ids.add(scenario_id)
                catalog_rows.append(
                    {
                        "scenario_id": scenario_id,
                        "domain_scope": row["subsystem_name"],
                        "scenario_type": artifact_family,
                        "purpose": f"Governed pytest evidence for suite {row['suite_id']}",
                        "input_artifact_refs": row["test_refs"],
                        "policy_refs": [],
                        "expected_outputs": {},
                        "expected_action_classes": [],
                        "expected_precedence_notes": [],
                        "status": row["status"],
                    }
                )
    return sorted(catalog_rows, key=lambda item: item["scenario_id"])


def _subsystem_rows_from_artifacts(day_utc: str, artifact_paths: dict[str, Path]) -> list[dict[str, Any]]:
    per_subsystem: dict[str, dict[str, Any]] = {
        name: {
            "subsystem_name": name,
            "status": "READY",
            "blocking_issues": [],
            "semantic_gaps": [],
            "required_test_refs": [],
            "approved_for_next_gate": True,
        }
        for name in REQUIRED_SUBSYSTEMS
    }
    for artifact_family, artifact_path in artifact_paths.items():
        if artifact_family == "scenario_catalog_v1":
            continue
        schema_relpath = {
            "scenario_test_result_v1": SCENARIO_TEST_RESULT_SCHEMA,
            "replay_test_result_v1": REPLAY_TEST_RESULT_SCHEMA,
            "integration_test_result_v1": INTEGRATION_TEST_RESULT_SCHEMA,
            "runtime_truth_integrity_result_v1": RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA,
            "workflow_restart_result_v1": WORKFLOW_RESTART_RESULT_SCHEMA,
            "paper_workflow_result_v1": PAPER_WORKFLOW_RESULT_SCHEMA,
        }[artifact_family]
        payload = read_testing_evidence_report_v1(
            truth_root=TRUTH_ROOT,
            artifact_family=artifact_family,
            day_utc=day_utc,
            schema_relpath=schema_relpath,
        )
        for row in payload["results"]:
            subsystem = str(row["subsystem_name"])
            target = per_subsystem[subsystem]
            result_ref = str(row.get("source_test_id") or row.get("scenario_id") or row["result_id"])
            target["required_test_refs"].append(f"{artifact_path}#{result_ref}")
            if bool(row["blocking"]):
                target["status"] = "BLOCKED"
                target["approved_for_next_gate"] = False
                failure_refs = [
                    str(note).split("failure_ref:", 1)[1]
                    for note in row["notes"]
                    if str(note).startswith("failure_ref:")
                ]
                issue_suffix = ",".join(failure_refs) if failure_refs else result_ref
                target["blocking_issues"].append(f"{result_ref}:{issue_suffix}")
            semantic_gaps = [
                str(note).split("semantic_gap:", 1)[1]
                for note in row["notes"]
                if str(note).startswith("semantic_gap:")
            ]
            if semantic_gaps:
                if target["status"] == "READY":
                    target["status"] = "SEMANTIC_GAP"
                target["approved_for_next_gate"] = False
                target["semantic_gaps"].extend(semantic_gaps)
    for row in per_subsystem.values():
        row["blocking_issues"] = sorted(set(row["blocking_issues"]))
        row["semantic_gaps"] = sorted(set(row["semantic_gaps"]))
        row["required_test_refs"] = sorted(set(row["required_test_refs"]))
    return [per_subsystem[name] for name in REQUIRED_SUBSYSTEMS]


def _derive_paper_open_state_from_governed_inputs(
    *,
    subsystem_payload_from_disk: dict[str, Any],
    unresolved_blockers: list[str],
    unresolved_semantic_gaps: list[str],
    ledger_authority_status: str,
) -> tuple[str, bool]:
    if str(ledger_authority_status).strip().upper() != "GRANTED":
        return ("BLOCKED", False)
    if (
        subsystem_payload_from_disk["status"] == "READY"
        and not subsystem_payload_from_disk["blocking_issues"]
        and not subsystem_payload_from_disk["semantic_gaps"]
        and not unresolved_blockers
        and not unresolved_semantic_gaps
    ):
        return ("READY", True)
    return ("BLOCKED", False)


def main() -> int:
    global TRUTH_ROOT
    parser = argparse.ArgumentParser(prog="run_testing_evidence_plane_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args()

    if str(args.truth_root).strip():
        TRUTH_ROOT = Path(str(args.truth_root)).resolve()

    day_utc = str(args.day_utc).strip()
    recorded_at = _now_utc()
    artifact_paths: dict[str, Path] = {}
    group_rows: dict[str, list[dict[str, Any]]] = {}

    for artifact_family, (schema_relpath, suite_configs) in RESULT_GROUPS.items():
        suite_rows = [_run_suite(config) for config in suite_configs]
        group_rows[artifact_family] = suite_rows
        schema_id = artifact_family.removesuffix("_v1")
        payload = build_suite_result_artifact_v1(
            schema_id=schema_id,
            schema_relpath=schema_relpath,
            result_id=f"{schema_id}-{day_utc}",
            suite_results=suite_rows,
            recorded_at=recorded_at,
        )
        artifact_paths[artifact_family] = write_testing_evidence_report_v1(
            truth_root=TRUTH_ROOT,
            artifact_family=artifact_family,
            day_utc=day_utc,
            payload=payload,
            schema_relpath=schema_relpath,
        )

    scenario_catalog_payload = build_scenario_catalog_v1(
        catalog_id=f"scenario-catalog-{day_utc}",
        scenarios=_build_scenario_catalog_rows(group_rows),
        created_at=recorded_at,
    )
    artifact_paths["scenario_catalog_v1"] = write_testing_evidence_report_v1(
        truth_root=TRUTH_ROOT,
        artifact_family="scenario_catalog_v1",
        day_utc=day_utc,
        payload=scenario_catalog_payload,
        schema_relpath=SCENARIO_CATALOG_SCHEMA,
    )

    subsystem_rows = _subsystem_rows_from_artifacts(day_utc, artifact_paths)
    subsystem_payload = build_subsystem_readiness_report_v1(
        readiness_id=f"subsystem-readiness-{day_utc}",
        subsystem_rows=subsystem_rows,
        recorded_at=recorded_at,
    )
    subsystem_path = write_testing_evidence_report_v1(
        truth_root=TRUTH_ROOT,
        artifact_family="subsystem_readiness_report_v1",
        day_utc=day_utc,
        payload=subsystem_payload,
        schema_relpath=SUBSYSTEM_READINESS_REPORT_SCHEMA,
    )
    artifact_paths["subsystem_readiness_report_v1"] = subsystem_path

    subsystem_payload_from_disk = read_testing_evidence_report_v1(
        truth_root=TRUTH_ROOT,
        artifact_family="subsystem_readiness_report_v1",
        day_utc=day_utc,
        schema_relpath=SUBSYSTEM_READINESS_REPORT_SCHEMA,
    )
    unresolved_blockers = sorted(
        {
            f"{row['subsystem_name']}:{issue}"
            for row in subsystem_payload_from_disk["subsystem_rows"]
            for issue in row["blocking_issues"]
        }
    )
    unresolved_gaps = sorted(
        {
            f"{row['subsystem_name']}:{gap}"
            for row in subsystem_payload_from_disk["subsystem_rows"]
            for gap in row["semantic_gaps"]
        }
    )
    ledger_path = resolve_paper_session_ledger_path(truth_root=TRUTH_ROOT, day_utc=day_utc)
    try:
        ledger_ref = read_paper_session_ledger_ref_v1(truth_root=TRUTH_ROOT, day_utc=day_utc)
        control_state = ledger_ref.payload.get("control_state") if isinstance(ledger_ref.payload.get("control_state"), dict) else {}
        ledger_authority_status = str(control_state.get("authority_status") or "").strip().upper() or "UNKNOWN"
        ledger_path = ledger_ref.path
    except Exception:
        ledger_authority_status = "UNKNOWN"
    overall_status, paper_open_allowed = _derive_paper_open_state_from_governed_inputs(
        subsystem_payload_from_disk=subsystem_payload_from_disk,
        unresolved_blockers=unresolved_blockers,
        unresolved_semantic_gaps=unresolved_gaps,
        ledger_authority_status=ledger_authority_status,
    )
    if ledger_authority_status != "GRANTED":
        rationale = (
            "paper_open_allowed=false because the canonical paper-session ledger did not grant authority "
            "for this day."
        )
    else:
        rationale = (
            "paper_open_allowed=false because governed subsystem readiness artifacts still record unresolved blockers "
            "or unresolved semantic gaps."
            if (unresolved_blockers or unresolved_gaps)
            else "paper_open_allowed=true because governed subsystem readiness artifacts record no unresolved blockers or semantic gaps."
        )
    paper_payload = build_governed_paper_open_readiness_v1(
        readiness_id=f"paper-open-readiness-{day_utc}",
        environment_name="PAPER",
        subsystem_readiness_refs=[str(subsystem_path)],
        unresolved_blockers=unresolved_blockers,
        unresolved_semantic_gaps=unresolved_gaps,
        rationale=rationale,
        recorded_at=recorded_at,
        overall_status=overall_status,
        paper_open_allowed=paper_open_allowed,
        authority_scope="DERIVED_ONLY_VIEW",
        paper_session_ledger_ref=str(ledger_path),
        ledger_authority_status=ledger_authority_status,
    ).to_dict()
    paper_path = write_testing_evidence_report_v1(
        truth_root=TRUTH_ROOT,
        artifact_family="paper_open_readiness_v1",
        day_utc=day_utc,
        payload=paper_payload,
        schema_relpath=PAPER_OPEN_READINESS_SCHEMA,
    )
    artifact_paths["paper_open_readiness_v1"] = paper_path

    summary = {
        "day_utc": day_utc,
        "artifact_paths": {family: str(path) for family, path in sorted(artifact_paths.items())},
        "paper_open_allowed": paper_payload["paper_open_allowed"],
        "unresolved_blockers": paper_payload["unresolved_blockers"],
        "unresolved_semantic_gaps": paper_payload["unresolved_semantic_gaps"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not summary["unresolved_blockers"] and not summary["unresolved_semantic_gaps"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
