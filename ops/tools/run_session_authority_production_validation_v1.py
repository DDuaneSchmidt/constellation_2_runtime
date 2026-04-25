#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.session_authority_monitor_v1 import (
    build_session_authority_status_payload_v1,
    derive_session_authority_alert_payload_v1,
    render_session_authority_alert_summary_v1,
    render_session_authority_status_summary_v1,
    write_session_authority_alert_v1,
    write_session_authority_status_v1,
)
from constellation_2.common.session_authority_v1 import (
    CLOSURE_STATUS_CLOSED,
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    derive_target_day_build_payload_v1,
    write_active_session_v1,
    write_target_day_admission_v1,
    write_target_day_build_v1,
)


DAY = "2026-04-10"
PRIOR_DAY = "2026-04-09"


def _artifact_row(
    tmp_path: Path,
    artifact_id: str,
    *,
    result_status: str = "PASS",
    role_class: str = "REQUIRED_DERIVED_GATE",
    blocking_reason_code: str = "",
    blocker_codes: list[str] | None = None,
    required: bool = True,
    path_family: str = "CANONICAL_RUNTIME_TRUTH_SUBPATH",
    target_day_expected: str = DAY,
    target_day_observed: str = DAY,
    date_binding_status: str = "MATCH",
    freshness_status: str = "CURRENT",
    provenance_present: bool = True,
    closure_status: str | None = None,
    observed_dependency_artifacts: list[str] | None = None,
) -> dict:
    if closure_status is None:
        closure_status = (
            CLOSURE_STATUS_CLOSED
            if result_status == "PASS"
            and path_family.startswith("CANONICAL_RUNTIME_TRUTH")
            and date_binding_status == "MATCH"
            and freshness_status == "CURRENT"
            and provenance_present
            else "OPEN"
        )
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": required,
        "role_class": role_class,
        "classification": "VALIDATION_FIXTURE",
        "canonical_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "authority_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "path_family": path_family,
        "observed_status": result_status,
        "result_status": result_status,
        "blocker_codes": blocker_codes or ([blocking_reason_code] if blocking_reason_code else []),
        "blocking_reason_code": blocking_reason_code,
        "schema_status": "VALID",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": freshness_status,
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": date_binding_status,
        "date_binding_value": target_day_observed,
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": provenance_present,
            "fields_present": ["producer.module", "generated_utc"] if provenance_present else [],
            "source": "validation_fixture" if provenance_present else "",
        },
        "closure_status": closure_status,
        "producer": {"module": "validation_fixture", "git_sha": "fixture"},
        "source_refs": [],
        "observed_dependency_artifacts": list(observed_dependency_artifacts or []),
    }


def _emit_fixture_state(
    *,
    truth_root: Path,
    artifact_rows: list[dict],
    target_day: str = DAY,
    prior_active: bool = False,
) -> dict:
    build_ref = write_target_day_build_v1(
        truth_root=truth_root,
        payload=derive_target_day_build_payload_v1(
            truth_root=truth_root,
            target_day=target_day,
            artifact_results=artifact_rows,
            source_refs=[],
        ),
    )
    admission_ref = write_target_day_admission_v1(
        truth_root=truth_root,
        payload=derive_target_day_admission_payload_v1(
            truth_root=truth_root,
            target_day=target_day,
            build_ref=build_ref,
        ),
    )
    prior_active_ref = None
    if prior_active:
        prior_build_ref = write_target_day_build_v1(
            truth_root=truth_root,
            payload=derive_target_day_build_payload_v1(
                truth_root=truth_root,
                target_day=PRIOR_DAY,
                artifact_results=[_artifact_row(truth_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected=PRIOR_DAY, target_day_observed=PRIOR_DAY)],
                source_refs=[],
            ),
        )
        prior_admission_ref = write_target_day_admission_v1(
            truth_root=truth_root,
            payload=derive_target_day_admission_payload_v1(
                truth_root=truth_root,
                target_day=PRIOR_DAY,
                build_ref=prior_build_ref,
            ),
        )
        prior_active_ref = write_active_session_v1(
            truth_root=truth_root,
            payload=derive_active_session_payload_v1(
                truth_root=truth_root,
                target_day=PRIOR_DAY,
                admission_ref=prior_admission_ref,
            ),
        )
    active_ref = write_active_session_v1(
        truth_root=truth_root,
        payload=derive_active_session_payload_v1(
            truth_root=truth_root,
            target_day=target_day,
            admission_ref=admission_ref,
            prior_active_session_ref=prior_active_ref,
        ),
    )
    status_ref = write_session_authority_status_v1(
        truth_root=truth_root,
        payload=build_session_authority_status_payload_v1(
            truth_root=truth_root,
            environment="PAPER",
        ),
    )
    alert_ref = write_session_authority_alert_v1(
        truth_root=truth_root,
        payload=derive_session_authority_alert_payload_v1(
            truth_root=truth_root,
            environment="PAPER",
            status_ref=status_ref,
        ),
    )
    return {
        "build_ref": {"path": str(build_ref.path), "sha256": build_ref.sha256},
        "admission_ref": {"path": str(admission_ref.path), "sha256": admission_ref.sha256},
        "active_ref": {"path": str(active_ref.path), "sha256": active_ref.sha256},
        "status_ref": {"path": str(status_ref.path), "sha256": status_ref.sha256},
        "alert_ref": {"path": str(alert_ref.path), "sha256": alert_ref.sha256},
        "status_summary": render_session_authority_status_summary_v1(status_ref.payload),
        "alert_summary": render_session_authority_alert_summary_v1(alert_ref.payload),
        "status": status_ref.payload,
        "alert": alert_ref.payload,
    }


def _scenario_missing_artifact(tmp_root: Path) -> dict:
    return _emit_fixture_state(
        truth_root=tmp_root,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_root,
                "market_calendar_day",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                blocker_codes=["MARKET_CALENDAR_DAY_MISSING"],
                freshness_status="STALE",
                provenance_present=False,
                closure_status="OPEN",
            )
        ],
    )


def _scenario_stale_artifact(tmp_root: Path) -> dict:
    return _emit_fixture_state(
        truth_root=tmp_root,
        prior_active=True,
        artifact_rows=[
            _artifact_row(tmp_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_root, "paper_policy_verdict_v1"),
            _artifact_row(
                tmp_root,
                "trade_submit_readiness_c2_v1",
                freshness_status="STALE",
                blocking_reason_code="STALE_ARTIFACT",
                closure_status="OPEN",
            ),
            _artifact_row(tmp_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )


def _scenario_hidden_dependency(tmp_root: Path) -> dict:
    build_ref = write_target_day_build_v1(
        truth_root=tmp_root,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_root,
            target_day=DAY,
            artifact_results=[
                _artifact_row(tmp_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_root, "paper_policy_verdict_v1"),
                _artifact_row(tmp_root, "trade_submit_readiness_c2_v1"),
                _artifact_row(tmp_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
            ],
            source_refs=[],
            hidden_dependency_check_result={
                "status": "FAIL",
                "blocking_reason_code": "HIDDEN_DEPENDENCY_DETECTED",
                "summary": "undeclared_dependency_artifacts=unexpected_gate_v1",
                "declared_inventory_artifacts": ["market_calendar_day", "paper_policy_verdict_v1", "trade_submit_readiness_c2_v1", "trading_day_state_machine_v1"],
                "observed_dependency_artifacts": ["market_calendar_day", "paper_policy_verdict_v1", "trade_submit_readiness_c2_v1", "trading_day_state_machine_v1", "unexpected_gate_v1"],
                "undeclared_dependency_artifacts": ["unexpected_gate_v1"],
            },
        ),
    )
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_root,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_root, target_day=DAY, build_ref=build_ref),
    )
    prior_build_ref = write_target_day_build_v1(
        truth_root=tmp_root,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_root,
            target_day=PRIOR_DAY,
            artifact_results=[_artifact_row(tmp_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected=PRIOR_DAY, target_day_observed=PRIOR_DAY)],
            source_refs=[],
        ),
    )
    prior_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_root,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_root, target_day=PRIOR_DAY, build_ref=prior_build_ref),
    )
    prior_active_ref = write_active_session_v1(
        truth_root=tmp_root,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_root,
            target_day=PRIOR_DAY,
            admission_ref=prior_admission_ref,
        ),
    )
    write_active_session_v1(
        truth_root=tmp_root,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_root,
            target_day=DAY,
            admission_ref=admission_ref,
            prior_active_session_ref=prior_active_ref,
        ),
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_root,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_root, environment="PAPER"),
    )
    alert_ref = write_session_authority_alert_v1(
        truth_root=tmp_root,
        payload=derive_session_authority_alert_payload_v1(
            truth_root=tmp_root,
            environment="PAPER",
            status_ref=status_ref,
        ),
    )
    return {
        "status": status_ref.payload,
        "alert": alert_ref.payload,
        "status_summary": render_session_authority_status_summary_v1(status_ref.payload),
        "alert_summary": render_session_authority_alert_summary_v1(alert_ref.payload),
    }


def _scenario_admit(tmp_root: Path) -> dict:
    return _emit_fixture_state(
        truth_root=tmp_root,
        artifact_rows=[
            _artifact_row(tmp_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_root, "paper_policy_verdict_v1"),
            _artifact_row(tmp_root, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_session_authority_production_validation_v1")
    ap.add_argument("--truth_root", default="", help="Optional canonical truth root for live read-only validation.")
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--skip-live", action="store_true")
    args = ap.parse_args(argv)

    result: dict[str, object] = {
        "environment": str(args.environment).strip().upper(),
        "fixture_scenarios": {},
        "live_runtime_validation": {},
    }

    if not args.skip_live and str(args.truth_root or "").strip():
        live_truth_root = Path(str(args.truth_root).strip()).resolve()
        live_status = build_session_authority_status_payload_v1(
            truth_root=live_truth_root,
            environment=str(args.environment).strip().upper(),
        )
        result["live_runtime_validation"] = {
            "truth_root": str(live_truth_root),
            "status_summary": render_session_authority_status_summary_v1(live_status),
            "status_severity": live_status["status_severity"],
            "target_day_admission_status": live_status["target_day_admission_status"],
            "rollover_status": live_status["rollover_status"],
            "top_blocker_reason_codes": live_status["top_blocker_reason_codes"],
        }

    with tempfile.TemporaryDirectory(prefix="session_authority_validation_") as tmpdir:
        tmp_root = Path(tmpdir).resolve()
        result["fixture_scenarios"] = {
            "missing_artifact_blocks": _scenario_missing_artifact(tmp_root / "missing"),
            "stale_artifact_blocks": _scenario_stale_artifact(tmp_root / "stale"),
            "hidden_dependency_blocks": _scenario_hidden_dependency(tmp_root / "hidden"),
            "complete_artifact_set_admits": _scenario_admit(tmp_root / "admit"),
        }

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
