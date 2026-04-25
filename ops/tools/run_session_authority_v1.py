#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.session_authority_v1 import (
    SessionAuthorityRefV1,
    derive_target_day_build_payload_v1,
    resolve_session_authority_target_day_v1,
    write_target_day_build_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_account_binding


_BASE_DOWNSTREAM_BUILD_CYCLE_SCRIPTS = {
    "ops/tools/run_submit_boundary_status_v1.py",
    "ops/tools/run_paper_session_ledger_v1.py",
    "ops/tools/run_startup_proof_validation_v1.py",
    "ops/tools/run_trading_day_state_machine_v1.py",
}

_PAPER_OPEN_NONBLOCKING_BUILD_SCRIPTS = {
    "ops/tools/run_paper_startup_authorization_convergence_v1.py",
    "ops/tools/run_paper_policy_verdict_v1.py",
    "ops/tools/run_startup_materialization_v1.py",
}


def _downstream_build_cycle_scripts_for_environment_v1(environment: str) -> set[str]:
    env = str(environment or "").strip().upper()
    scripts = set(_BASE_DOWNSTREAM_BUILD_CYCLE_SCRIPTS)
    if env == "PAPER":
        scripts.update(_PAPER_OPEN_NONBLOCKING_BUILD_SCRIPTS)
    return scripts


def _run_target_day_build(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    downstream_build_cycle_scripts: set[str],
) -> SessionAuthorityRefV1:
    from ops.tools.run_session_authority_control_plane_v1 import (
        collect_target_day_build_artifact_rows_v1,
    )
    from ops.tools.run_session_authority_diagnostic_v1 import (
        compute_hidden_dependency_check_result_v1,
    )
    from ops.tools.run_session_authority_orchestration_v1 import (
        collect_target_day_build_source_refs_v1,
        finalize_target_day_build_v1,
        run_primary_sleeve_capability_initialization_v1,
    )

    source_refs = collect_target_day_build_source_refs_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        run_primary_sleeve_capability_initialization_fn=run_primary_sleeve_capability_initialization_v1,
    )

    return finalize_target_day_build_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        source_refs=source_refs,
        collect_target_day_build_artifact_rows_fn=collect_target_day_build_artifact_rows_v1,
        compute_hidden_dependency_check_result_fn=compute_hidden_dependency_check_result_v1,
        derive_target_day_build_payload_fn=derive_target_day_build_payload_v1,
        write_target_day_build_fn=write_target_day_build_v1,
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
    )


def _resolve_ib_account(environment: str, requested_ib_account: str) -> str:
    env = str(environment).strip().upper()
    requested = str(requested_ib_account or "").strip()
    if env == "PAPER" and not requested:
        return resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    binding = resolve_governed_account_binding(
        repo_root=REPO_ROOT,
        environment=env,
        requested_ib_account=requested,
    )
    return binding.ib_account


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_session_authority_v1")
    ap.add_argument("--target_day", default="")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", default="")
    ap.add_argument("--phase", default="all", choices=["build", "admit", "activate", "all"])
    args = ap.parse_args(argv)

    from ops.tools.run_session_authority_orchestration_v1 import (
        finalize_session_authority_cli_result_v1,
        run_authority_reporting_tools_v1,
        run_post_build_alignment_tools_v1,
        run_session_authority_phase_flow_v1,
    )

    target_day = resolve_session_authority_target_day_v1(args.target_day)
    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    environment = str(args.environment).strip().upper()
    ib_account = _resolve_ib_account(environment, str(args.ib_account or "").strip())
    downstream_build_cycle_scripts = _downstream_build_cycle_scripts_for_environment_v1(environment)

    phase_bundle = run_session_authority_phase_flow_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        phase=args.phase,
        repo_root=REPO_ROOT,
        build_fn=lambda: _run_target_day_build(
            truth_root=truth_root,
            target_day=target_day,
            environment=environment,
            ib_account=ib_account,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        ),
        post_build_alignment_fn=lambda root, day: run_post_build_alignment_tools_v1(
            repo_root=REPO_ROOT,
            truth_root=root,
            target_day=day,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        ),
        reporting_fn=lambda root, day, include_control_plane: run_authority_reporting_tools_v1(
            repo_root=REPO_ROOT,
            truth_root=root,
            target_day=day,
            environment=environment,
            include_control_plane=include_control_plane,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        ),
    )

    cli_result = finalize_session_authority_cli_result_v1(
        truth_root=truth_root,
        target_day=target_day,
        phase=args.phase,
        environment=environment,
        ib_account=ib_account,
        build_ref=phase_bundle["build_ref"],
        admission_ref=phase_bundle["admission_ref"],
        active_ref=phase_bundle["active_ref"],
    )
    print(json.dumps(dict(cli_result["summary"]), sort_keys=True))
    return int(cli_result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
