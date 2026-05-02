#!/usr/bin/env python3
"""
c2_submit_paper_v5.py

Canonical submit entrypoint for PAPER execution.

Primary path:
- consume a durable execution submission record that freezes a sealed execution package.

Legacy compatibility path:
- disabled. Raw candidate submit is no longer authoritative.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[3]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / 'constellation_2').exists():
    raise SystemExit(f'FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}')

import argparse  # noqa: E402
from constellation_2.common.constitutional_runtime_v1 import (  # noqa: E402
    ConstitutionalRuntimeError,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.execution_build_authority_v1 import run_execution_build_authority_v1  # noqa: E402
from constellation_2.common.execution_kernel.execution_submission_record_v1 import (  # noqa: E402
    build_execution_submission_record_from_execution_package_v1,
)
from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line  # noqa: E402
from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import run_submit_boundary_paper_v4  # noqa: E402
from ops.tools.aegis_submit_enforcement_v1 import require_submit_enforcement_v1  # noqa: E402

BROKER_TRANSMIT_ENABLEMENT_MSG = (
    'broker transmit disabled by default; explicit micro-live path requires '
    'C2_ENABLE_BROKER_TRANSMIT=YES with --dry_run NO'
)


def _read_json_object(path: Path) -> dict:
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise SystemExit(f'FAIL_CLOSED: json_top_level_not_object:path={path}')
    return obj


def _looks_like_dependency_sha_mismatch(exc: ConstitutionalRuntimeError) -> bool:
    return 'CONSTITUTIONAL_ARTIFACT_DEPENDENCY_REF_SHA256_MISMATCH' in str(exc)


def _safe_eval_stamp(eval_time_utc: str) -> str:
    return (
        str(eval_time_utc).strip()
        .replace('-', '')
        .replace(':', '')
        .replace('T', '_')
        .replace('Z', 'Z')
    )


def _require_package_context_fields(*, package_obj: dict, package_path: Path) -> tuple[str, str, Path]:
    day_utc = str(package_obj.get('day_utc') or '').strip()
    if not day_utc:
        raise SystemExit(f'FAIL_CLOSED: execution_package_day_utc_missing:path={package_path}')
    submission_id = str(package_obj.get('submission_id') or '').strip()
    if not submission_id:
        raise SystemExit(f'FAIL_CLOSED: execution_package_submission_id_missing:path={package_path}')
    candidate_ref = package_obj.get('candidate_ref') if isinstance(package_obj.get('candidate_ref'), dict) else {}
    execution_truth_root_text = str(candidate_ref.get('execution_truth_root') or '').strip()
    if not execution_truth_root_text:
        raise SystemExit(f'FAIL_CLOSED: execution_package_execution_truth_root_missing:path={package_path}')
    execution_truth_root = Path(execution_truth_root_text).resolve()
    return day_utc, submission_id, execution_truth_root


def _submission_refresh_path_v1(
    *,
    execution_truth_root: Path,
    day_utc: str,
    submission_id: str,
    eval_time_utc: str,
) -> Path:
    refresh_root = (
        execution_truth_root
        / 'execution_kernel_v1'
        / 'submission_records'
        / str(day_utc).strip()
        / str(submission_id).strip()
        / '_submit_refresh_v1'
    ).resolve()
    refresh_root.mkdir(parents=True, exist_ok=True)
    stem = _safe_eval_stamp(eval_time_utc)
    candidate = (refresh_root / f'{stem}.submission_record.v1.json').resolve()
    if not candidate.exists():
        return candidate
    seq = 1
    while True:
        candidate = (refresh_root / f'{stem}__r{seq:02d}.submission_record.v1.json').resolve()
        if not candidate.exists():
            return candidate
        seq += 1


def _submission_record_mismatch_reason_v1(
    *,
    submission_record_path: Path,
    package_path: Path,
    package_obj: dict,
) -> str:
    try:
        record = _read_json_object(submission_record_path)
    except Exception as exc:  # noqa: BLE001
        return f'load_failed:{type(exc).__name__}'

    if str(record.get('status') or '').strip() != 'READY_TO_SUBMIT':
        return 'status'

    package_ref = record.get('execution_package_ref')
    if not isinstance(package_ref, dict):
        return 'execution_package_ref'

    record_pkg_path = Path(str(package_ref.get('path') or '').strip()).resolve()
    if record_pkg_path != package_path.resolve():
        return 'package_path'

    record_pkg_sha = str(package_ref.get('sha256') or '').strip()
    package_sha = str(package_obj.get('canonical_json_hash') or '').strip()
    if not record_pkg_sha or not package_sha or record_pkg_sha != package_sha:
        return 'package_sha256'

    if str(record.get('submission_id') or '').strip() != str(package_obj.get('submission_id') or '').strip():
        return 'submission_id'

    return ''


def _write_refreshed_submission_record_v1(
    *,
    package_obj: dict,
    package_path: Path,
    eval_time_utc: str,
    day_utc: str,
    submission_id: str,
    execution_truth_root: Path,
) -> Path:
    refreshed_record = build_execution_submission_record_from_execution_package_v1(
        execution_package_obj=package_obj,
        execution_package_path=package_path,
        day_utc=day_utc,
        produced_utc=str(eval_time_utc).strip(),
    )
    bridge_path = _submission_refresh_path_v1(
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
        eval_time_utc=eval_time_utc,
    )
    bridge_payload = refreshed_record.to_dict()
    bridge_path.write_text(
        json.dumps(bridge_payload, sort_keys=True, separators=(',', ':')) + '\n',
        encoding='utf-8',
    )
    return bridge_path


def _refresh_trade_submit_readiness_for_submit(
    *,
    repo_root: Path,
    day_utc: str,
    ib_account: str,
    environment: str,
    canonical_truth_root: Path,
    execution_truth_root: Path,
) -> None:
    import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module

    resolved_repo_root = Path(repo_root).resolve()
    resolved_truth_root = Path(canonical_truth_root).resolve()
    resolved_execution_root = Path(execution_truth_root).resolve()
    original_repo_root = readiness_module.REPO_ROOT
    original_truth_root = readiness_module.TRUTH_ROOT
    original_out_root = readiness_module.OUT_ROOT
    original_resolve_execution_root = readiness_module.resolve_sleeve_execution_root_v1
    original_argv = list(sys.argv)
    try:
        readiness_module.REPO_ROOT = resolved_repo_root
        readiness_module.TRUTH_ROOT = resolved_truth_root
        readiness_module.resolve_sleeve_execution_root_v1 = lambda **kwargs: SimpleNamespace(
            execution_root_path=resolved_execution_root
        )
        sys.argv = [
            "run_trade_submit_readiness_c2_v1.py",
            "--day_utc",
            str(day_utc).strip(),
            "--ib_account",
            str(ib_account).strip(),
            "--environment",
            str(environment).strip().upper(),
        ]
        rc = int(readiness_module.main())
        if rc != 0:
            raise SystemExit(
                "FAIL_CLOSED: trade_submit_readiness_refresh_failed "
                f"rc={rc} day_utc={day_utc} environment={environment} ib_account={ib_account}"
            )
    finally:
        sys.argv = original_argv
        readiness_module.REPO_ROOT = original_repo_root
        readiness_module.TRUTH_ROOT = original_truth_root
        readiness_module.OUT_ROOT = original_out_root
        readiness_module.resolve_sleeve_execution_root_v1 = original_resolve_execution_root


def _refresh_submit_paths_if_constitutional_dependency_stale(
    *,
    repo_root: Path,
    eval_time_utc: str,
    execution_package_path: Path,
    submission_record_path: Path,
) -> tuple[Path, Path]:
    package_path = execution_package_path.resolve()
    submission_path = submission_record_path.resolve()
    package_obj = _read_json_object(package_path)
    day_utc, submission_id, execution_truth_root = _require_package_context_fields(
        package_obj=package_obj,
        package_path=package_path,
    )

    submission_mismatch_reason = _submission_record_mismatch_reason_v1(
        submission_record_path=submission_path,
        package_path=package_path,
        package_obj=package_obj,
    )
    if submission_mismatch_reason:
        submission_path = _write_refreshed_submission_record_v1(
            package_obj=package_obj,
            package_path=package_path,
            eval_time_utc=eval_time_utc,
            day_utc=day_utc,
            submission_id=submission_id,
            execution_truth_root=execution_truth_root,
        )
        print(
            'INFO: EXECUTION_SUBMISSION_RECORD_REFRESH_APPLIED '
            f'reason={submission_mismatch_reason} '
            f'submission_record_path={submission_path}'
        )

    build_ref = package_obj.get('build_ref') if isinstance(package_obj.get('build_ref'), dict) else {}
    build_path_text = str(build_ref.get('path') or '').strip()
    if not build_path_text:
        raise SystemExit(f'FAIL_CLOSED: execution_package_build_ref_path_missing:path={package_path}')
    build_path = Path(build_path_text).resolve()
    if not build_path.exists() or not build_path.is_file():
        raise SystemExit(f'FAIL_CLOSED: execution_build_artifact_missing:path={build_path}')
    build_obj = _read_json_object(build_path)

    try:
        validate_governed_artifact_payload_v1(
            repo_root=repo_root,
            artifact_id='execution_build_v1',
            payload=build_obj,
        )
        return package_path, submission_path
    except ConstitutionalRuntimeError as exc:
        if not _looks_like_dependency_sha_mismatch(exc):
            raise SystemExit(f'FAIL_CLOSED: execution_build_constitutional_invalid:{exc}') from exc

    candidate_ref = package_obj.get('candidate_ref') if isinstance(package_obj.get('candidate_ref'), dict) else {}
    candidate_path_text = str(candidate_ref.get('phasec_out_dir') or '').strip()
    if not candidate_path_text:
        raise SystemExit(f'FAIL_CLOSED: execution_package_candidate_ref_missing:path={package_path}')
    candidate_path = Path(candidate_path_text).resolve()
    if not candidate_path.exists() or not candidate_path.is_dir():
        raise SystemExit(f'FAIL_CLOSED: execution_package_candidate_path_missing:path={candidate_path}')
    package_environment = str(package_obj.get('environment') or '').strip().upper()
    if package_environment != 'PAPER':
        raise SystemExit(f'FAIL_CLOSED: execution_package_environment_not_paper:path={package_path}')
    ib_account = str(package_obj.get('ib_account') or '').strip()
    if not ib_account:
        raise SystemExit(f'FAIL_CLOSED: execution_package_ib_account_missing:path={package_path}')
    canonical_truth_root_text = str(candidate_ref.get('canonical_truth_root') or '').strip()
    if canonical_truth_root_text:
        canonical_truth_root = Path(canonical_truth_root_text).resolve()
    else:
        canonical_truth_root = resolve_truth_root(repo_root=repo_root.resolve()).resolve()
    _refresh_trade_submit_readiness_for_submit(
        repo_root=repo_root,
        day_utc=day_utc,
        ib_account=ib_account,
        environment=package_environment,
        canonical_truth_root=canonical_truth_root,
        execution_truth_root=execution_truth_root,
    )

    operation_type = str(package_obj.get('operation_type') or '').strip() or 'fresh_paper_entry_v1'
    refreshed = run_execution_build_authority_v1(
        repo_root=repo_root.resolve(),
        operation_type=operation_type,
        candidate_path=candidate_path,
        materialize=True,
        emit_package=True,
    )
    refreshed_package_path = Path(str(refreshed.get('package_path') or '')).resolve()
    if not refreshed_package_path.exists() or not refreshed_package_path.is_file():
        raise SystemExit(
            f'FAIL_CLOSED: execution_package_refresh_missing:operation_type={operation_type}:candidate_path={candidate_path}'
        )
    refreshed_package_obj = _read_json_object(refreshed_package_path)
    refreshed_day = str(refreshed_package_obj.get('day_utc') or '').strip()
    if not refreshed_day:
        raise SystemExit(f'FAIL_CLOSED: refreshed_execution_package_day_utc_missing:path={refreshed_package_path}')

    refreshed_submission_id = str(refreshed_package_obj.get('submission_id') or '').strip()
    if not refreshed_submission_id:
        raise SystemExit(f'FAIL_CLOSED: refreshed_execution_package_submission_id_missing:path={refreshed_package_path}')
    refreshed_candidate_ref = (
        refreshed_package_obj.get('candidate_ref')
        if isinstance(refreshed_package_obj.get('candidate_ref'), dict)
        else {}
    )
    refreshed_execution_truth_root_text = str(refreshed_candidate_ref.get('execution_truth_root') or '').strip()
    if not refreshed_execution_truth_root_text:
        raise SystemExit(f'FAIL_CLOSED: refreshed_execution_package_execution_truth_root_missing:path={refreshed_package_path}')
    refreshed_execution_truth_root = Path(refreshed_execution_truth_root_text).resolve()
    bridge_path = _write_refreshed_submission_record_v1(
        package_obj=refreshed_package_obj,
        package_path=refreshed_package_path,
        eval_time_utc=eval_time_utc,
        day_utc=refreshed_day,
        submission_id=refreshed_submission_id,
        execution_truth_root=refreshed_execution_truth_root,
    )
    print(
        'INFO: EXECUTION_SUBMIT_REFRESH_APPLIED '
        f'package_path={refreshed_package_path} '
        f'submission_record_path={bridge_path}'
    )
    return refreshed_package_path, bridge_path


def _require_broker_transmit_enabled(*, dry_run: str) -> None:
    if dry_run == 'YES':
        return
    enabled = str(os.environ.get('C2_ENABLE_BROKER_TRANSMIT') or '').strip().upper()
    if enabled != 'YES':
        raise SystemExit(f'FAIL_CLOSED: {BROKER_TRANSMIT_ENABLEMENT_MSG}')


def main() -> int:
    ap = argparse.ArgumentParser(prog='c2_submit_paper_v5')
    ap.add_argument('--eval_time_utc', required=True)
    ap.add_argument('--phasec_out_dir', default='')
    ap.add_argument('--execution_package_path', default='')
    ap.add_argument('--submission_record_path', default='')
    ap.add_argument('--legacy_raw_candidate', choices=['YES', 'NO'], default='NO')
    ap.add_argument(
        '--risk_budget',
        default=str(_REPO_ROOT_FROM_FILE / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json'),
    )
    ap.add_argument('--ib_host', required=True)
    ap.add_argument('--ib_port', required=True, type=int)
    ap.add_argument('--ib_client_id', required=True, type=int)
    ap.add_argument('--ib_account', required=True)
    ap.add_argument('--dry_run', required=True, choices=['YES', 'NO'])
    ap.add_argument('--submissions_root_override', default='')
    args = ap.parse_args()

    dry_run = str(args.dry_run).strip().upper()
    _require_broker_transmit_enabled(dry_run=dry_run)

    phasec_text = str(args.phasec_out_dir or '').strip()
    package_text = str(args.execution_package_path or '').strip()
    submission_record_text = str(args.submission_record_path or '').strip()
    if phasec_text:
        raise SystemExit('FAIL_CLOSED: raw candidate submit is disabled; supply --submission_record_path')
    if not submission_record_text or not package_text:
        raise SystemExit('FAIL_CLOSED: both --submission_record_path and --execution_package_path are required')
    package_path = Path(package_text).resolve()
    submission_record_path = Path(submission_record_text).resolve()
    package_obj = _read_json_object(package_path)
    day_utc, _submission_id, execution_truth_root = _require_package_context_fields(
        package_obj=package_obj,
        package_path=package_path,
    )
    candidate_ref = package_obj.get('candidate_ref') if isinstance(package_obj.get('candidate_ref'), dict) else {}
    canonical_truth_root_text = str(candidate_ref.get('canonical_truth_root') or '').strip()
    canonical_truth_root = Path(canonical_truth_root_text).resolve() if canonical_truth_root_text else resolve_truth_root(repo_root=_REPO_ROOT_FROM_FILE.resolve()).resolve()
    require_submit_enforcement_v1(
        truth_root=canonical_truth_root,
        execution_root=execution_truth_root,
        day_utc=day_utc,
        action_id='submit_paper_order',
    )
    package_path, submission_record_path = _refresh_submit_paths_if_constitutional_dependency_stale(
        repo_root=_REPO_ROOT_FROM_FILE,
        eval_time_utc=str(args.eval_time_utc).strip(),
        execution_package_path=package_path,
        submission_record_path=submission_record_path,
    )

    try:
        rc = run_submit_boundary_paper_v4(
            repo_root=_REPO_ROOT_FROM_FILE,
            eval_time_utc=str(args.eval_time_utc).strip(),
            phasec_out_dir=(Path(phasec_text).resolve() if phasec_text else None),
            execution_package_path=package_path,
            submission_record_path=submission_record_path,
            allow_legacy_raw_candidate=False,
            risk_budget_path=Path(str(args.risk_budget).strip()).resolve(),
            ib_host=str(args.ib_host).strip(),
            ib_port=int(args.ib_port),
            ib_client_id=int(args.ib_client_id),
            ib_account=str(args.ib_account).strip(),
            dry_run=(dry_run == 'YES'),
            submissions_root_override=(Path(args.submissions_root_override).resolve() if str(args.submissions_root_override).strip() else None),
            refresh_trade_submit_readiness=False,
        )
        return int(rc)
    except Exception as exc:  # noqa: BLE001
        print(format_failure_line(
            'c2_submit_paper_v5',
            classify_failure(exc),
            error=repr(exc),
            eval_time_utc=str(args.eval_time_utc).strip(),
            phasec_out_dir=(str(Path(phasec_text).resolve()) if phasec_text else ''),
            execution_package_path=(str(Path(package_text).resolve()) if package_text else ''),
            submission_record_path=(str(Path(submission_record_text).resolve()) if submission_record_text else ''),
            ib_account=str(args.ib_account).strip(),
        ), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
