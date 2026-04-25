from __future__ import annotations

from pathlib import Path

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    read_json_obj_v1,
    write_exclusive_immutable_json_v1,
    write_immutable_json_v1,
)
from constellation_2.common.truth_root_v1 import resolve_truth_root


REPO_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_CONTROL_KERNEL_ROOT_NAME = 'runtime_control_kernel_v1'


def _truth_root(truth_root: str | Path | None) -> Path:
    if truth_root is None or str(truth_root).strip() == '':
        return resolve_truth_root(repo_root=REPO_ROOT).resolve()
    return Path(truth_root).expanduser().resolve()


def runtime_control_kernel_root_for_truth_root(*, truth_root: str | Path | None) -> Path:
    return (_truth_root(truth_root) / RUNTIME_CONTROL_KERNEL_ROOT_NAME).resolve()


def runtime_control_decision_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    runtime_control_decision_id: str,
) -> Path:
    return (
        runtime_control_kernel_root_for_truth_root(truth_root=truth_root)
        / 'decisions'
        / str(day_utc)
        / f'{runtime_control_decision_id}.runtime_control_decision.v1.json'
    ).resolve()


def runtime_control_record_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    environment: str,
    ib_account: str,
    runtime_control_record_id: str,
) -> Path:
    return (
        runtime_control_kernel_root_for_truth_root(truth_root=truth_root)
        / 'records'
        / str(day_utc)
        / str(environment).strip().upper()
        / str(ib_account).strip()
        / f'{runtime_control_record_id}.runtime_control_record.v1.json'
    ).resolve()


def runtime_control_run_envelope_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    run_id: str,
) -> Path:
    return (
        runtime_control_kernel_root_for_truth_root(truth_root=truth_root)
        / 'run_envelopes'
        / str(day_utc)
        / f'{run_id}.runtime_control_run_envelope.v1.json'
    ).resolve()

