from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, WriteResultV1, write_file_immutable_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
EXECUTION_KERNEL_ROOT_NAME = 'execution_kernel_v1'


def _truth_root(truth_root: str | Path | None) -> Path:
    if truth_root is None or str(truth_root).strip() == '':
        return resolve_truth_root(repo_root=REPO_ROOT).resolve()
    return Path(truth_root).expanduser().resolve()


def execution_kernel_root_for_truth_root(*, truth_root: str | Path | None) -> Path:
    return (_truth_root(truth_root) / EXECUTION_KERNEL_ROOT_NAME).resolve()


def execution_submission_decision_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_decision_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'submission_decisions'
        / str(day_utc)
        / f'{submission_decision_id}.execution_submission_decision.v1.json'
    ).resolve()


def approved_change_set_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    approved_change_set_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'approved_change_sets'
        / str(day_utc)
        / f'{approved_change_set_id}.approved_change_set.v1.json'
    ).resolve()


def multi_delta_execution_decision_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    multi_delta_execution_decision_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'multi_delta_execution_decisions'
        / str(day_utc)
        / f'{multi_delta_execution_decision_id}.multi_delta_execution_decision.v1.json'
    ).resolve()


def multi_delta_execution_record_dir_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    multi_delta_execution_record_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'multi_delta_execution_records'
        / str(day_utc)
        / str(multi_delta_execution_record_id)
    ).resolve()


def multi_delta_execution_record_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    multi_delta_execution_record_id: str,
) -> Path:
    return multi_delta_execution_record_dir_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        multi_delta_execution_record_id=multi_delta_execution_record_id,
    ) / 'multi_delta_execution_record.v1.json'


def execution_set_intent_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    execution_set_intent_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'execution_set_intents'
        / str(day_utc)
        / f'{execution_set_intent_id}.execution_set_intent.v1.json'
    ).resolve()


def execution_submission_record_dir_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'submission_records'
        / str(day_utc)
        / str(submission_id)
    ).resolve()


def execution_submission_record_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> Path:
    return execution_submission_record_dir_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
    ) / 'submission_record.v1.json'


def execution_state_record_dir_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'execution_state_records'
        / str(day_utc)
        / str(submission_id)
    ).resolve()


def execution_state_record_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
    execution_state_record_id: str,
) -> Path:
    return execution_state_record_dir_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
    ) / f'{execution_state_record_id}.execution_state_record.v1.json'


def execution_lifecycle_decision_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    execution_lifecycle_decision_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'lifecycle_decisions'
        / str(day_utc)
        / f'{execution_lifecycle_decision_id}.execution_lifecycle_decision.v1.json'
    ).resolve()


def execution_lifecycle_run_envelope_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    run_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'lifecycle_run_envelopes'
        / str(day_utc)
        / f'{run_id}.execution_lifecycle_run_envelope.v1.json'
    ).resolve()


def execution_run_envelope_path_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    run_id: str,
) -> Path:
    return (
        execution_kernel_root_for_truth_root(truth_root=truth_root)
        / 'run_envelopes'
        / str(day_utc)
        / f'{run_id}.execution_run_envelope.v1.json'
    ).resolve()


def submission_evidence_dir_for_truth_root_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> Path:
    return (_truth_root(truth_root) / 'execution_evidence_v1' / 'submissions' / str(day_utc) / str(submission_id)).resolve()


def load_execution_submission_record_by_submission_id_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> dict[str, Any]:
    path = execution_submission_record_path_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
    )
    if not path.exists():
        raise ValueError(f'MISSING_EXECUTION_SUBMISSION_RECORD:{path}')
    return read_json_obj_v1(path)


def fill_ledger_path_for_truth_root_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> Path:
    return (_truth_root(truth_root) / 'fill_ledger_v1' / str(day_utc) / f'{submission_id}.fill_ledger.v1.json').resolve()


def write_immutable_json_v1(path: Path, obj: dict[str, Any]) -> WriteResultV1:
    payload = canonical_json_bytes_v1(obj) + b'\n'
    return write_file_immutable_v1(path=path.resolve(), data=payload, create_dirs=True)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fsync_dir(path: Path) -> None:
    dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_exclusive_immutable_json_v1(path: Path, obj: dict[str, Any]) -> WriteResultV1:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b'\n'
    candidate_sha = _sha256(payload)

    tmp_fd: int | None = None
    tmp_path: str | None = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(prefix=f'.{path.name}.exclusive.', dir=str(path.parent))
        os.write(tmp_fd, payload)
        os.fsync(tmp_fd)
        os.close(tmp_fd)
        tmp_fd = None

        try:
            os.link(tmp_path, str(path))
            _fsync_dir(path)
            return WriteResultV1(path=str(path), sha256=candidate_sha, bytes_written=len(payload), action='WROTE')
        except FileExistsError:
            if not path.exists() or not path.is_file():
                raise ImmutableWriteError(f'TARGET_NOT_FILE:{path}')
            existing = path.read_bytes()
            existing_sha = _sha256(existing)
            if existing_sha == candidate_sha:
                return WriteResultV1(path=str(path), sha256=candidate_sha, bytes_written=0, action='SKIP_IDENTICAL')
            raise ImmutableWriteError(
                f'ATTEMPTED_REWRITE:{path} existing_sha={existing_sha} candidate_sha={candidate_sha}'
            )
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except Exception:
                pass
        if tmp_path is not None and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def truth_root_from_execution_kernel_artifact_path_v1(path: str | Path) -> Path:
    artifact_path = Path(path).expanduser().resolve()
    for parent in artifact_path.parents:
        if parent.name == EXECUTION_KERNEL_ROOT_NAME:
            return parent.parent.resolve()
    raise ValueError(f'EXECUTION_KERNEL_ROOT_NOT_FOUND:{artifact_path}')


def read_json_obj_v1(path: str | Path) -> dict[str, Any]:
    obj = json.loads(Path(path).expanduser().resolve().read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT:{path}')
    return obj
