from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from constellation_2.common.truth_root_v1 import resolve_truth_root


REPO_ROOT = Path(__file__).resolve().parents[4]
TRUTH_ROOT = resolve_truth_root(repo_root=REPO_ROOT)

EXEC_EVIDENCE_ROOT = (TRUTH_ROOT / "execution_evidence_v1").resolve()
PHASED_SUBMISSIONS_ROOT = (EXEC_EVIDENCE_ROOT / "submissions").resolve()


def _normalize_truth_root(truth_root: Path | str | None) -> Path:
    if truth_root is None:
        return TRUTH_ROOT
    return Path(truth_root).expanduser().resolve()


def execution_evidence_root_for_truth_root(*, truth_root: Path | str | None) -> Path:
    root = _normalize_truth_root(truth_root)
    return (root / "execution_evidence_v1").resolve()


def phased_submissions_root_for_truth_root(*, truth_root: Path | str | None) -> Path:
    return (execution_evidence_root_for_truth_root(truth_root=truth_root) / "submissions").resolve()


@dataclass(frozen=True)
class ExecEvidenceDayPathsV1:
    day_utc: str
    submissions_day_dir: Path
    manifests_day_dir: Path
    latest_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_v1(day_utc: str) -> ExecEvidenceDayPathsV1:
    return day_paths_for_truth_root_v1(day_utc=day_utc, truth_root=None)


def day_paths_for_truth_root_v1(*, day_utc: str, truth_root: Path | str | None) -> ExecEvidenceDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    exec_root = execution_evidence_root_for_truth_root(truth_root=truth_root)

    return ExecEvidenceDayPathsV1(
        day_utc=day,
        submissions_day_dir=(exec_root / "submissions" / day).resolve(),
        manifests_day_dir=(exec_root / "manifests" / day).resolve(),
        latest_path=(exec_root / "latest_pointer.v1.json").resolve(),
        failure_dir=(exec_root / "failures" / day).resolve(),
        failure_path=(exec_root / "failures" / day / "failure.json").resolve(),
    )


def submission_artifact_dir_v1(*, day_utc: str, submission_id: str) -> Path:
    return submission_artifact_dir_for_truth_root_v1(day_utc=day_utc, submission_id=submission_id, truth_root=None)


def submission_artifact_dir_for_truth_root_v1(*, day_utc: str, submission_id: str, truth_root: Path | str | None) -> Path:
    dp = day_paths_for_truth_root_v1(day_utc=day_utc, truth_root=truth_root)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.submissions_day_dir / sid).resolve()


def submission_manifest_path_v1(*, day_utc: str, submission_id: str) -> Path:
    return submission_manifest_path_for_truth_root_v1(day_utc=day_utc, submission_id=submission_id, truth_root=None)


def submission_manifest_path_for_truth_root_v1(*, day_utc: str, submission_id: str, truth_root: Path | str | None) -> Path:
    dp = day_paths_for_truth_root_v1(day_utc=day_utc, truth_root=truth_root)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.manifests_day_dir / f"{sid}.manifest.json").resolve()


def submission_manifest_identity_patch_path_v1(*, day_utc: str, submission_id: str) -> Path:
    return submission_manifest_identity_patch_path_for_truth_root_v1(
        day_utc=day_utc,
        submission_id=submission_id,
        truth_root=None,
    )


def submission_manifest_identity_patch_path_for_truth_root_v1(
    *,
    day_utc: str,
    submission_id: str,
    truth_root: Path | str | None,
) -> Path:
    dp = day_paths_for_truth_root_v1(day_utc=day_utc, truth_root=truth_root)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.manifests_day_dir / f"{sid}.manifest_identity_patch.v1.json").resolve()
