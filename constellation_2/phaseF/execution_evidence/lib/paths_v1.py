from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from constellation_2.common.truth_root_v1 import resolve_truth_root


REPO_ROOT = Path(__file__).resolve().parents[4]
TRUTH_ROOT = resolve_truth_root(repo_root=REPO_ROOT)

EXEC_EVIDENCE_ROOT = (TRUTH_ROOT / "execution_evidence_v1").resolve()
PHASED_SUBMISSIONS_ROOT = (EXEC_EVIDENCE_ROOT / "submissions").resolve()


@dataclass(frozen=True)
class ExecEvidenceDayPathsV1:
    day_utc: str
    submissions_day_dir: Path
    manifests_day_dir: Path
    latest_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_v1(day_utc: str) -> ExecEvidenceDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    return ExecEvidenceDayPathsV1(
        day_utc=day,
        submissions_day_dir=(EXEC_EVIDENCE_ROOT / "submissions" / day).resolve(),
        manifests_day_dir=(EXEC_EVIDENCE_ROOT / "manifests" / day).resolve(),
        latest_path=(EXEC_EVIDENCE_ROOT / "latest_pointer.v1.json").resolve(),
        failure_dir=(EXEC_EVIDENCE_ROOT / "failures" / day).resolve(),
        failure_path=(EXEC_EVIDENCE_ROOT / "failures" / day / "failure.json").resolve(),
    )


def submission_artifact_dir_v1(*, day_utc: str, submission_id: str) -> Path:
    dp = day_paths_v1(day_utc)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.submissions_day_dir / sid).resolve()


def submission_manifest_path_v1(*, day_utc: str, submission_id: str) -> Path:
    dp = day_paths_v1(day_utc)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.manifests_day_dir / f"{sid}.manifest.json").resolve()


def submission_manifest_identity_patch_path_v1(*, day_utc: str, submission_id: str) -> Path:
    dp = day_paths_v1(day_utc)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.manifests_day_dir / f"{sid}.manifest_identity_patch.v1.json").resolve()
