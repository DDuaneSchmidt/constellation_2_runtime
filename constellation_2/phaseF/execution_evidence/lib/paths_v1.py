from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()

TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

EXEC_ROOT = (TRUTH_ROOT / "execution_evidence_v1").resolve()


@dataclass(frozen=True)
class ExecEvidenceDayPathsV1:
    day_utc: str
    submissions_day_dir: Path
    manifests_day_dir: Path
    failures_day_dir: Path

    latest_path: Path
    failure_path: Path


def day_paths_v1(day_utc: str) -> ExecEvidenceDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    submissions_day_dir = EXEC_ROOT / "submissions" / day
    manifests_day_dir = EXEC_ROOT / "manifests" / day
    failures_day_dir = EXEC_ROOT / "failures" / day

    return ExecEvidenceDayPathsV1(
        day_utc=day,
        submissions_day_dir=submissions_day_dir,
        manifests_day_dir=manifests_day_dir,
        failures_day_dir=failures_day_dir,
        latest_path=EXEC_ROOT / "latest.json",
        failure_path=failures_day_dir / "failure.json",
    )


def submission_artifact_dir_v1(*, day_utc: str, submission_id: str) -> Path:
    dp = day_paths_v1(day_utc)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return dp.submissions_day_dir / sid


def submission_manifest_path_v1(*, day_utc: str, submission_id: str) -> Path:
    dp = day_paths_v1(day_utc)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return dp.manifests_day_dir / f"{sid}.manifest.json"

def submission_manifest_identity_patch_path_v1(*, day_utc: str, submission_id: str) -> Path:
    dp = day_paths_v1(day_utc)
    sid = (submission_id or "").strip()
    if not sid:
        raise ValueError("SUBMISSION_ID_REQUIRED")
    return (dp.manifests_day_dir / f"{sid}.manifest_identity_patch.v1.json").resolve()
