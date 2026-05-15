from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_release_integrity_status_v1 import build_aegis_release_integrity_status_v1  # noqa: E402


NOW = "2026-05-14T22:30:00Z"


def _head() -> str:
    return subprocess.check_output(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True).strip()


def test_release_mismatch_requires_operator_warning(tmp_path: Path) -> None:
    manifest = tmp_path / "current_release.v1.json"
    manifest.write_text(json.dumps({"commit": "0" * 40, "release_id": "old", "release_path": "/tmp/old"}), encoding="utf-8")

    status = build_aegis_release_integrity_status_v1(
        generated_at_utc=NOW,
        repo_root=REPO_ROOT,
        current_release_manifest=manifest,
        runtime_data_root=tmp_path,
    )

    assert status["release_match_status"] == "MISMATCH"
    assert status["operator_warning_required"] is True
    assert "ACTIVE_RELEASE_REPO_MISMATCH" in status["reason_codes"]


def test_matching_release_clears_release_mismatch_warning(tmp_path: Path) -> None:
    manifest = tmp_path / "current_release.v1.json"
    manifest.write_text(json.dumps({"commit": _head(), "release_id": "current", "release_path": "/tmp/current"}), encoding="utf-8")

    status = build_aegis_release_integrity_status_v1(
        generated_at_utc=NOW,
        repo_root=REPO_ROOT,
        current_release_manifest=manifest,
        runtime_data_root=tmp_path,
    )

    assert status["release_match_status"] == "MATCH"
    assert "ACTIVE_RELEASE_REPO_MISMATCH" not in status["reason_codes"]
