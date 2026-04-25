from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseG.allocation.run import run_allocation_day_v2 as allocation_module


DAY = "2026-03-16"
GIT_SHA = "a" * 40


def test_run_allocation_day_v2_reuses_existing_valid_summary(monkeypatch, tmp_path: Path) -> None:
    summary_path = tmp_path / "allocation_v1" / "summary" / DAY / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "day_utc": DAY,
                "producer": {"git_sha": GIT_SHA},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(allocation_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None)

    rc = allocation_module.main(
        [
            "--day_utc",
            DAY,
            "--producer_git_sha",
            GIT_SHA,
            "--producer_repo",
            "constellation",
            "--truth_root",
            str(tmp_path),
        ]
    )

    assert rc == 0
