from __future__ import annotations

from pathlib import Path
import sys

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_fresh_day_admission_v1 as fresh_day_cli


class _WriteRef:
    def __init__(self, path: Path, sha256: str) -> None:
        self.path = path
        self.sha256 = sha256


def test_run_fresh_day_admission_accepts_day_utc_alias(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    probe_path = truth_root / "reports" / "next_day_readiness_probe_v1" / "2026-04-24" / "next_day_readiness_probe.v1.json"
    probe_path.parent.mkdir(parents=True, exist_ok=True)
    probe_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        fresh_day_cli,
        "resolve_decision_truth_root_bridge_v1",
        lambda truth_root, repo_root=None, caller="": Path(truth_root).resolve(),
    )
    monkeypatch.setattr(
        fresh_day_cli,
        "resolve_single_paper_ib_account_from_sleeve_registry",
        lambda _repo_root: "DUO847203",
    )
    monkeypatch.setattr(
        fresh_day_cli,
        "resolve_next_day_readiness_probe_path",
        lambda *, truth_root, target_day_utc: probe_path,
    )
    monkeypatch.setattr(
        fresh_day_cli,
        "derive_fresh_day_admission_payload",
        lambda **kwargs: {
            "admission_status": "ADMIT",
            "blocking_items": [],
        },
    )
    monkeypatch.setattr(
        fresh_day_cli,
        "write_fresh_day_admission_v1",
        lambda **kwargs: _WriteRef(
            path=truth_root / "reports" / "fresh_day_admission_v1" / "2026-04-24" / "fresh_day_admission.v1.json",
            sha256="a" * 64,
        ),
    )

    exit_code = fresh_day_cli.main(
        [
            "--day_utc",
            "2026-04-24",
            "--truth_root",
            str(truth_root),
            "--environment",
            "PAPER",
        ]
    )

    assert exit_code == 0


def test_run_fresh_day_admission_rejects_conflicting_day_flags(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        fresh_day_cli,
        "resolve_decision_truth_root_bridge_v1",
        lambda truth_root, repo_root=None, caller="": Path(truth_root).resolve(),
    )

    with pytest.raises(SystemExit) as excinfo:
        fresh_day_cli.main(
            [
                "--target_day_utc",
                "2026-04-24",
                "--day_utc",
                "2026-04-25",
                "--truth_root",
                str(tmp_path),
                "--environment",
                "PAPER",
            ]
        )

    assert int(excinfo.value.code) == 2
