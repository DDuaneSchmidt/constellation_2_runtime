from __future__ import annotations

from pathlib import Path
import sys

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_capital_authority_allocation_day_v1 as capalloc_module


def _write_json(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload + "\n", encoding="utf-8")


def test_load_target_day_bootstrap_admission_reads_only_paper_bootstrap_admit(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    day = "2026-04-20"
    path = truth_root / "target_day_admission_v1" / f"{day}.json"
    _write_json(path, '{"admission_status":"ADMIT","mode":"PAPER_BOOTSTRAP"}')

    row = capalloc_module._load_target_day_bootstrap_admission(truth_root, day)

    assert isinstance(row, dict)
    assert str(row.get("path") or "") == str(path.resolve())
    assert len(str(row.get("sha256") or "")) == 64


def test_previous_day_economic_state_complete_false_when_only_blocked_exists(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    current_day = "2026-04-21"
    prev_day = "2026-04-20"
    build_path = truth_root / "reports" / "economic_state_build_v1" / prev_day / "ctx" / "economic_state_build.v1.json"
    _write_json(build_path, '{"closure_status":"BLOCKED"}')

    assert capalloc_module._previous_day_economic_state_complete(truth_root, current_day) is False

