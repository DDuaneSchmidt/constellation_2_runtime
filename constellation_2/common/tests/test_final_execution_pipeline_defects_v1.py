from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.submit_boundary_paper_v4 import (  # noqa: E402
    RC_PHASEC_OUT_DIR_UNSAFE,
    SubmitBoundaryV4Error,
    _require_phasec_out_dir_under_truth_root,
)
from ops.tools.run_c2_multi_sleeve_orchestrator_v1 import _build_enabled_sleeve_rollup_entry  # noqa: E402


def test_phasec_out_dir_accepts_canonical_truth_root_phasec_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    phasec_dir = truth_root / "phaseC_preflight_v1" / "2026-04-09" / "attempt_A0001" / "intent_hash"
    phasec_dir.mkdir(parents=True)

    _require_phasec_out_dir_under_truth_root(truth_root, phasec_dir)


def test_phasec_out_dir_rejects_external_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    outside = tmp_path / "outside" / "attempt_A0001"
    outside.mkdir(parents=True)

    with pytest.raises(SubmitBoundaryV4Error, match=RC_PHASEC_OUT_DIR_UNSAFE):
        _require_phasec_out_dir_under_truth_root(truth_root, outside)


def test_enabled_sleeve_rollup_entry_carries_required_status_field(tmp_path: Path) -> None:
    idx_path = tmp_path / "canonical_pointer_index.v1.jsonl"
    points_to_path = tmp_path / "orchestrator_run_verdict.v2.json"
    points_to_path.write_text("{}\n", encoding="utf-8")

    row = _build_enabled_sleeve_rollup_entry(
        sleeve_id="PRIMARY",
        mode="PAPER",
        ib_account="DUO847203",
        truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        rc=0,
        cmd_str="python3 fake.py",
        pointer_seq=1,
        idx_path=idx_path,
        points_to_path=points_to_path,
        verdict_obj={"status": "PASS", "reason_codes": [], "safety_breaches": []},
    )

    assert row["status"] == "PASS"
    assert row["verdict_status"] == "PASS"
