from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_exposure_net_day_v1 as exposure_module


DAY = "2026-04-22"
PREV_DAY = "2026-04-21"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")


def _write_paper_bootstrap_admission(canonical_truth_root: Path, *, day_utc: str) -> None:
    _write_json(
        canonical_truth_root / "target_day_admission_v1" / f"{day_utc}.json",
        {
            "day_utc": day_utc,
            "admission_status": "ADMIT",
            "mode": "PAPER_BOOTSTRAP",
        },
    )


def _write_zero_positions(sleeve_truth_root: Path, *, day_utc: str) -> None:
    _write_json(
        sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        {
            "day_utc": day_utc,
            "items": [],
        },
    )


def test_paper_bootstrap_missing_intents_writes_zero_exposure_baseline(tmp_path: Path) -> None:
    runtime_data_root = tmp_path / "runtime_data"
    canonical_truth_root = runtime_data_root / "truth"
    sleeve_truth_root = runtime_data_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    _write_paper_bootstrap_admission(canonical_truth_root, day_utc=DAY)
    _write_zero_positions(sleeve_truth_root, day_utc=DAY)

    rc = exposure_module.main(["--day_utc", DAY, "--truth_root", str(sleeve_truth_root)])
    assert rc == 0

    out_path = sleeve_truth_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["status"] == "OK"
    assert "PAPER_BOOTSTRAP_ZERO_EXPOSURE" in payload["reason_codes"]
    assert payload["portfolio"]["capital_at_risk_cents"] == 0


def test_missing_intents_dir_remains_fail_closed_outside_paper_bootstrap(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SystemExit, match="INTENTS_DAY_DIR_MISSING"):
        exposure_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])


def test_prior_exposure_chain_disables_bootstrap_zero_exposure(tmp_path: Path) -> None:
    runtime_data_root = tmp_path / "runtime_data"
    canonical_truth_root = runtime_data_root / "truth"
    sleeve_truth_root = runtime_data_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    _write_paper_bootstrap_admission(canonical_truth_root, day_utc=DAY)
    _write_zero_positions(sleeve_truth_root, day_utc=DAY)
    _write_json(
        sleeve_truth_root / "risk_v1" / "exposure_net_v1" / PREV_DAY / "exposure_net.v1.json",
        {"day_utc": PREV_DAY, "status": "OK"},
    )

    with pytest.raises(SystemExit, match="INTENTS_DAY_DIR_MISSING"):
        exposure_module.main(["--day_utc", DAY, "--truth_root", str(sleeve_truth_root)])


def test_existing_exposure_artifact_is_never_overwritten(tmp_path: Path) -> None:
    runtime_data_root = tmp_path / "runtime_data"
    canonical_truth_root = runtime_data_root / "truth"
    sleeve_truth_root = runtime_data_root / "truth_sleeves" / "PRIMARY" / "PAPER"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    _write_paper_bootstrap_admission(canonical_truth_root, day_utc=DAY)
    _write_zero_positions(sleeve_truth_root, day_utc=DAY)

    out_path = sleeve_truth_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json"
    original = {"sentinel": "original"}
    _write_json(out_path, original)

    with pytest.raises(SystemExit, match="REFUSE_OVERWRITE_EXISTING_FILE"):
        exposure_module.main(["--day_utc", DAY, "--truth_root", str(sleeve_truth_root)])

    assert json.loads(out_path.read_text(encoding="utf-8")) == original

