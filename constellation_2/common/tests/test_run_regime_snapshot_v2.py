from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_regime_snapshot_v2 as regime_module


def test_regime_snapshot_v2_accepts_truth_root_override_and_bootstraps(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    day_utc = "2026-04-09"

    rc = regime_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    out_path = truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["day_utc"] == day_utc
    assert payload["regime_label"] == "NORMAL"
    assert payload["blocking"] is False
    assert "DAY0_BOOTSTRAP_REGIME_INPUTS_MISSING_ALLOWED" in payload["reason_codes"]
