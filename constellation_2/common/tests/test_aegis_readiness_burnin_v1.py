from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ops.tools.run_aegis_readiness_burnin_v1 as burnin


def test_burnin_fails_if_any_replay_mismatch(tmp_path: Path, monkeypatch) -> None:
    def fake_run_day(**kwargs):
        return {
            "day_utc": kwargs["day_utc"],
            "status": "FAIL",
            "runtime_evaluation_hash": "a" * 64,
            "replay_status": "RUNTIME_EVALUATION_HASH_MISMATCH",
            "market_readiness_status": "READY",
            "sleeve_readiness_status": "READY",
            "remaining_blockers": [],
            "optional_warnings": [],
            "authority_hash_consistency": "MISMATCH",
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }

    monkeypatch.setattr(burnin, "run_day_v1", fake_run_day)

    rc = burnin.main(["--truth-root", str(tmp_path), "--days", "2026-05-20", "--generated-at-utc", "2026-05-20T22:00:00Z"])

    assert rc == 2
    report = next((tmp_path / "reports" / "readiness_burnin_v1").glob("*/readiness_burnin.v1.json"))
    payload = json.loads(report.read_text())
    assert payload["status"] == "FAIL"
    assert payload["days"][0]["replay_status"] == "RUNTIME_EVALUATION_HASH_MISMATCH"
    assert payload["autonomous_execution_allowed"] is False
