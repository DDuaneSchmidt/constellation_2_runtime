from __future__ import annotations

from pathlib import Path
import json
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.orchestrator import run_once
from constellation_2.common.atlas_v2_research_os.orchestrator_models import OrchestratorRunRecord, OrchestratorStatus
from constellation_2.common.atlas_v2_research_os.run_ledger import read_run_ledger, write_run_ledger


def test_write_run_ledger_appends_records(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    record = OrchestratorRunRecord(run_id="run-1", started_at="2026-06-05T00:00:00Z", completed_at="2026-06-05T00:00:01Z", status="COMPLETED", trigger_type="MANUAL", trigger_source={})
    path = write_run_ledger(record, root)
    write_run_ledger({**record.to_dict(), "run_id": "run-2"}, root)
    assert path.exists()
    assert [row["run_id"] for row in read_run_ledger(root)] == ["run-1", "run-2"]


def test_run_once_skips_when_local_lock_exists(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    lock = root / "orchestrator" / "orchestrator.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text(json.dumps({"lock_id": "existing"}), encoding="utf-8")
    record = run_once(root, truth_root=tmp_path / "truth", day="2026-06-05")
    assert record["status"] == OrchestratorStatus.SKIPPED_ALREADY_RUNNING.value
    assert record["skip_reason"] == "LOCK_HELD"
    assert read_run_ledger(root)[0]["status"] == "SKIPPED_ALREADY_RUNNING"
