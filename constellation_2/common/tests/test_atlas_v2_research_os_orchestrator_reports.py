from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.orchestrator import audit_only
from constellation_2.common.atlas_v2_research_os.orchestrator_reports import write_orchestrator_report


def test_write_orchestrator_report_creates_run_and_latest_files(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    record = {"run_id": "run-1", "started_at": "2026-06-05T00:00:00Z", "status": "DRY_RUN_COMPLETED", "trigger_type": "MANUAL", "selected_backlog_items": [], "workers_invoked": [], "artifacts_created": [], "safety_gate_results": [], "lineage_validation_result": {}, "skip_reason": None}
    paths = write_orchestrator_report(record, root)
    assert paths["json"].name == "run_run-1.json"
    assert paths["latest_json"].exists()
    assert "Forbidden uses" in paths["summary"].read_text(encoding="utf-8")


def test_audit_only_writes_report_without_selecting_work(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    record = audit_only(root, truth_root=tmp_path / "truth", day="2026-06-05")
    assert record["status"] == "AUDIT_ONLY_COMPLETED"
    assert (root / "orchestrator" / "latest.json").exists()


def test_cli_orchestrator_dry_run(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    result = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--orchestrator-dry-run"], check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)
    assert payload["status"] == "DRY_RUN_COMPLETED"
    assert (root / "orchestrator" / "latest_summary.md").exists()
