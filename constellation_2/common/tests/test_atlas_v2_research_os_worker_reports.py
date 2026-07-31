import json
import subprocess
import sys

from constellation_2.common.atlas_v2_research_os.worker_reports import build_worker_interface_report, write_worker_interface_report


def test_worker_interface_report_summarizes_connected_and_placeholder_adapters(tmp_path):
    report = build_worker_interface_report(day="2026-06-04")
    assert report["worker_count"] == 8
    assert report["contract_audit_result"] == "PASS"
    assert report["adapter_counts_by_status"] == {"CONNECTED_RESEARCH_ONLY": 5, "NOT_CONNECTED": 3}
    assert report["safety"]["scheduler_implemented"] is False
    assert report["safety"]["candidate_factory_behavior_changed"] is False

    paths = write_worker_interface_report(day="2026-06-04", report_root=tmp_path)
    assert paths["json"].exists()
    assert paths["summary"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["worker_count"] == 8


def test_worker_cli_flags_run():
    audit = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--worker-audit"], check=True, capture_output=True, text=True)
    assert json.loads(audit.stdout)["contract_audit_result"] == "PASS"
    listed = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--list-workers"], check=True, capture_output=True, text=True)
    assert len(json.loads(listed.stdout)) == 8
    report = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--worker-report"], check=True, capture_output=True, text=True)
    paths = json.loads(report.stdout)
    assert paths["json"].endswith("worker_interface_report.json")
