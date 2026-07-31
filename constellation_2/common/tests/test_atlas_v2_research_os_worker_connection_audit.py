import json

from constellation_2.common.atlas_v2_research_os.worker_connection_audit import build_worker_connection_audit, write_worker_connection_audit


def test_worker_connection_audit_discovers_and_classifies_components(tmp_path):
    report = build_worker_connection_audit(day="2026-06-05")
    assert report["component_count"] >= 6
    assert "AtlasV2ClaimIdeaGenerator" in report["safe_to_connect"]
    assert "AtlasV2CheapExperimentExecutor" in report["unsafe_to_connect"]
    paths = write_worker_connection_audit(tmp_path, day="2026-06-05")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_worker_connection_audit_v1"
