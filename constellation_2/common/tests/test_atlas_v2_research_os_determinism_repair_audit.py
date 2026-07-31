from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.determinism_repair_audit import write_determinism_repair_audit
from constellation_2.common.atlas_v2_research_os.evidence_lineage_graph import build_evidence_lineage_graph
from ops.aegis.operator_action_model_v1 import build_operator_action_model_v1, write_operator_action_model_v1
from ops.aegis.operator_action_model_self_check_v1 import build_operator_action_model_self_check_v1
from constellation_2.common.tests.test_aegis_operator_action_model_v1 import DAY, _seed_truth, _write_json


def test_determinism_audit_writes_byte_stable_json_and_csv(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_truth(truth_root)
    model = build_operator_action_model_v1(truth_root=truth_root, day_utc=DAY)
    write_operator_action_model_v1(truth_root=truth_root, day_utc=DAY, payload=model)

    paths = write_determinism_repair_audit(root=tmp_path / "reports" / "atlas_v2_research_os", truth_root=truth_root, day_utc=DAY)
    first = {key: path.read_bytes() for key, path in paths.items()}
    paths = write_determinism_repair_audit(root=tmp_path / "reports" / "atlas_v2_research_os", truth_root=truth_root, day_utc=DAY)
    second = {key: path.read_bytes() for key, path in paths.items()}

    assert first == second
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["confidence_impact"] == "NONE"
    assert payload["authority_boundary"]["candidate_promotion_authorized"] is False
    for key in ["findings", "repair_actions", "audit_replay_results"]:
        with paths[key].open(newline="", encoding="utf-8") as handle:
            assert list(csv.DictReader(handle))




def test_determinism_audit_defaults_to_latest_truth_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_truth(truth_root)
    older_day = "2026-05-31"
    older_model = build_operator_action_model_v1(truth_root=truth_root, day_utc=DAY)
    write_operator_action_model_v1(truth_root=truth_root, day_utc=older_day, payload={**older_model, "day_utc": older_day})
    latest_model = build_operator_action_model_v1(truth_root=truth_root, day_utc=DAY)
    write_operator_action_model_v1(truth_root=truth_root, day_utc=DAY, payload=latest_model)

    paths = write_determinism_repair_audit(root=tmp_path / "reports" / "atlas_v2_research_os", truth_root=truth_root)
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))

    assert payload["day_utc"] == DAY
    assert payload["summary"]["operator_action_model_nondeterminism_repaired"] is True

def test_operator_model_stable_under_shuffled_source_input_order(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    first = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    # Rewriting inputs in a different order must not affect sorted source artifacts/hashes or semantic output.
    control_path = tmp_path / "reports" / "aegis_chatgpt_control_packet_v1" / DAY / "aegis_chatgpt_control_packet.v1.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    shuffled = {key: control[key] for key in reversed(list(control.keys()))}
    _write_json(control_path, shuffled)
    second = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)

    assert first == second


def test_no_random_uuid_or_current_timestamp_in_deterministic_payloads(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_truth(truth_root)
    model = build_operator_action_model_v1(truth_root=truth_root, day_utc=DAY)
    write_operator_action_model_v1(truth_root=truth_root, day_utc=DAY, payload=model)
    paths = write_determinism_repair_audit(root=tmp_path / "reports" / "atlas_v2_research_os", truth_root=truth_root, day_utc=DAY)
    text = paths["json"].read_text(encoding="utf-8")

    payload = json.loads(text)
    assert "uuid4" not in text
    assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text)
    assert payload["day_utc"] == DAY


def test_provenance_only_operator_drift_is_isolated(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    write_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY, payload=model)
    packet_path = tmp_path / "reports" / "aegis_chatgpt_control_packet_v1" / DAY / "aegis_chatgpt_control_packet.v1.json"
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    packet["generated_at"] = "2026-06-01T10:09:00Z"
    _write_json(packet_path, packet)

    check = build_operator_action_model_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert check["ok"] is True
    assert check["provenance_only_drift"] is True
    assert check["semantic_stable_match"] is True


def test_build_094_lineage_report_remains_deterministic(tmp_path: Path) -> None:
    (tmp_path / "artifact_index.json").write_text(json.dumps({"artifacts": []}, sort_keys=True), encoding="utf-8")
    first = build_evidence_lineage_graph(tmp_path, created_at="2026-06-04T00:00:00Z")
    second = build_evidence_lineage_graph(tmp_path, created_at="2026-06-04T00:00:00Z")
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_no_research_logic_or_authority_changed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_truth(truth_root)
    model = build_operator_action_model_v1(truth_root=truth_root, day_utc=DAY)
    write_operator_action_model_v1(truth_root=truth_root, day_utc=DAY, payload=model)
    paths = write_determinism_repair_audit(root=tmp_path / "reports" / "atlas_v2_research_os", truth_root=truth_root, day_utc=DAY)
    report = json.loads(paths["json"].read_text(encoding="utf-8"))

    for action in report["repair_actions"]:
        assert action["research_logic_changed"] == "false"
        assert action["authority_changed"] == "false"
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
