from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.experience_loop_v1 import ledger_path, read_records  # noqa: E402
from ops.tools.build_aegis_experience_loop_v1 import ATLAS_V2_OBJECTS, build_certification, write_certification  # noqa: E402

DAY = "2026-06-04"
NOW = "2026-06-04T12:00:00Z"


def test_experience_loop_certification_references_atlas_v2_core_and_audits(tmp_path: Path) -> None:
    payload = build_certification(truth_root=tmp_path, day=DAY, generated_at=NOW)

    assert payload["record_type"] == "ExperienceEvent"
    assert payload["schema_version"] == "aegis_experience_loop.v1"
    assert payload["target_day"] == DAY
    assert payload["day_utc"] == DAY
    assert payload["certification_status"] == "CERTIFIED_READ_ONLY_AUDIT_ONLY"
    assert tuple(payload["atlas_v2_core_objects"]) == ATLAS_V2_OBJECTS
    assert payload["atlas_v2_append_only_ledgers"]["status"] == "CERTIFIED"
    assert payload["complete_linkage_audit"]["ok"] is True
    assert payload["complete_linkage_audit"]["complete_loop"] is True
    assert payload["forbidden_authority_audit"]["ok"] is True
    assert all(value is False for value in payload["forbidden_authority_audit"]["forbidden_authority"].values())
    assert [record["record_type"] for record in payload["certified_records"]] == list(ATLAS_V2_OBJECTS)


def test_experience_loop_certification_writes_graph_visible_report_contract(tmp_path: Path) -> None:
    path = write_certification(truth_root=tmp_path, day=DAY, generated_at=NOW)

    assert path.relative_to(tmp_path).as_posix() == "reports/aegis_experience_loop_v1/2026-06-04/experience_loop_certification.v1.json"
    assert ledger_path(tmp_path, DAY).relative_to(tmp_path).as_posix() == "events/aegis_experience_loop_v1/2026-06-04/experience_events.jsonl"
    assert path != ledger_path(tmp_path, DAY)

    text = path.read_text(encoding="utf-8")
    payload = json.loads(text)
    assert payload["artifact_id"] == "aegis_experience_loop_v1"
    assert payload["complete_linkage_audit"]["ok"] is True
    assert payload["scope"]["read_only"] is True

    # The report JSON is graph-facing. The event JSONL path is supporting runtime data
    # and may exist independently when append-only events have been recorded.
    loaded = read_records(truth_root=tmp_path, target_day=DAY)
    assert loaded == []
