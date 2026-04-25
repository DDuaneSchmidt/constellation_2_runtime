from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseL.ui_api.metadata_contract import validate_projection_envelope  # noqa: E402


def test_projection_envelope_rejects_authority_claims() -> None:
    payload = {
        "view_name": "positions",
        "surface_kind": "projection",
        "entity_scope": "positions",
        "truth_state": "canonical",
        "as_of_utc": "2026-04-17T00:00:00Z",
        "freshness_state": "fresh",
        "source_authority": ["positions_snapshot_v5"],
        "contract_id": "position_state_projection",
        "contract_version": "v1",
        "provenance_refs": [],
        "degradation_codes": [],
        "authoritative_writer": "ui_projection_writer",
    }
    result = validate_projection_envelope(payload)
    assert result["ok"] is False
    assert result["constitutional_non_authority_ok"] is False
    assert any("authoritative_writer" in error for error in result["errors"])
