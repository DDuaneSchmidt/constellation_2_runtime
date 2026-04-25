from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.position_normalization_v1 import (  # noqa: E402
    derive_position_normalization_payload_v1,
    write_position_normalization_v1,
)


DAY = "2026-04-15"


def test_position_normalization_native_positions_keep_native_risk_basis(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    payload = derive_position_normalization_payload_v1(
        day_utc=DAY,
        position_id="native-1",
        origin="NATIVE",
        entry_price="100",
        initial_stop_price="95",
        initial_r_value="5",
    )
    ref = write_position_normalization_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["origin"] == "NATIVE"
    assert written["risk_basis"] == "R_NATIVE"
    assert written["normalization_status"] == "NORMALIZED"
    assert written["scoring_eligibility"] == "INCLUDED"


def test_position_normalization_imported_positions_block_without_explicit_synthetic_risk(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    payload = derive_position_normalization_payload_v1(
        day_utc=DAY,
        position_id="imported-1",
        origin="IMPORTED",
        entry_price="100",
        initial_stop_price="",
        initial_r_value="",
        reference_entry_source="BROKER_POSITION_IMPORT",
        synthetic_stop_source="",
    )
    ref = write_position_normalization_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["origin"] == "IMPORTED"
    assert written["risk_basis"] == "UNMANAGED"
    assert written["normalization_status"] == "BLOCKED"
    assert written["scoring_eligibility"] == "EXCLUDED_BY_DEFAULT"
    assert "IMPORTED_SYNTHETIC_STOP_MISSING" in written["reason_codes"]
