from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.exit_decision_engine_v1 import (  # noqa: E402
    derive_exit_decision_payload_v1,
    write_exit_decision_v1,
)
from constellation_2.common.position_normalization_v1 import derive_position_normalization_payload_v1  # noqa: E402


DAY = "2026-04-15"


def test_exit_decision_engine_prioritizes_stop_breach_over_partial_profit(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    normalization_payload = derive_position_normalization_payload_v1(
        day_utc=DAY,
        position_id="native-stop",
        origin="NATIVE",
        entry_price="100",
        initial_stop_price="95",
        initial_r_value="5",
    )
    payload = derive_exit_decision_payload_v1(
        day_utc=DAY,
        position_id="native-stop",
        normalization_payload=normalization_payload,
        side="LONG",
        quantity="100",
        mark_price="94",
        current_stop_price="95",
        structure_stop_price="110",
        volatility_stop_price="109",
    )
    ref = write_exit_decision_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["management_state"] == "EXIT_PENDING"
    assert written["decision_action"] == "EXIT_FULL"
    assert written["reason_codes"][0] == "STOP_BREACH"


def test_exit_decision_engine_keeps_imported_positions_defensive_and_unscored(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    normalization_payload = derive_position_normalization_payload_v1(
        day_utc=DAY,
        position_id="imported-managed",
        origin="IMPORTED",
        entry_price="100",
        initial_stop_price="95",
        initial_r_value="5",
        reference_entry_source="BROKER_POSITION_IMPORT",
        synthetic_stop_source="OPERATOR_SYNTHETIC_STOP",
    )
    payload = derive_exit_decision_payload_v1(
        day_utc=DAY,
        position_id="imported-managed",
        normalization_payload=normalization_payload,
        side="LONG",
        quantity="50",
        mark_price="112.5",
        current_stop_price="95",
        structure_stop_price="108",
        volatility_stop_price="107",
    )
    ref = write_exit_decision_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["origin"] == "IMPORTED"
    assert written["scoring_eligibility"] == "EXCLUDED_BY_DEFAULT"
    assert written["decision_action"] != "TAKE_PARTIAL"
    assert written["management_state"] in {"OPEN_TRAILING", "OPEN_RISK_REDUCED"}
