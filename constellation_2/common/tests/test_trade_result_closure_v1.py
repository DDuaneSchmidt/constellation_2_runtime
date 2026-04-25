from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.position_normalization_v1 import derive_position_normalization_payload_v1  # noqa: E402
from constellation_2.common.trade_result_closure_v1 import (  # noqa: E402
    derive_trade_result_payload_v1,
    write_trade_result_v1,
)


DAY = "2026-04-15"


def test_trade_result_closure_preserves_native_scoring_eligibility(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    normalization_payload = derive_position_normalization_payload_v1(
        day_utc=DAY,
        position_id="native-close",
        origin="NATIVE",
        entry_price="100",
        initial_stop_price="95",
        initial_r_value="5",
    )
    payload = derive_trade_result_payload_v1(
        day_utc=DAY,
        position_id="native-close",
        normalization_payload=normalization_payload,
        exit_decision_payload={
            "decision_id": "decision-native",
            "side": "LONG",
            "reason_codes": ["STOP_BREACH"],
        },
        exit_price="110",
        entry_time_utc="2026-04-15T13:30:00Z",
        exit_time_utc="2026-04-15T14:30:00Z",
        mfe_r="2",
        mae_r="-0.5",
    )
    ref = write_trade_result_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["realized_r"] == "2"
    assert written["analytics_eligibility"] == "INCLUDED"


def test_trade_result_closure_quarantines_imported_synthetic_positions(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    normalization_payload = derive_position_normalization_payload_v1(
        day_utc=DAY,
        position_id="imported-close",
        origin="IMPORTED",
        entry_price="100",
        initial_stop_price="95",
        initial_r_value="5",
        reference_entry_source="BROKER_POSITION_IMPORT",
        synthetic_stop_source="OPERATOR_SYNTHETIC_STOP",
    )
    payload = derive_trade_result_payload_v1(
        day_utc=DAY,
        position_id="imported-close",
        normalization_payload=normalization_payload,
        exit_decision_payload={
            "decision_id": "decision-imported",
            "side": "LONG",
            "reason_codes": ["TIME_STOP"],
        },
        exit_price="105",
    )
    ref = write_trade_result_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["origin"] == "IMPORTED"
    assert written["risk_basis"] == "R_SYNTHETIC"
    assert written["realized_r"] == "1"
    assert written["analytics_eligibility"] == "EXCLUDED_BY_DEFAULT"
    assert written["synthetic_risk_labeled"] is True
