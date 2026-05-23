from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.repair_aegis_candidate_readiness_v1 import build_candidate_readiness_repair_v1


DAY = "2026-05-18"
SLEEVES = [
    "C2_CROSS_ASSET_TREND_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _fake_run(cmd: list[str]) -> dict:
    return {"command": " ".join(cmd), "exit_code": 0, "stdout": "", "stderr": ""}


def _fake_lifecycle(**_kwargs) -> dict[str, str]:
    return {"lifecycle": "candidate_lifecycle.v1.json", "summary": "candidate_lifecycle.summary.txt"}


def _fake_diagnostics(*, truth_root: Path, day_utc: str, payload: dict) -> dict[str, str]:
    path = truth_root / "reports/aegis_candidate_generation_diagnostics_v1" / day_utc / "candidate_generation_diagnostics.v1.json"
    _write_json(path, payload)
    return {"json": str(path), "summary": str(path.with_suffix(".summary.txt")), "matrix": str(path.with_suffix(".matrix.csv"))}


def _diagnostics_payload(**overrides: object) -> dict:
    payload = {
        "candidate_generation_status": "RAN",
        "operator_interpretation": "NORMAL_NO_SIGNAL",
        "total_sleeves_expected": 7,
        "total_sleeves_run": 7,
        "total_sleeves_blocked": 0,
        "total_candidates_generated": 0,
        "sleeves": [{"sleeve_id": sleeve, "run_status": "RAN", "candidate_count": 0} for sleeve in SLEEVES],
    }
    payload.update(overrides)
    return payload


def test_repair_attempts_all_seven_sleeves_and_does_not_fabricate_candidates(tmp_path: Path) -> None:
    with (
        patch("ops.tools.repair_aegis_candidate_readiness_v1._run", side_effect=_fake_run),
        patch("ops.tools.repair_aegis_candidate_readiness_v1.write_candidate_lifecycle_reports_v1", side_effect=_fake_lifecycle),
        patch("ops.tools.repair_aegis_candidate_readiness_v1.build_candidate_generation_diagnostics_v1", return_value=_diagnostics_payload()),
        patch("ops.tools.repair_aegis_candidate_readiness_v1.write_candidate_generation_diagnostics_v1", side_effect=_fake_diagnostics),
    ):
        payload = build_candidate_readiness_repair_v1(truth_root=tmp_path, day_utc=DAY)

    assert sorted(payload["expected_sleeve_ids"]) == sorted(SLEEVES)
    assert sorted(payload["attempted_sleeve_ids"]) == sorted(SLEEVES)
    command_text = [row["command"] for row in payload["commands"]]
    assert "ops/tools/build_aegis_symbol_map_v1.py" in command_text[0]
    assert any("ops/tools/build_aegis_sleeve_input_contracts_v1.py" in cmd for cmd in command_text)
    assert command_text.index(next(cmd for cmd in command_text if "ops/tools/refresh_aegis_market_data_v1.py" in cmd)) < command_text.index(next(cmd for cmd in command_text if "ops/tools/build_aegis_market_data_inputs_v1.py" in cmd))
    assert command_text.index(next(cmd for cmd in command_text if "ops/tools/build_aegis_market_data_inputs_v1.py" in cmd)) < command_text.index(next(cmd for cmd in command_text if "ops/tools/build_aegis_sleeve_readiness_v1.py" in cmd))
    sleeve_eval_idx = command_text.index(next(cmd for cmd in command_text if "ops/tools/run_sleeve_evaluation_kernel_v1.py" in cmd))
    portfolio_gate_idx = command_text.index(next(cmd for cmd in command_text if "ops/tools/run_portfolio_activation_gate_v1.py" in cmd))
    portfolio_scoring_idx = command_text.index(next(cmd for cmd in command_text if "ops/tools/run_portfolio_scoring_v1.py" in cmd))
    arbitration_idx = command_text.index(next(cmd for cmd in command_text if "ops/tools/run_intent_arbitration_v1.py" in cmd))
    promotion_idx = command_text.index(next(cmd for cmd in command_text if "ops/tools/promote_aegis_selected_intent_v1.py" in cmd))
    ranking_idx = command_text.index(next(cmd for cmd in command_text if "ops/tools/run_aegis_candidate_ranking_v1.py" in cmd))
    assert sleeve_eval_idx < portfolio_gate_idx < portfolio_scoring_idx < arbitration_idx < promotion_idx < ranking_idx
    assert payload["total_candidates_generated"] == 0
    assert payload["safety"]["fabricated_candidates"] is False
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False


def test_repair_reports_data_required_when_external_market_context_missing(tmp_path: Path) -> None:
    with (
        patch("ops.tools.repair_aegis_candidate_readiness_v1._run", side_effect=_fake_run),
        patch("ops.tools.repair_aegis_candidate_readiness_v1.write_candidate_lifecycle_reports_v1", side_effect=_fake_lifecycle),
        patch("ops.tools.repair_aegis_candidate_readiness_v1.build_candidate_generation_diagnostics_v1", return_value=_diagnostics_payload(operator_interpretation="DATA_BLOCKED")),
        patch("ops.tools.repair_aegis_candidate_readiness_v1.write_candidate_generation_diagnostics_v1", side_effect=_fake_diagnostics),
        patch.dict("os.environ", {"AEGIS_EVENT_MARKET_DATA_JSON": ""}, clear=False),
    ):
        payload = build_candidate_readiness_repair_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "DATA_REQUIRED"
    assert payload["data_required"][0]["artifact_id"] == "event_market_snapshot_v1"
    assert payload["safety"]["missing_data_bypassed"] is False
