from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.sleeve_evaluation_v1 import build_sleeve_evaluation_v1, write_sleeve_evaluation_v1  # noqa: E402

DAY = "2026-05-29"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path) -> None:
    expected = ["ACTIVE_NO_SIGNAL", "DISABLED", "MISSING_RUN", "REJECTED_ONLY", "DATA_BLOCKED", "WINNER"]
    _write(
        root / "reports" / "aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json",
        {
            "expected_sleeve_ids": expected,
            "enabled_sleeve_ids": ["ACTIVE_NO_SIGNAL", "MISSING_RUN", "REJECTED_ONLY", "DATA_BLOCKED", "WINNER"],
            "sleeves": [
                {"sleeve_id": "ACTIVE_NO_SIGNAL", "sleeve_name": "Active No Signal", "enabled": True, "expected_today": True, "can_run_candidate_generation": True, "data_status": "OK", "evaluation_status": "NO_INTENT", "evaluation_artifact_path": "artifact"},
                {"sleeve_id": "DISABLED", "enabled": False, "expected_today": False, "data_status": "OK", "evaluation_status": "DISABLED"},
                {"sleeve_id": "MISSING_RUN", "enabled": True, "expected_today": True, "can_run_candidate_generation": True, "data_status": "OK", "evaluation_status": ""},
                {"sleeve_id": "REJECTED_ONLY", "enabled": True, "expected_today": True, "can_run_candidate_generation": True, "data_status": "OK", "evaluation_status": "NO_INTENT", "evaluation_artifact_path": "artifact", "raw_signal_rejections": [{"sleeve_id": "REJECTED_ONLY", "symbol": "AAA", "reason": "VOLATILITY_FILTER_NOT_MET", "score": 0.91, "threshold": 1.0}]},
                {"sleeve_id": "DATA_BLOCKED", "enabled": True, "expected_today": True, "can_run_candidate_generation": False, "data_status": "MISSING", "canonical_blocker": "MISSING_REQUIRED_INPUTS", "missing_inputs": ["market.price.XYZ"], "evaluation_artifact_path": "artifact"},
                {"sleeve_id": "WINNER", "enabled": True, "expected_today": True, "can_run_candidate_generation": True, "data_status": "OK", "evaluation_status": "INTENT_CREATED", "evaluation_artifact_path": "artifact"},
            ],
        },
    )
    _write(
        root / "reports" / "sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {
            "outcomes": [
                {"sleeve_id": "ACTIVE_NO_SIGNAL", "enabled": True, "status": "NO_INTENT", "output_intents": 0, "rejected_intents": 0},
                {"sleeve_id": "DISABLED", "enabled": False, "status": "DISABLED", "output_intents": 0, "rejected_intents": 0},
                {"sleeve_id": "REJECTED_ONLY", "enabled": True, "status": "NO_INTENT", "output_intents": 0, "rejected_intents": 1, "rejected_intents_detail": [{"sleeve_id": "REJECTED_ONLY", "symbol": "AAA", "reason": "VOLATILITY_FILTER_NOT_MET", "score": 0.91, "threshold": 1.0}]},
                {"sleeve_id": "DATA_BLOCKED", "enabled": True, "status": "BLOCKED", "output_intents": 0, "rejected_intents": 0, "canonical_blocker": "MISSING_REQUIRED_INPUTS"},
                {"sleeve_id": "WINNER", "enabled": True, "status": "INTENT_CREATED", "output_intents": 1, "rejected_intents": 0},
            ]
        },
    )
    _write(
        root / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {"candidate_contracts": [{"candidate_id": "cand-1", "sleeve_id": "WINNER"}]},
    )


def _rows_by_id(payload: dict) -> dict[str, dict]:
    return {row["sleeve_id"]: row for row in payload["sleeves"]}


def test_enabled_sleeve_with_no_output_valid_run_is_classified_explicitly(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = _rows_by_id(build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY))

    assert rows["ACTIVE_NO_SIGNAL"]["classification"] == "SILENT_MARKET_CONDITION_NOT_MET"
    assert rows["ACTIVE_NO_SIGNAL"]["explanation"]
    assert rows["ACTIVE_NO_SIGNAL"]["repair_action"]


def test_disabled_sleeve_is_not_treated_as_failure(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = _rows_by_id(build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY))

    assert rows["DISABLED"]["classification"] == "SILENT_CONFIG_DISABLED"
    assert rows["DISABLED"]["should_modify_logic"] is False


def test_missing_run_artifact_is_data_blocked_without_runtime_evidence(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = _rows_by_id(build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY))

    assert rows["MISSING_RUN"]["classification"] == "SILENT_DATA_BLOCKED"
    assert rows["MISSING_RUN"]["run_artifact_present"] is False
    assert "cannot prove a runtime failure" in rows["MISSING_RUN"]["explanation"]


def test_rejected_intents_without_outputs_show_nearest_miss_summary(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = _rows_by_id(build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY))

    row = rows["REJECTED_ONLY"]
    assert row["classification"] == "SILENT_THRESHOLD_TOO_STRICT"
    assert row["nearest_miss_count"] >= 1
    assert "VOLATILITY_FILTER_NOT_MET" in row["threshold_summary"]
    assert row["top_nearest_misses"]


def test_report_contains_explanation_repair_action_and_no_blank_classifications(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["expected_sleeves"] == 6
    assert payload["summary"]["output_producing_sleeves"] == 1
    for row in payload["sleeves"]:
        assert row["classification"]
        if not row["produced_candidates"] and not row["produced_output_intents"]:
            assert row["explanation"]
            assert row["repair_action"]


def test_current_rollup_clears_stale_diagnostic_runtime_blocker(tmp_path: Path) -> None:
    _seed(tmp_path)
    diagnostics_path = tmp_path / "reports" / "aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json"
    diagnostics = json.loads(diagnostics_path.read_text(encoding="utf-8"))
    diagnostics["expected_sleeve_ids"].append("STALE_DIAG")
    diagnostics["sleeves"].append(
        {
            "sleeve_id": "STALE_DIAG",
            "enabled": True,
            "expected_today": True,
            "can_run_candidate_generation": True,
            "data_status": "OK",
            "evaluation_status": "BLOCKED",
            "canonical_blocker": "PRODUCER_NONZERO_RC",
            "evaluation_artifact_path": "old-artifact",
        }
    )
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rollup_path = tmp_path / "reports" / "sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json"
    rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
    rollup["outcomes"].append(
        {
            "sleeve_id": "STALE_DIAG",
            "enabled": True,
            "status": "NO_INTENT",
            "canonical_blocker": "",
            "reason_codes": ["NO_INTENT_DECLARED"],
            "output_intents": 0,
            "rejected_intents": 0,
        }
    )
    rollup_path.write_text(json.dumps(rollup, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    rows = _rows_by_id(build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY))

    assert rows["STALE_DIAG"]["classification"] == "SILENT_MARKET_CONDITION_NOT_MET"
    assert rows["STALE_DIAG"]["blocker_reason"] == "NO_INTENT_DECLARED"


def test_data_blocked_row_surfaces_exact_missing_inputs(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = _rows_by_id(build_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY))

    row = rows["DATA_BLOCKED"]
    assert row["classification"] == "SILENT_DATA_BLOCKED"
    assert row["missing_input_artifacts"] == ["market.price.XYZ"]
    assert row["missing_symbol_or_field_data"] == ["market.price.XYZ"]
    assert row["block_expected_or_defective"] == "DEFECTIVE_BINDING_OR_MISSING_MARKET_DATA"


def test_write_sleeve_evaluation_uses_expected_artifact_path(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = write_sleeve_evaluation_v1(truth_root=tmp_path, day_utc=DAY)

    assert path == tmp_path / "reports" / "aegis_sleeve_evaluation_v1" / DAY / "sleeve_evaluation.v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["candidate_creation_allowed"] is False
    assert payload["sleeve_logic_modification_allowed"] is False
