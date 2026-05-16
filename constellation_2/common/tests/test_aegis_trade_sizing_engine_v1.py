from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_manual_trade_packet_v1,
    build_promoted_sleeve_library_v1,
    validate_research_lab_artifact_v1,
)
from constellation_2.common.aegis_trade_sizing_engine_v1 import build_trade_sizing_guidance_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from ops.tools import record_manual_execution_receipt_v1 as receipt_cli  # noqa: E402
from ops.tools import record_trade_outcome_v1 as outcome_cli  # noqa: E402


NOW = "2026-05-15T21:00:00Z"
DAY = "2026-05-15"


def test_no_stop_blocks_sizing() -> None:
    sizing = build_trade_sizing_guidance_v1(candidate=_candidate(stop_price=""), promoted_sleeve_source_valid=True)

    assert sizing["suggested_quantity"] == 0
    assert "MISSING_STOP_PRICE" in sizing["sizing_blockers"]


def test_demo_and_dry_run_block_sizing() -> None:
    demo = build_trade_sizing_guidance_v1(candidate=_candidate(runtime_truth_classification="DEMO_ONLY"), promoted_sleeve_source_valid=True)
    dry = build_trade_sizing_guidance_v1(candidate=_candidate(runtime_truth_classification="DRY_RUN_ONLY"), promoted_sleeve_source_valid=True)

    assert "DEMO_ONLY_BLOCKS_SIZING" in demo["sizing_blockers"]
    assert "DRY_RUN_ONLY_BLOCKS_SIZING" in dry["sizing_blockers"]


def test_smoke_test_is_one_share() -> None:
    sizing = build_trade_sizing_guidance_v1(candidate=_candidate(sizing_tier="SMOKE_TEST"), promoted_sleeve_source_valid=True)

    assert sizing["suggested_quantity"] == 1
    assert sizing["max_loss_if_stopped"] == "5"
    assert "SMOKE_TEST_ONE_SHARE" in sizing["sizing_reason_codes"]


def test_risk_percentage_and_stop_distance_sizing_work() -> None:
    sizing = build_trade_sizing_guidance_v1(
        candidate=_candidate(sizing_tier="VERY_SMALL", portfolio_value_used="100000", entry_reference_price="100", stop_price="95"),
        promoted_sleeve_source_valid=True,
    )

    assert sizing["risk_pct_used"] == "0.1"
    assert sizing["allowed_dollar_risk"] == "100"
    assert sizing["risk_per_share"] == "5"
    assert sizing["suggested_quantity"] == 20


def test_overlap_and_regime_reductions_apply() -> None:
    sizing = build_trade_sizing_guidance_v1(
        candidate=_candidate(sizing_tier="NORMAL", portfolio_value_used="100000", overlap_adjustment="0.5", regime_adjustment="0.5"),
        promoted_sleeve_source_valid=True,
    )

    assert sizing["allowed_dollar_risk"] == "62.5"
    assert sizing["suggested_quantity"] == 12


def test_market_context_reduces_risk_without_increasing_size() -> None:
    sizing = build_trade_sizing_guidance_v1(
        candidate=_candidate(
            sizing_tier="NORMAL",
            portfolio_value_used="100000",
            market_context={
                "regime_label": "HIGH_VOLATILITY",
                "volatility_classification": "HIGH_VOL",
                "breadth_classification": "BREADTH_COLLAPSE",
                "macro_event_risk_level": "HIGH",
                "stale_data_status": "FRESH",
            },
        ),
        promoted_sleeve_source_valid=True,
    )

    assert sizing["market_context_adjustment"] == "0.5"
    assert sizing["allowed_dollar_risk"] == "125"
    assert sizing["suggested_quantity"] == 25
    assert "MARKET_CONTEXT_HIGH_VOL_RISK_REDUCTION" in sizing["sizing_reason_codes"]
    assert "MARKET_CONTEXT_BREADTH_COLLAPSE_RISK_REDUCTION" in sizing["sizing_reason_codes"]


def test_concentration_cap_blocks() -> None:
    sizing = build_trade_sizing_guidance_v1(
        candidate=_candidate(sizing_tier="NORMAL", concentration_adjustment="0"),
        promoted_sleeve_source_valid=True,
    )

    assert sizing["suggested_quantity"] == 0
    assert "CONCENTRATION_CAP_BLOCKED" in sizing["sizing_blockers"]


def test_stale_and_missing_risk_rules_block_sizing() -> None:
    stale = build_trade_sizing_guidance_v1(candidate=_candidate(stale_packet=True), promoted_sleeve_source_valid=True)
    missing_risk = build_trade_sizing_guidance_v1(candidate=_candidate(risk_per_trade=""), promoted_sleeve_source_valid=True)

    assert "STALE_PACKET_BLOCKS_SIZING" in stale["sizing_blockers"]
    assert "MISSING_RISK_RULES" in missing_risk["sizing_blockers"]


def test_manual_trade_packet_includes_sizing_fields_and_blocks_unpromoted() -> None:
    packet = build_manual_trade_packet_v1(
        packet_id="packet-1",
        run_id="run-1",
        date=DAY,
        generated_at_utc=NOW,
        regime_state="PANIC",
        promoted_sleeve_library=_library(),
        trade_candidates=[_candidate(sizing_tier="VERY_SMALL")],
    )
    validate_research_lab_artifact_v1(packet)
    trade = packet["trade_candidates"][0]

    assert trade["suggested_quantity"] == 20
    assert trade["risk_per_share"] == "5"
    assert trade["ai_selected_size"] is False
    assert trade["operator_can_override"] is True
    assert trade["sizing_blockers"] == []
    assert trade["actionable"] is True

    unpromoted = build_manual_trade_packet_v1(
        packet_id="packet-2",
        run_id="run-1",
        date=DAY,
        generated_at_utc=NOW,
        regime_state="PANIC",
        trade_candidates=[_candidate()],
    )
    assert unpromoted["trade_candidates"][0]["actionable"] is False
    assert "UNPROMOTED_SLEEVE_BLOCKS_SIZING" in unpromoted["trade_candidates"][0]["sizing_blockers"]


def test_receipt_and_outcome_capture_quantity_override(tmp_path: Path) -> None:
    packet = build_manual_trade_packet_v1(
        packet_id="packet-1",
        run_id="run-1",
        date=DAY,
        generated_at_utc=NOW,
        regime_state="PANIC",
        promoted_sleeve_library=_library(),
        trade_candidates=[_candidate(sizing_tier="VERY_SMALL")],
    )
    _write(tmp_path / "reports" / "manual_trade_packet_v1" / DAY / "run-1" / "manual_trade_packet.v1.json", packet)

    receipt_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--source_packet_id",
            "trade-1",
            "--symbol",
            "SPY",
            "--side",
            "BUY",
            "--quantity",
            "10",
            "--fill_price",
            "100.50",
            "--fill_timestamp_utc",
            NOW,
            "--stop_entered",
            "yes",
            "--stop_price",
            "95.00",
            "--operator_override_reason",
            "supervised smoke cap",
        ]
    )
    receipt = json.loads(next(tmp_path.rglob("manual_execution_receipt.v1.json")).read_text(encoding="utf-8"))
    assert receipt["suggested_quantity"] == 20
    assert receipt["actual_quantity"] == 10
    assert receipt["quantity_override"] is True
    assert receipt["sizing_quality"] == "OVERRIDDEN"

    outcome_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--trade_id",
            "trade-1",
            "--exit_price",
            "102.00",
            "--exit_timestamp_utc",
            NOW,
            "--outcome_status",
            "CLOSED",
        ]
    )
    ledger = json.loads(next(tmp_path.rglob("outcome_ledger.v1.json")).read_text(encoding="utf-8"))
    outcome = ledger["outcomes"][0]
    assert outcome["suggested_quantity"] == 20
    assert outcome["actual_quantity"] == 10
    assert outcome["sizing_quality"] == "OVERRIDDEN"


def test_no_broker_or_ib_automation_added() -> None:
    text = (REPO_ROOT / "constellation_2/common/aegis_trade_sizing_engine_v1.py").read_text(encoding="utf-8")
    for marker in ("ib_insync", "IBGateway", "placeOrder", "transmit"):
        assert marker not in text


def _candidate(**overrides: object) -> dict[str, object]:
    base = {
        "recommended_trade_id": "trade-1",
        "sleeve_id": "sleeve-a",
        "source_hypothesis_id": "hyp-a",
        "symbol": "SPY",
        "side": "BUY",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": "100",
        "order_type_suggestion": "LIMIT",
        "stop_price": "95",
        "stop_logic": "stop below invalidation",
        "risk_per_trade": "5",
        "edge_family": "PANIC_EXHAUSTION",
        "regime_state": "PANIC",
        "confidence": "MEDIUM",
        "inclusion_reason": "test",
        "exclusion_reason": "",
        "governance_notes": "manual only",
        "edge_overlap_result": "LOW_OVERLAP",
        "runtime_truth_classification": "REAL_RUNTIME",
    }
    base.update(overrides)
    return base


def _library() -> dict[str, object]:
    return build_promoted_sleeve_library_v1(
        generated_at_utc=NOW,
        sleeves=[
            {
                "sleeve_id": "sleeve-a",
                "source_hypothesis_id": "hyp-a",
                "edge_family": "PANIC_EXHAUSTION",
                "behavioral_thesis": "panic exhaustion",
                "regime_fit": ["PANIC"],
                "instrument_universe": ["SPY"],
                "entry_logic": "manual",
                "exit_logic": "manual",
                "stop_logic": "stop",
                "sizing_logic": "risk based",
                "invalidation_logic": "invalid",
                "known_failure_modes": ["continued stress"],
                "overlap_tags": ["equity_beta"],
                "promotion_evidence_path": "/tmp/evidence",
                "production_status": "paper_only",
                "created_at": NOW,
                "updated_at": NOW,
            }
        ],
    )


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
