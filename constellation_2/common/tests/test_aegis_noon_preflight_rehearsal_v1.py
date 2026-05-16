from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import build_promoted_sleeve_library_v1  # noqa: E402
from ops.tools import build_aegis_operator_status_v1 as operator_status_cli  # noqa: E402
from ops.tools import run_aegis_noon_preflight_rehearsal_v1 as preflight  # noqa: E402


DAY = "2026-05-15"
WEEKEND = "2026-05-16"
NOW = "2026-05-15T16:00:00Z"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path


def _calendar(root: Path, *, day: str = DAY, trading: bool = True) -> None:
    _write(root / "market_calendar_v1" / "dataset_manifest.json", {"files": [{"year": int(day[:4]), "file": f"NYSE/{day[:4]}.jsonl"}]})
    path = root / "market_calendar_v1" / "NYSE" / f"{day[:4]}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset_version": "v1",
                "day_utc": day,
                "exchange": "NYSE",
                "ingested_utc": NOW,
                "is_trading_session": trading,
                "source_hash": "a" * 64,
                "source_name": "test",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def _market_context(root: Path, *, day: str = DAY, stale: str = "FRESH") -> Path:
    return _write(
        root / "reports" / "event_market_snapshot_v1" / day / "run" / "event_market_snapshot.v1.json",
        {
            "schema_id": "event_market_snapshot",
            "artifact_id": "event_market_snapshot_v1",
            "day_utc": day,
            "generated_at_utc": NOW,
            "regime_label": "NORMAL",
            "volatility_classification": "NORMAL_VOL",
            "breadth_classification": "NORMAL",
            "macro_event_risk_level": "LOW",
            "stale_data_status": stale,
            "canonical_eod_state_mutated": False,
        },
    )


def _library(root: Path, *, day: str = DAY) -> Path:
    payload = build_promoted_sleeve_library_v1(
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
                "promotion_status": "promoted",
                "approved_by_human": True,
                "approved_for_lite_implementation": True,
                "created_at": NOW,
                "updated_at": NOW,
                "archived": False,
            }
        ],
    )
    return _write(root / "reports" / "promoted_sleeve_library_v1" / day / "run" / "promoted_sleeve_library.v1.json", payload)


def _candidate_input(root: Path, *, stop_price: str = "95") -> Path:
    return _write(
        root / "inputs" / "candidates.json",
        {
            "day_utc": DAY,
            "candidates": [
                {
                    "candidate_id": "c1",
                    "sleeve_id": "sleeve-a",
                    "symbol": "SPY",
                    "side": "BUY",
                    "entry_reference_price": "100",
                    "stop_price": stop_price,
                    "stop_logic": "below invalidation",
                    "risk_per_trade": "5",
                    "sizing_tier": "SMOKE_TEST",
                }
            ],
        },
    )


def _base(root: Path) -> None:
    _calendar(root)
    _market_context(root)
    _library(root)


def test_pass_does_not_alert(tmp_path: Path) -> None:
    _base(tmp_path)

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="pass",
        generated_at_utc=NOW,
        refresh_control_packet=False,
    )

    assert status["result"] == "PASS"
    assert status["email_alert_required"] is False
    assert status["delivery_status"] == "NOT_SENT"
    assert not list(tmp_path.rglob("preflight_alert_ledger.v1.json"))


def test_fail_creates_alert_candidate_and_truthful_dry_run_ledger(tmp_path: Path) -> None:
    _calendar(tmp_path)
    _market_context(tmp_path)

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="missing-sleeve",
        generated_at_utc=NOW,
        refresh_control_packet=False,
    )

    assert status["result"] == "FAIL"
    assert status["email_alert_required"] is True
    assert status["email_alert_sent"] is False
    assert status["delivery_status"] == "DRY_RUN_MESSAGE_BODY_ONLY"
    assert status["operator_alert_status"] == "alert not live"
    assert "PROMOTED_SLEEVE_LIBRARY_MISSING" in status["blockers"]
    assert "[Aegis Preflight FAIL]" in status["alert_subject"]
    assert "no_trades_or_broker_automation_occurred: true" in status["alert_body"]

    ledger = json.loads(Path(status["alert_ledger_path"]).read_text(encoding="utf-8"))
    assert ledger["delivery_status"] == "DRY_RUN_MESSAGE_BODY_ONLY"
    assert ledger["email_transport_proven"] is False
    assert ledger["operator_status"] == "alert not live"


def test_weekend_skips_and_does_not_alert(tmp_path: Path) -> None:
    _calendar(tmp_path, day=WEEKEND, trading=False)

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=WEEKEND,
        truth_root=tmp_path,
        run_id="weekend",
        generated_at_utc="2026-05-16T16:00:00Z",
        refresh_control_packet=False,
    )

    assert status["result"] == "SKIPPED_NON_TRADING_DAY"
    assert status["email_alert_required"] is False
    assert not list(tmp_path.rglob("preflight_alert_ledger.v1.json"))


def test_missing_market_context_alerts(tmp_path: Path) -> None:
    _calendar(tmp_path)
    _library(tmp_path)

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="missing-market-context",
        generated_at_utc=NOW,
        refresh_control_packet=False,
    )

    assert status["result"] == "FAIL"
    assert "MARKET_CONTEXT_SNAPSHOT_MISSING" in status["blockers"]
    assert status["email_alert_required"] is True


def test_sizing_failure_alerts(tmp_path: Path) -> None:
    _base(tmp_path)
    candidate_path = _candidate_input(tmp_path, stop_price="")

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="sizing-fail",
        generated_at_utc=NOW,
        candidate_input_path=str(candidate_path),
        refresh_control_packet=False,
    )

    assert status["result"] == "FAIL"
    assert "SIZING_ENGINE_VALIDATION_FAILED" in status["blockers"]
    assert status["email_alert_required"] is True


def test_eod_contract_validation_failure_alerts_without_canonical_eod_mutation(tmp_path: Path) -> None:
    _base(tmp_path)
    candidate_path = _candidate_input(tmp_path)

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="eod-contract-fail",
        generated_at_utc=NOW,
        candidate_input_path=str(candidate_path),
        refresh_control_packet=False,
    )

    assert status["result"] == "FAIL"
    assert "EOD_PIPELINE_CONTRACT_VALIDATION_FAILED" in status["blockers"]
    assert not list(tmp_path.rglob("aegis_lite_eod_report.v1.json"))
    assert not list(tmp_path.rglob("eod_run_manifest.v1.json"))


def test_operator_status_surfaces_latest_preflight(tmp_path: Path) -> None:
    _calendar(tmp_path)
    _market_context(tmp_path)
    preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="operator-status",
        generated_at_utc=NOW,
        refresh_control_packet=False,
    )

    operator_status_cli.main(["--truth_root", str(tmp_path), "--day_utc", DAY])
    status = json.loads(next(tmp_path.rglob("aegis_operator_status.v1.json")).read_text(encoding="utf-8"))

    assert status["latest_preflight_result"] == "FAIL"
    assert "PROMOTED_SLEEVE_LIBRARY_MISSING" in status["latest_preflight_blockers"]
    assert status["latest_preflight_email_alert_sent"] is False
    assert status["latest_preflight_email_transport_proven"] is False
    assert status["latest_preflight_canonical_eod_at_risk"] is True


def test_no_broker_ib_automation_and_no_trade_advice_enabled(tmp_path: Path) -> None:
    _base(tmp_path)

    status = preflight.run_noon_preflight_rehearsal_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="safety",
        generated_at_utc=NOW,
        refresh_control_packet=False,
    )
    diagnostic = json.loads(Path(status["diagnostic_dry_run_path"]).read_text(encoding="utf-8"))

    assert status["broker_submit_required"] is False
    assert status["ib_automation_required"] is False
    assert status["trade_advice_enabled"] is False
    assert diagnostic["broker_submit_required"] is False
    assert diagnostic["ib_automation_required"] is False
    assert diagnostic["trade_advice_enabled"] is False
