from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


MODES = ("DRY_RUN", "PAPER_TRANSMIT", "LIVE")
RUN_STYLES = ("MANUAL", "AUTO")
REQUIRED_PHASES = ("PRE_MARKET", "INTENT", "MATERIALIZATION", "SUBMIT", "POST_SUBMIT", "CLOSURE", "PACKET")
NO_SILENT_DAY_OUTCOMES = (
    "SUCCESS_DRY_RUN",
    "SUCCESS_TRANSMITTED",
    "NO_INTENT_EXPECTED",
    "BLOCKED_WITH_REASON",
    "FAILED_WITH_OWNER",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _check_day(day_utc: str) -> str:
    text = str(day_utc or "").strip()
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise ValueError(f"INVALID_DAY_UTC:{day_utc!r}")
    return text


def operating_contract_output_path(*, truth_root: Path, day_utc: str) -> Path:
    day = _check_day(day_utc)
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_operating_contract_v1"
        / day
        / "aegis_operating_contract.v1.json"
    ).resolve()


def build_aegis_operating_contract_v1(
    *,
    day_utc: str,
    mode: str = "DRY_RUN",
    run_style: str = "MANUAL",
    target_sleeve: str = "PRIMARY/PAPER",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    day = _check_day(day_utc)
    normalized_mode = str(mode or "").strip().upper()
    normalized_run_style = str(run_style or "").strip().upper()
    if normalized_mode not in MODES:
        raise ValueError(f"INVALID_MODE:{mode!r}")
    if normalized_run_style not in RUN_STYLES:
        raise ValueError(f"INVALID_RUN_STYLE:{run_style!r}")

    required_authorities_by_phase = {
        "PRE_MARKET": [
            "portfolio_account_authority_v1",
            "market_data_authority_v1",
            "runtime_service_authority_v1",
            "execution_mode_authority_v1",
            "paper_trading_day_authority_v1",
        ],
        "INTENT": ["strategy_decision_authority_v1"],
        "MATERIALIZATION": ["market_data_authority_v1", "paper_trading_day_authority_v1"],
        "SUBMIT": ["paper_trading_day_authority_v1", "execution_mode_authority_v1", "runtime_service_authority_v1"],
        "POST_SUBMIT": ["execution_lifecycle_authority_v1", "trade_lineage_graph_v1", "execution_mode_authority_v1"],
        "CLOSURE": ["trading_day_closure_authority_v1"],
        "PACKET": [],
    }
    required_services_by_mode_run_style = {
        "auto_runner": normalized_run_style == "AUTO",
        "submit_creator": normalized_mode in {"PAPER_TRANSMIT", "LIVE"} or normalized_run_style == "AUTO",
        "packet_exporter": True,
        "evidence_ledger_writer": True,
        "dashboard_projection": True,
    }
    run_style_policy = {
        "MANUAL": {
            "description": "Aegis runs only from an operator command.",
            "market_session_windows_required": False,
        },
        "AUTO": {
            "description": "Aegis may run during valid market session windows while obeying all authorities.",
            "market_session_windows_required": True,
            "submit_requires_paper_trading_day_authority": True,
            "submit_requires_execution_mode_authority": True,
            "submit_requires_evidence_cycle": True,
            "default_execution_mode": "DRY_RUN",
            "paper_transmit_policy": "DISABLED_UNLESS_EXPLICIT_EXISTING_POLICY_ENABLES_PAPER_TRANSMIT",
        },
    }
    return {
        "schema_id": "aegis_operating_contract",
        "schema_version": "v1",
        "day_utc": day,
        "mode": normalized_mode,
        "run_style": normalized_run_style,
        "target_sleeve": target_sleeve,
        "required_phases": list(REQUIRED_PHASES),
        "required_authorities_by_phase": required_authorities_by_phase,
        "required_services_by_mode_run_style": required_services_by_mode_run_style,
        "run_style_policy": run_style_policy[normalized_run_style],
        "success_states": ["SUCCESS_DRY_RUN", "SUCCESS_TRANSMITTED", "NO_INTENT_EXPECTED"],
        "fail_states": ["BLOCKED_WITH_REASON", "FAILED_WITH_OWNER"],
        "allowed_diagnostic_states": ["NOT_IMPLEMENTED", "MISSING_DIAGNOSTIC", "STALE_DIAGNOSTIC", "WARN"],
        "no_silent_day_rule": {
            "description": "Every trading day must resolve to exactly one terminal outcome.",
            "allowed_outcomes": list(NO_SILENT_DAY_OUTCOMES),
            "mutually_exclusive": True,
            "exactly_one_required": True,
        },
        "produced_utc": produced_utc or _utc_now_iso(),
    }


def write_aegis_operating_contract_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    path = operating_contract_output_path(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path
