from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


SCHEMA_ID = "C2_TRADING_DAY_READINESS_AUTHORITY_V1"
SCHEMA_VERSION = 1
TIMEZONE = "America/New_York"
EXCHANGE = "NYSE"

PREOPEN_MODES = {"PREOPEN_BUILD", "PREOPEN_ADMISSION"}


def _utc_now() -> datetime:
    override = str(os.environ.get("C2_TRADING_DAY_READINESS_NOW_UTC") or "").strip()
    if override:
        return _parse_utc(override)
    return datetime.now(UTC).replace(microsecond=0)


def _parse_utc(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        return _utc_now()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def trading_day_readiness_authority_output_path(*, truth_root: Path, target_day: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "trading_day_readiness_authority_v1"
        / target_day
        / "trading_day_readiness_authority.v1.json"
    )


def _artifact_paths(*, truth_root: Path, execution_root: Path, target_day: str) -> dict[str, str]:
    prior_day = _prior_day(target_day)
    return {
        "trading_day_readiness_authority_v1": str(trading_day_readiness_authority_output_path(truth_root=truth_root, target_day=target_day).resolve()),
        "same_day_broker_event_log": str((execution_root / "execution_evidence_v1" / "broker_events" / target_day / "broker_event_log.v1.jsonl").resolve()),
        "carry_forward_broker_event_log": str((execution_root / "execution_evidence_v1" / "broker_events" / prior_day / "broker_event_log.v1.jsonl").resolve()),
        "same_day_options_snapshot_root": str((execution_root / "options_chain_snapshot_v1" / target_day).resolve()),
        "same_day_accounting_nav_v2": str((truth_root / "accounting_v2" / "nav" / target_day / "nav.v2.json").resolve()),
        "prior_accounting_nav_v2": str((truth_root / "accounting_v2" / "nav" / prior_day / "nav.v2.json").resolve()),
        "submit_boundary_status_v1": str((truth_root / "reports" / "submit_boundary_status_v1" / target_day / "submit_boundary_status.v1.json").resolve()),
    }


def _prior_day(day: str) -> str:
    return (datetime.fromisoformat(day).date() - timedelta(days=1)).isoformat()


def _session_for_same_day(local_dt: datetime) -> tuple[str, str]:
    if local_dt.weekday() >= 5:
        return "CLOSED", "HISTORICAL_REPLAY"
    local_t = local_dt.time()
    if local_t < time(4, 0):
        return "CLOSED", "PREOPEN_BUILD"
    if local_t < time(9, 30):
        return "PRE_MARKET", "PREOPEN_ADMISSION"
    if local_t < time(16, 0):
        return "REGULAR_OPEN", "INTRADAY_SUBMIT_READY"
    return "AFTER_HOURS", "AFTER_HOURS_CLOSURE"


def _session_for_target_day(*, target_day: str, now: datetime, replay_mode: bool = False) -> tuple[str, str, str]:
    local = now.astimezone(ZoneInfo(TIMEZONE))
    current_day = local.date().isoformat()
    target = str(target_day).strip()
    if replay_mode:
        return current_day, "PAST_TARGET_DAY" if target < current_day else "CLOSED", "HISTORICAL_REPLAY"
    if target > current_day:
        return current_day, "FUTURE_TARGET_DAY", "PREOPEN_BUILD"
    if target < current_day:
        return current_day, "PAST_TARGET_DAY", "HISTORICAL_REPLAY"
    session_state, readiness_mode = _session_for_same_day(local)
    return current_day, session_state, readiness_mode


def _policy_for_mode(readiness_mode: str) -> tuple[dict[str, Any], list[str], list[str], bool, bool, bool, bool, str, str, str]:
    if readiness_mode in PREOPEN_MODES:
        evidence_policy = {
            "policy_id": "PREOPEN_CARRY_FORWARD_V1",
            "broker_event_log": "T_MINUS_1_ALLOWED",
            "broker_account_truth": "T_MINUS_1_ALLOWED",
            "options_snapshot": "FORBIDDEN_FUTURE_DATA",
            "submit": "FORBIDDEN_OUTSIDE_INTRADAY_MODE",
        }
        return (
            evidence_policy,
            ["broker_event_log", "broker_account_truth", "capital_supply", "accounting_nav_v2"],
            ["future_broker_event_log", "future_options_snapshot", "fabricated_live_account_truth"],
            False,
            False,
            False,
            False,
            "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
            readiness_mode,
            "Continue pre-open build/admission using governed prior-day carry-forward where allowed; do not fabricate future broker or market data.",
        )
    if readiness_mode == "INTRADAY_SUBMIT_READY":
        evidence_policy = {
            "policy_id": "INTRADAY_LIVE_EVIDENCE_V1",
            "broker_event_log": "SAME_DAY_REQUIRED",
            "broker_account_truth": "LIVE_OR_SAME_DAY_REQUIRED",
            "options_snapshot": "SELECTED_SYMBOL_REQUIRED_WHEN_OPTIONS_INTENT",
            "submit": "MAY_BE_ALLOWED_ONLY_IF_DOWNSTREAM_GATES_PASS",
        }
        return (
            evidence_policy,
            [],
            ["prior_day_broker_event_log_for_submit", "future_broker_event_log", "future_options_snapshot"],
            True,
            True,
            True,
            True,
            "",
            "",
            "Evaluate same-day broker, account, market data, safety, capital, and submit gates.",
        )
    if readiness_mode == "AFTER_HOURS_CLOSURE":
        evidence_policy = {
            "policy_id": "AFTER_HOURS_CLOSURE_V1",
            "broker_event_log": "CLOSURE_RECONCILIATION_EVIDENCE",
            "broker_account_truth": "CLOSURE_RECONCILIATION_EVIDENCE",
            "options_snapshot": "NOT_REQUIRED_AFTER_CLOSE",
            "submit": "FORBIDDEN_AFTER_CLOSE",
        }
        return (
            evidence_policy,
            ["closure_authority", "reconciliation", "broker_event_log"],
            ["new_market_open_quote_capture", "future_broker_event_log", "future_options_snapshot"],
            False,
            False,
            False,
            False,
            "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
            readiness_mode,
            "Run closure and reconciliation; do not request market-open quote capture after close.",
        )
    evidence_policy = {
        "policy_id": "HISTORICAL_REPLAY_V1",
        "broker_event_log": "HISTORICAL_ONLY",
        "broker_account_truth": "HISTORICAL_ONLY",
        "options_snapshot": "HISTORICAL_ONLY",
        "submit": "FORBIDDEN_REPLAY",
    }
    return (
        evidence_policy,
        ["historical_artifacts"],
        ["live_submit", "future_broker_event_log", "future_options_snapshot"],
        False,
        False,
        False,
        False,
        "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
        readiness_mode,
        "Use historical replay artifacts only; live submit is not allowed.",
    )


def evaluate_trading_day_readiness_authority_v1(
    *,
    target_day: str,
    truth_root: Path,
    execution_root: Path | None = None,
    environment: str = "PAPER",
    current_time_utc: str | datetime | None = None,
    replay_mode: bool = False,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    now = current_time_utc if isinstance(current_time_utc, datetime) else (_parse_utc(current_time_utc) if current_time_utc else _utc_now())
    target = str(target_day).strip()
    current_day, session_state, readiness_mode = _session_for_target_day(target_day=target, now=now, replay_mode=replay_mode)

    (
        evidence_policy,
        allowed_carry_forward_sources,
        forbidden_future_sources,
        requires_same_day_broker_event_log,
        requires_same_day_options_snapshot,
        requires_live_account_truth,
        submit_allowed_by_mode,
        canonical_blocker,
        blocking_phase,
        operator_next_action,
    ) = _policy_for_mode(readiness_mode)

    paths = _artifact_paths(truth_root=truth_root, execution_root=execution_root, target_day=target)
    prior_log_path = Path(paths["carry_forward_broker_event_log"])
    same_day_log_path = Path(paths["same_day_broker_event_log"])
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "target_day": target,
        "day_utc": target,
        "current_day": current_day,
        "current_time_utc": _iso(now),
        "timezone": TIMEZONE,
        "exchange": EXCHANGE,
        "environment": str(environment or "PAPER").strip().upper(),
        "session_state": session_state,
        "readiness_mode": readiness_mode,
        "evidence_policy": evidence_policy,
        "allowed_carry_forward_sources": allowed_carry_forward_sources,
        "forbidden_future_sources": forbidden_future_sources,
        "requires_same_day_broker_event_log": requires_same_day_broker_event_log,
        "requires_same_day_options_snapshot": requires_same_day_options_snapshot,
        "requires_live_account_truth": requires_live_account_truth,
        "submit_allowed_by_mode": submit_allowed_by_mode,
        "canonical_blocker": canonical_blocker,
        "blocking_phase": blocking_phase,
        "operator_next_action": operator_next_action,
        "produced_at_utc": _iso(_utc_now()),
        "artifact_paths": paths,
        "same_day_broker_event_log_exists": same_day_log_path.exists(),
        "carry_forward_broker_event_log_exists": prior_log_path.exists(),
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_trading_day_readiness_authority_v1(*, truth_root: Path, target_day: str, payload: dict[str, Any]) -> Path:
    path = trading_day_readiness_authority_output_path(truth_root=truth_root, target_day=target_day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")
    return path


def read_or_evaluate_trading_day_readiness_authority_v1(
    *,
    target_day: str,
    truth_root: Path,
    execution_root: Path | None = None,
    environment: str = "PAPER",
    current_time_utc: str | datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    path = trading_day_readiness_authority_output_path(truth_root=truth_root, target_day=target_day).resolve()
    payload = _read_json(path)
    now = current_time_utc if isinstance(current_time_utc, datetime) else (_parse_utc(current_time_utc) if current_time_utc else _utc_now())
    current_day, expected_session_state, expected_readiness_mode = _session_for_target_day(target_day=target_day, now=now)
    cached_target_day = str(payload.get("target_day") or payload.get("day_utc") or "").strip() if payload else ""
    cache_current = (
        payload is not None
        and cached_target_day == target_day
        and str(payload.get("current_day") or "").strip() == current_day
        and str(payload.get("session_state") or "").strip().upper() == expected_session_state
        and str(payload.get("readiness_mode") or "").strip().upper() == expected_readiness_mode
    )
    if not cache_current:
        payload = evaluate_trading_day_readiness_authority_v1(
            target_day=target_day,
            truth_root=truth_root,
            execution_root=execution_root,
            environment=environment,
            current_time_utc=now,
        )
        path = write_trading_day_readiness_authority_v1(truth_root=truth_root, target_day=target_day, payload=payload).resolve()
    return path, payload
