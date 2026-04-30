from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_SAFETY_STATE_AUTHORITY_V1"
SCHEMA_VERSION = 1
AUTHORITY_SCOPE = "SAFETY_STATE"

PASS_STATUSES = {"PASS", "OK", "READY", "AUTHORIZED", "ALLOW", "INACTIVE"}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.000001")))


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_codes(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = str(value or "").strip().upper()
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    return out


def _artifact_day(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("day_utc", "target_day", "trading_day", "session_day"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value[:10]
    return ""


def _record_input(
    *,
    name: str,
    path: Path,
    payload: dict[str, Any] | None,
    day_utc: str,
    stale_or_conflicting_inputs: list[dict[str, Any]],
) -> None:
    if payload is None:
        stale_or_conflicting_inputs.append(
            {
                "artifact": name,
                "path": str(path),
                "condition": "MISSING",
                "expected_day_utc": day_utc,
                "observed_day_utc": None,
            }
        )
        return
    observed_day = _artifact_day(payload)
    if observed_day and observed_day != day_utc:
        stale_or_conflicting_inputs.append(
            {
                "artifact": name,
                "path": str(path),
                "condition": "STALE_OR_WRONG_DAY",
                "expected_day_utc": day_utc,
                "observed_day_utc": observed_day,
            }
        )


def _walk_dicts(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        rows.append(obj)
        for value in obj.values():
            rows.extend(_walk_dicts(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk_dicts(value))
    return rows


def _extract_cents(payload: dict[str, Any] | None, *, preferred_keys: tuple[str, ...] = ()) -> int | None:
    if not isinstance(payload, dict):
        return None
    key_order = tuple(preferred_keys) + (
        "nav_current_cents",
        "current_nav_cents",
        "nav_total_cents",
        "total_nav_cents",
        "net_liquidation_cents",
        "net_liquidation_value_cents",
        "equity_with_loan_value_cents",
        "nav_prior_cents",
        "prior_nav_cents",
    )
    for row in _walk_dicts(payload):
        for key in key_order:
            value = _int(row.get(key))
            if value is not None:
                return value
    return None


def _extract_only_cents(payload: dict[str, Any] | None, keys: tuple[str, ...]) -> int | None:
    if not isinstance(payload, dict):
        return None
    for row in _walk_dicts(payload):
        for key in keys:
            value = _int(row.get(key))
            if value is not None:
                return value
    return None


def _extract_decimal(payload: dict[str, Any] | None, *keys: str) -> Decimal | None:
    if not isinstance(payload, dict):
        return None
    for row in _walk_dicts(payload):
        for key in keys:
            value = _decimal(row.get(key))
            if value is not None:
                return value
    return None


def _prior_nav_path(truth_root: Path, day_utc: str) -> Path:
    nav_root = truth_root / "accounting_v2" / "nav"
    candidates = sorted(
        path
        for path in nav_root.glob("*/nav.v2.json")
        if path.is_file() and path.parent.name < day_utc
    )
    if candidates:
        return candidates[-1].resolve()
    return (nav_root / "__prior_missing__" / "nav.v2.json").resolve()


def safety_state_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "safety_state_authority_v1" / day_utc / "safety_state_authority.v1.json"


def _operator_action(canonical_blocker: str) -> str:
    if canonical_blocker == "NAV_INVALID":
        return "Restore governed NAV/account-value evidence, then rerun safety_state_authority_v1, capital_risk_envelope_v2, global_kill_switch_state_v1, trade_submit_readiness_c2_v1, and submit_boundary_status_v1."
    if canonical_blocker == "DRAWDOWN_LIMIT_EXCEEDED":
        return "Keep submit blocked; resolve the governed drawdown/economic-state condition before allowing new entries."
    if canonical_blocker == "KILL_SWITCH_ACTIVE":
        return "Resolve the kill-switch source condition through governed safety inputs; do not disable the kill switch manually."
    if canonical_blocker == "CAPITAL_RISK_ENVELOPE_NOT_PASS":
        return "Regenerate or repair capital_risk_envelope_v2 from governed capital/NAV inputs, then rerun safety and submit boundary."
    if canonical_blocker == "TRADE_SUBMIT_READINESS_BLOCKED":
        return "Resolve trade_submit_readiness_c2_v1 blockers, then rerun safety_state_authority_v1 and submit_boundary_status_v1."
    if canonical_blocker == "SAFETY_INPUTS_DEGRADED":
        return "Regenerate stale or missing governed safety inputs, then rerun safety_state_authority_v1 and submit_boundary_status_v1."
    if canonical_blocker.endswith("_MISSING"):
        return "Produce the missing governed safety artifact, then rerun safety_state_authority_v1 and submit_boundary_status_v1."
    return "No safety repair action required."


def _root_cause(
    *,
    canonical_blocker: str,
    kill_switch_state: str,
    kill_switch_reason_codes: list[str],
    nav_valid: bool,
    drawdown_status: str,
) -> str:
    if canonical_blocker == "NAV_INVALID":
        dependency = f";kill_switch_dependency=state:{kill_switch_state}" if kill_switch_state == "ACTIVE" else ""
        return f"NAV_INVALID:current/prior NAV missing or non-positive{dependency}"
    if canonical_blocker == "DRAWDOWN_LIMIT_EXCEEDED":
        dependency = f";kill_switch_dependency=state:{kill_switch_state}" if kill_switch_state == "ACTIVE" else ""
        return f"DRAWDOWN_LIMIT_EXCEEDED:governed drawdown breached limit{dependency}"
    if canonical_blocker == "KILL_SWITCH_ACTIVE":
        if not nav_valid:
            return "KILL_SWITCH_ACTIVE:depends_on=NAV_INVALID"
        if drawdown_status == "BLOCKED":
            return "KILL_SWITCH_ACTIVE:depends_on=DRAWDOWN_LIMIT_EXCEEDED"
        codes = ",".join(kill_switch_reason_codes) if kill_switch_reason_codes else "NO_REASON_CODES"
        return f"KILL_SWITCH_ACTIVE:reason_codes={codes}"
    if canonical_blocker == "SAFETY_INPUTS_DEGRADED":
        return "SAFETY_INPUTS_DEGRADED:broker_supply_v1 or capital_supply_v1 missing/stale"
    if canonical_blocker:
        return canonical_blocker
    return "SAFETY_STATE_PASS"


def evaluate_safety_state_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    account: str = "",
    environment: str = "PAPER",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    account = str(account or "").strip()
    environment = str(environment or "PAPER").strip().upper()

    current_nav_path = (truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve()
    prior_nav_path = _prior_nav_path(truth_root, day_utc)
    portfolio_path = (truth_root / "reports" / "portfolio_account_authority_v1" / day_utc / "portfolio_account_authority.v1.json").resolve()
    broker_supply_path = (truth_root / "reports" / "broker_supply_v1" / day_utc / "broker_supply.v1.json").resolve()
    capital_supply_path = (truth_root / "reports" / "capital_supply_v1" / day_utc / "capital_supply.v1.json").resolve()
    kill_switch_path = (truth_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json").resolve()
    capital_envelope_path = (execution_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json").resolve()
    readiness_path = (execution_root / "trade_submit_readiness_c2_v1" / "_history" / environment / account / day_utc / "status.json").resolve()
    submit_boundary_path = (truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").resolve()

    current_nav = _read_json(current_nav_path)
    prior_nav = _read_json(prior_nav_path)
    portfolio = _read_json(portfolio_path)
    broker_supply = _read_json(broker_supply_path)
    capital_supply = _read_json(capital_supply_path)
    kill_switch = _read_json(kill_switch_path)
    capital_envelope = _read_json(capital_envelope_path)
    readiness = _read_json(readiness_path)
    submit_boundary = _read_json(submit_boundary_path)

    stale_or_conflicting_inputs: list[dict[str, Any]] = []
    for name, path, payload in (
        ("broker_supply_v1", broker_supply_path, broker_supply),
        ("capital_supply_v1", capital_supply_path, capital_supply),
        ("portfolio_account_authority_v1", portfolio_path, portfolio),
        ("nav_current_v2", current_nav_path, current_nav),
        ("nav_prior_v2", prior_nav_path, prior_nav),
        ("global_kill_switch_state_v1", kill_switch_path, kill_switch),
        ("capital_risk_envelope_v2", capital_envelope_path, capital_envelope),
        ("trade_submit_readiness_c2_v1", readiness_path, readiness),
    ):
        _record_input(
            name=name,
            path=path,
            payload=payload,
            day_utc=day_utc if name != "nav_prior_v2" else prior_nav_path.parent.name,
            stale_or_conflicting_inputs=stale_or_conflicting_inputs,
        )
    if submit_boundary is not None:
        _record_input(
            name="submit_boundary_status_v1",
            path=submit_boundary_path,
            payload=submit_boundary,
            day_utc=day_utc,
            stale_or_conflicting_inputs=stale_or_conflicting_inputs,
        )

    current_candidates: list[tuple[str, Path, dict[str, Any] | None, int | None]] = [
        ("nav_v2", current_nav_path, current_nav, _extract_cents(current_nav)),
        ("portfolio_account_authority_v1", portfolio_path, portfolio, _extract_cents(portfolio)),
        ("capital_risk_envelope_v2", capital_envelope_path, capital_envelope, _extract_cents(capital_envelope)),
        ("capital_supply_v1", capital_supply_path, capital_supply, _extract_cents(capital_supply)),
    ]
    nav_source = ""
    nav_current_cents: int | None = None
    for source_name, source_path, _payload, cents in current_candidates:
        if cents is not None:
            nav_source = f"{source_name}:{source_path}"
            nav_current_cents = cents
            break

    nav_prior_cents = _extract_only_cents(
        current_nav,
        ("nav_prior_cents", "prior_nav_cents", "previous_nav_cents", "rolling_peak_nav_cents"),
    )
    if nav_prior_cents is None:
        nav_prior_cents = _extract_cents(prior_nav)
    if nav_prior_cents is None:
        nav_prior_cents = _extract_only_cents(
            capital_envelope,
            ("nav_prior_cents", "prior_nav_cents", "rolling_peak_nav_cents", "peak_nav_cents"),
        )

    nav_valid = bool(nav_current_cents is not None and nav_current_cents > 0 and nav_prior_cents is not None and nav_prior_cents > 0)

    envelope = capital_envelope.get("envelope") if isinstance(capital_envelope, dict) and isinstance(capital_envelope.get("envelope"), dict) else {}
    capital_envelope_status = _normalize_status(
        (capital_envelope or {}).get("status")
        or (capital_envelope or {}).get("capital_risk_status")
        or envelope.get("status")
        or "MISSING"
    )

    drawdown_limit = _extract_decimal(capital_envelope, "drawdown_limit_pct", "risk_breach_drawdown_pct", "max_drawdown_pct")
    if drawdown_limit is None:
        drawdown_limit = Decimal("-0.100000")
    elif drawdown_limit > 0:
        drawdown_limit = -drawdown_limit

    if not nav_valid:
        drawdown_pct: Decimal | None = None
        drawdown_status = "NAV_INVALID"
    else:
        drawdown_pct = _extract_decimal(capital_envelope, "drawdown_pct")
        if drawdown_pct is None:
            drawdown_pct = (Decimal(nav_current_cents) / Decimal(nav_prior_cents)) - Decimal("1")
        drawdown_status = "BLOCKED" if drawdown_pct <= drawdown_limit else "PASS"

    kill_switch_state = _normalize_status((kill_switch or {}).get("state") or "MISSING")
    kill_switch_reason_codes = _normalize_codes((kill_switch or {}).get("reason_codes"))
    allow_entries_raw = (kill_switch or {}).get("allow_entries")
    kill_switch_allow_entries = bool(allow_entries_raw is True)
    allow_exits_raw = (kill_switch or {}).get("allow_exits")
    allow_exits = bool(allow_exits_raw is True) if isinstance(allow_exits_raw, bool) else True
    forced_mode = str((kill_switch or {}).get("forced_mode") or (kill_switch or {}).get("mode") or "NONE").strip().upper()
    kill_switch_ok = bool(kill_switch is not None and kill_switch_state == "INACTIVE" and kill_switch_allow_entries)

    readiness_status = _normalize_status((readiness or {}).get("status") or (readiness or {}).get("state") or "MISSING")
    readiness_ok = bool(readiness is not None and ((readiness or {}).get("ok") is True or readiness_status in PASS_STATUSES))
    if (readiness or {}).get("submit_allowed") is False:
        readiness_ok = False
    readiness_codes = _normalize_codes((readiness or {}).get("reason_codes") or (readiness or {}).get("reasons"))

    hard_blockers: list[str] = []
    if not nav_valid:
        hard_blockers.append("NAV_INVALID")
    elif drawdown_status == "BLOCKED":
        hard_blockers.append("DRAWDOWN_LIMIT_EXCEEDED")
    if kill_switch is None:
        hard_blockers.append("GLOBAL_KILL_SWITCH_STATE_MISSING")
    elif not kill_switch_ok:
        hard_blockers.append("KILL_SWITCH_ACTIVE")
    if capital_envelope is None:
        hard_blockers.append("CAPITAL_RISK_ENVELOPE_MISSING")
    elif capital_envelope_status not in PASS_STATUSES:
        hard_blockers.append("CAPITAL_RISK_ENVELOPE_NOT_PASS")
    if readiness is None:
        hard_blockers.append("TRADE_SUBMIT_READINESS_MISSING")
    elif not readiness_ok:
        hard_blockers.append("TRADE_SUBMIT_READINESS_BLOCKED")

    priority = [
        "NAV_INVALID",
        "DRAWDOWN_LIMIT_EXCEEDED",
        "KILL_SWITCH_ACTIVE",
        "GLOBAL_KILL_SWITCH_STATE_MISSING",
        "CAPITAL_RISK_ENVELOPE_NOT_PASS",
        "CAPITAL_RISK_ENVELOPE_MISSING",
        "TRADE_SUBMIT_READINESS_BLOCKED",
        "TRADE_SUBMIT_READINESS_MISSING",
    ]
    canonical_blocker = next((code for code in priority if code in hard_blockers), "")
    diagnostic_missing = {
        str(row.get("artifact"))
        for row in stale_or_conflicting_inputs
        if row.get("condition") == "MISSING" and row.get("artifact") in {"broker_supply_v1", "capital_supply_v1"}
    }
    if not canonical_blocker and diagnostic_missing:
        canonical_blocker = "SAFETY_INPUTS_DEGRADED"
    status = "BLOCKED" if canonical_blocker else ("DEGRADED" if diagnostic_missing else "PASS")
    if canonical_blocker == "SAFETY_INPUTS_DEGRADED":
        status = "DEGRADED"

    allow_entries = bool(status == "PASS" and nav_valid and drawdown_status == "PASS" and kill_switch_ok and capital_envelope_status in PASS_STATUSES and readiness_ok)
    root_cause = _root_cause(
        canonical_blocker=canonical_blocker,
        kill_switch_state=kill_switch_state,
        kill_switch_reason_codes=kill_switch_reason_codes,
        nav_valid=nav_valid,
        drawdown_status=drawdown_status,
    )

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "authority_scope": AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "status": status,
        "account": account,
        "environment": environment,
        "nav_source": nav_source,
        "nav_current_cents": nav_current_cents,
        "nav_prior_cents": nav_prior_cents,
        "nav_valid": nav_valid,
        "drawdown_pct": _decimal_text(drawdown_pct),
        "drawdown_limit_pct": _decimal_text(drawdown_limit),
        "drawdown_status": drawdown_status,
        "kill_switch_state": kill_switch_state,
        "kill_switch_reason_codes": kill_switch_reason_codes,
        "allow_entries": allow_entries,
        "allow_exits": allow_exits,
        "forced_mode": forced_mode,
        "capital_envelope_status": capital_envelope_status,
        "submit_safety_status": "PASS" if allow_entries else "BLOCKED",
        "canonical_blocker": canonical_blocker,
        "root_cause": root_cause,
        "upstream_artifact_paths": {
            "broker_supply_v1": str(broker_supply_path),
            "capital_supply_v1": str(capital_supply_path),
            "portfolio_account_authority_v1": str(portfolio_path),
            "nav_current_v2": str(current_nav_path),
            "nav_prior_v2": str(prior_nav_path),
            "global_kill_switch_state_v1": str(kill_switch_path),
            "capital_risk_envelope_v2": str(capital_envelope_path),
            "trade_submit_readiness_c2_v1": str(readiness_path),
            "submit_boundary_status_v1": str(submit_boundary_path),
        },
        "stale_or_conflicting_inputs": stale_or_conflicting_inputs,
        "operator_next_action": _operator_action(canonical_blocker),
        "hard_blockers": sorted(set(hard_blockers)),
        "trade_submit_readiness_status": readiness_status,
        "trade_submit_readiness_reason_codes": readiness_codes,
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_safety_state_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = safety_state_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
