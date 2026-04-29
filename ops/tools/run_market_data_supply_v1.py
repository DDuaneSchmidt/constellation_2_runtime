#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.run_aegis_requirement_graph_v1 import requirement_graph_path
from ops.tools.run_ib_market_data_entitlement_probe_v1 import entitlement_probe_path_v1

SCHEMA_VERSION = "market_data_supply.v1"
OPTIONS_CHAIN_SCHEMA = "constellation_2/schemas/options_chain_snapshot.v1.schema.json"
ALLOWED_BLOCKERS = {
    "MARKET_DATA_REQUIREMENT_UNOWNED",
    "IB_MARKET_DATA_CAPABILITY_UNKNOWN",
    "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
    "OPTIONS_CONTRACT_QUALIFICATION_FAILED",
    "OPTIONS_UNDERLYING_SPOT_MISSING",
    "OPTIONS_QUOTES_MISSING",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
    "OPTIONS_SNAPSHOT_STALE",
    "MARKET_DATA_AUTHORITY_BLOCKED",
    "DELAYED_DATA_POLICY_MISSING",
    "DELAYED_DATA_AVAILABLE_NOT_ACCEPTED",
    "OPTIONS_QUOTES_MISSING_BID_ASK",
    "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS",
    "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB",
    "OPTIONS_QUOTE_VALIDATION_TOO_STRICT",
    "OPTIONS_QUOTE_FIELDS_UNSUPPORTED",
    "OPTIONS_MARKET_DATA_POLICY_REJECTED_QUOTE_TYPE",
}
PAPER_DELAYED_POLICY_RELATIVE_PATH = Path("governance/paper_market_data_policy_v1.json")
MARKET_DATA_REQUIREMENT_ARTIFACTS = {
    "underlying_spot": "UNDERLYING_SPOT",
    "option_chain": "OPTION_CHAIN",
    "bid_ask_quotes": "BID_ASK_QUOTES",
    "freshness_certificate": "FRESHNESS_CERTIFICATE",
    "options_snapshot_artifact": "OPTIONS_SNAPSHOT",
}
PERMISSION_ERROR_CODES = {10089, 10091, 10167}
DELAYED_POLICY_CANDIDATES = (
    Path("governance/02_REGISTRIES/C2_MARKET_DATA_POLICY_V1.json"),
    Path("governance/02_REGISTRIES/C2_PAPER_MARKET_DATA_POLICY_V1.json"),
    Path("governance/05_CONTRACTS/C2/paper_market_data_policy_v1.contract.json"),
)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def market_data_supply_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "market_data_supply_v1" / day_utc / "market_data_supply.v1.json").resolve()


def _entitlement_probe_path(ctx: bod.BodContext, instrument: str) -> Path:
    return entitlement_probe_path_v1(truth_root=ctx.truth_root, day_utc=ctx.day_utc, symbol=instrument)


def _latest_capture_diagnostic(*, execution_root: Path, day_utc: str, symbol: str) -> Path | None:
    root = execution_root / "reports" / "options_chain_capture_ib_day_v1" / day_utc
    if not root.exists() or not root.is_dir():
        return None
    candidates = [
        path.resolve()
        for path in root.glob(f"ib_capture_{symbol.upper()}_*/options_chain_capture_diagnostic.v1.json")
        if path.is_file()
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda path: path.stat().st_mtime)
    return candidates[-1]


def _iter_dicts(value: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(value, dict):
        out.append(value)
        for item in value.values():
            out.extend(_iter_dicts(item))
    elif isinstance(value, list):
        for item in value:
            out.extend(_iter_dicts(item))
    return out


def _ib_error_code(row: dict[str, Any]) -> int | None:
    try:
        return int(row.get("error_code", row.get("code")))
    except Exception:
        return None


def _ib_permission_evidence(diagnostic: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for row in _iter_dicts(diagnostic):
        code = _ib_error_code(row)
        if code not in PERMISSION_ERROR_CODES:
            continue
        evidence.append(
            {
                "ib_error_code": code,
                "mapped_blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                "detail": str(row.get("error_detail") or row.get("message") or "").strip(),
                "req_id": row.get("req_id"),
                "observed_at_utc": row.get("observed_at_utc"),
            }
        )
    return evidence


def _ib_permission_evidence_from_entitlement(probe: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    path = str(probe.get("path") or "").strip()
    if path:
        evidence.append({"artifact_type": "ib_market_data_entitlement_probe", "path": path, "exists": True})
    for row in probe.get("ib_errors") or []:
        if not isinstance(row, dict):
            continue
        try:
            code = int(row.get("error_code"))
        except Exception:
            continue
        if code not in PERMISSION_ERROR_CODES:
            continue
        evidence.append(
            {
                "ib_error_code": code,
                "mapped_blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                "detail": str(row.get("error_message") or row.get("error_detail") or "").strip(),
                "req_id": row.get("req_id"),
                "observed_at_utc": row.get("observed_at_utc"),
            }
        )
    return evidence


def _delayed_data_policy(ctx: bod.BodContext) -> dict[str, Any]:
    candidates = [ctx.truth_root / PAPER_DELAYED_POLICY_RELATIVE_PATH]
    candidates.extend(REPO_ROOT / path for path in DELAYED_POLICY_CANDIDATES)
    for path in candidates:
        payload = _read_json(path)
        if not payload:
            continue
        mode = str(payload.get("mode") or payload.get("environment") or "").strip().upper()
        allowed_types = payload.get("allowed_data_types") if isinstance(payload.get("allowed_data_types"), list) else []
        raw = payload.get("allow_delayed_data")
        if raw is None:
            raw = (
                payload.get("paper_delayed_data_allowed")
                if "paper_delayed_data_allowed" in payload
                else payload.get("delayed_data_accepted_for_paper_readiness")
            )
        accepted = (
            ctx.environment == "PAPER"
            and mode == "PAPER"
            and raw is True
            and bool({"DELAYED", "DELAYED_FROZEN"}.intersection({str(item or "").strip().upper() for item in allowed_types}))
        )
        return {
            "status": "PRESENT",
            "policy_path": str(path),
            "delayed_data_accepted_by_policy": accepted,
            "policy_decision": "DELAYED_DATA_ACCEPTED_BY_POLICY" if accepted else "DELAYED_DATA_NOT_ACCEPTED_BY_POLICY",
            "mode": mode,
            "allow_delayed_data": bool(raw is True),
            "allowed_data_types": allowed_types,
            "requirements": payload.get("requirements") if isinstance(payload.get("requirements"), list) else [],
            "accepted_quote_fields": payload.get("accepted_quote_fields") if isinstance(payload.get("accepted_quote_fields"), list) else [],
            "minimum_acceptable_mode": str(payload.get("minimum_acceptable_mode") or "").strip(),
            "reason": str(payload.get("reason") or "").strip(),
        }
    return {
        "status": "MISSING",
        "policy_path": str((ctx.truth_root / PAPER_DELAYED_POLICY_RELATIVE_PATH).resolve()),
        "delayed_data_accepted_by_policy": False,
        "policy_decision": "DELAYED_DATA_POLICY_MISSING",
        "mode": ctx.environment,
        "allow_delayed_data": False,
        "allowed_data_types": [],
        "requirements": [],
        "accepted_quote_fields": [],
        "minimum_acceptable_mode": "",
        "reason": "",
    }


def _spot_seen(diagnostic: dict[str, Any]) -> bool:
    for row in _iter_dicts(diagnostic):
        spot = row.get("spot_snapshot")
        if not isinstance(spot, dict):
            continue
        for key in ("bid", "ask", "last", "close", "delayed_bid", "delayed_ask", "delayed_last", "delayed_close"):
            if spot.get(key) not in (None, ""):
                return True
    return False


def _contracts_qualified(diagnostic: dict[str, Any]) -> bool:
    for row in _iter_dicts(diagnostic):
        try:
            if int(row.get("contract_details_count") or 0) > 0:
                return True
        except Exception:
            continue
    return False


def _quotes_seen(diagnostic: dict[str, Any]) -> bool:
    for row in _iter_dicts(diagnostic):
        if row.get("valid_quote") is True:
            return True
        try:
            if int(row.get("valid_quote_count") or 0) > 0:
                return True
        except Exception:
            continue
    return False


def _requirement_rows(requirement_graph: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    raw = requirement_graph.get("requirements")
    if not isinstance(raw, list):
        return [], None
    requirements: list[dict[str, Any]] = []
    unowned: dict[str, Any] | None = None
    for row in raw:
        if not isinstance(row, dict):
            continue
        if str(row.get("owner_phase") or "").strip().upper() != "MARKET_DATA":
            continue
        artifact = str(row.get("required_artifact") or "").strip()
        if artifact not in MARKET_DATA_REQUIREMENT_ARTIFACTS:
            continue
        source_type = str(row.get("source_type") or "").strip().upper()
        if source_type not in {"ACTIVE_INTENT", "STATIC_POLICY"}:
            unowned = row
            continue
        if not str(row.get("source_id") or "").strip() or not str(row.get("instrument") or "").strip():
            unowned = row
            continue
        requirements.append(
            {
                "requirement_id": str(row.get("requirement_id") or "").strip(),
                "source_type": source_type,
                "source_id": str(row.get("source_id") or "").strip(),
                "instrument": str(row.get("instrument") or "").strip().upper(),
                "data_type": MARKET_DATA_REQUIREMENT_ARTIFACTS[artifact],
                "required": True,
            }
        )
    return requirements, unowned


def _provider_check(
    *,
    ctx: bod.BodContext,
    instrument: str,
    capability: str,
    status: str,
    evidence: list[dict[str, Any]],
    blocker: str = "",
    action: str = "",
) -> dict[str, Any]:
    return {
        "provider": "IBKR",
        "account": ctx.ib_account,
        "instrument": instrument,
        "capability": capability,
        "status": status,
        "evidence": evidence,
        "blocker": blocker,
        "operator_next_action": action,
    }


def _provider_checks_for_instrument(*, ctx: bod.BodContext, instrument: str) -> tuple[list[dict[str, Any]], str, str]:
    diagnostic_path = _latest_capture_diagnostic(execution_root=ctx.execution_root, day_utc=ctx.day_utc, symbol=instrument)
    diagnostic = _read_json(diagnostic_path) if diagnostic_path else {}
    evidence: list[dict[str, Any]] = []
    if diagnostic_path:
        evidence.append({"artifact_type": "options_chain_capture_diagnostic", "path": str(diagnostic_path), "exists": True})
    permission_evidence = _ib_permission_evidence(diagnostic)
    evidence.extend(permission_evidence[:20])
    action = "Enable IBKR API market-data permissions/subscriptions for the required SPY underlying and options feeds for the logged-in trading user/account."
    if permission_evidence:
        return (
            [
                _provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="UNAVAILABLE", evidence=evidence, blocker="OPTIONS_MARKET_DATA_PERMISSION_DENIED", action=action),
                _provider_check(ctx=ctx, instrument=instrument, capability="UNDERLYING_MARKET_DATA", status="UNAVAILABLE" if not _spot_seen(diagnostic) else "AVAILABLE", evidence=evidence, blocker="" if _spot_seen(diagnostic) else "OPTIONS_MARKET_DATA_PERMISSION_DENIED", action="" if _spot_seen(diagnostic) else action),
                _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_CHAIN_QUALIFICATION", status="AVAILABLE" if _contracts_qualified(diagnostic) else "UNKNOWN", evidence=evidence),
                _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_BID_ASK_QUOTES", status="UNAVAILABLE" if not _quotes_seen(diagnostic) else "AVAILABLE", evidence=evidence, blocker="" if _quotes_seen(diagnostic) else "OPTIONS_MARKET_DATA_PERMISSION_DENIED", action="" if _quotes_seen(diagnostic) else action),
            ],
            "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
            action,
        )
    if diagnostic_path:
        checks = [
            _provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="AVAILABLE" if (_spot_seen(diagnostic) or _quotes_seen(diagnostic)) else "UNKNOWN", evidence=evidence),
            _provider_check(ctx=ctx, instrument=instrument, capability="UNDERLYING_MARKET_DATA", status="AVAILABLE" if _spot_seen(diagnostic) else "UNKNOWN", evidence=evidence),
            _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_CHAIN_QUALIFICATION", status="AVAILABLE" if _contracts_qualified(diagnostic) else "UNKNOWN", evidence=evidence),
            _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_BID_ASK_QUOTES", status="AVAILABLE" if _quotes_seen(diagnostic) else "UNKNOWN", evidence=evidence),
        ]
        return checks, "", ""
    return (
        [_provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="UNKNOWN", evidence=[])],
        "",
        "",
    )


def _provider_checks_from_entitlement(
    *,
    ctx: bod.BodContext,
    instrument: str,
    entitlement_probe: dict[str, Any],
    delayed_policy: dict[str, Any],
) -> tuple[list[dict[str, Any]], str, str]:
    if not entitlement_probe:
        return [], "", ""
    status = str(entitlement_probe.get("status") or "").strip().upper()
    live_available = entitlement_probe.get("live_data_available") is True
    delayed_available = entitlement_probe.get("delayed_data_available") is True
    delayed_accepted = bool(delayed_policy.get("delayed_data_accepted_by_policy") is True)
    permission_evidence = _ib_permission_evidence_from_entitlement(entitlement_probe)
    evidence = permission_evidence or [{"artifact_type": "ib_market_data_entitlement_probe", "path": str(entitlement_probe.get("path") or _entitlement_probe_path(ctx, instrument)), "exists": True}]
    action = "Enable IBKR Client Portal market-data subscriptions and API market-data access for SPY underlying and options for the logged-in trading user/account."
    if live_available:
        return (
            [
                _provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="AVAILABLE", evidence=evidence),
                _provider_check(ctx=ctx, instrument=instrument, capability="UNDERLYING_MARKET_DATA", status="AVAILABLE", evidence=evidence),
                _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_BID_ASK_QUOTES", status="AVAILABLE", evidence=evidence),
            ],
            "",
            "",
        )
    if delayed_available and delayed_accepted:
        return (
            [
                _provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="AVAILABLE", evidence=evidence + [{"capability": "DELAYED_DATA_ACCEPTED_BY_POLICY", "policy_path": delayed_policy.get("policy_path", "")}]),
                _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_BID_ASK_QUOTES", status="AVAILABLE", evidence=evidence + [{"capability": "DELAYED_DATA_ACCEPTED_BY_POLICY", "policy_decision": "DELAYED_DATA_ACCEPTED_BY_POLICY", "policy_path": delayed_policy.get("policy_path", "")}]),
            ],
            "",
            "",
        )
    if status == "BLOCKED" or permission_evidence:
        if delayed_available and not delayed_accepted:
            action = "Live IBKR API market data is unavailable. Delayed/frozen data was observed but cannot satisfy readiness without an explicit governed paper delayed-data policy; enable live API market-data permissions or add/approve that policy."
        return (
            [
                _provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="UNAVAILABLE", evidence=evidence, blocker="OPTIONS_MARKET_DATA_PERMISSION_DENIED", action=action),
                _provider_check(ctx=ctx, instrument=instrument, capability="OPTIONS_BID_ASK_QUOTES", status="UNAVAILABLE", evidence=evidence, blocker="OPTIONS_MARKET_DATA_PERMISSION_DENIED", action=action),
            ],
            "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
            action,
        )
    return (
        [_provider_check(ctx=ctx, instrument=instrument, capability="IB_MARKET_DATA_API_ACCESS", status="UNKNOWN", evidence=evidence)],
        "",
        "",
    )


def _run_capture(ctx: bod.BodContext, instrument: str) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "ops/tools/run_options_chain_snapshot_required_day_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--truth_root",
        str(ctx.execution_root),
        "--symbol",
        instrument,
        "--symbols_from_intents",
        "NO",
    ]
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(ctx.execution_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, env=env, timeout=90)
    snapshot_path = ""
    cert_path = ""
    blocker = ""
    try:
        payload = json.loads(str(proc.stdout or "").splitlines()[-1])
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list) and results:
            first = results[0] if isinstance(results[0], dict) else {}
            blocker = str(first.get("reason_code") or "").strip()
            if str(first.get("status") or "").upper() == "PASS":
                snapshot_path = str(first.get("path") or "")
                cert_path = str(first.get("freshness_certificate_path") or "")
        elif str(payload.get("status") or "").upper() == "OK":
            blocker = ""
    if proc.returncode != 0 and not blocker:
        blocker = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
    return {
        "instrument": instrument,
        "command": " ".join(cmd),
        "status": "PASS" if proc.returncode == 0 else "BLOCKED",
        "blocker": blocker if blocker in ALLOWED_BLOCKERS else "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
        "snapshot_path": snapshot_path,
        "freshness_certificate_path": cert_path,
        "exit_code": int(proc.returncode),
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }


def _inside_regular_us_options_hours(now_utc: datetime | None = None) -> bool:
    now = now_utc or datetime.now(UTC)
    local = now.astimezone(ZoneInfo("America/New_York"))
    if local.weekday() >= 5:
        return False
    minutes = local.hour * 60 + local.minute
    return (9 * 60 + 30) <= minutes < (16 * 60)


def _market_session_state(now_utc: datetime | None = None) -> str:
    now = now_utc or datetime.now(UTC)
    local = now.astimezone(ZoneInfo("America/New_York"))
    if local.weekday() >= 5:
        return "NON_TRADING_DAY"
    minutes = local.hour * 60 + local.minute
    if minutes < (9 * 60 + 30):
        return "PRE_MARKET"
    if minutes < (16 * 60):
        return "REGULAR"
    return "AFTER_HOURS"


def _quote_field_names(row: dict[str, Any]) -> set[str]:
    fields: set[str] = set()
    for key in ("bid", "ask", "last", "close", "mark", "midpoint", "delayed_bid", "delayed_ask", "delayed_last", "delayed_close"):
        if row.get(key) not in (None, ""):
            fields.add(key)
    option_computation = row.get("option_computation")
    if isinstance(option_computation, dict):
        for comp in option_computation.values():
            if isinstance(comp, dict) and comp.get("opt_price") not in (None, ""):
                fields.add("mark")
    return fields


def _classify_delayed_quote_diagnostic(diagnostic: dict[str, Any], delayed_policy: dict[str, Any]) -> str:
    attempts = diagnostic.get("attempts") if isinstance(diagnostic.get("attempts"), list) else []
    delayed_attempt = next((row for row in attempts if isinstance(row, dict) and int(row.get("market_data_type_requested") or 0) == 3), {})
    if not delayed_attempt:
        return "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB"
    samples = delayed_attempt.get("quote_samples") if isinstance(delayed_attempt.get("quote_samples"), list) else []
    callbacks = [
        row for row in samples
        if isinstance(row, dict)
        and (
            list(row.get("observed_tick_types") or [])
            or row.get("market_data_type_callback") not in (None, "")
            or bool(row.get("option_computation"))
            or bool(_quote_field_names(row))
        )
    ]
    if not callbacks:
        return "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB"
    observed_fields: set[str] = set()
    for row in callbacks:
        observed_fields.update(_quote_field_names(row))
    has_bid_ask = {"bid", "ask"}.issubset(observed_fields) or {"delayed_bid", "delayed_ask"}.issubset(observed_fields)
    if has_bid_ask:
        return "OPTIONS_QUOTES_MISSING_BID_ASK"
    if observed_fields:
        accepted = {str(item or "").strip().lower() for item in delayed_policy.get("accepted_quote_fields") or []}
        normalized_observed = {field.removeprefix("delayed_") for field in observed_fields}
        minimum_mode = str(delayed_policy.get("minimum_acceptable_mode") or "").strip().upper()
        if minimum_mode == "BID_ASK_REQUIRED" and normalized_observed.intersection(accepted):
            return "OPTIONS_MARKET_DATA_POLICY_REJECTED_QUOTE_TYPE"
        return "OPTIONS_QUOTE_VALIDATION_TOO_STRICT"
    return "OPTIONS_QUOTE_FIELDS_UNSUPPORTED"


def _refine_delayed_capture_blocker(
    ctx: bod.BodContext,
    instrument: str,
    capture: dict[str, Any],
    *,
    delayed_data_used: bool,
    delayed_policy: dict[str, Any],
) -> str:
    blocker = str(capture.get("blocker") or "").strip()
    if not delayed_data_used:
        return blocker
    if not _inside_regular_us_options_hours():
        return "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS"
    diagnostic_path = _latest_capture_diagnostic(execution_root=ctx.execution_root, day_utc=ctx.day_utc, symbol=instrument)
    diagnostic = _read_json(diagnostic_path) if diagnostic_path else {}
    if diagnostic:
        classified = _classify_delayed_quote_diagnostic(diagnostic, delayed_policy)
        if classified:
            return classified
    text = "\n".join([str(capture.get("stdout_summary") or ""), str(capture.get("stderr_summary") or "")]).upper()
    if "MARKET_DATA_TYPE=3:NO_VALID_OPTION_QUOTES_CAPTURED" in text or "NO_VALID_OPTION_QUOTES_CAPTURED" in text:
        return "OPTIONS_QUOTES_MISSING_BID_ASK"
    if "MARKET_DATA_TYPE=3:UNDERLYING_SPOT_MISSING" in text:
        return "OPTIONS_UNDERLYING_SPOT_MISSING"
    if "CONTRACT" in text and "MISSING" in text:
        return "OPTIONS_CONTRACT_QUALIFICATION_FAILED"
    return blocker


def _latest_snapshot_for_symbol(*, execution_root: Path, day_utc: str, instrument: str) -> tuple[Path | None, Path | None, dict[str, Any], dict[str, Any]]:
    root = execution_root / "options_chain_snapshot_v1" / day_utc
    candidates: list[tuple[Path, Path, dict[str, Any], dict[str, Any]]] = []
    if root.exists() and root.is_dir():
        for path in root.rglob("options_chain_snapshot.v1.json"):
            payload = _read_json(path)
            underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
            observed = str(underlying.get("symbol") or payload.get("symbol") or "").strip().upper()
            if observed != instrument.upper():
                continue
            cert_path = path.parent / "freshness_certificate.v1.json"
            candidates.append((path.resolve(), cert_path.resolve(), payload, _read_json(cert_path)))
    if not candidates:
        return None, None, {}, {}
    candidates.sort(key=lambda row: str(row[0]))
    return candidates[-1]


def _snapshot_market_data_mode(snapshot: dict[str, Any]) -> str:
    provenance = snapshot.get("provenance") if isinstance(snapshot.get("provenance"), dict) else {}
    try:
        market_data_type = int(provenance.get("market_data_type"))
    except Exception:
        market_data_type = 0
    if market_data_type == 1:
        return "LIVE"
    if market_data_type in {3, 4}:
        return "DELAYED"
    return "UNKNOWN"


def _contract_has_quote(contract: dict[str, Any]) -> bool:
    for key in ("bid", "ask", "last", "mark", "mid", "close", "delayed_bid", "delayed_ask", "delayed_last", "delayed_close"):
        if contract.get(key) not in (None, ""):
            return True
    quote = contract.get("quote")
    if isinstance(quote, dict):
        return any(quote.get(key) not in (None, "") for key in ("bid", "ask", "last", "mark", "mid"))
    return False


def _validate_snapshot(*, ctx: bod.BodContext, instrument: str, eval_time_utc: str, data_mode: str = "UNKNOWN") -> tuple[str, dict[str, Any]]:
    snapshot_path, cert_path, snapshot, cert = _latest_snapshot_for_symbol(execution_root=ctx.execution_root, day_utc=ctx.day_utc, instrument=instrument)
    observed_mode = _snapshot_market_data_mode(snapshot) if snapshot else data_mode
    artifact = {
        "instrument": instrument,
        "snapshot_path": str(snapshot_path or ""),
        "freshness_certificate_path": str(cert_path or ""),
        "snapshot_valid": False,
        "freshness_valid": False,
        "data_mode": data_mode if data_mode != "UNKNOWN" else observed_mode,
        "quote_timestamp_available": False,
        "blocker": "",
    }
    if snapshot_path is None:
        artifact["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return "OPTIONS_SNAPSHOT_CAPTURE_FAILED", artifact
    if not str(snapshot_path).startswith(str((ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc).resolve())):
        artifact["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return "OPTIONS_SNAPSHOT_CAPTURE_FAILED", artifact
    try:
        validate_against_repo_schema_v1(snapshot, REPO_ROOT, OPTIONS_CHAIN_SCHEMA)
    except Exception:
        artifact["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return "OPTIONS_SNAPSHOT_CAPTURE_FAILED", artifact
    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    if str(underlying.get("symbol") or snapshot.get("symbol") or "").strip().upper() != instrument.upper():
        artifact["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return "OPTIONS_SNAPSHOT_CAPTURE_FAILED", artifact
    spot = underlying.get("spot_price") or underlying.get("spot") or snapshot.get("spot_price")
    if spot in (None, ""):
        artifact["blocker"] = "OPTIONS_UNDERLYING_SPOT_MISSING"
        return "OPTIONS_UNDERLYING_SPOT_MISSING", artifact
    if not str(snapshot.get("as_of_utc") or "").strip() or not str(underlying.get("spot_as_of_utc") or "").strip():
        artifact["blocker"] = "OPTIONS_SNAPSHOT_STALE"
        return "OPTIONS_SNAPSHOT_STALE", artifact
    contracts = snapshot.get("contracts")
    if not isinstance(contracts, list) or not contracts:
        artifact["blocker"] = "OPTIONS_CONTRACT_QUALIFICATION_FAILED"
        return "OPTIONS_CONTRACT_QUALIFICATION_FAILED", artifact
    if not any(isinstance(row, dict) and _contract_has_quote(row) for row in contracts):
        artifact["blocker"] = "OPTIONS_QUOTES_MISSING_BID_ASK"
        return "OPTIONS_QUOTES_MISSING_BID_ASK", artifact
    if cert_path is None or not cert_path.exists() or not cert:
        artifact["blocker"] = "OPTIONS_SNAPSHOT_STALE"
        return "OPTIONS_SNAPSHOT_STALE", artifact
    valid_until = _parse_iso(cert.get("valid_until_utc"))
    eval_time = _parse_iso(eval_time_utc)
    if valid_until is None or eval_time is None or valid_until < eval_time:
        artifact["blocker"] = "OPTIONS_SNAPSHOT_STALE"
        return "OPTIONS_SNAPSHOT_STALE", artifact
    artifact["snapshot_valid"] = True
    artifact["freshness_valid"] = True
    artifact["quote_timestamp_available"] = True
    artifact["data_mode"] = data_mode if data_mode != "UNKNOWN" else observed_mode
    return "", artifact


def _run_market_data_authority(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    cmd = [
        sys.executable,
        "ops/tools/run_market_data_authority_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--truth_root",
        str(ctx.truth_root),
        "--execution_root",
        str(ctx.execution_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, timeout=60)
    try:
        summary = json.loads(str(proc.stdout or "").splitlines()[-1])
    except Exception:
        summary = {}
    return {
        "command": " ".join(cmd),
        "exit_code": int(proc.returncode),
        "summary": summary if isinstance(summary, dict) else {},
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }, "" if proc.returncode == 0 else "MARKET_DATA_AUTHORITY_BLOCKED"


def _operator_action(blocker: str, instrument: str, day_utc: str) -> str:
    if blocker == "OPTIONS_MARKET_DATA_PERMISSION_DENIED":
        return "Enable IBKR Client Portal market-data subscriptions and API market-data access for SPY underlying and options for the logged-in trading user/account."
    if blocker == "MARKET_OPEN_DATA_PENDING":
        return f"BOD market-data prerequisites are complete enough for pre-market; rerun python3 ops/tools/run_market_open_data_gate_v1.py --day_utc {day_utc} --environment PAPER after 09:30 ET."
    if blocker == "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS":
        return f"Rerun SPY options delayed quote capture during regular US options market hours for {day_utc}, or enable live IBKR API option quote entitlement."
    if blocker == "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB":
        return "IB did not return delayed option quote callbacks; confirm delayed option quotes are enabled in TWS/API and retry during regular US options market hours."
    if blocker == "OPTIONS_QUOTES_MISSING_BID_ASK":
        return "SPY option bid/ask quotes are required for snapshot construction; enable OPRA/API option bid/ask quotes or retry during regular options market hours."
    if blocker == "OPTIONS_MARKET_DATA_POLICY_REJECTED_QUOTE_TYPE":
        return "IB returned only non-bid/ask option quote fields; current PAPER policy requires bid/ask for readiness, so enable option bid/ask quotes or change governed policy only if the strategy/order builder supports it."
    if blocker == "OPTIONS_QUOTE_VALIDATION_TOO_STRICT":
        return "IB returned non-bid/ask option fields; review whether the governed strategy/order path supports those fields before changing quote validation."
    if blocker == "DELAYED_DATA_POLICY_MISSING":
        return "Add or activate a governed paper delayed-data policy before delayed/frozen market data can satisfy readiness, or enable live IBKR API market data."
    if blocker == "DELAYED_DATA_AVAILABLE_NOT_ACCEPTED":
        return "Enable live IBKR API market data or explicitly approve governed paper delayed-data use before using delayed/frozen quotes for readiness."
    if blocker == "MARKET_DATA_REQUIREMENT_UNOWNED":
        return "Fix Requirement Graph ownership so the MARKET_DATA requirement points to an ACTIVE_INTENT or STATIC_POLICY."
    if blocker == "IB_MARKET_DATA_CAPABILITY_UNKNOWN":
        return f"Run python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc} to probe IBKR market-data capability for {instrument}."
    if blocker:
        return f"Resolve {blocker} for {instrument}, then rerun python3 ops/tools/run_market_data_supply_v1.py --day_utc {day_utc} --environment PAPER."
    return ""


def build_market_data_supply(ctx: bod.BodContext) -> dict[str, Any]:
    eval_time_utc = _now_iso()
    market_session_state = _market_session_state()
    req_path = requirement_graph_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    requirement_graph = _read_json(req_path)
    requirements, unowned = _requirement_rows(requirement_graph if str(requirement_graph.get("day_utc") or "") == ctx.day_utc else {})
    provider_checks: list[dict[str, Any]] = []
    capture_attempts: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    entitlement_probes: list[dict[str, Any]] = []
    authority_result: dict[str, Any] = {}
    delayed_policy = _delayed_data_policy(ctx)
    blocker = ""
    if unowned is not None:
        requirements.append(
            {
                "requirement_id": str(unowned.get("requirement_id") or "UNOWNED_MARKET_DATA_REQUIREMENT"),
                "source_type": str(unowned.get("source_type") or "").strip().upper() or "UNKNOWN",
                "source_id": str(unowned.get("source_id") or "").strip(),
                "instrument": str(unowned.get("instrument") or "").strip().upper(),
                "data_type": str(unowned.get("required_artifact") or "").strip(),
                "required": True,
            }
        )
        blocker = "MARKET_DATA_REQUIREMENT_UNOWNED"
    if not requirements and not blocker:
        return {
            "schema_id": "market_data_supply",
            "schema_version": SCHEMA_VERSION,
            "day_utc": ctx.day_utc,
            "environment": ctx.environment,
            "generated_at_utc": eval_time_utc,
            "market_session_state": market_session_state,
            "status": "SKIPPED",
            "canonical_blocker": "",
            "requirements": [],
            "provider_checks": [],
            "capture_attempts": [],
            "artifacts": [],
            "authority_result": {},
            "entitlement_probe_path": "",
            "entitlement_status": "NOT_REQUIRED",
            "tested_data_types": [],
            "live_data_available": False,
            "delayed_data_available": False,
            "delayed_data_accepted_by_policy": False,
            "delayed_data_policy": delayed_policy,
            "delayed_data_used": False,
            "market_data_mode": "NOT_REQUIRED",
            "policy_source_path": str(delayed_policy.get("policy_path") or ""),
            "ib_error_codes": [],
            "operator_next_action": "",
        }
    instruments = sorted({str(row.get("instrument") or "").strip().upper() for row in requirements if row.get("instrument")})
    provider_blocker = ""
    provider_action = ""
    for instrument in instruments:
        probe_path = _entitlement_probe_path(ctx, instrument)
        probe = _read_json(probe_path)
        if probe:
            probe = {**probe, "path": str(probe_path)}
            entitlement_probes.append(probe)
            checks, check_blocker, check_action = _provider_checks_from_entitlement(ctx=ctx, instrument=instrument, entitlement_probe=probe, delayed_policy=delayed_policy)
        else:
            checks, check_blocker, check_action = _provider_checks_for_instrument(ctx=ctx, instrument=instrument)
        provider_checks.extend(checks)
        if check_blocker and not provider_blocker:
            provider_blocker = check_blocker
            provider_action = check_action
    root_probe = entitlement_probes[0] if entitlement_probes else {}
    delayed_data_used = (
        bool(root_probe.get("live_data_available") is False)
        and bool(root_probe.get("delayed_data_available") is True)
        and bool(delayed_policy.get("delayed_data_accepted_by_policy") is True)
    )
    market_data_mode = "DELAYED" if delayed_data_used else ("LIVE" if root_probe.get("live_data_available") is True else "UNKNOWN")
    if blocker:
        pass
    elif provider_blocker:
        blocker = provider_blocker
    elif market_session_state == "PRE_MARKET":
        blocker = "MARKET_OPEN_DATA_PENDING"
    else:
        for instrument in instruments:
            unknown = any(
                row.get("instrument") == instrument
                and row.get("capability") == "IB_MARKET_DATA_API_ACCESS"
                and row.get("status") == "UNKNOWN"
                for row in provider_checks
            )
            if unknown:
                capture = _run_capture(ctx, instrument)
                capture["data_mode"] = market_data_mode
                capture["blocker"] = _refine_delayed_capture_blocker(ctx, instrument, capture, delayed_data_used=delayed_data_used, delayed_policy=delayed_policy)
                capture_attempts.append(capture)
                probe_path = _entitlement_probe_path(ctx, instrument)
                probe = _read_json(probe_path)
                if probe:
                    probe = {**probe, "path": str(probe_path)}
                    entitlement_probes.append(probe)
                    checks, check_blocker, check_action = _provider_checks_from_entitlement(ctx=ctx, instrument=instrument, entitlement_probe=probe, delayed_policy=delayed_policy)
                else:
                    checks, check_blocker, check_action = _provider_checks_for_instrument(ctx=ctx, instrument=instrument)
                provider_checks = [row for row in provider_checks if not (row.get("instrument") == instrument and row.get("provider") == "IBKR")]
                provider_checks.extend(checks)
                if check_blocker:
                    blocker = check_blocker
                    provider_action = check_action
                    break
                if capture["status"] != "PASS":
                    blocker = str(capture.get("blocker") or "OPTIONS_SNAPSHOT_CAPTURE_FAILED")
                    break
        if not blocker:
            for instrument in instruments:
                snapshot_path, _cert_path, _snapshot_payload, _cert_payload = _latest_snapshot_for_symbol(execution_root=ctx.execution_root, day_utc=ctx.day_utc, instrument=instrument)
                if snapshot_path is None and not any(row.get("instrument") == instrument for row in capture_attempts):
                    capture = _run_capture(ctx, instrument)
                    capture["data_mode"] = market_data_mode
                    capture["blocker"] = _refine_delayed_capture_blocker(ctx, instrument, capture, delayed_data_used=delayed_data_used, delayed_policy=delayed_policy)
                    capture_attempts.append(capture)
                    if capture["status"] != "PASS":
                        blocker = str(capture.get("blocker") or "OPTIONS_SNAPSHOT_CAPTURE_FAILED")
                        break
            if blocker:
                pass
        if not blocker:
            for instrument in instruments:
                validation_blocker, artifact = _validate_snapshot(ctx=ctx, instrument=instrument, eval_time_utc=eval_time_utc, data_mode=market_data_mode)
                artifacts.append(artifact)
                if validation_blocker:
                    blocker = validation_blocker
                    break
        if not blocker:
            authority_result, authority_blocker = _run_market_data_authority(ctx)
            if authority_blocker:
                blocker = authority_blocker
    status = "PRE_MARKET_PENDING" if blocker == "MARKET_OPEN_DATA_PENDING" else ("PASS" if not blocker else "BLOCKED")
    root_instrument = instruments[0] if instruments else ""
    action = provider_action or _operator_action(blocker, root_instrument, ctx.day_utc)
    ib_error_codes: list[int] = []
    for probe in entitlement_probes:
        for code in probe.get("ib_error_codes") or []:
            try:
                value = int(code)
            except Exception:
                continue
            if value not in ib_error_codes:
                ib_error_codes.append(value)
    return {
        "schema_id": "market_data_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": eval_time_utc,
        "market_session_state": market_session_state,
        "status": status,
        "canonical_blocker": blocker,
        "requirements": requirements,
        "provider_checks": provider_checks,
        "capture_attempts": capture_attempts,
        "artifacts": artifacts,
        "authority_result": authority_result,
        "entitlement_probe_path": str(_entitlement_probe_path(ctx, root_instrument)) if root_instrument else "",
        "entitlement_status": str(root_probe.get("status") or ("MISSING" if root_instrument else "NOT_REQUIRED")).strip().upper(),
        "tested_data_types": root_probe.get("tested_data_types") if isinstance(root_probe.get("tested_data_types"), list) else [],
        "live_data_available": bool(root_probe.get("live_data_available") is True),
        "delayed_data_available": bool(root_probe.get("delayed_data_available") is True),
        "delayed_data_accepted_by_policy": bool(delayed_policy.get("delayed_data_accepted_by_policy") is True),
        "delayed_data_policy": delayed_policy,
        "delayed_data_used": delayed_data_used,
        "market_data_mode": market_data_mode,
        "policy_source_path": str(delayed_policy.get("policy_path") or ""),
        "delayed_data_policy_result": (
            "DELAYED_DATA_ACCEPTED_BY_POLICY"
            if delayed_policy.get("delayed_data_accepted_by_policy") is True
            else ("DELAYED_DATA_AVAILABLE_NOT_ACCEPTED" if root_probe.get("delayed_data_available") is True else delayed_policy.get("policy_decision", "DELAYED_DATA_POLICY_MISSING"))
        ),
        "ib_error_codes": sorted(ib_error_codes),
        "entitlement_probes": entitlement_probes,
        "operator_next_action": action,
    }


def run_market_data_supply_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_market_data_supply(ctx)
    path = market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_MARKET_DATA_SUPPLY_COLLISION: {path}")
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_market_data_supply_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_market_data_supply_v1(day_utc, environment, str(args.truth_root or ""))
    print(
        json.dumps(
            {
                "status": payload["status"],
                "canonical_blocker": payload["canonical_blocker"],
                "market_data_supply_path": str(path),
            },
            sort_keys=True,
        )
    )
    return 0 if payload.get("status") in {"PASS", "SKIPPED", "PRE_MARKET_PENDING"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
