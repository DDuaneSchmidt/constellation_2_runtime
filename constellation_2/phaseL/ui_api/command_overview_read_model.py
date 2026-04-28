from __future__ import annotations

import json
from datetime import datetime, time, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .common import GLOBAL_TRUTH_ROOT, SLEEVE_TRUTH_ROOT, evidence_ref, is_day_str, iso_from_mtime, list_day_dirs, read_json_dict, utc_now_iso


AUTHORITY_SOURCES: Tuple[Dict[str, str], ...] = (
    {
        "name": "aegis_daily_operator_summary_v1",
        "root": "global",
        "rel": "reports/aegis_daily_operator_summary_v1/{day}/aegis_daily_operator_summary.v1.json",
    },
    {
        "name": "aegis_operating_contract_v1",
        "root": "global",
        "rel": "reports/aegis_operating_contract_v1/{day}/aegis_operating_contract.v1.json",
    },
    {
        "name": "aegis_authority_graph_v1",
        "root": "global",
        "rel": "reports/aegis_authority_graph_v1/{day}/aegis_authority_graph.v1.json",
    },
    {
        "name": "aegis_day_evidence_ledger_v1",
        "root": "global",
        "rel": "reports/aegis_day_evidence_ledger_v1/{day}/aegis_day_evidence_ledger.v1.json",
    },
    {
        "name": "paper_trading_day_authority_v1",
        "root": "global",
        "rel": "reports/paper_trading_day_authority_v1/{day}/paper_trading_day_authority.v1.json",
    },
    {
        "name": "strategy_decision_authority_v1",
        "root": "global",
        "rel": "reports/strategy_decision_authority_v1/{day}/strategy_decision_authority.v1.json",
    },
    {
        "name": "market_data_authority_v1",
        "root": "global",
        "rel": "reports/market_data_authority_v1/{day}/market_data_authority.v1.json",
    },
    {
        "name": "risk_sizing_authority_v1",
        "root": "global",
        "rel": "reports/risk_sizing_authority_v1/{day}/risk_sizing_authority.v1.json",
    },
    {
        "name": "portfolio_account_authority_v1",
        "root": "global",
        "rel": "reports/portfolio_account_authority_v1/{day}/portfolio_account_authority.v1.json",
    },
    {
        "name": "execution_mode_authority_v1",
        "root": "global",
        "rel": "reports/execution_mode_authority_v1/{day}/execution_mode_authority.v1.json",
    },
    {
        "name": "execution_lifecycle_authority_v1",
        "root": "sleeve",
        "rel": "reports/execution_lifecycle_authority_v1/{day}/execution_lifecycle_authority.v1.json",
    },
    {
        "name": "runtime_service_authority_v1",
        "root": "global",
        "rel": "reports/runtime_service_authority_v1/{day}/runtime_service_authority.v1.json",
    },
    {
        "name": "trading_day_closure_authority_v1",
        "root": "global",
        "rel": "reports/trading_day_closure_authority_v1/{day}/trading_day_closure_authority.v1.json",
    },
)


STATE_FIELD_BY_AUTHORITY = {
    "paper_trading_day_authority_v1": "state",
    "strategy_decision_authority_v1": "strategy_decision_state",
    "market_data_authority_v1": "market_data_state",
    "risk_sizing_authority_v1": "risk_sizing_state",
    "portfolio_account_authority_v1": "account_state",
    "execution_mode_authority_v1": "mode_state",
    "execution_lifecycle_authority_v1": "current_lifecycle_state",
    "runtime_service_authority_v1": "service_state",
    "trading_day_closure_authority_v1": "closure_state",
}


SUCCESS_STATES = {
    "PASS",
    "READY",
    "SUCCESS_DRY_RUN",
    "SUCCESS_TRANSMITTED",
    "NO_INTENT_EXPECTED",
    "DRY_RUN_COMPLETE",
    "DRY_RUN_CLOSED",
    "MANUAL_MODE_READY",
    "INTENT_CREATED",
    "SIZED",
    "ROUNDED",
    "OPERATOR_STATEMENT_ONLY",
    "DRY_RUN",
    "SUBMITTING",
}

WARNING_STATES = {
    "WARN",
    "WARNING",
    "STALE",
    "PARTIAL",
    "OPERATOR_STATEMENT_ONLY",
    "POST_SUBMIT_DIAGNOSTIC",
    "ROUNDED",
    "NOT_READY",
}

ERROR_STATES = {
    "FAIL",
    "ERROR",
    "FAILED",
    "BLOCKED",
    "LINEAGE_GAP",
    "IDENTITY_CONFLICT",
    "MISSING_REQUIRED_DATA",
    "MISSING_REQUIRED_SERVICE",
    "ACCOUNT_DATA_MISSING",
    "RECONCILIATION_REQUIRED",
}

MARKET_TIMEZONE = ZoneInfo("America/New_York")
MARKET_REVIEW_CUTOFF_LOCAL = time(16, 0)


def _root_for(source: Dict[str, str]) -> Path:
    return SLEEVE_TRUTH_ROOT if source["root"] == "sleeve" else GLOBAL_TRUTH_ROOT


def _path_for(source: Dict[str, str], day: str) -> Path:
    return (_root_for(source) / source["rel"].format(day=day)).resolve()


def _latest_authority_day() -> Optional[str]:
    priority_roots = [
        GLOBAL_TRUTH_ROOT / "reports" / "aegis_daily_operator_summary_v1",
        GLOBAL_TRUTH_ROOT / "reports" / "aegis_day_evidence_ledger_v1",
        GLOBAL_TRUTH_ROOT / "reports" / "aegis_authority_graph_v1",
    ]
    for root in priority_roots:
        days = list_day_dirs(root)
        if days:
            return days[-1]

    candidates: List[str] = []
    for source in AUTHORITY_SOURCES:
        source_root = _root_for(source)
        parts = source["rel"].split("{day}", 1)
        if not parts:
            continue
        candidates.extend(list_day_dirs((source_root / parts[0]).resolve()))
    if not candidates:
        return None
    return sorted(set(candidates))[-1]


def _read_sources(day: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    docs: Dict[str, Dict[str, Any]] = {}
    refs: Dict[str, Dict[str, Any]] = {}
    for source in AUTHORITY_SOURCES:
        name = source["name"]
        path = _path_for(source, day)
        doc, err = read_json_dict(path)
        exists = doc is not None and err is None
        if exists:
            docs[name] = doc or {}
        refs[name] = {
            "name": name,
            "artifact_type": name,
            "path": str(path),
            "exists": exists,
            "status": "PRESENT" if exists else "UNAVAILABLE",
            "last_update_utc": iso_from_mtime(path) if path.exists() else None,
            "error": "" if exists else (err or "FILE_NOT_FOUND"),
        }
    return docs, refs


def _first_text(*values: Any, default: str = "UNAVAILABLE") -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and not isinstance(value, (dict, list, tuple, set)):
            text = str(value).strip()
            if text:
                return text
    return default


def _state_for(name: str, doc: Optional[Dict[str, Any]]) -> str:
    if not isinstance(doc, dict):
        return "UNAVAILABLE"
    field = STATE_FIELD_BY_AUTHORITY.get(name, "status")
    return _first_text(doc.get(field), doc.get("status"))


def _timestamp_for(doc: Optional[Dict[str, Any]], ref: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(doc, dict):
        for key in ("produced_utc", "produced_at_utc", "finished_utc", "started_utc"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(ref, dict):
        value = ref.get("last_update_utc")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _latest_timestamp(values: Iterable[Optional[str]]) -> Optional[str]:
    present = sorted({value for value in values if isinstance(value, str) and value})
    return present[-1] if present else None


def _parse_utc_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_day_label(day: str) -> str:
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return day
    return parsed.strftime("%b %d")


def _market_calendar_path(day: str, exchange: str = "NYSE") -> Path:
    year = str(day)[0:4]
    nested = GLOBAL_TRUTH_ROOT / "market_calendar_v1" / exchange / f"{year}.jsonl"
    if nested.exists():
        return nested.resolve()
    return (GLOBAL_TRUTH_ROOT / "market_calendar_v1" / f"{year}.jsonl").resolve()


def _read_market_calendar_year(day: str, exchange: str = "NYSE") -> Tuple[Dict[str, bool], Path]:
    path = _market_calendar_path(day, exchange=exchange)
    sessions: Dict[str, bool] = {}
    if not path.exists():
        return sessions, path
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                obj = json.loads(text)
                row_day = obj.get("day_utc")
                is_session = obj.get("is_trading_session")
                if isinstance(row_day, str) and isinstance(is_session, bool):
                    sessions[row_day] = is_session
    except Exception:
        return {}, path
    return sessions, path


def _next_review_card_value(as_of_utc: str, fallback_day: str) -> Dict[str, str]:
    as_of = _parse_utc_timestamp(as_of_utc)
    if as_of is None:
        as_of = _parse_utc_timestamp(f"{fallback_day}T00:00:00Z")
    if as_of is None:
        return {
            "value": "UNAVAILABLE",
            "raw_value": "",
            "detail": "Market calendar unavailable",
            "tone": "muted",
        }

    local_as_of = as_of.astimezone(MARKET_TIMEZONE)
    candidate = local_as_of.date()
    if local_as_of.timetz().replace(tzinfo=None) >= MARKET_REVIEW_CUTOFF_LOCAL:
        candidate = candidate + timedelta(days=1)

    sessions, calendar_path = _read_market_calendar_year(candidate.isoformat())
    if not sessions:
        return {
            "value": "UNAVAILABLE",
            "raw_value": "",
            "detail": f"NYSE calendar unavailable: {calendar_path}",
            "tone": "muted",
        }

    for offset in range(0, 10):
        day = (candidate + timedelta(days=offset)).isoformat()
        if day[0:4] != candidate.isoformat()[0:4]:
            sessions, calendar_path = _read_market_calendar_year(day)
        if sessions.get(day) is True:
            return {
                "value": _format_day_label(day),
                "raw_value": day,
                "detail": f"NYSE review date from {calendar_path}; as-of {local_as_of.strftime('%Y-%m-%d %H:%M %Z')} America/New_York",
                "tone": "info",
            }

    return {
        "value": "UNAVAILABLE",
        "raw_value": "",
        "detail": f"No NYSE session found within 10 days after {candidate.isoformat()}",
        "tone": "muted",
    }


def _tone_from_state(state: str, *, operator_impact: str = "") -> str:
    normalized = str(state or "").strip().upper()
    impact = str(operator_impact or "").strip().upper()
    if normalized in ERROR_STATES or any(token in normalized for token in ("FAIL", "BLOCK", "CONFLICT", "MISSING_REQUIRED")):
        return "error"
    if impact == "POST_SUBMIT_DIAGNOSTIC":
        return "warning"
    if normalized in WARNING_STATES or any(token in normalized for token in ("WARN", "STALE", "PARTIAL", "DIAGNOSTIC")):
        return "warning"
    if normalized in SUCCESS_STATES or any(token in normalized for token in ("READY", "SUCCESS", "COMPLETE", "CLOSED")):
        return "ready"
    if normalized == "UNAVAILABLE":
        return "muted"
    return "info"


def _readiness_value(state: str, status: str = "", operator_impact: str = "") -> str:
    if state == "UNAVAILABLE":
        return "UNAVAILABLE"
    if status and status.upper() in {"FAIL", "WARN", "NOT_READY"}:
        return state
    if operator_impact:
        return state
    return state


def _severity_from_tone(tone: str) -> str:
    if tone == "error":
        return "HIGH"
    if tone == "warning":
        return "MEDIUM"
    if tone == "muted":
        return "LOW"
    return "INFO"


def _source_path_for(name: str, refs: Dict[str, Dict[str, Any]]) -> str:
    ref = refs.get(name) or {}
    return _first_text(ref.get("path"), default=f"{name}:UNAVAILABLE")


def _summary_cards(
    docs: Dict[str, Dict[str, Any]],
    refs: Dict[str, Dict[str, Any]],
    *,
    data_quality: str,
    issue_counts: Dict[str, int],
) -> List[Dict[str, str]]:
    summary = docs.get("aegis_daily_operator_summary_v1") or {}
    ledger = docs.get("aegis_day_evidence_ledger_v1") or {}
    contract = docs.get("aegis_operating_contract_v1") or {}
    paper = docs.get("paper_trading_day_authority_v1") or {}
    market = docs.get("market_data_authority_v1") or {}

    outcome = _first_text(ledger.get("final_daily_outcome"), summary.get("no_silent_day_outcome"), default="UNAVAILABLE")
    exceptions_value = str(issue_counts["blockers"] + issue_counts["diagnostics"])
    exception_detail = f'{issue_counts["blockers"]} blockers • {issue_counts["diagnostics"]} diagnostics'
    policy_state = _first_text(paper.get("status"), paper.get("state"), contract.get("mode"), default="UNAVAILABLE")
    next_review = _next_review_card_value(
        _first_text(
            _timestamp_for(market),
            _timestamp_for(ledger),
            _timestamp_for(summary),
            contract.get("produced_utc"),
            default="",
        ),
        _first_text(contract.get("day_utc"), summary.get("day_utc"), ledger.get("day_utc"), default=""),
    )

    return [
        {
            "label": "System Status",
            "value": outcome,
            "detail": f"Mode: {_first_text(summary.get('mode'), ledger.get('mode'), default='UNAVAILABLE')}",
            "tone": _tone_from_state(outcome),
        },
        {
            "label": "Exceptions",
            "value": exceptions_value,
            "detail": exception_detail,
            "tone": "error" if issue_counts["blockers"] else ("warning" if issue_counts["diagnostics"] else "ready"),
        },
        {
            "label": "Policy State",
            "value": policy_state,
            "detail": f"Run style: {_first_text(contract.get('run_style'), summary.get('run_style'), default='UNAVAILABLE')}",
            "tone": _tone_from_state(policy_state),
        },
        {
            "label": "Data Quality",
            "value": data_quality,
            "detail": _first_text(market.get("operator_impact"), market.get("market_data_state"), default="Authority coverage"),
            "tone": "ready" if data_quality != "UNAVAILABLE" else "muted",
        },
        {
            "label": "Next Review",
            "value": next_review["value"],
            "raw_value": next_review["raw_value"],
            "detail": next_review["detail"],
            "tone": next_review["tone"],
        },
    ]


def _exception_evidence_path(item: Dict[str, Any]) -> str:
    explicit = item.get("evidence_path")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    for key in ("source_a", "source_b"):
        source = item.get(key)
        if isinstance(source, dict) and isinstance(source.get("path"), str) and source.get("path", "").strip():
            return source["path"].strip()
    source_text = str(item.get("source") or "").strip()
    if source_text.startswith("/"):
        return source_text
    return ""


def _with_exception_actions(item: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(item.get("severity") or "MEDIUM").upper()
    evidence_path = _exception_evidence_path(item)
    workflow_route = "/control" if severity == "HIGH" else "/advisory"
    enriched = dict(item)
    enriched["primary_action"] = "view_evidence" if evidence_path else "open_workflow"
    enriched["actions"] = [
        {
            "id": "view_evidence",
            "label": "View Evidence",
            "kind": "evidence",
            "state": "ready" if evidence_path else "empty",
            "path": evidence_path,
            "empty_message": "No evidence artifact path is available for this exception.",
        },
        {
            "id": "acknowledge",
            "label": "Acknowledge",
            "kind": "local",
            "state": "ready",
            "message": "Acknowledged in this browser session.",
        },
        {
            "id": "create_decision",
            "label": "Create Decision",
            "kind": "route",
            "state": "ready",
            "route": "/advisory",
        },
        {
            "id": "open_workflow",
            "label": "Open Workflow",
            "kind": "route",
            "state": "ready",
            "route": workflow_route,
        },
    ]
    return enriched


def _blocker_exception(blocker: Dict[str, Any], source: str, timestamp: str) -> Optional[Dict[str, str]]:
    if not isinstance(blocker, dict) or not blocker:
        return None
    code = _first_text(blocker.get("code"), blocker.get("blocker_code"), blocker.get("first_blocker"), default="")
    if not code:
        return None
    return {
        "severity": "HIGH",
        "icon": "!",
        "timestamp": timestamp,
        "title": code,
        "body": _first_text(blocker.get("reason"), blocker.get("message"), blocker.get("description"), default="Authority blocker requires operator review."),
        "source": source,
    }


def _exceptions(docs: Dict[str, Dict[str, Any]], refs: Dict[str, Dict[str, Any]], as_of: str, day_utc: str) -> Tuple[List[Dict[str, str]], Dict[str, int]]:
    items: List[Dict[str, str]] = []
    blocker_count = 0
    diagnostic_count = 0

    summary = docs.get("aegis_daily_operator_summary_v1") or {}
    summary_blocker = summary.get("first_blocker")
    if isinstance(summary_blocker, dict) and summary_blocker:
        item = _blocker_exception(summary_blocker, "aegis_daily_operator_summary_v1", as_of)
        if item:
            items.append(item)
            blocker_count += 1

    graph = docs.get("aegis_authority_graph_v1") or {}
    for blocker in graph.get("blocking_nodes") or []:
        if not isinstance(blocker, dict):
            continue
        name = _first_text(blocker.get("authority_name"), blocker.get("name"), default="aegis_authority_graph_v1")
        state = _first_text(blocker.get("observed_state"), blocker.get("status"), default="BLOCKED")
        items.append(
            {
                "severity": "HIGH",
                "icon": "!",
                "timestamp": as_of,
                "title": f"{name}: {state}",
                "body": _first_text(blocker.get("first_blocker"), blocker.get("required_or_diagnostic"), default="Authority graph marks this node as blocking."),
                "source": _first_text(blocker.get("artifact_path"), default=_source_path_for("aegis_authority_graph_v1", refs)),
            }
        )
        blocker_count += 1

    ledger = docs.get("aegis_day_evidence_ledger_v1") or {}
    for blocker in ledger.get("blockers") or []:
        if isinstance(blocker, dict):
            item = _blocker_exception(blocker, "aegis_day_evidence_ledger_v1", as_of)
            if item:
                items.append(item)
                blocker_count += 1

    for diagnostic in ledger.get("diagnostics") or []:
        if not isinstance(diagnostic, dict):
            continue
        code = _first_text(diagnostic.get("code"), diagnostic.get("status"), default="")
        if not code:
            continue
        items.append(
            {
                "severity": "MEDIUM",
                "icon": "!",
                "timestamp": as_of,
                "title": code,
                "body": _first_text(diagnostic.get("reason"), diagnostic.get("phase"), default="Diagnostic recorded in daily evidence ledger."),
                "source": "aegis_day_evidence_ledger_v1",
            }
        )
        diagnostic_count += 1

    summary_outcome = _first_text(summary.get("no_silent_day_outcome"), default="")
    ledger_outcome = _first_text(ledger.get("final_daily_outcome"), default="")
    if summary_outcome and ledger_outcome and summary_outcome != ledger_outcome:
        summary_path = _source_path_for("aegis_daily_operator_summary_v1", refs)
        ledger_path = _source_path_for("aegis_day_evidence_ledger_v1", refs)
        summary_ts = _timestamp_for(summary, refs.get("aegis_daily_operator_summary_v1")) or ""
        ledger_ts = _timestamp_for(ledger, refs.get("aegis_day_evidence_ledger_v1")) or ""
        if summary_ts > ledger_ts:
            impact = "summary projection is newer than canonical evidence ledger; daily orchestration should refresh ledger and summary together"
        elif ledger_ts > summary_ts:
            impact = "operator summary projection is stale relative to canonical evidence ledger"
        else:
            impact = "canonical ledger and operator summary disagree with no freshness tie-breaker"
        fix_command = f"npm run aegis:paper:daily -- --day_utc {day_utc}"
        items.append(
            {
                "severity": "MEDIUM",
                "icon": "!",
                "timestamp": as_of,
                "title": "DAILY_OUTCOME_SOURCE_MISMATCH",
                "body": (
                    f"canonical evidence_ledger={ledger_outcome}; operator_summary={summary_outcome}; "
                    f"operator impact: {impact}; fix: {fix_command}"
                ),
                "source": f"ledger: {ledger_path} | summary: {summary_path}",
                "source_a": {"name": "aegis_day_evidence_ledger_v1", "value": ledger_outcome, "path": ledger_path, "produced_utc": ledger_ts},
                "source_b": {"name": "aegis_daily_operator_summary_v1", "value": summary_outcome, "path": summary_path, "produced_utc": summary_ts},
                "operator_impact": impact,
                "fix_command": fix_command,
            }
        )
        diagnostic_count += 1

    if not items:
        items.append(
            {
                "severity": "INFO",
                "icon": "✓",
                "timestamp": as_of,
                "title": "No Active Blockers",
                "body": "Authority graph and evidence ledger contain no blocking nodes for this day.",
                "source": _source_path_for("aegis_day_evidence_ledger_v1", refs),
            }
        )
    return [_with_exception_actions(item) for item in items[:5]], {"blockers": blocker_count, "diagnostics": diagnostic_count}


def _readiness_tiles(docs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    definitions = [
        ("Governance", "paper_trading_day_authority_v1"),
        ("Strategy", "strategy_decision_authority_v1"),
        ("Portfolio", "portfolio_account_authority_v1"),
        ("Risk", "risk_sizing_authority_v1"),
        ("Policy", "execution_mode_authority_v1"),
        ("Data", "market_data_authority_v1"),
        ("Controls", "runtime_service_authority_v1"),
        ("Monitoring", "execution_lifecycle_authority_v1"),
        ("Reporting", "trading_day_closure_authority_v1"),
    ]
    tiles: List[Dict[str, str]] = []
    score_total = 0
    for label, name in definitions:
        doc = docs.get(name)
        state = _state_for(name, doc)
        status = _first_text((doc or {}).get("status"), default="")
        impact = _first_text((doc or {}).get("operator_impact"), default="")
        tone = _tone_from_state(status if status in {"FAIL", "WARN"} else state, operator_impact=impact)
        if tone == "ready":
            score_total += 100
        elif tone == "warning":
            score_total += 60
        elif tone == "info":
            score_total += 80
        else:
            score_total += 0
        tiles.append({"label": label, "value": _readiness_value(state, status, impact), "tone": tone})
    score = round(score_total / max(len(definitions), 1))
    return {"score": f"{score}%", "tiles": tiles}


def _is_expired(doc: Dict[str, Any], as_of_utc: str) -> Tuple[bool, str, str]:
    state = _first_text(doc.get("runtime_state"), doc.get("service_state"), doc.get("status"), default="")
    if "EXPIRED" in state.upper():
        return True, state, "Runtime authority reports an expired state."
    as_of = _parse_utc_timestamp(as_of_utc)
    if as_of is None:
        return False, state or "UNKNOWN", "Runtime expiry could not be evaluated because as-of time is unavailable."
    for key in ("expires_utc", "expires_at_utc", "expires_at", "valid_until_utc"):
        expires = _parse_utc_timestamp(doc.get(key))
        if expires is not None and expires <= as_of:
            return True, state or "EXPIRED", f"{key} elapsed at {expires.isoformat().replace('+00:00', 'Z')}."
    return False, state or "ACTIVE", "No elapsed runtime expiry was found."


def _policy_counts_from_contract(contract: Dict[str, Any], expired: bool) -> Tuple[int, int, int, int]:
    explicit = contract.get("policy_runtime_counts")
    if isinstance(explicit, dict):
        return (
            int(explicit.get("active") or explicit.get("Active") or 0),
            int(explicit.get("pending") or explicit.get("Pending") or 0),
            int(explicit.get("draft") or explicit.get("Draft") or 0),
            int(explicit.get("expired") or explicit.get("Expired") or 0),
        )
    policies = contract.get("policies")
    if isinstance(policies, list):
        counts = {"active": 0, "pending": 0, "draft": 0, "expired": 0}
        for policy in policies:
            if not isinstance(policy, dict):
                continue
            status = str(policy.get("status") or policy.get("state") or "").strip().lower()
            if status in counts:
                counts[status] += 1
        return counts["active"], counts["pending"], counts["draft"], counts["expired"]

    required = contract.get("required_authorities_by_phase")
    active = 0
    if isinstance(required, dict):
        active = len({name for names in required.values() if isinstance(names, list) for name in names})
    pending = len(contract.get("allowed_diagnostic_states") or []) if isinstance(contract.get("allowed_diagnostic_states"), list) else 0
    success = len(contract.get("success_states") or []) if isinstance(contract.get("success_states"), list) else 0
    return active, pending, success, 1 if expired else 0


def _policy_state(contract: Dict[str, Any], runtime: Dict[str, Any], as_of_utc: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    expired, runtime_state, reason = _is_expired({**contract, **runtime}, as_of_utc)
    active, pending, draft, expired_count = _policy_counts_from_contract(contract, expired)
    runtime_tone = "error" if expired else _tone_from_state(runtime_state)
    if runtime_tone == "muted":
        runtime_tone = "info"
    return [
        {"label": "Active", "value": active, "tone": "ready"},
        {"label": "Pending", "value": pending, "tone": "warning"},
        {"label": "Draft", "value": draft, "tone": "info"},
        {"label": "Expired", "value": expired_count, "tone": "error" if expired_count else "ready"},
    ], {
        "state": runtime_state,
        "tone": runtime_tone,
        "expired": expired,
        "reason": reason,
        "remediation": "Refresh or regenerate the runtime authority and operating contract for the current market day." if expired else "No remediation required.",
    }


def _decisions(docs: Dict[str, Dict[str, Any]], as_of: str) -> List[Dict[str, str]]:
    return [
        {
            "status": _state_for("strategy_decision_authority_v1", docs.get("strategy_decision_authority_v1")),
            "title": "Strategy Decision",
            "description": f"{_first_text((docs.get('strategy_decision_authority_v1') or {}).get('intent_count'), default='0')} intent(s) mapped by strategy authority.",
            "timestamp": _timestamp_for(docs.get("strategy_decision_authority_v1")) or as_of,
            "actor": "strategy_decision_authority_v1",
        },
        {
            "status": _state_for("risk_sizing_authority_v1", docs.get("risk_sizing_authority_v1")),
            "title": "Risk Sizing",
            "description": _first_text((docs.get("risk_sizing_authority_v1") or {}).get("first_sizing_reason"), default="Final size reconciled by sizing authority."),
            "timestamp": _timestamp_for(docs.get("risk_sizing_authority_v1")) or as_of,
            "actor": "risk_sizing_authority_v1",
        },
        {
            "status": _state_for("trading_day_closure_authority_v1", docs.get("trading_day_closure_authority_v1")),
            "title": "Day Closure",
            "description": "Closure state is owned by trading_day_closure_authority_v1.",
            "timestamp": _timestamp_for(docs.get("trading_day_closure_authority_v1")) or as_of,
            "actor": "trading_day_closure_authority_v1",
        },
    ]


def _evidence_summary(docs: Dict[str, Dict[str, Any]], refs: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    ledger = docs.get("aegis_day_evidence_ledger_v1") or {}
    artifact_paths = ledger.get("artifact_paths") if isinstance(ledger.get("artifact_paths"), dict) else {}
    commands = ledger.get("commands") if isinstance(ledger.get("commands"), list) else []
    outputs = []
    for command in commands:
        if isinstance(command, dict) and isinstance(command.get("outputs"), list):
            outputs.extend(command.get("outputs") or [])
    total_refs = len(set([str(value) for value in artifact_paths.values()] + [str(value) for value in outputs]))
    failed_checks = len([command for command in commands if isinstance(command, dict) and int(command.get("exit_code") or 0) != 0])
    diagnostics = len(ledger.get("diagnostics") or []) if isinstance(ledger.get("diagnostics"), list) else 0
    last_ingestion = _latest_timestamp([_timestamp_for(ledger, refs.get("aegis_day_evidence_ledger_v1"))])
    return [
        {"label": "Total References", "value": str(total_refs)},
        {"label": "Diagnostics", "value": str(diagnostics)},
        {"label": "Failed Checks", "value": str(failed_checks)},
        {"label": "Last Ingestion", "value": last_ingestion or "UNAVAILABLE"},
    ]


def _lineage(docs: Dict[str, Dict[str, Any]], refs: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    graph = docs.get("aegis_authority_graph_v1") or {}
    nodes = graph.get("authority_nodes") if isinstance(graph.get("authority_nodes"), list) else []
    if nodes:
        out: List[Dict[str, str]] = []
        for node in nodes[:6]:
            if not isinstance(node, dict):
                continue
            name = _first_text(node.get("authority_name"), default="authority_node")
            out.append(
                {
                    "name": name,
                    "description": f"{_first_text(node.get('phase'), default='UNKNOWN')} • {_first_text(node.get('observed_state'), node.get('status'), default='UNKNOWN')}",
                    "status": _first_text(node.get("status"), default="UNKNOWN"),
                    "path": _first_text(node.get("artifact_path"), default=_source_path_for("aegis_authority_graph_v1", refs)),
                }
            )
        return out
    return [
        {
            "name": "authority_graph_unavailable",
            "description": "aegis_authority_graph_v1 is unavailable for this day.",
            "status": "UNAVAILABLE",
            "path": _source_path_for("aegis_authority_graph_v1", refs),
        }
    ]


def _data_quality(refs: Dict[str, Dict[str, Any]]) -> str:
    total = len(AUTHORITY_SOURCES)
    present = len([ref for ref in refs.values() if ref.get("exists")])
    if total == 0 or present == 0:
        return "UNAVAILABLE"
    return f"{(present / total) * 100:.1f}%"


def _source_state(refs: Dict[str, Dict[str, Any]]) -> str:
    present = len([ref for ref in refs.values() if ref.get("exists")])
    if present == 0:
        return "UNAVAILABLE"
    if present == len(AUTHORITY_SOURCES):
        return "REAL"
    return "MIXED / UNAVAILABLE"


def _fallback_badge(source_state: str) -> str:
    if source_state == "REAL":
        return ""
    if source_state == "UNAVAILABLE":
        return "MOCK / UNAVAILABLE"
    return source_state


def _context(
    docs: Dict[str, Dict[str, Any]],
    refs: Dict[str, Dict[str, Any]],
    *,
    data_quality: str,
    as_of: str,
    source_state: str,
) -> Dict[str, Any]:
    summary_path = _source_path_for("aegis_daily_operator_summary_v1", refs)
    summary_ts = _timestamp_for(
        docs.get("aegis_daily_operator_summary_v1"),
        refs.get("aegis_daily_operator_summary_v1"),
    )
    source_refs = [
        ref
        for ref in refs.values()
        if ref.get("exists")
    ][:6]
    if not source_refs:
        source_refs = list(refs.values())[:6]
    present_count = len([ref for ref in refs.values() if ref.get("exists")])
    data_quality_context = f"{data_quality} ({present_count}/{len(AUTHORITY_SOURCES)} authority artifacts; latest {as_of})"
    return {
        "answer": "This view answers: Are we operating within our governed intent?",
        "can": [
            "Review authority-owned readiness",
            "Investigate blockers and diagnostics",
            "Open supporting evidence",
            "Compare source-of-truth artifacts",
        ],
        "sourceOfTruth": f"aegis_daily_operator_summary_v1 ({summary_path}; updated {summary_ts or 'UNAVAILABLE'})",
        "freshness": as_of,
        "dataQuality": data_quality_context,
        "access": "View Only",
        "dataSourceState": source_state,
        "sourceRefs": source_refs,
        "mode": _first_text((docs.get("aegis_daily_operator_summary_v1") or {}).get("mode"), (docs.get("aegis_operating_contract_v1") or {}).get("mode"), default="UNAVAILABLE"),
    }


def build_command_overview_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = str(day).strip() if is_day_str(day) else (_latest_authority_day() or "")
    if not resolved_day:
        empty_refs: Dict[str, Dict[str, Any]] = {}
        data_quality = "UNAVAILABLE"
        return {
            "ok": True,
            "generated_utc": utc_now_iso(),
            "day_utc": None,
            "data_source_state": "UNAVAILABLE",
            "fallback_badge": "MOCK / UNAVAILABLE",
            "as_of_utc": utc_now_iso(),
            "as_of_label": "Data as of: UNAVAILABLE",
            "summary": [
                {"label": "System Status", "value": "UNAVAILABLE", "detail": "No authority day resolved", "tone": "muted"},
                {"label": "Exceptions", "value": "0", "detail": "0 blockers • 0 diagnostics", "tone": "muted"},
                {"label": "Policy State", "value": "UNAVAILABLE", "detail": "No operating contract", "tone": "muted"},
                {"label": "Data Quality", "value": data_quality, "detail": "No authority artifacts", "tone": "muted"},
                {"label": "Next Review", "value": "UNAVAILABLE", "detail": "No authority artifacts", "tone": "muted"},
            ],
            "exceptions": [
                {
                    "severity": "LOW",
                    "icon": "!",
                    "timestamp": "UNAVAILABLE",
                    "title": "Authority Artifacts Unavailable",
                    "body": "No authority-backed Command Overview artifacts were found.",
                    "source": "UNAVAILABLE",
                }
            ],
            "readiness": {"score": "0%", "tiles": []},
            "policy": [],
            "policy_runtime": {
                "state": "UNAVAILABLE",
                "tone": "muted",
                "expired": False,
                "reason": "No operating contract or runtime authority artifacts were found.",
                "remediation": "Generate authority artifacts for the current market day.",
            },
            "decisions": [],
            "evidence": [],
            "lineage": [],
            "context": _context({}, empty_refs, data_quality=data_quality, as_of="UNAVAILABLE", source_state="UNAVAILABLE"),
            "source_refs": [],
        }

    docs, refs = _read_sources(resolved_day)
    source_state = _source_state(refs)
    data_quality = _data_quality(refs)
    timestamps = [_timestamp_for(docs.get(name), refs.get(name)) for name in refs]
    as_of = _latest_timestamp(timestamps) or utc_now_iso()
    exceptions, issue_counts = _exceptions(docs, refs, as_of, resolved_day)

    policy_counts, policy_runtime = _policy_state(
        docs.get("aegis_operating_contract_v1") or {},
        docs.get("runtime_service_authority_v1") or {},
        as_of,
    )

    payload = {
        "ok": True,
        "generated_utc": utc_now_iso(),
        "day_utc": resolved_day,
        "data_source_state": source_state,
        "fallback_badge": _fallback_badge(source_state),
        "as_of_utc": as_of,
        "as_of_label": f"Data as of: {as_of}",
        "summary": _summary_cards(docs, refs, data_quality=data_quality, issue_counts=issue_counts),
        "exceptions": exceptions,
        "readiness": _readiness_tiles(docs),
        "policy": policy_counts,
        "policy_runtime": policy_runtime,
        "decisions": _decisions(docs, as_of),
        "evidence": _evidence_summary(docs, refs),
        "lineage": _lineage(docs, refs),
        "context": _context(docs, refs, data_quality=data_quality, as_of=as_of, source_state=source_state),
        "source_refs": list(refs.values()),
    }
    return payload
