from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1
from ops.aegis.market_data.market_data_provider_v1 import (
    SUPPORTED_PROVIDERS,
    _fetch_provider,
    provider_config_from_env_v1,
)
from ops.aegis.market_data.symbol_alias_registry_v1 import (
    canonicalize_symbol_list_v1,
    market_data_kind_v1,
    normalize_market_symbol_v1,
    provider_symbol_candidates_v1,
)
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1, sleeve_required_symbols_from_registry_v1
from ops.aegis.run_context_v1 import RunContext, child_run_context_v1, run_context_from_env_v1, step_allowed_v1, subprocess_env_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_FAMILY = "aegis_data_remediation_v1"
LEDGER_FILENAME = "data_remediation_ledger.v1.jsonl"
SUMMARY_FILENAME = "data_remediation_latest.v1.json"
BLOCKERS_FILENAME = "data_blockers.v1.json"

BLOCKER_TYPES = {
    "missing_required_symbol",
    "stale_required_symbol",
    "missing_scan_symbol",
    "provider_unavailable",
    "runtime_truth_unavailable",
    "missing_source_artifact",
    "projection_inconsistency",
    "market_session_mismatch",
    "data_schema_invalid",
    "data_validation_failed",
}

EVENT_TYPES = {
    "DATA_BLOCKER_DETECTED",
    "REMEDIATION_PLAYBOOK_STARTED",
    "PROVIDER_ATTEMPT_RECORDED",
    "DATA_VALIDATION_PASSED",
    "DATA_VALIDATION_FAILED",
    "REMEDIATION_HEALED_BLOCKER",
    "REMEDIATION_STILL_BLOCKED",
    "REMEDIATION_ESCALATED",
}

PLAYBOOK_REFRESH_REQUIRED_SYMBOL = "refresh_required_symbol_data"
PLAYBOOK_REFRESH_RUNTIME_TRUTH = "refresh_runtime_truth"
PLAYBOOK_REBUILD_PROJECTIONS = "rebuild_operator_projections"
PLAYBOOK_MARK_PROVIDER_DATA_NEEDED = "mark_provider_data_needed"
APPROVED_PLAYBOOK_IDS = {
    PLAYBOOK_REFRESH_REQUIRED_SYMBOL,
    PLAYBOOK_REFRESH_RUNTIME_TRUTH,
    PLAYBOOK_REBUILD_PROJECTIONS,
    PLAYBOOK_MARK_PROVIDER_DATA_NEEDED,
}

VIX_ALIAS_ORDER = ("VIX", "^VIX", "vix", "VIXCLS", "$VIX")
APPROVED_SOURCE_PROVIDERS = SUPPORTED_PROVIDERS - {"DISABLED"}


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def blocker_registry_v1() -> dict[str, Any]:
    return {
        "schema_id": "aegis_data_blocker_registry",
        "schema_version": "v1",
        "blocker_types": sorted(BLOCKER_TYPES),
        "required_fields": [
            "blocker_id",
            "blocker_type",
            "affected_symbol",
            "affected_sleeve_id",
            "required_session",
            "observed_session",
            "severity",
            "source_projection_fingerprint",
            "detected_at",
            "remediation_playbook_id",
        ],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    }


def remediation_playbook_registry_v1() -> dict[str, Any]:
    return {
        "schema_id": "aegis_remediation_playbook_registry",
        "schema_version": "v1",
        "playbooks": [
            {
                "playbook_id": PLAYBOOK_REFRESH_REQUIRED_SYMBOL,
                "approved": True,
                "deterministic": True,
                "steps": ["try_local_cache_refresh", "try_configured_primary_provider", "try_configured_fallback_provider", "try_canonical_aliases_in_order", "validate_schema_date_freshness", "write_validated_data_only"],
            },
            {
                "playbook_id": PLAYBOOK_REFRESH_RUNTIME_TRUTH,
                "approved": True,
                "deterministic": True,
                "steps": ["refresh_runtime_truth_artifact", "validate_timestamp_session", "rebuild_projection_facts"],
            },
            {
                "playbook_id": PLAYBOOK_REBUILD_PROJECTIONS,
                "approved": True,
                "deterministic": True,
                "steps": ["rebuild_health_facts", "rebuild_diagnostics", "rebuild_opportunities", "validate_projection_consistency"],
            },
            {
                "playbook_id": PLAYBOOK_MARK_PROVIDER_DATA_NEEDED,
                "approved": True,
                "deterministic": True,
                "steps": ["record_fail_closed_escalation", "do_not_mutate_data"],
            },
        ],
        "ai_generated_playbooks_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    }


def ledger_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / LEDGER_FILENAME


def latest_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / SUMMARY_FILENAME


def blockers_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / BLOCKERS_FILENAME


def _read_json(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def read_remediation_events_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def append_remediation_event_v1(*, truth_root: Path, day_utc: str, event: dict[str, Any]) -> dict[str, Any]:
    event = dict(event)
    event_type = str(event.get("event_type") or "")
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unapproved_remediation_event_type:{event_type}")
    event.setdefault("timestamp", now_utc_v1())
    event.setdefault("provider", "")
    event.setdefault("symbol_alias_attempted", "")
    event.setdefault("expected_session", day_utc)
    event.setdefault("observed_session", "")
    event.setdefault("validation_result", "")
    event.setdefault("before_readiness_status", "")
    event.setdefault("after_readiness_status", "")
    event.setdefault("source_hash", "")
    event.setdefault("audit_refs", [])
    event.setdefault("broker_execution_allowed", False)
    event.setdefault("autonomous_execution_allowed", False)
    event.setdefault("live_trading_allowed", False)
    event["event_id"] = event.get("event_id") or f"data-remediation-event:{stable_hash_v1(event)[:24]}"
    event["content_hash"] = stable_hash_v1(event)
    path = ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return {**event, "path": str(path)}


def latest_remediation_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = latest_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload = _read_json(path)
    if payload:
        return payload
    return {
        "schema_id": "aegis_data_remediation_latest",
        "schema_version": "v1",
        "day_utc": day_utc,
        "attempts": [],
        "events": read_remediation_events_v1(truth_root=truth_root, day_utc=day_utc),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def get_remediation_attempt_v1(*, truth_root: Path, day_utc: str, remediation_attempt_id: str) -> dict[str, Any]:
    events = [row for row in read_remediation_events_v1(truth_root=truth_root, day_utc=day_utc) if row.get("remediation_attempt_id") == remediation_attempt_id]
    return {
        "schema_id": "aegis_data_remediation_attempt",
        "schema_version": "v1",
        "day_utc": day_utc,
        "remediation_attempt_id": remediation_attempt_id,
        "events": events,
        "found": bool(events),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _source_fingerprint(*, paths: tuple[Path | None, ...], payloads: dict[str, Any]) -> str:
    refs: dict[str, Any] = {"payloads": payloads}
    for path in paths:
        if path and path.exists():
            refs[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return stable_hash_v1(refs)


def _symbol_from_item(item: str) -> str:
    text = str(item or "").strip()
    if text.startswith("market.volatility."):
        return normalize_market_symbol_v1(text.rsplit(".", 1)[-1])
    if text.startswith("market.price."):
        return normalize_market_symbol_v1(text.rsplit(".", 1)[-1])
    return normalize_market_symbol_v1(text)


def classify_data_blockers_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    market_path, market = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    registry_path, registry = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    readiness_path, readiness = latest_json_v1(root, "aegis_sleeve_readiness_v1", day_utc, "sleeve_readiness.v1.json")
    diagnostics_path, diagnostics = latest_json_v1(root, "aegis_candidate_generation_diagnostics_v1", day_utc, "candidate_generation_diagnostics.v1.json")
    runtime_path, runtime = latest_json_v1(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    sleeve_required = set(sleeve_required_symbols_from_registry_v1(repo_root=repo_root))
    symbol_to_sleeves: dict[str, list[str]] = {}
    for row in readiness.get("sleeves") or []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "")
        for item in row.get("blocking_inputs") or []:
            symbol = _symbol_from_item(str(item))
            if symbol:
                symbol_to_sleeves.setdefault(symbol, []).append(sleeve_id)
    fingerprint = _source_fingerprint(paths=(market_path, registry_path, readiness_path, diagnostics_path, runtime_path), payloads={"day_utc": day_utc})
    detected_at = now_utc_v1()
    blockers: list[dict[str, Any]] = []

    def add(blocker_type: str, symbol: str = "", sleeve_id: str = "", observed_session: str = "", severity: str = "warning") -> None:
        if blocker_type not in BLOCKER_TYPES:
            return
        playbook = _playbook_for_blocker(blocker_type)
        base = {
            "blocker_type": blocker_type,
            "affected_symbol": normalize_market_symbol_v1(symbol) if symbol else "",
            "affected_sleeve_id": sleeve_id,
            "required_session": day_utc,
            "observed_session": observed_session,
            "severity": severity,
            "source_projection_fingerprint": fingerprint,
            "detected_at": detected_at,
            "remediation_playbook_id": playbook,
        }
        base["blocker_id"] = "data-blocker:" + stable_hash_v1(base)[:24]
        blockers.append(base)

    symbols_payload = market.get("symbols") if isinstance(market.get("symbols"), dict) else {}
    for symbol in canonicalize_symbol_list_v1(market.get("missing_symbols") if isinstance(market.get("missing_symbols"), list) else []):
        affected = sorted(set(symbol_to_sleeves.get(symbol) or ([] if symbol not in sleeve_required else [""])))
        blocker_type = "missing_required_symbol" if symbol in sleeve_required else "missing_scan_symbol"
        if affected:
            for sleeve_id in affected:
                add(blocker_type, symbol=symbol, sleeve_id=sleeve_id, severity="critical" if sleeve_id else "warning")
        else:
            add(blocker_type, symbol=symbol, severity="warning")
    for symbol in canonicalize_symbol_list_v1(market.get("stale_symbols") if isinstance(market.get("stale_symbols"), list) else []):
        observed = str((symbols_payload.get(symbol) or {}).get("market_session_date") or "") if isinstance(symbols_payload.get(symbol), dict) else ""
        affected = sorted(set(symbol_to_sleeves.get(symbol) or ([] if symbol not in sleeve_required else [""])))
        if affected:
            for sleeve_id in affected:
                add("stale_required_symbol", symbol=symbol, sleeve_id=sleeve_id, observed_session=observed, severity="critical" if sleeve_id else "warning")
        else:
            add("missing_scan_symbol", symbol=symbol, observed_session=observed, severity="warning")
    provider_config = market.get("provider_config") if isinstance(market.get("provider_config"), dict) else {}
    if market and provider_config.get("configured") is False:
        add("provider_unavailable", severity="critical")
    runtime_class = str(runtime.get("runtime_truth_classification") or "").upper()
    if runtime and runtime_class in {"MISSING", "BLOCKED", "UNAVAILABLE"}:
        add("runtime_truth_unavailable", severity="critical")
    for item in registry.get("missing_items") or []:
        text = str(item)
        if text.startswith("source."):
            add("missing_source_artifact", severity="warning")
    return {
        "schema_id": "aegis_data_blockers",
        "schema_version": "v1",
        "day_utc": day_utc,
        "blockers": sorted(blockers, key=lambda row: str(row.get("blocker_id"))),
        "blocker_count": len(blockers),
        "source_projection_fingerprint": fingerprint,
        "source_artifacts": [str(path) for path in (market_path, registry_path, readiness_path, diagnostics_path, runtime_path) if path],
        "blocker_registry": blocker_registry_v1(),
        "remediation_playbook_registry": remediation_playbook_registry_v1(),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    }


def write_blockers_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = write_json_v1(blockers_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    return {"json": str(path)}


def _playbook_for_blocker(blocker_type: str) -> str:
    if blocker_type in {"missing_required_symbol", "stale_required_symbol"}:
        return PLAYBOOK_REFRESH_REQUIRED_SYMBOL
    if blocker_type in {"missing_scan_symbol", "provider_unavailable", "market_session_mismatch", "data_schema_invalid", "data_validation_failed"}:
        return PLAYBOOK_MARK_PROVIDER_DATA_NEEDED
    if blocker_type == "runtime_truth_unavailable":
        return PLAYBOOK_REFRESH_RUNTIME_TRUTH
    if blocker_type in {"missing_source_artifact", "projection_inconsistency"}:
        return PLAYBOOK_REBUILD_PROJECTIONS
    return PLAYBOOK_MARK_PROVIDER_DATA_NEEDED


def _existing_final_event(events: list[dict[str, Any]], blocker_id: str, playbook_id: str) -> dict[str, Any]:
    finals = [
        row for row in events
        if row.get("blocker_id") == blocker_id
        and row.get("playbook_id") == playbook_id
        and row.get("event_type") in {"REMEDIATION_HEALED_BLOCKER", "REMEDIATION_STILL_BLOCKED", "REMEDIATION_ESCALATED"}
    ]
    return finals[-1] if finals else {}


def _before_readiness(*, truth_root: Path, day_utc: str) -> str:
    _, readiness = latest_json_v1(Path(truth_root), "aegis_sleeve_readiness_v1", day_utc, "sleeve_readiness.v1.json")
    if not readiness:
        return "UNKNOWN"
    return f"ready={readiness.get('ready_count', 0)} ready_with_warnings={readiness.get('ready_with_warnings_count', 0)} blocked={readiness.get('blocked_count', 0)}"


def provider_attempt_order_v1() -> list[str]:
    config = provider_config_from_env_v1()
    order = ["LOCAL_CACHE", config.primary, config.fallback]
    out: list[str] = []
    for provider in order:
        provider = str(provider or "").strip().upper()
        if not provider or provider == "DISABLED" or provider not in APPROVED_SOURCE_PROVIDERS or provider in out:
            continue
        out.append(provider)
    return out


def alias_order_for_symbol_v1(symbol: str, symbol_map: dict[str, Any], provider: str) -> tuple[str, ...]:
    canonical = normalize_market_symbol_v1(symbol)
    if canonical == "VIX":
        return VIX_ALIAS_ORDER
    aliases = provider_symbol_candidates_v1(symbol_map, canonical, provider)
    return aliases or (canonical,)


def validate_remediated_data_v1(*, symbol: str, provider: str, row: dict[str, Any], expected_session: str) -> dict[str, Any]:
    canonical = normalize_market_symbol_v1(symbol)
    errors: list[str] = []
    if provider not in APPROVED_SOURCE_PROVIDERS:
        errors.append("PROVIDER_NOT_APPROVED")
    if normalize_market_symbol_v1(row.get("canonical_symbol") or row.get("symbol") or "") != canonical:
        errors.append("CANONICAL_SYMBOL_MISMATCH")
    observed = str(row.get("market_session_date") or "")[:10]
    if observed != expected_session:
        errors.append("SESSION_MISMATCH")
    if str(row.get("freshness_status") or "").upper() != "CURRENT":
        errors.append("FRESHNESS_NOT_CURRENT")
    if row.get("last_price") in {None, ""} and row.get("close") in {None, ""}:
        errors.append("VALUE_FIELD_EMPTY")
    if not row.get("source_hash"):
        errors.append("SOURCE_HASH_MISSING")
    if market_data_kind_v1(canonical) == "OHLCV" and row.get("close") in {None, ""}:
        errors.append("OHLCV_CLOSE_EMPTY")
    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "canonical_symbol": canonical,
        "provider": provider,
        "expected_session": expected_session,
        "observed_session": observed,
        "source_hash": str(row.get("source_hash") or ""),
        "deterministic": True,
        "synthetic_data_allowed": False,
        "stale_override_allowed": False,
    }


def _write_validated_local_cache_row(*, truth_root: Path, symbol: str, row: dict[str, Any], day_utc: str) -> Path:
    canonical = normalize_market_symbol_v1(symbol)
    path = Path(truth_root).expanduser().resolve() / "market_data_snapshot_v1" / canonical / f"{day_utc[:4]}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    close = row.get("close") if row.get("close") is not None else row.get("last_price")
    cache_row = {
        "symbol": canonical,
        "timestamp_utc": row.get("data_timestamp_utc") or f"{day_utc}T21:00:00Z",
        "open": row.get("open") if row.get("open") is not None else close,
        "high": row.get("high") if row.get("high") is not None else close,
        "low": row.get("low") if row.get("low") is not None else close,
        "close": close,
        "volume": row.get("volume") if row.get("volume") is not None else 0,
        "source_provider": row.get("provider"),
        "provider_symbol": row.get("provider_symbol"),
        "source_hash": row.get("source_hash"),
        "accepted_by": "aegis_data_remediation_v1",
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(cache_row, sort_keys=True) + "\n")
    return path


def _run_cmd(repo_root: Path, args: list[str], *, run_context: RunContext, timeout_seconds: int | None = None) -> dict[str, Any]:
    started = now_utc_v1()
    command = [sys.executable, *args]
    if timeout_seconds is None:
        timeout_seconds = int(os.environ.get("AEGIS_PROJECTION_REBUILD_COMMAND_TIMEOUT_SECONDS") or "8")
    try:
        proc = subprocess.run(
            command,
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=subprocess_env_v1(run_context),
            check=False,
        )
        return {
            "command": " ".join(command),
            "exit_code": proc.returncode,
            "stdout": proc.stdout.strip()[-2000:],
            "stderr": proc.stderr.strip()[-2000:],
            "started_at": started,
            "completed_at": now_utc_v1(),
            "timeout_seconds": timeout_seconds,
            "timed_out": False,
            "run_context": run_context.to_dict(),
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": " ".join(command),
            "exit_code": 124,
            "stdout": str(exc.stdout or "")[-2000:],
            "stderr": str(exc.stderr or "")[-2000:],
            "started_at": started,
            "completed_at": now_utc_v1(),
            "timeout_seconds": timeout_seconds,
            "timed_out": True,
            "run_context": run_context.to_dict(),
        }


def rebuild_data_projections_v1(*, truth_root: Path, repo_root: Path, day_utc: str, run_context: RunContext | None = None) -> dict[str, Any]:
    context = run_context or run_context_from_env_v1("rebuild_operator_projections", default_allow_market_data_refresh=False, default_allow_self_heal=False)
    allowed, reason = step_allowed_v1(context, "projection_rebuild")
    if not allowed:
        return {"commands": [], "ok": False, "skipped": True, "skip_reason": reason, "run_context": context.to_dict()}
    child = child_run_context_v1(
        context,
        "projection_rebuild",
        allow_market_data_refresh=False,
        allow_self_heal=False,
        allow_projection_rebuild=False,
        add_step="projection_rebuild",
    )
    # Projection rebuild is intentionally read-only with respect to provider fetches and diagnostics.
    commands = [
        _run_cmd(repo_root, ["ops/tools/build_aegis_data_registry_v1.py", "--truth_root", str(truth_root), "--day", day_utc], run_context=child),
        _run_cmd(repo_root, ["ops/tools/build_aegis_sleeve_input_contracts_v1.py", "--truth_root", str(truth_root), "--day", day_utc], run_context=child),
        _run_cmd(repo_root, ["ops/tools/build_aegis_sleeve_readiness_v1.py", "--truth_root", str(truth_root), "--day", day_utc], run_context=child),
        _run_cmd(repo_root, ["ops/tools/build_aegis_canonical_operator_state_v1.py", "--truth_root", str(truth_root), "--day", day_utc], run_context=child),
    ]
    return {"commands": commands, "ok": all(row["exit_code"] == 0 for row in commands), "run_context": child.to_dict(), "read_only_projection_rebuild": True}


def run_refresh_required_symbol_playbook_v1(*, truth_root: Path, repo_root: Path, day_utc: str, blocker: dict[str, Any], remediation_attempt_id: str, before_readiness: str) -> dict[str, Any]:
    symbol = normalize_market_symbol_v1(blocker.get("affected_symbol") or "")
    if not symbol:
        return {"status": "ESCALATED", "reason": "AFFECTED_SYMBOL_MISSING", "healed": False, "provider_attempts": []}
    symbol_map = build_symbol_map_v1(repo_root=repo_root, day_utc=day_utc)
    config = provider_config_from_env_v1()
    attempts: list[dict[str, Any]] = []
    for provider in provider_attempt_order_v1():
        aliases = alias_order_for_symbol_v1(symbol, symbol_map, provider)
        result = _fetch_provider(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=[symbol], symbol_map=symbol_map, config=config)
        row = result.symbols.get(symbol) if isinstance(result.symbols, dict) else None
        validation = validate_remediated_data_v1(symbol=symbol, provider=provider, row=row or {}, expected_session=day_utc)
        alias_used = str((row or {}).get("provider_symbol") or (aliases[0] if aliases else ""))
        event_type = "DATA_VALIDATION_PASSED" if validation["status"] == "PASS" else "DATA_VALIDATION_FAILED"
        append_remediation_event_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            event={
                "event_type": "PROVIDER_ATTEMPT_RECORDED",
                "remediation_attempt_id": remediation_attempt_id,
                "blocker_id": blocker["blocker_id"],
                "playbook_id": PLAYBOOK_REFRESH_REQUIRED_SYMBOL,
                "provider": provider,
                "symbol_alias_attempted": ",".join(aliases),
                "expected_session": day_utc,
                "observed_session": validation.get("observed_session", ""),
                "validation_result": result.request_status,
                "before_readiness_status": before_readiness,
                "source_hash": validation.get("source_hash", ""),
                "audit_refs": list(result.provider_results),
            },
        )
        append_remediation_event_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            event={
                "event_type": event_type,
                "remediation_attempt_id": remediation_attempt_id,
                "blocker_id": blocker["blocker_id"],
                "playbook_id": PLAYBOOK_REFRESH_REQUIRED_SYMBOL,
                "provider": provider,
                "symbol_alias_attempted": alias_used,
                "expected_session": day_utc,
                "observed_session": validation.get("observed_session", ""),
                "validation_result": validation["status"],
                "before_readiness_status": before_readiness,
                "source_hash": validation.get("source_hash", ""),
                "audit_refs": [validation],
            },
        )
        attempts.append({"provider": provider, "aliases": list(aliases), "request_status": result.request_status, "validation": validation})
        if validation["status"] == "PASS" and row:
            accepted_path = _write_validated_local_cache_row(truth_root=truth_root, symbol=symbol, row=row, day_utc=day_utc)
            return {"status": "HEALED", "healed": True, "provider": provider, "accepted_path": str(accepted_path), "source_hash": validation["source_hash"], "accepted_row": row, "provider_attempts": attempts}
    return {"status": "STILL_BLOCKED", "healed": False, "provider_attempts": attempts}


def _apply_accepted_market_data_to_latest_report(*, truth_root: Path, day_utc: str, symbol: str, row: dict[str, Any]) -> dict[str, Any]:
    path, report = latest_json_v1(truth_root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    if not path or not isinstance(report, dict) or not isinstance(row, dict) or not row:
        return {"updated": False, "reason": "MARKET_DATA_REPORT_MISSING"}
    canonical = normalize_market_symbol_v1(symbol)
    symbols = report.get("symbols") if isinstance(report.get("symbols"), dict) else {}
    symbols[canonical] = dict(row)
    report["symbols"] = symbols
    fetched = {str(item).upper() for item in report.get("fetched_symbols", []) if str(item)}
    fetched.add(canonical)
    report["fetched_symbols"] = sorted(fetched)
    report["missing_symbols"] = [item for item in report.get("missing_symbols", []) if str(item).upper() != canonical]
    report["stale_symbols"] = [item for item in report.get("stale_symbols", []) if str(item).upper() != canonical]
    item_ids = {canonical, f"market.price.{canonical}", f"market.volatility.{canonical}"}
    report["missing_fields"] = [item for item in report.get("missing_fields", []) if str(item) not in item_ids]
    report["stale_fields"] = [item for item in report.get("stale_fields", []) if str(item) not in item_ids]
    report["status"] = "CURRENT" if not report.get("missing_fields") and not report.get("stale_fields") else "PARTIAL"
    report["usable_for_candidate_generation"] = bool(report["status"] == "CURRENT")
    report["remediated_symbols"] = sorted({*(str(item).upper() for item in report.get("remediated_symbols", []) if str(item)), canonical})
    report["remediation_applied_at_utc"] = now_utc_v1()
    write_json_v1(Path(path), report)
    return {"updated": True, "path": str(path), "symbol": canonical, "status": report["status"]}


def _blocker_priority_v1(blocker: dict[str, Any]) -> tuple[int, str, str]:
    blocker_type = str(blocker.get("blocker_type") or "")
    symbol = normalize_market_symbol_v1(blocker.get("affected_symbol") or "")
    if blocker_type in {"missing_required_symbol", "stale_required_symbol"} and symbol == "VIX":
        rank = 0
    elif blocker_type in {"missing_required_symbol", "stale_required_symbol"}:
        rank = 1
    elif blocker_type == "runtime_truth_unavailable":
        rank = 2
    elif blocker_type == "missing_scan_symbol":
        rank = 4
    else:
        rank = 3
    return (rank, symbol, str(blocker.get("blocker_id") or ""))


def run_data_remediation_v1(*, truth_root: Path, repo_root: Path, day_utc: str, force: bool = False, requested_playbook_id: str = "", run_context: RunContext | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    context = run_context or run_context_from_env_v1("self-heal-data")
    allowed, guard_reason = step_allowed_v1(context, "self_heal")
    if not allowed:
        payload = latest_remediation_v1(truth_root=root, day_utc=day_utc)
        payload.update({"skipped": True, "skip_reason": guard_reason, "run_context": context.to_dict(), "broker_execution_allowed": False, "autonomous_execution_allowed": False})
        return payload
    context = child_run_context_v1(context, "self-heal-data", allow_market_data_refresh=False, allow_self_heal=False, add_step="self_heal")
    blockers_payload = classify_data_blockers_v1(truth_root=root, repo_root=repo, day_utc=day_utc)
    previous_latest = latest_remediation_v1(truth_root=root, day_utc=day_utc)
    write_blockers_v1(truth_root=root, day_utc=day_utc, payload=blockers_payload)
    events_before = read_remediation_events_v1(truth_root=root, day_utc=day_utc)
    attempts_summary: list[dict[str, Any]] = []
    provider_remediation_count = 0
    provider_remediation_limit = int(os.environ.get("AEGIS_SELF_HEAL_PROVIDER_BLOCKER_LIMIT") or "1")
    before_readiness = _before_readiness(truth_root=root, day_utc=day_utc)
    for blocker in sorted(blockers_payload["blockers"], key=_blocker_priority_v1):
        playbook_id = str(requested_playbook_id or blocker.get("remediation_playbook_id") or "")
        if playbook_id not in APPROVED_PLAYBOOK_IDS:
            continue
        if requested_playbook_id and playbook_id != requested_playbook_id:
            continue
        prior_final = _existing_final_event(events_before, str(blocker["blocker_id"]), playbook_id)
        if prior_final and not force:
            attempts_summary.append({"blocker_id": blocker["blocker_id"], "playbook_id": playbook_id, "status": "REUSED_PRIOR_RESULT", "prior_event_id": prior_final.get("event_id"), "healed": prior_final.get("event_type") == "REMEDIATION_HEALED_BLOCKER"})
            continue
        remediation_attempt_id = f"data-remediation:{day_utc}:{stable_hash_v1({'blocker': blocker, 'playbook_id': playbook_id})[:16]}"
        append_remediation_event_v1(truth_root=root, day_utc=day_utc, event={"event_type": "DATA_BLOCKER_DETECTED", "remediation_attempt_id": remediation_attempt_id, "blocker_id": blocker["blocker_id"], "playbook_id": playbook_id, "expected_session": day_utc, "observed_session": blocker.get("observed_session", ""), "validation_result": "DETECTED", "before_readiness_status": before_readiness, "audit_refs": [blocker]})
        append_remediation_event_v1(truth_root=root, day_utc=day_utc, event={"event_type": "REMEDIATION_PLAYBOOK_STARTED", "remediation_attempt_id": remediation_attempt_id, "blocker_id": blocker["blocker_id"], "playbook_id": playbook_id, "expected_session": day_utc, "before_readiness_status": before_readiness, "audit_refs": [blocker]})
        if playbook_id == PLAYBOOK_REFRESH_REQUIRED_SYMBOL:
            if provider_remediation_count >= provider_remediation_limit:
                result = {"status": "ESCALATED", "healed": False, "reason": "SELF_HEAL_PROVIDER_REMEDIATION_BUDGET_EXHAUSTED", "provider_attempts": []}
            else:
                provider_remediation_count += 1
                result = run_refresh_required_symbol_playbook_v1(truth_root=root, repo_root=repo, day_utc=day_utc, blocker=blocker, remediation_attempt_id=remediation_attempt_id, before_readiness=before_readiness)
        elif playbook_id == PLAYBOOK_REFRESH_RUNTIME_TRUTH:
            command = _run_cmd(repo, ["ops/tools/run_aegis_runtime_truth_kernel_v1.py", "--truth_root", str(root), "--day", day_utc], run_context=context)
            result = {"status": "HEALED" if command["exit_code"] == 0 else "STILL_BLOCKED", "healed": command["exit_code"] == 0, "commands": [command]}
        elif playbook_id == PLAYBOOK_REBUILD_PROJECTIONS:
            rebuild = rebuild_data_projections_v1(truth_root=root, repo_root=repo, day_utc=day_utc, run_context=context)
            result = {"status": "HEALED" if rebuild["ok"] else "STILL_BLOCKED", "healed": bool(rebuild["ok"]), "projection_rebuild": rebuild}
        else:
            result = {"status": "ESCALATED", "healed": False, "reason": "APPROVED_FAIL_CLOSED_ESCALATION"}
        if result.get("healed") and result.get("accepted_row"):
            result["market_data_report_update"] = _apply_accepted_market_data_to_latest_report(
                truth_root=root,
                day_utc=day_utc,
                symbol=str(blocker.get("affected_symbol") or ""),
                row=dict(result.get("accepted_row") or {}),
            )
        rebuild = result.get("projection_rebuild", {}) if playbook_id == PLAYBOOK_REBUILD_PROJECTIONS else {}
        after_readiness = _before_readiness(truth_root=root, day_utc=day_utc)
        post_blockers = classify_data_blockers_v1(truth_root=root, repo_root=repo, day_utc=day_utc)
        still_active = any(row.get("blocker_type") == blocker.get("blocker_type") and row.get("affected_symbol") == blocker.get("affected_symbol") and row.get("affected_sleeve_id") == blocker.get("affected_sleeve_id") for row in post_blockers.get("blockers", []))
        final_type = "REMEDIATION_HEALED_BLOCKER" if result.get("healed") and not still_active else ("REMEDIATION_ESCALATED" if result.get("status") == "ESCALATED" else "REMEDIATION_STILL_BLOCKED")
        append_remediation_event_v1(truth_root=root, day_utc=day_utc, event={"event_type": final_type, "remediation_attempt_id": remediation_attempt_id, "blocker_id": blocker["blocker_id"], "playbook_id": playbook_id, "provider": str(result.get("provider") or ""), "expected_session": day_utc, "observed_session": str(result.get("observed_session") or ""), "validation_result": str(result.get("status") or ""), "before_readiness_status": before_readiness, "after_readiness_status": after_readiness, "source_hash": str(result.get("source_hash") or ""), "audit_refs": [result, {"projection_rebuild": rebuild, "post_blocker_count": len(post_blockers.get("blockers", []))}]})
        attempts_summary.append({"remediation_attempt_id": remediation_attempt_id, "blocker_id": blocker["blocker_id"], "playbook_id": playbook_id, "status": final_type, "healed": final_type == "REMEDIATION_HEALED_BLOCKER", "result": result})
    final_projection_rebuild = rebuild_data_projections_v1(truth_root=root, repo_root=repo, day_utc=day_utc, run_context=context) if attempts_summary else {}
    if attempts_summary:
        final_attempts = attempts_summary
    elif not blockers_payload["blockers"] and isinstance(previous_latest.get("attempts"), list):
        final_attempts = [
            {
                "blocker_id": row.get("blocker_id"),
                "playbook_id": row.get("playbook_id"),
                "status": "REUSED_PRIOR_RESULT",
                "prior_status": row.get("status"),
                "healed": bool(row.get("healed")),
            }
            for row in previous_latest.get("attempts", [])
            if isinstance(row, dict)
        ]
    else:
        final_attempts = []
    payload = {
        "schema_id": "aegis_data_remediation_latest",
        "schema_version": "v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "blockers": blockers_payload["blockers"],
        "blocker_count": blockers_payload["blocker_count"],
        "attempts": final_attempts,
        "attempt_count": len(final_attempts),
        "events": read_remediation_events_v1(truth_root=root, day_utc=day_utc),
        "blocker_registry": blocker_registry_v1(),
        "remediation_playbook_registry": remediation_playbook_registry_v1(),
        "ai_role": {"may_summarize": True, "may_choose_unapproved_provider": False, "may_create_values": False, "may_change_freshness_gates": False, "may_mutate_symbol_mappings": False, "may_mark_blocker_healed": False},
        "governance": {"fake_data_allowed": False, "synthetic_values_allowed": False, "stale_override_allowed": False, "manual_value_entry_allowed": False, "provider_mapping_mutation_allowed": False, "sleeve_mutation_allowed": False, "broker_execution_allowed": False, "live_trading_allowed": False, "autonomous_trading_allowed": False, "order_routing_allowed": False},
        "run_context": context.to_dict(),
        "read_only_projection_rebuild": True,
        "projection_rebuild": final_projection_rebuild,
        "provider_remediation_limit": provider_remediation_limit,
        "provider_remediation_count": provider_remediation_count,
    }
    write_json_v1(latest_path_v1(truth_root=root, day_utc=day_utc), payload)
    return payload
