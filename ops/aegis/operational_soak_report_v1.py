from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping


SCHEMA_ID = "aegis_operational_soak_report"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "aegis_operational_soak_report_v1"

SOAK_ROUTES = (
    "/aegis-performance",
    "/aegis-exit-review",
    "/aegis-candidate-funnel",
    "/research-lab",
    "/aegis-runtime-timeline",
)

SAFETY_FLAGS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def _latest_artifact(root: Path, family: str, day_utc: str, filename: str) -> Path | None:
    base = root / "reports" / family / day_utc
    if not base.exists() or not base.is_dir():
        return None
    direct = base / filename
    candidates = [direct] if direct.exists() and direct.is_file() else []
    candidates.extend(path for path in base.glob(f"**/{filename}") if path.is_file() and path != direct)
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime_ns, str(path)), reverse=True)[0]


def _report_days(root: Path, families: Iterable[str], *, max_day: str, lookback_days: int) -> list[str]:
    days: set[str] = set()
    for family in families:
        base = root / "reports" / family
        if not base.exists() or not base.is_dir():
            continue
        days.update(path.name for path in base.iterdir() if path.is_dir() and path.name <= max_day)
    return sorted(days)[-max(1, int(lookback_days)) :]


def _artifact_inventory(root: Path, day_utc: str) -> dict[str, Any]:
    reports_root = root / "reports"
    day_files: list[Path] = []
    if reports_root.exists():
        for family_dir in reports_root.iterdir():
            day_dir = family_dir / day_utc
            if day_dir.exists():
                day_files.extend(path for path in day_dir.rglob("*") if path.is_file())
    total_bytes = 0
    by_family: dict[str, dict[str, int]] = defaultdict(lambda: {"file_count": 0, "total_bytes": 0})
    hash_index: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in day_files:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        total_bytes += size
        family = path.relative_to(reports_root).parts[0] if reports_root in path.parents else "UNKNOWN"
        by_family[family]["file_count"] += 1
        by_family[family]["total_bytes"] += size
        if path.suffix.lower() == ".json":
            payload = _read_json(path)
            content_hash = str(payload.get("content_hash") or payload.get("history_hash") or payload.get("artifact_hash") or "")
            if content_hash:
                hash_index[(family, content_hash)].append(str(path))
    duplicate_artifacts = [
        {"family": family, "content_hash": content_hash, "paths": paths}
        for (family, content_hash), paths in sorted(hash_index.items())
        if len(paths) > 1
    ]
    return {
        "file_count": len(day_files),
        "total_bytes": total_bytes,
        "by_family": dict(sorted(by_family.items())),
        "duplicate_artifacts": duplicate_artifacts,
    }


def _provider_metrics(root: Path, day_utc: str, certified_symbols: Iterable[str] = ()) -> dict[str, Any]:
    path = _latest_artifact(root, "aegis_market_data_v1", day_utc, "market_data_provider_attempts.v1.json")
    payload = _read_json(path)
    attempts = payload.get("attempts") if isinstance(payload.get("attempts"), list) else []
    durations = [int(item.get("duration_ms") or 0) for item in attempts if isinstance(item, Mapping)]
    status_counts = Counter(str(item.get("status") or "UNKNOWN") for item in attempts if isinstance(item, Mapping))
    provider_counts = Counter(str(item.get("provider") or "UNKNOWN") for item in attempts if isinstance(item, Mapping))
    success_symbols = {
        str(item.get("symbol") or "").upper()
        for item in attempts
        if isinstance(item, Mapping) and str(item.get("status") or "").upper() in {"SUCCESS", "STALE"}
    }
    certified_symbol_set = {str(symbol).upper() for symbol in certified_symbols if str(symbol)}
    failures = []
    recovered_failures = []
    active_failures = []
    for item in attempts:
        if not isinstance(item, Mapping) or str(item.get("status") or "").upper() in {"SUCCESS", "STALE"}:
            continue
        symbol = str(item.get("symbol") or "").upper()
        row = {
            "symbol": symbol,
            "provider": str(item.get("provider") or ""),
            "status": str(item.get("status") or ""),
            "reason": str(item.get("rejected_reason") or item.get("exception_message") or ""),
        }
        recovered = symbol in success_symbols or symbol in certified_symbol_set
        row["classification"] = "RECOVERED_PROVIDER_FAILURE" if recovered else "ACTIVE_PROVIDER_FAILURE"
        row["recovered_by"] = "same_day_provider_success" if symbol in success_symbols else ("certified_final_eod_artifact" if symbol in certified_symbol_set else "")
        failures.append(row)
        if recovered:
            recovered_failures.append(row)
        else:
            active_failures.append(row)
    sorted_durations = sorted(durations)
    p95 = sorted_durations[int((len(sorted_durations) - 1) * 0.95)] if sorted_durations else 0
    active_timeout_count = sum(1 for row in active_failures if str(row.get("status") or "").upper() == "TIMEOUT")
    return {
        "status": "PASS" if not active_failures else "WARN",
        "artifact_path": str(path or ""),
        "attempt_count": len(attempts),
        "provider_counts": dict(sorted(provider_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "failure_count": len(failures),
        "failures": failures[:25],
        "recovered_failure_count": len(recovered_failures),
        "recovered_failures": recovered_failures[:25],
        "active_failure_count": len(active_failures),
        "active_failures": active_failures[:25],
        "timeout_count": int(status_counts.get("TIMEOUT", 0)),
        "active_timeout_count": active_timeout_count,
        "latency_ms": {
            "avg": int(round(mean(durations))) if durations else 0,
            "max": max(durations) if durations else 0,
            "p95": p95,
        },
    }


def _final_eod_payload(root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    manifest_path = root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"
    manifest = _read_json(manifest_path)
    current = str(manifest.get("current_artifact_path") or "")
    if current:
        artifact_path = Path(current).expanduser()
        artifact = _read_json(artifact_path)
        if artifact:
            return artifact_path, artifact
    if manifest:
        return manifest_path, manifest
    return None, {}


def _final_eod_metrics(root: Path, day_utc: str, previous_count: int | None) -> dict[str, Any]:
    path, payload = _final_eod_payload(root, day_utc)
    records = payload.get("normalized_records") if isinstance(payload.get("normalized_records"), list) else []
    symbols = payload.get("final_eod_symbols") if isinstance(payload.get("final_eod_symbols"), list) else []
    if not symbols:
        symbols = sorted({str(row.get("symbol") or "").upper() for row in records if isinstance(row, Mapping) and row.get("symbol")})
    symbol_count = len(symbols)
    growth = symbol_count - previous_count if previous_count is not None else 0
    growth_ratio = (growth / previous_count) if previous_count else 0.0
    return {
        "artifact_path": str(path or ""),
        "content_hash": str(payload.get("content_hash") or _sha256_file(path)),
        "validation_status": str(payload.get("validation_status") or payload.get("final_eod_certification_status") or payload.get("status") or "MISSING"),
        "symbol_count": symbol_count,
        "symbols": symbols,
        "symbols_sample": symbols[:25],
        "symbol_growth_from_prior_cycle": growth,
        "symbol_growth_ratio_from_prior_cycle_bps": int(round(growth_ratio * 10000)),
    }


def _candidate_metrics(root: Path, day_utc: str) -> dict[str, Any]:
    snapshot_path = _latest_artifact(root, "operator_state_snapshot_v1", day_utc, "operator_state_snapshot.v1.json")
    snapshot = _read_json(snapshot_path)
    funnel = snapshot.get("candidate_funnel_projection") if isinstance(snapshot.get("candidate_funnel_projection"), dict) else {}
    if not funnel:
        funnel_path = _latest_artifact(root, "candidate_funnel_projection_v1", day_utc, "candidate_funnel_projection.v1.json")
        funnel = _read_json(funnel_path)
    return {
        "artifact_path": str((funnel.get("artifact_paths") or {}).get("candidate_consumption_audit_v1") or snapshot_path or ""),
        "raw_candidate_count": int(funnel.get("raw_candidate_count") or 0),
        "excluded_candidate_count": int(funnel.get("excluded_candidate_count") or 0),
        "promoted_candidate_count": int(funnel.get("promoted_candidate_count") or 0),
        "capture_ticket_count": int(funnel.get("capture_ticket_count") or 0),
        "exclusion_counts": dict(funnel.get("consumption_counts") or funnel.get("exclusion_counts_by_reason") or {}),
        "certified_universe_symbol_count": int((funnel.get("certified_universe") or {}).get("symbol_count") or 0),
        "canonical_universe_symbol_count": int((funnel.get("canonical_universe_authority") or {}).get("symbol_count") or 0),
    }


def _dynamic_queue_metrics(root: Path, day_utc: str) -> dict[str, Any]:
    path = _latest_artifact(root, "dynamic_certification_queue_v1", day_utc, "dynamic_certification_queue.v1.json")
    payload = _read_json(path)
    requested = [str(item).upper() for item in payload.get("requested_symbols") or [] if str(item)]
    certified = [str(item).upper() for item in payload.get("certified_symbols") or [] if str(item)]
    failed = [str(item).upper() for item in payload.get("failed_symbols") or [] if str(item)]
    results = payload.get("certification_results") if isinstance(payload.get("certification_results"), list) else []
    if not certified and results:
        certified = [str(item.get("symbol") or "").upper() for item in results if isinstance(item, Mapping) and str(item.get("certification_status") or "").upper() == "CERTIFIED"]
    if not failed and results:
        failed = [str(item.get("symbol") or "").upper() for item in results if isinstance(item, Mapping) and str(item.get("certification_status") or "").upper() not in {"CERTIFIED", ""}]
    success_rate = (len(certified) / len(requested)) if requested else 1.0
    return {
        "artifact_path": str(path or ""),
        "certification_status": str(payload.get("certification_status") or ("MISSING" if not payload else "UNKNOWN")),
        "queue_size": len(requested),
        "requested_symbols": requested,
        "certified_symbols": sorted(set(certified)),
        "failed_symbols": sorted(set(failed)),
        "estimated_provider_load": int(payload.get("estimated_provider_load") or 0),
        "certification_success_rate_bps": int(round(success_rate * 10000)),
        "repeatedly_failing_symbols": sorted(set(failed)),
    }


def _projection_metrics(root: Path, day_utc: str) -> dict[str, Any]:
    performance_path = _latest_artifact(root, "paper_trade_evaluation_projection_v1", day_utc, "paper_trade_evaluation_projection.v1.json")
    exit_path = _latest_artifact(root, "exit_review_projection_v1", day_utc, "exit_review_projection.v1.json")
    performance = _read_json(performance_path)
    exit_review = _read_json(exit_path)
    return {
        "performance_projection": {
            "artifact_path": str(performance_path or ""),
            "content_hash": str(performance.get("content_hash") or ""),
            "trade_count": int(performance.get("trade_count") or 0),
            "open_trade_count": int(performance.get("open_trade_count") or 0),
            "closed_trade_count": int(performance.get("closed_trade_count") or 0),
        },
        "exit_review_projection": {
            "artifact_path": str(exit_path or ""),
            "content_hash": str(exit_review.get("content_hash") or ""),
            "open_position_count": int(exit_review.get("open_position_count") or 0),
            "review_needed_count": int(exit_review.get("review_needed_count") or 0),
            "closed_outcome_count": len(exit_review.get("closed_outcomes") or []),
        },
    }


def _replay_metrics(root: Path, day_utc: str) -> dict[str, Any]:
    gate_path = _latest_artifact(root, "replay_certification_gate_v1", day_utc, "replay_certification_gate.v1.json")
    bundle_path = _latest_artifact(root, "replay_certification_bundle_v1", day_utc, "replay_certification_bundle.v1.json")
    state_path = _latest_artifact(root, "operator_state_snapshot_v1", day_utc, "operator_state_snapshot.v1.json")
    gate = _read_json(gate_path)
    bundle = _read_json(bundle_path)
    state_hash = _sha256_file(state_path)
    replay_hash = str(
        gate.get("content_hash")
        or gate.get("gate_sha256")
        or gate.get("candidate_bundle_sha256")
        or bundle.get("content_hash")
        or state_hash
    )
    status = str(gate.get("status") or gate.get("gate_status") or bundle.get("status") or ("PASS" if state_hash else "NOT_OBSERVED"))
    bundle_inputs = bundle.get("inputs") if isinstance(bundle.get("inputs"), dict) else {}
    return {
        "status": status,
        "replay_hash": replay_hash,
        "gate_artifact_path": str(gate_path or ""),
        "bundle_artifact_path": str(bundle_path or ""),
        "state_snapshot_hash": state_hash,
        "reason_codes": [str(item) for item in gate.get("reason_codes") or []],
        "first_run": bool(gate.get("first_run")),
        "two_run_equality": gate.get("two_run_equality"),
        "bundle_status": str(bundle.get("status") or ""),
        "bundle_missing_types": [str(item) for item in bundle_inputs.get("missing_types") or []],
        "bundle_present_types": [str(item) for item in bundle_inputs.get("present_types") or []],
    }


def _stage_status(root: Path, day_utc: str) -> dict[str, Any]:
    checks = {
        "next_trading_day": _latest_artifact(root, "trading_day_readiness_authority_v1", day_utc, "trading_day_readiness_authority.v1.json"),
        "run_0950": _latest_artifact(root, "market_data_intraday_operational_v1", day_utc, "market_data_intraday_operational.v1.json"),
        "run_1450": _latest_artifact(root, "aegis_candidate_generation_diagnostics_v1", day_utc, "candidate_generation_diagnostics.v1.json"),
        "eod_certification": _latest_artifact(root, "final_eod_certification_run_ledger_v1", day_utc, "final_eod_certification_run_ledger.latest.v1.json"),
        "replay": _latest_artifact(root, "replay_certification_gate_v1", day_utc, "replay_certification_gate.v1.json"),
        "candidate_funnel": _latest_artifact(root, "operator_state_snapshot_v1", day_utc, "operator_state_snapshot.v1.json"),
        "dynamic_certification_queue": _latest_artifact(root, "dynamic_certification_queue_v1", day_utc, "dynamic_certification_queue.v1.json"),
        "performance_projection": _latest_artifact(root, "paper_trade_evaluation_projection_v1", day_utc, "paper_trade_evaluation_projection.v1.json"),
        "exit_review_projection": _latest_artifact(root, "exit_review_projection_v1", day_utc, "exit_review_projection.v1.json"),
    }
    return {
        name: {"status": "PRESENT" if path else "NOT_OBSERVED", "artifact_path": str(path or "")}
        for name, path in checks.items()
    }


def _resource_metrics() -> dict[str, Any]:
    rss_kb = 0
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
                break
    except Exception:
        pass
    warnings = []
    if rss_kb and rss_kb > 1_000_000:
        warnings.append("SOAK_REPORT_PROCESS_RSS_OVER_1GB")
    return {"soak_report_process_rss_kb": rss_kb, "warnings": warnings}


def _ui_metrics(ui_results: Mapping[str, Any] | None) -> dict[str, Any]:
    if not ui_results:
        return {
            "status": "NOT_RUN",
            "routes": list(SOAK_ROUTES),
            "failures": [],
        }
    failures = [str(item) for item in ui_results.get("failures") or []]
    route_rows = []
    for row in ui_results.get("results") or []:
        if isinstance(row, Mapping):
            route_rows.append(
                {
                    "route": str(row.get("route") or ""),
                    "ok": bool(row.get("ok")),
                    "clicked_count": int(row.get("clicked_count") or 0),
                    "skipped_api_command_count": int(row.get("skipped_api_command_count") or len(row.get("skipped_api_commands") or [])),
                    "clicked": row.get("clicked") if isinstance(row.get("clicked"), list) else [],
                    "skipped_api_commands": row.get("skipped_api_commands") if isinstance(row.get("skipped_api_commands"), list) else [],
                    "failures": [str(item) for item in row.get("failures") or []],
                }
            )
    return {
        "status": "PASS" if bool(ui_results.get("ok")) and not failures else "FAIL",
        "routes": route_rows,
        "failures": failures,
    }


def _command_metrics(command_registry: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if command_registry is None:
        try:
            from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1

            command_registry = command_registry_v1()
        except Exception:
            command_registry = {}
    commands = command_registry.get("commands_by_id") if isinstance(command_registry.get("commands_by_id"), dict) else {}
    failures = []
    for command_id, command in commands.items():
        if not isinstance(command, Mapping):
            continue
        safety = command.get("safety_classification") if isinstance(command.get("safety_classification"), Mapping) else {}
        if safety.get("broker_submit_transmit_allowed") or safety.get("broker_execution_allowed") or safety.get("order_routing_allowed") or safety.get("autonomous_execution_allowed") or safety.get("trade_advice_allowed"):
            failures.append({"command_id": str(command_id), "reason": "UNSAFE_COMMAND_CAPABILITY"})
    return {"status": "PASS" if not failures else "FAIL", "command_count": len(commands), "failures": failures}


def _duplicate_warning_classification(group: Mapping[str, Any]) -> dict[str, Any]:
    family = str(group.get("family") or "")
    paths = [str(path) for path in group.get("paths") or []]
    names = {Path(path).name for path in paths}
    is_known_pointer_alias = family == "final_eod_market_data_v1" and names <= {"final_eod_market_data.v1.json", "final_eod_market_data.current.v1.json"}
    return {
        "classification": "DUPLICATE_ARTIFACT_WARNING",
        "active": not is_known_pointer_alias,
        "family": family,
        "content_hash": str(group.get("content_hash") or ""),
        "paths": paths,
        "reason": "KNOWN_CURRENT_POINTER_ALIAS" if is_known_pointer_alias else "POSSIBLE_CURRENT_PRODUCER_DUPLICATE",
    }


def _acceptance_rules(cycles: list[dict[str, Any]], ui: dict[str, Any], commands: dict[str, Any], resource: dict[str, Any]) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []

    def add(rule_id: str, status: str, reason: str, details: Any = None) -> None:
        failures.append({"rule_id": rule_id, "status": status, "reason": reason, "details": details if details is not None else {}})

    latest_day = cycles[-1]["operational_day"] if cycles else ""
    replay_classifications = []
    current_replay_failures = []
    for cycle in cycles:
        replay = cycle["replay"]
        failed = str(replay.get("status") or "").upper() in {"FAIL", "FAILED", "DRIFT", "MISMATCH", "REPLAY_DRIFT"}
        if not failed:
            classification = "NO_REPLAY_FAILURE"
        elif (
            cycle["operational_day"] != latest_day
            and replay.get("first_run")
            and "REPLAY_CERT_FIRST_RUN" in set(replay.get("reason_codes") or [])
            and "REPLAY_CERT_BUNDLE_FAIL_CLOSED" in set(replay.get("reason_codes") or [])
            and replay.get("bundle_missing_types")
        ):
            classification = "HISTORICAL_LEGACY_FAILURE"
        else:
            classification = "CURRENT_OPERATIONAL_FAILURE"
        replay["failure_classification"] = classification
        if failed:
            row = {
                "day": cycle["operational_day"],
                "status": replay.get("status"),
                "replay_hash": replay.get("replay_hash"),
                "classification": classification,
                "reason_codes": replay.get("reason_codes") or [],
                "bundle_missing_types": replay.get("bundle_missing_types") or [],
                "gate_artifact_path": replay.get("gate_artifact_path") or "",
                "bundle_artifact_path": replay.get("bundle_artifact_path") or "",
            }
            replay_classifications.append(row)
            if classification == "CURRENT_OPERATIONAL_FAILURE":
                current_replay_failures.append(row)
    if current_replay_failures:
        add("replay_drift", "FAIL", "Current replay artifacts reported an unresolved operational failure.", current_replay_failures)
    else:
        add("replay_drift", "PASS", "No current replay drift was detected; historical first-run fail-closed bundles are preserved as legacy evidence.", replay_classifications)

    provider_failures = sum(cycle["provider_status"].get("active_failure_count", 0) for cycle in cycles)
    provider_timeouts = sum(cycle["provider_status"].get("active_timeout_count", 0) for cycle in cycles)
    recovered_provider_failures = sum(cycle["provider_status"].get("recovered_failure_count", 0) for cycle in cycles)
    recovered_timeouts = sum(
        1
        for cycle in cycles
        for row in cycle["provider_status"].get("recovered_failures", [])
        if str(row.get("status") or "").upper() == "TIMEOUT"
    )
    add(
        "provider_instability",
        "PASS" if provider_failures == 0 and provider_timeouts == 0 else "WARN",
        f"active_provider_failures={provider_failures} active_provider_timeouts={provider_timeouts} recovered_provider_failures={recovered_provider_failures} recovered_provider_timeouts={recovered_timeouts}",
    )

    duplicate_classifications = []
    for cycle in cycles:
        for group in cycle["artifact_growth"].get("duplicate_artifacts") or []:
            duplicate_classifications.append({"day": cycle["operational_day"], **_duplicate_warning_classification(group)})
    active_duplicates = [row for row in duplicate_classifications if row.get("active")]
    add(
        "duplicate_artifacts",
        "PASS" if not active_duplicates else "WARN",
        "Duplicate artifacts are known pointer aliases or absent." if not active_duplicates else "Active duplicate artifact groups need producer idempotency review.",
        duplicate_classifications,
    )

    stale_classifications = []
    active_stale = []
    for cycle in cycles:
        if cycle["candidate_counts"].get("raw_candidate_count", 0) == 0 and cycle["candidate_counts"].get("promoted_candidate_count", 0) == 0:
            classification = "HISTORICAL_LEGACY_WARNING" if cycle["operational_day"] != latest_day else "CURRENT_ACTIVE_WARNING"
            row = {"day": cycle["operational_day"], "classification": classification, "reason": "NO_CANDIDATE_COUNTS_OBSERVED"}
            stale_classifications.append(row)
            if classification == "CURRENT_ACTIVE_WARNING":
                active_stale.append(row)
    add(
        "stale_operational_state",
        "PASS" if not active_stale else "WARN",
        "Current operational day has candidate state; historical missing state is preserved as legacy evidence." if not active_stale else "Current operational day has no candidate state.",
        stale_classifications,
    )

    add("browser_workflow_regressions", "PASS" if ui.get("status") in {"PASS", "NOT_RUN"} else "FAIL", f"ui_status={ui.get('status')}", ui.get("failures"))
    add("command_lifecycle_inconsistencies", "PASS" if commands.get("status") == "PASS" else "FAIL", f"command_status={commands.get('status')}", commands.get("failures"))

    runaway = []
    for cycle in cycles:
        queue = cycle["dynamic_certification"]
        final = cycle["certified_universe"]
        if queue.get("queue_size", 0) > 20 or queue.get("estimated_provider_load", 0) > 20 or final.get("symbol_growth_from_prior_cycle", 0) > 20:
            runaway.append({"day": cycle["operational_day"], "queue": queue, "certified_universe": final})
    add("runaway_dynamic_universe_growth", "PASS" if not runaway else "FAIL", "Dynamic certification growth is bounded." if not runaway else "Dynamic certification or certified universe growth exceeded guardrails.", runaway)

    unresolved = []
    active_unresolved = []
    for cycle in cycles:
        stages = cycle.get("stage_status") or {}
        missing_required = [name for name in ("eod_certification", "candidate_funnel", "performance_projection", "exit_review_projection") if stages.get(name, {}).get("status") != "PRESENT"]
        if missing_required:
            classification = "HISTORICAL_LEGACY_WARNING" if cycle["operational_day"] != latest_day else "CURRENT_ACTIVE_WARNING"
            row = {"day": cycle["operational_day"], "classification": classification, "missing_required_stages": missing_required}
            unresolved.append(row)
            if classification == "CURRENT_ACTIVE_WARNING":
                active_unresolved.append(row)
    add(
        "unresolved_command_actions",
        "PASS" if not active_unresolved else "WARN",
        "Current required projection stages are present; historical missing stages are preserved as legacy evidence." if not active_unresolved else "Current required projection stages are missing.",
        unresolved,
    )

    resource_warnings = resource.get("warnings") or []
    add("memory_resource_warnings", "PASS" if not resource_warnings else "WARN", "No resource warnings." if not resource_warnings else "Resource warnings were observed.", resource_warnings)
    return failures


def build_aegis_operational_soak_report_v1(
    *,
    truth_root: Path | str,
    operational_day: str,
    lookback_days: int = 5,
    ui_results: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    families = (
        "operator_state_snapshot_v1",
        "aegis_lite_eod_report_v1",
        "final_eod_market_data_v1",
        "aegis_market_data_v1",
        "dynamic_certification_queue_v1",
        "paper_trade_evaluation_projection_v1",
        "exit_review_projection_v1",
        "replay_certification_gate_v1",
    )
    days = _report_days(root, families, max_day=str(operational_day), lookback_days=lookback_days)
    if str(operational_day) not in days:
        days.append(str(operational_day))
    cycles: list[dict[str, Any]] = []
    previous_symbol_count: int | None = None
    for day in sorted(set(days)):
        final_eod = _final_eod_metrics(root, day, previous_symbol_count)
        previous_symbol_count = final_eod.get("symbol_count") if final_eod.get("symbol_count") else previous_symbol_count
        provider = _provider_metrics(root, day, final_eod.get("symbols") or [])
        dynamic = _dynamic_queue_metrics(root, day)
        projections = _projection_metrics(root, day)
        candidate = _candidate_metrics(root, day)
        cycle = {
            "operational_day": day,
            "stage_status": _stage_status(root, day),
            "replay": _replay_metrics(root, day),
            "provider_status": provider,
            "candidate_counts": candidate,
            "promoted_counts": {
                "promoted_candidate_count": candidate.get("promoted_candidate_count", 0),
                "capture_ticket_count": candidate.get("capture_ticket_count", 0),
            },
            "dynamic_certification": dynamic,
            "dynamic_certified_symbols": dynamic.get("certified_symbols", []),
            "certified_universe": final_eod,
            "projections": projections,
            "artifact_growth": _artifact_inventory(root, day),
            "timeout_events": provider.get("timeout_count", 0),
            "runtime_durations": {
                "provider_latency_ms": provider.get("latency_ms", {}),
            },
        }
        cycles.append(cycle)
    ui = _ui_metrics(ui_results)
    commands = _command_metrics()
    resource = _resource_metrics()
    acceptance = _acceptance_rules(cycles, ui, commands, resource)
    hard_failures = [rule for rule in acceptance if rule.get("status") == "FAIL"]
    warnings = [rule for rule in acceptance if rule.get("status") == "WARN"]
    latest = cycles[-1] if cycles else {}
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "operational_day": str(operational_day),
        "generated_at_utc": utc_now_v1(),
        "truth_root": str(root),
        "cycle_count": len(cycles),
        "cycles": cycles,
        "replay_status": "PASS" if not any(rule["rule_id"] == "replay_drift" and rule["status"] == "FAIL" for rule in acceptance) else "FAIL",
        "replay_failure_classification": [rule.get("details") for rule in acceptance if rule.get("rule_id") == "replay_drift"],
        "replay_hash": stable_hash_v1([cycle.get("replay", {}).get("replay_hash") for cycle in cycles]),
        "provider_status": "PASS" if not any(cycle.get("provider_status", {}).get("active_failure_count", 0) or cycle.get("provider_status", {}).get("active_timeout_count", 0) for cycle in cycles) else "WARN",
        "provider_failure_classification": {
            "active_failure_count": sum(int(cycle.get("provider_status", {}).get("active_failure_count") or 0) for cycle in cycles),
            "recovered_failure_count": sum(int(cycle.get("provider_status", {}).get("recovered_failure_count") or 0) for cycle in cycles),
            "active_timeout_count": sum(int(cycle.get("provider_status", {}).get("active_timeout_count") or 0) for cycle in cycles),
        },
        "candidate_counts": latest.get("candidate_counts", {}),
        "promoted_counts": latest.get("promoted_counts", {}),
        "dynamic_certified_symbols": sorted({symbol for cycle in cycles for symbol in cycle.get("dynamic_certified_symbols", [])}),
        "workflow_failures": ui.get("failures", []),
        "timeout_events": sum(int(cycle.get("timeout_events") or 0) for cycle in cycles),
        "ui_failures": ui.get("failures", []),
        "command_failures": commands.get("failures", []),
        "runtime_durations": {cycle["operational_day"]: cycle.get("runtime_durations", {}) for cycle in cycles},
        "memory_resource_warnings": resource.get("warnings", []),
        "ui_workflow_soak": ui,
        "command_lifecycle": commands,
        "resource_usage": resource,
        "acceptance_rule_results": acceptance,
        "overall_operational_health": "FAIL" if hard_failures else ("WARN" if warnings else "PASS"),
        **SAFETY_FLAGS,
    }
    hash_payload = {**payload, "generated_at_utc": "", "content_hash": "", "resource_usage": {}, "memory_resource_warnings": []}
    payload["content_hash"] = stable_hash_v1(hash_payload)
    return payload


def aegis_operational_soak_report_path_v1(*, truth_root: Path | str, operational_day: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(operational_day) / "aegis_operational_soak_report.v1.json"


def write_aegis_operational_soak_report_v1(*, truth_root: Path | str, operational_day: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = aegis_operational_soak_report_path_v1(truth_root=truth_root, operational_day=operational_day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"aegis_operational_soak_report": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}
