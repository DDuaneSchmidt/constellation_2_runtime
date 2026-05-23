from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.domain_source_registry_v1 import (
    domain_source_contract_v1,
    load_domain_source_registry_v1,
    render_domain_source_command_v1,
    render_domain_source_path_v1,
)


SCHEMA_ID = "aegis_domain_certification"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "domain_certification_v1"
DOMAIN_SNAPSHOT_FAMILY = "domain_snapshot_v1"

DOMAIN_IDS = (
    "US_EQUITIES_EOD",
    "US_EQUITIES_INTRADAY",
    "VOLATILITY",
    "RATES_BONDS",
    "MACRO_CALENDAR",
    "EARNINGS_EVENTS",
    "CORPORATE_ACTIONS",
)

CERTIFIED = "CERTIFIED"
PARTIAL = "PARTIAL"
DEGRADED = "DEGRADED"
DELAYED = "DELAYED"
CONFLICTED = "CONFLICTED"
FAILED = "FAILED"

US_BOND_SYMBOLS = {"HYG", "LQD", "IEF", "TLT", "SHY", "BIL", "AGG", "BND", "TIP"}

SLEEVE_DOMAIN_REGISTRY: dict[str, dict[str, list[str]]] = {
    "C2_MEAN_REVERSION_EQ_V1": {
        "required": ["US_EQUITIES_INTRADAY"],
        "optional": ["VOLATILITY"],
    },
    "C2_TREND_EQ_PRIMARY_V1": {
        "required": ["US_EQUITIES_INTRADAY"],
        "optional": ["RATES_BONDS", "VOLATILITY"],
    },
    "C2_EVENT_DISLOCATION_V1": {
        "required": ["US_EQUITIES_INTRADAY", "MACRO_CALENDAR"],
        "optional": ["VOLATILITY", "EARNINGS_EVENTS"],
    },
    "C2_VOL_INCOME_DEFINED_RISK_V1": {
        "required": ["US_EQUITIES_INTRADAY", "VOLATILITY"],
        "optional": ["CORPORATE_ACTIONS"],
    },
    "C2_CROSS_ASSET_TREND_V1": {
        "required": ["US_EQUITIES_INTRADAY", "RATES_BONDS"],
        "optional": ["VOLATILITY", "MACRO_CALENDAR"],
    },
    "C2_MARKET_NEUTRAL_SPREAD_V1": {
        "required": ["US_EQUITIES_INTRADAY"],
        "optional": ["CORPORATE_ACTIONS"],
    },
    "C2_DEFENSIVE_TAIL_V1": {
        "required": ["US_EQUITIES_INTRADAY", "VOLATILITY", "RATES_BONDS"],
        "optional": ["MACRO_CALENDAR"],
    },
    "C2_INTENT_SIMULATOR_V1": {
        "required": [],
        "optional": ["US_EQUITIES_INTRADAY", "VOLATILITY", "MACRO_CALENDAR"],
    },
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: dict[str, Any]) -> str:
    clean = {key: value for key, value in payload.items() if key not in {"content_hash", "artifact_hash", "output_hash"}}
    blob = json.dumps(clean, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def sha256_file_v1(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_score_v1(value: Any) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        number = Decimal("0")
    if number < 0:
        number = Decimal("0")
    if number > 1:
        number = Decimal("1")
    return f"{number.quantize(Decimal('0.000001'))}"


def read_json_v1(path: Path) -> dict[str, Any]:
    try:
        if path.exists() and path.is_file():
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
    except Exception:
        return {}
    return {}


def domain_certification_report_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "domain_certification.v1.json"


def domain_snapshot_dir_v1(*, truth_root: Path | str, day_utc: str, domain_id: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / DOMAIN_SNAPSHOT_FAMILY / day_utc / domain_id


def _input_records(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    records = [row for row in inputs.get("input_records", []) if isinstance(row, dict)]
    if records:
        return records
    symbols = inputs.get("symbols") if isinstance(inputs.get("symbols"), dict) else {}
    synthesized: list[dict[str, Any]] = []
    for symbol, row in sorted(symbols.items()):
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or row.get("canonical_symbol") or symbol).upper()
        if not sym:
            continue
        freshness = str(row.get("freshness_status") or "").upper()
        eligible = row.get("candidate_generation_eligible") is True or (row.get("usable_for") if isinstance(row.get("usable_for"), dict) else {}).get("sleeve_intraday_generation") is True
        validation_status = "VALID" if freshness == "CURRENT" and eligible else (freshness or "MISSING")
        synthesized.append({
            "data_item_id": "market.volatility.VIX" if sym == "VIX" else f"market.price.{sym}",
            "symbol": sym,
            "validation_status": validation_status,
            "source_vendor": row.get("provider") or row.get("source") or inputs.get("source") or "",
            "reason": row.get("reason") or row.get("freshness_reason") or "",
            "raw_source_hash": row.get("source_hash") or row.get("raw_source_hash") or "",
            "source_timestamp_utc": row.get("source_timestamp_utc") or row.get("data_timestamp_utc") or "",
            "market_data_mode": row.get("market_data_mode") or inputs.get("market_data_mode") or "",
        })
    return synthesized


def _source_ref(path: Path, artifact_type: str) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "artifact_type": artifact_type,
        "path": str(path),
        "exists": exists,
        "sha256": sha256_file_v1(path) if exists else "",
    }


def _provider_rows(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for attempt in payload.get("provider_results", []) if isinstance(payload.get("provider_results"), list) else []:
            if not isinstance(attempt, dict):
                continue
            provider = str(attempt.get("provider") or attempt.get("source") or "").strip() or "UNKNOWN"
            status = str(attempt.get("request_status") or attempt.get("status") or "").strip() or "UNKNOWN"
            reason = str(attempt.get("failure_reason") or attempt.get("reason") or "").strip()
            key = (provider, json.dumps({"status": status, "reason": reason, "symbols": attempt.get("fetched_symbols") or attempt.get("symbols")}, sort_keys=True, default=str))
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "provider": provider,
                    "status": status,
                    "failure_reason": reason,
                    "fetched_symbols": sorted(str(symbol).upper() for symbol in attempt.get("fetched_symbols", []) if str(symbol)),
                    "missing_symbols": sorted(str(symbol).upper() for symbol in attempt.get("missing_symbols", []) if str(symbol)),
                }
            )
    if not rows:
        providers = sorted({str(row.get("source_vendor") or "").strip() for payload in payloads for row in _input_records(payload) if str(row.get("source_vendor") or "").strip()})
        rows = [{"provider": provider, "status": "OBSERVED", "failure_reason": "", "fetched_symbols": [], "missing_symbols": []} for provider in providers]
    return rows


def _records_for_domain(domain_id: str, inputs_payload: dict[str, Any]) -> list[dict[str, Any]]:
    records = _input_records(inputs_payload)
    out: list[dict[str, Any]] = []
    for row in records:
        item_id = str(row.get("data_item_id") or "")
        symbol = str(row.get("symbol") or item_id.rsplit(".", 1)[-1]).upper()
        if domain_id == "VOLATILITY" and item_id == "market.volatility.VIX":
            out.append(row)
        elif domain_id == "RATES_BONDS" and item_id.startswith("market.price.") and symbol in US_BOND_SYMBOLS:
            out.append(row)
        elif domain_id == "US_EQUITIES_INTRADAY" and item_id.startswith("market.price.") and symbol not in US_BOND_SYMBOLS:
            out.append(row)
    return out


def _external_domain_artifact(root: Path, day_utc: str, domain_id: str) -> tuple[Path, str]:
    contract = domain_source_contract_v1(domain_id)
    if contract:
        return render_domain_source_path_v1(truth_root=root, day_utc=day_utc, contract=contract), str(contract.get("required_source") or "")
    mapping = {
        "MACRO_CALENDAR": ("macro_calendar_v1", "macro_calendar.v1.json"),
        "EARNINGS_EVENTS": ("earnings_events_v1", "earnings_events.v1.json"),
        "CORPORATE_ACTIONS": ("corporate_actions_v1", "corporate_actions.v1.json"),
    }
    family, filename = mapping.get(domain_id, ("", ""))
    return root / "reports" / family / day_utc / filename, family


def _status_from_records(records: list[dict[str, Any]], *, required_count: int, provider_rows: list[dict[str, Any]]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    provider_text = json.dumps(provider_rows, sort_keys=True).upper()
    if "CONFLICT" in provider_text or "MISMATCH" in provider_text:
        return CONFLICTED, ["PROVIDER_CONFLICT_DETECTED"]
    if records:
        valid = [row for row in records if str(row.get("validation_status") or "").upper() == "VALID"]
        invalid = [row for row in records if str(row.get("validation_status") or "").upper() not in {"VALID", ""}]
        if len(valid) == len(records):
            return CERTIFIED, []
        if valid:
            reasons.extend(sorted({str(row.get("reason") or row.get("validation_status") or "INVALID_INPUT") for row in invalid}))
            return PARTIAL, reasons or ["PARTIAL_INPUT_COVERAGE"]
        reasons.extend(sorted({str(row.get("reason") or row.get("validation_status") or "NO_VALID_INPUTS") for row in records}))
        if any(str(row.get("validation_status") or "").upper() in {"MALFORMED", "TAMPERED"} for row in records):
            return FAILED, reasons or ["DOMAIN_INPUT_INVALID"]
        return DELAYED, reasons or ["DOMAIN_INPUTS_UNAVAILABLE"]
    if required_count:
        return DELAYED, ["DOMAIN_INPUTS_MISSING"]
    return DEGRADED, ["NO_DOMAIN_INPUT_REQUIRED_OR_OBSERVED"]


def _final_eod_status(root: Path, day_utc: str) -> tuple[dict[str, Any], Path]:
    contract = domain_source_contract_v1("US_EQUITIES_EOD")
    canonical_path = render_domain_source_path_v1(truth_root=root, day_utc=day_utc, contract=contract) if contract else Path("")
    sidecar_manifest = root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.current.v1.json"
    legacy_path = root / "reports" / "market_data_final_eod_v1" / day_utc / "market_data_final_eod.v1.json"
    for path in [canonical_path, sidecar_manifest, legacy_path]:
        if path and path.exists() and path.is_file():
            payload = read_json_v1(path)
            if payload.get("schema_id") == "final_eod_market_data_current_manifest.v1":
                artifact_path = Path(str(payload.get("current_artifact_path") or ""))
                artifact = read_json_v1(artifact_path) if artifact_path.exists() else {}
                return artifact, artifact_path if artifact else path
            return payload, path
    return {}, canonical_path or legacy_path


def _build_domain_snapshot(
    *,
    root: Path,
    day_utc: str,
    domain_id: str,
    inputs_payload: dict[str, Any],
    inputs_path: Path,
    final_payload: dict[str, Any],
    final_path: Path,
    generated_at_utc: str,
) -> dict[str, Any]:
    if domain_id == "US_EQUITIES_EOD":
        source_payloads = [final_payload] if final_payload else []
        provider_rows = _provider_rows(source_payloads)
        source_refs = [_source_ref(final_path, "final_eod_market_data_v1")]
        if final_payload:
            status_text = " ".join(str(final_payload.get(key) or "") for key in ("status", "validation_status", "final_eod_certification_status", "certification_state")).upper()
            payload_reasons = [str(reason) for reason in (final_payload.get("reason_codes") or []) if str(reason)]
            if any(token in status_text for token in ("VALID", "CERTIFIED", "PASS", "READY")):
                status, reasons = CERTIFIED, []
            elif any(token in status_text for token in ("FAIL", "INVALID", "REJECTED")):
                status, reasons = FAILED, payload_reasons or ["FINAL_EOD_SOURCE_FAILED"]
            else:
                status, reasons = DELAYED, payload_reasons or ["FINAL_EOD_NOT_CERTIFIED"]
            requested = [str(symbol).upper() for symbol in final_payload.get("requested_symbols", []) if str(symbol)]
            final_symbols = [str(symbol).upper() for symbol in final_payload.get("final_eod_symbols", []) if str(symbol)]
            if not final_symbols:
                final_symbols = [str(symbol).upper() for symbol in final_payload.get("fetched_symbols", []) if str(symbol)] if status == CERTIFIED else []
            completeness = canonical_score_v1(len(set(requested) & set(final_symbols)) / max(1, len(requested))) if requested else canonical_score_v1(1 if status == CERTIFIED else 0)
        else:
            status, reasons, completeness = DELAYED, ["FINAL_EOD_ARTIFACT_MISSING"], canonical_score_v1(0)
    elif domain_id in {"MACRO_CALENDAR", "EARNINGS_EVENTS", "CORPORATE_ACTIONS"}:
        artifact_path, artifact_family = _external_domain_artifact(root, day_utc, domain_id)
        payload = read_json_v1(artifact_path)
        source_refs = [_source_ref(artifact_path, artifact_family)]
        provider_rows = _provider_rows([payload])
        if payload:
            status_text = str(payload.get("status") or payload.get("validation_status") or "").upper()
            if "CONFLICT" in status_text:
                status, reasons = CONFLICTED, ["PROVIDER_CONFLICT_DETECTED"]
            elif any(token in status_text for token in ("VALID", "CERTIFIED", "READY", "PASS")):
                status, reasons = CERTIFIED, []
            elif any(token in status_text for token in ("FAIL", "INVALID", "REJECTED")):
                status, reasons = FAILED, ["DOMAIN_SOURCE_FAILED"]
            else:
                status, reasons = DELAYED, ["DOMAIN_SOURCE_NOT_READY"]
            completeness = canonical_score_v1(payload.get("completeness_score") or (1 if status == CERTIFIED else 0))
        else:
            status, reasons, completeness = DELAYED, [f"{domain_id}_SOURCE_MISSING"], canonical_score_v1(0)
    else:
        records = _records_for_domain(domain_id, inputs_payload)
        source_refs = [_source_ref(inputs_path, "market_data_inputs_v1")]
        provider_rows = _provider_rows([inputs_payload])
        status, reasons = _status_from_records(records, required_count=len(records), provider_rows=provider_rows)
        completeness = canonical_score_v1(len([row for row in records if str(row.get("validation_status") or "").upper() == "VALID"]) / max(1, len(records))) if records else canonical_score_v1(0)
    source_contract = domain_source_contract_v1(domain_id)
    snapshot_basis = {
        "schema_id": "aegis_domain_snapshot",
        "schema_version": "v1",
        "domain_id": domain_id,
        "day_utc": day_utc,
        "captured_at_utc": generated_at_utc,
        "certification_status": status,
        "completeness_score": completeness,
        "freshness_state": status,
        "provider_results": provider_rows,
        "source_refs": source_refs,
        "reason_codes": reasons,
        "domain_diagnostics": (
            final_payload.get("validation_details")
            if domain_id == "US_EQUITIES_EOD" and isinstance(final_payload.get("validation_details"), dict)
            else (
                {
                    "provider_coverage_plan": final_payload.get("provider_coverage_plan") if isinstance(final_payload.get("provider_coverage_plan"), dict) else {},
                    "provider_coverage_action_item": final_payload.get("provider_coverage_action_item") if isinstance(final_payload.get("provider_coverage_action_item"), dict) else {},
                }
                if domain_id == "US_EQUITIES_EOD"
                else {}
            )
        ),
        "source_contract": {
            "required_source": source_contract.get("required_source", ""),
            "provider_or_source": source_contract.get("provider_or_source", ""),
            "artifact_path_template": source_contract.get("artifact_path_template", ""),
            "validation_command": source_contract.get("validation_command", ""),
            "rebuild_command": source_contract.get("rebuild_command", ""),
            "certification_command": source_contract.get("certification_command", ""),
            "repair_mode_when_missing": source_contract.get("repair_mode_when_missing", ""),
            "setup_required_message": source_contract.get("setup_required_message", ""),
            "coverage_requirement": source_contract.get("coverage_requirement", ""),
        },
    }
    content_hash = stable_hash_v1(snapshot_basis)
    snapshot_id = f"domain-snapshot:{day_utc}:{domain_id}:{content_hash[:16]}"
    return {
        **snapshot_basis,
        "snapshot_id": snapshot_id,
        "content_hash": content_hash,
        "single_provider_risk": len({row.get("provider") for row in provider_rows if row.get("provider")}) == 1,
        "quorum_state": "SINGLE_PROVIDER_RISK_RECORDED" if len({row.get("provider") for row in provider_rows if row.get("provider")}) <= 1 else ("CONFLICTED" if status == CONFLICTED else "QUORUM_COMPARED"),
        "lineage": {
            "source_hashes": [row["sha256"] for row in source_refs if row.get("sha256")],
            "source_paths": [row["path"] for row in source_refs],
        },
        "safety": {
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }


def _domain_is_usable(status: str) -> bool:
    return status in {CERTIFIED, PARTIAL, DEGRADED}


def _operator_domain_message(domain_id: str, status: str) -> str:
    names = {
        "MACRO_CALENDAR": "macro calendar data",
        "EARNINGS_EVENTS": "earnings/event data",
        "CORPORATE_ACTIONS": "corporate actions data",
        "VOLATILITY": "volatility data",
        "RATES_BONDS": "rates and bond ETF data",
        "US_EQUITIES_EOD": "final US equities EOD data",
        "US_EQUITIES_INTRADAY": "current US equities intraday data",
    }
    if status == CERTIFIED:
        return f"{names.get(domain_id, domain_id)} certified."
    if status == PARTIAL:
        return f"{names.get(domain_id, domain_id)} partially certified."
    if status == DEGRADED:
        return f"{names.get(domain_id, domain_id)} unavailable but optional for current workflow."
    if status == CONFLICTED:
        return f"Provider conflict in {names.get(domain_id, domain_id)}."
    if status == FAILED:
        return f"{names.get(domain_id, domain_id)} failed certification."
    return f"Waiting on {names.get(domain_id, domain_id)}."


DOMAIN_REPAIR_GUIDANCE: dict[tuple[str, str], dict[str, Any]] = {
    ("US_EQUITIES_EOD", "FINAL_EOD_ARTIFACT_MISSING"): {
        "repair_state_label": "Aegis will retry automatically",
        "repair_action_label": "Run/retry EOD market-data artifact build",
        "required_source_label": "final_eod_market_data_v1 final EOD artifact",
        "steps": [
            "Queue automatic final EOD artifact build",
            "Run provider/file source build",
            "Validate generated final EOD artifact",
            "Re-run domain certification",
        ],
    },
    ("US_EQUITIES_EOD", "FINAL_EOD_NOT_CERTIFIED"): {
        "repair_state_label": "Aegis will retry automatically",
        "repair_action_label": "Retry final EOD provider/source build and recertify",
        "required_source_label": "valid final_eod_market_data_v1 artifact with complete target-day coverage",
        "steps": [
            "Queue automatic final EOD repair lifecycle",
            "Rebuild final EOD source artifact from governed provider/source",
            "Validate target-day symbol coverage and source status",
            "Re-run domain certification and move to CERTIFIED only if validation passes",
        ],
    },
    ("MACRO_CALENDAR", "MACRO_CALENDAR_SOURCE_MISSING"): {
        "repair_state_label": "Source setup required",
        "repair_action_label": "Open/upload/configure macro calendar source",
        "required_source_label": "macro_calendar_v1 source artifact or configured API",
        "steps": [
            "Open/upload/configure macro calendar source",
            "Validate source file/API",
            "Re-run macro calendar domain certification",
        ],
    },
    ("EARNINGS_EVENTS", "EARNINGS_EVENTS_SOURCE_MISSING"): {
        "repair_state_label": "Source setup required",
        "repair_action_label": "Open/upload/configure earnings events source",
        "required_source_label": "earnings_events_v1 source artifact or configured API",
        "steps": [
            "Open/upload/configure earnings events source",
            "Validate event coverage for relevant symbols",
            "Re-run earnings event domain certification",
        ],
    },
    ("CORPORATE_ACTIONS", "CORPORATE_ACTIONS_SOURCE_MISSING"): {
        "repair_state_label": "Source setup required",
        "repair_action_label": "Open/upload/configure corporate actions source",
        "required_source_label": "corporate_actions_v1 source artifact or configured API",
        "steps": [
            "Open/upload/configure corporate actions source",
            "Validate split/dividend/action coverage",
            "Re-run corporate actions certification",
        ],
    },
}


def _domain_repair_last_attempt(snapshot: dict[str, Any]) -> str:
    source_refs = [row for row in snapshot.get("source_refs", []) if isinstance(row, dict)]
    if source_refs and not any(row.get("exists") for row in source_refs):
        return "No source artifact observed"
    provider_rows = [row for row in snapshot.get("provider_results", []) if isinstance(row, dict)]
    if provider_rows:
        labels: list[str] = []
        for row in provider_rows[:3]:
            status = str(row.get("status") or "UNKNOWN").strip() or "UNKNOWN"
            reason = str(row.get("failure_reason") or "").strip()
            labels.append(f"{status}{f' - {reason}' if reason else ''}")
        return "; ".join(labels)
    if source_refs:
        return "Source artifact observed; certification still delayed"
    return "No certification attempt recorded"


def _latest_domain_command_results_v1(root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    path = root / "reports" / "aegis_operator_command_audit_v1" / day_utc / "operator_command_audit.v1.jsonl"
    latest: dict[str, dict[str, Any]] = {}
    if not path.exists() or not path.is_file():
        return latest
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if str(row.get("command_id") or "") != "REPAIR_DOMAIN":
            continue
        domain_id = str(row.get("target_id") or "")
        if not domain_id:
            continue
        summary = row.get("response_summary") if isinstance(row.get("response_summary"), dict) else {}
        command_result = summary.get("command_result") if isinstance(summary.get("command_result"), dict) else {}
        if not command_result:
            command_result = {
                "schema_id": "aegis_command_result_panel.v1",
                "status_label": str(summary.get("result_status") or row.get("result") or "Command result"),
                "result_status": str(summary.get("result_status") or row.get("result") or "UNKNOWN"),
                "plain_english_result": str(summary.get("user_message") or "Command completed."),
                "next_required_step": str(summary.get("next_state") or "Review the current domain state."),
                "audit_id": str(row.get("audit_id") or ""),
                "timestamp": str(row.get("requested_at") or ""),
            }
        command_result = {
            **command_result,
            "audit_id": command_result.get("audit_id") or row.get("audit_id") or "",
            "timestamp": command_result.get("timestamp") or row.get("requested_at") or "",
        }
        previous = latest.get(domain_id)
        if not previous or str(command_result.get("timestamp") or "") >= str(previous.get("timestamp") or ""):
            latest[domain_id] = command_result
    return latest


def _domain_repair_plan(
    *,
    snapshot: dict[str, Any],
    affected_sleeves: list[str],
    truth_root: Path,
    affected_hypotheses: list[str] | None = None,
    latest_command_result: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if str(snapshot.get("certification_status") or "").upper() not in {DELAYED, FAILED}:
        return None
    domain_id = str(snapshot.get("domain_id") or "")
    reason = str((snapshot.get("reason_codes") or [""])[0] or "DOMAIN_DELAYED")
    guidance = DOMAIN_REPAIR_GUIDANCE.get((domain_id, reason))
    if guidance is None:
        guidance = {
            "repair_state_label": "Aegis will retry automatically",
            "repair_action_label": "Review domain source and re-run domain certification",
            "required_source_label": str((snapshot.get("source_refs") or [{}])[0].get("artifact_type") or "Domain source artifact"),
            "steps": [
                "Review required domain source/artifact",
                "Validate source coverage",
                "Re-run domain certification",
            ],
        }
    source_ref = next((row for row in snapshot.get("source_refs", []) if isinstance(row, dict)), {})
    source_contract = snapshot.get("source_contract") if isinstance(snapshot.get("source_contract"), dict) else domain_source_contract_v1(domain_id)
    source_missing = reason.endswith("_SOURCE_MISSING")
    repair_mode = str(source_contract.get("repair_mode_when_missing") or ("SOURCE_SETUP_REQUIRED" if source_missing else "PROVIDER_WAIT"))
    if domain_id == "US_EQUITIES_EOD" and reason in {"FINAL_EOD_ARTIFACT_MISSING", "FINAL_EOD_NOT_CERTIFIED"}:
        repair_mode = "AUTO_REBUILD"
    diagnostics = snapshot.get("domain_diagnostics") if isinstance(snapshot.get("domain_diagnostics"), dict) else {}
    coverage_action = diagnostics.get("provider_coverage_action_item") if isinstance(diagnostics.get("provider_coverage_action_item"), dict) else {}
    repair_action_label = guidance["repair_action_label"]
    repair_steps = list(guidance["steps"])
    next_retry_label = "automatic repair orchestration" if repair_mode == "AUTO_REBUILD" else guidance["repair_state_label"]
    if str(coverage_action.get("status") or "").upper() == "PROVIDER_COVERAGE_INCOMPLETE":
        repair_mode = "SOURCE_SETUP_REQUIRED"
        repair_action_label = "Upload/configure EOD source"
        next_retry_label = "Upload/configure EOD source or configure a reliable EOD provider before recertification."
        repair_steps = [
            "Review required universe size, stale symbols, missing symbols, and timeout symbols",
            "Configure a reliable certification-eligible EOD provider or add a fallback/source artifact",
            "Rebuild final EOD source artifact",
            "Re-run US_EQUITIES_EOD domain certification",
        ]
    validation_template = str(source_contract.get("validation_command") or "")
    rebuild_template = str(source_contract.get("rebuild_command") or "")
    certification_template = str(source_contract.get("certification_command") or "")
    return {
        "domain_id": domain_id,
        "domain_name": domain_id.replace("_", " ").title(),
        "reason": reason,
        "repair_mode": repair_mode,
        "repair_stage": "SOURCE_SETUP_REQUIRED" if repair_mode == "SOURCE_SETUP_REQUIRED" else "QUEUED_FOR_AUTOMATIC_REPAIR",
        "affected_sleeves": sorted(set(affected_sleeves)),
        "affected_hypotheses": sorted(set(affected_hypotheses or [])),
        "required_source_artifact": {
            "label": guidance["required_source_label"],
            "artifact_type": source_ref.get("artifact_type") or source_contract.get("required_source") or "",
            "path": source_ref.get("path") or "",
            "exists": bool(source_ref.get("exists")),
            "provider_or_source": source_contract.get("provider_or_source", ""),
            "setup_required_message": source_contract.get("setup_required_message", ""),
            "coverage_requirement": source_contract.get("coverage_requirement", ""),
        },
        "source_contract": source_contract,
        "validation_command": render_domain_source_command_v1(command_template=validation_template, truth_root=truth_root, day_utc=str(snapshot.get("day_utc") or ""), domain_id=domain_id) if validation_template else "",
        "rebuild_command": render_domain_source_command_v1(command_template=rebuild_template, truth_root=truth_root, day_utc=str(snapshot.get("day_utc") or ""), domain_id=domain_id) if rebuild_template else "",
        "certification_command": render_domain_source_command_v1(command_template=certification_template, truth_root=truth_root, day_utc=str(snapshot.get("day_utc") or ""), domain_id=domain_id) if certification_template else "",
        "last_attempt": _domain_repair_last_attempt(snapshot),
        "next_retry": next_retry_label,
        "repair_action": repair_action_label,
        "repair_steps": repair_steps,
        "operator_action_required": repair_mode != "AUTO_REBUILD",
        "source_setup_required": repair_mode == "SOURCE_SETUP_REQUIRED",
        "automatic_repair_available": repair_mode == "AUTO_REBUILD",
        "command_id": "REPAIR_DOMAIN",
        "target_type": "domain_certification",
        "target_id": domain_id,
        "repair_button_label": "View repair" if repair_mode == "AUTO_REBUILD" else "Source setup",
        "repair_status_after_request": "automatic lifecycle will record queued/running/validating/recertifying/completed/failed",
        "diagnostics": diagnostics,
        **({"latest_command_result": latest_command_result} if latest_command_result else {}),
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }


def build_domain_certification_report_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or utc_now_v1()
    inputs_path = root / "reports" / "market_data_inputs_v1" / day_utc / "market_data_inputs.v1.json"
    inputs_payload = read_json_v1(inputs_path)
    intraday_path = root / "reports" / "market_data_intraday_operational_v1" / day_utc / "market_data_intraday_operational.v1.json"
    intraday_payload = read_json_v1(intraday_path)
    source_registry = load_domain_source_registry_v1()
    final_payload, final_path = _final_eod_status(root, day_utc)
    snapshots = []
    for domain_id in DOMAIN_IDS:
        lane_payload = inputs_payload
        lane_path = inputs_path
        if domain_id not in {"US_EQUITIES_EOD", "MACRO_CALENDAR", "EARNINGS_EVENTS", "CORPORATE_ACTIONS"} and intraday_payload:
            lane_payload = intraday_payload
            lane_path = intraday_path
        snapshots.append(_build_domain_snapshot(
            root=root,
            day_utc=day_utc,
            domain_id=domain_id,
            inputs_payload=lane_payload,
            inputs_path=lane_path,
            final_payload=final_payload,
            final_path=final_path,
            generated_at_utc=generated_at,
        ))
    by_domain = {row["domain_id"]: row for row in snapshots}
    sleeve_rows: list[dict[str, Any]] = []
    for sleeve_id, deps in sorted(SLEEVE_DOMAIN_REGISTRY.items()):
        required = list(deps.get("required") or [])
        optional = list(deps.get("optional") or [])
        required_blockers = [domain_id for domain_id in required if not _domain_is_usable(str(by_domain[domain_id]["certification_status"]))]
        optional_degraded = [domain_id for domain_id in optional if str(by_domain[domain_id]["certification_status"]) != CERTIFIED]
        if required_blockers:
            status = "BLOCKED"
            next_action = "Resolve required domain blockers before this sleeve can advance."
        elif optional_degraded:
            status = "READY_WITH_WARNINGS"
            next_action = "Sleeve can continue; optional domain gaps reduce confidence only."
        else:
            status = "READY"
            next_action = "No domain certification action required."
        sleeve_rows.append(
            {
                "sleeve_id": sleeve_id,
                "required_domains": required,
                "optional_domains": optional,
                "blocking_domains": required_blockers,
                "optional_degraded_domains": optional_degraded,
                "sleeve_certification_status": status,
                "operator_blockers": [_operator_domain_message(domain_id, str(by_domain[domain_id]["certification_status"])) for domain_id in required_blockers],
                "operator_next_action": next_action,
            }
        )
    affected_by_domain: dict[str, list[str]] = {domain_id: [] for domain_id in DOMAIN_IDS}
    blocking_by_domain: dict[str, list[str]] = {domain_id: [] for domain_id in DOMAIN_IDS}
    for row in sleeve_rows:
        for domain_id in row["blocking_domains"]:
            affected_by_domain.setdefault(domain_id, []).append(row["sleeve_id"])
            blocking_by_domain.setdefault(domain_id, []).append(row["sleeve_id"])
        for domain_id in row["optional_degraded_domains"]:
            affected_by_domain.setdefault(domain_id, []).append(row["sleeve_id"])
    latest_command_results_by_domain = _latest_domain_command_results_v1(root, day_utc)
    repair_actions_by_domain = {
        snapshot["domain_id"]: _domain_repair_plan(
            snapshot=snapshot,
            affected_sleeves=affected_by_domain.get(snapshot["domain_id"], []),
            truth_root=root,
            affected_hypotheses=[],
            latest_command_result=latest_command_results_by_domain.get(snapshot["domain_id"], {}),
        )
        for snapshot in snapshots
    }
    snapshots_with_paths = []
    for snapshot in snapshots:
        snapshot_path = domain_snapshot_dir_v1(truth_root=root, day_utc=day_utc, domain_id=snapshot["domain_id"]) / f"{snapshot['content_hash']}.json"
        repair_plan = repair_actions_by_domain.get(snapshot["domain_id"])
        snapshots_with_paths.append({
            **snapshot,
            "artifact_path": str(snapshot_path),
            "blocking_sleeves": sorted(blocking_by_domain.get(snapshot["domain_id"], [])),
            "affected_sleeves": sorted(set(affected_by_domain.get(snapshot["domain_id"], []))),
            **({"repair_plan": repair_plan} if repair_plan else {}),
        })
    report = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "domain_certification_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "domains": snapshots_with_paths,
        "domain_dependency_registry": [
            {"sleeve_id": sleeve_id, **deps}
            for sleeve_id, deps in sorted(SLEEVE_DOMAIN_REGISTRY.items())
        ],
        "domain_source_registry": source_registry,
        "sleeve_domain_statuses": sleeve_rows,
        "domain_repair_actions": [row for row in repair_actions_by_domain.values() if row],
        "summary": {
            "certified_domains": len([row for row in snapshots if row["certification_status"] == CERTIFIED]),
            "partial_domains": len([row for row in snapshots if row["certification_status"] == PARTIAL]),
            "degraded_domains": len([row for row in snapshots if row["certification_status"] == DEGRADED]),
            "delayed_domains": len([row for row in snapshots if row["certification_status"] == DELAYED]),
            "conflicted_domains": len([row for row in snapshots if row["certification_status"] == CONFLICTED]),
            "failed_domains": len([row for row in snapshots if row["certification_status"] == FAILED]),
            "blocked_sleeves": len([row for row in sleeve_rows if row["sleeve_certification_status"] == "BLOCKED"]),
        },
        "recovery_ladder": [
            "normal_retry",
            "alternate_provider_if_configured",
            "certify_unaffected_domains",
            "isolate_affected_sleeves",
            "escalate_unresolved_critical_domains",
        ],
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
    report["content_hash"] = stable_hash_v1(report)
    return report


def write_domain_certification_report_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    written: dict[str, str] = {}
    for snapshot in payload.get("domains", []) if isinstance(payload.get("domains"), list) else []:
        if not isinstance(snapshot, dict):
            continue
        path = Path(str(snapshot.get("artifact_path") or ""))
        if not path:
            path = domain_snapshot_dir_v1(truth_root=root, day_utc=day_utc, domain_id=str(snapshot.get("domain_id") or "UNKNOWN")) / f"{snapshot.get('content_hash')}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(snapshot, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
        if path.exists() and path.read_text(encoding="utf-8") != data:
            raise ValueError(f"domain snapshot collision: {path}")
        if not path.exists():
            path.write_text(data, encoding="utf-8")
        written[f"domain:{snapshot.get('domain_id')}"] = str(path)
    report_path = domain_certification_report_path_v1(truth_root=root, day_utc=day_utc)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    summary_path = report_path.parent / "domain_certification.v1.txt"
    summary_path.write_text(render_domain_certification_report_v1(payload), encoding="utf-8")
    written["json"] = str(report_path)
    written["txt"] = str(summary_path)
    return written


def render_domain_certification_report_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS DOMAIN CERTIFICATION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"content_hash: {payload.get('content_hash')}",
        "",
        "domains:",
    ]
    for row in payload.get("domains", []) if isinstance(payload.get("domains"), list) else []:
        lines.append(
            f"- {row.get('domain_id')}: status={row.get('certification_status')} completeness={row.get('completeness_score')} "
            f"single_provider_risk={str(row.get('single_provider_risk')).lower()} blockers={','.join(row.get('blocking_sleeves') or [])}"
        )
    lines.extend(["", "sleeves:"])
    for row in payload.get("sleeve_domain_statuses", []) if isinstance(payload.get("sleeve_domain_statuses"), list) else []:
        lines.append(f"- {row.get('sleeve_id')}: {row.get('sleeve_certification_status')} blockers={','.join(row.get('blocking_domains') or [])}")
    lines.extend(["", "broker_submit_transmit_allowed: false", "autonomous_execution_allowed: false", "trade_advice_allowed: false"])
    return "\n".join(lines) + "\n"
