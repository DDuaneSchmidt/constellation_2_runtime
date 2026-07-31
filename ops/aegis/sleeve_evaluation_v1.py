from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


REPORT_FAMILY = "aegis_sleeve_evaluation_v1"
REPORT_FILENAME = "sleeve_evaluation.v1.json"

SAFETY = {
    "read_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "allocation_instructions_allowed": False,
    "candidate_creation_allowed": False,
    "sleeve_logic_modification_allowed": False,
}

BLOCKED_CLASSIFICATIONS = {
    "SILENT_DATA_BLOCKED",
    "SILENT_CONFIG_DISABLED",
    "SILENT_IMPLEMENTATION_GAP",
    "SILENT_RUNTIME_FAILURE",
    "SILENT_UNKNOWN_REQUIRES_INVESTIGATION",
}


def sleeve_evaluation_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_sleeve_evaluation_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _source_paths(root, day)
    calendar_context = _calendar_context_v1(root, day)
    payloads = {name: _read_json(path) for name, path in paths.items()}
    diagnostics = payloads["candidate_generation_diagnostics_v1"]
    rollup = payloads["sleeve_evaluation_kernel_v1"]
    contracts = payloads["candidate_contracts_v1"]

    expected_ids = _expected_sleeve_ids(diagnostics, rollup)
    diagnostic_rows = {str(row.get("sleeve_id") or ""): row for row in _rows(diagnostics, "sleeves")}
    rollup_rows = {str(row.get("sleeve_id") or ""): row for row in _rows(rollup, "outcomes")}
    contract_rows = _rows(contracts, "candidate_contracts", "contracts", "candidates")
    candidate_count_by_sleeve = Counter(str(row.get("sleeve_id") or "UNKNOWN") for row in contract_rows)
    last_candidate_by_sleeve = _last_candidate_dates(root, day)
    last_success_by_sleeve = _last_successful_runs(root, day)

    sleeves: list[dict[str, Any]] = []
    for sleeve_id in expected_ids:
        diag = diagnostic_rows.get(sleeve_id, {})
        outcome = rollup_rows.get(sleeve_id, {})
        row = _build_sleeve_row(
            sleeve_id=sleeve_id,
            diagnostic_row=diag,
            rollup_row=outcome,
            candidate_count=int(candidate_count_by_sleeve.get(sleeve_id, 0)),
            last_candidate_date=last_candidate_by_sleeve.get(sleeve_id),
            last_successful_run=last_success_by_sleeve.get(sleeve_id),
            calendar_context=calendar_context,
        )
        sleeves.append(row)

    silent = [row for row in sleeves if not row["produced_output_intents"] and not row["produced_candidates"]]
    summary = {
        "expected_sleeves": len(expected_ids),
        "ran_sleeves": sum(1 for row in sleeves if row["actually_ran"]),
        "output_producing_sleeves": sum(1 for row in sleeves if row["produced_output_intents"] or row["produced_candidates"]),
        "silent_sleeves": len(silent),
        "blocked_sleeves": sum(1 for row in silent if row["classification"] in BLOCKED_CLASSIFICATIONS),
        "candidate_count_by_sleeve": {sleeve_id: int(candidate_count_by_sleeve.get(sleeve_id, 0)) for sleeve_id in expected_ids},
        "rejected_intent_count_by_sleeve": {row["sleeve_id"]: row["rejected_intent_count"] for row in sleeves},
        "data_quality_by_sleeve": {
            row["sleeve_id"]: {
                "market_data_status": row["market_data_status"],
                "classification": row["classification"],
                "blocker_reason": row["blocker_reason"],
            }
            for row in sleeves
        },
    }
    payload = {
        "schema_id": "aegis_sleeve_evaluation",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at": _now(),
        "status": "PARTIAL" if summary["blocked_sleeves"] else "READY",
        "summary": summary,
        "sleeves": sleeves,
        "source_artifact_paths": {**{name: str(path) for name, path in paths.items() if path.exists()}, **({"market_calendar": str(calendar_context.get("path"))} if calendar_context.get("path") else {})},
        "source_hashes": {**{name: _sha256(path) for name, path in paths.items() if path.exists()}, **({"market_calendar": _sha256(Path(str(calendar_context.get("path"))))} if calendar_context.get("path") else {})},
        "notes": [
            "This artifact classifies sleeve silence from existing run, diagnostics, and contract artifacts.",
            "It does not create candidates, modify sleeve thresholds, change sleeve logic, or provide trade advice.",
        ],
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_sleeve_evaluation_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_sleeve_evaluation_v1(truth_root=root, day_utc=day_utc))
    path = sleeve_evaluation_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path



def _read_jsonl_rows_v1(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _calendar_context_v1(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    for row in _read_jsonl_rows_v1(path):
        row_day = str(row.get("date") or row.get("day_utc") or "")[:10]
        if row_day == day_utc:
            return {"path": str(path), "is_trading_session": row.get("is_trading_session"), "market_session_label": row.get("session_label") or row.get("trading_day_type") or ""}
    return {"path": str(path) if path.exists() else "", "is_trading_session": None, "market_session_label": "UNKNOWN"}

def _build_sleeve_row(
    *,
    sleeve_id: str,
    diagnostic_row: Mapping[str, Any],
    rollup_row: Mapping[str, Any],
    candidate_count: int,
    last_candidate_date: str | None,
    last_successful_run: str | None,
    calendar_context: Mapping[str, Any],
) -> dict[str, Any]:
    enabled = _first_bool(diagnostic_row.get("enabled"), rollup_row.get("enabled"), True)
    is_trading_session = calendar_context.get("is_trading_session")
    expected_to_run = False if is_trading_session is False else _first_bool(diagnostic_row.get("expected_today"), diagnostic_row.get("expected_to_run"), enabled)
    run_artifact_present = bool(rollup_row or diagnostic_row.get("evaluation_artifact_path"))
    status = _first_text(rollup_row.get("status"), diagnostic_row.get("evaluation_status")).upper()
    # The kernel rollup is the current runtime result. Older diagnostics can
    # retain a stale blocker from a prior invocation, so use them only when no
    # rollup row exists. An empty rollup blocker means the current run cleared it.
    blocker = str(rollup_row.get("canonical_blocker") or "").strip() if rollup_row else _first_text(diagnostic_row.get("canonical_blocker"))
    if rollup_row and not rollup_row.get("data_status") and str(rollup_row.get("status") or "").upper() in {"INTENT_CREATED", "NO_INTENT", "FILTERED_OUT"}:
        market_data_status = "OK"
    else:
        market_data_status = _first_text(rollup_row.get("data_status"), diagnostic_row.get("data_status"), "UNKNOWN").upper()
    if rollup_row and not rollup_row.get("can_run_candidate_generation") and str(rollup_row.get("status") or "").upper() in {"INTENT_CREATED", "NO_INTENT", "FILTERED_OUT"} and not blocker:
        can_run = True
    else:
        can_run = _first_bool(rollup_row.get("can_run_candidate_generation"), diagnostic_row.get("can_run_candidate_generation"), True)
    output_count = int(_first_number(rollup_row.get("output_intents"), diagnostic_row.get("candidate_count"), 0) or 0)
    rejected_rows = _rejected_rows_for_sleeve(sleeve_id, diagnostic_row, rollup_row)
    rejected_count = int(_first_number(rollup_row.get("rejected_intents"), diagnostic_row.get("raw_signal_rejection_count"), len(rejected_rows)) or 0)
    if rejected_count == 0:
        rejected_count = len(rejected_rows)
    actually_ran = bool(
        enabled
        and expected_to_run
        and run_artifact_present
        and can_run
        and market_data_status not in {"MISSING", "BLOCKED"}
    )
    nearest = _nearest_miss_summary(rejected_rows)
    missing_inputs = _explicit_missing_inputs(diagnostic_row, rollup_row)
    classification, explanation, repair_action, should_modify, recommendation = _classify(
        enabled=enabled,
        expected_to_run=expected_to_run,
        actually_ran=actually_ran,
        run_artifact_present=run_artifact_present,
        status=status,
        blocker=blocker,
        market_data_status=market_data_status,
        can_run=can_run,
        candidate_count=candidate_count,
        output_count=output_count,
        rejected_count=rejected_count,
        missing_inputs=missing_inputs,
        reason_codes=_as_list(rollup_row.get("reason_codes")) + _as_list(rollup_row.get("reasons")) + (["NON_TRADING_DAY_NO_SLEEVE_RUN_EXPECTED"] if is_trading_session is False else []),
    )
    return {
        "sleeve_id": sleeve_id,
        "sleeve_name": _first_text(diagnostic_row.get("sleeve_name"), rollup_row.get("sleeve_name"), sleeve_id),
        "enabled": enabled,
        "expected_to_run": expected_to_run,
        "actually_ran": actually_ran,
        "run_artifact_present": run_artifact_present,
        "produced_output_intents": output_count > 0,
        "produced_rejected_intents": rejected_count > 0,
        "produced_candidates": candidate_count > 0,
        "blocker_reason": blocker or ("NON_TRADING_DAY_NO_SLEEVE_RUN_EXPECTED" if is_trading_session is False else _first_text(";".join(_as_list(rollup_row.get("reason_codes"))), "")),
        "last_successful_run": last_successful_run,
        "last_candidate_date": last_candidate_date,
        "output_intent_count": output_count,
        "rejected_intent_count": rejected_count,
        "candidate_count": candidate_count,
        "market_data_status": market_data_status,
        "missing_input_artifacts": missing_inputs,
        "missing_symbol_or_field_data": _missing_symbol_or_field_data(missing_inputs),
        "required_producer": _required_producer_for_missing_inputs(missing_inputs),
        "block_expected_or_defective": _block_expected_or_defective(classification, missing_inputs),
        "universe_size": _universe_size(diagnostic_row, rollup_row),
        "eligible_universe_size": _eligible_universe_size(diagnostic_row, rollup_row),
        "threshold_summary": nearest["threshold_summary"],
        "nearest_miss_count": nearest["nearest_miss_count"],
        "top_nearest_misses": nearest["top_nearest_misses"],
        "classification": classification,
        "explanation": explanation,
        "repair_action": repair_action,
        "should_modify_logic": should_modify,
        "modification_recommendation": recommendation,
    }


def _classify(
    *,
    enabled: bool,
    expected_to_run: bool,
    actually_ran: bool,
    run_artifact_present: bool,
    status: str,
    blocker: str,
    market_data_status: str,
    can_run: bool,
    candidate_count: int,
    output_count: int,
    rejected_count: int,
    missing_inputs: list[str],
    reason_codes: list[str],
) -> tuple[str, str, str, bool, str]:
    blocker_upper = blocker.upper()
    reasons = {str(item).upper() for item in reason_codes + missing_inputs if str(item).strip()}
    if candidate_count > 0 or output_count > 0:
        return (
            "OUTPUT_PRODUCED",
            "The sleeve produced a candidate or output intent for the target day.",
            "No silent-sleeve repair action required.",
            False,
            "No sleeve logic change recommended from silent-sleeve evaluation.",
        )
    if not enabled:
        return (
            "SILENT_CONFIG_DISABLED",
            "The sleeve is configured disabled, so silence is expected and not treated as a runtime failure.",
            "Review sleeve configuration only if this sleeve should be active.",
            False,
            "Do not modify sleeve logic; decide whether configuration should enable the sleeve.",
        )
    if not expected_to_run:
        if "NON_TRADING_DAY_NO_SLEEVE_RUN_EXPECTED" in reasons:
            return (
                "SILENT_MARKET_CONDITION_NOT_MET",
                "The target day is not a trading session, so sleeve runs and output intents are not expected.",
                "No repair needed unless the market calendar is wrong.",
                False,
                "No sleeve logic change recommended; silence is expected on non-trading days.",
            )
        return (
            "SILENT_CONFIG_DISABLED",
            "The sleeve was enabled but not expected to run on the target day or cadence.",
            "Check cadence/configuration before investigating signal thresholds.",
            False,
            "Do not modify sleeve logic unless the cadence/configuration is wrong.",
        )
    if not run_artifact_present:
        return (
            "SILENT_DATA_BLOCKED",
            "No current-day sleeve run artifact or diagnostics row was available, so Aegis cannot prove a runtime failure for this sleeve.",
            "Regenerate the current-day sleeve run/diagnostic input artifacts, then rerun sleeve evaluation.",
            False,
            "Do not modify sleeve logic until current-day run artifacts exist.",
        )
    if market_data_status in {"MISSING", "BLOCKED"} or not can_run or blocker_upper in {"MISSING_REQUIRED_INPUTS", "SLEEVE_INPUT_REQUIREMENT_BLOCKED"} or {"MISSING_REQUIRED_INPUTS", "SLEEVE_INPUT_REQUIREMENT_BLOCKED"} & reasons:
        missing_detail = ", ".join(missing_inputs) if missing_inputs else "required sleeve inputs"
        return (
            "SILENT_DATA_BLOCKED",
            f"The sleeve was expected but required data or input contracts were missing or blocked: {missing_detail}.",
            "Repair the named missing inputs with their required producer, then rerun sleeve evaluation.",
            False,
            "Do not modify sleeve logic until required data inputs are available.",
        )
    if blocker_upper in {"PRODUCER_NONZERO_RC", "ENGINE_INVOCATION_TIMEOUT"} or status in {"BLOCKED", "DEGRADED"}:
        return (
            "SILENT_RUNTIME_FAILURE",
            f"The sleeve runner did not complete cleanly ({blocker_upper or status}).",
            "Inspect the sleeve producer command stderr/stdout and repair the runtime failure.",
            False,
            "Repair implementation/runtime failure before reviewing thresholds.",
        )
    if rejected_count > 0:
        return (
            "SILENT_THRESHOLD_TOO_STRICT",
            "The sleeve generated rejected intents but no output candidates, indicating filters or thresholds blocked promotion.",
            "Review rejected-intent reasons and nearest-miss rows before considering threshold changes.",
            True,
            "Threshold review may be warranted, but no logic change is made by this report.",
        )
    if status == "NO_INTENT" and not blocker_upper and ("NO_INTENT_DECLARED" in reasons or not reason_codes):
        return (
            "SILENT_MARKET_CONDITION_NOT_MET",
            "The sleeve ran and explicitly declared no intent; current market conditions did not meet signal requirements.",
            "No repair needed unless this persists beyond expected market regimes.",
            False,
            "No sleeve logic change recommended from the available evidence.",
        )
    if actually_ran and status in {"NO_INTENT", "FILTERED_OUT", ""}:
        return (
            "SILENT_MARKET_CONDITION_NOT_MET",
            "The sleeve ran with usable inputs but declared no intent; current market conditions did not meet signal requirements.",
            "No repair needed unless this persists beyond expected market regimes.",
            False,
            "No sleeve logic change recommended from the available evidence.",
        )
    return (
        "SILENT_UNKNOWN_REQUIRES_INVESTIGATION",
        "The sleeve was silent, but existing artifacts do not explain whether the cause is data, runtime, or threshold related.",
        "Inspect sleeve input contracts, producer logs, and candidate diagnostics for the target day.",
        False,
        "Do not modify sleeve logic until missing evidence is resolved.",
    )


def _explicit_missing_inputs(diagnostic_row: Mapping[str, Any], rollup_row: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    sources = (rollup_row,) if rollup_row else (diagnostic_row,)
    for source in sources:
        values.extend(str(item) for item in _as_list(source.get("missing_inputs")) if str(item).strip())
        values.extend(str(item) for item in _as_list(source.get("blocking_inputs")) if str(item).strip())
    if not values and rollup_row and str(rollup_row.get("status") or "").upper() in {"BLOCKED", "DEGRADED"}:
        values.extend(str(item) for item in _as_list(diagnostic_row.get("missing_inputs")) if str(item).strip())
        values.extend(str(item) for item in _as_list(diagnostic_row.get("blocking_inputs")) if str(item).strip())
    readiness = rollup_row.get("sleeve_readiness") if isinstance(rollup_row.get("sleeve_readiness"), Mapping) else {}
    values.extend(str(item) for item in _as_list(readiness.get("blocking_inputs")) if str(item).strip())
    stderr = str(rollup_row.get("stderr_summary") or "")
    marker = "MISSING_REQUIRED_INPUTS:"
    if marker in stderr:
        tail = stderr.split(marker, 1)[1].split("\n", 1)[0]
        values.extend(part.strip() for part in tail.split(";") if part.strip())
    return sorted(dict.fromkeys(values))


def _missing_symbol_or_field_data(missing_inputs: list[str]) -> list[str]:
    out: list[str] = []
    for item in missing_inputs:
        text = str(item)
        if text.startswith("market.price.") or text.startswith("market.volatility."):
            out.append(text)
        elif "market_data" in text or "snapshot" in text:
            out.append(text)
    return sorted(dict.fromkeys(out))


def _required_producer_for_missing_inputs(missing_inputs: list[str]) -> str:
    joined = " ".join(str(item) for item in missing_inputs)
    if "market.volatility.VIX" in joined or "market.price." in joined:
        return "TARGET_DAY=<day> npm run aegis:market-data-inputs; TARGET_DAY=<day> npm run aegis:sleeve-evaluation"
    if "market_data_snapshot_v1" in joined or "accounting_v1/nav" in joined or "positions_snapshot_v2" in joined:
        return "Run the legacy sleeve input compatibility snapshot producer for market_data_snapshot_v1, accounting_v1/nav, and positions_snapshot_v2, then rerun sleeve evaluation."
    if missing_inputs:
        return "Run the producer for the named missing sleeve inputs, then rerun sleeve evaluation."
    return "No repair producer required."


def _block_expected_or_defective(classification: str, missing_inputs: list[str]) -> str:
    if classification != "SILENT_DATA_BLOCKED":
        return "NOT_BLOCKED"
    if any(str(item).startswith("market.") for item in missing_inputs):
        return "DEFECTIVE_BINDING_OR_MISSING_MARKET_DATA"
    if missing_inputs:
        return "DEFECTIVE_MISSING_REQUIRED_ARTIFACT"
    return "DEFECTIVE_UNEXPLAINED_DATA_BLOCK"


def _nearest_miss_summary(rejected_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rejected_rows:
        return {
            "nearest_miss_count": 0,
            "threshold_summary": "NEAREST_MISS_DATA_MISSING",
            "top_nearest_misses": [{"status": "NEAREST_MISS_DATA_MISSING", "detail": "No rejected-intent or nearest-miss rows were present in source artifacts."}],
        }
    reason_counts = Counter(_first_text(row.get("reason"), row.get("rejection_reason"), row.get("status"), "REJECTED") for row in rejected_rows)
    top_reason, top_count = reason_counts.most_common(1)[0]
    return {
        "nearest_miss_count": len(rejected_rows),
        "threshold_summary": f"{len(rejected_rows)} rejected intents; top blocker: {top_reason} ({top_count}). Exact within-10-percent threshold distance is unavailable unless producer scores and thresholds are present.",
        "top_nearest_misses": [
            {
                "symbol": _first_text(row.get("symbol"), row.get("underlying_symbol"), row.get("ticker"), "UNKNOWN"),
                "reason": _first_text(row.get("reason"), row.get("rejection_reason"), row.get("status"), "REJECTED"),
                "score": _first_number(row.get("score"), row.get("portfolio_score_total"), row.get("signal_score")),
                "threshold": _first_number(row.get("threshold"), row.get("minimum_score"), row.get("promotion_threshold")),
            }
            for row in rejected_rows[:5]
        ],
    }


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "candidate_generation_diagnostics_v1": root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        "sleeve_evaluation_kernel_v1": root / "reports" / "sleeve_evaluation_kernel_v1" / day / "sleeve_evaluation_rollup.v1.json",
        "sleeve_input_contracts_v1": root / "reports" / "aegis_sleeve_input_contracts_v1" / day / "sleeve_input_contracts.v1.json",
        "sleeve_readiness_v1": root / "reports" / "aegis_sleeve_readiness_v1" / day / "sleeve_readiness.v1.json",
        "candidate_contracts_v1": root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json",
        "signal_evidence_boundary_v1": root / "reports" / "aegis_signal_evidence_boundary_v1" / day / "signal_evidence_boundary.v1.json",
    }


def _expected_sleeve_ids(diagnostics: Mapping[str, Any], rollup: Mapping[str, Any]) -> list[str]:
    values = _as_list(diagnostics.get("expected_sleeve_ids")) or _as_list(diagnostics.get("enabled_sleeve_ids"))
    if not values:
        values = [str(row.get("sleeve_id") or "") for row in _rows(diagnostics, "sleeves")]
    if not values:
        values = [str(row.get("sleeve_id") or "") for row in _rows(rollup, "outcomes")]
    return sorted({str(value).strip() for value in values if str(value).strip() and str(value).strip() != "C2_INTENT_SIMULATOR_V1"})


def _rejected_rows_for_sleeve(sleeve_id: str, diagnostic_row: Mapping[str, Any], rollup_row: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("raw_signal_rejections", "rejected_raw_signals", "rejected_intent_rows", "rejected_intents_detail"):
        rows.extend(_dict_rows(diagnostic_row.get(key)))
        rows.extend(_dict_rows(rollup_row.get(key)))
    filtered = []
    for row in rows:
        row_sleeve = str(row.get("sleeve_id") or row.get("engine_id") or sleeve_id)
        if row_sleeve == sleeve_id:
            filtered.append(row)
    return filtered


def _last_candidate_dates(root: Path, day: str) -> dict[str, str]:
    out: dict[str, str] = {}
    base = root / "reports" / "aegis_candidate_contracts_v1"
    for path in sorted(base.glob("*/candidate_contracts.v1.json")):
        path_day = path.parent.name
        if path_day > day:
            continue
        for row in _rows(_read_json(path), "candidate_contracts", "contracts", "candidates"):
            sleeve_id = str(row.get("sleeve_id") or "").strip()
            if sleeve_id:
                out[sleeve_id] = path_day
    return out


def _last_successful_runs(root: Path, day: str) -> dict[str, str]:
    out: dict[str, str] = {}
    base = root / "reports" / "sleeve_evaluation_kernel_v1"
    for path in sorted(base.glob("*/sleeve_evaluation_rollup.v1.json")):
        path_day = path.parent.name
        if path_day > day:
            continue
        payload = _read_json(path)
        for row in _rows(payload, "outcomes"):
            sleeve_id = str(row.get("sleeve_id") or "").strip()
            status = str(row.get("status") or "").upper()
            if sleeve_id and status in {"INTENT_CREATED", "NO_INTENT", "FILTERED_OUT"}:
                out[sleeve_id] = path_day
    return out


def _universe_size(diagnostic_row: Mapping[str, Any], rollup_row: Mapping[str, Any]) -> int | None:
    allowed = _as_list(diagnostic_row.get("allowed_symbols")) or _as_list(rollup_row.get("allowed_symbols"))
    value = _first_number(diagnostic_row.get("universe_size"), rollup_row.get("universe_size"))
    if value is not None:
        return int(value)
    return len(allowed) if allowed else None


def _eligible_universe_size(diagnostic_row: Mapping[str, Any], rollup_row: Mapping[str, Any]) -> int | None:
    value = _first_number(diagnostic_row.get("eligible_universe_size"), rollup_row.get("eligible_universe_size"))
    if value is not None:
        return int(value)
    if _first_bool(diagnostic_row.get("can_run_candidate_generation"), None, True):
        return _universe_size(diagnostic_row, rollup_row)
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _dict_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(row) for row in value if isinstance(row, Mapping)]
    if isinstance(value, Mapping):
        return [dict(value)]
    return []


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value in (None, "", "n/a") or isinstance(value, bool):
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return None


def _first_bool(*values: Any) -> bool:
    for value in values:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            continue
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "enabled"}:
            return True
        if text in {"false", "0", "no", "disabled"}:
            return False
    return False


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
