from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.operator_state.trade_candidate_contract_v1 import content_hash_file_v1


REPORT_FAMILY = "aegis_signal_evidence_graph_v1"
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
EQUITY_POLICY_RELPATH = Path("governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json")
OPTIONS_POLICY_RELPATH = Path("governance/02_REGISTRIES/C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json")
ENGINE_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json")
EVENT_DISLOCATION_ENGINE_ID = "C2_EVENT_DISLOCATION_V1"
UNGOVERNED_SYMBOL_SUPPRESSED = "UNGOVERNED_SYMBOL_SUPPRESSED"

FAILURE_NOT_DEMANDED = "EVIDENCE_NOT_DEMANDED"
FAILURE_NOT_FETCHED = "EVIDENCE_NOT_FETCHED"
FAILURE_NOT_CERTIFIED = "EVIDENCE_NOT_CERTIFIED"
FAILURE_STALE = "EVIDENCE_STALE"
FAILURE_SYMBOL_MISMATCH = "EVIDENCE_SYMBOL_MISMATCH"
FAILURE_FIELD_UNFULFILLED = "CANDIDATE_FIELD_UNFULFILLED"
FAILURE_ARBITRATION_MISSING = "ARBITRATION_EVIDENCE_MISSING"

PRICE_PURPOSE = "ENTRY_REFERENCE_PRICE"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _upper(value) in {"1", "TRUE", "YES", "PASS", "VALID", "CURRENT", "CERTIFIED"}


def _is_numeric(value: Any) -> bool:
    try:
        float(str(value))
        return True
    except Exception:
        return False


def _is_paper_rehearsal_ref(*values: Any) -> bool:
    return any("paper_rehearsal" in _text(value).lower() for value in values)


def _executed_sleeve(row: dict[str, Any]) -> bool:
    status = _upper(row.get("status") or row.get("current_status"))
    signal_state = _upper(_safe_dict(row.get("signal_state")).get("state"))
    try:
        output_count = int(row.get("output_count") or 0)
    except (TypeError, ValueError):
        output_count = 0
    return status in {"NO_INTENT", "INTENT_CREATED", "FILTERED_OUT", "BLOCKED"} or signal_state == "ACTIVE" or output_count > 0


def _iter_sleeve_outcomes(root: Path, day_utc: str) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    rollup_path = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc / "sleeve_evaluation_rollup.v1.json"
    rollup = read_json_v1(rollup_path)
    for row in _safe_list(rollup.get("outcomes") or rollup.get("sleeve_outcomes")):
        if isinstance(row, dict):
            outcomes.append(row)
    known = {_upper(row.get("sleeve_id") or row.get("engine_id")) for row in outcomes}
    family_root = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc
    if family_root.exists():
        for child in sorted(family_root.iterdir()):
            if not child.is_dir():
                continue
            sleeve_id = child.name.strip().upper()
            if not sleeve_id or sleeve_id in known:
                continue
            payload = read_json_v1(child / "sleeve_evaluation.v1.json")
            if payload:
                outcomes.append(payload)
    return outcomes


def _collect_real_raw_signals(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in _iter_sleeve_outcomes(root, day_utc):
        sleeve_id = _upper(row.get("sleeve_id") or row.get("engine_id"))
        if not sleeve_id or sleeve_id == SIMULATOR_ENGINE_ID or not _executed_sleeve(row):
            continue
        source_artifact = _text(row.get("artifact_path"))
        batch = _safe_dict(row.get("exposure_intent_batch"))
        intents = _safe_list(batch.get("output_intents")) or _safe_list(row.get("output_intents"))
        for item in intents:
            if not isinstance(item, dict):
                continue
            raw_signal_id = _text(item.get("raw_signal_id") or item.get("intent_id") or item.get("intent_hash"))
            symbol = _upper(item.get("symbol") or item.get("symbol_or_pair"))
            evidence_path = _text(item.get("intent_path") or item.get("raw_intent_path") or source_artifact)
            if not raw_signal_id or not symbol or _is_paper_rehearsal_ref(raw_signal_id, evidence_path):
                continue
            key = (sleeve_id, symbol, raw_signal_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "raw_signal_id": raw_signal_id,
                    "intent_id": _text(item.get("intent_id")),
                    "intent_hash": _text(item.get("intent_hash")),
                    "sleeve_id": sleeve_id,
                    "symbol": symbol,
                    "signal_type": _text(item.get("schema_id") or "exposure_intent"),
                    "source_artifact_path": source_artifact,
                    "source_artifact_hash": content_hash_file_v1(Path(source_artifact).expanduser()) if source_artifact and Path(source_artifact).expanduser().exists() else "",
                    "evidence_path": evidence_path,
                    "graph_linkage": f"sleeve_evaluation_kernel_v1.{sleeve_id}",
                    "reason_codes": [str(code) for code in _safe_list(row.get("reason_codes")) if _text(code)],
                    "lifecycle_reason_codes": [str(code) for code in _safe_list(row.get("lifecycle_reason_codes")) if _text(code)],
                    "sleeve_status": _upper(row.get("status") or row.get("current_status")),
                }
            )
    return out


def _load_structure_policies(repo_root: Path) -> tuple[dict[str, str], list[dict[str, Any]]]:
    policy_paths = {
        "equity_structure_policy": str((repo_root / EQUITY_POLICY_RELPATH).resolve()),
        "exposure_to_options_policy": str((repo_root / OPTIONS_POLICY_RELPATH).resolve()),
    }
    return policy_paths, [read_json_v1(repo_root / EQUITY_POLICY_RELPATH), read_json_v1(repo_root / OPTIONS_POLICY_RELPATH)]


def _engine_registry(repo_root: Path) -> tuple[str, dict[str, Any]]:
    path = (repo_root / ENGINE_REGISTRY_RELPATH).resolve()
    return str(path), read_json_v1(path)


def _engine_row(engine_registry: dict[str, Any], sleeve_id: str) -> dict[str, Any]:
    for key in ("engines", "engine_models", "models", "rows"):
        for row in _safe_list(engine_registry.get(key)):
            if isinstance(row, dict) and _upper(row.get("engine_id") or row.get("sleeve_id")) == sleeve_id:
                return row
    return {}


def _governed_symbols_for_sleeve(engine_registry: dict[str, Any], sleeve_id: str) -> list[str]:
    row = _engine_row(engine_registry, sleeve_id)
    return sorted({_upper(item) for item in _safe_list(row.get("allowed_symbols")) if _upper(item)})


def _policy_for_sleeve(policy_payloads: list[dict[str, Any]], sleeve_id: str) -> dict[str, Any]:
    for payload in policy_payloads:
        for row in _safe_list(payload.get("engine_policies")):
            if isinstance(row, dict) and _upper(row.get("engine_id")) == sleeve_id:
                return row
    return {}


def _deterministic_intent_id(signal: dict[str, Any], intent: dict[str, Any]) -> str:
    existing = _text(intent.get("intent_id") or signal.get("intent_id") or signal.get("raw_signal_id"))
    if existing:
        return existing
    return "intent_" + canonical_hash_for_c2_artifact_v1(
        {
            "raw_signal_id": signal.get("raw_signal_id"),
            "sleeve_id": signal.get("sleeve_id"),
            "symbol": signal.get("symbol"),
            "schema": "aegis_signal_evidence_graph_v1.intent_id",
        }
    )[:24]


def _derive_direction_and_instrument(intent: dict[str, Any], policy_row: dict[str, Any]) -> tuple[str, str]:
    exposure_type = _upper(intent.get("exposure_type") or _safe_dict(policy_row.get("exposure_requirements")).get("exposure_type"))
    structure_type = _upper(_safe_dict(policy_row.get("structure_template")).get("structure_type"))
    allowed_action = _upper(_safe_dict(policy_row.get("structure_template")).get("allowed_action"))
    if exposure_type == "LONG_EQUITY" and structure_type == "EQUITY_SPOT" and allowed_action in {"BUY", ""}:
        return "LONG", "LONG_EQUITY"
    options_template = _safe_dict(policy_row.get("options_template"))
    option_requirements = _safe_dict(policy_row.get("exposure_requirements"))
    option_direction = _upper(option_requirements.get("required_option_direction") or _safe_dict(options_template.get("strategy")).get("direction"))
    if exposure_type == "SHORT_VOL_DEFINED" and options_template and option_direction in {"SELL", "CREDIT"}:
        return "SELL", "SHORT_VOL_DEFINED"
    return "", ""


def _item_by_id(payload: dict[str, Any], data_item_id: str) -> dict[str, Any]:
    for row in _safe_list(payload.get("data_items")):
        if isinstance(row, dict) and _text(row.get("data_item_id")) == data_item_id:
            return row
    return {}


def _input_record_for_item(payload: dict[str, Any], data_item_id: str) -> dict[str, Any]:
    for row in _safe_list(payload.get("input_records")):
        if isinstance(row, dict) and _text(row.get("data_item_id")) == data_item_id:
            return row
    return {}


def _entry_price_certification_by_key(payload: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _safe_list(payload.get("rows")):
        if not isinstance(row, dict):
            continue
        key = (_upper(row.get("sleeve_id")), _upper(row.get("symbol")), _text(row.get("raw_signal_id")))
        if all(key):
            out[key] = row
    return out


def _certification_failure_to_evidence_reason(status: str) -> str:
    status = _upper(status)
    if status == "CERTIFIED":
        return ""
    if status == "UNCERTIFIED_MISSING_PRICE":
        return FAILURE_NOT_FETCHED
    if status in {"UNCERTIFIED_MISSING_SOURCE", "UNCERTIFIED_BAD_SCHEMA", "UNCERTIFIED_SOURCE_NOT_ALLOWED", "UNCERTIFIED_UNKNOWN"}:
        return FAILURE_NOT_CERTIFIED
    if status == "UNCERTIFIED_STALE_PRICE" or status == "UNCERTIFIED_MARKET_CLOSED":
        return FAILURE_STALE
    if status == "UNCERTIFIED_BAD_SYMBOL":
        return FAILURE_SYMBOL_MISMATCH
    return FAILURE_NOT_CERTIFIED


def _artifact_hash(path_text: str) -> str:
    path = Path(path_text).expanduser() if path_text else None
    if path and path.exists() and path.is_file():
        return content_hash_file_v1(path.resolve())
    return ""


def _base_evidence(*, evidence_id: str, data_item_id: str, purpose: str, freshness_requirement: str, certification_required: bool, demanded: bool) -> dict[str, Any]:
    return {
        "evidence_id": evidence_id,
        "data_item_id": data_item_id,
        "purpose": purpose,
        "freshness_requirement": freshness_requirement,
        "freshness_policy_mode": freshness_requirement,
        "freshness_window_seconds": 0,
        "stale_by_seconds": None,
        "stale_reason": "",
        "certification_required": bool(certification_required),
        "demanded": bool(demanded),
        "fetched": False,
        "certified": False,
        "consumed": False,
        "source_artifact_path": "",
        "source_hash": "",
        "failure_reason": FAILURE_NOT_DEMANDED if not demanded else "",
        "value": None,
        "provider": "",
        "timestamp_utc": "",
        "session_date": "",
        "field": "",
    }


def _price_evidence_row(*, symbol: str, day_utc: str, data_registry_payload: dict[str, Any], market_inputs_payload: dict[str, Any], market_inputs_path: Path, certification_row: dict[str, Any] | None = None) -> dict[str, Any]:
    data_item_id = f"market.price.{symbol}"
    cert = certification_row if isinstance(certification_row, dict) else {}
    if cert:
        status = _upper(cert.get("certification_status"))
        row = _base_evidence(
            evidence_id=f"{PRICE_PURPOSE}:{symbol}",
            data_item_id=data_item_id,
            purpose=PRICE_PURPOSE,
            freshness_requirement="CURRENT_SESSION_CERTIFIED",
            certification_required=True,
            demanded=True,
        )
        row.update(
            {
                "fetched": bool(_text(cert.get("price"))),
                "certified": status == "CERTIFIED",
                "source_artifact_path": _text(cert.get("source_artifact")),
                "source_hash": _text(cert.get("source_hash")),
                "provider": _text(cert.get("source")),
                "timestamp_utc": _text(cert.get("price_timestamp")),
                "session_date": _text(cert.get("market_session")),
                "field": "price",
                "value": _text(cert.get("price")) if status == "CERTIFIED" else None,
                "failure_reason": _certification_failure_to_evidence_reason(status),
                "stale_reason": ";".join(str(code) for code in _safe_list(cert.get("certification_reason_codes")) if _text(code)) or status,
                "freshness_window_seconds": int(cert.get("freshness_window_seconds") or 0),
                "entry_reference_price_certification_status": status,
                "entry_reference_price_certification_reason_codes": _safe_list(cert.get("certification_reason_codes")),
                "entry_reference_price_certification_id": _text(cert.get("certification_id")),
                "entry_reference_price_certification_path": _text(cert.get("certification_artifact_path")),
                "safe_repair_available": bool(cert.get("safe_repair_available")),
            }
        )
        return row
    row = _base_evidence(
        evidence_id=f"{PRICE_PURPOSE}:{symbol}",
        data_item_id=data_item_id,
        purpose=PRICE_PURPOSE,
        freshness_requirement="CURRENT_SESSION_CERTIFIED",
        certification_required=True,
        demanded=True,
    )
    registry_row = _item_by_id(data_registry_payload, data_item_id)
    input_row = _input_record_for_item(market_inputs_payload, data_item_id)
    if input_row:
        row.update(
            {
                "fetched": True,
                "source_artifact_path": str(market_inputs_path.resolve()) if market_inputs_path.exists() else "",
                "source_hash": _artifact_hash(str(market_inputs_path.resolve())) if market_inputs_path.exists() else "",
                "provider": _text(input_row.get("source_vendor")),
                "timestamp_utc": _text(input_row.get("source_timestamp_utc")),
                "session_date": _text(input_row.get("day_utc")),
                "field": _text(input_row.get("field_type") or "value"),
            }
        )
    if not registry_row:
        row["failure_reason"] = FAILURE_NOT_CERTIFIED if input_row else FAILURE_NOT_FETCHED
        return row
    row.update(
        {
            "fetched": True,
            "source_artifact_path": _text(registry_row.get("source_artifact_path")) or row.get("source_artifact_path") or "",
            "source_hash": _text(registry_row.get("source_hash")) or row.get("source_hash") or "",
            "provider": _text(registry_row.get("provider")) or row.get("provider") or "",
            "timestamp_utc": _text(registry_row.get("data_timestamp_utc")) or row.get("timestamp_utc") or "",
            "session_date": _text(registry_row.get("market_session_date")) or row.get("session_date") or "",
            "field": _text(registry_row.get("field") or row.get("field") or "value"),
        }
    )
    if _upper(registry_row.get("symbol")) not in {"", symbol}:
        row["failure_reason"] = FAILURE_SYMBOL_MISMATCH
        row["stale_reason"] = row["failure_reason"]
        return row
    registry_status = _upper(registry_row.get("status"))
    if registry_status in {"", "MISSING", "NOT_FETCHED"}:
        row["fetched"] = False
        row["source_artifact_path"] = ""
        row["source_hash"] = ""
        row["provider"] = ""
        row["timestamp_utc"] = ""
        row["session_date"] = ""
        row["failure_reason"] = FAILURE_NOT_FETCHED
        row["stale_reason"] = ";".join(str(item) for item in _safe_list(registry_row.get("notes")) if _text(item)) or "PRICE_EVIDENCE_NOT_FETCHED"
        return row
    if registry_status != "CURRENT" or _text(registry_row.get("market_session_date")) != day_utc:
        row["failure_reason"] = FAILURE_STALE
        row["stale_reason"] = f"registry_status={registry_status or 'UNKNOWN'};market_session_date={_text(registry_row.get('market_session_date')) or 'UNKNOWN'};required_day={day_utc}"
        return row
    if _upper(registry_row.get("market_data_validation_status")) != "VALID":
        row["failure_reason"] = FAILURE_NOT_CERTIFIED
        row["stale_reason"] = row["failure_reason"]
        return row
    if not row["source_artifact_path"] or not row["source_hash"] or not row["provider"]:
        row["failure_reason"] = FAILURE_NOT_CERTIFIED
        row["stale_reason"] = row["failure_reason"]
        return row
    actual_hash = _artifact_hash(row["source_artifact_path"])
    if not actual_hash or actual_hash != row["source_hash"]:
        row["source_hash"] = actual_hash
        row["failure_reason"] = FAILURE_NOT_CERTIFIED
        row["stale_reason"] = row["failure_reason"]
        return row
    if not input_row:
        row["failure_reason"] = FAILURE_SYMBOL_MISMATCH
        row["stale_reason"] = row["failure_reason"]
        return row
    if _upper(input_row.get("symbol")) != symbol or _text(input_row.get("data_item_id")) != data_item_id:
        row["failure_reason"] = FAILURE_SYMBOL_MISMATCH
        row["stale_reason"] = row["failure_reason"]
        return row
    if _upper(input_row.get("validation_status")) != "VALID" or _text(input_row.get("day_utc")) != day_utc:
        row["failure_reason"] = FAILURE_STALE
        row["stale_reason"] = f"input_validation_status={_upper(input_row.get('validation_status')) or 'UNKNOWN'};input_day={_text(input_row.get('day_utc')) or 'UNKNOWN'};required_day={day_utc}"
        return row
    if not _is_numeric(input_row.get("value")):
        row["failure_reason"] = FAILURE_NOT_CERTIFIED
        row["stale_reason"] = row["failure_reason"]
        return row
    row.update(
        {
            "certified": True,
            "value": _text(input_row.get("value")),
            "provider": _text(input_row.get("source_vendor") or row.get("provider")),
            "timestamp_utc": _text(input_row.get("source_timestamp_utc") or row.get("timestamp_utc")),
            "session_date": _text(input_row.get("day_utc") or row.get("session_date")),
            "field": _text(input_row.get("field_type") or row.get("field") or "value"),
            "failure_reason": "",
        }
    )
    return row


def _registry_backed_evidence(*, evidence_id: str, data_item_id: str, purpose: str, demanded: bool, day_utc: str, data_registry_payload: dict[str, Any]) -> dict[str, Any]:
    row = _base_evidence(
        evidence_id=evidence_id,
        data_item_id=data_item_id,
        purpose=purpose,
        freshness_requirement="CURRENT_SESSION_CERTIFIED",
        certification_required=True,
        demanded=demanded,
    )
    if not demanded:
        return row
    registry_row = _item_by_id(data_registry_payload, data_item_id)
    if not registry_row:
        row["failure_reason"] = FAILURE_NOT_FETCHED
        return row
    row.update(
        {
            "fetched": True,
            "source_artifact_path": _text(registry_row.get("source_artifact_path")),
            "source_hash": _text(registry_row.get("source_hash")),
            "provider": _text(registry_row.get("provider")),
            "timestamp_utc": _text(registry_row.get("data_timestamp_utc")),
            "session_date": _text(registry_row.get("market_session_date")),
            "field": _text(registry_row.get("field") or "value"),
        }
    )
    if _upper(registry_row.get("status")) != "CURRENT" or _text(registry_row.get("market_session_date")) != day_utc:
        row["failure_reason"] = FAILURE_STALE
        return row
    if _upper(registry_row.get("market_data_validation_status")) != "VALID":
        row["failure_reason"] = FAILURE_NOT_CERTIFIED
        return row
    row["certified"] = True
    row["failure_reason"] = ""
    return row


def _supplemental_evidence_rows(*, signal: dict[str, Any], instrument_type: str, day_utc: str, data_registry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    symbol = _upper(signal.get("symbol"))
    sleeve_id = _upper(signal.get("sleeve_id"))
    signal_type = _upper(signal.get("signal_type"))
    rows = []
    rows.append(
        _base_evidence(
            evidence_id=f"VOLUME:{symbol}",
            data_item_id=f"market.volume.{symbol}",
            purpose="VOLUME",
            freshness_requirement="CURRENT_SESSION_CERTIFIED",
            certification_required=True,
            demanded=False,
        )
    )
    rows.append(
        _base_evidence(
            evidence_id=f"LIQUIDITY:{symbol}",
            data_item_id=f"market.liquidity.{symbol}",
            purpose="LIQUIDITY",
            freshness_requirement="CURRENT_SESSION_CERTIFIED",
            certification_required=True,
            demanded=False,
        )
    )
    volatility_demanded = any(token in sleeve_id for token in ("VOL", "TAIL")) or "VOLATILITY" in signal_type
    rows.append(
        _registry_backed_evidence(
            evidence_id="VOLATILITY:VIX",
            data_item_id="market.volatility.VIX",
            purpose="VOLATILITY",
            demanded=volatility_demanded,
            day_utc=day_utc,
            data_registry_payload=data_registry_payload,
        )
    )
    option_demanded = "OPTION" in instrument_type or "OPTION" in signal_type
    rows.append(
        _base_evidence(
            evidence_id=f"OPTION_CHAIN:{symbol}",
            data_item_id=f"options.chain.{symbol}",
            purpose="OPTION_CHAIN",
            freshness_requirement="CURRENT_SESSION_CERTIFIED",
            certification_required=True,
            demanded=option_demanded,
        )
    )
    event_demanded = "EVENT" in sleeve_id or "EVENT" in signal_type
    rows.append(
        _base_evidence(
            evidence_id=f"EVENT_DATA:{symbol}",
            data_item_id=f"event.data.{symbol}",
            purpose="EVENT_DATA",
            freshness_requirement="CURRENT_SESSION_CERTIFIED",
            certification_required=True,
            demanded=event_demanded,
        )
    )
    regime_demanded = "REGIME" in signal_type
    rows.append(
        _base_evidence(
            evidence_id="REGIME_CONTEXT:CURRENT",
            data_item_id="regime.context.current",
            purpose="REGIME_CONTEXT",
            freshness_requirement="CURRENT_SESSION_CERTIFIED",
            certification_required=True,
            demanded=regime_demanded,
        )
    )
    return rows


def _matching_arbitration_row(signal: dict[str, Any], arbitration_payload: dict[str, Any]) -> dict[str, Any]:
    raw_id = _text(signal.get("raw_signal_id"))
    sleeve_id = _upper(signal.get("sleeve_id"))
    symbol = _upper(signal.get("symbol"))
    rows = []
    for key in ("candidate_intents", "raw_candidate_intents", "rejected_or_filtered_intents"):
        rows.extend(item for item in _safe_list(arbitration_payload.get(key)) if isinstance(item, dict))
    selected = _safe_dict(arbitration_payload.get("selected_intent"))
    if selected:
        rows.append(selected)
    for row in rows:
        if _is_paper_rehearsal_ref(row.get("intent_id"), row.get("intent_path")):
            continue
        if _text(row.get("intent_id") or row.get("raw_signal_id") or row.get("intent_hash")) == raw_id:
            return row
        if _upper(row.get("sleeve_id") or row.get("engine_id")) == sleeve_id and _upper(row.get("symbol") or row.get("symbol_or_pair")) == symbol:
            return row
    return {}


def _candidate_missing_fields(row: dict[str, Any]) -> list[str]:
    missing = []
    if not _text(row.get("raw_signal_id")):
        missing.append("candidate.raw_signal_id")
    if not _text(row.get("intent_id")):
        missing.append("candidate.intent_id")
    if not _text(row.get("direction")):
        missing.append("candidate.direction")
    if not _text(row.get("instrument_type")):
        missing.append("candidate.instrument_type")
    if not _text(row.get("risk_per_trade")):
        missing.append("candidate.risk_per_trade")
    if not _text(row.get("executable_status")):
        missing.append("candidate.executable_status")
    if not _text(row.get("governance_status")):
        missing.append("candidate.governance_status")
    price_edge = next((item for item in _safe_list(row.get("required_evidence")) if _text(item.get("purpose")) == PRICE_PURPOSE), {})
    if not _text(price_edge.get("value")) or not _bool(price_edge.get("certified")):
        missing.append("candidate.entry_reference_price")
    return missing


def _candidate_rejection(stage: str, reason: str, symbol: str) -> str:
    if reason == FAILURE_NOT_FETCHED:
        return f"Demand and fetch certified evidence for {symbol}, then rerun signal evidence graph, candidate contracts, and diagnostics."
    if reason == FAILURE_NOT_CERTIFIED:
        return f"Certify fetched evidence for {symbol} before candidate construction."
    if reason == FAILURE_STALE:
        return f"Refresh same-session evidence for {symbol}, then rerun candidate construction."
    if reason == FAILURE_SYMBOL_MISMATCH:
        return f"Repair evidence symbol mapping for {symbol}, then rerun candidate construction."
    if reason == FAILURE_ARBITRATION_MISSING:
        return "Repair arbitration evidence ingestion for candidate-ready signals; do not bypass arbitration."
    if stage == "CANDIDATE_CONVERSION":
        return "Repair candidate evidence fulfillment so every candidate field is backed by certified evidence."
    return "Inspect raw signal lineage, evidence graph, and downstream artifacts together."


def build_signal_evidence_graph_v1(*, truth_root: Path, day_utc: str, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve() if repo_root is not None else _repo_root()
    data_registry_path, data_registry_payload = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    market_inputs_path, market_inputs_payload = latest_json_v1(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")
    arbitration_path, arbitration_payload = latest_json_v1(root, "intent_arbitration_v1", day_utc, "intent_arbitration.v1.json")
    certification_path, certification_payload = latest_json_v1(root, "aegis_entry_reference_price_certification_v1", day_utc, "entry_reference_price_certification.v1.json")
    certification_by_key = _entry_price_certification_by_key(_safe_dict(certification_payload))
    policy_paths, policy_payloads = _load_structure_policies(repo)
    engine_registry_path, engine_registry_payload = _engine_registry(repo)
    signals = _collect_real_raw_signals(root, day_utc)
    rows: list[dict[str, Any]] = []
    failure_counts: Counter[str] = Counter()
    for signal in signals:
        intent_path = Path(_text(signal.get("evidence_path"))).expanduser()
        intent = read_json_v1(intent_path) if intent_path.exists() else {}
        sleeve_id = _upper(signal.get("sleeve_id"))
        symbol = _upper(signal.get("symbol"))
        policy_row = _policy_for_sleeve(policy_payloads, sleeve_id)
        governed_symbols = _governed_symbols_for_sleeve(engine_registry_payload, sleeve_id)
        pre_contract_suppression_reason = ""
        if sleeve_id == EVENT_DISLOCATION_ENGINE_ID and governed_symbols and symbol not in governed_symbols:
            pre_contract_suppression_reason = UNGOVERNED_SYMBOL_SUPPRESSED
        direction, instrument_type = _derive_direction_and_instrument(intent, policy_row)
        if pre_contract_suppression_reason:
            direction = ""
            instrument_type = ""
        intent_id = _deterministic_intent_id(signal, intent)
        risk_per_trade = _text(_safe_dict(intent.get("constraints")).get("max_risk_pct"))
        executable_status = "REVIEW_ONLY" if _upper(signal.get("sleeve_status")) in {"INTENT_CREATED", "NO_INTENT", "FILTERED_OUT", "BLOCKED"} else ""
        governance_status = "GOVERNED" if policy_row and instrument_type else ""
        if pre_contract_suppression_reason:
            governance_status = ""
        required_evidence = [
            _price_evidence_row(
                symbol=symbol,
                day_utc=day_utc,
                data_registry_payload=_safe_dict(data_registry_payload),
                market_inputs_payload=_safe_dict(market_inputs_payload),
                market_inputs_path=Path(market_inputs_path) if market_inputs_path else Path(),
                certification_row=certification_by_key.get((_upper(signal.get("sleeve_id")), _upper(signal.get("symbol")), _text(signal.get("raw_signal_id")))),
            ),
            *_supplemental_evidence_rows(
                signal=signal,
                instrument_type=instrument_type,
                day_utc=day_utc,
                data_registry_payload=_safe_dict(data_registry_payload),
            ),
        ]
        row = {
            "raw_signal_id": _text(signal.get("raw_signal_id")),
            "intent_id": intent_id,
            "intent_hash": _text(intent.get("intent_hash") or signal.get("intent_hash")),
            "sleeve_id": sleeve_id,
            "symbol": symbol,
            "signal_type": _text(signal.get("signal_type")),
            "direction": direction,
            "instrument_type": instrument_type,
            "risk_per_trade": risk_per_trade,
            "executable_status": executable_status,
            "governance_status": governance_status,
            "source_artifact_path": _text(signal.get("source_artifact_path")),
            "source_hash": _text(signal.get("source_artifact_hash")),
            "evidence_path": _text(signal.get("evidence_path")),
            "graph_linkage": _text(signal.get("graph_linkage")),
            "required_evidence": required_evidence,
            "governed_universe_source": engine_registry_path if sleeve_id == EVENT_DISLOCATION_ENGINE_ID else "",
            "governed_symbols": governed_symbols if sleeve_id == EVENT_DISLOCATION_ENGINE_ID else [],
            "symbol_governance_status": "GOVERNED" if symbol in governed_symbols else ("UNGOVERNED" if sleeve_id == EVENT_DISLOCATION_ENGINE_ID else ""),
            "pre_contract_suppression_reason": pre_contract_suppression_reason,
        }
        missing_fields = _candidate_missing_fields(row)
        if pre_contract_suppression_reason:
            missing_fields = []
        demanded = [item for item in required_evidence if _bool(item.get("demanded"))]
        fetched_ok = all(_bool(item.get("fetched")) for item in demanded)
        certified_ok = all((not _bool(item.get("certification_required"))) or _bool(item.get("certified")) for item in demanded)
        fulfillment_status = "FULFILLED" if demanded and fetched_ok else ("EMPTY" if not demanded else "UNFULFILLED")
        certification_status = "CERTIFIED" if demanded and certified_ok else ("NOT_REQUIRED" if not demanded else "UNCERTIFIED")
        arbitration_row = _matching_arbitration_row(row, _safe_dict(arbitration_payload))
        rejection_stage = ""
        rejection_reason = ""
        if pre_contract_suppression_reason:
            rejection_stage = "PRE_CONTRACT_GOVERNED_UNIVERSE"
            rejection_reason = pre_contract_suppression_reason
            candidate_contract_status = "SUPPRESSED"
        elif missing_fields:
            rejection_stage = "CANDIDATE_CONVERSION"
            price_edge = next((item for item in required_evidence if _text(item.get("purpose")) == PRICE_PURPOSE), {})
            rejection_reason = _text(price_edge.get("failure_reason")) or FAILURE_FIELD_UNFULFILLED
            if not rejection_reason:
                rejection_reason = FAILURE_FIELD_UNFULFILLED
            candidate_contract_status = "REJECTED"
        else:
            candidate_contract_status = "VALID"
            price_edge = next((item for item in required_evidence if _text(item.get("purpose")) == PRICE_PURPOSE), {})
            price_edge["consumed"] = True
            if not arbitration_row:
                rejection_stage = "INTENT_ARBITRATION"
                rejection_reason = FAILURE_ARBITRATION_MISSING
        next_repair_action = _candidate_rejection(rejection_stage, rejection_reason, _upper(signal.get("symbol"))) if rejection_reason else "No repair action required."
        row.update(
            {
                "fulfillment_status": fulfillment_status,
                "certification_status": certification_status,
                "candidate_contract_status": candidate_contract_status,
                "missing_candidate_fields": missing_fields,
                "rejection_stage": rejection_stage,
                "rejection_reason": rejection_reason,
                "next_repair_action": next_repair_action,
            }
        )
        if rejection_reason:
            failure_counts[rejection_reason] += 1
        rows.append(row)
    return {
        "schema_id": "aegis_signal_evidence_graph",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": now_utc_v1(),
        "day_utc": day_utc,
        "total_raw_signals": len(rows),
        "signals": rows,
        "failure_reason_counts": dict(sorted(failure_counts.items())),
        "input_artifacts": {
            "data_registry": str(data_registry_path or ""),
            "market_data_inputs": str(market_inputs_path or ""),
            "entry_reference_price_certification": str(certification_path or ""),
            "intent_arbitration": str(arbitration_path or ""),
            "engine_registry": engine_registry_path,
            **policy_paths,
        },
        "paper_rehearsal_excluded_from_real_totals": True,
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "manual_capture_allowed": False,
        },
    }


def write_signal_evidence_graph_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "signal_evidence_graph.v1.json", payload)
    summary_path = out_dir / "signal_evidence_graph.summary.txt"
    lines = [
        "AEGIS SIGNAL EVIDENCE GRAPH v1",
        f"day_utc: {payload.get('day_utc')}",
        f"total_raw_signals: {payload.get('total_raw_signals')}",
        "failure_reason_counts:",
    ]
    for key, value in sorted((_safe_dict(payload.get("failure_reason_counts"))).items()):
        lines.append(f"- {key}: {value}")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path)}
