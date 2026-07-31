from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1

FAMILY = "aegis_data_action_routing_v1"
FILENAME = "data_action_routing.v1.json"
SCHEMA_ID = "aegis_data_action_routing"
SCHEMA_VERSION = "v1"
POLICY_VERSION = "aegis_data_action_routing_policy_v1"

OPERATOR_PROVIDED_DATA_REQUIRED = "OPERATOR_PROVIDED_DATA_REQUIRED"
SYSTEM_DATA_PIPELINE_REQUIRED = "SYSTEM_DATA_PIPELINE_REQUIRED"
WAITING_FOR_MARKET_DATA = "WAITING_FOR_MARKET_DATA"
DATA_NOT_AVAILABLE = "DATA_NOT_AVAILABLE"

VALID_CLASSIFICATIONS = {
    OPERATOR_PROVIDED_DATA_REQUIRED,
    SYSTEM_DATA_PIPELINE_REQUIRED,
    WAITING_FOR_MARKET_DATA,
    DATA_NOT_AVAILABLE,
}

SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_candidate_mutation": True,
    "no_paper_lifecycle_mutation": True,
    "no_producer_logic_change": True,
    "no_safety_gate_change": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "safety_gates_changed": False,
}


def data_action_routing_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / FAMILY / day_utc / FILENAME


def build_data_action_routing_v1(*, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    computed_at = computed_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    paths = _input_paths(root, day_utc)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}

    rows = _routing_rows(payloads, paths, computed_at)
    ambiguous = [
        row for row in rows
        if str(row.get("blocker_code") or "").upper() == "MISSING_DATA"
        and str(row.get("data_action_classification") or "") not in VALID_CLASSIFICATIONS
    ]
    david_count = sum(1 for row in rows if row.get("david_action_required") is True)
    by_owner: dict[str, int] = {}
    by_classification: dict[str, int] = {}
    for row in rows:
        owner = str(row.get("owner") or "UNAVAILABLE")
        classification = str(row.get("data_action_classification") or "")
        by_owner[owner] = by_owner.get(owner, 0) + 1
        by_classification[classification] = by_classification.get(classification, 0) + 1

    body: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day_utc,
        "target_day": day_utc,
        "computed_at_utc": computed_at,
        "routing_rows": rows,
        "summary": {
            "missing_data_route_count": len(rows),
            "david_action_required_count": david_count,
            "ambiguous_missing_data_count": len(ambiguous),
            "classification_counts": by_classification,
            "owner_counts": by_owner,
        },
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: _sha(path) for key, path in sorted(paths.items())},
        "safety_statement": "Data action routing is read-only classification. It does not change candidate generation, producer logic, paper lifecycle, trading, broker behavior, or safety gates.",
        **SAFETY,
    }
    body["content_hash"] = _stable_hash({k: v for k, v in body.items() if k not in {"computed_at_utc", "content_hash"}})
    return body


def write_data_action_routing_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_data_action_routing_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(data_action_routing_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    specs = {
        "operator_action_queue": ("aegis_operator_action_queue_v1", "operator_action_queue.v1.json"),
        "generated_throughput": ("aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"),
        "oil_shock_candidate_flow": ("aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"),
    }
    paths: dict[str, Path] = {}
    for key, (family, filename) in specs.items():
        path, _payload = latest_json_v1(root, family, day, filename)
        paths[key] = path or root / "reports" / family / day / filename
    return paths


def _routing_rows(payloads: Mapping[str, Mapping[str, Any]], paths: Mapping[str, Path], computed_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    action_by_id = {
        str(row.get("hypothesis_id") or row.get("hypothesis_name") or ""): row
        for row in _list(payloads.get("operator_action_queue", {}).get("actions"))
        if isinstance(row, Mapping)
    }
    for action in _list(payloads.get("operator_action_queue", {}).get("actions")):
        if isinstance(action, Mapping):
            rows.append(_operator_data_action_row(action, paths, computed_at))

    oil = _oil_row(payloads.get("oil_shock_candidate_flow", {}))
    if oil and str(oil.get("exact_blocker") or oil.get("blocker_classification") or "").upper() in {"MISSING_DATA", "SYSTEM_DATA_PIPELINE_REQUIRED"}:
        key = str(oil.get("hypothesis_id") or oil.get("hypothesis_name") or "")
        explicit_action = action_by_id.get(key)
        rows.append(_oil_missing_data_row(oil, paths, computed_at, explicit_action=explicit_action if isinstance(explicit_action, Mapping) else None))

    seen = set()
    out = []
    for row in rows:
        key = (row["hypothesis_id"], row["blocker_code"], row["owner"])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _operator_data_action_row(action: Mapping[str, Any], paths: Mapping[str, Path], computed_at: str) -> dict[str, Any]:
    source_paths = [str(item) for item in _list(action.get("source_artifact_paths")) if str(item)]
    if not source_paths:
        source_paths = [str(paths["operator_action_queue"])]
    return {
        "hypothesis_id": str(action.get("hypothesis_id") or ""),
        "hypothesis_name": str(action.get("hypothesis_name") or "Macro Calendar"),
        "blocker_code": "MISSING_DATA",
        "data_action_classification": OPERATOR_PROVIDED_DATA_REQUIRED,
        "david_action_required": True,
        "missing_data_description": "Macro Calendar needs a macro event calendar source.",
        "required_fields": [str(item) for item in _list(action.get("required_fields"))],
        "owner": "DAVID",
        "next_step": "Connect Source, Upload Dataset, Mark Not Available, or Defer.",
        "exact_buttons": [str(item) for item in _list(action.get("exact_buttons"))],
        "source_artifact_paths": source_paths,
        "source_artifact_hashes": _hashes_for_paths(source_paths),
        "computed_at_utc": computed_at,
    }


def _oil_missing_data_row(oil: Mapping[str, Any], paths: Mapping[str, Path], computed_at: str, *, explicit_action: Mapping[str, Any] | None) -> dict[str, Any]:
    if explicit_action:
        classification = OPERATOR_PROVIDED_DATA_REQUIRED
        owner = "DAVID"
        david_action = True
        next_step = "Use the explicit Oil Shock data-source action created by Aegis."
        description = str(explicit_action.get("why_action_needed") or "Oil Shock has an explicit David data-source action.")
        buttons = [str(item) for item in _list(explicit_action.get("exact_buttons"))]
    else:
        classification = SYSTEM_DATA_PIPELINE_REQUIRED if str(oil.get("exact_blocker") or "").upper() == "SYSTEM_DATA_PIPELINE_REQUIRED" else (WAITING_FOR_MARKET_DATA if oil.get("due_to", {}).get("missing_data") else SYSTEM_DATA_PIPELINE_REQUIRED)
        owner = "AEGIS_SYSTEM" if classification == SYSTEM_DATA_PIPELINE_REQUIRED else "MARKET_CONDITIONS"
        david_action = False
        next_step = str(oil.get("next_expected_step") or "Wait for system market data evidence, then rerun candidate producer.")
        description = "Oil Shock requires system market data evidence. No David action is required unless Aegis creates a data-source action."
        buttons = []
    source_paths = [str(paths["oil_shock_candidate_flow"])]
    return {
        "hypothesis_id": str(oil.get("hypothesis_id") or ""),
        "hypothesis_name": str(oil.get("hypothesis_name") or "Oil shock reversals across energy ETFs"),
        "blocker_code": SYSTEM_DATA_PIPELINE_REQUIRED if classification == SYSTEM_DATA_PIPELINE_REQUIRED else "MISSING_DATA",
        "data_action_classification": classification,
        "david_action_required": david_action,
        "missing_data_description": description,
        "required_fields": [str(item) for item in _list(oil.get("required_evidence_fields"))],
        "owner": owner,
        "next_step": next_step,
        "exact_buttons": buttons,
        "source_artifact_paths": source_paths,
        "source_artifact_hashes": _hashes_for_paths(source_paths),
        "computed_at_utc": computed_at,
    }


def _oil_row(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("oil_shock") if isinstance(payload, Mapping) else {}
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _sha(path: Path | str | None) -> str:
    try:
        p = Path(path) if path else None
        return hashlib.sha256(p.read_bytes()).hexdigest() if p and p.exists() else ""
    except OSError:
        return ""


def _hashes_for_paths(paths: list[str]) -> dict[str, str]:
    return {path: _sha(path) for path in paths}


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()
