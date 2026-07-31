from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1
from ops.aegis.operator_state.trade_candidate_contract_v1 import content_hash_file_v1
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1
from ops.aegis.entry_reference_price_certification_v1 import build_entry_reference_price_certification_v1, write_entry_reference_price_certification_v1
from ops.aegis.research_mapping_rules_v1 import declared_or_legacy_mapping_v1


REPORT_FAMILY = "aegis_candidate_contracts_v1"
REQUIRED_FIELDS = (
    "raw_signal_id",
    "intent_id",
    "sleeve_id",
    "symbol",
    "direction",
    "instrument_type",
    "entry_reference_price",
    "risk_per_trade",
    "stop_price",
    "stop_loss_bps",
    "executable_status",
    "governance_status",
)
ENTRY_PRICE_REASON_MISSING = "ENTRY_REFERENCE_PRICE_MISSING"
ENTRY_PRICE_REASON_STALE = "ENTRY_REFERENCE_PRICE_STALE"
ENTRY_PRICE_REASON_UNCERTIFIED = "ENTRY_REFERENCE_PRICE_UNCERTIFIED"
ENTRY_PRICE_REASON_SYMBOL_MISMATCH = "ENTRY_REFERENCE_PRICE_SYMBOL_MISMATCH"
STOP_PRICE_REASON_MISSING = "STOP_PRICE_MISSING"
RISK_POLICY_REASON_MISSING = "RISK_POLICY_STOP_LOSS_BPS_DEFAULT_MISSING"
RISK_POLICY_REGISTRY_PATH = Path("governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json")
UNGOVERNED_SYMBOL_SUPPRESSED = "UNGOVERNED_SYMBOL_SUPPRESSED"

GRAPH_TO_PRICE_REASON = {
    "EVIDENCE_NOT_FETCHED": ENTRY_PRICE_REASON_MISSING,
    "EVIDENCE_NOT_CERTIFIED": ENTRY_PRICE_REASON_UNCERTIFIED,
    "EVIDENCE_STALE": ENTRY_PRICE_REASON_STALE,
    "EVIDENCE_SYMBOL_MISMATCH": ENTRY_PRICE_REASON_SYMBOL_MISMATCH,
}

GRAPH_TO_PRICE_STATUS = {
    "EVIDENCE_NOT_FETCHED": "MISSING",
    "EVIDENCE_NOT_CERTIFIED": "UNCERTIFIED",
    "EVIDENCE_STALE": "STALE",
    "EVIDENCE_SYMBOL_MISMATCH": "SYMBOL_MISMATCH",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _money(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def _parse_utc(value: Any) -> datetime | None:
    raw = _text(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _session_stale_by_seconds(*, price_timestamp: str, candidate_snapshot_timestamp: str, price_session_date: str, day_utc: str) -> int | None:
    if price_session_date and day_utc and price_session_date != day_utc:
        try:
            price_day = datetime.fromisoformat(price_session_date).replace(tzinfo=UTC)
            required_day = datetime.fromisoformat(day_utc).replace(tzinfo=UTC)
        except ValueError:
            price_day = required_day = None
        if price_day and required_day:
            return max(0, int((required_day - price_day).total_seconds()))
    price_dt = _parse_utc(price_timestamp)
    snapshot_dt = _parse_utc(candidate_snapshot_timestamp)
    if price_dt and snapshot_dt:
        return max(0, int((snapshot_dt - price_dt).total_seconds()))
    return None


def _entry_price_freshness_diagnostics(edge: dict[str, Any], *, signal_graph: dict[str, Any], day_utc: str) -> dict[str, Any]:
    price_timestamp = _text(edge.get("timestamp_utc"))
    candidate_snapshot_timestamp = _text(signal_graph.get("generated_at_utc") or signal_graph.get("generated_at"))
    price_session_date = _text(edge.get("session_date"))
    policy_mode = _text(edge.get("freshness_policy_mode") or edge.get("freshness_requirement") or "CURRENT_SESSION_CERTIFIED")
    failure_reason = _text(edge.get("failure_reason"))
    stale_by = _session_stale_by_seconds(
        price_timestamp=price_timestamp,
        candidate_snapshot_timestamp=candidate_snapshot_timestamp,
        price_session_date=price_session_date,
        day_utc=day_utc,
    ) if failure_reason == "EVIDENCE_STALE" else None
    stale_reason = _text(edge.get("stale_reason") or failure_reason)
    if failure_reason == "EVIDENCE_NOT_FETCHED" and not price_timestamp:
        stale_reason = stale_reason or "PRICE_TIMESTAMP_MISSING_FOR_CURRENT_SESSION_CERTIFIED"
    elif failure_reason == "EVIDENCE_STALE" and price_session_date != day_utc:
        stale_reason = stale_reason or f"PRICE_SESSION_DATE_{price_session_date or 'UNKNOWN'}_DOES_NOT_MATCH_{day_utc}"
    return {
        "price_timestamp": price_timestamp,
        "candidate_snapshot_timestamp": candidate_snapshot_timestamp,
        "freshness_window_seconds": int(edge.get("freshness_window_seconds") or 0),
        "stale_by_seconds": stale_by,
        "freshness_policy_mode": policy_mode,
        "stale_reason": stale_reason,
    }

def _evidence_hashes(paths: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        if path.exists() and path.is_file():
            hashes[str(path.resolve())] = content_hash_file_v1(path.resolve())
    return hashes


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _risk_policy(*, repo_root: Path, sleeve_id: str) -> tuple[dict[str, Any], Path]:
    path = (repo_root / RISK_POLICY_REGISTRY_PATH).resolve()
    registry = _read_json(path)
    policies = registry.get("policies") if isinstance(registry.get("policies"), dict) else registry
    row = policies.get(sleeve_id) if isinstance(policies, dict) else {}
    return (dict(row) if isinstance(row, dict) else {}), path


def _derive_stop_fields(*, repo_root: Path, sleeve_id: str, entry_reference_price: str, direction: str) -> dict[str, Any]:
    policy, path = _risk_policy(repo_root=repo_root, sleeve_id=sleeve_id)
    stop_bps = _dec(policy.get("stop_loss_bps_default"))
    entry = _dec(entry_reference_price)
    source = f"C2_RISK_POLICY_REGISTRY_V1:{sleeve_id}.stop_loss_bps_default" if stop_bps is not None else ""
    stop_price: Decimal | None = None
    if entry is not None and entry > 0 and stop_bps is not None and stop_bps > 0:
        multiplier = Decimal("1") + (stop_bps / Decimal("10000")) if direction.upper() == "SHORT" else Decimal("1") - (stop_bps / Decimal("10000"))
        if multiplier > 0:
            stop_price = entry * multiplier
    missing_reason = ""
    if stop_price is None:
        missing_reason = RISK_POLICY_REASON_MISSING if stop_bps is None or stop_bps <= 0 else STOP_PRICE_REASON_MISSING
    return {
        "stop_price": _money(stop_price),
        "stop_loss_bps": format(stop_bps.normalize(), "f") if stop_bps is not None else "",
        "stop_price_source": source,
        "stop_price_status": "DERIVED" if stop_price is not None else "MISSING",
        "stop_price_missing_reason": missing_reason,
        "risk_policy_source_path": str(path) if path.exists() else "",
        "risk_policy_source_hash": content_hash_file_v1(path) if path.exists() and path.is_file() else "",
    }


def _candidate_id(day_utc: str, raw_signal_id: str, sleeve_id: str, symbol: str) -> str:
    return "candidate_contract_" + canonical_hash_for_c2_artifact_v1(
        {
            "day_utc": day_utc,
            "raw_signal_id": raw_signal_id,
            "sleeve_id": sleeve_id,
            "symbol": symbol,
            "schema": "aegis_candidate_contracts_v1",
        }
    )[:24]


def _entry_price_edge(signal_row: dict[str, Any]) -> dict[str, Any]:
    for item in _safe_list(signal_row.get("required_evidence")):
        if isinstance(item, dict) and _text(item.get("purpose")) == "ENTRY_REFERENCE_PRICE":
            return item
    return {}


def _price_reason(edge: dict[str, Any]) -> str:
    return GRAPH_TO_PRICE_REASON.get(_text(edge.get("failure_reason")), ENTRY_PRICE_REASON_MISSING)


def _is_pre_contract_suppressed_signal(signal_row: dict[str, Any]) -> bool:
    status = _text(signal_row.get("candidate_contract_status")).upper()
    reason = _text(signal_row.get("pre_contract_suppression_reason") or signal_row.get("rejection_reason")).upper()
    stage = _text(signal_row.get("rejection_stage")).upper()
    return (
        status == "SUPPRESSED"
        and reason == UNGOVERNED_SYMBOL_SUPPRESSED
        and stage == "PRE_CONTRACT_GOVERNED_UNIVERSE"
    )


def build_candidate_contracts_v1(*, truth_root: Path, day_utc: str, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve() if repo_root is not None else _repo_root()
    cert_payload = build_entry_reference_price_certification_v1(truth_root=root, day_utc=day_utc)
    write_entry_reference_price_certification_v1(truth_root=root, day_utc=day_utc, payload=cert_payload)
    signal_graph = build_signal_evidence_graph_v1(truth_root=root, day_utc=day_utc, repo_root=repo)
    graph_rows = _safe_list(signal_graph.get("signals"))
    created: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    pre_contract_suppressed: list[dict[str, Any]] = []
    rejection_reasons: Counter[str] = Counter()
    pre_contract_suppression_reasons: Counter[str] = Counter()
    for signal_row in graph_rows:
        if not isinstance(signal_row, dict):
            continue
        edge = _entry_price_edge(signal_row)
        graph_input_paths = list(_safe_dict(signal_graph.get("input_artifacts")).values())
        evidence_paths = [
            _text(signal_row.get("source_artifact_path")),
            _text(signal_row.get("evidence_path")),
            _text(edge.get("source_artifact_path")),
            *graph_input_paths,
        ]
        evidence_paths = [path for path in dict.fromkeys(path for path in evidence_paths if _text(path))]
        if _is_pre_contract_suppressed_signal(signal_row):
            reason = _text(signal_row.get("pre_contract_suppression_reason") or signal_row.get("rejection_reason")) or UNGOVERNED_SYMBOL_SUPPRESSED
            pre_contract_suppression_reasons[reason] += 1
            pre_contract_suppressed.append(
                {
                    "raw_signal_id": _text(signal_row.get("raw_signal_id")),
                    "intent_id": _text(signal_row.get("intent_id")),
                    "intent_hash": _text(signal_row.get("intent_hash")),
                    "sleeve_id": _text(signal_row.get("sleeve_id")),
                    "symbol": _text(signal_row.get("symbol")),
                    "signal_type": _text(signal_row.get("signal_type")),
                    "source_artifact": _text(signal_row.get("source_artifact_path")),
                    "evidence_path": _text(signal_row.get("evidence_path")),
                    "graph_linkage": _text(signal_row.get("graph_linkage")),
                    "evidence_paths": evidence_paths,
                    "evidence_hashes": _evidence_hashes(evidence_paths),
                    "contract_validation_status": "NOT_SUBMITTED_PRE_CONTRACT_SUPPRESSION",
                    "pre_contract_suppression_reason": reason,
                    "rejection_stage": _text(signal_row.get("rejection_stage")),
                    "governed_universe_source": _text(signal_row.get("governed_universe_source")),
                    "governed_symbols": _safe_list(signal_row.get("governed_symbols")),
                    "symbol_governance_status": _text(signal_row.get("symbol_governance_status")),
                    "candidate_contract_input_created": False,
                    "next_repair_action": _text(signal_row.get("next_repair_action")) or "No candidate repair required; signal symbol is outside governed Event Dislocation universe.",
                }
            )
            continue
        missing_fields = [str(item) for item in _safe_list(signal_row.get("missing_candidate_fields")) if _text(item)]
        freshness_diagnostics = _entry_price_freshness_diagnostics(edge, signal_graph=signal_graph, day_utc=day_utc)
        shared = {
            "raw_signal_id": _text(signal_row.get("raw_signal_id")),
            "intent_id": _text(signal_row.get("intent_id")),
            "intent_hash": _text(signal_row.get("intent_hash")),
            "sleeve_id": _text(signal_row.get("sleeve_id")),
            "symbol": _text(signal_row.get("symbol")),
            "signal_type": _text(signal_row.get("signal_type")),
            "source_artifact": _text(signal_row.get("source_artifact_path")),
            "evidence_path": _text(signal_row.get("evidence_path")),
            "graph_linkage": _text(signal_row.get("graph_linkage")),
            "evidence_paths": evidence_paths,
            "evidence_hashes": _evidence_hashes(evidence_paths),
            "paper_rehearsal_excluded_from_real_totals": True,
            "entry_reference_price": _text(edge.get("value")),
            "entry_reference_price_field": _text(edge.get("field")),
            "entry_reference_price_source_path": _text(edge.get("source_artifact_path")),
            "entry_reference_price_source_hash": _text(edge.get("source_hash")),
            "entry_reference_price_provider": _text(edge.get("provider")),
            "entry_reference_price_timestamp_utc": _text(edge.get("timestamp_utc")),
            "entry_reference_price_session_date": _text(edge.get("session_date")),
            "entry_reference_price_missing_symbol": _text(signal_row.get("symbol")),
            "entry_reference_price_checked_evidence_paths": [_text(path) for path in _safe_list(signal_graph.get("input_artifacts", {}).values()) if _text(path)] if isinstance(signal_graph.get("input_artifacts"), dict) else [],
            "entry_reference_price_status": "VALID" if _text(edge.get("value")) else GRAPH_TO_PRICE_STATUS.get(_text(edge.get("failure_reason")), "MISSING"),
            "entry_reference_price_certification_status": _text(edge.get("entry_reference_price_certification_status")) or ("CERTIFIED" if _text(edge.get("value")) else ""),
            "entry_reference_price_certification_reason_codes": _safe_list(edge.get("entry_reference_price_certification_reason_codes")),
            "entry_reference_price_certification_id": _text(edge.get("entry_reference_price_certification_id")),
            "entry_reference_price_safe_repair_available": bool(edge.get("safe_repair_available")),
            "candidate_snapshot_timestamp_utc": freshness_diagnostics["candidate_snapshot_timestamp"],
            "price_timestamp": freshness_diagnostics["price_timestamp"],
            "candidate_snapshot_timestamp": freshness_diagnostics["candidate_snapshot_timestamp"],
            "freshness_window_seconds": freshness_diagnostics["freshness_window_seconds"],
            "stale_by_seconds": freshness_diagnostics["stale_by_seconds"],
            "freshness_policy_mode": freshness_diagnostics["freshness_policy_mode"],
            "stale_reason": freshness_diagnostics["stale_reason"],
            "entry_reference_price_freshness": freshness_diagnostics,
        }
        hypothesis_mapping = declared_or_legacy_mapping_v1(shared, sleeve_id=shared["sleeve_id"])
        row = {
            **shared,
            "hypothesis_id": hypothesis_mapping.get("hypothesis_id", ""),
            "thesis_id": hypothesis_mapping.get("thesis_id", ""),
            "hypothesis_mapping_confidence": hypothesis_mapping.get("mapping_confidence", ""),
            "hypothesis_mapping_source": hypothesis_mapping.get("mapping_source", ""),
            "candidate_id": _candidate_id(day_utc, shared["raw_signal_id"], shared["sleeve_id"], shared["symbol"]),
            "direction": _text(signal_row.get("direction")),
            "instrument_type": _text(signal_row.get("instrument_type")),
            "risk_per_trade": _text(signal_row.get("risk_per_trade")),
            "executable_status": _text(signal_row.get("executable_status")),
            "governance_status": _text(signal_row.get("governance_status")),
            "market_data_status": _text(edge.get("failure_reason") or ("VALID" if _text(edge.get("value")) else "MISSING")),
            "market_session_date": _text(edge.get("session_date")),
            "signal_evidence_graph_path": str((root / "reports" / "aegis_signal_evidence_graph_v1" / day_utc / "signal_evidence_graph.v1.json").resolve()),
            "signal_evidence_graph_status": _text(signal_row.get("candidate_contract_status")),
        }
        stop_fields = _derive_stop_fields(
            repo_root=repo,
            sleeve_id=row["sleeve_id"],
            entry_reference_price=row["entry_reference_price"],
            direction=row["direction"],
        )
        row.update(stop_fields)
        if not row["stop_price"]:
            missing_fields.append("candidate.stop_price")
        if not row["stop_loss_bps"]:
            missing_fields.append("candidate.stop_loss_bps")
        if row["risk_policy_source_path"]:
            row["evidence_paths"] = [path for path in dict.fromkeys([*row["evidence_paths"], row["risk_policy_source_path"]]) if _text(path)]
            row["evidence_hashes"] = _evidence_hashes(row["evidence_paths"])
        if _text(signal_row.get("candidate_contract_status")) != "VALID" or missing_fields:
            reason = _price_reason(edge) if "candidate.entry_reference_price" in missing_fields else (
                "INSTRUMENT_TYPE_NOT_GOVERNED" if "candidate.instrument_type" in missing_fields else (
                    row["stop_price_missing_reason"] if "candidate.stop_price" in missing_fields or "candidate.stop_loss_bps" in missing_fields else "CANDIDATE_CONTRACT_FIELDS_MISSING"
                )
            )
            rejection_reasons[reason] += 1
            rejected.append(
                {
                    **row,
                    "contract_validation_status": "REJECTED",
                    "missing_contract_fields": missing_fields,
                    "rejection_reason": reason,
                    "detail_reason_codes": _safe_list(row.get("entry_reference_price_certification_reason_codes")) if reason.startswith("ENTRY_REFERENCE_PRICE") else [],
                    "entry_reference_price_certification_detail_status": _text(row.get("entry_reference_price_certification_status")),
                    "next_repair_action": _text(signal_row.get("next_repair_action")) or "Repair evidence fulfillment and rerun candidate contracts.",
                }
            )
            continue
        created.append(
            {
                **row,
                "contract_validation_status": "VALID",
                "review_only": True,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_approval_allowed": False,
            }
        )
    status = "PASS" if created and not rejected else ("PARTIAL" if created else ("VALID_NO_CANDIDATE_CONDITIONS" if pre_contract_suppressed else "FAILED"))
    top_missing = Counter()
    for row in rejected:
        for field in _safe_list(row.get("missing_contract_fields")):
            top_missing[str(field)] += 1
    return {
        "schema_id": "aegis_candidate_contracts",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": now_utc_v1(),
        "day_utc": day_utc,
        "candidates_created": len(created),
        "candidates_rejected": len(rejected),
        "pre_contract_suppressed_count": len(pre_contract_suppressed),
        "pre_contract_suppression_reasons": [{"reason": reason, "count": count} for reason, count in pre_contract_suppression_reasons.most_common()],
        "rejection_reasons": [{"reason": reason, "count": count} for reason, count in rejection_reasons.most_common()],
        "source_raw_signal_ids": [row.get("raw_signal_id") for row in graph_rows if isinstance(row, dict)],
        "evidence_paths": sorted({path for row in created + rejected for path in _safe_list(row.get("evidence_paths")) if _text(path)}),
        "contract_validation_status": status,
        "candidate_contract_required_fields": [f"candidate.{field}" for field in REQUIRED_FIELDS],
        "candidate_contracts": created,
        "rejected_raw_signals": rejected,
        "pre_contract_suppressed_raw_signals": pre_contract_suppressed,
        "top_missing_contract_fields": [{"field": field, "count": count} for field, count in top_missing.most_common()],
        "input_artifacts": {
            "signal_evidence_graph": str((root / "reports" / "aegis_signal_evidence_graph_v1" / day_utc / "signal_evidence_graph.v1.json").resolve()),
            **(_safe_dict(signal_graph.get("input_artifacts"))),
        },
        "signal_evidence_graph": signal_graph,
        "paper_rehearsal_excluded_from_real_totals": True,
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
        },
    }


def write_candidate_contracts_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "candidate_contracts.v1.json", payload)
    summary_path = out_dir / "candidate_contracts.summary.txt"
    summary_path.write_text(render_candidate_contracts_summary_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path)}


def render_candidate_contracts_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS CANDIDATE CONTRACTS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"contract_validation_status: {payload.get('contract_validation_status')}",
        f"candidates_created: {payload.get('candidates_created')}",
        f"candidates_rejected: {payload.get('candidates_rejected')}",
        f"pre_contract_suppressed_count: {payload.get('pre_contract_suppressed_count', 0)}",
        f"signal_evidence_graph: {_safe_dict(payload.get('input_artifacts')).get('signal_evidence_graph', '')}",
        "rejection_reasons:",
    ]
    for row in _safe_list(payload.get("rejection_reasons")):
        if isinstance(row, dict):
            lines.append(f"- {row.get('reason')}: {row.get('count')}")
    return "\n".join(lines) + "\n"
