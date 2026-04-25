from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from constellation_2.common.broker_fact_spine_v1 import (
    resolve_broker_observation_health_path,
    resolve_broker_raw_journal_path,
    resolve_fact_ledger_path,
)
from constellation_2.common.execution_identity_binding_v1 import (
    GovernedExecutionIdentityV1,
    resolve_governed_execution_identity_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, sha256_file_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    SchemaValidationError,
    validate_against_repo_schema_v1,
)
from constellation_2.phaseF.accounting.lib.immut_write_v1 import WriteResultV1, write_file_immutable_v1


REPO_ROOT = Path(__file__).resolve().parents[2]

TRADE_IDENTITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/trade_identity.v1.schema.json"
INCORPORATED_STATE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json"
)
DESCRIPTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciled_trade_description.v1.schema.json"
HEALTH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_health.v1.schema.json"
PROVENANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_provenance.v1.schema.json"
SUMMARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/reconciled_trade_state_summary.v1.schema.json"
CORE1_HEALTH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_health.v1.schema.json"

CORE2_FAMILY = "reconciled_trade_state_v1"
CORE2_RULE_PACK = "core2_final_strengthened_architecture_v1"
IDENTITY_RULE_VERSION = "trade_identity_resolution_v1"
DESCRIPTION_RULE_VERSION = "reconciled_trade_description_v1"
HEALTH_RULE_VERSION = "reconciliation_health_v1"

OWN_CONSTELLATION = "CONSTELLATION_OWNED"
OWN_FOREIGN = "FOREIGN_MANUAL"
OWN_AMBIGUOUS = "AMBIGUOUS_OWNERSHIP"
OWN_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"

AMBIGUITY_NONE = "NONE"
AMBIGUITY_TRADE_IDENTITY_UNRESOLVED = "TRADE_IDENTITY_UNRESOLVED"
AMBIGUITY_OWNERSHIP = "OWNERSHIP_AMBIGUOUS"
AMBIGUITY_ACCOUNT_MISMATCH = "ACCOUNT_MISMATCH"
AMBIGUITY_INSTRUMENT = "INSTRUMENT_IDENTITY_UNRESOLVED"
AMBIGUITY_LINEAGE = "LINEAGE_UNRESOLVED"

BLOCKER_CLEAR = "CLEAR"
BLOCKER_BLOCKED = "BLOCKED"

HEALTH_TRUSTED = "TRUSTED"
HEALTH_DEGRADED = "DEGRADED"
HEALTH_BLOCKED = "BLOCKED"

POSTURE_SAFE = "SAFE"
POSTURE_DEGRADED = "DEGRADED"
POSTURE_BLOCKED = "BLOCKED"

LIFECYCLE_UNRESOLVED = "UNRESOLVED"
LIFECYCLE_FLAT = "FLAT"
LIFECYCLE_WORKING_ENTRY = "WORKING_ENTRY"
LIFECYCLE_OPEN_LONG = "OPEN_LONG"
LIFECYCLE_OPEN_SHORT = "OPEN_SHORT"
LIFECYCLE_OPEN_LONG_WORKING_EXIT = "OPEN_LONG_WITH_WORKING_EXIT"
LIFECYCLE_OPEN_SHORT_WORKING_EXIT = "OPEN_SHORT_WITH_WORKING_EXIT"
LIFECYCLE_CLOSED = "CLOSED"
LIFECYCLE_AMBIGUOUS = "AMBIGUOUS"

PROTECTION_NA = "NOT_APPLICABLE"
PROTECTION_PRESENT = "PROTECTION_WORKING_PRESENT"
PROTECTION_NOT_OBSERVED = "PROTECTION_NOT_OBSERVED"
PROTECTION_UNKNOWN = "PROTECTION_UNKNOWN"

RC_UPSTREAM_OBSERVATION_STALE = "TRADE_RECON_UPSTREAM_OBSERVATION_STALE"
RC_REPLAY_WINDOW_UNRESOLVED = "TRADE_RECON_REPLAY_WINDOW_UNRESOLVED"
RC_SEQUENCE_UNCERTAINTY = "TRADE_RECON_SEQUENCE_UNCERTAINTY_TOO_HIGH"
RC_INSUFFICIENT_BROKER_EVIDENCE = "TRADE_RECON_INSUFFICIENT_BROKER_EVIDENCE"
RC_TRADE_IDENTITY_UNRESOLVED = "TRADE_IDENTITY_UNRESOLVED"
RC_OWNERSHIP_AMBIGUOUS = "TRADE_IDENTITY_OWNERSHIP_AMBIGUOUS"
RC_ACCOUNT_MISMATCH = "TRADE_IDENTITY_ACCOUNT_MISMATCH"
RC_INSTRUMENT_IDENTITY_UNRESOLVED = "TRADE_IDENTITY_INSTRUMENT_UNRESOLVED"
RC_LINEAGE_UNRESOLVED = "TRADE_IDENTITY_LINEAGE_UNRESOLVED"
RC_POSITION_ORDER_FILL_INCONSISTENCY = "TRADE_RECON_POSITION_ORDER_FILL_INCONSISTENCY"
RC_CONFLICTING_INCORPORATED_FILLS = "TRADE_RECON_CONFLICTING_INCORPORATED_FILLS"
RC_ORPHAN_ORDER_CONFLICT = "TRADE_RECON_ORPHAN_ORDER_CONFLICT"
RC_FLAT_OPEN_CONTRADICTION = "TRADE_RECON_FLAT_OPEN_CONTRADICTION"
RC_DUPLICATE_UNRESOLVED_LINEAGE = "TRADE_RECON_DUPLICATE_UNRESOLVED_LINEAGE"
RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED = "TRADE_OWNERSHIP_FOREIGN_MANUAL_ACTIVITY_SUSPECTED"
RC_UNOWNED_LIVE_POSITION = "TRADE_OWNERSHIP_UNOWNED_LIVE_POSITION"
RC_MIXED_OWNERSHIP_EVIDENCE = "TRADE_OWNERSHIP_MIXED_EVIDENCE"
RC_CURRENT_TRUTH_TOO_STALE = "TRADE_RECON_CURRENT_TRUTH_TOO_STALE_FOR_DOWNSTREAM_ACTION_USE"
RC_LAST_SUCCESSFUL_OUTSIDE_WINDOW = "TRADE_RECON_LAST_SUCCESSFUL_RECONCILIATION_OUTSIDE_ALLOWED_WINDOW"
RC_CORE1_HEALTH_SCHEMA_NONCOMPLIANT = "TRADE_RECON_CORE1_HEALTH_SCHEMA_NONCOMPLIANT"

CORE1_TO_CORE2_REASON_MAP = {
    "BROKER_OBSERVATION_STREAM_STALE": RC_UPSTREAM_OBSERVATION_STALE,
    "BROKER_OBSERVATION_REPLAY_OVERLAP_UNRESOLVED": RC_REPLAY_WINDOW_UNRESOLVED,
    "BROKER_OBSERVATION_SEQUENCE_UNCERTAINTY": RC_SEQUENCE_UNCERTAINTY,
    "BROKER_OBSERVATION_FOREIGN_OR_MANUAL_SUSPECTED": RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED,
}

TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELLED", "API_CANCELLED", "APICANCELLED", "INACTIVE"}
WORKING_ORDER_STATUSES = {"SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT", "PENDINGCANCEL"}

FACT_SCHEMA_IDS = (
    "observed_order_fact",
    "observed_order_status_fact",
    "observed_fill_fact",
    "observed_position_fact",
)


@dataclass(frozen=True)
class Core2TradeArtifactsV1:
    trade_identity_id: str
    ownership_classification: str
    health_state: str
    trade_identity_path: Path
    incorporated_state_path: Path
    description_path: Path
    health_path: Path
    provenance_path: Path


@dataclass(frozen=True)
class ReconciledTradeStateMaterializationV1:
    execution_root_path: Path
    materialization_set_id: str
    summary_path: Path
    trade_artifacts: tuple[Core2TradeArtifactsV1, ...]
    summary: Dict[str, Any]


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _read_jsonl_objects(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _coerce_utc_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        raise ValueError("UTC_TEXT_MISSING")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_sort_key(text: str) -> tuple[datetime, str]:
    return (datetime.fromisoformat(_coerce_utc_text(text).replace("Z", "+00:00")), text)


def _decimal_from_text(text: Any) -> Decimal:
    raw = str(text or "").strip()
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal("1")), "f")
    return format(normalized, "f")


def _fact_sort_key(row: Mapping[str, Any]) -> tuple[datetime, int, str, str]:
    observed = _coerce_utc_text(str(row.get("observed_utc") or "1970-01-01T00:00:00Z"))
    try:
        sequence = int(row.get("journal_sequence_number") or 0)
    except Exception:
        sequence = 0
    return (
        datetime.fromisoformat(observed.replace("Z", "+00:00")),
        sequence,
        str(row.get("schema_id") or ""),
        str(row.get("fact_record_id") or ""),
    )


def _instrument_identity(row: Mapping[str, Any]) -> Dict[str, str]:
    identity = row.get("contract_identity")
    if isinstance(identity, dict):
        return {
            "symbol": str(identity.get("symbol") or ""),
            "sec_type": str(identity.get("sec_type") or ""),
            "exchange": str(identity.get("exchange") or ""),
            "currency": str(identity.get("currency") or ""),
            "raw_summary": str(identity.get("raw_summary") or ""),
        }
    return {"symbol": "", "sec_type": "", "exchange": "", "currency": "", "raw_summary": ""}


def _instrument_key(row: Mapping[str, Any]) -> str:
    identity = _instrument_identity(row)
    parts = [
        identity["raw_summary"],
        identity["symbol"],
        identity["sec_type"],
        identity["exchange"],
        identity["currency"],
    ]
    if any(parts):
        return "|".join(parts)
    for field in ("order_id", "perm_id", "execution_id"):
        value = str(row.get(field) or "").strip()
        if value:
            return f"UNRESOLVED|{field}|{value}"
    return "UNRESOLVED|NO_LINEAGE"


def _lineage_hints(rows: Sequence[Mapping[str, Any]]) -> tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    key_by_lineage: Dict[str, str] = {}
    identity_by_key: Dict[str, Dict[str, str]] = {}
    for row in rows:
        key = _instrument_key(row)
        if key.startswith("UNRESOLVED|"):
            continue
        identity_by_key[key] = _instrument_identity(row)
        for lineage_value in (str(row.get("order_id") or "").strip(), str(row.get("perm_id") or "").strip()):
            if lineage_value:
                key_by_lineage[lineage_value] = key
    return key_by_lineage, identity_by_key


def _instrument_key_with_hints(
    row: Mapping[str, Any],
    *,
    lineage_key_hints: Mapping[str, str],
) -> str:
    key = _instrument_key(row)
    if not key.startswith("UNRESOLVED|"):
        return key
    for lineage_value in (str(row.get("order_id") or "").strip(), str(row.get("perm_id") or "").strip()):
        if lineage_value and lineage_value in lineage_key_hints:
            return str(lineage_key_hints[lineage_value])
    return key


def _resolved_instrument_identity(rows: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    for row in rows:
        identity = _instrument_identity(row)
        if any(identity.values()):
            return identity
    return {"symbol": "", "sec_type": "", "exchange": "", "currency": "", "raw_summary": ""}


def _unique_strings(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _signed_fill_quantity(fill_row: Mapping[str, Any]) -> Decimal:
    qty = _decimal_from_text(fill_row.get("fill_quantity"))
    side = str(fill_row.get("side") or "").strip().upper()
    if side in {"BOT", "BUY"}:
        return qty
    if side in {"SLD", "SELL"}:
        return qty * Decimal("-1")
    return Decimal("0")


def _order_key(row: Mapping[str, Any]) -> str:
    order_id = str(row.get("order_id") or "").strip()
    perm_id = str(row.get("perm_id") or "").strip()
    if order_id or perm_id:
        return f"{order_id}|{perm_id}"
    return f"UNRESOLVED|{row.get('fact_record_id')}"


def _status_is_terminal(status: str) -> bool:
    normalized = str(status or "").strip().upper().replace(" ", "_")
    return normalized in TERMINAL_ORDER_STATUSES


def _status_is_working(status: str) -> bool:
    normalized = str(status or "").strip().upper().replace(" ", "")
    if normalized in {"", "UNKNOWN"}:
        return False
    if normalized in {"FILLED", "CANCELLED", "APICANCELLED", "INACTIVE"}:
        return False
    return normalized in WORKING_ORDER_STATUSES or True


def _is_activity_fact(row: Mapping[str, Any]) -> bool:
    schema_id = str(row.get("schema_id") or "").strip()
    if schema_id == "observed_order_fact":
        return True
    if schema_id == "observed_order_status_fact":
        return not _status_is_terminal(str(row.get("status") or ""))
    if schema_id == "observed_fill_fact":
        return _signed_fill_quantity(row) != 0
    if schema_id == "observed_position_fact":
        return _decimal_from_text(row.get("position_quantity")) != 0
    return False


def _split_continuity_segments(rows: Sequence[Mapping[str, Any]]) -> List[List[Dict[str, Any]]]:
    sorted_rows = [dict(row) for row in sorted(rows, key=_fact_sort_key)]
    if not sorted_rows:
        return []
    segments: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    closed = False
    running_fill_qty = Decimal("0")
    for row in sorted_rows:
        if current and closed and _is_activity_fact(row):
            segments.append(current)
            current = []
            closed = False
            running_fill_qty = Decimal("0")
        current.append(row)
        schema_id = str(row.get("schema_id") or "").strip()
        if schema_id == "observed_fill_fact":
            running_fill_qty += _signed_fill_quantity(row)
            if running_fill_qty == 0:
                closed = True
        elif schema_id == "observed_position_fact":
            if _decimal_from_text(row.get("position_quantity")) == 0:
                closed = True
    if current:
        segments.append(current)
    return segments


def _map_core1_reason_codes(core1_payload: Mapping[str, Any]) -> tuple[List[str], List[str]]:
    blocker_codes: List[str] = []
    degraded_codes: List[str] = []
    current_state = str(core1_payload.get("current_state") or "").strip().upper()
    freshness_status = str(core1_payload.get("freshness_status") or "").strip().upper()
    sequence_status = str(core1_payload.get("sequence_status") or "").strip().upper()
    blocker_source = list(core1_payload.get("blocker_codes") or [])
    degraded_source = list(core1_payload.get("degraded_codes") or [])
    for code in blocker_source:
        mapped = CORE1_TO_CORE2_REASON_MAP.get(str(code), str(code))
        blocker_codes.append(mapped)
    for code in degraded_source:
        mapped = CORE1_TO_CORE2_REASON_MAP.get(str(code), str(code))
        degraded_codes.append(mapped)
    if current_state == "BLOCKED":
        if freshness_status == "STALE":
            blocker_codes.append(RC_UPSTREAM_OBSERVATION_STALE)
        if sequence_status == "UNCERTAIN":
            blocker_codes.append(RC_SEQUENCE_UNCERTAINTY)
    if bool(core1_payload.get("schema_compliant")) is False:
        blocker_codes.append(RC_CORE1_HEALTH_SCHEMA_NONCOMPLIANT)
    if freshness_status == "STALE":
        degraded_codes.append(RC_UPSTREAM_OBSERVATION_STALE)
    if sequence_status == "UNCERTAIN":
        blocker_codes.append(RC_SEQUENCE_UNCERTAINTY)
    return _unique_strings(blocker_codes), _unique_strings([code for code in degraded_codes if code not in blocker_codes])


def _normalize_core1_health_payload(raw_payload: Mapping[str, Any], *, schema_compliant: bool) -> Dict[str, Any]:
    current_state = str(raw_payload.get("current_state") or raw_payload.get("downstream_trust_verdict") or "BLOCKED").strip().upper()
    if current_state not in {HEALTH_TRUSTED, HEALTH_DEGRADED, HEALTH_BLOCKED}:
        current_state = HEALTH_BLOCKED
    downstream_trust_verdict = str(raw_payload.get("downstream_trust_verdict") or current_state).strip().upper()
    if downstream_trust_verdict not in {HEALTH_TRUSTED, HEALTH_DEGRADED, HEALTH_BLOCKED}:
        downstream_trust_verdict = HEALTH_BLOCKED
    freshness_status = str(raw_payload.get("freshness_status") or "UNAVAILABLE").strip().upper()
    if freshness_status not in {"FRESH", "STALE", "UNAVAILABLE"}:
        freshness_status = "UNAVAILABLE"
    sequence_status = str(raw_payload.get("sequence_status") or "UNCERTAIN").strip().upper()
    if sequence_status not in {"OK", "UNCERTAIN"}:
        sequence_status = "UNCERTAIN"
    session_status = str(raw_payload.get("session_status") or "UNAVAILABLE").strip().upper()
    if session_status not in {"HEALTHY", "RECONNECT_IN_PROGRESS", "UNAVAILABLE"}:
        session_status = "UNAVAILABLE"
    blocker_codes = _unique_strings(str(code) for code in raw_payload.get("blocker_codes") or [])
    degraded_codes = _unique_strings(str(code) for code in raw_payload.get("degraded_codes") or [])
    if not schema_compliant:
        current_state = HEALTH_BLOCKED
        downstream_trust_verdict = HEALTH_BLOCKED
        blocker_codes.append(RC_CORE1_HEALTH_SCHEMA_NONCOMPLIANT)
    return {
        "schema_compliant": bool(schema_compliant),
        "current_state": current_state,
        "downstream_trust_verdict": downstream_trust_verdict,
        "freshness_status": freshness_status,
        "freshness_age_seconds": int(raw_payload.get("freshness_age_seconds") or 0),
        "sequence_status": sequence_status,
        "session_status": session_status,
        "blocker_codes": _unique_strings(blocker_codes),
        "degraded_codes": _unique_strings(degraded_codes),
        "evaluation_utc": str(raw_payload.get("evaluation_utc") or raw_payload.get("generated_utc") or ""),
        "generated_utc": str(raw_payload.get("generated_utc") or raw_payload.get("evaluation_utc") or ""),
        "raw_payload": dict(raw_payload),
    }


def _classify_ownership(rows: Sequence[Mapping[str, Any]]) -> tuple[str, List[str], str, List[str]]:
    statuses = {str(row.get("attribution_status") or "").strip() for row in rows}
    account_ids = {str(row.get("account_id") or "").strip() for row in rows if str(row.get("account_id") or "").strip()}
    resolved_identity = _resolved_instrument_identity(rows)
    reason_codes: List[str] = []
    blocker_codes: List[str] = []
    ambiguity_state = AMBIGUITY_NONE
    if len(account_ids) > 1:
        blocker_codes.append(RC_ACCOUNT_MISMATCH)
        ambiguity_state = AMBIGUITY_ACCOUNT_MISMATCH
    if not any(resolved_identity.values()):
        blocker_codes.append(RC_INSTRUMENT_IDENTITY_UNRESOLVED)
        if ambiguity_state == AMBIGUITY_NONE:
            ambiguity_state = AMBIGUITY_INSTRUMENT
    has_lineage = any(
        str(row.get("order_id") or "").strip()
        or str(row.get("perm_id") or "").strip()
        or str(row.get("execution_id") or "").strip()
        or str(row.get("schema_id") or "").strip() == "observed_position_fact"
        for row in rows
    )
    if not has_lineage:
        blocker_codes.append(RC_LINEAGE_UNRESOLVED)
        if ambiguity_state == AMBIGUITY_NONE:
            ambiguity_state = AMBIGUITY_LINEAGE
    if "ATTRIBUTED" in statuses and statuses <= {"ATTRIBUTED"} and not blocker_codes:
        return OWN_CONSTELLATION, ["GOVERNED_ATTRIBUTED_FACTS_ONLY"], ambiguity_state, blocker_codes
    if "FOREIGN_OR_MANUAL_SUSPECTED" in statuses and statuses <= {"FOREIGN_OR_MANUAL_SUSPECTED"}:
        reason_codes.append(RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED)
        return OWN_FOREIGN, reason_codes, ambiguity_state, blocker_codes
    if "ATTRIBUTED" in statuses and "FOREIGN_OR_MANUAL_SUSPECTED" in statuses:
        reason_codes.append(RC_MIXED_OWNERSHIP_EVIDENCE)
        blocker_codes.append(RC_MIXED_OWNERSHIP_EVIDENCE)
        ambiguity_state = AMBIGUITY_OWNERSHIP
        return OWN_AMBIGUOUS, reason_codes, ambiguity_state, _unique_strings(blocker_codes)
    if "AMBIGUOUS_ATTRIBUTION" in statuses:
        reason_codes.append(RC_OWNERSHIP_AMBIGUOUS)
        blocker_codes.append(RC_OWNERSHIP_AMBIGUOUS)
        ambiguity_state = AMBIGUITY_OWNERSHIP
        return OWN_AMBIGUOUS, reason_codes, ambiguity_state, _unique_strings(blocker_codes)
    reason_codes.append(RC_INSUFFICIENT_BROKER_EVIDENCE)
    if ambiguity_state == AMBIGUITY_NONE:
        ambiguity_state = AMBIGUITY_TRADE_IDENTITY_UNRESOLVED
    blocker_codes.append(RC_TRADE_IDENTITY_UNRESOLVED)
    return OWN_INSUFFICIENT, reason_codes, ambiguity_state, _unique_strings(blocker_codes)


def _build_trade_identity_payload(
    *,
    day_utc: str,
    evaluation_utc: str,
    materialization_set_id: str,
    governed_identity: GovernedExecutionIdentityV1,
    rows: Sequence[Mapping[str, Any]],
    segment_index: int,
) -> Dict[str, Any]:
    sorted_rows = sorted(rows, key=_fact_sort_key)
    ownership_classification, ownership_reason_codes, ambiguity_state, blocker_codes = _classify_ownership(sorted_rows)
    instrument_identity = _resolved_instrument_identity(sorted_rows)
    continuity_key = "|".join(
        [
            governed_identity.environment,
            governed_identity.sleeve_id,
            str(sorted_rows[0].get("account_id") or governed_identity.account_id) if sorted_rows else governed_identity.account_id,
            (
                "|".join(
                    [
                        instrument_identity["raw_summary"],
                        instrument_identity["symbol"],
                        instrument_identity["sec_type"],
                        instrument_identity["exchange"],
                        instrument_identity["currency"],
                    ]
                )
                if any(instrument_identity.values())
                else "UNRESOLVED"
            ),
        ]
    )
    trade_identity_id = _sha256_text(f"{continuity_key}|segment={segment_index}")
    opened_by = str(sorted_rows[0].get("fact_record_id") or "") if sorted_rows else ""
    closed_by = ""
    continuity_status = "OPEN"
    for row in reversed(sorted_rows):
        if str(row.get("schema_id") or "") == "observed_position_fact" and _decimal_from_text(row.get("position_quantity")) == 0:
            closed_by = str(row.get("fact_record_id") or "")
            continuity_status = "CLOSED"
            break
    if not closed_by:
        running = Decimal("0")
        for row in sorted_rows:
            if str(row.get("schema_id") or "") == "observed_fill_fact":
                running += _signed_fill_quantity(row)
                if running == 0:
                    closed_by = str(row.get("fact_record_id") or "")
                    continuity_status = "CLOSED"
    if blocker_codes and continuity_status == "OPEN":
        continuity_status = "UNRESOLVED"
    return {
        "schema_id": "trade_identity",
        "schema_version": "v1",
        "authority_owner": "trade_identity_resolution_v1",
        "materialization_set_id": materialization_set_id,
        "day_utc": parse_day_utc_v1(day_utc),
        "evaluation_utc": _coerce_utc_text(evaluation_utc),
        "trade_identity_id": trade_identity_id,
        "environment": governed_identity.environment,
        "sleeve_id": governed_identity.sleeve_id,
        "account_id": str(sorted_rows[0].get("account_id") or governed_identity.account_id) if sorted_rows else governed_identity.account_id,
        "instrument_identity": instrument_identity,
        "ownership_classification": ownership_classification,
        "ownership_reason_codes": _unique_strings(ownership_reason_codes),
        "lineage_attachment_refs": {
            "order_ids": _unique_strings(str(row.get("order_id") or "") for row in sorted_rows),
            "perm_ids": _unique_strings(str(row.get("perm_id") or "") for row in sorted_rows),
            "execution_ids": _unique_strings(str(row.get("execution_id") or "") for row in sorted_rows),
            "fact_record_ids": _unique_strings(str(row.get("fact_record_id") or "") for row in sorted_rows),
        },
        "open_close_continuity": {
            "continuity_key": continuity_key,
            "segment_index": int(segment_index),
            "continuity_status": continuity_status,
            "opened_by_fact_record_id": opened_by,
            "closed_by_fact_record_id": closed_by,
        },
        "ambiguity_state": ambiguity_state,
        "blocker_state": BLOCKER_BLOCKED if blocker_codes else BLOCKER_CLEAR,
        "blocker_codes": _unique_strings(blocker_codes),
        "derived_only": False,
    }


def _merge_fills(rows: Sequence[Mapping[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("schema_id") or "") != "observed_fill_fact":
            continue
        execution_id = str(row.get("execution_id") or "").strip() or str(row.get("fact_record_id") or "").strip()
        grouped[execution_id].append(row)
    incorporated: List[Dict[str, Any]] = []
    ignored: List[Dict[str, Any]] = []
    blocked: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    for execution_id in sorted(grouped):
        bucket = sorted(grouped[execution_id], key=_fact_sort_key)
        best = {
            "execution_id": execution_id,
            "order_id": "",
            "perm_id": "",
            "fill_quantity": "0",
            "fill_price": "0",
            "side": "",
            "commission": "0",
            "currency": "",
            "observed_utc": _coerce_utc_text(str(bucket[-1].get("observed_utc") or "1970-01-01T00:00:00Z")),
            "fact_record_ids": _unique_strings(str(row.get("fact_record_id") or "") for row in bucket),
        }
        conflict = False
        for row in bucket:
            if best["order_id"] and str(row.get("order_id") or "").strip() and best["order_id"] != str(row.get("order_id") or "").strip():
                conflict = True
            if best["perm_id"] and str(row.get("perm_id") or "").strip() and best["perm_id"] != str(row.get("perm_id") or "").strip():
                conflict = True
            if best["fill_quantity"] != "0" and str(row.get("fill_quantity") or "").strip() and best["fill_quantity"] != str(row.get("fill_quantity") or "").strip():
                conflict = True
            if best["fill_price"] != "0" and str(row.get("fill_price") or "").strip() and best["fill_price"] != str(row.get("fill_price") or "").strip():
                conflict = True
            if best["side"] and str(row.get("side") or "").strip() and best["side"] != str(row.get("side") or "").strip():
                conflict = True
            if str(row.get("order_id") or "").strip():
                best["order_id"] = str(row.get("order_id") or "").strip()
            if str(row.get("perm_id") or "").strip():
                best["perm_id"] = str(row.get("perm_id") or "").strip()
            if str(row.get("fill_quantity") or "").strip():
                best["fill_quantity"] = str(row.get("fill_quantity") or "").strip()
            if str(row.get("fill_price") or "").strip():
                best["fill_price"] = str(row.get("fill_price") or "").strip()
            if str(row.get("side") or "").strip():
                best["side"] = str(row.get("side") or "").strip()
            if str(row.get("commission") or "").strip():
                best["commission"] = str(row.get("commission") or "").strip()
            if str(row.get("currency") or "").strip():
                best["currency"] = str(row.get("currency") or "").strip()
        if conflict:
            reason_codes.append(RC_CONFLICTING_INCORPORATED_FILLS)
            for row in bucket:
                blocked.append(
                    {
                        "schema_id": str(row.get("schema_id") or ""),
                        "fact_record_id": str(row.get("fact_record_id") or ""),
                        "canonical_event_identity": str(row.get("canonical_event_identity") or ""),
                        "reason_code": RC_CONFLICTING_INCORPORATED_FILLS,
                        "note": f"execution_id={execution_id}",
                    }
                )
            continue
        incorporated.append(best)
        for row in bucket[1:]:
            ignored.append(
                {
                    "schema_id": str(row.get("schema_id") or ""),
                    "fact_record_id": str(row.get("fact_record_id") or ""),
                    "canonical_event_identity": str(row.get("canonical_event_identity") or ""),
                    "reason_code": "SUPERSEDED_BY_EXECUTION_MERGE",
                    "note": f"execution_id={execution_id}",
                }
            )
    return incorporated, ignored, blocked, _unique_strings(reason_codes)


def _build_orders(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[str],
    List[Dict[str, Any]],
]:
    order_rows = [row for row in rows if str(row.get("schema_id") or "") == "observed_order_fact"]
    status_rows = [row for row in rows if str(row.get("schema_id") or "") == "observed_order_status_fact"]
    orders_by_key: Dict[str, Dict[str, Any]] = {}
    ignored: List[Dict[str, Any]] = []
    blocked: List[Dict[str, Any]] = []
    orphan_orders: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    for row in sorted(order_rows, key=_fact_sort_key):
        key = _order_key(row)
        existing = orders_by_key.get(key)
        if existing is None:
            orders_by_key[key] = {
                "order_key": key,
                "order_id": str(row.get("order_id") or ""),
                "perm_id": str(row.get("perm_id") or ""),
                "status": "",
                "action": str(((row.get("order_summary") or {}).get("action")) or ""),
                "total_quantity": str(((row.get("order_summary") or {}).get("total_quantity")) or ""),
                "filled_quantity": "0",
                "remaining_quantity": "",
                "observed_utc": _coerce_utc_text(str(row.get("observed_utc") or "1970-01-01T00:00:00Z")),
                "fact_record_ids": [str(row.get("fact_record_id") or "")],
            }
            continue
        existing["fact_record_ids"].append(str(row.get("fact_record_id") or ""))
        ignored.append(
            {
                "schema_id": str(row.get("schema_id") or ""),
                "fact_record_id": str(row.get("fact_record_id") or ""),
                "canonical_event_identity": str(row.get("canonical_event_identity") or ""),
                "reason_code": "SUPERSEDED_ORDER_FACT",
                "note": f"order_key={key}",
            }
        )
    for row in sorted(status_rows, key=_fact_sort_key):
        key = _order_key(row)
        target = orders_by_key.get(key)
        if target is None:
            orphan_orders.append(
                {
                    "order_key": key,
                    "fact_record_id": str(row.get("fact_record_id") or ""),
                    "reason_code": RC_ORPHAN_ORDER_CONFLICT,
                }
            )
            blocked.append(
                {
                    "schema_id": str(row.get("schema_id") or ""),
                    "fact_record_id": str(row.get("fact_record_id") or ""),
                    "canonical_event_identity": str(row.get("canonical_event_identity") or ""),
                    "reason_code": RC_ORPHAN_ORDER_CONFLICT,
                    "note": f"order_key={key}",
                }
            )
            reason_codes.append(RC_ORPHAN_ORDER_CONFLICT)
            continue
        target["status"] = str(row.get("status") or "")
        target["filled_quantity"] = str(row.get("filled_quantity") or "")
        target["remaining_quantity"] = str(row.get("remaining_quantity") or "")
        target["observed_utc"] = _coerce_utc_text(str(row.get("observed_utc") or target["observed_utc"]))
        target["fact_record_ids"].append(str(row.get("fact_record_id") or ""))
    working_orders: List[Dict[str, Any]] = []
    terminal_orders: List[Dict[str, Any]] = []
    for key in sorted(orders_by_key):
        order = dict(orders_by_key[key])
        order["fact_record_ids"] = _unique_strings(order["fact_record_ids"])
        if _status_is_terminal(order["status"]):
            terminal_orders.append(order)
        else:
            working_orders.append(order)
    return working_orders, terminal_orders, orphan_orders, ignored, _unique_strings(reason_codes), blocked


def _current_position(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any] | None:
    positions = [row for row in rows if str(row.get("schema_id") or "") == "observed_position_fact"]
    if not positions:
        return None
    return dict(sorted(positions, key=_fact_sort_key)[-1])


def _find_prior_incorporated_state_path(
    *,
    execution_root_path: Path,
    trade_identity_id: str,
    current_materialization_set_id: str,
) -> Path | None:
    root = (Path(execution_root_path).resolve() / CORE2_FAMILY / "materializations").resolve()
    if not root.exists() or not root.is_dir():
        return None
    candidates: List[tuple[str, Path]] = []
    for path in root.glob(f"*/**/trades/{trade_identity_id}/incorporated_broker_trade_state.v1.json"):
        if current_materialization_set_id in str(path):
            continue
        try:
            payload = _read_json_obj(path)
        except Exception:
            continue
        candidates.append((str(payload.get("evaluation_utc") or ""), path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _change_summary(current_payload: Mapping[str, Any], prior_path: Path | None) -> Dict[str, Any]:
    if prior_path is None:
        return {
            "change_status": "INITIAL_MATERIALIZATION",
            "prior_state_ref": {"artifact_path": "", "artifact_sha256": ""},
            "changed_fields": [],
            "summary": "Initial Core 2 materialization for this trade identity.",
        }
    prior_payload = _read_json_obj(prior_path)
    excluded = {"materialization_set_id", "evaluation_utc", "day_utc", "trade_identity_ref", "upstream_core1_evidence_refs"}
    changed_fields = sorted(
        key
        for key in set(current_payload.keys()).union(prior_payload.keys())
        if key not in excluded and current_payload.get(key) != prior_payload.get(key)
    )
    if not changed_fields:
        status = "NO_MATERIAL_CHANGE"
        summary = "No material top-level change from prior incorporated state."
    else:
        status = "MATERIAL_CHANGE"
        summary = "Changed top-level fields: " + ",".join(changed_fields)
    return {
        "change_status": status,
        "prior_state_ref": {
            "artifact_path": str(prior_path),
            "artifact_sha256": sha256_file_v1(prior_path),
        },
        "changed_fields": changed_fields,
        "summary": summary,
    }


def _write_validated_immutable_json(*, repo_root: Path, path: Path, payload: Dict[str, Any], schema_relpath: str) -> WriteResultV1:
    validate_against_repo_schema_v1(payload, repo_root, schema_relpath)
    raw = canonical_json_bytes_v1(payload) + b"\n"
    return write_file_immutable_v1(path=path, data=raw, create_dirs=True)


def _resolve_core2_root(execution_root_path: Path) -> Path:
    return (Path(execution_root_path).resolve() / CORE2_FAMILY).resolve()


def _resolve_trade_dir(*, execution_root_path: Path, day_utc: str, materialization_set_id: str, trade_identity_id: str) -> Path:
    return (
        _resolve_core2_root(execution_root_path)
        / "materializations"
        / parse_day_utc_v1(day_utc)
        / materialization_set_id
        / "trades"
        / trade_identity_id
    ).resolve()


def _resolve_summary_path(*, execution_root_path: Path, day_utc: str, materialization_set_id: str) -> Path:
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "reconciled_trade_state_summary_v1"
        / parse_day_utc_v1(day_utc)
        / materialization_set_id
        / "reconciled_trade_state_summary.v1.json"
    ).resolve()


def _load_core1_inputs(
    *,
    repo_root: Path,
    execution_root_path: Path,
    day_utc: str,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any], Dict[str, Any]]:
    fact_rows = {
        schema_id: _read_jsonl_objects(resolve_fact_ledger_path(execution_root_path=execution_root_path, day_utc=day_utc, schema_id=schema_id))
        for schema_id in FACT_SCHEMA_IDS
    }
    health_path = resolve_broker_observation_health_path(execution_root_path=execution_root_path, day_utc=day_utc)
    if not health_path.exists() or not health_path.is_file():
        raise ValueError(f"CORE1_HEALTH_MISSING:path={health_path}")
    raw_health_payload = _read_json_obj(health_path)
    try:
        validate_against_repo_schema_v1(raw_health_payload, repo_root, CORE1_HEALTH_SCHEMA)
        health_payload = _normalize_core1_health_payload(raw_health_payload, schema_compliant=True)
    except SchemaValidationError:
        health_payload = _normalize_core1_health_payload(raw_health_payload, schema_compliant=False)
    refs = {
        "raw_journal_ref": {
            "artifact_path": str(resolve_broker_raw_journal_path(execution_root_path=execution_root_path, day_utc=day_utc)),
            "artifact_sha256": sha256_file_v1(resolve_broker_raw_journal_path(execution_root_path=execution_root_path, day_utc=day_utc)),
        },
        "fact_ledger_refs": {
            schema_id: {
                "artifact_path": str(resolve_fact_ledger_path(execution_root_path=execution_root_path, day_utc=day_utc, schema_id=schema_id)),
                "artifact_sha256": sha256_file_v1(resolve_fact_ledger_path(execution_root_path=execution_root_path, day_utc=day_utc, schema_id=schema_id)),
            }
            for schema_id in FACT_SCHEMA_IDS
        },
        "health_ref": {
            "artifact_path": str(health_path),
            "artifact_sha256": sha256_file_v1(health_path),
        },
    }
    return fact_rows, health_payload, refs


def _resolve_identity_groups(
    *,
    day_utc: str,
    evaluation_utc: str,
    materialization_set_id: str,
    governed_identity: GovernedExecutionIdentityV1,
    fact_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    all_rows: List[Mapping[str, Any]] = []
    for schema_id in FACT_SCHEMA_IDS:
        all_rows.extend(list(fact_rows.get(schema_id, [])))
    lineage_key_hints, _ = _lineage_hints(all_rows)
    for schema_id in FACT_SCHEMA_IDS:
        for row in fact_rows.get(schema_id, []):
            grouped[
                (
                    str(row.get("account_id") or ""),
                    _instrument_key_with_hints(row, lineage_key_hints=lineage_key_hints),
                )
            ].append(row)
    results: List[Dict[str, Any]] = []
    for (_, _), rows in sorted(grouped.items(), key=lambda item: item[0]):
        for index, segment in enumerate(_split_continuity_segments(rows), start=1):
            identity_payload = _build_trade_identity_payload(
                day_utc=day_utc,
                evaluation_utc=evaluation_utc,
                materialization_set_id=materialization_set_id,
                governed_identity=governed_identity,
                rows=segment,
                segment_index=index,
            )
            results.append({"identity_payload": identity_payload, "segment_rows": segment})
    return results


def _materialize_state(
    *,
    evaluation_utc: str,
    materialization_set_id: str,
    identity_payload: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    core1_health: Mapping[str, Any],
    core1_refs: Mapping[str, Any],
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    incorporated_fills, ignored_fills, blocked_fills, fill_reason_codes = _merge_fills(rows)
    working_orders, terminal_orders, orphan_orders, ignored_orders, order_reason_codes, blocked_orders = _build_orders(rows)
    current_position = _current_position(rows)
    current_quantity_decimal = _decimal_from_text((current_position or {}).get("position_quantity"))
    if current_position is None:
        current_quantity_decimal = sum((_signed_fill_quantity(fill) for fill in incorporated_fills), Decimal("0"))
    drift_reason_codes: List[str] = []
    net_fill_quantity = sum((_signed_fill_quantity(fill) for fill in incorporated_fills), Decimal("0"))
    if current_position is not None and current_quantity_decimal != net_fill_quantity:
        drift_reason_codes.append(RC_POSITION_ORDER_FILL_INCONSISTENCY)
    blocker_codes = list(identity_payload.get("blocker_codes") or [])
    degraded_codes: List[str] = []
    core1_blockers, core1_degraded = _map_core1_reason_codes(core1_health)
    blocker_codes.extend(core1_blockers)
    degraded_codes.extend(core1_degraded)
    blocker_codes.extend(fill_reason_codes)
    blocker_codes.extend(order_reason_codes)
    blocker_codes.extend(drift_reason_codes)
    latest_observed_utc = max(
        (_coerce_utc_text(str(row.get("observed_utc") or "")) for row in rows if str(row.get("observed_utc") or "").strip()),
        default=_coerce_utc_text(evaluation_utc),
    )
    age_seconds = max(
        0,
        int(
            (
                datetime.fromisoformat(_coerce_utc_text(evaluation_utc).replace("Z", "+00:00"))
                - datetime.fromisoformat(latest_observed_utc.replace("Z", "+00:00"))
            ).total_seconds()
        ),
    )
    if current_position is None:
        degraded_codes.append(RC_INSUFFICIENT_BROKER_EVIDENCE)
    if age_seconds > 600:
        blocker_codes.append(RC_CURRENT_TRUTH_TOO_STALE)
        blocker_codes.append(RC_LAST_SUCCESSFUL_OUTSIDE_WINDOW)
    elif age_seconds > 120:
        degraded_codes.append(RC_CURRENT_TRUTH_TOO_STALE)
    ownership_classification = str(identity_payload.get("ownership_classification") or "")
    if ownership_classification == OWN_FOREIGN:
        blocker_codes.append(RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED)
        if current_quantity_decimal != 0:
            blocker_codes.append(RC_UNOWNED_LIVE_POSITION)
    if ownership_classification == OWN_AMBIGUOUS:
        blocker_codes.append(RC_OWNERSHIP_AMBIGUOUS)
    if ownership_classification == OWN_INSUFFICIENT:
        blocker_codes.append(RC_TRADE_IDENTITY_UNRESOLVED)
    blocker_codes = _unique_strings(blocker_codes)
    degraded_codes = _unique_strings(code for code in degraded_codes if code not in blocker_codes)
    side = "UNKNOWN"
    if current_quantity_decimal > 0:
        side = "LONG"
    elif current_quantity_decimal < 0:
        side = "SHORT"
    elif current_quantity_decimal == 0:
        side = "FLAT"
    max_sequence = max((int(row.get("journal_sequence_number") or 0) for row in rows), default=0)
    state_payload = {
        "schema_id": "incorporated_broker_trade_state",
        "schema_version": "v1",
        "authority_owner": "incorporated_broker_trade_state_v1",
        "truth_owner_status": "CANONICAL_CURRENT_TRUTH_OWNER",
        "materialization_set_id": materialization_set_id,
        "day_utc": str(identity_payload.get("day_utc") or ""),
        "evaluation_utc": _coerce_utc_text(evaluation_utc),
        "trade_identity_ref": {},
        "environment": str(identity_payload.get("environment") or ""),
        "sleeve_id": str(identity_payload.get("sleeve_id") or ""),
        "account_id": str(identity_payload.get("account_id") or ""),
        "instrument_identity": dict(identity_payload.get("instrument_identity") or {}),
        "ownership_classification": ownership_classification,
        "current_quantity": _decimal_text(current_quantity_decimal),
        "side": side,
        "average_cost": str((current_position or {}).get("average_cost") or ""),
        "incorporated_fills": incorporated_fills,
        "current_working_orders": working_orders,
        "terminal_order_lineage": terminal_orders,
        "orphan_order_facts": orphan_orders,
        "last_reconciled_fact_boundary": {
            "max_journal_sequence_number": max_sequence,
            "latest_observed_utc": latest_observed_utc,
            "raw_journal_path": str(core1_refs["raw_journal_ref"]["artifact_path"]),
            "raw_journal_sha256": str(core1_refs["raw_journal_ref"]["artifact_sha256"]),
        },
        "drift_basis": {
            "position_quantity": _decimal_text(_decimal_from_text((current_position or {}).get("position_quantity"))),
            "net_fill_quantity": _decimal_text(net_fill_quantity),
            "position_vs_fill_consistent": not drift_reason_codes,
            "mismatch_quantity": _decimal_text(current_quantity_decimal - net_fill_quantity),
            "reason_codes": _unique_strings(drift_reason_codes),
        },
        "freshness_basis": {
            "core1_downstream_trust_verdict": str(core1_health.get("downstream_trust_verdict") or ""),
            "core1_freshness_status": str(core1_health.get("freshness_status") or ""),
            "evaluation_utc": _coerce_utc_text(evaluation_utc),
            "latest_observed_utc": latest_observed_utc,
            "age_seconds": age_seconds,
        },
        "first_blocker": blocker_codes[0] if blocker_codes else "",
        "ambiguity_state": str(identity_payload.get("ambiguity_state") or AMBIGUITY_NONE),
        "blocker_codes": blocker_codes,
        "degraded_codes": degraded_codes,
        "last_successful_reconciliation_utc": "" if blocker_codes else _coerce_utc_text(evaluation_utc),
        "upstream_core1_evidence_refs": dict(core1_refs),
        "derived_only": False,
    }
    ignored_refs = ignored_fills + ignored_orders
    blocked_refs = blocked_fills + blocked_orders
    return state_payload, ignored_refs, blocked_refs


def derive_reconciliation_health_v1(state_payload: Mapping[str, Any]) -> Dict[str, Any]:
    blocker_reasons = _unique_strings(str(item) for item in state_payload.get("blocker_codes") or [])
    degraded_reasons = _unique_strings(str(item) for item in state_payload.get("degraded_codes") or [])
    age_seconds = int(((state_payload.get("freshness_basis") or {}).get("age_seconds")) or 0)
    if blocker_reasons:
        current_state = HEALTH_BLOCKED
    elif degraded_reasons:
        current_state = HEALTH_DEGRADED
    else:
        current_state = HEALTH_TRUSTED
    freshness_status = "FRESH"
    if age_seconds > 600:
        freshness_status = "TOO_STALE"
    elif age_seconds > 120:
        freshness_status = "STALE"
    ambiguity_status = "AMBIGUOUS" if str(state_payload.get("ambiguity_state") or "") != AMBIGUITY_NONE else "CLEAR"
    drift_status = "DRIFTED" if list(((state_payload.get("drift_basis") or {}).get("reason_codes")) or []) else "CLEAR"
    insufficient_evidence_status = "INSUFFICIENT" if RC_INSUFFICIENT_BROKER_EVIDENCE in blocker_reasons or RC_INSUFFICIENT_BROKER_EVIDENCE in degraded_reasons else "SUFFICIENT"
    downstream_action_posture = POSTURE_SAFE
    if current_state == HEALTH_BLOCKED:
        downstream_action_posture = POSTURE_BLOCKED
    elif current_state == HEALTH_DEGRADED:
        downstream_action_posture = POSTURE_DEGRADED
    return {
        "schema_id": "reconciliation_health",
        "schema_version": "v1",
        "authority_owner": "reconciliation_health_v1",
        "materialization_set_id": str(state_payload.get("materialization_set_id") or ""),
        "day_utc": str(state_payload.get("day_utc") or ""),
        "evaluation_utc": str(state_payload.get("evaluation_utc") or ""),
        "trade_identity_id": str(((state_payload.get("trade_identity_ref") or {}).get("trade_identity_id")) or ""),
        "incorporated_state_ref": {},
        "current_state": current_state,
        "blocker_reasons": blocker_reasons,
        "degraded_reasons": degraded_reasons,
        "freshness_status": freshness_status,
        "ambiguity_status": ambiguity_status,
        "drift_status": drift_status,
        "insufficient_evidence_status": insufficient_evidence_status,
        "downstream_action_posture": downstream_action_posture,
        "derived_only": True,
    }


def derive_reconciled_trade_description_v1(
    *,
    state_payload: Mapping[str, Any],
    health_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    quantity = _decimal_from_text(state_payload.get("current_quantity"))
    working_orders = list(state_payload.get("current_working_orders") or [])
    opposite_side_working = False
    if quantity > 0:
        opposite_side_working = any(str(order.get("action") or "").strip().upper() in {"SELL", "SLD"} for order in working_orders)
    elif quantity < 0:
        opposite_side_working = any(str(order.get("action") or "").strip().upper() in {"BUY", "BOT"} for order in working_orders)
    lifecycle_status = LIFECYCLE_FLAT
    if health_payload["current_state"] == HEALTH_BLOCKED and str(state_payload.get("ambiguity_state") or "") != AMBIGUITY_NONE:
        lifecycle_status = LIFECYCLE_AMBIGUOUS
    elif str(state_payload.get("ambiguity_state") or "") != AMBIGUITY_NONE:
        lifecycle_status = LIFECYCLE_UNRESOLVED
    elif quantity > 0 and opposite_side_working:
        lifecycle_status = LIFECYCLE_OPEN_LONG_WORKING_EXIT
    elif quantity > 0:
        lifecycle_status = LIFECYCLE_OPEN_LONG
    elif quantity < 0 and opposite_side_working:
        lifecycle_status = LIFECYCLE_OPEN_SHORT_WORKING_EXIT
    elif quantity < 0:
        lifecycle_status = LIFECYCLE_OPEN_SHORT
    elif quantity == 0 and working_orders:
        lifecycle_status = LIFECYCLE_WORKING_ENTRY
    elif quantity == 0 and list(state_payload.get("incorporated_fills") or []):
        lifecycle_status = LIFECYCLE_CLOSED
    protection_status = PROTECTION_NA
    if quantity != 0:
        protection_status = PROTECTION_PRESENT if opposite_side_working else PROTECTION_NOT_OBSERVED
    if health_payload["current_state"] == HEALTH_BLOCKED and protection_status == PROTECTION_NOT_OBSERVED:
        protection_status = PROTECTION_UNKNOWN
    reconciliation_status = health_payload["current_state"]
    if str(state_payload.get("ambiguity_state") or "") != AMBIGUITY_NONE:
        reconciliation_status = "AMBIGUOUS"
    return {
        "schema_id": "reconciled_trade_description",
        "schema_version": "v1",
        "authority_owner": "reconciled_trade_description_v1",
        "materialization_set_id": str(state_payload.get("materialization_set_id") or ""),
        "day_utc": str(state_payload.get("day_utc") or ""),
        "evaluation_utc": str(state_payload.get("evaluation_utc") or ""),
        "trade_identity_id": str(((state_payload.get("trade_identity_ref") or {}).get("trade_identity_id")) or ""),
        "incorporated_state_ref": {},
        "lifecycle_status": lifecycle_status,
        "protection_status": protection_status,
        "reconciliation_descriptive_status": reconciliation_status,
        "downstream_posture": str(health_payload.get("downstream_action_posture") or POSTURE_BLOCKED),
        "derived_from_incorporated_state": True,
        "derived_only": True,
    }


def _build_provenance(
    *,
    execution_root_path: Path,
    state_payload: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    ignored_refs: Sequence[Mapping[str, Any]],
    blocked_refs: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    incorporated_fact_ids: set[str] = set()
    for fill in state_payload.get("incorporated_fills") or []:
        incorporated_fact_ids.update(str(item) for item in fill.get("fact_record_ids") or [])
    for order in state_payload.get("current_working_orders") or []:
        incorporated_fact_ids.update(str(item) for item in order.get("fact_record_ids") or [])
    for order in state_payload.get("terminal_order_lineage") or []:
        incorporated_fact_ids.update(str(item) for item in order.get("fact_record_ids") or [])
    position = next((row for row in rows if str(row.get("schema_id") or "") == "observed_position_fact" and str(row.get("fact_record_id") or "") in _unique_strings([str(row.get("fact_record_id") or "") for row in rows])), None)
    if position is not None:
        incorporated_fact_ids.add(str(position.get("fact_record_id") or ""))
    incorporated_refs = [
        {
            "schema_id": str(row.get("schema_id") or ""),
            "fact_record_id": str(row.get("fact_record_id") or ""),
            "canonical_event_identity": str(row.get("canonical_event_identity") or ""),
            "reason_code": "INCORPORATED_IN_CURRENT_STATE",
            "note": "",
        }
        for row in rows
        if str(row.get("fact_record_id") or "") in incorporated_fact_ids
    ]
    prior_path = _find_prior_incorporated_state_path(
        execution_root_path=execution_root_path,
        trade_identity_id=str(((state_payload.get("trade_identity_ref") or {}).get("trade_identity_id")) or ""),
        current_materialization_set_id=str(state_payload.get("materialization_set_id") or ""),
    )
    return {
        "schema_id": "reconciliation_provenance",
        "schema_version": "v1",
        "authority_owner": "reconciliation_provenance_v1",
        "materialization_set_id": str(state_payload.get("materialization_set_id") or ""),
        "day_utc": str(state_payload.get("day_utc") or ""),
        "evaluation_utc": str(state_payload.get("evaluation_utc") or ""),
        "trade_identity_id": str(((state_payload.get("trade_identity_ref") or {}).get("trade_identity_id")) or ""),
        "incorporated_state_ref": {},
        "rule_versions": {
            "core2_rule_pack": CORE2_RULE_PACK,
            "identity_rules": IDENTITY_RULE_VERSION,
            "description_rules": DESCRIPTION_RULE_VERSION,
            "health_rules": HEALTH_RULE_VERSION,
        },
        "upstream_core1_evidence_refs": dict(state_payload.get("upstream_core1_evidence_refs") or {}),
        "incorporated_fact_refs": incorporated_refs,
        "ignored_fact_refs": list(ignored_refs),
        "blocked_fact_refs": list(blocked_refs),
        "prior_state_change_summary": _change_summary(state_payload, prior_path),
        "derived_only": True,
    }


def materialize_reconciled_trade_state_v1(
    *,
    repo_root: Path,
    day_utc: str,
    environment: str = "PAPER",
    sleeve_id: str = "PRIMARY",
    evaluation_utc: str = "",
) -> ReconciledTradeStateMaterializationV1:
    repo_root = Path(repo_root).resolve()
    day = parse_day_utc_v1(day_utc)
    governed_identity = resolve_governed_execution_identity_v1(
        repo_root=repo_root,
        environment=environment,
        sleeve_id=sleeve_id,
    )
    execution_roots = resolve_governed_paper_execution_roots(
        repo_root=repo_root,
        environment=environment,
        ib_account=governed_identity.account_id,
        sleeve_id=governed_identity.sleeve_id,
    )
    execution_root_path = Path(execution_roots.execution_root_path).resolve()
    fact_rows, core1_health, core1_refs = _load_core1_inputs(
        repo_root=repo_root,
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    effective_evaluation_utc = _coerce_utc_text(
        evaluation_utc or str(core1_health.get("evaluation_utc") or "") or str(core1_health.get("generated_utc") or "")
    )
    materialization_seed = json.dumps(
        {
            "day_utc": day,
            "evaluation_utc": effective_evaluation_utc,
            "raw_journal_sha256": core1_refs["raw_journal_ref"]["artifact_sha256"],
            "fact_ledger_shas": {key: value["artifact_sha256"] for key, value in core1_refs["fact_ledger_refs"].items()},
            "health_sha256": core1_refs["health_ref"]["artifact_sha256"],
            "rule_pack": CORE2_RULE_PACK,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    materialization_set_id = _sha256_text(materialization_seed)
    groups = _resolve_identity_groups(
        day_utc=day,
        evaluation_utc=effective_evaluation_utc,
        materialization_set_id=materialization_set_id,
        governed_identity=governed_identity,
        fact_rows=fact_rows,
    )
    trade_artifacts: List[Core2TradeArtifactsV1] = []
    summary_refs: List[Dict[str, Any]] = []
    counts_by_ownership = Counter(
        {
            OWN_CONSTELLATION: 0,
            OWN_FOREIGN: 0,
            OWN_AMBIGUOUS: 0,
            OWN_INSUFFICIENT: 0,
        }
    )
    counts_by_health = Counter({HEALTH_TRUSTED: 0, HEALTH_DEGRADED: 0, HEALTH_BLOCKED: 0})
    blocker_counts: Counter[str] = Counter()
    for group in groups:
        identity_payload = dict(group["identity_payload"])
        trade_dir = _resolve_trade_dir(
            execution_root_path=execution_root_path,
            day_utc=day,
            materialization_set_id=materialization_set_id,
            trade_identity_id=str(identity_payload["trade_identity_id"]),
        )
        identity_path = (trade_dir / "trade_identity.v1.json").resolve()
        identity_write = _write_validated_immutable_json(
            repo_root=repo_root,
            path=identity_path,
            payload=identity_payload,
            schema_relpath=TRADE_IDENTITY_SCHEMA,
        )
        state_payload, ignored_refs, blocked_refs = _materialize_state(
            evaluation_utc=effective_evaluation_utc,
            materialization_set_id=materialization_set_id,
            identity_payload=identity_payload,
            rows=group["segment_rows"],
            core1_health=core1_health,
            core1_refs=core1_refs,
        )
        state_payload["trade_identity_ref"] = {
            "trade_identity_id": str(identity_payload["trade_identity_id"]),
            "artifact_path": str(identity_path),
            "artifact_sha256": identity_write.sha256,
        }
        state_path = (trade_dir / "incorporated_broker_trade_state.v1.json").resolve()
        state_write = _write_validated_immutable_json(
            repo_root=repo_root,
            path=state_path,
            payload=state_payload,
            schema_relpath=INCORPORATED_STATE_SCHEMA,
        )
        health_payload = derive_reconciliation_health_v1(state_payload)
        health_payload["trade_identity_id"] = str(identity_payload["trade_identity_id"])
        health_payload["incorporated_state_ref"] = {
            "artifact_path": str(state_path),
            "artifact_sha256": state_write.sha256,
        }
        health_path = (trade_dir / "reconciliation_health.v1.json").resolve()
        health_write = _write_validated_immutable_json(
            repo_root=repo_root,
            path=health_path,
            payload=health_payload,
            schema_relpath=HEALTH_SCHEMA,
        )
        description_payload = derive_reconciled_trade_description_v1(
            state_payload=state_payload,
            health_payload=health_payload,
        )
        description_payload["trade_identity_id"] = str(identity_payload["trade_identity_id"])
        description_payload["incorporated_state_ref"] = {
            "artifact_path": str(state_path),
            "artifact_sha256": state_write.sha256,
        }
        description_path = (trade_dir / "reconciled_trade_description.v1.json").resolve()
        description_write = _write_validated_immutable_json(
            repo_root=repo_root,
            path=description_path,
            payload=description_payload,
            schema_relpath=DESCRIPTION_SCHEMA,
        )
        provenance_payload = _build_provenance(
            execution_root_path=execution_root_path,
            state_payload=state_payload,
            rows=group["segment_rows"],
            ignored_refs=ignored_refs,
            blocked_refs=blocked_refs,
        )
        provenance_payload["trade_identity_id"] = str(identity_payload["trade_identity_id"])
        provenance_payload["incorporated_state_ref"] = {
            "artifact_path": str(state_path),
            "artifact_sha256": state_write.sha256,
        }
        provenance_path = (trade_dir / "reconciliation_provenance.v1.json").resolve()
        provenance_write = _write_validated_immutable_json(
            repo_root=repo_root,
            path=provenance_path,
            payload=provenance_payload,
            schema_relpath=PROVENANCE_SCHEMA,
        )
        trade_artifacts.append(
            Core2TradeArtifactsV1(
                trade_identity_id=str(identity_payload["trade_identity_id"]),
                ownership_classification=str(identity_payload["ownership_classification"]),
                health_state=str(health_payload["current_state"]),
                trade_identity_path=identity_path,
                incorporated_state_path=state_path,
                description_path=description_path,
                health_path=health_path,
                provenance_path=provenance_path,
            )
        )
        counts_by_ownership[str(identity_payload["ownership_classification"])] += 1
        counts_by_health[str(health_payload["current_state"])] += 1
        for code in health_payload["blocker_reasons"]:
            blocker_counts[code] += 1
        summary_refs.append(
            {
                "trade_identity_id": str(identity_payload["trade_identity_id"]),
                "ownership_classification": str(identity_payload["ownership_classification"]),
                "health_state": str(health_payload["current_state"]),
                "incorporated_state_path": str(state_path),
                "description_path": str(description_path),
                "health_path": str(health_path),
                "provenance_path": str(provenance_path),
            }
        )
    summary_payload = {
        "schema_id": "reconciled_trade_state_summary",
        "schema_version": "v1",
        "authority_owner": "reconciled_trade_state_summary_v1",
        "materialization_set_id": materialization_set_id,
        "day_utc": day,
        "evaluation_utc": effective_evaluation_utc,
        "environment": governed_identity.environment,
        "sleeve_id": governed_identity.sleeve_id,
        "execution_root_path": str(execution_root_path),
        "counts_by_ownership": {
            OWN_CONSTELLATION: int(counts_by_ownership[OWN_CONSTELLATION]),
            OWN_FOREIGN: int(counts_by_ownership[OWN_FOREIGN]),
            OWN_AMBIGUOUS: int(counts_by_ownership[OWN_AMBIGUOUS]),
            OWN_INSUFFICIENT: int(counts_by_ownership[OWN_INSUFFICIENT]),
        },
        "counts_by_health": {
            HEALTH_TRUSTED: int(counts_by_health[HEALTH_TRUSTED]),
            HEALTH_DEGRADED: int(counts_by_health[HEALTH_DEGRADED]),
            HEALTH_BLOCKED: int(counts_by_health[HEALTH_BLOCKED]),
        },
        "blocker_counts": [{"code": code, "count": int(count)} for code, count in sorted(blocker_counts.items())],
        "trade_refs": summary_refs,
        "derived_only": True,
    }
    summary_path = _resolve_summary_path(
        execution_root_path=execution_root_path,
        day_utc=day,
        materialization_set_id=materialization_set_id,
    )
    _ = _write_validated_immutable_json(
        repo_root=repo_root,
        path=summary_path,
        payload=summary_payload,
        schema_relpath=SUMMARY_SCHEMA,
    )
    return ReconciledTradeStateMaterializationV1(
        execution_root_path=execution_root_path,
        materialization_set_id=materialization_set_id,
        summary_path=summary_path,
        trade_artifacts=tuple(sorted(trade_artifacts, key=lambda item: item.trade_identity_id)),
        summary=summary_payload,
    )


__all__ = [
    "CORE2_FAMILY",
    "CORE2_RULE_PACK",
    "DESCRIPTION_RULE_VERSION",
    "HEALTH_RULE_VERSION",
    "IDENTITY_RULE_VERSION",
    "OWN_AMBIGUOUS",
    "OWN_CONSTELLATION",
    "OWN_FOREIGN",
    "OWN_INSUFFICIENT",
    "POSTURE_BLOCKED",
    "POSTURE_DEGRADED",
    "POSTURE_SAFE",
    "RC_ACCOUNT_MISMATCH",
    "RC_CONFLICTING_INCORPORATED_FILLS",
    "RC_CURRENT_TRUTH_TOO_STALE",
    "RC_FLAT_OPEN_CONTRADICTION",
    "RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED",
    "RC_INSTRUMENT_IDENTITY_UNRESOLVED",
    "RC_INSUFFICIENT_BROKER_EVIDENCE",
    "RC_LINEAGE_UNRESOLVED",
    "RC_MIXED_OWNERSHIP_EVIDENCE",
    "RC_ORPHAN_ORDER_CONFLICT",
    "RC_OWNERSHIP_AMBIGUOUS",
    "RC_POSITION_ORDER_FILL_INCONSISTENCY",
    "RC_REPLAY_WINDOW_UNRESOLVED",
    "RC_SEQUENCE_UNCERTAINTY",
    "RC_TRADE_IDENTITY_UNRESOLVED",
    "RC_UNOWNED_LIVE_POSITION",
    "ReconciledTradeStateMaterializationV1",
    "derive_reconciled_trade_description_v1",
    "derive_reconciliation_health_v1",
    "materialize_reconciled_trade_state_v1",
]
