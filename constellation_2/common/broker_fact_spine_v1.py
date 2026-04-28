from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from constellation_2.common.execution_identity_binding_v1 import (
    GovernedExecutionIdentityV1,
    resolve_governed_execution_identity_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    parse_day_utc_v1,
    sha256_file_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]

BROKER_FACT_SPINE_OWNER = "broker_fact_spine_v1"
BROKER_OBSERVATION_HEALTH_OWNER = "broker_observation_health_v1"
BROKER_OBSERVATION_TRUST_DEPENDENCY_OWNER = "broker_observation_trust_dependency_v1"
CORE2_CORE1_DEPENDENCY_GATE_OWNER = "core2_core1_dependency_gate_v1"

RAW_ENVELOPE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_raw_evidence_envelope.v1.schema.json"
)
FACT_SCHEMA_RELPATHS: dict[str, str] = {
    "observation_session_fact": "governance/04_DATA/SCHEMAS/C2/FACTS/observation_session_fact.v1.schema.json",
    "observed_order_fact": "governance/04_DATA/SCHEMAS/C2/FACTS/observed_order_fact.v1.schema.json",
    "observed_order_status_fact": "governance/04_DATA/SCHEMAS/C2/FACTS/observed_order_status_fact.v1.schema.json",
    "observed_fill_fact": "governance/04_DATA/SCHEMAS/C2/FACTS/observed_fill_fact.v1.schema.json",
    "observed_position_fact": "governance/04_DATA/SCHEMAS/C2/FACTS/observed_position_fact.v1.schema.json",
}
BROKER_OBSERVATION_HEALTH_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_health.v1.schema.json"
)
BROKER_FACT_SPINE_AUDIT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_fact_spine_audit.v1.schema.json"
)
BROKER_OBSERVATION_TRUST_DEPENDENCY_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_trust_dependency.v1.schema.json"
)
CORE1_PRE_CORE2_READINESS_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/core1_pre_core2_readiness.v1.schema.json"
)

EVENT_IDENTITY_RULE_VERSION = "broker_fact_identity_v1"
TRUST_RULE_OWNER = "broker_observation_health_v1"
DETERMINISTIC_SENTINEL_UTC = "1970-01-01T00:00:00Z"

RAW_ATTRIBUTION_ATTRIBUTED = "ATTRIBUTED"
RAW_ATTRIBUTION_PARTIAL = "PARTIAL"
RAW_ATTRIBUTION_AMBIGUOUS = "AMBIGUOUS"
RAW_ATTRIBUTION_FOREIGN = "FOREIGN"
RAW_ATTRIBUTION_UNRESOLVED = "UNRESOLVED"

QUALITY_OK = "OK"
QUALITY_MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
QUALITY_UNSUPPORTED_EVENT = "UNSUPPORTED_EVENT_TYPE"
QUALITY_MISSING_REQUIRED_IDENTITY = "MISSING_REQUIRED_IDENTITY"

DUPLICATE_UNIQUE = "UNIQUE"
DUPLICATE_EVENT = "DUPLICATE_EVENT"
DUPLICATE_REPLAY_OVERLAP = "REPLAY_OVERLAP"
DUPLICATE_CONFLICT = "CONFLICTING_DUPLICATE_CLASSIFICATION"

REPLAY_NOT_OBSERVED = "NOT_OBSERVED"
REPLAYING = "REPLAYING"
REPLAY_COMPLETE = "REPLAY_COMPLETE"
REPLAY_UNCERTAIN = "REPLAY_UNCERTAIN"

RECONNECT_NONE = "NONE"
RECONNECT_IN_PROGRESS = "RECONNECT_IN_PROGRESS"
RECONNECT_RECOVERED = "RECONNECT_RECOVERED"

GAP_NONE = "NONE"
GAP_SUSPECTED = "GAP_SUSPECTED"
GAP_UNRESOLVED = "GAP_UNRESOLVED"

DOWNSTREAM_NORMAL = "NORMAL_CONSUMPTION"
DOWNSTREAM_DEGRADED_ONLY = "DEGRADED_ONLY"
DOWNSTREAM_FAIL_CLOSED = "FAIL_CLOSED"

READINESS_READY = "READY"
READINESS_DEGRADED_ONLY = "DEGRADED_ONLY"
READINESS_BLOCKED = "BLOCKED"

LEGACY_BOUNDARY_HARDENED = "HARDENED_RUNTIME_DAY"
LEGACY_BOUNDARY_LEGACY = "LEGACY_RUNTIME_DAY_NON_CONSUMABLE"
LEGACY_BOUNDARY_MIXED = "MIXED_RUNTIME_DAY_BLOCKED"
LEGACY_BOUNDARY_MISSING = "RAW_JOURNAL_MISSING"

CORE2_COMPATIBILITY_HARD_CUT = "HARD_CUT_HARDENED_ONLY"

REPLAY_CLASSIFICATION_NOT_REPLAY = "NOT_REPLAY"
REPLAY_CLASSIFICATION_REPLAY_SESSION = "REPLAY_SESSION_EVENT"
REPLAY_CLASSIFICATION_REPLAY_OVERLAP = "REPLAY_OVERLAP"
REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK = "DUPLICATE_CALLBACK"
REPLAY_CLASSIFICATION_UNCERTAIN = "REPLAY_UNCERTAIN"

TRUST_TRUSTED = "TRUSTED"
TRUST_DEGRADED = "DEGRADED"
TRUST_BLOCKED = "BLOCKED"

FRESHNESS_FRESH = "FRESH"
FRESHNESS_STALE = "STALE"
FRESHNESS_UNAVAILABLE = "UNAVAILABLE"

SESSION_HEALTHY = "HEALTHY"
SESSION_RECONNECTING = "RECONNECT_IN_PROGRESS"
SESSION_UNAVAILABLE = "UNAVAILABLE"

SEQ_OK = "OK"
SEQ_UNCERTAIN = "UNCERTAIN"

RC_STREAM_STALE = "BROKER_OBSERVATION_STREAM_STALE"
RC_OBSERVER_SESSION_UNAVAILABLE = "BROKER_OBSERVER_SESSION_UNAVAILABLE"
RC_RECONNECT_IN_PROGRESS = "BROKER_OBSERVATION_RECONNECT_IN_PROGRESS"
RC_RECONNECT_RECOVERED = "BROKER_OBSERVATION_RECONNECT_RECOVERED"
RC_REPLAY_IN_PROGRESS = "BROKER_OBSERVATION_REPLAY_IN_PROGRESS"
RC_REPLAY_COMPLETE = "BROKER_OBSERVATION_REPLAY_COMPLETE"
RC_REPLAY_UNCERTAIN = "BROKER_OBSERVATION_REPLAY_UNCERTAIN"
RC_REPLAY_OVERLAP_DETECTED = "BROKER_OBSERVATION_REPLAY_OVERLAP_DETECTED"
RC_SNAPSHOT_GAP_SUSPECTED = "BROKER_OBSERVATION_SNAPSHOT_GAP_SUSPECTED"
RC_GAP_UNRESOLVED = "BROKER_OBSERVATION_GAP_UNRESOLVED"
RC_SEQUENCE_UNCERTAINTY = "BROKER_OBSERVATION_SEQUENCE_UNCERTAINTY"
RC_ENVIRONMENT_MISSING = "BROKER_OBSERVATION_ENVIRONMENT_MISSING"
RC_ACCOUNT_MISSING = "BROKER_OBSERVATION_ACCOUNT_MISSING"
RC_CLIENT_ID_OBSERVER_MISSING = "BROKER_OBSERVATION_CLIENT_ID_OBSERVER_MISSING"
RC_ATTRIBUTION_PARTIAL = "BROKER_OBSERVATION_ATTRIBUTION_PARTIAL"
RC_SLEEVE_ATTRIBUTION_AMBIGUOUS = "BROKER_OBSERVATION_ATTRIBUTION_AMBIGUOUS"
RC_FOREIGN_MANUAL_SUSPECTED = "BROKER_OBSERVATION_FOREIGN_BROKER_ACTIVITY_SUSPECTED"
RC_ATTRIBUTION_UNRESOLVED = "BROKER_OBSERVATION_ATTRIBUTION_UNRESOLVED"
RC_MALFORMED_PAYLOAD = "BROKER_OBSERVATION_MALFORMED_PAYLOAD"
RC_UNSUPPORTED_EVENT_TYPE = "BROKER_OBSERVATION_UNSUPPORTED_EVENT_TYPE"
RC_MISSING_CONTRACT_IDENTITY = "BROKER_OBSERVATION_MISSING_CONTRACT_IDENTITY"
RC_MISSING_ORDER_IDENTIFIER = "BROKER_OBSERVATION_MISSING_ORDER_IDENTIFIER"
RC_MISSING_EXECUTION_IDENTIFIER = "BROKER_OBSERVATION_MISSING_EXECUTION_IDENTIFIER"
RC_CANONICAL_EVENT_IDENTITY_UNRESOLVED = "BROKER_OBSERVATION_CANONICAL_EVENT_IDENTITY_UNRESOLVED"
RC_ORDERING_BASIS_UNRESOLVED = "BROKER_OBSERVATION_ORDERING_BASIS_UNRESOLVED"
RC_DUPLICATE_EVENT_DETECTED = "BROKER_OBSERVATION_DUPLICATE_EVENT_DETECTED"
RC_DUPLICATE_CLASSIFICATION_CONFLICT = "BROKER_OBSERVATION_DUPLICATE_CLASSIFICATION_CONFLICT"
RC_REPLAY_OVERLAP_UNRESOLVED = "BROKER_OBSERVATION_REPLAY_OVERLAP_UNRESOLVED"
RC_DOWNSTREAM_DEGRADED_ONLY = "BROKER_OBSERVATION_DOWNSTREAM_CONSUMPTION_DEGRADED_ONLY"
RC_DOWNSTREAM_BLOCKED = "BROKER_OBSERVATION_DOWNSTREAM_CONSUMPTION_BLOCKED"
RC_CORE2_REQUIRED_ARTIFACT_MISSING = "CORE2_CORE1_REQUIRED_ARTIFACT_MISSING"
RC_CORE2_RAW_DIRECT_CONSUMPTION_FORBIDDEN = "CORE2_CORE1_RAW_JOURNAL_DIRECT_CONSUMPTION_FORBIDDEN"
RC_CORE1_IDENTITY_STABILITY_UNPROVEN = "CORE1_IDENTITY_STABILITY_UNPROVEN"
RC_CORE1_LEGACY_RUNTIME_DAY_NON_CONSUMABLE = "CORE1_LEGACY_RUNTIME_DAY_NON_CONSUMABLE"
RC_CORE1_MIXED_RUNTIME_DAY_BLOCKED = "CORE1_MIXED_RUNTIME_DAY_BLOCKED"
RC_CORE1_RAW_JOURNAL_MISSING = "CORE1_RAW_JOURNAL_MISSING"

SUPPORTED_SESSION_EVENTS = {
    "starting",
    "nextValidId",
    "reqAllOpenOrders",
    "reqExecutions",
    "reqPositions",
    "reqAccountSummary",
    "poll_reqAllOpenOrders",
    "poll_reqExecutions",
    "openOrderEnd",
    "execDetailsEnd",
    "positionEnd",
    "accountSummary",
    "accountSummaryEnd",
    "error",
    "bootstrapHandshakeComplete",
    "connectionClosed",
    "stopped",
}
FACT_EVENT_ORDER = "openOrder"
FACT_EVENT_ORDER_STATUS = "orderStatus"
FACT_EVENT_FILL = "execDetails"
FACT_EVENT_COMMISSION = "commissionReport"
FACT_EVENT_POSITION = "position"
REPLAY_REQUEST_EVENTS = {"reqAllOpenOrders", "poll_reqAllOpenOrders", "reqExecutions", "poll_reqExecutions"}
REPLAY_END_EVENTS = {"openOrderEnd", "execDetailsEnd", "bootstrapHandshakeComplete"}
CONNECTED_EVENTS = {"nextValidId", "bootstrapHandshakeComplete"}
DISCONNECTED_EVENTS = {"connectionClosed", "stopped"}
RECONNECT_ERROR_CODES = {"1100", "1101", "1102"}


@dataclass(frozen=True)
class BrokerFactSpineMaterializationV1:
    raw_journal_path: Path
    fact_ledger_paths: dict[str, Path]
    health_path: Path
    trust_dependency_path: Path
    audit_path: Path
    summary: dict[str, Any]


@dataclass(frozen=True)
class Core1PreCore2ReadinessV1:
    report_path: Path
    payload: dict[str, Any]


class BrokerRawEvidenceJournalWriterV1:
    def __init__(
        self,
        *,
        repo_root: Path,
        execution_root_path: Path,
        day_utc: str,
        environment: str,
        sleeve_id: str,
        source_adapter_name: str,
        source_session_id: str,
        source_path: str,
    ) -> None:
        self.repo_root = Path(repo_root).resolve()
        self.execution_root_path = Path(execution_root_path).resolve()
        self.day_utc = parse_day_utc_v1(day_utc)
        self.source_adapter_name = str(source_adapter_name).strip()
        self.source_session_id = str(source_session_id).strip()
        self.source_path = str(source_path).strip() or self.source_adapter_name
        self.governed_identity = resolve_governed_execution_identity_v1(
            repo_root=self.repo_root,
            environment=environment,
            sleeve_id=sleeve_id,
        )
        self.raw_journal_path = resolve_broker_raw_journal_path(
            execution_root_path=self.execution_root_path,
            day_utc=self.day_utc,
        )
        self.raw_journal_path.parent.mkdir(parents=True, exist_ok=True)
        self._journal_sequence_number = _last_raw_journal_sequence(self.raw_journal_path)
        self._source_line_number = 0
        self._handle = self.raw_journal_path.open("a", encoding="utf-8")

    def close(self) -> None:
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
        except Exception:
            pass
        try:
            self._handle.close()
        except Exception:
            pass

    def write_payload(self, raw_payload: Mapping[str, Any]) -> dict[str, Any]:
        self._source_line_number += 1
        self._journal_sequence_number += 1
        payload = build_raw_evidence_envelope_from_payload_v1(
            day_utc=self.day_utc,
            raw_payload=raw_payload,
            source_adapter_name=self.source_adapter_name,
            source_session_id=self.source_session_id,
            source_path=self.source_path,
            source_line_number=self._source_line_number,
            governed_identity=self.governed_identity,
        )
        payload["journal_sequence_number"] = self._journal_sequence_number
        validate_against_repo_schema_v1(payload, REPO_ROOT, RAW_ENVELOPE_SCHEMA_RELPATH)
        self._handle.write(canonical_json_bytes_v1(payload).decode("utf-8") + "\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        return payload


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_text(text: str) -> datetime | None:
    value = str(text or "").strip()
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce_utc_text(text: str, *, fallback: str) -> str:
    parsed = _parse_utc_text(text)
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z") if parsed else fallback


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()


def _json_loads_optional(text: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(text)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def resolve_legacy_broker_event_log_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "execution_evidence_v1"
        / "broker_events"
        / parse_day_utc_v1(day_utc)
        / "broker_event_log.v1.jsonl"
    ).resolve()


def resolve_broker_fact_spine_root(*, execution_root_path: Path) -> Path:
    return (Path(execution_root_path).resolve() / "broker_fact_spine_v1").resolve()


def resolve_broker_raw_journal_path(*, execution_root_path: Path, day_utc: str) -> Path:
    return (
        resolve_broker_fact_spine_root(execution_root_path=execution_root_path)
        / "raw_journal"
        / parse_day_utc_v1(day_utc)
        / "broker_raw_evidence_envelope.v1.jsonl"
    ).resolve()


def resolve_fact_ledger_path(*, execution_root_path: Path, day_utc: str, schema_id: str) -> Path:
    return (
        resolve_broker_fact_spine_root(execution_root_path=execution_root_path)
        / "fact_ledger"
        / parse_day_utc_v1(day_utc)
        / f"{schema_id}.v1.jsonl"
    ).resolve()


def resolve_broker_observation_health_path(*, execution_root_path: Path, day_utc: str) -> Path:
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "broker_observation_health_v1"
        / parse_day_utc_v1(day_utc)
        / "broker_observation_health.v1.json"
    ).resolve()


def resolve_broker_observation_trust_dependency_path(*, execution_root_path: Path, day_utc: str) -> Path:
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "broker_observation_trust_dependency_v1"
        / parse_day_utc_v1(day_utc)
        / "broker_observation_trust_dependency.v1.json"
    ).resolve()


def resolve_broker_fact_spine_audit_path(*, execution_root_path: Path, day_utc: str) -> Path:
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "broker_fact_spine_audit_v1"
        / parse_day_utc_v1(day_utc)
        / "broker_fact_spine_audit.v1.json"
    ).resolve()


def resolve_core1_pre_core2_readiness_path(*, execution_root_path: Path, day_utc: str) -> Path:
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "core1_pre_core2_readiness_v1"
        / parse_day_utc_v1(day_utc)
        / "core1_pre_core2_readiness.v1.json"
    ).resolve()


def _read_jsonl_objects(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        payload = _json_loads_optional(line)
        if payload is not None:
            rows.append(payload)
    return rows


def _existing_ids(path: Path, *, id_field: str) -> set[str]:
    return {
        str(row.get(id_field) or "").strip()
        for row in _read_jsonl_objects(path)
        if str(row.get(id_field) or "").strip()
    }


def _last_raw_journal_sequence(path: Path) -> int:
    last = 0
    for row in _read_jsonl_objects(path):
        try:
            value = int(row.get("journal_sequence_number"))
        except Exception:
            continue
        if value > last:
            last = value
    return last


def _append_validated_jsonl_records(
    *,
    path: Path,
    records: Sequence[dict[str, Any]],
    schema_relpath: str,
    id_field: str,
) -> tuple[int, int]:
    existing_ids = _existing_ids(path, id_field=id_field)
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            record_id = str(record.get(id_field) or "").strip()
            if not record_id:
                raise ValueError(f"MISSING_ID_FIELD:{id_field}")
            if record_id in existing_ids:
                skipped += 1
                continue
            validate_against_repo_schema_v1(record, REPO_ROOT, schema_relpath)
            handle.write(canonical_json_bytes_v1(record).decode("utf-8") + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            existing_ids.add(record_id)
            written += 1
    return written, skipped


def _ordering_basis_rank(ordering_basis: str) -> int:
    return {
        "OBSERVED_UTC": 0,
        "SOURCE_SEQUENCE_NUMBER": 1,
        "SOURCE_EVENT_IDENTITY": 2,
        "UNRESOLVED": 3,
    }.get(str(ordering_basis or "").strip(), 4)


def _raw_row_normalization_key(row: Mapping[str, Any]) -> tuple[int, str, int, str]:
    return (
        _ordering_basis_rank(str(row.get("ordering_basis") or "").strip()),
        str(row.get("ordering_key") or "").strip(),
        int(row.get("journal_sequence_number") or 0),
        str(row.get("raw_record_id") or "").strip(),
    )


def _source_args(raw_payload: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(raw_payload, Mapping):
        return []
    ib_fields = raw_payload.get("ib_fields")
    if not isinstance(ib_fields, Mapping):
        return []
    args = ib_fields.get("args")
    if not isinstance(args, list):
        return []
    return [
        str(item.get("value") or "").strip()
        for item in args
        if isinstance(item, Mapping) and str(item.get("value") or "").strip()
    ]


def _kv_from_args(args: Sequence[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in args:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = str(key).strip()
        value = str(value).strip()
        if key and key not in mapping:
            mapping[key] = value
    return mapping


def _nonempty_text(value: Any) -> str:
    return str(value or "").strip()


def _extract_lineage_text(
    *,
    raw_payload: Mapping[str, Any] | None,
    args_mapping: Mapping[str, str],
    aliases: Sequence[str],
) -> str:
    for alias in aliases:
        value = _nonempty_text(args_mapping.get(alias))
        if value:
            return value

    if not isinstance(raw_payload, Mapping):
        return ""

    containers: list[Mapping[str, Any]] = [raw_payload]
    for key in ("lineage", "attribution", "execution", "engine", "metadata", "ib_fields"):
        nested = raw_payload.get(key)
        if isinstance(nested, Mapping):
            containers.append(nested)

    for container in containers:
        for alias in aliases:
            value = _nonempty_text(container.get(alias))
            if value:
                return value
    return ""


def _event_type(raw_payload: Mapping[str, Any] | None) -> str:
    if not isinstance(raw_payload, Mapping):
        return ""
    return str(raw_payload.get("event_type") or "").strip()


def _broker_obj(raw_payload: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(raw_payload, Mapping):
        return {}
    broker = raw_payload.get("broker")
    return broker if isinstance(broker, Mapping) else {}


def _source_session_id(
    *,
    raw_payload: Mapping[str, Any] | None,
    source_adapter_name: str,
    environment: str,
    client_id_observer: str,
    day_utc: str,
) -> str:
    if isinstance(raw_payload, Mapping) and str(raw_payload.get("source_session_id") or "").strip():
        return str(raw_payload.get("source_session_id") or "").strip()
    return f"{source_adapter_name}:{environment or 'UNKNOWN'}:{client_id_observer or 'UNKNOWN'}:{parse_day_utc_v1(day_utc)}"


def _explicit_hint(raw_payload: Mapping[str, Any] | None, field_name: str) -> str:
    if not isinstance(raw_payload, Mapping):
        return ""
    return str(raw_payload.get(field_name) or "").strip()


def _source_sequence_number(raw_payload: Mapping[str, Any] | None) -> int:
    if not isinstance(raw_payload, Mapping):
        return 0
    for key in ("sequence_number", "source_sequence_number"):
        try:
            value = int(raw_payload.get(key) or 0)
        except Exception:
            value = 0
        if value > 0:
            return value
    return 0


def _direct_account_from_payload(raw_payload: Mapping[str, Any] | None) -> str:
    args = _source_args(raw_payload)
    mapping = _kv_from_args(args)
    for key in ("account", "acctCode", "accountId"):
        value = str(mapping.get(key) or _explicit_hint(raw_payload, key) or "").strip()
        if value:
            return value
    return str(_explicit_hint(raw_payload, "explicit_account_id") or "").strip()


def _observed_utc_fields(raw_payload: Mapping[str, Any] | None) -> tuple[str, str]:
    broker_event_utc = _coerce_utc_text(_explicit_hint(raw_payload, "broker_event_utc"), fallback="")
    if broker_event_utc:
        return broker_event_utc, "BROKER_EVENT_UTC"
    received_utc = _coerce_utc_text(str((raw_payload or {}).get("received_utc") or ""), fallback="")
    if received_utc:
        return received_utc, "LEGACY_RECEIVED_UTC"
    return DETERMINISTIC_SENTINEL_UTC, "DETERMINISTIC_SENTINEL_UTC"


def _source_event_identity(raw_payload: Mapping[str, Any] | None, *, source_adapter_name: str) -> str:
    args = _source_args(raw_payload)
    broker = _broker_obj(raw_payload)
    identity_payload = {
        "schema_id": str((raw_payload or {}).get("schema_id") or "").strip(),
        "schema_version": str((raw_payload or {}).get("schema_version") or "").strip(),
        "source_adapter_name": str(source_adapter_name).strip(),
        "event_type": _event_type(raw_payload),
        "broker_name": str(broker.get("name") or "").strip(),
        "broker_environment": str(broker.get("environment") or "").strip().upper(),
        "broker_client_id": str(broker.get("client_id") or "").strip(),
        "args": list(args),
        "explicit_sleeve_id": _explicit_hint(raw_payload, "explicit_sleeve_id").upper(),
        "explicit_account_id": _explicit_hint(raw_payload, "explicit_account_id"),
    }
    return _sha256_text(canonical_json_bytes_v1(identity_payload).decode("utf-8"))


def _ordering_basis_and_key(
    *,
    observed_utc: str,
    observed_utc_source: str,
    source_sequence_number: int,
    source_event_identity: str,
) -> tuple[str, str]:
    if observed_utc_source in {"BROKER_EVENT_UTC", "LEGACY_RECEIVED_UTC"}:
        return "OBSERVED_UTC", f"{observed_utc}|{source_event_identity}"
    if source_sequence_number > 0:
        return "SOURCE_SEQUENCE_NUMBER", f"{source_sequence_number:020d}|{source_event_identity}"
    if source_event_identity:
        return "SOURCE_EVENT_IDENTITY", source_event_identity
    return "UNRESOLVED", ""


def _event_is_reconnect_error(raw_payload: Mapping[str, Any] | None) -> bool:
    if _event_type(raw_payload) != "error":
        return False
    mapping = _kv_from_args(_source_args(raw_payload))
    return str(mapping.get("errorCode") or "").strip() in RECONNECT_ERROR_CODES


def _derive_replay_state_hint(raw_payload: Mapping[str, Any] | None) -> str:
    event_type = _event_type(raw_payload)
    if event_type in REPLAY_REQUEST_EVENTS:
        return REPLAYING
    if event_type in REPLAY_END_EVENTS:
        return REPLAY_COMPLETE
    if _event_is_reconnect_error(raw_payload):
        return REPLAY_UNCERTAIN
    return REPLAY_NOT_OBSERVED


def _derive_reconnect_state_hint(raw_payload: Mapping[str, Any] | None) -> str:
    event_type = _event_type(raw_payload)
    if event_type == "connectionClosed" or _event_is_reconnect_error(raw_payload):
        return RECONNECT_IN_PROGRESS
    return RECONNECT_NONE


def _derive_gap_state_hint(raw_payload: Mapping[str, Any] | None) -> str:
    event_type = _event_type(raw_payload)
    if event_type in REPLAY_REQUEST_EVENTS:
        return GAP_SUSPECTED
    if event_type == "connectionClosed" or _event_is_reconnect_error(raw_payload):
        return GAP_UNRESOLVED
    return GAP_NONE


def _classify_raw_attribution(
    *,
    raw_payload: Mapping[str, Any] | None,
    governed_identity: GovernedExecutionIdentityV1,
) -> tuple[str, list[str], dict[str, str]]:
    broker = _broker_obj(raw_payload)
    environment = str(broker.get("environment") or _explicit_hint(raw_payload, "environment")).strip().upper()
    client_id_observer = str(broker.get("client_id") or _explicit_hint(raw_payload, "client_id_observer")).strip()
    explicit_sleeve_id = _explicit_hint(raw_payload, "explicit_sleeve_id").upper()
    direct_account_id = _direct_account_from_payload(raw_payload)
    event_type = _event_type(raw_payload)

    identity = {
        "environment": environment,
        "account_id": "",
        "client_id_observer": client_id_observer,
        "sleeve_id": "",
        "host": governed_identity.host,
        "port": str(governed_identity.port),
    }

    if not environment:
        return RAW_ATTRIBUTION_UNRESOLVED, [RC_ENVIRONMENT_MISSING, RC_ATTRIBUTION_UNRESOLVED], identity
    identity["environment"] = environment
    if not client_id_observer:
        return RAW_ATTRIBUTION_UNRESOLVED, [RC_CLIENT_ID_OBSERVER_MISSING, RC_ATTRIBUTION_UNRESOLVED], identity
    if environment != governed_identity.environment:
        return RAW_ATTRIBUTION_FOREIGN, [RC_FOREIGN_MANUAL_SUSPECTED], identity
    if client_id_observer != str(governed_identity.client_id_observer):
        return RAW_ATTRIBUTION_FOREIGN, [RC_FOREIGN_MANUAL_SUSPECTED], identity
    if explicit_sleeve_id and explicit_sleeve_id != governed_identity.sleeve_id:
        identity["account_id"] = governed_identity.account_id
        return RAW_ATTRIBUTION_AMBIGUOUS, [RC_SLEEVE_ATTRIBUTION_AMBIGUOUS], identity
    if direct_account_id and direct_account_id != governed_identity.account_id:
        identity["account_id"] = direct_account_id
        return RAW_ATTRIBUTION_FOREIGN, [RC_FOREIGN_MANUAL_SUSPECTED], identity
    identity["sleeve_id"] = governed_identity.sleeve_id
    if event_type == FACT_EVENT_POSITION and not direct_account_id:
        return RAW_ATTRIBUTION_PARTIAL, [RC_ACCOUNT_MISSING, RC_ATTRIBUTION_PARTIAL], identity
    identity["account_id"] = governed_identity.account_id
    return RAW_ATTRIBUTION_ATTRIBUTED, [], identity


def _classify_raw_quality(
    raw_payload: Mapping[str, Any] | None,
    *,
    ordering_basis: str,
    source_event_identity: str,
) -> tuple[str, list[str]]:
    if raw_payload is None:
        return QUALITY_MALFORMED_PAYLOAD, [RC_MALFORMED_PAYLOAD]
    event_type = _event_type(raw_payload)
    supported = event_type in SUPPORTED_SESSION_EVENTS or event_type in {
        FACT_EVENT_ORDER,
        FACT_EVENT_ORDER_STATUS,
        FACT_EVENT_FILL,
        FACT_EVENT_COMMISSION,
        FACT_EVENT_POSITION,
    }
    if not supported:
        return QUALITY_UNSUPPORTED_EVENT, [RC_UNSUPPORTED_EVENT_TYPE]
    reason_codes: list[str] = []
    if ordering_basis == "UNRESOLVED":
        reason_codes.append(RC_ORDERING_BASIS_UNRESOLVED)
    if not source_event_identity:
        reason_codes.append(RC_CANONICAL_EVENT_IDENTITY_UNRESOLVED)
    if reason_codes:
        return QUALITY_MISSING_REQUIRED_IDENTITY, reason_codes
    return QUALITY_OK, []


def build_raw_evidence_envelope_from_payload_v1(
    *,
    day_utc: str,
    raw_payload: Mapping[str, Any] | None,
    source_adapter_name: str,
    source_session_id: str,
    source_path: str,
    source_line_number: int,
    governed_identity: GovernedExecutionIdentityV1,
) -> dict[str, Any]:
    ingested_utc = _utc_now()
    observed_utc, observed_utc_source = _observed_utc_fields(raw_payload)
    resolved_source_session_id = str(source_session_id).strip() or _source_session_id(
        raw_payload=raw_payload,
        source_adapter_name=source_adapter_name,
        environment=governed_identity.environment,
        client_id_observer=str(governed_identity.client_id_observer),
        day_utc=parse_day_utc_v1(day_utc),
    )
    source_sequence_number = _source_sequence_number(raw_payload)
    source_event_identity = _source_event_identity(raw_payload, source_adapter_name=source_adapter_name)
    ordering_basis, ordering_key = _ordering_basis_and_key(
        observed_utc=observed_utc,
        observed_utc_source=observed_utc_source,
        source_sequence_number=source_sequence_number,
        source_event_identity=source_event_identity,
    )
    replay_state_hint = _derive_replay_state_hint(raw_payload)
    reconnect_state_hint = _derive_reconnect_state_hint(raw_payload)
    gap_state_hint = _derive_gap_state_hint(raw_payload)
    attribution_status, attribution_reason_codes, attributed_identity = _classify_raw_attribution(
        raw_payload=raw_payload,
        governed_identity=governed_identity,
    )
    quality_status, quality_reason_codes = _classify_raw_quality(
        raw_payload,
        ordering_basis=ordering_basis,
        source_event_identity=source_event_identity,
    )
    payload_json = dict(raw_payload) if isinstance(raw_payload, Mapping) else None
    payload_text = (
        canonical_json_bytes_v1(payload_json).decode("utf-8")
        if payload_json is not None
        else str(raw_payload or "")
    )
    return {
        "schema_id": "broker_raw_evidence_envelope",
        "schema_version": "v1",
        "raw_record_id": _sha256_text(
            "|".join(
                [
                    str(source_path).strip(),
                    str(int(source_line_number)),
                    _sha256_text(payload_text),
                    BROKER_FACT_SPINE_OWNER,
                ]
            )
        ),
        "journal_sequence_number": 0,
        "ingested_utc": ingested_utc,
        "observed_utc": observed_utc,
        "observed_utc_source": observed_utc_source,
        "source_adapter_name": str(source_adapter_name).strip(),
        "source_session_id": resolved_source_session_id,
        "source_sequence_number": int(source_sequence_number),
        "source_event_type": _event_type(raw_payload),
        "source_event_identity": source_event_identity,
        "source_path": str(source_path).strip(),
        "source_line_number": int(source_line_number),
        "source_payload_schema_id": str((raw_payload or {}).get("schema_id") or "").strip(),
        "source_payload_schema_version": str((raw_payload or {}).get("schema_version") or "").strip(),
        "source_payload_sha256": _sha256_text(payload_text),
        "source_received_utc": str((raw_payload or {}).get("received_utc") or "").strip(),
        "ordering_basis": ordering_basis,
        "ordering_key": ordering_key,
        "replay_state_hint": replay_state_hint,
        "reconnect_state_hint": reconnect_state_hint,
        "gap_state_hint": gap_state_hint,
        "event_identity_rule_version": EVENT_IDENTITY_RULE_VERSION,
        "environment": attributed_identity["environment"],
        "account_id": attributed_identity["account_id"],
        "client_id_observer": attributed_identity["client_id_observer"],
        "sleeve_id": attributed_identity["sleeve_id"],
        "host": attributed_identity["host"],
        "port": attributed_identity["port"],
        "attribution_status": attribution_status,
        "attribution_reason_codes": attribution_reason_codes,
        "quality_status": quality_status,
        "quality_reason_codes": quality_reason_codes,
        "raw_payload_text": payload_text,
        "raw_payload_json": payload_json,
    }


def build_raw_evidence_envelopes_from_source_v1(
    *,
    repo_root: Path,
    day_utc: str,
    environment: str,
    sleeve_id: str,
    source_path: Path,
    source_adapter_name: str = "legacy_ib_execution_observer_v1",
) -> list[dict[str, Any]]:
    repo_root = Path(repo_root).resolve()
    day = parse_day_utc_v1(day_utc)
    governed_identity = resolve_governed_execution_identity_v1(
        repo_root=repo_root,
        environment=environment,
        sleeve_id=sleeve_id,
    )
    lines = source_path.read_text(encoding="utf-8", errors="replace").splitlines()
    envelopes: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(lines, start=1):
        raw_text = str(raw_line).rstrip("\n")
        if not raw_text.strip():
            continue
        raw_payload = _json_loads_optional(raw_text)
        source_session_id = _source_session_id(
            raw_payload=raw_payload,
            source_adapter_name=source_adapter_name,
            environment=governed_identity.environment,
            client_id_observer=str(governed_identity.client_id_observer),
            day_utc=day,
        )
        envelopes.append(
            build_raw_evidence_envelope_from_payload_v1(
                day_utc=day,
                raw_payload=raw_payload,
                source_adapter_name=str(
                    _explicit_hint(raw_payload, "source_adapter_name") or source_adapter_name
                ).strip(),
                source_session_id=source_session_id,
                source_path=str(source_path.resolve()),
                source_line_number=line_number,
                governed_identity=governed_identity,
            )
        )
    return envelopes


def append_raw_evidence_envelopes_v1(
    *,
    raw_journal_path: Path,
    raw_envelopes: Sequence[dict[str, Any]],
) -> tuple[int, int]:
    next_sequence = _last_raw_journal_sequence(raw_journal_path)
    prepared: list[dict[str, Any]] = []
    existing_ids = _existing_ids(raw_journal_path, id_field="raw_record_id")
    for row in raw_envelopes:
        raw_record_id = str(row.get("raw_record_id") or "").strip()
        if raw_record_id in existing_ids:
            continue
        next_sequence += 1
        payload = dict(row)
        payload["journal_sequence_number"] = next_sequence
        prepared.append(payload)
        existing_ids.add(raw_record_id)
    written, skipped = _append_validated_jsonl_records(
        path=raw_journal_path,
        records=prepared,
        schema_relpath=RAW_ENVELOPE_SCHEMA_RELPATH,
        id_field="raw_record_id",
    )
    return written, skipped + max(0, len(raw_envelopes) - len(prepared))


def _contract_identity_from_args(args: Sequence[str]) -> tuple[dict[str, str], list[str]]:
    mapping = _kv_from_args(args)
    contract_identity = {
        "symbol": str(mapping.get("symbol") or ""),
        "sec_type": str(mapping.get("secType") or ""),
        "exchange": str(mapping.get("exchange") or ""),
        "currency": str(mapping.get("currency") or ""),
        "raw_summary": str(
            mapping.get("contract")
            or mapping.get("contract_summary")
            or " ".join(
                item
                for item in args
                if item.startswith("symbol=") or item.startswith("contract=")
            )
        ).strip(),
    }
    quality_reason_codes = [RC_MISSING_CONTRACT_IDENTITY] if not any(contract_identity.values()) else []
    return contract_identity, quality_reason_codes


def _base_fact_payload(
    *,
    schema_id: str,
    raw_row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_id": schema_id,
        "schema_version": "v1",
        "fact_record_id": "",
        "canonical_event_identity": "",
        "duplicate_classification": DUPLICATE_UNIQUE,
        "replay_classification": REPLAY_CLASSIFICATION_NOT_REPLAY,
        "event_identity_rule_version": str(
            raw_row.get("event_identity_rule_version") or EVENT_IDENTITY_RULE_VERSION
        ),
        "observed_utc": str(raw_row.get("observed_utc") or "").strip(),
        "observed_utc_source": str(raw_row.get("observed_utc_source") or "").strip(),
        "normalized_utc": _utc_now(),
        "raw_record_id": str(raw_row.get("raw_record_id") or "").strip(),
        "journal_sequence_number": int(raw_row.get("journal_sequence_number") or 0),
        "source_session_id": str(raw_row.get("source_session_id") or "").strip(),
        "source_sequence_number": int(raw_row.get("source_sequence_number") or 0),
        "source_event_type": str(raw_row.get("source_event_type") or "").strip(),
        "source_event_identity": str(raw_row.get("source_event_identity") or "").strip(),
        "ordering_basis": str(raw_row.get("ordering_basis") or "").strip(),
        "ordering_key": str(raw_row.get("ordering_key") or "").strip(),
        "replay_state_hint": str(raw_row.get("replay_state_hint") or "").strip(),
        "reconnect_state_hint": str(raw_row.get("reconnect_state_hint") or "").strip(),
        "gap_state_hint": str(raw_row.get("gap_state_hint") or "").strip(),
        "environment": str(raw_row.get("environment") or "").strip(),
        "account_id": str(raw_row.get("account_id") or "").strip(),
        "client_id_observer": str(raw_row.get("client_id_observer") or "").strip(),
        "sleeve_id": str(raw_row.get("sleeve_id") or "").strip(),
        "attribution_status": str(raw_row.get("attribution_status") or "").strip(),
        "attribution_reason_codes": list(raw_row.get("attribution_reason_codes") or []),
        "quality_status": str(raw_row.get("quality_status") or "").strip() or QUALITY_OK,
        "quality_reason_codes": list(raw_row.get("quality_reason_codes") or []),
    }


def _session_state_hint(event_type: str, args: Sequence[str]) -> str:
    if event_type == "starting":
        return "STARTING"
    if event_type in CONNECTED_EVENTS:
        return "CONNECTED"
    if event_type in DISCONNECTED_EVENTS:
        return "DISCONNECTED"
    if event_type == "stopped":
        return "STOPPED"
    if event_type == "error" and any(
        code in " ".join(args)
        for code in ("errorCode=1100", "errorCode=1101", "errorCode=1102")
    ):
        return "RECONNECTING"
    if event_type in REPLAY_REQUEST_EVENTS:
        return "REPLAYING"
    if event_type in REPLAY_END_EVENTS:
        return "REPLAY_COMPLETE"
    return "OBSERVING"


def _build_session_fact(raw_row: Mapping[str, Any], *, session_instance_id: str) -> dict[str, Any]:
    payload_json = raw_row.get("raw_payload_json")
    args = _source_args(payload_json if isinstance(payload_json, Mapping) else None)
    payload = _base_fact_payload(schema_id="observation_session_fact", raw_row=raw_row)
    payload.update(
        {
            "session_instance_id": session_instance_id,
            "session_event_type": str(raw_row.get("source_event_type") or "").strip(),
            "session_state_hint": _session_state_hint(
                str(raw_row.get("source_event_type") or "").strip(),
                args,
            ),
            "event_summary": " ".join(args).strip(),
        }
    )
    return payload


def _build_order_fact(raw_row: Mapping[str, Any]) -> dict[str, Any]:
    payload_json = raw_row.get("raw_payload_json")
    args = _source_args(payload_json if isinstance(payload_json, Mapping) else None)
    mapping = _kv_from_args(args)
    contract_identity, contract_quality = _contract_identity_from_args(args)
    payload = _base_fact_payload(schema_id="observed_order_fact", raw_row=raw_row)
    payload.update(
        {
            "order_id": str(mapping.get("orderId") or ""),
            "perm_id": str(mapping.get("permId") or ""),
            "contract_identity": contract_identity,
            "order_summary": {
                "action": str(mapping.get("action") or ""),
                "total_quantity": str(mapping.get("totalQuantity") or ""),
                "order_type": str(mapping.get("orderType") or ""),
                "limit_price": str(mapping.get("lmtPrice") or ""),
                "raw_order": str(mapping.get("order") or ""),
                "raw_order_state": str(mapping.get("orderState") or ""),
            },
        }
    )
    payload["quality_reason_codes"] = list(payload["quality_reason_codes"]) + contract_quality
    if not payload["order_id"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
        payload["quality_reason_codes"].append(RC_MISSING_ORDER_IDENTIFIER)
    elif payload["quality_reason_codes"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
    return payload


def _build_order_status_fact(raw_row: Mapping[str, Any]) -> dict[str, Any]:
    payload_json = raw_row.get("raw_payload_json")
    args = _source_args(payload_json if isinstance(payload_json, Mapping) else None)
    mapping = _kv_from_args(args)
    payload = _base_fact_payload(schema_id="observed_order_status_fact", raw_row=raw_row)
    payload.update(
        {
            "order_id": str(mapping.get("orderId") or ""),
            "perm_id": str(mapping.get("permId") or ""),
            "status": str(mapping.get("status") or ""),
            "filled_quantity": str(mapping.get("filled") or ""),
            "remaining_quantity": str(mapping.get("remaining") or ""),
            "avg_fill_price": str(mapping.get("avgFillPrice") or ""),
            "last_fill_price": str(mapping.get("lastFillPrice") or ""),
        }
    )
    if not payload["order_id"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
        payload["quality_reason_codes"] = list(payload["quality_reason_codes"]) + [
            RC_MISSING_ORDER_IDENTIFIER
        ]
    return payload


def _build_fill_fact(raw_row: Mapping[str, Any]) -> dict[str, Any]:
    payload_json = raw_row.get("raw_payload_json")
    args = _source_args(payload_json if isinstance(payload_json, Mapping) else None)
    mapping = _kv_from_args(args)
    contract_identity, contract_quality = _contract_identity_from_args(args)
    payload = _base_fact_payload(schema_id="observed_fill_fact", raw_row=raw_row)
    payload.update(
        {
            "execution_id": str(mapping.get("execId") or mapping.get("executionId") or ""),
            "order_id": str(mapping.get("orderId") or ""),
            "perm_id": str(mapping.get("permId") or ""),
            "contract_identity": contract_identity,
            "fill_quantity": str(mapping.get("shares") or mapping.get("quantity") or ""),
            "fill_price": str(mapping.get("price") or ""),
            "side": str(mapping.get("side") or ""),
            "commission": str(mapping.get("commission") or ""),
            "currency": str(mapping.get("currency") or ""),
        }
    )
    payload["quality_reason_codes"] = list(payload["quality_reason_codes"]) + contract_quality
    if not payload["execution_id"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
        payload["quality_reason_codes"].append(RC_MISSING_EXECUTION_IDENTIFIER)
    elif payload["quality_reason_codes"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
    return payload


def _build_position_fact(raw_row: Mapping[str, Any]) -> dict[str, Any]:
    payload_json = raw_row.get("raw_payload_json")
    payload_map = payload_json if isinstance(payload_json, Mapping) else None
    args = _source_args(payload_json if isinstance(payload_json, Mapping) else None)
    mapping = _kv_from_args(args)
    contract_identity, contract_quality = _contract_identity_from_args(args)
    payload = _base_fact_payload(schema_id="observed_position_fact", raw_row=raw_row)
    payload.update(
        {
            "contract_identity": contract_identity,
            "position_quantity": str(mapping.get("position") or mapping.get("quantity") or ""),
            "average_cost": str(mapping.get("avgCost") or ""),
            "market_price": str(mapping.get("marketPrice") or ""),
            "order_id": _extract_lineage_text(
                raw_payload=payload_map,
                args_mapping=mapping,
                aliases=("orderId", "order_id"),
            ),
            "perm_id": _extract_lineage_text(
                raw_payload=payload_map,
                args_mapping=mapping,
                aliases=("permId", "perm_id"),
            ),
            "native_engine_id": _extract_lineage_text(
                raw_payload=payload_map,
                args_mapping=mapping,
                aliases=("native_engine_id", "nativeEngineId", "nativeEngineID"),
            ),
            "engine_id": _extract_lineage_text(
                raw_payload=payload_map,
                args_mapping=mapping,
                aliases=("engine_id", "engineId", "engineID"),
            ),
            "strategy_engine_id": _extract_lineage_text(
                raw_payload=payload_map,
                args_mapping=mapping,
                aliases=("strategy_engine_id", "strategyEngineId", "strategyEngineID"),
            ),
        }
    )
    payload["quality_reason_codes"] = list(payload["quality_reason_codes"]) + contract_quality
    if not payload["account_id"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
        payload["quality_reason_codes"].append(RC_ACCOUNT_MISSING)
    elif payload["quality_reason_codes"]:
        payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
    return payload


def _canonical_identity_components(payload: Mapping[str, Any]) -> list[str]:
    schema_id = str(payload.get("schema_id") or "").strip()
    source_event_identity = str(payload.get("source_event_identity") or "").strip()
    if schema_id == "observation_session_fact":
        return [
            str(payload.get("session_event_type") or "").strip(),
            str(payload.get("event_summary") or "").strip(),
            source_event_identity,
        ]
    if schema_id == "observed_order_fact":
        return [
            str(payload.get("order_id") or "").strip(),
            str(payload.get("perm_id") or "").strip(),
            str(((payload.get("contract_identity") or {}).get("raw_summary")) or "").strip(),
            str(((payload.get("order_summary") or {}).get("action")) or "").strip(),
            str(((payload.get("order_summary") or {}).get("total_quantity")) or "").strip(),
            source_event_identity,
        ]
    if schema_id == "observed_order_status_fact":
        return [
            str(payload.get("order_id") or "").strip(),
            str(payload.get("perm_id") or "").strip(),
            str(payload.get("status") or "").strip(),
            str(payload.get("filled_quantity") or "").strip(),
            str(payload.get("remaining_quantity") or "").strip(),
            source_event_identity,
        ]
    if schema_id == "observed_fill_fact":
        return [
            str(payload.get("execution_id") or "").strip(),
            str(payload.get("order_id") or "").strip(),
            str(payload.get("perm_id") or "").strip(),
            str(payload.get("fill_quantity") or "").strip(),
            str(payload.get("fill_price") or "").strip(),
            source_event_identity,
        ]
    return [
        str(payload.get("account_id") or "").strip(),
        str(((payload.get("contract_identity") or {}).get("raw_summary")) or "").strip(),
        str(payload.get("position_quantity") or "").strip(),
        str(payload.get("average_cost") or "").strip(),
        source_event_identity,
    ]


def _dedupe_payload_digest(payload: Mapping[str, Any]) -> str:
    stable_payload = {
        key: value
        for key, value in dict(payload).items()
        if key
        not in {
            "fact_record_id",
            "raw_record_id",
            "journal_sequence_number",
            "normalized_utc",
            "source_session_id",
            "observed_utc",
            "observed_utc_source",
            "ordering_basis",
            "ordering_key",
            "replay_state_hint",
            "reconnect_state_hint",
            "gap_state_hint",
        }
    }
    return _canonical_sha256(stable_payload)


def normalize_broker_fact_records_v1(
    *,
    raw_rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    fact_rows: dict[str, list[dict[str, Any]]] = {
        schema_id: [] for schema_id in FACT_SCHEMA_RELPATHS
    }
    seen_canonical_identities: dict[str, tuple[str, str]] = {}
    session_segment_index: dict[str, int] = {}

    ordered_raw_rows = sorted(
        [dict(row) for row in raw_rows],
        key=_raw_row_normalization_key,
    )

    for raw_row in ordered_raw_rows:
        event_type = str(raw_row.get("source_event_type") or "").strip()
        source_session_id = str(raw_row.get("source_session_id") or "").strip()
        if event_type == "starting":
            session_segment_index[source_session_id] = (
                session_segment_index.get(source_session_id, 0) + 1
            )
        current_segment = session_segment_index.get(source_session_id, 1)
        session_instance_id = f"{source_session_id}:segment:{current_segment}"

        payloads: list[dict[str, Any]] = []
        if event_type in SUPPORTED_SESSION_EVENTS:
            payloads.append(
                _build_session_fact(raw_row, session_instance_id=session_instance_id)
            )
        elif event_type == FACT_EVENT_ORDER:
            payloads.append(_build_order_fact(raw_row))
        elif event_type == FACT_EVENT_ORDER_STATUS:
            payloads.append(_build_order_status_fact(raw_row))
        elif event_type in {FACT_EVENT_FILL, FACT_EVENT_COMMISSION}:
            payloads.append(_build_fill_fact(raw_row))
        elif event_type == FACT_EVENT_POSITION:
            payloads.append(_build_position_fact(raw_row))

        for payload in payloads:
            components = _canonical_identity_components(payload)
            if not any(components):
                payload["quality_status"] = QUALITY_MISSING_REQUIRED_IDENTITY
                payload["quality_reason_codes"] = list(
                    payload["quality_reason_codes"]
                ) + [RC_CANONICAL_EVENT_IDENTITY_UNRESOLVED]
                components = [str(payload.get("raw_record_id") or "").strip()]
            canonical_event_identity = _sha256_text(
                "|".join([payload["schema_id"]] + components)
            )
            payload["canonical_event_identity"] = canonical_event_identity
            payload["fact_record_id"] = _sha256_text(
                "|".join([payload["schema_id"], str(payload.get("raw_record_id") or "").strip()])
            )
            payload_digest = _dedupe_payload_digest(payload)
            seen = seen_canonical_identities.get(canonical_event_identity)
            if seen is None:
                payload["duplicate_classification"] = DUPLICATE_UNIQUE
                if payload["replay_state_hint"] in {REPLAYING, REPLAY_COMPLETE}:
                    payload["replay_classification"] = REPLAY_CLASSIFICATION_REPLAY_SESSION
                else:
                    payload["replay_classification"] = REPLAY_CLASSIFICATION_NOT_REPLAY
                seen_canonical_identities[canonical_event_identity] = (
                    str(payload.get("source_session_id") or "").strip(),
                    payload_digest,
                )
            else:
                seen_session_id, seen_digest = seen
                same_session = seen_session_id == str(payload.get("source_session_id") or "").strip()
                if seen_digest != payload_digest:
                    payload["duplicate_classification"] = DUPLICATE_CONFLICT
                    payload["replay_classification"] = REPLAY_CLASSIFICATION_UNCERTAIN
                elif same_session:
                    payload["duplicate_classification"] = DUPLICATE_EVENT
                    payload["replay_classification"] = REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK
                else:
                    payload["duplicate_classification"] = DUPLICATE_REPLAY_OVERLAP
                    payload["replay_classification"] = REPLAY_CLASSIFICATION_REPLAY_OVERLAP
            fact_rows[payload["schema_id"]].append(payload)
    return fact_rows


def append_fact_ledgers_v1(
    *,
    fact_ledger_paths: Mapping[str, Path],
    fact_rows_by_schema: Mapping[str, Sequence[dict[str, Any]]],
) -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}
    for schema_id, rows in fact_rows_by_schema.items():
        schema_relpath = FACT_SCHEMA_RELPATHS[schema_id]
        written, skipped = _append_validated_jsonl_records(
            path=fact_ledger_paths[schema_id],
            records=list(rows),
            schema_relpath=schema_relpath,
            id_field="fact_record_id",
        )
        results[schema_id] = {"written": written, "skipped_existing": skipped}
    return results


def _sequence_status(raw_rows: Sequence[Mapping[str, Any]]) -> tuple[str, list[str]]:
    expected = 1
    uncertain = False
    for row in sorted(raw_rows, key=lambda row: int(row.get("journal_sequence_number") or 0)):
        try:
            actual = int(row.get("journal_sequence_number") or 0)
        except Exception:
            uncertain = True
            break
        if actual != expected:
            uncertain = True
            break
        expected += 1
    return (SEQ_UNCERTAIN, [RC_SEQUENCE_UNCERTAINTY]) if uncertain else (SEQ_OK, [])


def _freshness_status(*, last_ingested_utc: str, evaluation_utc: str) -> tuple[str, int]:
    last_dt = _parse_utc_text(last_ingested_utc)
    eval_dt = _parse_utc_text(evaluation_utc)
    if last_dt is None or eval_dt is None:
        return FRESHNESS_UNAVAILABLE, 0
    age_seconds = max(0, int((eval_dt - last_dt).total_seconds()))
    return (FRESHNESS_FRESH if age_seconds <= 900 else FRESHNESS_STALE, age_seconds)


def _artifact_ref(path: Path) -> dict[str, str]:
    return {
        "artifact_path": str(path.resolve()),
        "artifact_sha256": sha256_file_v1(path) if path.exists() else "",
    }


def _aggregate_attribution_state(
    raw_rows: Sequence[Mapping[str, Any]],
) -> tuple[str, dict[str, int]]:
    counts = {
        RAW_ATTRIBUTION_ATTRIBUTED: sum(
            1
            for row in raw_rows
            if str(row.get("attribution_status") or "").strip()
            == RAW_ATTRIBUTION_ATTRIBUTED
        ),
        RAW_ATTRIBUTION_PARTIAL: sum(
            1
            for row in raw_rows
            if str(row.get("attribution_status") or "").strip()
            == RAW_ATTRIBUTION_PARTIAL
        ),
        RAW_ATTRIBUTION_AMBIGUOUS: sum(
            1
            for row in raw_rows
            if str(row.get("attribution_status") or "").strip()
            == RAW_ATTRIBUTION_AMBIGUOUS
        ),
        RAW_ATTRIBUTION_FOREIGN: sum(
            1
            for row in raw_rows
            if str(row.get("attribution_status") or "").strip()
            == RAW_ATTRIBUTION_FOREIGN
        ),
        RAW_ATTRIBUTION_UNRESOLVED: sum(
            1
            for row in raw_rows
            if str(row.get("attribution_status") or "").strip()
            == RAW_ATTRIBUTION_UNRESOLVED
        ),
    }
    if counts[RAW_ATTRIBUTION_UNRESOLVED]:
        return RAW_ATTRIBUTION_UNRESOLVED, counts
    if counts[RAW_ATTRIBUTION_FOREIGN]:
        return RAW_ATTRIBUTION_FOREIGN, counts
    if counts[RAW_ATTRIBUTION_AMBIGUOUS]:
        return RAW_ATTRIBUTION_AMBIGUOUS, counts
    if counts[RAW_ATTRIBUTION_PARTIAL]:
        return RAW_ATTRIBUTION_PARTIAL, counts
    if counts[RAW_ATTRIBUTION_ATTRIBUTED]:
        return RAW_ATTRIBUTION_ATTRIBUTED, counts
    return RAW_ATTRIBUTION_UNRESOLVED, counts


def _derive_reconnect_status(session_rows: Sequence[Mapping[str, Any]]) -> str:
    reconnect_indices = [
        index
        for index, row in enumerate(session_rows)
        if str(row.get("reconnect_state_hint") or "").strip() == RECONNECT_IN_PROGRESS
        or str(row.get("session_state_hint") or "").strip() in {"RECONNECTING", "DISCONNECTED"}
    ]
    if not reconnect_indices:
        return RECONNECT_NONE
    last_reconnect_index = reconnect_indices[-1]
    recovered = any(
        index > last_reconnect_index and str(row.get("session_state_hint") or "").strip() == "CONNECTED"
        for index, row in enumerate(session_rows)
    )
    return RECONNECT_RECOVERED if recovered else RECONNECT_IN_PROGRESS


def _derive_replay_status(
    session_rows: Sequence[Mapping[str, Any]],
    fact_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> str:
    replay_requests = sum(
        1
        for row in session_rows
        if str(row.get("session_event_type") or "").strip() in REPLAY_REQUEST_EVENTS
    )
    replay_completes = sum(
        1
        for row in session_rows
        if str(row.get("session_event_type") or "").strip() in REPLAY_END_EVENTS
    )
    replay_overlap_count = sum(
        1
        for rows in fact_rows.values()
        for row in rows
        if str(row.get("duplicate_classification") or "").strip() == DUPLICATE_REPLAY_OVERLAP
    )
    conflict_count = sum(
        1
        for rows in fact_rows.values()
        for row in rows
        if str(row.get("duplicate_classification") or "").strip() == DUPLICATE_CONFLICT
    )
    if conflict_count:
        return REPLAY_UNCERTAIN
    if replay_overlap_count:
        return REPLAY_UNCERTAIN
    if replay_requests > replay_completes:
        return REPLAYING
    if replay_requests > 0 and replay_completes >= replay_requests:
        return REPLAY_COMPLETE
    return REPLAY_NOT_OBSERVED


def _derive_gap_status(
    *,
    session_rows: Sequence[Mapping[str, Any]],
    replay_status: str,
    reconnect_status: str,
) -> str:
    replay_requests = sum(
        1
        for row in session_rows
        if str(row.get("session_event_type") or "").strip() in REPLAY_REQUEST_EVENTS
    )
    replay_completes = sum(
        1
        for row in session_rows
        if str(row.get("session_event_type") or "").strip() in REPLAY_END_EVENTS
    )
    if replay_status == REPLAY_UNCERTAIN or (
        reconnect_status == RECONNECT_IN_PROGRESS and replay_requests > replay_completes
    ):
        return GAP_UNRESOLVED
    if replay_requests > replay_completes:
        return GAP_SUSPECTED
    return GAP_NONE


def build_broker_observation_health_payload_v1(
    *,
    execution_root_path: Path,
    day_utc: str,
    evaluation_utc: str,
    raw_journal_path: Path,
    fact_ledger_paths: Mapping[str, Path],
) -> dict[str, Any]:
    day = parse_day_utc_v1(day_utc)
    raw_rows = _read_jsonl_objects(raw_journal_path)
    fact_rows = {
        schema_id: _read_jsonl_objects(path)
        for schema_id, path in fact_ledger_paths.items()
    }
    session_rows = fact_rows["observation_session_fact"]
    last_ingested_utc = max(
        (
            str(row.get("ingested_utc") or "").strip()
            for row in raw_rows
            if str(row.get("ingested_utc") or "").strip()
        ),
        default="",
    )
    freshness_status, freshness_age_seconds = _freshness_status(
        last_ingested_utc=last_ingested_utc,
        evaluation_utc=evaluation_utc,
    )
    sequence_status, sequence_codes = _sequence_status(raw_rows)
    replay_status = _derive_replay_status(session_rows, fact_rows)
    reconnect_status = _derive_reconnect_status(session_rows)
    gap_status = _derive_gap_status(
        session_rows=session_rows,
        replay_status=replay_status,
        reconnect_status=reconnect_status,
    )

    has_connected = any(
        str(row.get("session_state_hint") or "").strip() == "CONNECTED"
        for row in session_rows
    )
    last_session_state = (
        str(session_rows[-1].get("session_state_hint") or "").strip()
        if session_rows
        else ""
    )
    if not raw_rows or not has_connected or last_session_state in {"DISCONNECTED", "STOPPED"}:
        session_status = SESSION_UNAVAILABLE
    elif reconnect_status == RECONNECT_IN_PROGRESS:
        session_status = SESSION_RECONNECTING
    else:
        session_status = SESSION_HEALTHY

    attribution_status, attribution_counts = _aggregate_attribution_state(raw_rows)
    quality_reason_codes = [
        code
        for row in raw_rows
        for code in (row.get("quality_reason_codes") or [])
        if str(code).strip()
    ] + [
        code
        for rows in fact_rows.values()
        for row in rows
        for code in (row.get("quality_reason_codes") or [])
        if str(code).strip()
    ]

    duplicate_counts = {
        DUPLICATE_EVENT: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("duplicate_classification") or "").strip() == DUPLICATE_EVENT
        ),
        DUPLICATE_REPLAY_OVERLAP: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("duplicate_classification") or "").strip()
            == DUPLICATE_REPLAY_OVERLAP
        ),
        DUPLICATE_CONFLICT: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("duplicate_classification") or "").strip() == DUPLICATE_CONFLICT
        ),
    }
    replay_classification_counts = {
        REPLAY_CLASSIFICATION_REPLAY_SESSION: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("replay_classification") or "").strip()
            == REPLAY_CLASSIFICATION_REPLAY_SESSION
        ),
        REPLAY_CLASSIFICATION_REPLAY_OVERLAP: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("replay_classification") or "").strip()
            == REPLAY_CLASSIFICATION_REPLAY_OVERLAP
        ),
        REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("replay_classification") or "").strip()
            == REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK
        ),
        REPLAY_CLASSIFICATION_UNCERTAIN: sum(
            1
            for rows in fact_rows.values()
            for row in rows
            if str(row.get("replay_classification") or "").strip()
            == REPLAY_CLASSIFICATION_UNCERTAIN
        ),
    }

    blocker_codes: list[str] = []
    degraded_codes: list[str] = []

    if freshness_status in {FRESHNESS_STALE, FRESHNESS_UNAVAILABLE}:
        blocker_codes.append(RC_STREAM_STALE)
    if session_status == SESSION_UNAVAILABLE:
        blocker_codes.append(RC_OBSERVER_SESSION_UNAVAILABLE)
    if reconnect_status == RECONNECT_IN_PROGRESS:
        blocker_codes.append(RC_RECONNECT_IN_PROGRESS)
    elif reconnect_status == RECONNECT_RECOVERED:
        degraded_codes.append(RC_RECONNECT_RECOVERED)
    if replay_status == REPLAYING:
        blocker_codes.append(RC_REPLAY_IN_PROGRESS)
    elif replay_status == REPLAY_COMPLETE and reconnect_status != RECONNECT_NONE:
        degraded_codes.append(RC_REPLAY_COMPLETE)
    elif replay_status == REPLAY_UNCERTAIN:
        blocker_codes.append(RC_REPLAY_UNCERTAIN)
    if gap_status == GAP_SUSPECTED:
        degraded_codes.append(RC_SNAPSHOT_GAP_SUSPECTED)
    elif gap_status == GAP_UNRESOLVED:
        blocker_codes.append(RC_GAP_UNRESOLVED)

    blocker_codes.extend(sequence_codes)
    if attribution_status == RAW_ATTRIBUTION_PARTIAL:
        degraded_codes.append(RC_ATTRIBUTION_PARTIAL)
    elif attribution_status == RAW_ATTRIBUTION_AMBIGUOUS:
        blocker_codes.append(RC_SLEEVE_ATTRIBUTION_AMBIGUOUS)
    elif attribution_status == RAW_ATTRIBUTION_FOREIGN:
        blocker_codes.append(RC_FOREIGN_MANUAL_SUSPECTED)
    elif attribution_status == RAW_ATTRIBUTION_UNRESOLVED:
        blocker_codes.append(RC_ATTRIBUTION_UNRESOLVED)

    for code in quality_reason_codes:
        if code in {
            RC_MALFORMED_PAYLOAD,
            RC_MISSING_ORDER_IDENTIFIER,
            RC_MISSING_EXECUTION_IDENTIFIER,
            RC_MISSING_CONTRACT_IDENTITY,
            RC_CANONICAL_EVENT_IDENTITY_UNRESOLVED,
            RC_ORDERING_BASIS_UNRESOLVED,
            RC_ENVIRONMENT_MISSING,
            RC_ACCOUNT_MISSING,
            RC_CLIENT_ID_OBSERVER_MISSING,
        }:
            blocker_codes.append(code)
        elif code == RC_UNSUPPORTED_EVENT_TYPE:
            degraded_codes.append(code)

    if duplicate_counts[DUPLICATE_EVENT]:
        degraded_codes.append(RC_DUPLICATE_EVENT_DETECTED)
    if duplicate_counts[DUPLICATE_REPLAY_OVERLAP]:
        degraded_codes.append(RC_REPLAY_OVERLAP_DETECTED)
    if duplicate_counts[DUPLICATE_CONFLICT]:
        blocker_codes.append(RC_DUPLICATE_CLASSIFICATION_CONFLICT)
    if replay_classification_counts[REPLAY_CLASSIFICATION_UNCERTAIN]:
        blocker_codes.append(RC_REPLAY_OVERLAP_UNRESOLVED)

    blocker_codes = list(dict.fromkeys(code for code in blocker_codes if str(code).strip()))
    degraded_codes = [
        code
        for code in dict.fromkeys(code for code in degraded_codes if str(code).strip())
        if code not in blocker_codes
    ]

    if blocker_codes:
        trust_verdict = TRUST_BLOCKED
        downstream_posture = DOWNSTREAM_FAIL_CLOSED
        may_consume_normally = False
        may_consume_with_degraded_posture = False
        must_fail_closed = True
        blocker_codes.append(RC_DOWNSTREAM_BLOCKED)
    elif degraded_codes:
        trust_verdict = TRUST_DEGRADED
        downstream_posture = DOWNSTREAM_DEGRADED_ONLY
        may_consume_normally = False
        may_consume_with_degraded_posture = True
        must_fail_closed = False
        degraded_codes.append(RC_DOWNSTREAM_DEGRADED_ONLY)
    else:
        trust_verdict = TRUST_TRUSTED
        downstream_posture = DOWNSTREAM_NORMAL
        may_consume_normally = True
        may_consume_with_degraded_posture = False
        must_fail_closed = False

    blocker_codes = list(dict.fromkeys(blocker_codes))
    degraded_codes = [
        code for code in dict.fromkeys(degraded_codes) if code not in blocker_codes
    ]

    if trust_verdict == TRUST_TRUSTED:
        recommended_operator_action = "No operator action required."
    elif trust_verdict == TRUST_DEGRADED:
        recommended_operator_action = (
            "Consume Core 1 only under explicit degraded-posture allowances until replay/reconnect or attribution degradation clears."
        )
    else:
        recommended_operator_action = (
            "Fail closed for downstream consumption until Core 1 blockers are resolved."
        )

    return {
        "schema_id": "broker_observation_health",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "day_utc": day,
        "evaluation_utc": evaluation_utc,
        "authority_owner": BROKER_OBSERVATION_HEALTH_OWNER,
        "trust_rule_owner": TRUST_RULE_OWNER,
        "execution_root_path": str(Path(execution_root_path).resolve()),
        "raw_journal_ref": _artifact_ref(raw_journal_path),
        "fact_ledger_refs": {
            schema_id: _artifact_ref(path)
            for schema_id, path in fact_ledger_paths.items()
        },
        "current_state": trust_verdict,
        "downstream_trust_verdict": trust_verdict,
        "downstream_consumption_posture": downstream_posture,
        "may_consume_normally": may_consume_normally,
        "may_consume_with_degraded_posture": may_consume_with_degraded_posture,
        "must_fail_closed": must_fail_closed,
        "freshness_status": freshness_status,
        "freshness_age_seconds": freshness_age_seconds,
        "session_status": session_status,
        "sequence_status": sequence_status,
        "replay_status": replay_status,
        "reconnect_status": reconnect_status,
        "gap_status": gap_status,
        "attribution_status": attribution_status,
        "event_identity_rule_version": EVENT_IDENTITY_RULE_VERSION,
        "blocker_codes": blocker_codes,
        "degraded_codes": degraded_codes,
        "counts": {
            "raw_record_count": len(raw_rows),
            "observation_session_fact_count": len(fact_rows["observation_session_fact"]),
            "observed_order_fact_count": len(fact_rows["observed_order_fact"]),
            "observed_order_status_fact_count": len(fact_rows["observed_order_status_fact"]),
            "observed_fill_fact_count": len(fact_rows["observed_fill_fact"]),
            "observed_position_fact_count": len(fact_rows["observed_position_fact"]),
            "attributed_raw_record_count": attribution_counts[RAW_ATTRIBUTION_ATTRIBUTED],
            "partial_raw_record_count": attribution_counts[RAW_ATTRIBUTION_PARTIAL],
            "ambiguous_raw_record_count": attribution_counts[RAW_ATTRIBUTION_AMBIGUOUS],
            "foreign_raw_record_count": attribution_counts[RAW_ATTRIBUTION_FOREIGN],
            "unresolved_raw_record_count": attribution_counts[RAW_ATTRIBUTION_UNRESOLVED],
            "duplicate_fact_count": duplicate_counts[DUPLICATE_EVENT],
            "replay_overlap_fact_count": duplicate_counts[DUPLICATE_REPLAY_OVERLAP],
            "conflicting_duplicate_fact_count": duplicate_counts[DUPLICATE_CONFLICT],
            "replay_session_fact_count": replay_classification_counts[REPLAY_CLASSIFICATION_REPLAY_SESSION],
            "replay_uncertain_fact_count": replay_classification_counts[REPLAY_CLASSIFICATION_UNCERTAIN],
        },
        "summary": (
            f"trust={trust_verdict} posture={downstream_posture} freshness={freshness_status} "
            f"replay={replay_status} reconnect={reconnect_status} gap={gap_status} attribution={attribution_status}"
        ),
        "recommended_operator_action": recommended_operator_action,
    }


def build_broker_observation_trust_dependency_payload_v1(
    *,
    day_utc: str,
    health_payload: Mapping[str, Any],
    health_path: Path,
) -> dict[str, Any]:
    return {
        "schema_id": "broker_observation_trust_dependency",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "day_utc": parse_day_utc_v1(day_utc),
        "authority_owner": BROKER_OBSERVATION_TRUST_DEPENDENCY_OWNER,
        "health_ref": _artifact_ref(health_path),
        "trust_verdict": str(health_payload.get("downstream_trust_verdict") or "").strip(),
        "downstream_consumption_posture": str(
            health_payload.get("downstream_consumption_posture") or ""
        ).strip(),
        "may_consume_normally": bool(health_payload.get("may_consume_normally")),
        "may_consume_with_degraded_posture": bool(
            health_payload.get("may_consume_with_degraded_posture")
        ),
        "must_fail_closed": bool(health_payload.get("must_fail_closed")),
        "core2_consumption_mode": (
            "NORMAL_ONLY"
            if bool(health_payload.get("may_consume_normally"))
            else "DEGRADED_ALLOWED"
            if bool(health_payload.get("may_consume_with_degraded_posture"))
            else "FAIL_CLOSED"
        ),
        "replay_status": str(health_payload.get("replay_status") or "").strip(),
        "reconnect_status": str(health_payload.get("reconnect_status") or "").strip(),
        "gap_status": str(health_payload.get("gap_status") or "").strip(),
        "attribution_status": str(health_payload.get("attribution_status") or "").strip(),
        "core2_allowed_artifact_families": [
            "observation_session_fact.v1",
            "observed_order_fact.v1",
            "observed_order_status_fact.v1",
            "observed_fill_fact.v1",
            "observed_position_fact.v1",
            "broker_observation_health.v1",
            "broker_observation_trust_dependency.v1",
        ],
        "core2_forbidden_artifact_families": [
            "broker_raw_evidence_envelope.v1",
        ],
        "blocker_codes": list(health_payload.get("blocker_codes") or []),
        "degraded_codes": list(health_payload.get("degraded_codes") or []),
        "required_operator_action": str(
            health_payload.get("recommended_operator_action") or ""
        ).strip(),
        "summary": str(health_payload.get("summary") or "").strip(),
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = _json_loads_optional(path.read_text(encoding="utf-8")) if path.exists() else None
    return payload if payload is not None else {}


def _fact_signature_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    return sorted(
        [
            {
                "fact_record_id": str(row.get("fact_record_id") or "").strip(),
                "canonical_event_identity": str(row.get("canonical_event_identity") or "").strip(),
                "duplicate_classification": str(row.get("duplicate_classification") or "").strip(),
                "replay_classification": str(row.get("replay_classification") or "").strip(),
                "source_event_identity": str(row.get("source_event_identity") or "").strip(),
                "ordering_key": str(row.get("ordering_key") or "").strip(),
            }
            for row in rows
        ],
        key=lambda row: (
            row["fact_record_id"],
            row["canonical_event_identity"],
            row["ordering_key"],
            row["source_event_identity"],
        ),
    )


def _fact_signature_map(
    fact_rows_by_schema: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, list[dict[str, str]]]:
    return {
        schema_id: _fact_signature_rows(rows)
        for schema_id, rows in fact_rows_by_schema.items()
    }


def _validate_hardened_raw_rows(
    raw_rows: Sequence[Mapping[str, Any]],
) -> tuple[str, list[str], list[str]]:
    if not raw_rows:
        return LEGACY_BOUNDARY_MISSING, [RC_CORE1_RAW_JOURNAL_MISSING], []
    invalid_count = 0
    invalid_details: list[str] = []
    for index, row in enumerate(raw_rows, start=1):
        try:
            validate_against_repo_schema_v1(dict(row), REPO_ROOT, RAW_ENVELOPE_SCHEMA_RELPATH)
        except Exception as exc:
            invalid_count += 1
            invalid_details.append(f"row={index}:{exc}")
    if invalid_count == 0:
        return LEGACY_BOUNDARY_HARDENED, [], []
    if invalid_count == len(raw_rows):
        return LEGACY_BOUNDARY_LEGACY, [RC_CORE1_LEGACY_RUNTIME_DAY_NON_CONSUMABLE], invalid_details
    return LEGACY_BOUNDARY_MIXED, [RC_CORE1_MIXED_RUNTIME_DAY_BLOCKED], invalid_details


def _verify_identity_stability(
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    fact_rows_by_schema: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[str, list[str], dict[str, str]]:
    if not raw_rows:
        return "UNSTABLE", [RC_CORE1_IDENTITY_STABILITY_UNPROVEN], {
            "reason": "raw_journal_missing",
        }
    normalized_once = normalize_broker_fact_records_v1(raw_rows=raw_rows)
    normalized_reversed = normalize_broker_fact_records_v1(raw_rows=list(reversed(list(raw_rows))))
    materialized_signature = _fact_signature_map(fact_rows_by_schema)
    once_signature = _fact_signature_map(normalized_once)
    reversed_signature = _fact_signature_map(normalized_reversed)
    stable = once_signature == reversed_signature == materialized_signature
    return (
        "STABLE" if stable else "UNSTABLE",
        [] if stable else [RC_CORE1_IDENTITY_STABILITY_UNPROVEN],
        {
            "input_signature_sha256": _sha256_text(
                canonical_json_bytes_v1(once_signature).decode("utf-8")
            ),
            "reverse_signature_sha256": _sha256_text(
                canonical_json_bytes_v1(reversed_signature).decode("utf-8")
            ),
            "materialized_signature_sha256": _sha256_text(
                canonical_json_bytes_v1(materialized_signature).decode("utf-8")
            ),
        },
    )


def build_core1_pre_core2_readiness_payload_v1(
    *,
    execution_root_path: Path,
    day_utc: str,
    raw_journal_path: Path,
    fact_ledger_paths: Mapping[str, Path],
    health_path: Path,
    trust_dependency_path: Path,
) -> dict[str, Any]:
    day = parse_day_utc_v1(day_utc)
    raw_rows = _read_jsonl_objects(raw_journal_path)
    fact_rows_by_schema = {
        schema_id: _read_jsonl_objects(path)
        for schema_id, path in fact_ledger_paths.items()
    }
    trust_dependency_payload = _load_json_object(trust_dependency_path)

    required_artifacts = {
        "raw_journal": raw_journal_path,
        "health": health_path,
        "trust_dependency": trust_dependency_path,
        **{schema_id: path for schema_id, path in fact_ledger_paths.items()},
    }
    missing_artifacts = [
        f"{artifact_id}:{path}"
        for artifact_id, path in required_artifacts.items()
        if not path.exists()
    ]

    legacy_boundary_status, legacy_blockers, legacy_details = _validate_hardened_raw_rows(raw_rows)
    identity_stability_status, identity_blockers, identity_details = _verify_identity_stability(
        raw_rows=raw_rows,
        fact_rows_by_schema=fact_rows_by_schema,
    )

    blocker_codes: list[str] = []
    degraded_codes: list[str] = []
    if missing_artifacts:
        blocker_codes.append(RC_CORE2_REQUIRED_ARTIFACT_MISSING)
    blocker_codes.extend(legacy_blockers)
    blocker_codes.extend(identity_blockers)
    blocker_codes.extend(list(trust_dependency_payload.get("blocker_codes") or []))
    degraded_codes.extend(list(trust_dependency_payload.get("degraded_codes") or []))
    blocker_codes = list(dict.fromkeys(code for code in blocker_codes if str(code).strip()))
    degraded_codes = [
        code
        for code in dict.fromkeys(code for code in degraded_codes if str(code).strip())
        if code not in blocker_codes
    ]

    trust_verdict = str(trust_dependency_payload.get("trust_verdict") or "").strip()
    downstream_posture = str(
        trust_dependency_payload.get("downstream_consumption_posture") or ""
    ).strip()
    if blocker_codes or bool(trust_dependency_payload.get("must_fail_closed")):
        current_state = READINESS_BLOCKED
        downstream_posture = DOWNSTREAM_FAIL_CLOSED
        safe_for_core2_consumption = False
        first_blocker_code = blocker_codes[0] if blocker_codes else RC_DOWNSTREAM_BLOCKED
        first_blocker_summary = (
            "Core 1 remains fail-closed for Core 2 consumption."
            if first_blocker_code == RC_DOWNSTREAM_BLOCKED
            else f"Core 1 pre-Core-2 dependency blocker: {first_blocker_code}"
        )
    elif bool(trust_dependency_payload.get("may_consume_with_degraded_posture")):
        current_state = READINESS_DEGRADED_ONLY
        downstream_posture = DOWNSTREAM_DEGRADED_ONLY
        safe_for_core2_consumption = True
        first_blocker_code = ""
        first_blocker_summary = ""
    else:
        current_state = READINESS_READY
        downstream_posture = DOWNSTREAM_NORMAL
        safe_for_core2_consumption = True
        first_blocker_code = ""
        first_blocker_summary = ""

    allowed_input_refs = {
        "broker_observation_health_v1": _artifact_ref(health_path),
        "broker_observation_trust_dependency_v1": _artifact_ref(trust_dependency_path),
        **{
            f"{schema_id}.v1": _artifact_ref(path)
            for schema_id, path in fact_ledger_paths.items()
        },
    }

    return {
        "schema_id": "core1_pre_core2_readiness",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "day_utc": day,
        "authority_owner": CORE2_CORE1_DEPENDENCY_GATE_OWNER,
        "execution_root_path": str(Path(execution_root_path).resolve()),
        "current_state": current_state,
        "safe_for_core2_consumption": safe_for_core2_consumption,
        "core2_allowed_input_refs": allowed_input_refs,
        "core2_forbidden_direct_inputs": [str(raw_journal_path.resolve())],
        "compatibility_mode": CORE2_COMPATIBILITY_HARD_CUT,
        "legacy_boundary_status": legacy_boundary_status,
        "identity_stability_status": identity_stability_status,
        "identity_stability_details": identity_details,
        "trust_verdict": trust_verdict or TRUST_BLOCKED,
        "downstream_consumption_posture": downstream_posture,
        "replay_status": str(trust_dependency_payload.get("replay_status") or "").strip(),
        "reconnect_status": str(trust_dependency_payload.get("reconnect_status") or "").strip(),
        "gap_status": str(trust_dependency_payload.get("gap_status") or "").strip(),
        "attribution_status": str(trust_dependency_payload.get("attribution_status") or "").strip(),
        "first_blocker": {
            "reason_code": first_blocker_code,
            "summary": first_blocker_summary,
        },
        "blocker_codes": blocker_codes,
        "degraded_codes": degraded_codes,
        "missing_artifacts": missing_artifacts,
        "legacy_boundary_details": legacy_details,
        "summary": (
            f"core2_posture={downstream_posture or DOWNSTREAM_FAIL_CLOSED} "
            f"legacy_boundary={legacy_boundary_status} identity_stability={identity_stability_status} "
            f"trust={trust_verdict or TRUST_BLOCKED}"
        ),
    }


def materialize_core1_pre_core2_readiness_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
    sleeve_id: str = "PRIMARY",
) -> Core1PreCore2ReadinessV1:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    governed_identity = resolve_governed_execution_identity_v1(
        repo_root=repo_root,
        environment=environment,
        sleeve_id=sleeve_id,
    )
    governed_roots = resolve_governed_paper_execution_roots(
        repo_root=repo_root,
        environment=environment,
        ib_account=governed_identity.account_id,
        sleeve_id=governed_identity.sleeve_id,
    )
    execution_root_path = Path(governed_roots.execution_root_path).resolve()
    day = parse_day_utc_v1(day_utc)
    raw_journal_path = resolve_broker_raw_journal_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    fact_ledger_paths = {
        schema_id: resolve_fact_ledger_path(
            execution_root_path=execution_root_path,
            day_utc=day,
            schema_id=schema_id,
        )
        for schema_id in FACT_SCHEMA_RELPATHS
    }
    health_path = resolve_broker_observation_health_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    trust_dependency_path = resolve_broker_observation_trust_dependency_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    payload = build_core1_pre_core2_readiness_payload_v1(
        execution_root_path=execution_root_path,
        day_utc=day,
        raw_journal_path=raw_journal_path,
        fact_ledger_paths=fact_ledger_paths,
        health_path=health_path,
        trust_dependency_path=trust_dependency_path,
    )
    report_path = resolve_core1_pre_core2_readiness_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    atomic_write_validated_json_v1(
        path=report_path,
        payload=payload,
        schema_relpath=CORE1_PRE_CORE2_READINESS_SCHEMA_RELPATH,
    )
    return Core1PreCore2ReadinessV1(report_path=report_path, payload=payload)


def build_broker_fact_spine_audit_payload_v1(
    *,
    execution_root_path: Path,
    day_utc: str,
    raw_journal_path: Path,
    fact_ledger_paths: Mapping[str, Path],
    health_payload: Mapping[str, Any],
    health_path: Path,
    trust_dependency_path: Path,
    source_path: Path,
) -> dict[str, Any]:
    raw_rows = _read_jsonl_objects(raw_journal_path)
    latest_observed_utc = max(
        (
            str(row.get("observed_utc") or "").strip()
            for row in raw_rows
            if str(row.get("observed_utc") or "").strip()
        ),
        default="",
    )
    return {
        "schema_id": "broker_fact_spine_audit",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "day_utc": parse_day_utc_v1(day_utc),
        "authority_owner": BROKER_FACT_SPINE_OWNER,
        "execution_root_path": str(Path(execution_root_path).resolve()),
        "source_input_ref": _artifact_ref(source_path),
        "raw_journal_ref": _artifact_ref(raw_journal_path),
        "fact_ledger_refs": {
            schema_id: _artifact_ref(path)
            for schema_id, path in fact_ledger_paths.items()
        },
        "health_ref": _artifact_ref(health_path),
        "trust_dependency_ref": _artifact_ref(trust_dependency_path),
        "current_state": str(health_payload.get("current_state") or "").strip(),
        "downstream_consumption_posture": str(
            health_payload.get("downstream_consumption_posture") or ""
        ).strip(),
        "latest_observed_utc": latest_observed_utc,
        "summary": str(health_payload.get("summary") or "").strip(),
        "counts": dict(health_payload.get("counts") or {}),
        "derived_only": True,
    }


def materialize_broker_fact_spine_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
    sleeve_id: str = "PRIMARY",
    evaluation_utc: str = "",
    source_path: Path | None = None,
) -> BrokerFactSpineMaterializationV1:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    governed_identity = resolve_governed_execution_identity_v1(
        repo_root=repo_root,
        environment=environment,
        sleeve_id=sleeve_id,
    )
    governed_roots = resolve_governed_paper_execution_roots(
        repo_root=repo_root,
        environment=environment,
        ib_account=governed_identity.account_id,
        sleeve_id=governed_identity.sleeve_id,
    )
    execution_root_path = Path(governed_roots.execution_root_path).resolve()
    resolved_source_path = (
        Path(source_path).resolve()
        if source_path is not None
        else resolve_legacy_broker_event_log_path(truth_root=truth_root, day_utc=day)
    )
    raw_journal_path = resolve_broker_raw_journal_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    fact_ledger_paths = {
        schema_id: resolve_fact_ledger_path(
            execution_root_path=execution_root_path,
            day_utc=day,
            schema_id=schema_id,
        )
        for schema_id in FACT_SCHEMA_RELPATHS
    }
    raw_envelopes = build_raw_evidence_envelopes_from_source_v1(
        repo_root=repo_root,
        day_utc=day,
        environment=environment,
        sleeve_id=sleeve_id,
        source_path=resolved_source_path,
    )
    raw_written, raw_skipped = append_raw_evidence_envelopes_v1(
        raw_journal_path=raw_journal_path,
        raw_envelopes=raw_envelopes,
    )
    normalized_rows = normalize_broker_fact_records_v1(
        raw_rows=_read_jsonl_objects(raw_journal_path),
    )
    fact_write_results = append_fact_ledgers_v1(
        fact_ledger_paths=fact_ledger_paths,
        fact_rows_by_schema=normalized_rows,
    )
    effective_evaluation_utc = _coerce_utc_text(
        evaluation_utc,
        fallback=max(
            (
                str(row.get("observed_utc") or "").strip()
                for row in _read_jsonl_objects(raw_journal_path)
                if str(row.get("observed_utc") or "").strip()
            ),
            default=_utc_now(),
        ),
    )
    health_payload = build_broker_observation_health_payload_v1(
        execution_root_path=execution_root_path,
        day_utc=day,
        evaluation_utc=effective_evaluation_utc,
        raw_journal_path=raw_journal_path,
        fact_ledger_paths=fact_ledger_paths,
    )
    health_path = resolve_broker_observation_health_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    health_ref = atomic_write_validated_json_v1(
        path=health_path,
        payload=health_payload,
        schema_relpath=BROKER_OBSERVATION_HEALTH_SCHEMA_RELPATH,
    )
    trust_dependency_payload = build_broker_observation_trust_dependency_payload_v1(
        day_utc=day,
        health_payload=health_payload,
        health_path=health_path,
    )
    trust_dependency_payload["health_ref"]["artifact_sha256"] = health_ref.sha256
    trust_dependency_path = resolve_broker_observation_trust_dependency_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    trust_dependency_ref = atomic_write_validated_json_v1(
        path=trust_dependency_path,
        payload=trust_dependency_payload,
        schema_relpath=BROKER_OBSERVATION_TRUST_DEPENDENCY_SCHEMA_RELPATH,
    )
    audit_payload = build_broker_fact_spine_audit_payload_v1(
        execution_root_path=execution_root_path,
        day_utc=day,
        raw_journal_path=raw_journal_path,
        fact_ledger_paths=fact_ledger_paths,
        health_payload=health_payload,
        health_path=health_path,
        trust_dependency_path=trust_dependency_path,
        source_path=resolved_source_path,
    )
    audit_payload["health_ref"]["artifact_sha256"] = health_ref.sha256
    audit_payload["trust_dependency_ref"]["artifact_sha256"] = trust_dependency_ref.sha256
    audit_path = resolve_broker_fact_spine_audit_path(
        execution_root_path=execution_root_path,
        day_utc=day,
    )
    audit_ref = atomic_write_validated_json_v1(
        path=audit_path,
        payload=audit_payload,
        schema_relpath=BROKER_FACT_SPINE_AUDIT_SCHEMA_RELPATH,
    )
    summary = {
        "day_utc": day,
        "source_path": str(resolved_source_path),
        "execution_root_path": str(execution_root_path),
        "raw_journal_path": str(raw_journal_path),
        "fact_ledger_paths": {schema_id: str(path) for schema_id, path in fact_ledger_paths.items()},
        "health_path": str(health_path),
        "trust_dependency_path": str(trust_dependency_path),
        "audit_path": str(audit_path),
        "raw_records_written": raw_written,
        "raw_records_skipped_existing": raw_skipped,
        "fact_write_results": fact_write_results,
        "trust_verdict": str(health_payload.get("downstream_trust_verdict") or "").strip(),
        "downstream_consumption_posture": str(
            health_payload.get("downstream_consumption_posture") or ""
        ).strip(),
        "replay_status": str(health_payload.get("replay_status") or "").strip(),
        "reconnect_status": str(health_payload.get("reconnect_status") or "").strip(),
        "gap_status": str(health_payload.get("gap_status") or "").strip(),
        "attribution_status": str(health_payload.get("attribution_status") or "").strip(),
        "blocker_codes": list(health_payload.get("blocker_codes") or []),
        "degraded_codes": list(health_payload.get("degraded_codes") or []),
        "audit_sha256": audit_ref.sha256,
    }
    return BrokerFactSpineMaterializationV1(
        raw_journal_path=raw_journal_path,
        fact_ledger_paths=fact_ledger_paths,
        health_path=health_path,
        trust_dependency_path=trust_dependency_path,
        audit_path=audit_path,
        summary=summary,
    )


__all__ = [
    BROKER_FACT_SPINE_OWNER,
    BROKER_OBSERVATION_HEALTH_OWNER,
    BROKER_OBSERVATION_TRUST_DEPENDENCY_OWNER,
    CORE1_PRE_CORE2_READINESS_SCHEMA_RELPATH,
    CORE2_COMPATIBILITY_HARD_CUT,
    CORE2_CORE1_DEPENDENCY_GATE_OWNER,
    BrokerFactSpineMaterializationV1,
    BrokerRawEvidenceJournalWriterV1,
    Core1PreCore2ReadinessV1,
    DOWNSTREAM_DEGRADED_ONLY,
    DOWNSTREAM_FAIL_CLOSED,
    DOWNSTREAM_NORMAL,
    DUPLICATE_CONFLICT,
    DUPLICATE_EVENT,
    DUPLICATE_REPLAY_OVERLAP,
    GAP_NONE,
    GAP_SUSPECTED,
    GAP_UNRESOLVED,
    RAW_ATTRIBUTION_AMBIGUOUS,
    RAW_ATTRIBUTION_ATTRIBUTED,
    RAW_ATTRIBUTION_FOREIGN,
    RAW_ATTRIBUTION_PARTIAL,
    RAW_ATTRIBUTION_UNRESOLVED,
    RC_ACCOUNT_MISSING,
    RC_ATTRIBUTION_PARTIAL,
    RC_ATTRIBUTION_UNRESOLVED,
    RC_CANONICAL_EVENT_IDENTITY_UNRESOLVED,
    RC_CLIENT_ID_OBSERVER_MISSING,
    RC_CORE1_IDENTITY_STABILITY_UNPROVEN,
    RC_CORE1_LEGACY_RUNTIME_DAY_NON_CONSUMABLE,
    RC_CORE1_MIXED_RUNTIME_DAY_BLOCKED,
    RC_CORE1_RAW_JOURNAL_MISSING,
    RC_CORE2_RAW_DIRECT_CONSUMPTION_FORBIDDEN,
    RC_CORE2_REQUIRED_ARTIFACT_MISSING,
    RC_DOWNSTREAM_BLOCKED,
    RC_DOWNSTREAM_DEGRADED_ONLY,
    RC_DUPLICATE_CLASSIFICATION_CONFLICT,
    RC_DUPLICATE_EVENT_DETECTED,
    RC_ENVIRONMENT_MISSING,
    RC_FOREIGN_MANUAL_SUSPECTED,
    RC_GAP_UNRESOLVED,
    RC_MALFORMED_PAYLOAD,
    RC_MISSING_CONTRACT_IDENTITY,
    RC_MISSING_EXECUTION_IDENTIFIER,
    RC_MISSING_ORDER_IDENTIFIER,
    RC_OBSERVER_SESSION_UNAVAILABLE,
    RC_ORDERING_BASIS_UNRESOLVED,
    RC_RECONNECT_IN_PROGRESS,
    RC_RECONNECT_RECOVERED,
    RC_REPLAY_COMPLETE,
    RC_REPLAY_IN_PROGRESS,
    RC_REPLAY_OVERLAP_DETECTED,
    RC_REPLAY_OVERLAP_UNRESOLVED,
    RC_REPLAY_UNCERTAIN,
    RC_SEQUENCE_UNCERTAINTY,
    RC_SLEEVE_ATTRIBUTION_AMBIGUOUS,
    RC_SNAPSHOT_GAP_SUSPECTED,
    RC_STREAM_STALE,
    RC_UNSUPPORTED_EVENT_TYPE,
    READINESS_BLOCKED,
    READINESS_DEGRADED_ONLY,
    READINESS_READY,
    RECONNECT_IN_PROGRESS,
    RECONNECT_NONE,
    RECONNECT_RECOVERED,
    REPLAYING,
    REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK,
    REPLAY_CLASSIFICATION_NOT_REPLAY,
    REPLAY_CLASSIFICATION_REPLAY_OVERLAP,
    REPLAY_CLASSIFICATION_REPLAY_SESSION,
    REPLAY_CLASSIFICATION_UNCERTAIN,
    REPLAY_COMPLETE,
    REPLAY_NOT_OBSERVED,
    REPLAY_UNCERTAIN,
    TRUST_BLOCKED,
    TRUST_DEGRADED,
    TRUST_TRUSTED,
    build_broker_observation_health_payload_v1,
    build_broker_observation_trust_dependency_payload_v1,
    build_core1_pre_core2_readiness_payload_v1,
    build_raw_evidence_envelope_from_payload_v1,
    build_raw_evidence_envelopes_from_source_v1,
    materialize_broker_fact_spine_v1,
    materialize_core1_pre_core2_readiness_v1,
    normalize_broker_fact_records_v1,
    resolve_broker_fact_spine_audit_path,
    resolve_broker_observation_health_path,
    resolve_broker_observation_trust_dependency_path,
    resolve_core1_pre_core2_readiness_path,
    resolve_broker_raw_journal_path,
    resolve_fact_ledger_path,
    resolve_legacy_broker_event_log_path,
]
