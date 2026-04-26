from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any


EVENT_SCHEMA_VERSION = "research_event.v1"
SUPPORTED_EVENT_TYPES = {
    "TRADING_DAY_CLOSED",
    "SANDBOX_RESULT_COMPLETED",
}
SUPPORTED_EVENT_SOURCES = {
    "AEGIS_CORE",
    "RESEARCH_LAB",
    "IB_REPORT",
}
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ResearchEventValidationError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _stable_digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def build_idempotency_key_v1(
    *,
    event_type: str,
    day_utc: str,
    source: str,
    source_artifacts: list[str],
) -> str:
    normalized = {
        "event_type": str(event_type).strip().upper(),
        "day_utc": str(day_utc).strip(),
        "source": str(source).strip().upper(),
        "source_artifacts": sorted([str(path).strip() for path in source_artifacts if str(path).strip()]),
    }
    return _stable_digest(normalized)


def build_event_id_v1(
    *,
    event_type: str,
    day_utc: str,
    source: str,
    idempotency_key: str,
) -> str:
    normalized = {
        "event_type": str(event_type).strip().upper(),
        "day_utc": str(day_utc).strip(),
        "source": str(source).strip().upper(),
        "idempotency_key": str(idempotency_key).strip().lower(),
    }
    return _stable_digest(normalized)


def validate_sandbox_result_v1(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ResearchEventValidationError("SANDBOX_RESULT_TOP_LEVEL_NOT_OBJECT")
    required = {
        "schema_version",
        "test_id",
        "idea_id",
        "status",
        "metrics",
        "in_sample_metrics",
        "out_of_sample_metrics",
        "cost_model_status",
        "slippage_model_status",
        "sample_size",
        "warnings",
        "generated_utc",
    }
    missing = sorted([field for field in required if field not in payload])
    if missing:
        raise ResearchEventValidationError(f"SANDBOX_RESULT_MISSING_FIELDS:{','.join(missing)}")
    if payload.get("schema_version") != "sandbox_result.v1":
        raise ResearchEventValidationError("SANDBOX_RESULT_SCHEMA_VERSION_MISMATCH")
    if str(payload.get("status") or "").strip().upper() not in {"PASS", "FAIL", "INCONCLUSIVE"}:
        raise ResearchEventValidationError("SANDBOX_RESULT_STATUS_INVALID")
    sample_size = payload.get("sample_size")
    if not isinstance(sample_size, int) or sample_size < 0:
        raise ResearchEventValidationError("SANDBOX_RESULT_SAMPLE_SIZE_INVALID")
    for field in ("metrics", "in_sample_metrics", "out_of_sample_metrics"):
        if not isinstance(payload.get(field), dict):
            raise ResearchEventValidationError(f"SANDBOX_RESULT_FIELD_NOT_OBJECT:{field}")
    if not isinstance(payload.get("warnings"), list):
        raise ResearchEventValidationError("SANDBOX_RESULT_WARNINGS_NOT_LIST")
    return dict(payload)


def validate_ai_result_review_v1(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ResearchEventValidationError("AI_REVIEW_TOP_LEVEL_NOT_OBJECT")
    required = {
        "schema_version",
        "idea_id",
        "test_id",
        "review_status",
        "action_required_for_human",
        "primary_reason",
        "findings",
        "overfitting_risk",
        "data_quality_risk",
        "recommended_next_action",
        "codex_task_recommended",
        "generated_utc",
    }
    missing = sorted([field for field in required if field not in payload])
    if missing:
        raise ResearchEventValidationError(f"AI_REVIEW_MISSING_FIELDS:{','.join(missing)}")
    if payload.get("schema_version") != "ai_result_review.v1":
        raise ResearchEventValidationError("AI_REVIEW_SCHEMA_VERSION_MISMATCH")
    allowed = {
        "AUTO_REJECTED",
        "AUTO_ARCHIVED",
        "RETEST",
        "PAPER_CANDIDATE_RECOMMENDED",
        "MANUAL_REVIEW_REQUIRED",
    }
    if str(payload.get("review_status") or "").strip() not in allowed:
        raise ResearchEventValidationError("AI_REVIEW_STATUS_INVALID")
    if not isinstance(payload.get("action_required_for_human"), bool):
        raise ResearchEventValidationError("AI_REVIEW_ACTION_REQUIRED_NOT_BOOL")
    if not isinstance(payload.get("findings"), list):
        raise ResearchEventValidationError("AI_REVIEW_FINDINGS_NOT_LIST")
    return dict(payload)


def validate_research_event_v1(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ResearchEventValidationError("RESEARCH_EVENT_TOP_LEVEL_NOT_OBJECT")
    required = {
        "schema_version",
        "event_id",
        "event_type",
        "day_utc",
        "created_utc",
        "source",
        "source_artifacts",
        "status",
        "idempotency_key",
        "requires_ai_review",
    }
    missing = sorted([field for field in required if field not in payload])
    if missing:
        raise ResearchEventValidationError(f"RESEARCH_EVENT_MISSING_FIELDS:{','.join(missing)}")
    if payload.get("schema_version") != EVENT_SCHEMA_VERSION:
        raise ResearchEventValidationError("RESEARCH_EVENT_SCHEMA_VERSION_MISMATCH")
    event_type = str(payload.get("event_type") or "").strip().upper()
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise ResearchEventValidationError(f"RESEARCH_EVENT_TYPE_UNSUPPORTED:{event_type}")
    day_utc = str(payload.get("day_utc") or "").strip()
    if not DAY_RE.match(day_utc):
        raise ResearchEventValidationError("RESEARCH_EVENT_DAY_INVALID")
    try:
        datetime.fromisoformat(day_utc)
    except ValueError as exc:
        raise ResearchEventValidationError("RESEARCH_EVENT_DAY_INVALID") from exc
    source = str(payload.get("source") or "").strip().upper()
    if source not in SUPPORTED_EVENT_SOURCES:
        raise ResearchEventValidationError(f"RESEARCH_EVENT_SOURCE_INVALID:{source}")
    source_artifacts = payload.get("source_artifacts")
    if not isinstance(source_artifacts, list):
        raise ResearchEventValidationError("RESEARCH_EVENT_SOURCE_ARTIFACTS_NOT_LIST")
    if any(not isinstance(path, str) or not path.strip() for path in source_artifacts):
        raise ResearchEventValidationError("RESEARCH_EVENT_SOURCE_ARTIFACT_PATH_INVALID")
    status = str(payload.get("status") or "").strip().upper()
    if status not in {"NEW", "PROCESSED", "REJECTED", "FAILED"}:
        raise ResearchEventValidationError(f"RESEARCH_EVENT_STATUS_INVALID:{status}")
    if not isinstance(payload.get("requires_ai_review"), bool):
        raise ResearchEventValidationError("RESEARCH_EVENT_REQUIRES_AI_REVIEW_NOT_BOOL")
    idem = str(payload.get("idempotency_key") or "").strip().lower()
    if len(idem) != 64 or any(ch not in "0123456789abcdef" for ch in idem):
        raise ResearchEventValidationError("RESEARCH_EVENT_IDEMPOTENCY_KEY_INVALID")
    event_id = str(payload.get("event_id") or "").strip().lower()
    expected_event_id = build_event_id_v1(
        event_type=event_type,
        day_utc=day_utc,
        source=source,
        idempotency_key=idem,
    )
    if event_id != expected_event_id:
        raise ResearchEventValidationError("RESEARCH_EVENT_ID_INVALID")
    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": event_id,
        "event_type": event_type,
        "day_utc": day_utc,
        "created_utc": str(payload.get("created_utc") or "").strip(),
        "source": source,
        "source_artifacts": sorted([str(path).strip() for path in source_artifacts]),
        "status": status,
        "idempotency_key": idem,
        "requires_ai_review": bool(payload.get("requires_ai_review")),
    }


def build_research_event_v1(
    *,
    event_type: str,
    day_utc: str,
    source: str,
    source_artifacts: list[str],
    requires_ai_review: bool,
    idempotency_key: str | None = None,
    created_utc: str | None = None,
) -> dict[str, Any]:
    normalized_event_type = str(event_type).strip().upper()
    normalized_source = str(source).strip().upper()
    normalized_day = str(day_utc).strip()
    normalized_artifacts = sorted([str(path).strip() for path in source_artifacts if str(path).strip()])
    idem = str(idempotency_key or "").strip().lower()
    if not idem:
        idem = build_idempotency_key_v1(
            event_type=normalized_event_type,
            day_utc=normalized_day,
            source=normalized_source,
            source_artifacts=normalized_artifacts,
        )
    event_id = build_event_id_v1(
        event_type=normalized_event_type,
        day_utc=normalized_day,
        source=normalized_source,
        idempotency_key=idem,
    )
    payload = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": event_id,
        "event_type": normalized_event_type,
        "day_utc": normalized_day,
        "created_utc": str(created_utc or utc_now_iso()),
        "source": normalized_source,
        "source_artifacts": normalized_artifacts,
        "status": "NEW",
        "idempotency_key": idem,
        "requires_ai_review": bool(requires_ai_review),
    }
    return validate_research_event_v1(payload)
