"""
Reliability Ledger V1 (Observational / Administrative Only)

Scope constraints:
- This module records reliability observations, issues, and readiness assessments.
- This module must not be imported into strategy, execution, capital, routing, sizing,
  or live/paper trading decision paths.
- Readiness results are advisory/operator-facing only and do not gate execution.

Operational boundaries:
- Persistence is isolated to the reliability ledger database.
- Access is via reliability API/UI routes only.
- No order generation, routing, sizing, or submission behavior is controlled here.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from constellation_2.common.truth_root_v1 import resolve_runtime_root


RELIABILITY_DB_ENV_VAR_V1 = "C2_RELIABILITY_LEDGER_DB_PATH"
RELIABILITY_DB_RELATIVE_DEFAULT_V1: tuple[str, ...] = (
    "reliability_v1",
    "reliability_ledger.v1.sqlite3",
)


ISSUE_TYPES = (
    "bug",
    "missing_functionality",
    "regression",
    "paper_trading_incident",
    "readiness_blocker",
)

ISSUE_CATEGORIES = (
    "strategy",
    "execution",
    "data",
    "infrastructure",
    "ui_missing_functionality",
)

ISSUE_SEVERITIES = (
    "low",
    "medium",
    "high",
    "critical",
)

ISSUE_STATUSES = (
    "open",
    "triaged",
    "fix_proposed",
    "fix_in_progress",
    "fix_submitted",
    "verification_pending",
    "verified",
    "closed",
    "duplicate",
    "wont_fix",
)

CODEX_STATUSES = (
    "not_needed",
    "task_created",
    "in_progress",
    "patch_submitted",
    "tests_failed",
    "tests_passed",
    "verification_pending",
    "complete",
)

LINK_TYPES = (
    "primary_evidence",
    "recurrence",
    "related",
    "verification",
)

WORK_ORDER_STATUSES = (
    "queued",
    "in_progress",
    "patch_submitted",
    "tests_failed",
    "tests_passed",
    "needs_review",
    "accepted",
    "rejected",
    "canceled",
)

FIX_ATTEMPT_STATUSES = (
    "started",
    "patch_submitted",
    "tests_failed",
    "tests_passed",
    "abandoned",
    "rejected",
)

VERIFICATION_METHODS = (
    "automated_test",
    "manual_review",
    "paper_trading_replay",
    "observation_window",
    "operator_confirmation",
)

VERIFICATION_STATUSES = (
    "pending",
    "passed",
    "failed",
    "inconclusive",
)

READINESS_STATUSES = (
    "not_ready",
    "conditionally_ready",
    "ready",
)

OPEN_ISSUE_STATUSES = {
    "open",
    "triaged",
    "fix_proposed",
    "fix_in_progress",
    "fix_submitted",
    "verification_pending",
}

FIX_PENDING_STATUSES = {
    "fix_submitted",
    "verification_pending",
}

CLEAN_WINDOW_DAYS_REQUIRED = 7


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_id(prefix: str) -> str:
    return f"{prefix}:{uuid.uuid4().hex}"


def _coerce_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _require_text(name: str, value: Any) -> str:
    text = _coerce_text(value)
    if not text:
        raise ValueError(f"RELIABILITY_{name}_REQUIRED")
    return text


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _bool_int(value: bool) -> int:
    return 1 if value else 0


def _parse_iso(value: Any) -> datetime | None:
    text = _coerce_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _normalize_iso(name: str, value: Any, *, default_now: bool = False) -> str:
    parsed = _parse_iso(value)
    if parsed is None:
        if default_now:
            return _utc_now_iso()
        raise ValueError(f"RELIABILITY_{name}_INVALID_ISO8601")
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_enum(name: str, value: Any, allowed: Iterable[str]) -> str:
    text = _require_text(name, value)
    if text not in set(allowed):
        raise ValueError(f"RELIABILITY_{name}_INVALID")
    return text


def _json_canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _json_hash(value: Any) -> str:
    digest = hashlib.sha256()
    digest.update(_json_canonical(value).encode("utf-8"))
    return digest.hexdigest()


def _parse_json_text(value: Any, *, default: Any) -> Any:
    text = _coerce_text(value)
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _normalize_json_array(name: str, value: Any) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"RELIABILITY_{name}_MUST_BE_ARRAY")
    return value


def _normalize_json_object(name: str, value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"RELIABILITY_{name}_MUST_BE_OBJECT")
    return value


def resolve_reliability_db_path_v1() -> Path:
    raw = _coerce_text(os.environ.get(RELIABILITY_DB_ENV_VAR_V1))
    if raw:
        return Path(raw).expanduser().resolve()
    runtime_root = resolve_runtime_root().resolve()
    path = runtime_root
    for part in RELIABILITY_DB_RELATIVE_DEFAULT_V1:
        path = path / part
    return path.resolve()


def _sql_literal_list(values: Iterable[str]) -> str:
    return ", ".join(f"'{item}'" for item in values)


def _open_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


@contextmanager
def reliability_connection_v1(path: Path | None = None):
    resolved = (path or resolve_reliability_db_path_v1()).resolve()
    conn = _open_connection(resolved)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_reliability_schema_v1(path: Path | None = None) -> Path:
    resolved = (path or resolve_reliability_db_path_v1()).resolve()
    with reliability_connection_v1(resolved) as conn:
        conn.execute(
            """
CREATE TABLE IF NOT EXISTS reliability_schema_migrations_v1 (
    migration_id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
)
""".strip()
        )

        row = conn.execute(
            "SELECT migration_id FROM reliability_schema_migrations_v1 WHERE migration_id = ?",
            ("001_reliability_ledger_schema_v1",),
        ).fetchone()
        if row is None:
            conn.executescript(
                f"""
CREATE TABLE IF NOT EXISTS reliability_observations (
    id TEXT PRIMARY KEY,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    source TEXT NOT NULL,
    environment TEXT NOT NULL,
    run_id TEXT NOT NULL DEFAULT '',
    component TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    severity_hint TEXT NOT NULL,
    summary TEXT NOT NULL,
    raw_ref TEXT NOT NULL DEFAULT '',
    structured_payload TEXT NOT NULL DEFAULT '{{}}',
    structured_payload_hash TEXT NOT NULL,
    ai_detected INTEGER NOT NULL CHECK (ai_detected IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_reliability_observations_observed_at
ON reliability_observations (observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_reliability_observations_source
ON reliability_observations (source);
CREATE INDEX IF NOT EXISTS idx_reliability_observations_environment
ON reliability_observations (environment);
CREATE INDEX IF NOT EXISTS idx_reliability_observations_run_id
ON reliability_observations (run_id);
CREATE INDEX IF NOT EXISTS idx_reliability_observations_component
ON reliability_observations (component);
CREATE INDEX IF NOT EXISTS idx_reliability_observations_event_type
ON reliability_observations (event_type);
CREATE INDEX IF NOT EXISTS idx_reliability_observations_severity
ON reliability_observations (severity_hint);

CREATE TRIGGER IF NOT EXISTS reliability_observations_no_update
BEFORE UPDATE ON reliability_observations
BEGIN
  SELECT RAISE(ABORT, 'RELIABILITY_OBSERVATIONS_ARE_APPEND_ONLY');
END;

CREATE TRIGGER IF NOT EXISTS reliability_observations_no_delete
BEFORE DELETE ON reliability_observations
BEGIN
  SELECT RAISE(ABORT, 'RELIABILITY_OBSERVATIONS_ARE_APPEND_ONLY');
END;

CREATE TABLE IF NOT EXISTS reliability_issues (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    title TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ({_sql_literal_list(ISSUE_TYPES)})),
    category TEXT NOT NULL CHECK (category IN ({_sql_literal_list(ISSUE_CATEGORIES)})),
    severity TEXT NOT NULL CHECK (severity IN ({_sql_literal_list(ISSUE_SEVERITIES)})),
    status TEXT NOT NULL CHECK (status IN ({_sql_literal_list(ISSUE_STATUSES)})),
    canonical_key TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL CHECK (occurrence_count >= 1),
    readiness_blocker INTEGER NOT NULL CHECK (readiness_blocker IN (0, 1)),
    impact_summary TEXT NOT NULL,
    expected_behavior TEXT NOT NULL,
    actual_behavior TEXT NOT NULL,
    resolution_summary TEXT NOT NULL DEFAULT '',
    resolved_at TEXT,
    verified_at TEXT,
    verification_method TEXT NOT NULL DEFAULT '',
    codex_task_id TEXT NOT NULL DEFAULT '',
    codex_status TEXT NOT NULL CHECK (codex_status IN ({_sql_literal_list(CODEX_STATUSES)})),
    created_by TEXT NOT NULL,
    ai_assisted INTEGER NOT NULL CHECK (ai_assisted IN (0, 1)),
    ai_confidence REAL
);

CREATE INDEX IF NOT EXISTS idx_reliability_issues_status
ON reliability_issues (status);
CREATE INDEX IF NOT EXISTS idx_reliability_issues_type
ON reliability_issues (type);
CREATE INDEX IF NOT EXISTS idx_reliability_issues_category
ON reliability_issues (category);
CREATE INDEX IF NOT EXISTS idx_reliability_issues_severity
ON reliability_issues (severity);
CREATE INDEX IF NOT EXISTS idx_reliability_issues_canonical
ON reliability_issues (canonical_key);
CREATE INDEX IF NOT EXISTS idx_reliability_issues_last_seen
ON reliability_issues (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS reliability_issue_observations (
    issue_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    link_type TEXT NOT NULL CHECK (link_type IN ({_sql_literal_list(LINK_TYPES)})),
    created_at TEXT NOT NULL,
    PRIMARY KEY (issue_id, observation_id, link_type),
    FOREIGN KEY (issue_id) REFERENCES reliability_issues (id) ON DELETE CASCADE,
    FOREIGN KEY (observation_id) REFERENCES reliability_observations (id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_reliability_issue_observations_issue
ON reliability_issue_observations (issue_id);
CREATE INDEX IF NOT EXISTS idx_reliability_issue_observations_observation
ON reliability_issue_observations (observation_id);

CREATE TABLE IF NOT EXISTS readiness_assessments (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ({_sql_literal_list(READINESS_STATUSES)})),
    score INTEGER NOT NULL,
    decision_reason TEXT NOT NULL,
    blocking_issue_ids TEXT NOT NULL DEFAULT '[]',
    rule_version TEXT NOT NULL,
    generated_by TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readiness_assessments_created
ON readiness_assessments (created_at DESC);

CREATE TABLE IF NOT EXISTS readiness_rule_results (
    assessment_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_name TEXT NOT NULL,
    passed INTEGER NOT NULL CHECK (passed IN (0, 1)),
    details TEXT NOT NULL,
    blocking_issue_ids TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (assessment_id, rule_id),
    FOREIGN KEY (assessment_id) REFERENCES readiness_assessments (id) ON DELETE CASCADE
);
""".strip()
            )
            conn.execute(
                "INSERT INTO reliability_schema_migrations_v1 (migration_id, applied_at) VALUES (?, ?)",
                ("001_reliability_ledger_schema_v1", _utc_now_iso()),
            )

        workflow_row = conn.execute(
            "SELECT migration_id FROM reliability_schema_migrations_v1 WHERE migration_id = ?",
            ("002_reliability_workflow_queue_v1",),
        ).fetchone()
        if workflow_row is None:
            conn.executescript(
                f"""
CREATE TABLE IF NOT EXISTS reliability_work_orders (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ({_sql_literal_list(WORK_ORDER_STATUSES)})),
    objective TEXT NOT NULL,
    affected_component TEXT NOT NULL DEFAULT '',
    expected_behavior TEXT NOT NULL DEFAULT '',
    actual_behavior TEXT NOT NULL DEFAULT '',
    constraints TEXT NOT NULL DEFAULT '[]',
    forbidden_changes TEXT NOT NULL DEFAULT '[]',
    likely_files TEXT NOT NULL DEFAULT '[]',
    required_tests TEXT NOT NULL DEFAULT '[]',
    verification_criteria TEXT NOT NULL DEFAULT '[]',
    assigned_agent TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT 'operator',
    ai_generated INTEGER NOT NULL CHECK (ai_generated IN (0, 1)),
    ai_confidence REAL,
    FOREIGN KEY (issue_id) REFERENCES reliability_issues (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_reliability_work_orders_issue
ON reliability_work_orders (issue_id);
CREATE INDEX IF NOT EXISTS idx_reliability_work_orders_status
ON reliability_work_orders (status);
CREATE INDEX IF NOT EXISTS idx_reliability_work_orders_updated
ON reliability_work_orders (updated_at DESC);

CREATE TABLE IF NOT EXISTS reliability_fix_attempts (
    id TEXT PRIMARY KEY,
    work_order_id TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK (status IN ({_sql_literal_list(FIX_ATTEMPT_STATUSES)})),
    files_changed TEXT NOT NULL DEFAULT '[]',
    diff_ref TEXT NOT NULL DEFAULT '',
    tests_run TEXT NOT NULL DEFAULT '[]',
    test_results TEXT NOT NULL DEFAULT '{{}}',
    codex_summary TEXT NOT NULL DEFAULT '',
    risk_notes TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT 'operator',
    FOREIGN KEY (work_order_id) REFERENCES reliability_work_orders (id) ON DELETE CASCADE,
    FOREIGN KEY (issue_id) REFERENCES reliability_issues (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_reliability_fix_attempts_work_order
ON reliability_fix_attempts (work_order_id);
CREATE INDEX IF NOT EXISTS idx_reliability_fix_attempts_issue
ON reliability_fix_attempts (issue_id);
CREATE INDEX IF NOT EXISTS idx_reliability_fix_attempts_status
ON reliability_fix_attempts (status);
CREATE INDEX IF NOT EXISTS idx_reliability_fix_attempts_started
ON reliability_fix_attempts (started_at DESC);

CREATE TABLE IF NOT EXISTS reliability_verifications (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    work_order_id TEXT,
    fix_attempt_id TEXT,
    created_at TEXT NOT NULL,
    method TEXT NOT NULL CHECK (method IN ({_sql_literal_list(VERIFICATION_METHODS)})),
    status TEXT NOT NULL CHECK (status IN ({_sql_literal_list(VERIFICATION_STATUSES)})),
    evidence TEXT NOT NULL DEFAULT '{{}}',
    verified_by TEXT NOT NULL DEFAULT 'operator',
    verified_at TEXT,
    notes TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (issue_id) REFERENCES reliability_issues (id) ON DELETE CASCADE,
    FOREIGN KEY (work_order_id) REFERENCES reliability_work_orders (id) ON DELETE SET NULL,
    FOREIGN KEY (fix_attempt_id) REFERENCES reliability_fix_attempts (id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_reliability_verifications_issue
ON reliability_verifications (issue_id);
CREATE INDEX IF NOT EXISTS idx_reliability_verifications_work_order
ON reliability_verifications (work_order_id);
CREATE INDEX IF NOT EXISTS idx_reliability_verifications_fix_attempt
ON reliability_verifications (fix_attempt_id);
CREATE INDEX IF NOT EXISTS idx_reliability_verifications_status
ON reliability_verifications (status);
CREATE INDEX IF NOT EXISTS idx_reliability_verifications_created
ON reliability_verifications (created_at DESC);
""".strip()
            )
            conn.execute(
                "INSERT INTO reliability_schema_migrations_v1 (migration_id, applied_at) VALUES (?, ?)",
                ("002_reliability_workflow_queue_v1", _utc_now_iso()),
            )
    return resolved


def _row_to_observation(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "observed_at": row["observed_at"],
        "created_at": row["created_at"],
        "source": row["source"],
        "environment": row["environment"],
        "run_id": row["run_id"],
        "component": row["component"],
        "event_type": row["event_type"],
        "severity_hint": row["severity_hint"],
        "summary": row["summary"],
        "raw_ref": row["raw_ref"],
        "structured_payload": _parse_json_text(row["structured_payload"], default={}),
        "structured_payload_hash": row["structured_payload_hash"],
        "ai_detected": bool(row["ai_detected"]),
    }


def _row_to_issue(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "title": row["title"],
        "type": row["type"],
        "category": row["category"],
        "severity": row["severity"],
        "status": row["status"],
        "canonical_key": row["canonical_key"],
        "first_seen_at": row["first_seen_at"],
        "last_seen_at": row["last_seen_at"],
        "occurrence_count": int(row["occurrence_count"]),
        "readiness_blocker": bool(row["readiness_blocker"]),
        "impact_summary": row["impact_summary"],
        "expected_behavior": row["expected_behavior"],
        "actual_behavior": row["actual_behavior"],
        "resolution_summary": row["resolution_summary"],
        "resolved_at": row["resolved_at"],
        "verified_at": row["verified_at"],
        "verification_method": row["verification_method"],
        "codex_task_id": row["codex_task_id"],
        "codex_status": row["codex_status"],
        "created_by": row["created_by"],
        "ai_assisted": bool(row["ai_assisted"]),
        "ai_confidence": row["ai_confidence"],
    }


def _row_to_work_order(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "issue_id": row["issue_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "status": row["status"],
        "objective": row["objective"],
        "affected_component": row["affected_component"],
        "expected_behavior": row["expected_behavior"],
        "actual_behavior": row["actual_behavior"],
        "constraints": _parse_json_text(row["constraints"], default=[]),
        "forbidden_changes": _parse_json_text(row["forbidden_changes"], default=[]),
        "likely_files": _parse_json_text(row["likely_files"], default=[]),
        "required_tests": _parse_json_text(row["required_tests"], default=[]),
        "verification_criteria": _parse_json_text(row["verification_criteria"], default=[]),
        "assigned_agent": row["assigned_agent"],
        "created_by": row["created_by"],
        "ai_generated": bool(row["ai_generated"]),
        "ai_confidence": row["ai_confidence"],
    }


def _row_to_fix_attempt(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "work_order_id": row["work_order_id"],
        "issue_id": row["issue_id"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "status": row["status"],
        "files_changed": _parse_json_text(row["files_changed"], default=[]),
        "diff_ref": row["diff_ref"],
        "tests_run": _parse_json_text(row["tests_run"], default=[]),
        "test_results": _parse_json_text(row["test_results"], default={}),
        "codex_summary": row["codex_summary"],
        "risk_notes": row["risk_notes"],
        "created_by": row["created_by"],
    }


def _row_to_verification(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "issue_id": row["issue_id"],
        "work_order_id": row["work_order_id"],
        "fix_attempt_id": row["fix_attempt_id"],
        "created_at": row["created_at"],
        "method": row["method"],
        "status": row["status"],
        "evidence": _parse_json_text(row["evidence"], default={}),
        "verified_by": row["verified_by"],
        "verified_at": row["verified_at"],
        "notes": row["notes"],
    }


def _row_to_assessment(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "period_start": row["period_start"],
        "period_end": row["period_end"],
        "status": row["status"],
        "score": int(row["score"]),
        "decision_reason": row["decision_reason"],
        "blocking_issue_ids": _parse_json_text(row["blocking_issue_ids"], default=[]),
        "rule_version": row["rule_version"],
        "generated_by": row["generated_by"],
    }


def _row_to_rule_result(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "assessment_id": row["assessment_id"],
        "rule_id": row["rule_id"],
        "rule_name": row["rule_name"],
        "passed": bool(row["passed"]),
        "details": _parse_json_text(row["details"], default={}),
        "blocking_issue_ids": _parse_json_text(row["blocking_issue_ids"], default=[]),
    }


def _build_codex_task(issue: dict[str, Any], *, evidence_refs: list[str] | None = None) -> dict[str, Any]:
    return {
        "issue_id": issue["id"],
        "title": issue["title"],
        "affected_component": issue["category"],
        "actual_behavior": issue["actual_behavior"],
        "expected_behavior": issue["expected_behavior"],
        "evidence_refs": evidence_refs or [],
        "constraints": [
            "Do not modify strategy, execution, or capital logic.",
            "Keep changes isolated to reliability/observability surfaces.",
            "Preserve existing public API behavior outside reliability namespace.",
        ],
        "likely_files": [
            "constellation_2/phaseL/ui_api/reliability_ledger_v1.py",
            "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py",
            "constellation_2/phaseL/ui/static/operator_shell/pages/index.js",
        ],
        "required_tests": [
            "Reliability readiness rule regression tests",
            "Issue lifecycle status-transition tests",
            "API contract tests for reliability endpoints",
        ],
        "verification_criteria": [
            "Issue status progresses to verified only with verification evidence.",
            "Readiness assessment explains blocking issues deterministically.",
            "No trading/runtime behavior changed outside reliability endpoints.",
        ],
        "forbidden_changes": [
            "No changes to order generation/routing/sizing logic.",
            "No mutation of existing trading strategy decisions.",
        ],
    }


def _slugify(value: str, *, max_len: int = 96) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned[:max_len] or f"issue-{uuid.uuid4().hex[:12]}"


def _coerce_confidence(value: Any, *, field_name: str = "AI_CONFIDENCE") -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception as exc:
        raise ValueError(f"RELIABILITY_{field_name}_INVALID") from exc
    if parsed < 0 or parsed > 1:
        raise ValueError(f"RELIABILITY_{field_name}_INVALID")
    return parsed


def _set_issue_state(
    conn: sqlite3.Connection,
    *,
    issue_id: str,
    status: str | None = None,
    codex_status: str | None = None,
    verification_method: str | None = None,
    verified_at: str | None = None,
    resolution_summary: str | None = None,
) -> dict[str, Any]:
    issue_row = _load_issue_row(conn, issue_id)
    current = _row_to_issue(issue_row)
    updates: dict[str, Any] = {}
    if status:
        if status not in ISSUE_STATUSES:
            raise ValueError("RELIABILITY_STATUS_INVALID")
        if current["status"] in {"closed", "duplicate", "wont_fix"} and status not in {"closed", "duplicate", "wont_fix"}:
            status = current["status"]
        updates["status"] = status
    if codex_status:
        if codex_status not in CODEX_STATUSES:
            raise ValueError("RELIABILITY_CODEX_STATUS_INVALID")
        updates["codex_status"] = codex_status
    if verification_method is not None:
        updates["verification_method"] = _coerce_text(verification_method)
    if verified_at is not None:
        updates["verified_at"] = verified_at
    if resolution_summary is not None:
        updates["resolution_summary"] = _coerce_text(resolution_summary)
    if not updates:
        return current
    updates["updated_at"] = _utc_now_iso()
    set_sql = ", ".join(f"{key} = ?" for key in updates.keys())
    conn.execute(
        f"UPDATE reliability_issues SET {set_sql} WHERE id = ?",
        tuple(updates.values()) + (issue_id,),
    )
    return _row_to_issue(_load_issue_row(conn, issue_id))


def _work_order_status_from_fix_attempt_status(status: str) -> str:
    mapping = {
        "started": "in_progress",
        "patch_submitted": "patch_submitted",
        "tests_failed": "tests_failed",
        "tests_passed": "tests_passed",
        "abandoned": "canceled",
        "rejected": "rejected",
    }
    return mapping.get(status, "in_progress")


def _issue_status_from_fix_attempt_status(status: str) -> str:
    mapping = {
        "started": "fix_in_progress",
        "patch_submitted": "fix_submitted",
        "tests_failed": "fix_in_progress",
        "tests_passed": "verification_pending",
        "abandoned": "triaged",
        "rejected": "triaged",
    }
    return mapping.get(status, "fix_in_progress")


def _build_codex_work_order_prompt(issue: dict[str, Any], work_order: dict[str, Any]) -> str:
    lines = [
        "Reliability Work Order (Advisory Only)",
        f"Issue ID: {issue.get('id', '')}",
        f"Work Order ID: {work_order.get('id', '')}",
        f"Objective: {work_order.get('objective', '')}",
        f"Affected Component: {work_order.get('affected_component', '')}",
        f"Expected Behavior: {work_order.get('expected_behavior', '')}",
        f"Actual Behavior: {work_order.get('actual_behavior', '')}",
        "",
        "Constraints:",
    ]
    lines.extend(f"- {item}" for item in _normalize_json_array("CONSTRAINTS", work_order.get("constraints")))
    lines.append("Forbidden Changes:")
    lines.extend(f"- {item}" for item in _normalize_json_array("FORBIDDEN_CHANGES", work_order.get("forbidden_changes")))
    lines.append("Likely Files:")
    lines.extend(f"- {item}" for item in _normalize_json_array("LIKELY_FILES", work_order.get("likely_files")))
    lines.append("Required Tests:")
    lines.extend(f"- {item}" for item in _normalize_json_array("REQUIRED_TESTS", work_order.get("required_tests")))
    lines.append("Verification Criteria:")
    lines.extend(f"- {item}" for item in _normalize_json_array("VERIFICATION_CRITERIA", work_order.get("verification_criteria")))
    lines.append("")
    lines.append("Safety: Do not modify strategy, execution, capital sizing/routing, broker behavior, or trading logic.")
    return "\n".join(lines)


def create_reliability_observation_v1(payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    observed_at = _normalize_iso("OBSERVED_AT", payload.get("observed_at"), default_now=True)
    created_at = _utc_now_iso()
    structured_payload = payload.get("structured_payload")
    if structured_payload is None:
        structured_payload = {}
    if not isinstance(structured_payload, (dict, list)):
        raise ValueError("RELIABILITY_STRUCTURED_PAYLOAD_MUST_BE_OBJECT_OR_ARRAY")
    observation = {
        "id": _new_id("reliability_observation"),
        "observed_at": observed_at,
        "created_at": created_at,
        "source": _require_text("SOURCE", payload.get("source")),
        "environment": _require_text("ENVIRONMENT", payload.get("environment")),
        "run_id": _coerce_text(payload.get("run_id")),
        "component": _coerce_text(payload.get("component")),
        "event_type": _require_text("EVENT_TYPE", payload.get("event_type")),
        "severity_hint": _coerce_text(payload.get("severity_hint"), default="unknown") or "unknown",
        "summary": _require_text("SUMMARY", payload.get("summary")),
        "raw_ref": _coerce_text(payload.get("raw_ref")),
        "structured_payload": structured_payload,
        "structured_payload_hash": _json_hash(structured_payload),
        "ai_detected": _coerce_bool(payload.get("ai_detected"), default=False),
    }
    with reliability_connection_v1(db_path) as conn:
        conn.execute(
            """
INSERT INTO reliability_observations (
    id, observed_at, created_at, source, environment, run_id, component,
    event_type, severity_hint, summary, raw_ref, structured_payload,
    structured_payload_hash, ai_detected
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
            (
                observation["id"],
                observation["observed_at"],
                observation["created_at"],
                observation["source"],
                observation["environment"],
                observation["run_id"],
                observation["component"],
                observation["event_type"],
                observation["severity_hint"],
                observation["summary"],
                observation["raw_ref"],
                _json_canonical(observation["structured_payload"]),
                observation["structured_payload_hash"],
                _bool_int(observation["ai_detected"]),
            ),
        )
    return observation


def list_reliability_observations_v1(filters: dict[str, Any] | None = None) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    filters = filters or {}
    where: list[str] = []
    params: list[Any] = []

    for key in ("source", "component", "environment", "run_id", "event_type", "severity_hint"):
        value = _coerce_text(filters.get(key))
        if value:
            where.append(f"{key} = ?")
            params.append(value)

    date_from = _coerce_text(filters.get("date_from"))
    if date_from:
        where.append("observed_at >= ?")
        params.append(date_from)
    date_to = _coerce_text(filters.get("date_to"))
    if date_to:
        where.append("observed_at <= ?")
        params.append(date_to)

    sql = "SELECT * FROM reliability_observations"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY observed_at DESC, created_at DESC"

    with reliability_connection_v1(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    observations = [_row_to_observation(row) for row in rows]
    return {"ok": True, "observations": observations, "total_count": len(observations)}


def _load_issue_row(conn: sqlite3.Connection, issue_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM reliability_issues WHERE id = ?", (issue_id,)).fetchone()
    if row is None:
        raise ValueError("RELIABILITY_ISSUE_NOT_FOUND")
    return row


def _resolve_issue_id_v1(
    conn: sqlite3.Connection,
    issue_id: str,
    *,
    required: bool = True,
) -> str | None:
    normalized = _require_text("ISSUE_ID", issue_id)
    direct = conn.execute(
        "SELECT id FROM reliability_issues WHERE id = ?",
        (normalized,),
    ).fetchone()
    if direct is not None:
        return str(direct["id"])

    lookup = normalized
    if lookup.lower().startswith("issue-"):
        lookup = lookup.split("-", 1)[1]
    elif lookup.startswith("reliability_issue:"):
        lookup = lookup.split(":", 1)[1]
    lookup = lookup.strip().lower()
    if not lookup:
        if required:
            raise ValueError("RELIABILITY_ISSUE_NOT_FOUND")
        return None

    rows = conn.execute(
        """
SELECT id
FROM reliability_issues
WHERE lower(id) LIKE ?
ORDER BY updated_at DESC, created_at DESC
LIMIT 3
""".strip(),
        (f"reliability_issue:{lookup}%",),
    ).fetchall()
    if not rows:
        if required:
            raise ValueError("RELIABILITY_ISSUE_NOT_FOUND")
        return None
    if len(rows) > 1:
        raise ValueError("RELIABILITY_ISSUE_ID_AMBIGUOUS")
    return str(rows[0]["id"])


def _load_work_order_row(conn: sqlite3.Connection, work_order_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM reliability_work_orders WHERE id = ?", (work_order_id,)).fetchone()
    if row is None:
        raise ValueError("RELIABILITY_WORK_ORDER_NOT_FOUND")
    return row


def _load_fix_attempt_row(conn: sqlite3.Connection, attempt_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM reliability_fix_attempts WHERE id = ?", (attempt_id,)).fetchone()
    if row is None:
        raise ValueError("RELIABILITY_FIX_ATTEMPT_NOT_FOUND")
    return row


def _load_verification_row(conn: sqlite3.Connection, verification_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM reliability_verifications WHERE id = ?", (verification_id,)).fetchone()
    if row is None:
        raise ValueError("RELIABILITY_VERIFICATION_NOT_FOUND")
    return row


def _normalize_issue_create_payload(payload: dict[str, Any]) -> dict[str, Any]:
    now = _utc_now_iso()
    issue_type = _require_enum("ISSUE_TYPE", payload.get("type"), ISSUE_TYPES)
    readiness_blocker_default = issue_type == "readiness_blocker"
    ai_confidence = payload.get("ai_confidence")
    if ai_confidence is not None:
        try:
            ai_confidence = float(ai_confidence)
        except Exception as exc:
            raise ValueError("RELIABILITY_AI_CONFIDENCE_INVALID") from exc
        if ai_confidence < 0 or ai_confidence > 1:
            raise ValueError("RELIABILITY_AI_CONFIDENCE_INVALID")

    return {
        "id": _new_id("reliability_issue"),
        "created_at": now,
        "updated_at": now,
        "title": _require_text("TITLE", payload.get("title")),
        "type": issue_type,
        "category": _require_enum("CATEGORY", payload.get("category"), ISSUE_CATEGORIES),
        "severity": _require_enum("SEVERITY", payload.get("severity"), ISSUE_SEVERITIES),
        "status": _require_enum("STATUS", payload.get("status") or "open", ISSUE_STATUSES),
        "canonical_key": _require_text("CANONICAL_KEY", payload.get("canonical_key")),
        "first_seen_at": _normalize_iso("FIRST_SEEN_AT", payload.get("first_seen_at"), default_now=True),
        "last_seen_at": _normalize_iso("LAST_SEEN_AT", payload.get("last_seen_at"), default_now=True),
        "occurrence_count": max(1, int(payload.get("occurrence_count") or 1)),
        "readiness_blocker": _coerce_bool(payload.get("readiness_blocker"), default=readiness_blocker_default),
        "impact_summary": _require_text("IMPACT_SUMMARY", payload.get("impact_summary")),
        "expected_behavior": _require_text("EXPECTED_BEHAVIOR", payload.get("expected_behavior")),
        "actual_behavior": _require_text("ACTUAL_BEHAVIOR", payload.get("actual_behavior")),
        "resolution_summary": _coerce_text(payload.get("resolution_summary")),
        "resolved_at": _coerce_text(payload.get("resolved_at")) or None,
        "verified_at": _coerce_text(payload.get("verified_at")) or None,
        "verification_method": _coerce_text(payload.get("verification_method")),
        "codex_task_id": _coerce_text(payload.get("codex_task_id")),
        "codex_status": _require_enum("CODEX_STATUS", payload.get("codex_status") or "not_needed", CODEX_STATUSES),
        "created_by": _coerce_text(payload.get("created_by"), default="operator") or "operator",
        "ai_assisted": _coerce_bool(payload.get("ai_assisted"), default=False),
        "ai_confidence": ai_confidence,
    }


def _insert_issue(conn: sqlite3.Connection, issue: dict[str, Any]) -> None:
    conn.execute(
        """
INSERT INTO reliability_issues (
    id, created_at, updated_at, title, type, category, severity, status,
    canonical_key, first_seen_at, last_seen_at, occurrence_count, readiness_blocker,
    impact_summary, expected_behavior, actual_behavior, resolution_summary,
    resolved_at, verified_at, verification_method, codex_task_id, codex_status,
    created_by, ai_assisted, ai_confidence
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
        (
            issue["id"],
            issue["created_at"],
            issue["updated_at"],
            issue["title"],
            issue["type"],
            issue["category"],
            issue["severity"],
            issue["status"],
            issue["canonical_key"],
            issue["first_seen_at"],
            issue["last_seen_at"],
            issue["occurrence_count"],
            _bool_int(issue["readiness_blocker"]),
            issue["impact_summary"],
            issue["expected_behavior"],
            issue["actual_behavior"],
            issue["resolution_summary"],
            issue["resolved_at"],
            issue["verified_at"],
            issue["verification_method"],
            issue["codex_task_id"],
            issue["codex_status"],
            issue["created_by"],
            _bool_int(issue["ai_assisted"]),
            issue["ai_confidence"],
        ),
    )


def _link_issue_observations(conn: sqlite3.Connection, *, issue_id: str, observation_ids: Iterable[str], link_type: str) -> None:
    normalized_type = _require_enum("LINK_TYPE", link_type, LINK_TYPES)
    now = _utc_now_iso()
    for observation_id in observation_ids:
        obs_id = _coerce_text(observation_id)
        if not obs_id:
            continue
        row = conn.execute("SELECT id, observed_at FROM reliability_observations WHERE id = ?", (obs_id,)).fetchone()
        if row is None:
            raise ValueError("RELIABILITY_OBSERVATION_NOT_FOUND")
        conn.execute(
            """
INSERT OR IGNORE INTO reliability_issue_observations (issue_id, observation_id, link_type, created_at)
VALUES (?, ?, ?, ?)
""".strip(),
            (issue_id, obs_id, normalized_type, now),
        )
        if normalized_type == "recurrence":
            conn.execute(
                """
UPDATE reliability_issues
SET occurrence_count = occurrence_count + 1,
    last_seen_at = CASE
        WHEN last_seen_at < ? THEN ?
        ELSE last_seen_at
    END,
    updated_at = ?
WHERE id = ?
""".strip(),
                (row["observed_at"], row["observed_at"], now, issue_id),
            )


def create_reliability_issue_v1(payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    issue = _normalize_issue_create_payload(payload)
    observation_ids = payload.get("observation_ids")
    if not isinstance(observation_ids, list):
        observation_ids = []

    with reliability_connection_v1(db_path) as conn:
        existing = conn.execute(
            """
SELECT *
FROM reliability_issues
WHERE canonical_key = ?
  AND status NOT IN ('duplicate', 'wont_fix')
ORDER BY updated_at DESC
LIMIT 1
""".strip(),
            (issue["canonical_key"],),
        ).fetchone()

        if existing is not None:
            now = _utc_now_iso()
            issue_id = str(existing["id"])
            if observation_ids:
                conn.execute(
                    """
UPDATE reliability_issues
SET updated_at = ?
WHERE id = ?
""".strip(),
                    (now, issue_id),
                )
                _link_issue_observations(
                    conn,
                    issue_id=issue_id,
                    observation_ids=observation_ids,
                    link_type="recurrence",
                )
            else:
                conn.execute(
                    """
UPDATE reliability_issues
SET occurrence_count = occurrence_count + 1,
    last_seen_at = ?,
    updated_at = ?
WHERE id = ?
""".strip(),
                    (issue["last_seen_at"], now, issue_id),
                )
            row = _load_issue_row(conn, issue_id)
            normalized = _row_to_issue(row)
            normalized["canonical_match_reused"] = True
            return {"ok": True, "issue": normalized}

        _insert_issue(conn, issue)
        if observation_ids:
            _link_issue_observations(
                conn,
                issue_id=issue["id"],
                observation_ids=observation_ids,
                link_type="primary_evidence",
            )

        row = _load_issue_row(conn, issue["id"])
        return {"ok": True, "issue": _row_to_issue(row)}


def list_reliability_issues_v1(filters: dict[str, Any] | None = None) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    filters = filters or {}

    joins: list[str] = []
    where: list[str] = []
    params: list[Any] = []

    for key in ("status", "type", "category", "severity", "canonical_key"):
        value = _coerce_text(filters.get(key))
        if value:
            where.append(f"i.{key} = ?")
            params.append(value)

    rb = filters.get("readiness_blocker")
    if rb is not None and str(rb).strip() != "":
        where.append("i.readiness_blocker = ?")
        params.append(_bool_int(_coerce_bool(rb)))

    environment = _coerce_text(filters.get("environment"))
    if environment:
        joins.append("JOIN reliability_issue_observations l_env ON l_env.issue_id = i.id")
        joins.append("JOIN reliability_observations o_env ON o_env.id = l_env.observation_id")
        where.append("o_env.environment = ?")
        params.append(environment)

    date_from = _coerce_text(filters.get("date_from"))
    if date_from:
        where.append("i.last_seen_at >= ?")
        params.append(date_from)
    date_to = _coerce_text(filters.get("date_to"))
    if date_to:
        where.append("i.first_seen_at <= ?")
        params.append(date_to)

    sql = "SELECT DISTINCT i.* FROM reliability_issues i"
    if joins:
        sql += " " + " ".join(joins)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY CASE i.severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, i.updated_at DESC"

    with reliability_connection_v1(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()

    issues = [_row_to_issue(row) for row in rows]
    return {"ok": True, "issues": issues, "total_count": len(issues)}


def get_reliability_issue_v1(issue_id: str) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("ISSUE_ID", issue_id)

    with reliability_connection_v1(db_path) as conn:
        issue_row = _load_issue_row(conn, normalized_id)
        link_rows = conn.execute(
            """
SELECT l.issue_id, l.observation_id, l.link_type, l.created_at,
       o.observed_at, o.source, o.environment, o.component, o.event_type,
       o.severity_hint, o.summary, o.raw_ref
FROM reliability_issue_observations l
JOIN reliability_observations o ON o.id = l.observation_id
WHERE l.issue_id = ?
ORDER BY o.observed_at ASC, l.created_at ASC
""".strip(),
            (normalized_id,),
        ).fetchall()
        work_order_rows = conn.execute(
            "SELECT * FROM reliability_work_orders WHERE issue_id = ? ORDER BY updated_at DESC",
            (normalized_id,),
        ).fetchall()
        fix_attempt_rows = conn.execute(
            "SELECT * FROM reliability_fix_attempts WHERE issue_id = ? ORDER BY started_at DESC",
            (normalized_id,),
        ).fetchall()
        verification_rows = conn.execute(
            "SELECT * FROM reliability_verifications WHERE issue_id = ? ORDER BY created_at DESC",
            (normalized_id,),
        ).fetchall()

    issue = _row_to_issue(issue_row)
    linked_observations = [
        {
            "issue_id": row["issue_id"],
            "observation_id": row["observation_id"],
            "link_type": row["link_type"],
            "created_at": row["created_at"],
            "observed_at": row["observed_at"],
            "source": row["source"],
            "environment": row["environment"],
            "component": row["component"],
            "event_type": row["event_type"],
            "severity_hint": row["severity_hint"],
            "summary": row["summary"],
            "raw_ref": row["raw_ref"],
        }
        for row in link_rows
    ]

    recurrence_timeline = [
        {
            "observed_at": row["observed_at"],
            "observation_id": row["observation_id"],
            "link_type": row["link_type"],
            "summary": row["summary"],
        }
        for row in link_rows
    ]

    evidence_refs = [item["observation_id"] for item in linked_observations]
    codex_task = _build_codex_task(issue, evidence_refs=evidence_refs)
    work_orders = [_row_to_work_order(row) for row in work_order_rows]
    fix_attempts = [_row_to_fix_attempt(row) for row in fix_attempt_rows]
    verifications = [_row_to_verification(row) for row in verification_rows]

    next_action = "none"
    if issue["status"] in OPEN_ISSUE_STATUSES and issue["readiness_blocker"] and not work_orders:
        next_action = "create_work_order"
    elif any(item["status"] == "queued" for item in work_orders):
        next_action = "start_fix_attempt"
    elif any(item["status"] in {"tests_passed", "patch_submitted"} for item in fix_attempts) and not any(item["status"] == "passed" for item in verifications):
        next_action = "record_verification"
    elif issue["status"] == "verified":
        next_action = "close_issue"

    return {
        "ok": True,
        "issue": issue,
        "linked_observations": linked_observations,
        "recurrence_timeline": recurrence_timeline,
        "codex_task": codex_task,
        "work_orders": work_orders,
        "fix_attempts": fix_attempts,
        "verifications": verifications,
        "next_action": next_action,
    }


def update_reliability_issue_v1(issue_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("ISSUE_ID", issue_id)
    if not isinstance(patch, dict):
        raise ValueError("RELIABILITY_PATCH_BODY_MUST_BE_OBJECT")

    allowed_fields = {
        "title",
        "type",
        "category",
        "severity",
        "status",
        "readiness_blocker",
        "impact_summary",
        "expected_behavior",
        "actual_behavior",
        "resolution_summary",
        "resolved_at",
        "verified_at",
        "verification_method",
        "codex_task_id",
        "codex_status",
        "ai_assisted",
        "ai_confidence",
    }

    with reliability_connection_v1(db_path) as conn:
        current_row = _load_issue_row(conn, normalized_id)
        current = _row_to_issue(current_row)

        updates: dict[str, Any] = {}
        for key, value in patch.items():
            if key not in allowed_fields:
                continue
            if key == "type":
                updates[key] = _require_enum("ISSUE_TYPE", value, ISSUE_TYPES)
            elif key == "category":
                updates[key] = _require_enum("CATEGORY", value, ISSUE_CATEGORIES)
            elif key == "severity":
                updates[key] = _require_enum("SEVERITY", value, ISSUE_SEVERITIES)
            elif key == "status":
                updates[key] = _require_enum("STATUS", value, ISSUE_STATUSES)
            elif key == "codex_status":
                updates[key] = _require_enum("CODEX_STATUS", value, CODEX_STATUSES)
            elif key == "readiness_blocker":
                updates[key] = _bool_int(_coerce_bool(value))
            elif key == "ai_assisted":
                updates[key] = _bool_int(_coerce_bool(value))
            elif key == "ai_confidence":
                updates[key] = _coerce_confidence(value)
            elif key in {"resolved_at", "verified_at"}:
                updates[key] = _normalize_iso(key.upper(), value, default_now=False) if _coerce_text(value) else None
            else:
                updates[key] = _coerce_text(value)

        target_status = updates.get("status", current["status"])
        if target_status == "closed" and current["status"] not in {"verified", "wont_fix", "duplicate", "closed"}:
            raise ValueError("RELIABILITY_CLOSE_REQUIRES_VERIFIED")

        if target_status == "verified":
            verification_method = updates.get("verification_method", current.get("verification_method") or "")
            if not _coerce_text(verification_method):
                raise ValueError("RELIABILITY_VERIFIED_REQUIRES_VERIFICATION_METHOD")
            updates["verification_method"] = _coerce_text(verification_method)
            if not updates.get("verified_at"):
                updates["verified_at"] = _utc_now_iso()

        updates["updated_at"] = _utc_now_iso()

        if not updates:
            return {"ok": True, "issue": current}

        set_sql = ", ".join(f"{key} = ?" for key in updates.keys())
        conn.execute(
            f"UPDATE reliability_issues SET {set_sql} WHERE id = ?",
            tuple(updates.values()) + (normalized_id,),
        )

        updated_row = _load_issue_row(conn, normalized_id)
        return {"ok": True, "issue": _row_to_issue(updated_row)}


def _normalize_work_order_payload(issue: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    confidence = _coerce_confidence(payload.get("ai_confidence"))
    return {
        "id": _new_id("reliability_work_order"),
        "issue_id": issue["id"],
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "status": _require_enum("WORK_ORDER_STATUS", payload.get("status") or "queued", WORK_ORDER_STATUSES),
        "objective": _require_text("OBJECTIVE", payload.get("objective") or issue.get("impact_summary")),
        "affected_component": _coerce_text(payload.get("affected_component"), default=issue.get("category") or ""),
        "expected_behavior": _coerce_text(payload.get("expected_behavior"), default=issue.get("expected_behavior") or ""),
        "actual_behavior": _coerce_text(payload.get("actual_behavior"), default=issue.get("actual_behavior") or ""),
        "constraints": _normalize_json_array("CONSTRAINTS", payload.get("constraints")),
        "forbidden_changes": _normalize_json_array("FORBIDDEN_CHANGES", payload.get("forbidden_changes")),
        "likely_files": _normalize_json_array("LIKELY_FILES", payload.get("likely_files")),
        "required_tests": _normalize_json_array("REQUIRED_TESTS", payload.get("required_tests")),
        "verification_criteria": _normalize_json_array("VERIFICATION_CRITERIA", payload.get("verification_criteria")),
        "assigned_agent": _coerce_text(payload.get("assigned_agent")),
        "created_by": _coerce_text(payload.get("created_by"), default="operator") or "operator",
        "ai_generated": _coerce_bool(payload.get("ai_generated"), default=False),
        "ai_confidence": confidence,
    }


def _insert_work_order(conn: sqlite3.Connection, work_order: dict[str, Any]) -> None:
    conn.execute(
        """
INSERT INTO reliability_work_orders (
    id, issue_id, created_at, updated_at, status, objective, affected_component,
    expected_behavior, actual_behavior, constraints, forbidden_changes, likely_files,
    required_tests, verification_criteria, assigned_agent, created_by, ai_generated,
    ai_confidence
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
        (
            work_order["id"],
            work_order["issue_id"],
            work_order["created_at"],
            work_order["updated_at"],
            work_order["status"],
            work_order["objective"],
            work_order["affected_component"],
            work_order["expected_behavior"],
            work_order["actual_behavior"],
            _json_canonical(work_order["constraints"]),
            _json_canonical(work_order["forbidden_changes"]),
            _json_canonical(work_order["likely_files"]),
            _json_canonical(work_order["required_tests"]),
            _json_canonical(work_order["verification_criteria"]),
            work_order["assigned_agent"],
            work_order["created_by"],
            _bool_int(work_order["ai_generated"]),
            work_order["ai_confidence"],
        ),
    )


def list_reliability_work_orders_v1(filters: dict[str, Any] | None = None) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    filters = filters or {}
    with reliability_connection_v1(db_path) as conn:
        where: list[str] = []
        params: list[Any] = []
        issue_filter = _coerce_text(filters.get("issue_id"))
        if issue_filter:
            canonical_issue_id = _resolve_issue_id_v1(conn, issue_filter, required=False)
            if not canonical_issue_id:
                return {"ok": True, "work_orders": [], "total_count": 0}
            where.append("issue_id = ?")
            params.append(canonical_issue_id)
        for key in ("status", "assigned_agent", "created_by"):
            value = _coerce_text(filters.get(key))
            if value:
                where.append(f"{key} = ?")
                params.append(value)
        sql = "SELECT * FROM reliability_work_orders"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC, created_at DESC"
        rows = conn.execute(sql, tuple(params)).fetchall()
    work_orders = [_row_to_work_order(row) for row in rows]
    return {"ok": True, "work_orders": work_orders, "total_count": len(work_orders)}


def list_reliability_issue_work_orders_v1(issue_id: str) -> dict[str, Any]:
    normalized_issue_id = _require_text("ISSUE_ID", issue_id)
    return list_reliability_work_orders_v1({"issue_id": normalized_issue_id})


def create_reliability_work_order_v1(payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_issue_id = _require_text("ISSUE_ID", payload.get("issue_id"))
    with reliability_connection_v1(db_path) as conn:
        canonical_issue_id = _resolve_issue_id_v1(conn, normalized_issue_id, required=True)
        issue = _row_to_issue(_load_issue_row(conn, canonical_issue_id))
        work_order = _normalize_work_order_payload(issue, payload)
        _insert_work_order(conn, work_order)
        issue_status = issue["status"]
        if issue_status in {"open", "triaged"}:
            issue_status = "fix_proposed"
        updated_issue = _set_issue_state(
            conn,
            issue_id=canonical_issue_id,
            status=issue_status,
            codex_status="task_created",
        )
        created = _row_to_work_order(_load_work_order_row(conn, work_order["id"]))
    return {"ok": True, "work_order": created, "issue": updated_issue}


def create_reliability_issue_work_order_v1(issue_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = dict(payload or {})
    body["issue_id"] = _require_text("ISSUE_ID", issue_id)
    return create_reliability_work_order_v1(body)


def get_reliability_work_order_v1(work_order_id: str) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("WORK_ORDER_ID", work_order_id)
    with reliability_connection_v1(db_path) as conn:
        row = _load_work_order_row(conn, normalized_id)
        work_order = _row_to_work_order(row)
        issue = _row_to_issue(_load_issue_row(conn, work_order["issue_id"]))
        attempt_rows = conn.execute(
            "SELECT * FROM reliability_fix_attempts WHERE work_order_id = ? ORDER BY started_at DESC",
            (normalized_id,),
        ).fetchall()
        verification_rows = conn.execute(
            "SELECT * FROM reliability_verifications WHERE work_order_id = ? ORDER BY created_at DESC",
            (normalized_id,),
        ).fetchall()
    return {
        "ok": True,
        "work_order": work_order,
        "issue": issue,
        "fix_attempts": [_row_to_fix_attempt(item) for item in attempt_rows],
        "verifications": [_row_to_verification(item) for item in verification_rows],
        "codex_ready_prompt": _build_codex_work_order_prompt(issue, work_order),
    }


def update_reliability_work_order_v1(work_order_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("WORK_ORDER_ID", work_order_id)
    if not isinstance(patch, dict):
        raise ValueError("RELIABILITY_PATCH_BODY_MUST_BE_OBJECT")

    with reliability_connection_v1(db_path) as conn:
        current = _row_to_work_order(_load_work_order_row(conn, normalized_id))
        updates: dict[str, Any] = {}
        for key, value in patch.items():
            if key == "status":
                updates["status"] = _require_enum("WORK_ORDER_STATUS", value, WORK_ORDER_STATUSES)
            elif key in {"objective", "affected_component", "expected_behavior", "actual_behavior", "assigned_agent", "created_by"}:
                updates[key] = _coerce_text(value)
            elif key in {"constraints", "forbidden_changes", "likely_files", "required_tests", "verification_criteria"}:
                updates[key] = _json_canonical(_normalize_json_array(key.upper(), value))
            elif key == "ai_generated":
                updates[key] = _bool_int(_coerce_bool(value))
            elif key == "ai_confidence":
                updates[key] = _coerce_confidence(value)
        if not updates:
            return {"ok": True, "work_order": current}
        updates["updated_at"] = _utc_now_iso()
        set_sql = ", ".join(f"{key} = ?" for key in updates.keys())
        conn.execute(
            f"UPDATE reliability_work_orders SET {set_sql} WHERE id = ?",
            tuple(updates.values()) + (normalized_id,),
        )
        updated = _row_to_work_order(_load_work_order_row(conn, normalized_id))
        issue_status = None
        if updated["status"] == "in_progress":
            issue_status = "fix_in_progress"
        elif updated["status"] == "patch_submitted":
            issue_status = "fix_submitted"
        elif updated["status"] == "tests_passed":
            issue_status = "verification_pending"
        elif updated["status"] == "tests_failed":
            issue_status = "fix_in_progress"
        if issue_status:
            _set_issue_state(conn, issue_id=updated["issue_id"], status=issue_status)
    return {"ok": True, "work_order": updated}


def create_reliability_work_order_from_issue_v1(issue_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    db_path = ensure_reliability_schema_v1()
    normalized_issue_id = _require_text("ISSUE_ID", issue_id)
    with reliability_connection_v1(db_path) as conn:
        canonical_issue_id = _resolve_issue_id_v1(conn, normalized_issue_id, required=True)
        issue = _row_to_issue(_load_issue_row(conn, canonical_issue_id))
        linked_rows = conn.execute(
            "SELECT observation_id FROM reliability_issue_observations WHERE issue_id = ? ORDER BY created_at DESC",
            (canonical_issue_id,),
        ).fetchall()
        evidence_refs = [str(row["observation_id"]) for row in linked_rows]
        codex_task = _build_codex_task(issue, evidence_refs=evidence_refs)
        operator_instruction = _coerce_text(payload.get("operator_instruction"))
        work_order_payload = {
            "issue_id": canonical_issue_id,
            "status": "queued",
            "objective": operator_instruction or issue.get("impact_summary") or issue.get("title"),
            "affected_component": issue.get("category"),
            "expected_behavior": issue.get("expected_behavior"),
            "actual_behavior": issue.get("actual_behavior"),
            "constraints": codex_task.get("constraints", []),
            "forbidden_changes": codex_task.get("forbidden_changes", []),
            "likely_files": codex_task.get("likely_files", []),
            "required_tests": codex_task.get("required_tests", []),
            "verification_criteria": codex_task.get("verification_criteria", []),
            "assigned_agent": _coerce_text(payload.get("assigned_agent")),
            "created_by": "ai",
            "ai_generated": True,
            "ai_confidence": _coerce_confidence(payload.get("ai_confidence"), field_name="WORK_ORDER_AI_CONFIDENCE") or 0.78,
        }
        work_order = _normalize_work_order_payload(issue, work_order_payload)
        _insert_work_order(conn, work_order)
        issue_status = issue["status"]
        if issue_status in {"open", "triaged"}:
            issue_status = "fix_proposed"
        updated_issue = _set_issue_state(
            conn,
            issue_id=canonical_issue_id,
            status=issue_status,
            codex_status="task_created",
        )
        created = _row_to_work_order(_load_work_order_row(conn, work_order["id"]))
    return {"ok": True, "work_order": created, "issue": updated_issue}


def _normalize_fix_attempt_payload(work_order: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    status = _require_enum("FIX_ATTEMPT_STATUS", payload.get("status") or "started", FIX_ATTEMPT_STATUSES)
    started_at = _normalize_iso("STARTED_AT", payload.get("started_at"), default_now=True)
    completed_at = _coerce_text(payload.get("completed_at"))
    if not completed_at and status in {"patch_submitted", "tests_failed", "tests_passed", "abandoned", "rejected"}:
        completed_at = _utc_now_iso()
    normalized_completed = _normalize_iso("COMPLETED_AT", completed_at, default_now=False) if completed_at else None
    return {
        "id": _new_id("reliability_fix_attempt"),
        "work_order_id": work_order["id"],
        "issue_id": work_order["issue_id"],
        "started_at": started_at,
        "completed_at": normalized_completed,
        "status": status,
        "files_changed": _normalize_json_array("FILES_CHANGED", payload.get("files_changed")),
        "diff_ref": _coerce_text(payload.get("diff_ref")),
        "tests_run": _normalize_json_array("TESTS_RUN", payload.get("tests_run")),
        "test_results": _normalize_json_object("TEST_RESULTS", payload.get("test_results")),
        "codex_summary": _coerce_text(payload.get("codex_summary")),
        "risk_notes": _coerce_text(payload.get("risk_notes")),
        "created_by": _coerce_text(payload.get("created_by"), default="operator") or "operator",
    }


def _insert_fix_attempt(conn: sqlite3.Connection, attempt: dict[str, Any]) -> None:
    conn.execute(
        """
INSERT INTO reliability_fix_attempts (
    id, work_order_id, issue_id, started_at, completed_at, status,
    files_changed, diff_ref, tests_run, test_results, codex_summary,
    risk_notes, created_by
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
        (
            attempt["id"],
            attempt["work_order_id"],
            attempt["issue_id"],
            attempt["started_at"],
            attempt["completed_at"],
            attempt["status"],
            _json_canonical(attempt["files_changed"]),
            attempt["diff_ref"],
            _json_canonical(attempt["tests_run"]),
            _json_canonical(attempt["test_results"]),
            attempt["codex_summary"],
            attempt["risk_notes"],
            attempt["created_by"],
        ),
    )


def _apply_fix_attempt_transitions(
    conn: sqlite3.Connection,
    *,
    issue_id: str,
    work_order_id: str,
    attempt_status: str,
) -> dict[str, Any]:
    work_order_status = _work_order_status_from_fix_attempt_status(attempt_status)
    conn.execute(
        "UPDATE reliability_work_orders SET status = ?, updated_at = ? WHERE id = ?",
        (work_order_status, _utc_now_iso(), work_order_id),
    )
    codex_status = {
        "started": "in_progress",
        "patch_submitted": "patch_submitted",
        "tests_failed": "tests_failed",
        "tests_passed": "tests_passed",
        "abandoned": "task_created",
        "rejected": "task_created",
    }.get(attempt_status, "in_progress")
    issue_status = _issue_status_from_fix_attempt_status(attempt_status)
    return _set_issue_state(conn, issue_id=issue_id, status=issue_status, codex_status=codex_status)


def list_reliability_fix_attempts_v1(filters: dict[str, Any] | None = None) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    filters = filters or {}
    where: list[str] = []
    params: list[Any] = []
    for key in ("issue_id", "work_order_id", "status", "created_by"):
        value = _coerce_text(filters.get(key))
        if value:
            where.append(f"{key} = ?")
            params.append(value)
    sql = "SELECT * FROM reliability_fix_attempts"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY started_at DESC"
    with reliability_connection_v1(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    fix_attempts = [_row_to_fix_attempt(row) for row in rows]
    return {"ok": True, "fix_attempts": fix_attempts, "total_count": len(fix_attempts)}


def list_reliability_work_order_fix_attempts_v1(work_order_id: str) -> dict[str, Any]:
    normalized_work_order_id = _require_text("WORK_ORDER_ID", work_order_id)
    return list_reliability_fix_attempts_v1({"work_order_id": normalized_work_order_id})


def create_reliability_fix_attempt_v1(payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_work_order_id = _require_text("WORK_ORDER_ID", payload.get("work_order_id"))
    with reliability_connection_v1(db_path) as conn:
        work_order = _row_to_work_order(_load_work_order_row(conn, normalized_work_order_id))
        attempt = _normalize_fix_attempt_payload(work_order, payload)
        _insert_fix_attempt(conn, attempt)
        issue = _apply_fix_attempt_transitions(
            conn,
            issue_id=work_order["issue_id"],
            work_order_id=work_order["id"],
            attempt_status=attempt["status"],
        )
        created = _row_to_fix_attempt(_load_fix_attempt_row(conn, attempt["id"]))
        updated_work_order = _row_to_work_order(_load_work_order_row(conn, work_order["id"]))
    return {"ok": True, "fix_attempt": created, "work_order": updated_work_order, "issue": issue}


def create_reliability_work_order_fix_attempt_v1(work_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = dict(payload or {})
    body["work_order_id"] = _require_text("WORK_ORDER_ID", work_order_id)
    return create_reliability_fix_attempt_v1(body)


def record_reliability_fix_attempt_v1(work_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return create_reliability_work_order_fix_attempt_v1(work_order_id, payload)


def get_reliability_fix_attempt_v1(fix_attempt_id: str) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("FIX_ATTEMPT_ID", fix_attempt_id)
    with reliability_connection_v1(db_path) as conn:
        attempt = _row_to_fix_attempt(_load_fix_attempt_row(conn, normalized_id))
        work_order = _row_to_work_order(_load_work_order_row(conn, attempt["work_order_id"]))
        issue = _row_to_issue(_load_issue_row(conn, attempt["issue_id"]))
    return {"ok": True, "fix_attempt": attempt, "work_order": work_order, "issue": issue}


def update_reliability_fix_attempt_v1(fix_attempt_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("FIX_ATTEMPT_ID", fix_attempt_id)
    if not isinstance(patch, dict):
        raise ValueError("RELIABILITY_PATCH_BODY_MUST_BE_OBJECT")
    with reliability_connection_v1(db_path) as conn:
        current = _row_to_fix_attempt(_load_fix_attempt_row(conn, normalized_id))
        updates: dict[str, Any] = {}
        for key, value in patch.items():
            if key == "status":
                updates[key] = _require_enum("FIX_ATTEMPT_STATUS", value, FIX_ATTEMPT_STATUSES)
            elif key in {"diff_ref", "codex_summary", "risk_notes", "created_by"}:
                updates[key] = _coerce_text(value)
            elif key == "files_changed":
                updates[key] = _json_canonical(_normalize_json_array("FILES_CHANGED", value))
            elif key == "tests_run":
                updates[key] = _json_canonical(_normalize_json_array("TESTS_RUN", value))
            elif key == "test_results":
                updates[key] = _json_canonical(_normalize_json_object("TEST_RESULTS", value))
            elif key in {"started_at", "completed_at"}:
                updates[key] = _normalize_iso(key.upper(), value, default_now=False) if _coerce_text(value) else None
        if not updates:
            return {"ok": True, "fix_attempt": current}
        set_sql = ", ".join(f"{key} = ?" for key in updates.keys())
        conn.execute(
            f"UPDATE reliability_fix_attempts SET {set_sql} WHERE id = ?",
            tuple(updates.values()) + (normalized_id,),
        )
        updated = _row_to_fix_attempt(_load_fix_attempt_row(conn, normalized_id))
        issue = None
        work_order = None
        if "status" in updates:
            issue = _apply_fix_attempt_transitions(
                conn,
                issue_id=updated["issue_id"],
                work_order_id=updated["work_order_id"],
                attempt_status=updated["status"],
            )
            work_order = _row_to_work_order(_load_work_order_row(conn, updated["work_order_id"]))
    result = {"ok": True, "fix_attempt": updated}
    if issue is not None:
        result["issue"] = issue
    if work_order is not None:
        result["work_order"] = work_order
    return result


def _normalize_verification_payload(payload: dict[str, Any]) -> dict[str, Any]:
    verification_status = _require_enum("VERIFICATION_STATUS", payload.get("status") or "pending", VERIFICATION_STATUSES)
    verified_at = _coerce_text(payload.get("verified_at"))
    if not verified_at and verification_status in {"passed", "failed", "inconclusive"}:
        verified_at = _utc_now_iso()
    return {
        "id": _new_id("reliability_verification"),
        "issue_id": _require_text("ISSUE_ID", payload.get("issue_id")),
        "work_order_id": _coerce_text(payload.get("work_order_id")) or None,
        "fix_attempt_id": _coerce_text(payload.get("fix_attempt_id")) or None,
        "created_at": _utc_now_iso(),
        "method": _require_enum("VERIFICATION_METHOD", payload.get("method"), VERIFICATION_METHODS),
        "status": verification_status,
        "evidence": _normalize_json_object("EVIDENCE", payload.get("evidence")),
        "verified_by": _coerce_text(payload.get("verified_by"), default="operator") or "operator",
        "verified_at": _normalize_iso("VERIFIED_AT", verified_at, default_now=False) if verified_at else None,
        "notes": _coerce_text(payload.get("notes")),
    }


def _insert_verification(conn: sqlite3.Connection, verification: dict[str, Any]) -> None:
    conn.execute(
        """
INSERT INTO reliability_verifications (
    id, issue_id, work_order_id, fix_attempt_id, created_at, method, status,
    evidence, verified_by, verified_at, notes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
        (
            verification["id"],
            verification["issue_id"],
            verification["work_order_id"],
            verification["fix_attempt_id"],
            verification["created_at"],
            verification["method"],
            verification["status"],
            _json_canonical(verification["evidence"]),
            verification["verified_by"],
            verification["verified_at"],
            verification["notes"],
        ),
    )


def _issue_status_for_failed_verification(conn: sqlite3.Connection, issue_id: str) -> str:
    active = conn.execute(
        """
SELECT COUNT(*) AS count
FROM reliability_work_orders
WHERE issue_id = ?
  AND status IN ('in_progress', 'patch_submitted', 'tests_failed', 'tests_passed', 'needs_review')
""".strip(),
        (issue_id,),
    ).fetchone()
    if int(active["count"]) > 0:
        return "fix_in_progress"
    return "open"


def _apply_verification_transitions(conn: sqlite3.Connection, verification: dict[str, Any]) -> dict[str, Any]:
    issue_id = verification["issue_id"]
    status = verification["status"]
    if status == "passed":
        return _set_issue_state(
            conn,
            issue_id=issue_id,
            status="verified",
            codex_status="verification_pending",
            verification_method=verification["method"],
            verified_at=verification["verified_at"] or _utc_now_iso(),
        )
    if status == "failed":
        fallback_status = _issue_status_for_failed_verification(conn, issue_id)
        return _set_issue_state(
            conn,
            issue_id=issue_id,
            status=fallback_status,
            codex_status="tests_failed",
        )
    if status in {"pending", "inconclusive"}:
        issue = _row_to_issue(_load_issue_row(conn, issue_id))
        if issue["status"] in {"fix_submitted", "fix_in_progress"}:
            return _set_issue_state(conn, issue_id=issue_id, status="verification_pending", codex_status="verification_pending")
    return _row_to_issue(_load_issue_row(conn, issue_id))


def list_reliability_verifications_v1(filters: dict[str, Any] | None = None) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    filters = filters or {}
    where: list[str] = []
    params: list[Any] = []
    for key in ("issue_id", "work_order_id", "fix_attempt_id", "method", "status", "verified_by"):
        value = _coerce_text(filters.get(key))
        if value:
            where.append(f"{key} = ?")
            params.append(value)
    sql = "SELECT * FROM reliability_verifications"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC"
    with reliability_connection_v1(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    verifications = [_row_to_verification(row) for row in rows]
    return {"ok": True, "verifications": verifications, "total_count": len(verifications)}


def list_reliability_issue_verifications_v1(issue_id: str) -> dict[str, Any]:
    normalized_issue_id = _require_text("ISSUE_ID", issue_id)
    return list_reliability_verifications_v1({"issue_id": normalized_issue_id})


def create_reliability_verification_v1(payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    with reliability_connection_v1(db_path) as conn:
        verification = _normalize_verification_payload(payload)
        _load_issue_row(conn, verification["issue_id"])
        if verification["work_order_id"]:
            work_order = _row_to_work_order(_load_work_order_row(conn, verification["work_order_id"]))
            if work_order["issue_id"] != verification["issue_id"]:
                raise ValueError("RELIABILITY_WORK_ORDER_ISSUE_MISMATCH")
        if verification["fix_attempt_id"]:
            attempt = _row_to_fix_attempt(_load_fix_attempt_row(conn, verification["fix_attempt_id"]))
            if attempt["issue_id"] != verification["issue_id"]:
                raise ValueError("RELIABILITY_FIX_ATTEMPT_ISSUE_MISMATCH")
            if verification["work_order_id"] and attempt["work_order_id"] != verification["work_order_id"]:
                raise ValueError("RELIABILITY_FIX_ATTEMPT_WORK_ORDER_MISMATCH")
        _insert_verification(conn, verification)
        issue = _apply_verification_transitions(conn, verification)
        created = _row_to_verification(_load_verification_row(conn, verification["id"]))
    return {"ok": True, "verification": created, "issue": issue}


def create_reliability_issue_verification_v1(issue_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = dict(payload or {})
    body["issue_id"] = _require_text("ISSUE_ID", issue_id)
    return create_reliability_verification_v1(body)


def verify_reliability_issue_v1(issue_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return create_reliability_issue_verification_v1(issue_id, payload)


def get_reliability_verification_v1(verification_id: str) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("VERIFICATION_ID", verification_id)
    with reliability_connection_v1(db_path) as conn:
        verification = _row_to_verification(_load_verification_row(conn, normalized_id))
        issue = _row_to_issue(_load_issue_row(conn, verification["issue_id"]))
    return {"ok": True, "verification": verification, "issue": issue}


def update_reliability_verification_v1(verification_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("VERIFICATION_ID", verification_id)
    if not isinstance(patch, dict):
        raise ValueError("RELIABILITY_PATCH_BODY_MUST_BE_OBJECT")
    with reliability_connection_v1(db_path) as conn:
        current = _row_to_verification(_load_verification_row(conn, normalized_id))
        updates: dict[str, Any] = {}
        for key, value in patch.items():
            if key == "method":
                updates[key] = _require_enum("VERIFICATION_METHOD", value, VERIFICATION_METHODS)
            elif key == "status":
                updates[key] = _require_enum("VERIFICATION_STATUS", value, VERIFICATION_STATUSES)
            elif key == "evidence":
                updates[key] = _json_canonical(_normalize_json_object("EVIDENCE", value))
            elif key == "verified_by":
                updates[key] = _coerce_text(value)
            elif key == "verified_at":
                updates[key] = _normalize_iso("VERIFIED_AT", value, default_now=False) if _coerce_text(value) else None
            elif key == "notes":
                updates[key] = _coerce_text(value)
        if not updates:
            return {"ok": True, "verification": current}
        set_sql = ", ".join(f"{key} = ?" for key in updates.keys())
        conn.execute(
            f"UPDATE reliability_verifications SET {set_sql} WHERE id = ?",
            tuple(updates.values()) + (normalized_id,),
        )
        updated = _row_to_verification(_load_verification_row(conn, normalized_id))
        issue = None
        if "status" in updates or "method" in updates:
            issue = _apply_verification_transitions(conn, updated)
    result = {"ok": True, "verification": updated}
    if issue is not None:
        result["issue"] = issue
    return result


def list_reliability_next_actions_v1() -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    actions: list[dict[str, Any]] = []
    with reliability_connection_v1(db_path) as conn:
        issue_rows = conn.execute("SELECT * FROM reliability_issues ORDER BY updated_at DESC").fetchall()
        work_order_rows = conn.execute("SELECT * FROM reliability_work_orders ORDER BY updated_at DESC").fetchall()
        fix_rows = conn.execute("SELECT * FROM reliability_fix_attempts ORDER BY started_at DESC").fetchall()
        verification_rows = conn.execute("SELECT * FROM reliability_verifications ORDER BY created_at DESC").fetchall()

    issues = [_row_to_issue(row) for row in issue_rows]
    work_orders = [_row_to_work_order(row) for row in work_order_rows]
    fix_attempts = [_row_to_fix_attempt(row) for row in fix_rows]
    verifications = [_row_to_verification(row) for row in verification_rows]

    work_orders_by_issue: dict[str, list[dict[str, Any]]] = {}
    for row in work_orders:
        work_orders_by_issue.setdefault(row["issue_id"], []).append(row)
    fix_by_work_order: dict[str, list[dict[str, Any]]] = {}
    for row in fix_attempts:
        fix_by_work_order.setdefault(row["work_order_id"], []).append(row)
    passed_verifications_by_issue: dict[str, list[dict[str, Any]]] = {}
    for row in verifications:
        if row["status"] == "passed":
            passed_verifications_by_issue.setdefault(row["issue_id"], []).append(row)

    for issue in issues:
        if issue["status"] in OPEN_ISSUE_STATUSES and issue["readiness_blocker"] and not work_orders_by_issue.get(issue["id"]):
            actions.append(
                {
                    "priority": 10,
                    "action_type": "create_work_order",
                    "issue_id": issue["id"],
                    "title": issue["title"],
                    "reason": "Open readiness blocker has no work order.",
                }
            )

    for work_order in work_orders:
        if work_order["status"] == "queued":
            actions.append(
                {
                    "priority": 20,
                    "action_type": "start_fix_attempt",
                    "work_order_id": work_order["id"],
                    "issue_id": work_order["issue_id"],
                    "reason": "Queued work order is awaiting first fix attempt.",
                }
            )

    for attempt in fix_attempts:
        if attempt["status"] == "tests_failed":
            actions.append(
                {
                    "priority": 30,
                    "action_type": "investigate_tests_failed",
                    "fix_attempt_id": attempt["id"],
                    "work_order_id": attempt["work_order_id"],
                    "issue_id": attempt["issue_id"],
                    "reason": "Fix attempt tests failed and issue remains unresolved.",
                }
            )
        if attempt["status"] in {"tests_passed", "patch_submitted"} and not passed_verifications_by_issue.get(attempt["issue_id"]):
            actions.append(
                {
                    "priority": 40,
                    "action_type": "record_verification",
                    "fix_attempt_id": attempt["id"],
                    "work_order_id": attempt["work_order_id"],
                    "issue_id": attempt["issue_id"],
                    "reason": "Fix attempt is awaiting verification evidence.",
                }
            )

    for issue in issues:
        if issue["status"] == "verified":
            actions.append(
                {
                    "priority": 50,
                    "action_type": "close_verified_issue",
                    "issue_id": issue["id"],
                    "reason": "Issue is verified and ready for explicit closure decision.",
                }
            )
        if issue["status"] in OPEN_ISSUE_STATUSES and int(issue.get("occurrence_count") or 0) > 1:
            actions.append(
                {
                    "priority": 60,
                    "action_type": "triage_recurring_issue",
                    "issue_id": issue["id"],
                    "reason": "Issue recurred and should be re-triaged.",
                }
            )

    actions.sort(key=lambda item: (int(item.get("priority") or 999), str(item.get("issue_id") or ""), str(item.get("work_order_id") or "")))
    return {"ok": True, "generated_at": _utc_now_iso(), "actions": actions}


def link_reliability_issue_observation_v1(issue_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_issue_id = _require_text("ISSUE_ID", issue_id)
    observation_id = _require_text("OBSERVATION_ID", payload.get("observation_id"))
    link_type = _require_enum("LINK_TYPE", payload.get("link_type") or "related", LINK_TYPES)
    now = _utc_now_iso()

    with reliability_connection_v1(db_path) as conn:
        _load_issue_row(conn, normalized_issue_id)
        obs = conn.execute(
            "SELECT id, observed_at FROM reliability_observations WHERE id = ?",
            (observation_id,),
        ).fetchone()
        if obs is None:
            raise ValueError("RELIABILITY_OBSERVATION_NOT_FOUND")

        conn.execute(
            """
INSERT OR IGNORE INTO reliability_issue_observations (issue_id, observation_id, link_type, created_at)
VALUES (?, ?, ?, ?)
""".strip(),
            (normalized_issue_id, observation_id, link_type, now),
        )

        if link_type == "recurrence":
            conn.execute(
                """
UPDATE reliability_issues
SET occurrence_count = occurrence_count + 1,
    last_seen_at = CASE WHEN last_seen_at < ? THEN ? ELSE last_seen_at END,
    updated_at = ?
WHERE id = ?
""".strip(),
                (obs["observed_at"], obs["observed_at"], now, normalized_issue_id),
            )

        issue_row = _load_issue_row(conn, normalized_issue_id)

    return {
        "ok": True,
        "link": {
            "issue_id": normalized_issue_id,
            "observation_id": observation_id,
            "link_type": link_type,
            "created_at": now,
        },
        "issue": _row_to_issue(issue_row),
    }


def _draft_type_from_text(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ("missing", "not implemented", "todo")):
        return "missing_functionality"
    if "regression" in lowered:
        return "regression"
    if any(token in lowered for token in ("paper incident", "paper-trading", "paper trading incident")):
        return "paper_trading_incident"
    if any(token in lowered for token in ("blocker", "cannot trade", "not ready")):
        return "readiness_blocker"
    return "bug"


def _draft_category_from_text(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ("order", "execution", "routing", "fill", "submit")):
        return "execution"
    if any(token in lowered for token in ("data", "snapshot", "feed", "stale", "missing field")):
        return "data"
    if any(token in lowered for token in ("ui", "screen", "button", "layout", "dashboard")):
        return "ui_missing_functionality"
    if any(token in lowered for token in ("strategy", "signal", "alpha", "model")):
        return "strategy"
    return "infrastructure"


def _draft_severity_from_text(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ("critical", "sev0", "unsafe", "loss", "catastrophic")):
        return "critical"
    if any(token in lowered for token in ("high", "sev1", "order correctness", "wrong order")):
        return "high"
    if any(token in lowered for token in ("medium", "sev2")):
        return "medium"
    return "low"


def _draft_confidence(operator_message: str, observation_count: int) -> float:
    base = 0.35
    if operator_message.strip():
        base += 0.2
    if observation_count > 0:
        base += min(0.35, observation_count * 0.05)
    return round(min(0.95, base), 2)


def draft_reliability_issue_v1(payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    operator_message = _coerce_text(payload.get("operator_message"))
    observation_ids = payload.get("observation_ids") if isinstance(payload.get("observation_ids"), list) else []
    raw_log_refs = payload.get("raw_log_refs") if isinstance(payload.get("raw_log_refs"), list) else []
    environment = _coerce_text(payload.get("environment"), default="PAPER") or "PAPER"
    component = _coerce_text(payload.get("component"), default="unspecified") or "unspecified"

    if not operator_message and not observation_ids:
        raise ValueError("RELIABILITY_OPERATOR_MESSAGE_OR_OBSERVATIONS_REQUIRED")

    inference_text = operator_message or "operator_report"
    issue_type = _draft_type_from_text(inference_text)
    category = _draft_category_from_text(inference_text)
    severity = _draft_severity_from_text(inference_text)
    readiness_blocker = issue_type == "readiness_blocker" or severity == "critical" or (
        severity == "high" and category in {"execution", "data"}
    )

    confidence = _draft_confidence(operator_message, len(observation_ids))
    canonical_seed = f"{component}-{issue_type}-{operator_message[:120]}"
    canonical_key = _slugify(canonical_seed)

    draft_issue = {
        "title": _coerce_text(payload.get("title"), default=(operator_message[:96] or "Reliability issue draft")),
        "type": issue_type,
        "category": category,
        "severity": severity,
        "canonical_key": canonical_key,
        "expected_behavior": _coerce_text(
            payload.get("expected_behavior"),
            default="System should preserve deterministic, fail-closed operator behavior without reliability regressions.",
        ),
        "actual_behavior": operator_message or "Issue reported without detailed narrative.",
        "impact_summary": _coerce_text(
            payload.get("impact_summary"),
            default="Observed reliability risk requires triage before trusting live capital deployment.",
        ),
        "readiness_blocker": readiness_blocker,
        "confidence": confidence,
        "suggested_codex_task": {
            "issue_id": "pending_create",
            "title": _coerce_text(payload.get("title"), default=(operator_message[:96] or "Reliability issue draft")),
            "affected_component": component,
            "actual_behavior": operator_message or "Issue reported without detailed narrative.",
            "expected_behavior": _coerce_text(
                payload.get("expected_behavior"),
                default="System should preserve deterministic, fail-closed operator behavior without reliability regressions.",
            ),
            "evidence_refs": list(observation_ids) + [str(item) for item in raw_log_refs],
            "constraints": [
                "No changes to strategy/execution/capital logic.",
                "Reliability module remains observational + administrative.",
            ],
            "likely_files": [
                "constellation_2/phaseL/ui_api/reliability_ledger_v1.py",
                "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py",
            ],
            "required_tests": [
                "Readiness rule evaluation tests",
                "Issue lifecycle + recurrence tests",
            ],
            "verification_criteria": [
                "Fix has evidence-backed verification before issue closure.",
                "Readiness decision rationale remains deterministic.",
            ],
            "forbidden_changes": [
                "No mutation of trading strategy decisions.",
                "No order routing/sizing behavior changes.",
            ],
        },
        "linked_observation_ids": list(observation_ids),
    }

    if not _coerce_bool(payload.get("create"), default=False):
        return {"ok": True, "draft_issue": draft_issue}

    created_observation_ids = list(observation_ids)
    if not created_observation_ids and not raw_log_refs and operator_message:
        observation = create_reliability_observation_v1(
            {
                "observed_at": _utc_now_iso(),
                "source": "operator_chat",
                "environment": environment,
                "run_id": _coerce_text(payload.get("run_id")),
                "component": component,
                "event_type": "operator_report",
                "severity_hint": severity,
                "summary": operator_message[:200],
                "raw_ref": "",
                "structured_payload": {
                    "operator_message": operator_message,
                    "raw_log_refs": raw_log_refs,
                },
                "ai_detected": True,
            }
        )
        created_observation_ids.append(observation["id"])

    issue_payload = {
        "title": draft_issue["title"],
        "type": draft_issue["type"],
        "category": draft_issue["category"],
        "severity": draft_issue["severity"],
        "status": "open",
        "canonical_key": draft_issue["canonical_key"],
        "readiness_blocker": draft_issue["readiness_blocker"],
        "impact_summary": draft_issue["impact_summary"],
        "expected_behavior": draft_issue["expected_behavior"],
        "actual_behavior": draft_issue["actual_behavior"],
        "created_by": "ai",
        "ai_assisted": True,
        "ai_confidence": draft_issue["confidence"],
        "codex_status": "task_created",
        "observation_ids": created_observation_ids,
    }
    created_issue = create_reliability_issue_v1(issue_payload)
    issue_id = created_issue["issue"]["id"]

    if created_observation_ids:
        with reliability_connection_v1(db_path) as conn:
            _link_issue_observations(
                conn,
                issue_id=issue_id,
                observation_ids=created_observation_ids,
                link_type="primary_evidence",
            )

    issue_detail = get_reliability_issue_v1(issue_id)
    draft_issue["linked_observation_ids"] = created_observation_ids
    draft_issue["suggested_codex_task"] = _build_codex_task(
        issue_detail["issue"],
        evidence_refs=created_observation_ids,
    )

    return {
        "ok": True,
        "draft_issue": draft_issue,
        "created": True,
        "issue": issue_detail["issue"],
        "linked_observation_ids": created_observation_ids,
    }


def _is_order_correctness_incident(issue: dict[str, Any]) -> bool:
    combined = f"{issue.get('impact_summary', '')} {issue.get('actual_behavior', '')}".lower()
    return any(token in combined for token in ("order correctness", "wrong order", "execution safety", "incorrect order"))


def _is_unresolved(issue: dict[str, Any]) -> bool:
    return issue.get("status") in OPEN_ISSUE_STATUSES


def _collect_rule(
    *,
    rule_id: str,
    rule_name: str,
    passed: bool,
    details: dict[str, Any],
    blocking_issue_ids: list[str],
) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "rule_name": rule_name,
        "passed": bool(passed),
        "details": details,
        "blocking_issue_ids": list(dict.fromkeys(blocking_issue_ids)),
    }


def _summarize_issue_workflow(
    *,
    issues: list[dict[str, Any]],
    work_orders: list[dict[str, Any]],
    fix_attempts: list[dict[str, Any]],
    verifications: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for issue in issues:
        summary[str(issue["id"])] = {
            "work_order_count": 0,
            "fix_attempt_count": 0,
            "passed_verification_count": 0,
            "failed_verification_count": 0,
            "latest_fix_attempt_at": None,
            "latest_failed_verification_at": None,
            "failed_verification_after_latest_fix": False,
        }

    for work_order in work_orders:
        issue_id = str(work_order.get("issue_id") or "")
        if issue_id not in summary:
            continue
        summary[issue_id]["work_order_count"] += 1

    for attempt in fix_attempts:
        issue_id = str(attempt.get("issue_id") or "")
        if issue_id not in summary:
            continue
        summary[issue_id]["fix_attempt_count"] += 1
        marker = _parse_iso(attempt.get("completed_at") or attempt.get("started_at"))
        latest = summary[issue_id]["latest_fix_attempt_at"]
        if marker is not None and (latest is None or marker > latest):
            summary[issue_id]["latest_fix_attempt_at"] = marker

    for verification in verifications:
        issue_id = str(verification.get("issue_id") or "")
        if issue_id not in summary:
            continue
        status = str(verification.get("status") or "")
        created = _parse_iso(verification.get("created_at"))
        if status == "passed":
            summary[issue_id]["passed_verification_count"] += 1
        if status == "failed":
            summary[issue_id]["failed_verification_count"] += 1
            latest_failed = summary[issue_id]["latest_failed_verification_at"]
            if created is not None and (latest_failed is None or created > latest_failed):
                summary[issue_id]["latest_failed_verification_at"] = created

    for issue_id, row in summary.items():
        latest_fix = row["latest_fix_attempt_at"]
        latest_failed = row["latest_failed_verification_at"]
        row["failed_verification_after_latest_fix"] = (
            latest_fix is not None and latest_failed is not None and latest_failed > latest_fix
        )
    return summary


def _evaluate_readiness_rules(
    *,
    issues: list[dict[str, Any]],
    workflow_summary: dict[str, dict[str, Any]],
    period_start: str,
    period_end: str,
    severe_observation_count: int,
) -> tuple[str, int, str, list[dict[str, Any]], list[str]]:
    open_critical = [i for i in issues if _is_unresolved(i) and i["severity"] == "critical"]
    open_high_execution = [i for i in issues if _is_unresolved(i) and i["severity"] == "high" and i["category"] == "execution"]
    open_high_data = [i for i in issues if _is_unresolved(i) and i["severity"] == "high" and i["category"] == "data"]
    open_readiness_blockers = [i for i in issues if _is_unresolved(i) and bool(i["readiness_blocker"])]
    blockers_without_work_order = [
        i for i in open_readiness_blockers if int((workflow_summary.get(i["id"]) or {}).get("work_order_count") or 0) == 0
    ]
    blockers_without_fix_attempt = [
        i
        for i in open_readiness_blockers
        if int((workflow_summary.get(i["id"]) or {}).get("work_order_count") or 0) > 0
        and int((workflow_summary.get(i["id"]) or {}).get("fix_attempt_count") or 0) == 0
    ]
    blockers_without_passed_verification = [
        i
        for i in open_readiness_blockers
        if int((workflow_summary.get(i["id"]) or {}).get("fix_attempt_count") or 0) > 0
        and int((workflow_summary.get(i["id"]) or {}).get("passed_verification_count") or 0) == 0
    ]
    fix_submitted_without_verification = [
        i
        for i in issues
        if i["status"] == "fix_submitted"
        and int((workflow_summary.get(i["id"]) or {}).get("passed_verification_count") or 0) == 0
    ]
    failed_after_latest_fix = [
        i for i in issues if bool((workflow_summary.get(i["id"]) or {}).get("failed_verification_after_latest_fix"))
    ]

    start_dt = _parse_iso(period_start) or datetime.now(UTC) - timedelta(days=14)
    end_dt = _parse_iso(period_end) or datetime.now(UTC)
    recurred_after_verification = []
    for issue in issues:
        verified_marker = _parse_iso(issue.get("verified_at"))
        last_seen = _parse_iso(issue.get("last_seen_at"))
        if verified_marker is None or last_seen is None:
            continue
        if last_seen > verified_marker and start_dt <= last_seen <= end_dt:
            recurred_after_verification.append(issue)

    rule_results = [
        _collect_rule(
            rule_id="R1_OPEN_CRITICAL",
            rule_name="No open critical issues",
            passed=len(open_critical) == 0,
            details={"open_critical_count": len(open_critical)},
            blocking_issue_ids=[item["id"] for item in open_critical],
        ),
        _collect_rule(
            rule_id="R2_OPEN_HIGH_EXECUTION",
            rule_name="No open high-severity execution issues",
            passed=len(open_high_execution) == 0,
            details={"open_high_execution_count": len(open_high_execution)},
            blocking_issue_ids=[item["id"] for item in open_high_execution],
        ),
        _collect_rule(
            rule_id="R3_OPEN_HIGH_DATA",
            rule_name="No open high-severity data issues",
            passed=len(open_high_data) == 0,
            details={"open_high_data_count": len(open_high_data)},
            blocking_issue_ids=[item["id"] for item in open_high_data],
        ),
        _collect_rule(
            rule_id="R4_OPEN_READINESS_BLOCKERS",
            rule_name="No open readiness blockers",
            passed=len(open_readiness_blockers) == 0,
            details={"open_readiness_blocker_count": len(open_readiness_blockers)},
            blocking_issue_ids=[item["id"] for item in open_readiness_blockers],
        ),
        _collect_rule(
            rule_id="R5_BLOCKER_WORK_ORDER_REQUIRED",
            rule_name="Open readiness blockers must have work orders",
            passed=len(blockers_without_work_order) == 0,
            details={"blocker_without_work_order_count": len(blockers_without_work_order)},
            blocking_issue_ids=[item["id"] for item in blockers_without_work_order],
        ),
        _collect_rule(
            rule_id="R6_BLOCKER_FIX_ATTEMPT_REQUIRED",
            rule_name="Open readiness blockers with work orders must have fix attempts",
            passed=len(blockers_without_fix_attempt) == 0,
            details={"blocker_without_fix_attempt_count": len(blockers_without_fix_attempt)},
            blocking_issue_ids=[item["id"] for item in blockers_without_fix_attempt],
        ),
        _collect_rule(
            rule_id="R7_BLOCKER_VERIFICATION_REQUIRED",
            rule_name="Open readiness blockers with fix attempts require passed verification",
            passed=len(blockers_without_passed_verification) == 0,
            details={"blocker_without_passed_verification_count": len(blockers_without_passed_verification)},
            blocking_issue_ids=[item["id"] for item in blockers_without_passed_verification],
        ),
        _collect_rule(
            rule_id="R8_FIX_SUBMITTED_REQUIRES_VERIFICATION",
            rule_name="Fix submitted status does not imply verification",
            passed=len(fix_submitted_without_verification) == 0,
            details={"fix_submitted_without_verification_count": len(fix_submitted_without_verification)},
            blocking_issue_ids=[item["id"] for item in fix_submitted_without_verification],
        ),
        _collect_rule(
            rule_id="R9_RECURRENCE_AFTER_VERIFICATION",
            rule_name="Issue must not recur after verification",
            passed=len(recurred_after_verification) == 0,
            details={"recurrence_after_verification_count": len(recurred_after_verification), "period_start": period_start, "period_end": period_end},
            blocking_issue_ids=[item["id"] for item in recurred_after_verification],
        ),
        _collect_rule(
            rule_id="R10_FAILED_VERIFICATION_AFTER_LATEST_FIX",
            rule_name="No failed verification after latest fix attempt",
            passed=len(failed_after_latest_fix) == 0,
            details={"failed_verification_after_latest_fix_count": len(failed_after_latest_fix)},
            blocking_issue_ids=[item["id"] for item in failed_after_latest_fix],
        ),
    ]

    blocking_issue_ids: list[str] = []
    for result in rule_results:
        if not result["passed"]:
            blocking_issue_ids.extend(result["blocking_issue_ids"])
    blocking_issue_ids = list(dict.fromkeys(blocking_issue_ids))

    if blocking_issue_ids:
        status = "not_ready"
        score = max(5, 30 - len(blocking_issue_ids) * 2)
        reason = "Blocking reliability rules failed: " + ", ".join(
            result["rule_name"]
            for result in rule_results
            if not result["passed"]
        )
        return status, score, reason, rule_results, blocking_issue_ids

    medium_low_non_execution_open = [
        i
        for i in issues
        if _is_unresolved(i)
        and i["severity"] in {"low", "medium"}
        and i["category"] not in {"execution", "data"}
    ]
    unresolved_recurring_failures = [
        i
        for i in issues
        if _is_unresolved(i) and int(i.get("occurrence_count") or 0) > 1
    ]

    last_blocker_verified_dt = None
    for issue in issues:
        if issue["readiness_blocker"]:
            verified = _parse_iso(issue.get("verified_at"))
            if verified is not None and (last_blocker_verified_dt is None or verified > last_blocker_verified_dt):
                last_blocker_verified_dt = verified

    clean_window_met = False
    clean_window_started = False
    if last_blocker_verified_dt is not None:
        clean_window_started = True
        clean_window_met = (end_dt - last_blocker_verified_dt) >= timedelta(days=CLEAN_WINDOW_DAYS_REQUIRED)

    all_blockers_verified = True
    for issue in issues:
        if not issue["readiness_blocker"]:
            continue
        if int((workflow_summary.get(issue["id"]) or {}).get("passed_verification_count") or 0) == 0:
            all_blockers_verified = False
            break

    if (
        len(open_critical) == 0
        and len(open_high_execution) == 0
        and len(open_high_data) == 0
        and len(open_readiness_blockers) == 0
        and all_blockers_verified
        and len(failed_after_latest_fix) == 0
        and len(recurred_after_verification) == 0
        and len(medium_low_non_execution_open) == 0
        and len(unresolved_recurring_failures) == 0
        and severe_observation_count == 0
        and clean_window_met
    ):
        status = "ready"
        score = 95
        reason = "All blocking rules pass, fixes are verified, and clean observation window has aged sufficiently."
        return status, score, reason, rule_results, []

    status = "conditionally_ready"
    score = max(58, 80 - (len(medium_low_non_execution_open) * 3) - (severe_observation_count * 5))
    if clean_window_started and not clean_window_met:
        reason = "All blocker fixes are verified, but clean observation window has started and is still aging."
    else:
        reason = "No critical/high execution/data blockers remain, but residual non-blocking reliability risk still requires monitoring."
    return status, score, reason, rule_results, []


def assess_reliability_readiness_v1(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    db_path = ensure_reliability_schema_v1()
    created_at = _utc_now_iso()
    period_end = _normalize_iso("PERIOD_END", payload.get("period_end"), default_now=True)
    parsed_end = _parse_iso(period_end) or datetime.now(UTC)
    default_start = (parsed_end - timedelta(days=14)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    period_start = _normalize_iso("PERIOD_START", payload.get("period_start") or default_start, default_now=False)

    with reliability_connection_v1(db_path) as conn:
        issue_rows = conn.execute("SELECT * FROM reliability_issues ORDER BY updated_at DESC").fetchall()
        issues = [_row_to_issue(row) for row in issue_rows]
        work_order_rows = conn.execute("SELECT * FROM reliability_work_orders ORDER BY updated_at DESC").fetchall()
        fix_attempt_rows = conn.execute("SELECT * FROM reliability_fix_attempts ORDER BY started_at DESC").fetchall()
        verification_rows = conn.execute("SELECT * FROM reliability_verifications ORDER BY created_at DESC").fetchall()
        workflow_summary = _summarize_issue_workflow(
            issues=issues,
            work_orders=[_row_to_work_order(row) for row in work_order_rows],
            fix_attempts=[_row_to_fix_attempt(row) for row in fix_attempt_rows],
            verifications=[_row_to_verification(row) for row in verification_rows],
        )
        severe_observation_count = conn.execute(
            """
SELECT COUNT(*) AS count
FROM reliability_observations
WHERE observed_at >= ?
  AND observed_at <= ?
  AND LOWER(severity_hint) IN ('high', 'critical')
""".strip(),
            (period_start, period_end),
        ).fetchone()["count"]

        status, score, decision_reason, rule_results, blocking_issue_ids = _evaluate_readiness_rules(
            issues=issues,
            workflow_summary=workflow_summary,
            period_start=period_start,
            period_end=period_end,
            severe_observation_count=int(severe_observation_count),
        )

        assessment = {
            "id": _new_id("reliability_assessment"),
            "created_at": created_at,
            "period_start": period_start,
            "period_end": period_end,
            "status": status,
            "score": int(score),
            "decision_reason": decision_reason,
            "blocking_issue_ids": blocking_issue_ids,
            "rule_version": _coerce_text(payload.get("rule_version"), default="reliability_rules_v1"),
            "generated_by": _coerce_text(payload.get("generated_by"), default="operator") or "operator",
        }

        conn.execute(
            """
INSERT INTO readiness_assessments (
    id, created_at, period_start, period_end, status, score,
    decision_reason, blocking_issue_ids, rule_version, generated_by
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
            (
                assessment["id"],
                assessment["created_at"],
                assessment["period_start"],
                assessment["period_end"],
                assessment["status"],
                assessment["score"],
                assessment["decision_reason"],
                _json_canonical(assessment["blocking_issue_ids"]),
                assessment["rule_version"],
                assessment["generated_by"],
            ),
        )

        for result in rule_results:
            conn.execute(
                """
INSERT INTO readiness_rule_results (
    assessment_id, rule_id, rule_name, passed, details, blocking_issue_ids
) VALUES (?, ?, ?, ?, ?, ?)
""".strip(),
                (
                    assessment["id"],
                    result["rule_id"],
                    result["rule_name"],
                    _bool_int(bool(result["passed"])),
                    _json_canonical(result["details"]),
                    _json_canonical(result["blocking_issue_ids"]),
                ),
            )

    return {
        "ok": True,
        "assessment": assessment,
        "rule_results": rule_results,
    }


def _load_assessment_with_rules(conn: sqlite3.Connection, assessment_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM readiness_assessments WHERE id = ?",
        (assessment_id,),
    ).fetchone()
    if row is None:
        raise ValueError("RELIABILITY_ASSESSMENT_NOT_FOUND")
    assessment = _row_to_assessment(row)
    rule_rows = conn.execute(
        "SELECT * FROM readiness_rule_results WHERE assessment_id = ? ORDER BY rule_id",
        (assessment_id,),
    ).fetchall()
    return {
        "ok": True,
        "assessment": assessment,
        "rule_results": [_row_to_rule_result(rule_row) for rule_row in rule_rows],
    }


def get_latest_reliability_readiness_v1() -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    with reliability_connection_v1(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM readiness_assessments ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return {"ok": True, "assessment": None, "rule_results": []}
        return _load_assessment_with_rules(conn, str(row["id"]))


def get_reliability_readiness_v1(assessment_id: str) -> dict[str, Any]:
    db_path = ensure_reliability_schema_v1()
    normalized_id = _require_text("ASSESSMENT_ID", assessment_id)
    with reliability_connection_v1(db_path) as conn:
        return _load_assessment_with_rules(conn, normalized_id)
