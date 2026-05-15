from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_research_lab_v1 import (
    build_research_inbox_item_v1,
    write_research_lab_artifact_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
OPERATOR_INBOX_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_inbox.v1.schema.json"
OPERATOR_INBOX_REVIEW_REPORT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_inbox_review_report.v1.schema.json"
OPERATOR_INBOX_STATUSES = {
    "CAPTURED",
    "REVIEWED",
    "PROMOTED_TO_IDEA",
    "PROMOTED_TO_HYPOTHESIS",
    "ARCHIVED",
    "REJECTED",
}
OPERATOR_INBOX_CATEGORIES = {
    "RESEARCH_IDEA",
    "HYPOTHESIS_SEED",
    "SLEEVE_IMPROVEMENT",
    "EVENT_AWARENESS",
    "DATASET_GAP",
    "UI_IMPROVEMENT",
    "OPERATIONAL_FIX",
    "PERFORMANCE_REVIEW",
    "FUTURE_AUTOMATION",
    "OTHER",
}
OPEN_STATUSES = {"CAPTURED", "REVIEWED"}


def operator_inbox_item_path_v1(*, truth_root: Path, inbox_item_id: str) -> Path:
    return Path(truth_root).resolve() / "operator_inbox_v1" / _safe_id(inbox_item_id) / "operator_inbox.v1.json"


def operator_inbox_review_report_path_v1(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "operator_inbox_review_report_v1" / "latest" / "operator_inbox_review_report.v1.json"


def validate_operator_inbox_artifact_v1(payload: dict[str, Any]) -> None:
    schema_id = str(payload.get("schema_id") or "")
    if schema_id == "operator_inbox":
        validate_against_repo_schema_v1(payload, REPO_ROOT, OPERATOR_INBOX_SCHEMA)
        return
    if schema_id == "operator_inbox_review_report":
        validate_against_repo_schema_v1(payload, REPO_ROOT, OPERATOR_INBOX_REVIEW_REPORT_SCHEMA)
        return
    raise ValueError(f"UNSUPPORTED_OPERATOR_INBOX_SCHEMA:{schema_id}")


def write_operator_inbox_artifact_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_operator_inbox_artifact_v1(payload)
    if payload["schema_id"] == "operator_inbox":
        path = operator_inbox_item_path_v1(truth_root=truth_root, inbox_item_id=str(payload["inbox_item_id"]))
    else:
        path = operator_inbox_review_report_path_v1(truth_root=truth_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_operator_inbox_item_v1(
    *,
    inbox_item_id: str,
    title: str,
    description: str,
    category: str,
    source: str = "MANUAL",
    created_at: str,
    updated_at: str = "",
    status: str = "CAPTURED",
    priority: str = "NORMAL",
    tags: list[str] | str | None = None,
    related_artifact_refs: list[str] | str | None = None,
    promoted_to_research_idea_id: str = "",
    promoted_to_hypothesis_id: str = "",
    notes: str = "",
) -> dict[str, Any]:
    payload = {
        "schema_id": "operator_inbox",
        "schema_version": "v1",
        "artifact_id": "operator_inbox_v1",
        "inbox_item_id": inbox_item_id,
        "title": title,
        "description": description,
        "category": _enum(category, OPERATOR_INBOX_CATEGORIES, "category"),
        "source": source,
        "created_at": created_at,
        "updated_at": updated_at or created_at,
        "status": _enum(status, OPERATOR_INBOX_STATUSES, "status"),
        "priority": priority.upper(),
        "tags": _strings(tags),
        "related_artifact_refs": _strings(related_artifact_refs),
        "promoted_to_research_idea_id": promoted_to_research_idea_id,
        "promoted_to_hypothesis_id": promoted_to_hypothesis_id,
        "notes": notes,
        "operator_inbox_only": True,
        "research_task_creation_allowed": False,
        "trade_creation_allowed": False,
        "sleeve_creation_allowed": False,
        "lite_runtime_mutation_allowed": False,
        "production_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def capture_operator_inbox_item_v1(
    *,
    truth_root: Path,
    title: str,
    description: str,
    category: str,
    created_at: str,
    source: str = "MANUAL",
    priority: str = "NORMAL",
    tags: list[str] | str | None = None,
    related_artifact_refs: list[str] | str | None = None,
    notes: str = "",
) -> tuple[dict[str, Any], Path]:
    inbox_item_id = _operator_inbox_id(title=title, created_at=created_at)
    item = build_operator_inbox_item_v1(
        inbox_item_id=inbox_item_id,
        title=title,
        description=description,
        category=category,
        source=source,
        created_at=created_at,
        priority=priority,
        tags=tags,
        related_artifact_refs=related_artifact_refs,
        notes=notes,
    )
    return item, write_operator_inbox_artifact_v1(truth_root=truth_root, payload=item)


def list_operator_inbox_items_v1(*, truth_root: Path, include_closed: bool = False) -> list[dict[str, Any]]:
    root = Path(truth_root).resolve() / "operator_inbox_v1"
    rows = []
    for path in sorted(root.rglob("operator_inbox.v1.json")) if root.exists() else []:
        item = _read_json(path)
        if include_closed or str(item.get("status") or "") in OPEN_STATUSES:
            rows.append(item)
    return sorted(rows, key=lambda row: (str(row.get("status") or ""), str(row.get("priority") or ""), str(row.get("created_at") or ""), str(row.get("inbox_item_id") or "")))


def load_operator_inbox_item_v1(*, truth_root: Path, inbox_item_id: str) -> dict[str, Any]:
    path = operator_inbox_item_path_v1(truth_root=truth_root, inbox_item_id=inbox_item_id)
    if not path.exists():
        raise FileNotFoundError(f"OPERATOR_INBOX_ITEM_NOT_FOUND:{inbox_item_id}")
    return _read_json(path)


def transition_operator_inbox_item_v1(
    *,
    item: dict[str, Any],
    status: str,
    updated_at: str,
    notes: str = "",
    promoted_to_research_idea_id: str = "",
    promoted_to_hypothesis_id: str = "",
) -> dict[str, Any]:
    old_status = str(item.get("status") or "")
    new_status = _enum(status, OPERATOR_INBOX_STATUSES, "status")
    _validate_transition(old_status=old_status, new_status=new_status)
    updated = {
        **item,
        "status": new_status,
        "updated_at": updated_at,
        "notes": _append_note(str(item.get("notes") or ""), notes),
        "promoted_to_research_idea_id": promoted_to_research_idea_id or str(item.get("promoted_to_research_idea_id") or ""),
        "promoted_to_hypothesis_id": promoted_to_hypothesis_id or str(item.get("promoted_to_hypothesis_id") or ""),
        "canonical_json_hash": None,
    }
    if new_status == "PROMOTED_TO_IDEA" and not updated["promoted_to_research_idea_id"]:
        raise ValueError("PROMOTED_TO_IDEA_REQUIRES_RESEARCH_IDEA_ID")
    if new_status == "PROMOTED_TO_HYPOTHESIS" and not updated["promoted_to_hypothesis_id"]:
        raise ValueError("PROMOTED_TO_HYPOTHESIS_REQUIRES_HYPOTHESIS_ID")
    updated["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(updated)
    validate_operator_inbox_artifact_v1(updated)
    return updated


def promote_operator_inbox_to_research_idea_v1(
    *,
    truth_root: Path,
    item: dict[str, Any],
    day_utc: str,
    updated_at: str,
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    if str(item.get("status") or "") != "REVIEWED":
        raise ValueError("PROMOTE_TO_IDEA_REQUIRES_REVIEWED_STATUS")
    idea_id = f"operator-inbox:{item['inbox_item_id']}"
    idea = build_research_inbox_item_v1(
        inbox_id=idea_id,
        created_at_utc=updated_at,
        source="MANUAL",
        raw_text=f"{item.get('title')}: {item.get('description')}",
        tags=[*_strings(item.get("tags")), "operator_inbox"],
        related_symbols=[],
        related_edge_family=str(item.get("category") or ""),
        suggested_program="",
        triage_status="NEW",
        converted_hypothesis_id="",
        notes=f"Promoted from operator_inbox.v1:{item['inbox_item_id']}. {item.get('notes') or ''}".strip(),
    )
    idea_path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=idea)
    updated = transition_operator_inbox_item_v1(
        item=item,
        status="PROMOTED_TO_IDEA",
        updated_at=updated_at,
        promoted_to_research_idea_id=idea_id,
        notes=f"Promoted to research_inbox_item.v1:{idea_id}",
    )
    write_operator_inbox_artifact_v1(truth_root=truth_root, payload=updated)
    return updated, idea, idea_path


def build_operator_inbox_review_report_v1(
    *,
    truth_root: Path,
    generated_at_utc: str,
    items: list[dict[str, Any]],
    stale_days: int = 7,
) -> dict[str, Any]:
    ordered = sorted(items, key=lambda row: (str(row.get("status") or ""), str(row.get("created_at") or ""), str(row.get("inbox_item_id") or "")))
    captured = [row for row in ordered if row["status"] == "CAPTURED"]
    reviewed = [row for row in ordered if row["status"] == "REVIEWED"]
    promoted = [row for row in ordered if row["status"] in {"PROMOTED_TO_IDEA", "PROMOTED_TO_HYPOTHESIS"}]
    closed = [row for row in ordered if row["status"] in {"ARCHIVED", "REJECTED"}]
    stale = [row for row in captured if _age_days(row.get("created_at"), generated_at_utc) >= stale_days]
    high_priority = [row for row in ordered if row["status"] in OPEN_STATUSES and str(row.get("priority") or "").upper() in {"HIGH", "CRITICAL"}]
    lineage_gaps = []
    for row in promoted:
        if row["status"] == "PROMOTED_TO_IDEA" and not str(row.get("promoted_to_research_idea_id") or ""):
            lineage_gaps.append({"inbox_item_id": row["inbox_item_id"], "missing": "promoted_to_research_idea_id"})
        if row["status"] == "PROMOTED_TO_HYPOTHESIS" and not str(row.get("promoted_to_hypothesis_id") or ""):
            lineage_gaps.append({"inbox_item_id": row["inbox_item_id"], "missing": "promoted_to_hypothesis_id"})
    counts = {status: sum(1 for row in ordered if row["status"] == status) for status in sorted(OPERATOR_INBOX_STATUSES)}
    payload = {
        "schema_id": "operator_inbox_review_report",
        "schema_version": "v1",
        "artifact_id": "operator_inbox_review_report_v1",
        "report_id": "operator_inbox_review_report_v1:latest",
        "generated_at_utc": generated_at_utc,
        "truth_root": str(Path(truth_root).resolve()),
        "item_counts": {"total": len(ordered), **counts},
        "captured_items": [_summary(row) for row in captured],
        "reviewed_items": [_summary(row) for row in reviewed],
        "promoted_items": [_summary(row) for row in promoted],
        "archived_rejected_items": [_summary(row) for row in closed],
        "stale_captured_items": [_summary(row) for row in stale],
        "high_priority_open_items": [_summary(row) for row in high_priority],
        "lineage_gaps": lineage_gaps,
        "operator_inbox_only": True,
        "research_task_creation_allowed": False,
        "trade_creation_allowed": False,
        "sleeve_creation_allowed": False,
        "lite_runtime_mutation_allowed": False,
        "production_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "inbox_item_id": str(row.get("inbox_item_id") or ""),
        "title": str(row.get("title") or ""),
        "category": str(row.get("category") or ""),
        "status": str(row.get("status") or ""),
        "priority": str(row.get("priority") or ""),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
        "promoted_to_research_idea_id": str(row.get("promoted_to_research_idea_id") or ""),
        "promoted_to_hypothesis_id": str(row.get("promoted_to_hypothesis_id") or ""),
    }


def _validate_transition(*, old_status: str, new_status: str) -> None:
    allowed = {
        "CAPTURED": {"REVIEWED", "ARCHIVED", "REJECTED"},
        "REVIEWED": {"PROMOTED_TO_IDEA", "PROMOTED_TO_HYPOTHESIS", "ARCHIVED", "REJECTED"},
        "PROMOTED_TO_IDEA": {"ARCHIVED"},
        "PROMOTED_TO_HYPOTHESIS": {"ARCHIVED"},
        "ARCHIVED": set(),
        "REJECTED": set(),
    }
    if new_status not in allowed.get(old_status, set()):
        raise ValueError(f"INVALID_OPERATOR_INBOX_TRANSITION:{old_status}->{new_status}")


def _operator_inbox_id(*, title: str, created_at: str) -> str:
    digest = canonical_hash_for_c2_artifact_v1({"title": title, "created_at": created_at})[:12]
    return f"op-inbox-{created_at[:10].replace('-', '')}-{digest}"


def _append_note(existing: str, new_note: str) -> str:
    if not new_note:
        return existing
    if not existing:
        return new_note
    return f"{existing}\n{new_note}"


def _enum(value: str, allowed: set[str], field: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"INVALID_OPERATOR_INBOX_{field.upper()}:{normalized}")
    return normalized


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "operator_inbox"


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"OPERATOR_INBOX_JSON_NOT_OBJECT:{path}")
    return payload


def _age_days(created_at: Any, now: str) -> int:
    try:
        created = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        current = datetime.fromisoformat(str(now).replace("Z", "+00:00"))
    except ValueError:
        return 0
    return max(0, (current - created).days)
