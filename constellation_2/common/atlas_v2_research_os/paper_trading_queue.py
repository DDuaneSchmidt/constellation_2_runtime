from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .paper_trade_test_plan import validate_test_plan
from .paper_trading_queue_governance import validate_paper_test_human_approval_only, validate_paper_trading_queue_allowed
from .paper_trading_queue_models import MEASUREMENT_ONLY_AUTHORITY_BOUNDARY, PaperTradingQueueItem, PaperTradingReadinessReview, validate_paper_trading_queue_item_model

QUEUE_PATH = "paper_trading_queue.json"
TEST_PLAN_PATH = "paper_trade_test_plans.json"
READINESS_REVIEW_PATH = "paper_trading_readiness_reviews.json"
READY_FOR_REVIEW_STATES = {"NEW", "READY_FOR_HUMAN_REVIEW"}


class PaperTradingQueueError(ValueError):
    pass


def create_queue_store(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    for filename in [QUEUE_PATH, TEST_PLAN_PATH, READINESS_REVIEW_PATH]:
        target = path / filename
        if not target.exists():
            target.write_text("[]\n", encoding="utf-8")
    return path


def enqueue_paper_trade_candidate(
    *,
    candidate_id: str,
    test_plan: dict[str, Any],
    queue_item_id: str | None = None,
    priority: float = 0.0,
    created_at: str | None = None,
    root: str | Path = DEFAULT_STORE_ROOT,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validate_test_plan(test_plan)
    if test_plan["candidate_id"] != candidate_id:
        raise PaperTradingQueueError("candidate_id must match test plan candidate_id")
    store = create_queue_store(root)
    plans = _read(store / TEST_PLAN_PATH)
    if not any(row["test_plan_id"] == test_plan["test_plan_id"] for row in plans):
        plans.append(test_plan)
        _write(store / TEST_PLAN_PATH, plans)
    item_id = queue_item_id or f"paper-queue-{candidate_id}-{test_plan['test_plan_id']}"
    rows = _read(store / QUEUE_PATH)
    existing = next((row for row in rows if row["queue_item_id"] == item_id), None)
    if existing:
        return existing
    if any(row["candidate_id"] == candidate_id and row["test_plan_id"] == test_plan["test_plan_id"] and row["state"] not in {"COMPLETED", "REJECTED", "RETIRED"} for row in rows):
        raise PaperTradingQueueError("unresolved queue item already exists for candidate/test plan")
    item = PaperTradingQueueItem(
        queue_item_id=item_id,
        candidate_id=candidate_id,
        test_plan_id=test_plan["test_plan_id"],
        state="READY_FOR_HUMAN_REVIEW" if test_plan.get("human_review_required", True) else "NEW",
        priority=float(priority),
        created_at=created_at or now_utc(),
        review_status="PENDING",
        metadata={
            "measurement_only": True,
            "authority_boundary": MEASUREMENT_ONLY_AUTHORITY_BOUNDARY,
            **dict(metadata or {}),
        },
    ).to_dict()
    validate_paper_trading_queue_allowed(item)
    rows.append(item)
    _write(store / QUEUE_PATH, rows)
    return item


def prioritize_paper_trading_queue(root: str | Path = DEFAULT_STORE_ROOT, *, limit: int | None = None) -> list[dict[str, Any]]:
    rows = [row for row in _read(create_queue_store(root) / QUEUE_PATH) if row["state"] not in {"COMPLETED", "REJECTED", "RETIRED", "FAILED_GOVERNANCE"}]
    ranked = sorted(rows, key=lambda row: (-float(row.get("priority", 0.0)), row["created_at"], row["queue_item_id"]))
    return ranked[:limit] if limit is not None else ranked


def approve_for_paper_test(
    queue_item_id: str,
    *,
    reviewer: str,
    reasons: list[str] | None = None,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = create_queue_store(root)
    rows = _read(store / QUEUE_PATH)
    item = _find_queue_item(rows, queue_item_id)
    if item["state"] not in READY_FOR_REVIEW_STATES:
        raise PaperTradingQueueError(f"queue item is not ready for approval: {item['state']}")
    review = PaperTradingReadinessReview(
        review_id=f"review-{queue_item_id}",
        queue_item_id=queue_item_id,
        candidate_id=item["candidate_id"],
        test_plan_id=item["test_plan_id"],
        created_at=created_at or now_utc(),
        review_status="APPROVED",
        reviewer=reviewer,
        reasons=list(reasons or ["Human approved for paper testing only."]),
        approved_for_paper_test=True,
        metadata={"measurement_only": True, "authority_boundary": MEASUREMENT_ONLY_AUTHORITY_BOUNDARY, **dict(metadata or {})},
    ).to_dict()
    validate_paper_test_human_approval_only(review)
    item["state"] = "APPROVED_FOR_PAPER_TEST"
    item["review_status"] = "APPROVED"
    item["metadata"] = {**item.get("metadata", {}), "approved_for_paper_test_only": True}
    validate_paper_trading_queue_allowed(item)
    _write(store / QUEUE_PATH, rows)
    _append_review(store, review)
    return item


def reject_paper_trade_candidate(
    queue_item_id: str,
    *,
    reviewer: str,
    reasons: list[str] | None = None,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str | None = None,
) -> dict[str, Any]:
    store = create_queue_store(root)
    rows = _read(store / QUEUE_PATH)
    item = _find_queue_item(rows, queue_item_id)
    review = PaperTradingReadinessReview(
        review_id=f"review-{queue_item_id}-rejected",
        queue_item_id=queue_item_id,
        candidate_id=item["candidate_id"],
        test_plan_id=item["test_plan_id"],
        created_at=created_at or now_utc(),
        review_status="REJECTED",
        reviewer=reviewer,
        reasons=list(reasons or ["Rejected for paper test queue."]),
        approved_for_paper_test=False,
        metadata={"measurement_only": True, "authority_boundary": MEASUREMENT_ONLY_AUTHORITY_BOUNDARY},
    ).to_dict()
    validate_paper_trading_queue_allowed(review)
    item["state"] = "REJECTED"
    item["review_status"] = "REJECTED"
    item["blocked_reason"] = "; ".join(review["reasons"])
    _write(store / QUEUE_PATH, rows)
    _append_review(store, review)
    return item


def list_ready_for_review(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return [row for row in prioritize_paper_trading_queue(root) if row["state"] in READY_FOR_REVIEW_STATES and row.get("review_status") == "PENDING"]


def list_paper_trading_queue(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return _read(create_queue_store(root) / QUEUE_PATH)


def list_paper_trade_test_plans(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return _read(create_queue_store(root) / TEST_PLAN_PATH)


def list_paper_trading_readiness_reviews(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return _read(create_queue_store(root) / READINESS_REVIEW_PATH)


def _find_queue_item(rows: list[dict[str, Any]], queue_item_id: str) -> dict[str, Any]:
    for row in rows:
        if row["queue_item_id"] == queue_item_id:
            validate_paper_trading_queue_item_model(row)
            return row
    raise PaperTradingQueueError(f"queue item not found: {queue_item_id}")


def _append_review(store: Path, review: dict[str, Any]) -> None:
    reviews = _read(store / READINESS_REVIEW_PATH)
    reviews.append(review)
    _write(store / READINESS_REVIEW_PATH, reviews)


def _read(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
