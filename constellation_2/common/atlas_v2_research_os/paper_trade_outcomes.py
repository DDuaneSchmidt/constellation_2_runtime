from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .paper_trade_outcome_governance import validate_paper_trade_outcome_allowed
from .paper_trade_outcome_models import PaperTradeOutcome, validate_paper_trade_outcome

OUTCOME_FILENAME = "paper_trade_outcomes.jsonl"


def record_paper_trade_outcome(root: str | Path = DEFAULT_STORE_ROOT, **kwargs: Any) -> dict[str, Any]:
    validate_paper_trade_outcome_allowed(kwargs)
    row = PaperTradeOutcome(
        outcome_id=kwargs.get("outcome_id") or _outcome_id(kwargs),
        candidate_id=kwargs["candidate_id"],
        test_plan_id=kwargs["test_plan_id"],
        created_at=kwargs.get("created_at") or _now(),
        observation_start=kwargs["observation_start"],
        observation_end=kwargs["observation_end"],
        sample_size=int(kwargs["sample_size"]),
        wins=int(kwargs["wins"]),
        losses=int(kwargs["losses"]),
        average_return=float(kwargs["average_return"]),
        expectancy=float(kwargs["expectancy"]),
        max_drawdown=float(kwargs["max_drawdown"]),
        profit_factor=float(kwargs["profit_factor"]),
        regime_context=str(kwargs.get("regime_context", "UNKNOWN")),
        hypothesis_confirmed=bool(kwargs.get("hypothesis_confirmed", False)),
        hypothesis_weakened=bool(kwargs.get("hypothesis_weakened", False)),
        hypothesis_falsified=bool(kwargs.get("hypothesis_falsified", False)),
        failure_reasons=list(kwargs.get("failure_reasons", [])),
        success_reasons=list(kwargs.get("success_reasons", [])),
        source_artifact_ids=list(kwargs.get("source_artifact_ids", [])),
        metadata={**dict(kwargs.get("metadata", {})), "paper_only": True, "no_authority_created": True},
    ).to_dict()
    rows = list_paper_trade_outcomes(root)
    if any(existing["outcome_id"] == row["outcome_id"] for existing in rows):
        raise ValueError(f"paper outcome exists: {row['outcome_id']}")
    path = paper_trade_outcome_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
    return row


def list_paper_trade_outcomes(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    path = paper_trade_outcome_path(root)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        validate_paper_trade_outcome(row)
        rows.append(row)
    return rows


def paper_trade_outcome_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return Path(root) / "paper_trade_outcomes" / OUTCOME_FILENAME


def _outcome_id(payload: dict[str, Any]) -> str:
    parts = [payload.get("candidate_id"), payload.get("test_plan_id"), payload.get("observation_end")]
    return "pto-" + hashlib.sha1(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
