from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .orchestrator_models import OrchestratorRunRecord


def ledger_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return Path(root) / "orchestrator" / "run_ledger.jsonl"


def write_run_ledger(record: OrchestratorRunRecord | dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    path = ledger_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")
    return path


def read_run_ledger(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    path = ledger_path(root)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows
