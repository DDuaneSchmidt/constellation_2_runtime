from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from research_lab.storage.hashing import canonical_json


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any], *, overwrite: bool = False) -> Path:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite immutable manifest: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return path


def append_jsonl(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(canonical_json(payload) + "\n")
    return path


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows

