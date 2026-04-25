from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Iterable
import json

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]


def tax_output_root_v1(output_root: str | Path) -> Path:
    return Path(output_root).expanduser().resolve()


def tax_json_path_v1(
    *,
    output_root: str | Path,
    lane: str,
    family: str,
    scope_id: str,
    record_id: str,
) -> Path:
    return (
        tax_output_root_v1(output_root)
        / lane
        / family
        / "scopes"
        / str(scope_id).strip()
        / f"{str(record_id).strip()}.{str(family).strip()}.v1.json"
    ).resolve()


def tax_jsonl_path_v1(
    *,
    output_root: str | Path,
    family: str,
    scope_id: str,
    filename: str,
) -> Path:
    return (
        tax_output_root_v1(output_root)
        / "journals"
        / family
        / "scopes"
        / str(scope_id).strip()
        / filename
    ).resolve()


def write_immutable_validated_json_v1(
    *,
    path: str | Path,
    payload: dict[str, Any],
    schema_relpath: str,
) -> Path:
    resolved = Path(path).expanduser().resolve()
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    raw = canonical_json_bytes_v1(payload) + b"\n"
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if resolved.exists() and resolved.read_bytes() != raw:
        raise ValueError(f"TAX_IMMUTABLE_CONFLICT:{resolved}")
    resolved.write_bytes(raw)
    return resolved


def append_validated_jsonl_v1(
    *,
    path: str | Path,
    payloads: Iterable[dict[str, Any]],
    schema_relpath: str,
) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    existing_hashes: set[str] = set()
    if resolved.exists():
        for raw_line in resolved.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            existing_hashes.add(hashlib.sha256(line.encode("utf-8")).hexdigest())
    appended = 0
    chunks: list[bytes] = []
    for payload in payloads:
        validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
        chunk = canonical_json_bytes_v1(payload) + b"\n"
        chunk_hash = hashlib.sha256(chunk.rstrip(b"\n")).hexdigest()
        if chunk_hash in existing_hashes:
            continue
        existing_hashes.add(chunk_hash)
        chunks.append(chunk)
        appended += 1
    if chunks:
        tmp = resolved.with_suffix(resolved.suffix + f".tmp.{os.getpid()}")
        base = resolved.read_bytes() if resolved.exists() else b""
        tmp.write_bytes(base + b"".join(chunks))
        os.replace(str(tmp), str(resolved))
    return {
        "journal_path": str(resolved),
        "journal_sha256": hashlib.sha256(resolved.read_bytes()).hexdigest() if resolved.exists() else "",
        "appended_count": appended,
    }


def read_json_v1(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TAX_TOP_LEVEL_NOT_OBJECT:{path}")
    return payload
