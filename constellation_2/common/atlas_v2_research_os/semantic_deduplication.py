from __future__ import annotations

import json
import re
import string
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .memory_index import list_memory_objects, memory_root

DUPLICATE_FILE = "duplicate_links.json"
POTENTIAL_DUPLICATE_THRESHOLD = 0.70
LIKELY_DUPLICATE_THRESHOLD = 0.85
ALIASES = {
    "opening drive": "opening range",
    "opening range move": "opening range",
    "first session breakout": "opening range breakout",
    "sneaky pivot": "opening range",
    "vwap reclaim": "vwap average reclaim",
}
STOPWORDS = {"a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it", "of", "on", "or", "that", "the", "to", "when", "with"}


def normalize_research_text(text: str) -> str:
    value = text.lower()
    for alias, canonical in ALIASES.items():
        value = value.replace(alias, canonical)
    value = value.translate(str.maketrans("", "", string.punctuation))
    tokens = [token for token in re.split(r"\s+", value.strip()) if token and token not in STOPWORDS]
    return " ".join(tokens)


def compute_token_overlap(a: str, b: str) -> float:
    a_tokens = set(normalize_research_text(a).split())
    b_tokens = set(normalize_research_text(b).split())
    return _overlap(a_tokens, b_tokens)


def compute_mechanism_overlap(a_tags: list[str], b_tags: list[str]) -> float:
    return _overlap(set(a_tags), set(b_tags))


def compute_regime_overlap(a_contexts: list[str], b_contexts: list[str]) -> float:
    return _overlap(set(a_contexts), set(b_contexts))


def compute_source_lineage_overlap(a_sources: list[str], b_sources: list[str]) -> float:
    return _overlap(set(a_sources), set(b_sources))


def compute_semantic_duplicate_score(*, text_a: str, text_b: str, mechanism_tags_a: list[str] | None = None, mechanism_tags_b: list[str] | None = None, regime_contexts_a: list[str] | None = None, regime_contexts_b: list[str] | None = None, source_artifact_ids_a: list[str] | None = None, source_artifact_ids_b: list[str] | None = None) -> float:
    return round(
        0.50 * compute_token_overlap(text_a, text_b)
        + 0.25 * compute_mechanism_overlap(mechanism_tags_a or [], mechanism_tags_b or [])
        + 0.15 * compute_regime_overlap(regime_contexts_a or [], regime_contexts_b or [])
        + 0.10 * compute_source_lineage_overlap(source_artifact_ids_a or [], source_artifact_ids_b or []),
        6,
    )


def find_potential_duplicates(root: str | Path = DEFAULT_STORE_ROOT, *, threshold: float = POTENTIAL_DUPLICATE_THRESHOLD) -> list[dict[str, Any]]:
    rows = list_memory_objects(root)
    matches: list[dict[str, Any]] = []
    for i, left in enumerate(rows):
        for right in rows[i + 1 :]:
            score = compute_semantic_duplicate_score(
                text_a=_memory_text(left),
                text_b=_memory_text(right),
                mechanism_tags_a=left.get("mechanism_tags", []),
                mechanism_tags_b=right.get("mechanism_tags", []),
                regime_contexts_a=left.get("regime_context_ids", []),
                regime_contexts_b=right.get("regime_context_ids", []),
                source_artifact_ids_a=left.get("source_artifact_ids", []),
                source_artifact_ids_b=right.get("source_artifact_ids", []),
            )
            if score >= threshold:
                matches.append({
                    "left_memory_id": left["memory_id"],
                    "right_memory_id": right["memory_id"],
                    "duplicate_score": score,
                    "status": "LIKELY_DUPLICATE" if score >= LIKELY_DUPLICATE_THRESHOLD else "POTENTIAL_DUPLICATE",
                    "needs_review": score < LIKELY_DUPLICATE_THRESHOLD,
                })
    return matches


def mark_duplicate(root: str | Path, left_memory_id: str, right_memory_id: str, *, reviewer: str = "operator", reason: str = "") -> dict[str, Any]:
    return _record_duplicate(root, left_memory_id, right_memory_id, "DUPLICATE", reviewer, reason)


def mark_not_duplicate(root: str | Path, left_memory_id: str, right_memory_id: str, *, reviewer: str = "operator", reason: str = "") -> dict[str, Any]:
    return _record_duplicate(root, left_memory_id, right_memory_id, "NOT_DUPLICATE", reviewer, reason)


def duplicate_links_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return memory_root(root) / DUPLICATE_FILE


def load_duplicate_links(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    path = duplicate_links_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_links(root, [])
    return json.loads(path.read_text(encoding="utf-8"))


def _record_duplicate(root: str | Path, left_memory_id: str, right_memory_id: str, status: str, reviewer: str, reason: str) -> dict[str, Any]:
    links = load_duplicate_links(root)
    row = {
        "duplicate_link_id": f"dup-{left_memory_id}-{right_memory_id}-{len(links) + 1:04d}",
        "left_memory_id": left_memory_id,
        "right_memory_id": right_memory_id,
        "status": status,
        "reviewer": reviewer,
        "reason": reason,
        "source_memory_ids": [left_memory_id, right_memory_id],
    }
    links.append(row)
    _write_links(root, links)
    return row


def _write_links(root: str | Path, links: list[dict[str, Any]]) -> None:
    path = duplicate_links_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(links, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _overlap(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _memory_text(row: dict[str, Any]) -> str:
    metadata = row.get("metadata", {}) or {}
    return str(metadata.get("canonical_text") or metadata.get("claim_text") or metadata.get("hypothesis_text") or metadata.get("description") or row.get("memory_id", ""))
