from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.memory_index import add_memory_object
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryType, create_memory_object
from constellation_2.common.atlas_v2_research_os.semantic_deduplication import compute_mechanism_overlap, compute_semantic_duplicate_score, compute_token_overlap, find_potential_duplicates, load_duplicate_links, mark_duplicate, mark_not_duplicate, normalize_research_text

NOW = "2026-06-04T00:00:00Z"


def _seed(root: Path) -> None:
    add_memory_object(create_memory_object(memory_id="mem-a", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-open"], source_artifact_ids=["artifact-a"], labels=["generated_only"], metadata={"canonical_text": "Sneaky Pivot opening range confirmation claim"}), root)
    add_memory_object(create_memory_object(memory_id="mem-b", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-open"], source_artifact_ids=["artifact-a", "artifact-b"], labels=["generated_only"], metadata={"canonical_text": "Opening drive opening range confirmation claim"}), root)


def test_normalizes_text() -> None:
    assert normalize_research_text("The Sneaky Pivot, with confirmation!") == "opening range confirmation"


def test_computes_token_and_mechanism_overlap() -> None:
    assert compute_token_overlap("opening range breakout", "opening range move") > 0.5
    assert compute_mechanism_overlap(["OPENING_RANGE"], ["OPENING_RANGE", "VOLATILITY_EXPANSION"]) == 0.5


def test_flags_potential_duplicate() -> None:
    score = compute_semantic_duplicate_score(text_a="opening range breakout", text_b="opening range move", mechanism_tags_a=["OPENING_RANGE"], mechanism_tags_b=["OPENING_RANGE"], regime_contexts_a=[], regime_contexts_b=[])
    assert score >= 0.70


def test_flags_likely_duplicate_and_preserves_sources(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    matches = find_potential_duplicates(root)
    assert matches[0]["status"] == "LIKELY_DUPLICATE"
    left = matches[0]["left_memory_id"]
    right = matches[0]["right_memory_id"]
    assert {left, right} == {"mem-a", "mem-b"}


def test_mark_duplicate_and_not_duplicate(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    mark_duplicate(root, "mem-a", "mem-b", reviewer="test", reason="same mechanism")
    mark_not_duplicate(root, "mem-a", "mem-c", reviewer="test", reason="different scope")
    links = load_duplicate_links(root)
    assert links[0]["status"] == "DUPLICATE"
    assert links[1]["status"] == "NOT_DUPLICATE"
    assert links[0]["source_memory_ids"] == ["mem-a", "mem-b"]
