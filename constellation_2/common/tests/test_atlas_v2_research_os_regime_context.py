from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.memory_index import add_memory_object
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryType, create_memory_object
from constellation_2.common.atlas_v2_research_os.regime_context import assign_regime_context_to_memory, create_regime_context, get_memory_by_regime_context, list_regime_contexts

NOW = "2026-06-04T00:00:00Z"


def test_creates_explicit_regime_context(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    row = create_regime_context(root=root, regime_context_id="regime-1", labels=["OPENING_SESSION", "HIGH_VOLATILITY"], description="explicit source", confidence=0.5, created_at=NOW)
    assert row["validation_status"] == "NOT_VALIDATED"


def test_supports_unknown(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    row = create_regime_context(root=root, regime_context_id="regime-unknown", labels=["UNKNOWN"], created_at=NOW)
    assert row["labels"] == ["UNKNOWN"]


def test_assigns_memory_to_regime_context(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    create_regime_context(root=root, regime_context_id="regime-2", labels=["CHOP"], created_at=NOW)
    add_memory_object(create_memory_object(memory_id="mem-1", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, is_root=True), root)
    row = assign_regime_context_to_memory(root, "mem-1", "regime-2")
    assert row["regime_context_ids"] == ["regime-2"]
    assert get_memory_by_regime_context(root, "regime-2")[0]["memory_id"] == "mem-1"


def test_regime_context_does_not_imply_validation(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    create_regime_context(root=root, regime_context_id="regime-3", labels=["TREND"], created_at=NOW)
    assert list_regime_contexts(root)[0]["validation_status"] == "NOT_VALIDATED"
