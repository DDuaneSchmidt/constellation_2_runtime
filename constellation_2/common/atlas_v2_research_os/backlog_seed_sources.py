from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .failure_observatory import list_failures
from .failure_patterns import get_repeated_failures, list_failure_patterns
from .memory_index import list_memory_objects, memory_root
from .regime_context import list_regime_contexts
from .semantic_deduplication import find_potential_duplicates, load_duplicate_links


def collect_seed_sources(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    root_path = Path(root)
    store = ArtifactStore(root_path)
    artifacts = store.list_artifacts()
    return {
        "research_memory": list_memory_objects(root_path),
        "failure_patterns": list_failure_patterns(root_path),
        "repeated_failures": get_repeated_failures(root_path, 2),
        "failure_observatory": list_failures(root_path),
        "duplicate_links": _safe(load_duplicate_links, root_path, default=[]),
        "potential_duplicates": _safe(find_potential_duplicates, root_path, default=[]),
        "regime_contexts": list_regime_contexts(root_path),
        "research_effectiveness": _read_latest(root_path / "research_effectiveness" / "latest.json"),
        "learning_validation": _read_latest(root_path / "learning_validation" / "latest.json"),
        "candidate_quality": _read_latest(root_path / "candidate_quality" / "latest.json"),
        "generated_research_claims": [row for row in artifacts if row.get("artifact_type") == ArtifactType.GENERATED_RESEARCH_CLAIM.value],
        "research_hypotheses": [row for row in artifacts if row.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value],
        "cheap_experiment_specs": [row for row in artifacts if row.get("artifact_type") == ArtifactType.CHEAP_EXPERIMENT_SPEC.value],
        "paper_trade_candidates": _read_candidates(root_path),
        "paper_trading_queue": _read_json(root_path / "paper_trading_queue.json", default=[]),
        "paper_trade_outcomes": _read_json(root_path / "paper_trade_outcomes.json", default=[]),
    }


def source_inventory_summary(sources: dict[str, Any]) -> dict[str, int]:
    return {key: len(value) if isinstance(value, list) else (1 if value else 0) for key, value in sorted(sources.items())}


def _read_candidates(root: Path) -> list[dict[str, Any]]:
    latest = _read_json(root / "paper_trade_candidates" / "latest.json", default={})
    if isinstance(latest, dict):
        rows = latest.get("candidates") or latest.get("paper_trade_candidates") or []
        return rows if isinstance(rows, list) else []
    return latest if isinstance(latest, list) else []


def _read_latest(path: Path) -> dict[str, Any]:
    payload = _read_json(path, default={})
    return payload if isinstance(payload, dict) else {}


def _read_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _safe(func: Any, *args: Any, default: Any) -> Any:
    try:
        return func(*args)
    except Exception:
        return default
