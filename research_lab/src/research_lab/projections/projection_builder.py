from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.projections.hypothesis_queue_projection import build_hypothesis_queue_projection
from research_lab.projections.operator_queue_projection import build_operator_queue_projection
from research_lab.projections.projection_validator import validate_research_projections


def rebuild_research_projections(*, projection: str = "all", store_root: Path | None = None, strict: bool = False, actor: str = "AegisProjection") -> dict[str, Any]:
    if projection not in {"all", "hypothesis_queue", "operator_home"}:
        raise ValueError(f"unsupported projection: {projection}")
    outputs: dict[str, Any] = {}
    if projection in {"all", "hypothesis_queue"}:
        outputs["hypothesis_queue"] = build_hypothesis_queue_projection(store_root=store_root, actor=actor, strict=strict, write=True)
    if projection in {"all", "operator_home"}:
        if projection == "all" and "hypothesis_queue" not in outputs:
            outputs["hypothesis_queue"] = build_hypothesis_queue_projection(store_root=store_root, actor=actor, strict=strict, write=True)
        outputs["operator_home"] = build_operator_queue_projection(store_root=store_root, actor=actor, strict=strict, write=True)
    validation = validate_research_projections(projection=projection, store_root=store_root, strict=strict, actor=actor)
    return {"ok": validation["ok"] and all(item.get("ok") for item in outputs.values()), "projection": projection, "outputs": outputs, "validation": validation}
