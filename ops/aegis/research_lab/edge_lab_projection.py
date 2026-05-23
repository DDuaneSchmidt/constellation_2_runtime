from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.research_lab.research_store_reader import build_edge_lab_projection


def read_edge_lab_projection_v1(*, sleeve_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return build_edge_lab_projection(sleeve_id=sleeve_id, store_root=store_root)
