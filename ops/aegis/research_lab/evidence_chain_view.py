from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.research_lab.research_store_reader import build_evidence_chain_view


def read_evidence_chain_view_v1(*, sleeve_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return build_evidence_chain_view(sleeve_id=sleeve_id, store_root=store_root)
