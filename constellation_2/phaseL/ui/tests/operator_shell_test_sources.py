from __future__ import annotations

from pathlib import Path


def pages_source_v1(root: Path) -> str:
    base = Path(root)
    pages = base / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
    route_metadata = base / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
    return pages.read_text(encoding="utf-8") + "\n" + route_metadata.read_text(encoding="utf-8")
