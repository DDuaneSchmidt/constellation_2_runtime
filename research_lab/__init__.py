"""Research Lab namespace shim.

The implementation lives under research_lab/src/research_lab so this
repository can run `python -m research_lab.cli` without packaging first.
"""

from __future__ import annotations

from pathlib import Path

_SRC_PACKAGE = Path(__file__).resolve().parent / "src" / "research_lab"
if _SRC_PACKAGE.exists():
    __path__.append(str(_SRC_PACKAGE))  # type: ignore[name-defined]

SCHEMA_VERSION = "research_lab.v1"
