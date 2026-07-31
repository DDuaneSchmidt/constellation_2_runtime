#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.similar_symbol_expansion_program import (
    run_similar_symbol_expansion_program,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas v2 similar-symbol expansion program report.")
    parser.add_argument("--root", default="reports/atlas_v2_research_os")
    parser.add_argument("--created-at", default=None)
    args = parser.parse_args()

    report = run_similar_symbol_expansion_program(root=args.root, created_at=args.created_at)
    print(json.dumps({"ok": True, "path": f"{args.root}/similar_symbol_expansion_program/latest.json", "summary": report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
