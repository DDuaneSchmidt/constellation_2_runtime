from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.expansion_program_review import (
    run_expansion_program_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas v2 Builds 183-186 expansion program review.")
    parser.add_argument("--root", default="reports/atlas_v2_research_os")
    parser.add_argument("--created-at", default=None)
    args = parser.parse_args()
    report = run_expansion_program_review(root=Path(args.root), created_at=args.created_at)
    print(
        json.dumps(
            {
                "program_decision": report["summary"]["program_decision"],
                "strongest_supported_decision": report["summary"]["strongest_supported_decision"],
                "output": str(Path(args.root) / "expansion_program_review"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
