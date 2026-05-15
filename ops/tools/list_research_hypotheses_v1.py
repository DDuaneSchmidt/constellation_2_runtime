#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="list_research_hypotheses_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    root = truth_root / "research_lab" / "research_hypothesis_v1"
    if args.day_utc:
        root = root / args.day_utc
    rows = []
    for path in sorted(root.rglob("research_hypothesis.v1.json")) if root.exists() else []:
        payload = _read_json(path)
        rows.append(
            {
                "hypothesis_id": payload.get("hypothesis_id"),
                "title": payload.get("title"),
                "status": payload.get("status"),
                "source": payload.get("source"),
                "path": str(path),
            }
        )
    print(json.dumps({"hypotheses": rows, "count": len(rows), "research_lab_only": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
