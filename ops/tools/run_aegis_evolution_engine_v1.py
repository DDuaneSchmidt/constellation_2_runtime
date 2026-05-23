#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.evolution_engine_v1 import build_evolution_engine_v1, evolution_auxiliary_outputs_v1, render_evolution_summary_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import write_json_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_evolution_engine_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_evolution_engine_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=day)
    out_dir = root / "reports" / "aegis_evolution_engine_v1" / day
    engine_path = write_json_v1(out_dir / "evolution_engine.v1.json", payload)
    summary_path = out_dir / "evolution_engine.summary.txt"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(render_evolution_summary_v1(payload), encoding="utf-8")
    aux = evolution_auxiliary_outputs_v1(payload)
    paths = {"evolution_engine": str(engine_path), "summary": str(summary_path)}
    for name, aux_payload in aux.items():
        paths[name] = str(write_json_v1(out_dir / f"{name}.v1.json", aux_payload))
    print(json.dumps({"path": str(engine_path), "recommendation_count": payload["recommendation_count"], "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
