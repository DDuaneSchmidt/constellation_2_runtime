#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_lite_eod_v1 import artifact_ref_v1, now_utc_iso_v1, read_candidate_input_v1  # noqa: E402
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1  # noqa: E402
from ops.tools.run_aegis_lite_eod_pipeline_v1 import (  # noqa: E402
    build_aegis_lite_eod_pipeline_v1,
    filter_promoted_sleeve_candidates_v1,
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_lite_eod_engine_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--run_id", default="")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--promoted_sleeve_library", required=True)
    parser.add_argument("--candidate_input", required=True)
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    run_id = str(args.run_id or f"aegis_lite_eod_engine_v1:{day_utc}")
    generated_at_utc = str(args.generated_at_utc or now_utc_iso_v1())
    library_path = Path(args.promoted_sleeve_library).expanduser().resolve()
    input_payload = read_candidate_input_v1(Path(args.candidate_input))
    filtered = filter_promoted_sleeve_candidates_v1(input_payload, _read_json(library_path))
    filtered["source_artifact_lineage"] = [
        *([item for item in filtered.get("source_artifact_lineage", []) if isinstance(item, dict)] if isinstance(filtered.get("source_artifact_lineage"), list) else []),
        artifact_ref_v1(library_path, artifact_type="promoted_sleeve_library_v1"),
    ]
    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        input_payload=filtered,
        operator_notes="Aegis Lite EOD engine used promoted_sleeve_library_v1; unpromoted sleeve candidates were excluded.",
    )
    print(
        json.dumps(
            {
                "status": report["report_status"],
                "manual_execution_status": report["manual_execution_status"],
                "candidate_count": len(report["selected_trade_candidates"]),
                "promoted_filter": filtered["promoted_sleeve_filter"],
                "path": report["artifact_path"],
            },
            sort_keys=True,
        )
    )
    return 0 if report["report_status"] in {"READY", "READY_WITH_WARNINGS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
