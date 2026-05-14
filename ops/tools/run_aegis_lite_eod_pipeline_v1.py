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

from constellation_2.common.aegis_lite_eod_v1 import (  # noqa: E402
    artifact_ref_v1,
    build_aegis_lite_eod_report_v1,
    build_sleeve_edge_overlap_review_v1,
    now_utc_iso_v1,
    read_candidate_input_v1,
    validate_aegis_lite_eod_report_v1,
    validate_sleeve_edge_overlap_review_v1,
    write_aegis_lite_eod_report_v1,
    write_sleeve_edge_overlap_review_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1  # noqa: E402


def build_aegis_lite_eod_pipeline_v1(
    *,
    day_utc: str,
    truth_root: Path,
    run_id: str,
    generated_at_utc: str,
    input_payload: dict[str, Any],
    operator_notes: str = "",
) -> dict[str, Any]:
    candidates = input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else []
    source_lineage = input_payload.get("source_artifact_lineage") if isinstance(input_payload.get("source_artifact_lineage"), list) else []
    overlap = build_sleeve_edge_overlap_review_v1(
        day_utc=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        candidates=candidates,
        source_artifact_lineage=source_lineage,
    )
    validate_sleeve_edge_overlap_review_v1(overlap)
    overlap_path = write_sleeve_edge_overlap_review_v1(truth_root=truth_root, payload=overlap)
    report = build_aegis_lite_eod_report_v1(
        day_utc=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        truth_root=truth_root,
        candidates=candidates,
        overlap_review=overlap,
        market_session=_object(input_payload.get("market_session")),
        market_regime_state=_object(input_payload.get("market_regime_state")),
        data_freshness_status=_object(input_payload.get("data_freshness_status")),
        governance_status=_object(input_payload.get("governance_status")),
        sleeve_outputs=_objects(input_payload.get("sleeve_outputs")),
        sleeve_performance_summary=_objects(input_payload.get("sleeve_performance_summary")),
        sandbox_research_notes=_objects(input_payload.get("sandbox_research_notes")),
        operator_notes=operator_notes or str(input_payload.get("operator_notes") or ""),
        source_artifact_lineage=[*source_lineage, artifact_ref_v1(overlap_path, artifact_type="sleeve_edge_overlap_review_v1")],
    )
    out_path = Path(str(report["artifact_path"]))
    attach_producer_contract_v1(
        report,
        producer_name="ops/tools/run_aegis_lite_eod_pipeline_v1.py",
        producer_command=(
            "python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py "
            f"--day_utc {day_utc} --truth_root {truth_root} --run_id {run_id}"
        ),
        input_artifacts=[Path(ref.get("path", "")) for ref in source_lineage if isinstance(ref, dict) and str(ref.get("path") or "")],
        output_artifacts=[overlap_path, out_path],
        schema_versions={"sleeve_edge_overlap_review": "v1", "aegis_lite_eod_report": "v1"},
    )
    report["run_receipt"]["producer_contract_attached"] = True
    report["canonical_json_hash"] = None
    report["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(report)
    validate_aegis_lite_eod_report_v1(report)
    write_aegis_lite_eod_report_v1(truth_root=truth_root, payload=report)
    return report


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _objects(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_lite_eod_pipeline_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--run_id", default="")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--candidate_input", required=True)
    parser.add_argument("--operator_notes", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    run_id = str(args.run_id or f"aegis_lite_eod_v1:{day_utc}")
    generated_at_utc = str(args.generated_at_utc or now_utc_iso_v1())
    input_payload = read_candidate_input_v1(Path(args.candidate_input))
    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        input_payload=input_payload,
        operator_notes=str(args.operator_notes or ""),
    )
    print(
        json.dumps(
            {
                "status": report["report_status"],
                "manual_execution_status": report["manual_execution_status"],
                "path": report["artifact_path"],
            },
            sort_keys=True,
        )
    )
    return 0 if report["report_status"] in {"READY", "READY_WITH_WARNINGS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
