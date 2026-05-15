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
from constellation_2.common.aegis_lite_operating_status_v1 import (  # noqa: E402
    aegis_lite_operating_status_path_v1,
    build_aegis_lite_operating_status_v1,
    write_aegis_lite_operating_status_v1,
)
from constellation_2.common.aegis_lite_promoted_candidates_v1 import (  # noqa: E402
    build_demo_candidate_input_v1,
    build_demo_promoted_sleeve_library_v1,
    build_promoted_candidate_set_v1,
    write_promoted_candidate_set_v1,
    write_promoted_sleeve_library_v1,
)
from constellation_2.common.aegis_release_integrity_status_v1 import (  # noqa: E402
    build_aegis_release_integrity_status_v1,
    write_aegis_release_integrity_status_v1,
)
from constellation_2.common.aegis_legacy_paper_runtime_status_v1 import (  # noqa: E402
    build_legacy_paper_runtime_status_v1,
    write_legacy_paper_runtime_status_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1  # noqa: E402
from constellation_2.common.aegis_lite_manual_feedback_v1 import (  # noqa: E402
    build_edge_cluster_v1,
    build_operator_execution_queue_v1,
    validate_manual_feedback_artifact_v1,
    write_manual_feedback_artifact_v1,
)


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
    edge_cluster = _object(input_payload.get("edge_cluster"))
    edge_cluster_path: Path | None = None
    if not edge_cluster:
        edge_cluster = build_edge_cluster_v1(day_utc=day_utc, run_id=run_id, candidates=candidates)
        validate_manual_feedback_artifact_v1(edge_cluster)
        edge_cluster_path = write_manual_feedback_artifact_v1(truth_root=truth_root, payload=edge_cluster)
    operator_execution_queue = _object(input_payload.get("operator_execution_queue"))
    operator_queue_path: Path | None = None
    if not operator_execution_queue:
        operator_execution_queue = build_operator_execution_queue_v1(
            day_utc=day_utc,
            run_id=run_id,
            candidates=candidates,
            edge_clusters=edge_cluster,
        )
        validate_manual_feedback_artifact_v1(operator_execution_queue)
        operator_queue_path = write_manual_feedback_artifact_v1(truth_root=truth_root, payload=operator_execution_queue)
    generated_refs = [artifact_ref_v1(overlap_path, artifact_type="sleeve_edge_overlap_review_v1")]
    if edge_cluster_path is not None:
        generated_refs.append(artifact_ref_v1(edge_cluster_path, artifact_type="edge_cluster_v1"))
    if operator_queue_path is not None:
        generated_refs.append(artifact_ref_v1(operator_queue_path, artifact_type="operator_execution_queue_v1"))
    promoted_candidate_set = build_promoted_candidate_set_v1(
        day_utc=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        candidates=candidates,
    )
    promoted_candidate_set_path = write_promoted_candidate_set_v1(truth_root=truth_root, payload=promoted_candidate_set)
    generated_refs.append(artifact_ref_v1(promoted_candidate_set_path, artifact_type="promoted_candidate_set_v1"))
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
        source_artifact_lineage=[*source_lineage, *generated_refs],
        manual_operator_decisions=_objects(input_payload.get("manual_operator_decisions")),
        manual_execution_events=_objects(input_payload.get("manual_execution_events")),
        portfolio_position_snapshot=_object(input_payload.get("portfolio_position_snapshot")),
        protective_order_snapshot=_object(input_payload.get("protective_order_snapshot")),
        trade_outcome_attribution=_object(input_payload.get("trade_outcome_attribution")),
        edge_cluster=edge_cluster,
        operator_execution_queue=operator_execution_queue,
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
        output_artifacts=[path for path in [overlap_path, edge_cluster_path, operator_queue_path, out_path] if path is not None],
        schema_versions={"sleeve_edge_overlap_review": "v1", "aegis_lite_eod_report": "v1"},
    )
    report["run_receipt"]["producer_contract_attached"] = True
    status_path = aegis_lite_operating_status_path_v1(truth_root=truth_root, day_utc=day_utc)
    report["run_receipt"]["aegis_lite_operating_status_path"] = str(status_path)
    report["canonical_json_hash"] = None
    report["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(report)
    validate_aegis_lite_eod_report_v1(report)
    report_path = write_aegis_lite_eod_report_v1(truth_root=truth_root, payload=report)
    release_integrity = build_aegis_release_integrity_status_v1(generated_at_utc=generated_at_utc)
    release_integrity_path = write_aegis_release_integrity_status_v1(truth_root=truth_root, payload=release_integrity)
    legacy_runtime = build_legacy_paper_runtime_status_v1(generated_at_utc=generated_at_utc)
    legacy_runtime_path = write_legacy_paper_runtime_status_v1(truth_root=truth_root, payload=legacy_runtime)
    status = build_aegis_lite_operating_status_v1(
        day_utc=day_utc,
        generated_at_utc=generated_at_utc,
        truth_root=truth_root,
        report=report,
        report_path=report_path,
        queue_path=operator_queue_path,
        release_integrity_status=release_integrity,
        legacy_paper_runtime_status=legacy_runtime,
        source_artifact_lineage=[
            *source_lineage,
            *generated_refs,
            artifact_ref_v1(report_path, artifact_type="aegis_lite_eod_report_v1"),
            artifact_ref_v1(release_integrity_path, artifact_type="aegis_release_integrity_status_v1"),
            artifact_ref_v1(legacy_runtime_path, artifact_type="legacy_paper_runtime_status_v1"),
        ],
    )
    write_aegis_lite_operating_status_v1(truth_root=truth_root, payload=status)
    return report


def filter_promoted_sleeve_candidates_v1(input_payload: dict[str, Any], promoted_sleeve_library: dict[str, Any]) -> dict[str, Any]:
    sleeves = promoted_sleeve_library.get("promoted_sleeves") if isinstance(promoted_sleeve_library.get("promoted_sleeves"), list) else promoted_sleeve_library.get("sleeves")
    sleeves = sleeves if isinstance(sleeves, list) else []
    promoted_ids = {
        str(row.get("sleeve_id") or "").strip()
        for row in sleeves
        if isinstance(row, dict)
        and str(row.get("promotion_status") or "").lower() == "promoted"
        and (
            bool(row.get("approved_by_human", False))
            or str(row.get("human_approval_status") or row.get("approval_status") or "").lower() in {"approved", "human_approved"}
        )
        and (
            bool(row.get("approved_for_lite_implementation", False))
            or str(row.get("implementation_status") or "").lower() in {"approved", "implemented", "active", "production_ready"}
        )
        and not bool(row.get("archived", False))
    }
    candidates = input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else []
    accepted = [row for row in candidates if isinstance(row, dict) and str(row.get("sleeve_id") or "").strip() in promoted_ids]
    rejected = [
        {
            "candidate_id": str(row.get("candidate_id") or ""),
            "sleeve_id": str(row.get("sleeve_id") or ""),
            "reason_code": "SLEEVE_NOT_APPROVED_FOR_LITE_OPERATION",
        }
        for row in candidates
        if isinstance(row, dict) and str(row.get("sleeve_id") or "").strip() not in promoted_ids
    ]
    return {
        **input_payload,
        "candidates": accepted,
        "promoted_sleeve_filter": {
            "promoted_sleeve_ids": sorted(promoted_ids),
            "accepted_candidate_count": len(accepted),
            "rejected_candidate_count": len(rejected),
            "rejected_candidates": rejected,
            "research_lab_artifacts_directly_executable": False,
            "human_approval_required": True,
            "approved_implementation_required": True,
        },
    }


def prepare_operational_input_payload_v1(
    *,
    candidate_input_path: str,
    promoted_sleeve_library_path: str,
    manual_only: bool,
) -> dict[str, Any]:
    if not manual_only:
        raise ValueError("AEGIS_LITE_PIPELINE_REQUIRES_MANUAL_ONLY")
    if not candidate_input_path:
        return _advisory_only_payload_v1(
            reason_codes=["CANDIDATE_INPUT_MISSING", "PROMOTED_SLEEVE_LIBRARY_REQUIRED"],
        )
    input_path = Path(candidate_input_path).expanduser().resolve()
    input_payload = read_candidate_input_v1(input_path)
    lineage = _objects(input_payload.get("source_artifact_lineage"))
    input_payload["source_artifact_lineage"] = [*lineage, artifact_ref_v1(input_path, artifact_type="aegis_lite_candidate_input_v1")]
    if not promoted_sleeve_library_path:
        payload = _advisory_only_payload_v1(
            reason_codes=["PROMOTED_SLEEVE_LIBRARY_MISSING"],
            source_lineage=input_payload["source_artifact_lineage"],
        )
        payload["unpromoted_candidate_count"] = len(input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else [])
        return payload
    library_path = Path(promoted_sleeve_library_path).expanduser().resolve()
    filtered = filter_promoted_sleeve_candidates_v1(input_payload, _read_json(library_path))
    filtered["source_artifact_lineage"] = [
        *(_objects(filtered.get("source_artifact_lineage"))),
        artifact_ref_v1(library_path, artifact_type="promoted_sleeve_library_v1"),
    ]
    if not filtered.get("candidates"):
        filtered["data_freshness_status"] = _status_with_reason(
            filtered.get("data_freshness_status"),
            default_status="PASS",
            reason_code="NO_PROMOTED_LITE_CANDIDATES",
        )
        filtered["governance_status"] = _status_with_reason(
            filtered.get("governance_status"),
            default_status="BLOCKED",
            reason_code="NO_PROMOTED_LITE_CANDIDATES",
        )
        filtered["governance_status"]["status"] = "BLOCKED"
        filtered["data_freshness_status"] = _status_with_reason(
            filtered.get("data_freshness_status"),
            default_status="PASS",
            reason_code="NO_PROMOTED_EXECUTABLE_CANDIDATES",
        )
        filtered["governance_status"] = _status_with_reason(
            filtered.get("governance_status"),
            default_status="BLOCKED",
            reason_code="NO_PROMOTED_EXECUTABLE_CANDIDATES",
        )
        filtered["governance_status"]["status"] = "BLOCKED"
    return filtered


def prepare_demo_operational_input_payload_v1(*, truth_root: Path, day_utc: str, run_id: str, generated_at_utc: str) -> dict[str, Any]:
    library = build_demo_promoted_sleeve_library_v1(generated_at_utc=generated_at_utc)
    library_path = write_promoted_sleeve_library_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id, payload=library)
    candidate_input = build_demo_candidate_input_v1(generated_at_utc=generated_at_utc)
    filtered = filter_promoted_sleeve_candidates_v1(candidate_input, library)
    filtered["source_artifact_lineage"] = [
        artifact_ref_v1(library_path, artifact_type="promoted_sleeve_library_v1"),
    ]
    return filtered


def apply_nyse_trading_day_gate_v1(*, input_payload: dict[str, Any], truth_root: Path, day_utc: str) -> dict[str, Any]:
    calendar_path = Path(truth_root).resolve() / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    state = _nyse_calendar_state_v1(calendar_path=calendar_path, day_utc=day_utc)
    if state == "TRADING_DAY":
        lineage = _objects(input_payload.get("source_artifact_lineage"))
        return {**input_payload, "source_artifact_lineage": [*lineage, artifact_ref_v1(calendar_path, artifact_type="market_calendar_v1:NYSE")]}
    reason = "NYSE_MARKET_CALENDAR_MISSING" if state == "MISSING" else "NYSE_NON_TRADING_DAY"
    return _advisory_only_payload_v1(
        reason_codes=[reason],
        source_lineage=[*(_objects(input_payload.get("source_artifact_lineage"))), artifact_ref_v1(calendar_path, artifact_type="market_calendar_v1:NYSE")],
    )


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _objects(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_OBJECT_REQUIRED:{path}")
    return payload


def _advisory_only_payload_v1(
    *,
    reason_codes: list[str],
    source_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "candidates": [],
        "data_freshness_status": {"status": "BLOCKED", "reason_codes": reason_codes},
        "governance_status": {"status": "BLOCKED", "reason_codes": reason_codes},
        "market_regime_state": {"status": "UNKNOWN", "reason_codes": reason_codes},
        "source_artifact_lineage": source_lineage or [],
        "operator_notes": "Aegis Lite operational path is advisory-only because no approved promoted candidates were available.",
    }


def _nyse_calendar_state_v1(*, calendar_path: Path, day_utc: str) -> str:
    if not calendar_path.exists():
        return "MISSING"
    try:
        with calendar_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                if isinstance(row, dict) and str(row.get("day_utc") or "") == day_utc:
                    return "TRADING_DAY" if bool(row.get("is_trading_session")) else "NON_TRADING_DAY"
    except (OSError, json.JSONDecodeError):
        return "MISSING"
    return "MISSING"


def _status_with_reason(value: Any, *, default_status: str, reason_code: str) -> dict[str, Any]:
    status = value if isinstance(value, dict) else {}
    reason_codes = [str(item) for item in status.get("reason_codes", []) if str(item)] if isinstance(status.get("reason_codes"), list) else []
    if reason_code not in reason_codes:
        reason_codes.append(reason_code)
    return {**status, "status": str(status.get("status") or default_status), "reason_codes": reason_codes}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_lite_eod_pipeline_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--run_id", default="")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--candidate_input", default="")
    parser.add_argument("--promoted_sleeve_library", default="")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--manual-only", action="store_true")
    parser.add_argument("--allow-not-ready-exit-zero", action="store_true")
    parser.add_argument("--demo-promoted-candidates", action="store_true")
    parser.add_argument("--operator_notes", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    run_id = str(args.run_id or f"aegis_lite_eod_v1:{day_utc}")
    generated_at_utc = str(args.generated_at_utc or now_utc_iso_v1())
    if args.demo_promoted_candidates:
        if not args.manual_only:
            raise SystemExit("FAIL: demo promoted candidates require --manual-only")
        input_payload = prepare_demo_operational_input_payload_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            run_id=run_id,
            generated_at_utc=generated_at_utc,
        )
    else:
        input_payload = prepare_operational_input_payload_v1(
            candidate_input_path=str(args.candidate_input or ""),
            promoted_sleeve_library_path=str(args.promoted_sleeve_library or ""),
            manual_only=bool(args.manual_only),
        )
    input_payload = apply_nyse_trading_day_gate_v1(input_payload=input_payload, truth_root=truth_root, day_utc=day_utc)
    input_payload["environment"] = str(args.environment or "PAPER").upper()
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
                "readiness_classification": report["readiness_classification"],
                "path": report["artifact_path"],
                "operating_status_path": report["run_receipt"].get("aegis_lite_operating_status_path", ""),
                "broker_submit_required": False,
                "manual_execution_only": True,
            },
            sort_keys=True,
        )
    )
    if args.allow_not_ready_exit_zero:
        return 0
    return 0 if report["report_status"] in {"READY", "READY_WITH_WARNINGS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
