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
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1  # noqa: E402
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1  # noqa: E402
from constellation_2.common.aegis_lite_manual_feedback_v1 import (  # noqa: E402
    build_edge_cluster_v1,
    build_operator_execution_queue_v1,
    validate_manual_feedback_artifact_v1,
    write_manual_feedback_artifact_v1,
)
from constellation_2.common.aegis_eod_artifact_contract_v1 import (  # noqa: E402
    build_candidate_consumption_audit_v1,
    build_candidate_lineage_v1,
    build_eod_run_manifest_v1,
    build_market_snapshot_authority_v1,
    build_promoted_sleeve_manifest_v1,
    build_synthetic_advisory_rows_v1,
    candidate_consumption_audit_path_v1,
    candidate_lineage_path_v1,
    eod_run_manifest_path_v1,
    load_upstream_candidate_rows_v1,
    market_snapshot_authority_path_v1,
    overall_status_from_inputs_v1,
    promoted_sleeve_manifest_path_v1,
    validate_eod_input_contract_v1,
    write_candidate_consumption_audit_v1,
    write_candidate_lineage_v1,
    write_eod_run_manifest_v1,
    write_market_snapshot_authority_v1,
    write_promoted_sleeve_manifest_v1,
)
from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_manual_trade_packet_v1,
    validate_research_lab_artifact_v1,
)


def manual_trade_packet_path_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "manual_trade_packet_v1"
        / day_utc
        / _safe_run_id(run_id)
        / "manual_trade_packet.v1.json"
    )


def write_manual_trade_packet_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_research_lab_artifact_v1(payload)
    path = manual_trade_packet_path_v1(
        truth_root=truth_root,
        day_utc=str(payload["date"]),
        run_id=str(payload["run_id"]),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_aegis_lite_eod_pipeline_v1(
    *,
    day_utc: str,
    truth_root: Path,
    run_id: str,
    generated_at_utc: str,
    input_payload: dict[str, Any],
    operator_notes: str = "",
    run_mode: str = "INTRADAY_OPERATIONAL",
) -> dict[str, Any]:
    run_mode = str(run_mode or "INTRADAY_OPERATIONAL").strip().upper()
    input_payload["run_mode"] = run_mode
    input_payload["market_data_mode"] = run_mode
    input_payload["final_eod_certification_status"] = "PENDING" if run_mode == "INTRADAY_OPERATIONAL" else "PASS"
    candidates = input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else []
    source_lineage = input_payload.get("source_artifact_lineage") if isinstance(input_payload.get("source_artifact_lineage"), list) else []
    upstream_candidate_manifest_path = _upstream_candidate_manifest_path_v1(truth_root=truth_root, day_utc=day_utc)
    upstream_candidate_manifest = _read_json_or_empty(upstream_candidate_manifest_path)
    raw_rows = _objects(input_payload.get("raw_candidates")) or candidates or load_upstream_candidate_rows_v1(upstream_candidate_manifest_path)
    if raw_rows:
        input_payload["raw_candidates"] = raw_rows
        input_payload.setdefault("raw_candidate_status", "RAW_CANDIDATES_AVAILABLE")
        input_payload.setdefault("raw_candidate_count", len(raw_rows))
        _remove_status_reason_v1(input_payload, "CANDIDATE_INPUT_MISSING")
        _remove_status_reason_v1(input_payload, "RAW_CANDIDATES_EXISTED_NOT_CONSUMED")
    if not input_payload.get("sleeve_eval_artifact_path") and upstream_candidate_manifest.get("source_rollup_path"):
        input_payload["sleeve_eval_artifact_path"] = str(upstream_candidate_manifest.get("source_rollup_path") or "")
    if not input_payload.get("raw_candidate_absent_reason") and not raw_rows and input_payload.get("candidate_input_path"):
        input_payload["raw_candidate_absent_reason"] = "NO_RAW_CANDIDATES"
        input_payload["raw_candidate_status"] = "NO_RAW_CANDIDATES"
    release_integrity = build_aegis_release_integrity_status_v1(generated_at_utc=generated_at_utc)
    release_integrity_path = write_aegis_release_integrity_status_v1(truth_root=truth_root, payload=release_integrity)
    legacy_runtime = build_legacy_paper_runtime_status_v1(generated_at_utc=generated_at_utc)
    legacy_runtime_path = write_legacy_paper_runtime_status_v1(truth_root=truth_root, payload=legacy_runtime)
    input_payload.setdefault("readiness_artifact_path", str(legacy_runtime_path))
    market_snapshot_authority = build_market_snapshot_authority_v1(
        truth_root=truth_root,
        trading_date=day_utc,
        run_id=run_id,
        created_at_utc=generated_at_utc,
        required_symbols=_required_symbols_v1(candidates),
        source_artifact_lineage=source_lineage,
    )
    market_snapshot_authority_path = write_market_snapshot_authority_v1(truth_root=truth_root, payload=market_snapshot_authority)
    promoted_sleeve_manifest = build_promoted_sleeve_manifest_v1(
        trading_date=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        promoted_sleeve_library=_object(input_payload.get("promoted_sleeve_library")),
        source_artifact=str(input_payload.get("promoted_sleeve_library_path") or ""),
    )
    promoted_sleeve_manifest_path = write_promoted_sleeve_manifest_v1(truth_root=truth_root, payload=promoted_sleeve_manifest)
    input_contract = validate_eod_input_contract_v1(
        trading_date=day_utc,
        input_payload=input_payload,
        market_snapshot_authority=market_snapshot_authority,
        promoted_sleeve_manifest=promoted_sleeve_manifest,
        upstream_candidate_manifest_path=upstream_candidate_manifest_path,
    )
    if input_contract["status"] != "PASS":
        for reason_code in input_contract["blockers"]:
            input_payload["data_freshness_status"] = _status_with_reason(
                input_payload.get("data_freshness_status"),
                default_status="BLOCKED",
                reason_code=str(reason_code),
            )
            input_payload["governance_status"] = _status_with_reason(
                input_payload.get("governance_status"),
                default_status="BLOCKED",
                reason_code=str(reason_code),
            )
            input_payload["market_regime_state"] = _status_with_reason(
                input_payload.get("market_regime_state"),
                default_status="UNKNOWN",
                reason_code=str(reason_code),
            )
        input_payload["data_freshness_status"]["status"] = "BLOCKED"
        input_payload["governance_status"]["status"] = "BLOCKED"
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
            generated_at_utc=generated_at_utc,
            candidates=candidates,
            edge_clusters=edge_cluster,
            empty_reason=_operator_queue_empty_reason_v1(input_contract=input_contract, candidates=candidates),
            input_contract_status=str(input_contract.get("status") or "UNKNOWN"),
        )
        validate_manual_feedback_artifact_v1(operator_execution_queue)
        operator_queue_path = write_manual_feedback_artifact_v1(truth_root=truth_root, payload=operator_execution_queue)
    manual_trade_packet = build_manual_trade_packet_v1(
        packet_id=f"manual_trade_packet:{run_id}",
        run_id=run_id,
        date=day_utc,
        generated_at_utc=generated_at_utc,
        regime_state=str(_object(input_payload.get("market_regime_state")).get("status") or "UNKNOWN"),
        trade_candidates=_manual_trade_packet_candidates_v1(candidates),
        promoted_sleeve_library=_object(input_payload.get("promoted_sleeve_library")),
    )
    manual_trade_packet_path = write_manual_trade_packet_v1(truth_root=truth_root, payload=manual_trade_packet)
    generated_refs = [
        artifact_ref_v1(market_snapshot_authority_path, artifact_type="market_snapshot_authority_v1"),
        artifact_ref_v1(promoted_sleeve_manifest_path, artifact_type="promoted_sleeve_manifest_v1"),
        artifact_ref_v1(overlap_path, artifact_type="sleeve_edge_overlap_review_v1"),
    ]
    if edge_cluster_path is not None:
        generated_refs.append(artifact_ref_v1(edge_cluster_path, artifact_type="edge_cluster_v1"))
    if operator_queue_path is not None:
        generated_refs.append(artifact_ref_v1(operator_queue_path, artifact_type="operator_execution_queue_v1"))
    generated_refs.append(artifact_ref_v1(manual_trade_packet_path, artifact_type="manual_trade_packet_v1"))
    promoted_candidate_set = build_promoted_candidate_set_v1(
        day_utc=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        candidates=candidates,
    )
    promoted_candidate_set_path = write_promoted_candidate_set_v1(truth_root=truth_root, payload=promoted_candidate_set)
    generated_refs.append(artifact_ref_v1(promoted_candidate_set_path, artifact_type="promoted_candidate_set_v1"))
    lineage = build_candidate_lineage_v1(
        trading_date=day_utc,
        run_id=run_id,
        created_at_utc=generated_at_utc,
        input_payload=input_payload,
        raw_rows=raw_rows,
        consumed_candidates=candidates,
        promoted_sleeve_manifest_path=str(promoted_sleeve_manifest_path),
        input_contract=input_contract,
        readiness_status=str(legacy_runtime.get("runtime_status") or legacy_runtime.get("status") or "UNKNOWN"),
    )
    candidate_lineage_path = write_candidate_lineage_v1(truth_root=truth_root, payload=lineage)
    generated_refs.append(artifact_ref_v1(candidate_lineage_path, artifact_type="candidate_lineage_v1"))
    certified_symbols, certified_artifact_path = _certified_eod_universe_v1(truth_root=truth_root, day_utc=day_utc)
    candidate_consumption_audit = build_candidate_consumption_audit_v1(
        trading_date=day_utc,
        run_id=run_id,
        created_at_utc=generated_at_utc,
        raw_rows=raw_rows,
        consumed_candidates=candidates,
        promoted_sleeve_manifest=promoted_sleeve_manifest,
        certified_symbols=certified_symbols,
        certified_artifact_path=certified_artifact_path,
        input_contract=input_contract,
    )
    candidate_consumption_audit_path = write_candidate_consumption_audit_v1(truth_root=truth_root, payload=candidate_consumption_audit)
    generated_refs.append(artifact_ref_v1(candidate_consumption_audit_path, artifact_type="candidate_consumption_audit_v1"))
    synthetic_advisory_rows = build_synthetic_advisory_rows_v1(lineage=lineage, input_contract=input_contract)
    manifest_path = eod_run_manifest_path_v1(truth_root=truth_root, trading_date=day_utc, run_id=run_id)
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
        eod_input_contract=input_contract,
        blocked_advisory_candidates=synthetic_advisory_rows,
        candidate_lineage_artifact_path=str(candidate_lineage_path),
        candidate_consumption_audit_artifact_path=str(candidate_consumption_audit_path),
        eod_run_manifest_path=str(manifest_path),
        market_snapshot_authority_path=str(market_snapshot_authority_path),
        promoted_sleeve_manifest_path=str(promoted_sleeve_manifest_path),
    )
    out_path = Path(str(report["artifact_path"]))
    attach_producer_contract_v1(
        report,
        producer_name="ops/tools/run_aegis_lite_eod_pipeline_v1.py",
        producer_command=(
            "python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py "
            f"--day_utc {day_utc} --truth_root {truth_root} --run_id {run_id} --run-mode {run_mode}"
        ),
        input_artifacts=[Path(ref.get("path", "")) for ref in source_lineage if isinstance(ref, dict) and str(ref.get("path") or "")],
        output_artifacts=[
            path
            for path in [
                market_snapshot_authority_path,
                promoted_sleeve_manifest_path,
                overlap_path,
                edge_cluster_path,
                operator_queue_path,
                manual_trade_packet_path,
                promoted_candidate_set_path,
                candidate_lineage_path,
                candidate_consumption_audit_path,
                out_path,
                release_integrity_path,
                legacy_runtime_path,
            ]
            if path is not None
        ],
        schema_versions={"sleeve_edge_overlap_review": "v1", "operator_execution_queue": "v1", "manual_trade_packet": "v1", "aegis_lite_eod_report": "v1"},
    )
    report["run_receipt"]["producer_contract_attached"] = True
    status_path = aegis_lite_operating_status_path_v1(truth_root=truth_root, day_utc=day_utc)
    report["run_receipt"]["aegis_lite_operating_status_path"] = str(status_path)
    report["canonical_json_hash"] = None
    report["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(report)
    validate_aegis_lite_eod_report_v1(report)
    report_path = write_aegis_lite_eod_report_v1(truth_root=truth_root, payload=report)
    manifest = build_eod_run_manifest_v1(
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        trading_date=day_utc,
        release_id=str(release_integrity.get("active_release_id") or release_integrity.get("release_id") or ""),
        git_commit=str(release_integrity.get("repo_head_commit") or release_integrity.get("active_release_commit") or ""),
        repo_dirty_status=str(release_integrity.get("repo_dirty_status") or release_integrity.get("repo_dirty") or "UNKNOWN"),
        active_release_repo_match=str(release_integrity.get("release_match_status") or "UNKNOWN"),
        paths={
            "market_snapshot_artifact_path": str(market_snapshot_authority_path),
            "sleeve_eval_artifact_path": str(input_contract.get("sleeve_eval_artifact_path") or ""),
            "raw_candidate_artifact_path": str(input_contract.get("candidate_input_path") or upstream_candidate_manifest_path),
            "promoted_sleeve_manifest_path": str(promoted_sleeve_manifest_path),
            "promoted_candidate_artifact_path": str(promoted_candidate_set_path),
            "readiness_artifact_path": str(legacy_runtime_path),
            "operator_queue_artifact_path": str(operator_queue_path or ""),
            "report_artifact_path": str(report_path),
            "candidate_lineage_artifact_path": str(candidate_lineage_path),
        },
        overall_status=overall_status_from_inputs_v1(
            input_contract=input_contract,
            report=report,
            release_match_status=str(release_integrity.get("release_match_status") or ""),
        ),
        blockers=_dedupe_strings_v1(
            [
                *_strings(input_contract.get("blockers")),
                *_strings(report.get("do_not_trade_blockers")),
                *_strings(release_integrity.get("reason_codes")),
            ]
        ),
        advisory_only=str(report.get("readiness_classification") or "") == "ADVISORY_ONLY" or report.get("report_status") != "READY",
        broker_transmit_control_touched=False,
        ib_submit_automation_invoked=False,
    )
    manifest_path = write_eod_run_manifest_v1(truth_root=truth_root, payload=manifest)
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
            artifact_ref_v1(manifest_path, artifact_type="eod_run_manifest_v1"),
            artifact_ref_v1(release_integrity_path, artifact_type="aegis_release_integrity_status_v1"),
            artifact_ref_v1(legacy_runtime_path, artifact_type="legacy_paper_runtime_status_v1"),
        ],
    )
    operating_status_path = write_aegis_lite_operating_status_v1(truth_root=truth_root, payload=status)
    input_hashes = contract_input_hashes_for_paths_v1([Path(str(ref.get("path"))) for ref in source_lineage if isinstance(ref, dict) and str(ref.get("path") or "")])
    emitted_event_ids = []
    for artifact_path, payload, producer_id, validation in [
        (operator_queue_path, operator_execution_queue, "ops/tools/run_aegis_lite_eod_pipeline_v1.py:operator_execution_queue", "VALID" if operator_queue_path is not None else "MISSING"),
        (manual_trade_packet_path, manual_trade_packet, "ops/tools/run_aegis_lite_eod_pipeline_v1.py:manual_trade_packet", "VALID"),
        (report_path, report, "ops/tools/run_aegis_lite_eod_pipeline_v1.py:aegis_lite_eod_report", _eod_report_evidence_validation_v1(report)),
        (operating_status_path, status, "ops/tools/run_aegis_lite_eod_pipeline_v1.py:aegis_lite_operating_status", "VALID"),
    ]:
        if artifact_path is None:
            continue
        results = emit_artifact_evidence_transaction_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            artifact_path=Path(artifact_path),
            payload=payload,
            producer_id=producer_id,
            producer_version="v1",
            run_id=run_id,
            created_at_utc=generated_at_utc,
            input_hashes=input_hashes,
            validation_status=validation,
        )
        emitted_event_ids.extend(str(row.get("event", {}).get("event_id") or "") for row in results)
    report.setdefault("run_receipt", {})["native_evidence_event_ids"] = emitted_event_ids
    return report


def _eod_report_evidence_validation_v1(report: dict[str, Any]) -> str:
    status = str(report.get("report_status") or "").upper()
    if status in {"READY", "READY_WITH_WARNINGS"}:
        return "VALID"
    if status == "ADVISORY_ONLY" and str(report.get("manual_execution_status") or "").upper() == "NOT_READY":
        return "VALID"
    return "INVALID"


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
        "promoted_sleeve_library": promoted_sleeve_library,
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
    input_payload["candidate_input_path"] = str(input_path)
    input_payload["raw_candidates"] = input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else []
    lineage = _objects(input_payload.get("source_artifact_lineage"))
    input_payload["source_artifact_lineage"] = [*lineage, artifact_ref_v1(input_path, artifact_type="aegis_lite_candidate_input_v1")]
    if not promoted_sleeve_library_path:
        payload = _advisory_only_payload_v1(
            reason_codes=["PROMOTED_SLEEVE_LIBRARY_MISSING"],
            source_lineage=input_payload["source_artifact_lineage"],
        )
        payload["candidate_input_path"] = str(input_path)
        payload["raw_candidates"] = input_payload["raw_candidates"]
        payload["unpromoted_candidate_count"] = len(input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else [])
        return payload
    library_path = Path(promoted_sleeve_library_path).expanduser().resolve()
    filtered = filter_promoted_sleeve_candidates_v1(input_payload, _read_json(library_path))
    filtered["candidate_input_path"] = str(input_path)
    filtered["promoted_sleeve_library_path"] = str(library_path)
    filtered["raw_candidates"] = input_payload["raw_candidates"]
    filtered["source_artifact_lineage"] = [
        *(_objects(filtered.get("source_artifact_lineage"))),
        artifact_ref_v1(library_path, artifact_type="promoted_sleeve_library_v1"),
    ]
    if not filtered.get("candidates") and not filtered.get("raw_candidates"):
        filtered["raw_candidate_absent_reason"] = "NO_RAW_CANDIDATES"
        filtered["raw_candidate_status"] = "NO_RAW_CANDIDATES"
        filtered.setdefault("data_freshness_status", {"status": "PASS", "reason_codes": []})
        filtered.setdefault("governance_status", {"status": "PASS", "reason_codes": []})
        filtered.setdefault("market_regime_state", {"status": "NO_SIGNAL", "reason_codes": []})
    elif not filtered.get("candidates"):
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


def _manual_trade_packet_candidates_v1(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        direction = str(raw.get("direction") or raw.get("side") or "").strip().upper()
        side = str(raw.get("side") or ("SELL" if direction == "SHORT" else "BUY" if direction else "")).strip().upper()
        rows.append(
            {
                **raw,
                "side": side,
                "source_hypothesis_id": str(raw.get("source_hypothesis_id") or raw.get("research_hypothesis_id") or raw.get("hypothesis_id") or ""),
                "quantity_or_sizing_guidance": str(raw.get("quantity_or_sizing_guidance") or raw.get("sizing_guidance") or ""),
                "inclusion_reason": str(raw.get("inclusion_reason") or ",".join(str(item) for item in _strings(raw.get("reason_codes")))),
                "exclusion_reason": str(raw.get("exclusion_reason") or ""),
                "governance_notes": str(raw.get("governance_notes") or ""),
                "edge_overlap_result": str(raw.get("edge_overlap_result") or raw.get("edge_overlap_status") or ""),
            }
        )
    return rows


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


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _safe_run_id(run_id: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(run_id or "").strip())
    return cleaned or "aegis_lite_eod_v1"


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_OBJECT_REQUIRED:{path}")
    return payload


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return _read_json(path)
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def _upstream_candidate_manifest_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "candidate_generation_manifest_v1"
        / day_utc
        / f"sleeve_evaluation_kernel_v1:{day_utc}"
        / "candidate_generation_manifest.v1.json"
    )


def _required_symbols_v1(rows: list[dict[str, Any]]) -> list[str]:
    symbols: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol") or row.get("symbol_or_pair") or "").strip().upper()
        if not symbol:
            continue
        for part in symbol.replace("/", ",").split(","):
            item = part.strip().upper()
            if item:
                symbols.add(item)
    return sorted(symbols)


def _certified_eod_universe_v1(*, truth_root: Path, day_utc: str) -> tuple[list[str], str]:
    path = Path(truth_root).resolve() / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"
    payload = _read_json_or_empty(path)
    symbols = payload.get("final_eod_symbols")
    if not isinstance(symbols, list):
        symbols = payload.get("fetched_symbols")
    if not isinstance(symbols, list):
        raw_symbols = payload.get("symbols")
        if isinstance(raw_symbols, dict):
            symbols = list(raw_symbols.keys())
        elif isinstance(raw_symbols, list):
            symbols = raw_symbols
    if not isinstance(symbols, list):
        records = payload.get("normalized_records") if isinstance(payload.get("normalized_records"), list) else []
        symbols = [row.get("canonical_symbol") for row in records if isinstance(row, dict)]
    return sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()}), str(path if path.exists() else "")


def _operator_queue_empty_reason_v1(*, input_contract: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
    if candidates:
        return ""
    blockers = _strings(input_contract.get("blockers"))
    if "CANDIDATE_INPUT_MISSING" in blockers:
        return "CANDIDATE_INPUT_MISSING"
    if "NO_PROMOTABLE_CANDIDATES" in blockers:
        return "NO_PROMOTABLE_CANDIDATES"
    if "MISSING_PROMOTION_APPROVAL" in blockers:
        return "MISSING_PROMOTION_APPROVAL"
    if "PROMOTED_SLEEVE_LIBRARY_REQUIRED" in blockers:
        return "PROMOTED_SLEEVE_LIBRARY_REQUIRED"
    if str(input_contract.get("raw_candidate_status") or "").upper() == "NO_RAW_CANDIDATES":
        return "NO_RAW_CANDIDATES_GENERATED"
    return "NO_EXECUTABLE_CANDIDATES"


def _dedupe_strings_v1(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


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


def _remove_status_reason_v1(input_payload: dict[str, Any], reason_code: str) -> None:
    for key in ("data_freshness_status", "governance_status", "market_regime_state"):
        status = input_payload.get(key)
        if not isinstance(status, dict):
            continue
        reasons = status.get("reason_codes")
        if isinstance(reasons, list):
            status["reason_codes"] = [str(item) for item in reasons if str(item) != reason_code]


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
    parser.add_argument("--run-mode", "--run_mode", dest="run_mode", choices=["INTRADAY_OPERATIONAL", "FINAL_EOD_CERTIFIED"], default="INTRADAY_OPERATIONAL")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    generated_at_utc = str(args.generated_at_utc or now_utc_iso_v1())
    default_run_suffix = generated_at_utc.replace(":", "").replace("-", "").replace("T", "_").replace("Z", "Z")
    run_id = str(args.run_id or f"aegis_lite_eod_v1:{day_utc}:{default_run_suffix}")
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
        run_mode=str(args.run_mode or "INTRADAY_OPERATIONAL"),
    )
    print(
        json.dumps(
            {
                "status": report["report_status"],
                "manual_execution_status": report["manual_execution_status"],
                "readiness_classification": report["readiness_classification"],
                "path": report["artifact_path"],
                "operating_status_path": report["run_receipt"].get("aegis_lite_operating_status_path", ""),
                "event_ids": report["run_receipt"].get("native_evidence_event_ids", []),
                "broker_submit_required": False,
                "manual_execution_only": True,
                "run_mode": str(args.run_mode or "INTRADAY_OPERATIONAL"),
                "market_data_mode": str(args.run_mode or "INTRADAY_OPERATIONAL"),
                "trade_advice_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    if args.allow_not_ready_exit_zero:
        return 0
    return 0 if report["report_status"] in {"READY", "READY_WITH_WARNINGS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
