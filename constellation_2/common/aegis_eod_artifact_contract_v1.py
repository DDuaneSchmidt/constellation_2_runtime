from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_eod_v1 import artifact_ref_v1, sha256_file_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
EOD_RUN_MANIFEST_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/eod_run_manifest.v1.schema.json"
CANDIDATE_LINEAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_lineage.v1.schema.json"
CANDIDATE_CONSUMPTION_AUDIT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_consumption_audit.v1.schema.json"
PROMOTED_SLEEVE_MANIFEST_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/promoted_sleeve_manifest.v1.schema.json"
MARKET_SNAPSHOT_AUTHORITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/market_snapshot_authority.v1.schema.json"

EOD_STATUS_INPUT_CONTRACT_FAILED = "INPUT_CONTRACT_FAILED"
EOD_STATUS_DATA_NOT_READY = "DATA_NOT_READY"
EOD_STATUS_RUNTIME_NOT_READY = "RUNTIME_NOT_READY"
EOD_STATUS_RELEASE_MISMATCH = "RELEASE_MISMATCH"
EOD_STATUS_ADVISORY_ONLY = "ADVISORY_ONLY"
EOD_STATUS_BLOCKED = "BLOCKED"
EOD_STATUS_SUCCESS = "SUCCESS"


def eod_run_manifest_path_v1(*, truth_root: Path, trading_date: str, run_id: str) -> Path:
    return _artifact_path(truth_root=truth_root, family="eod_run_manifest_v1", trading_date=trading_date, run_id=run_id, filename="eod_run_manifest.v1.json")


def candidate_lineage_path_v1(*, truth_root: Path, trading_date: str, run_id: str) -> Path:
    return _artifact_path(truth_root=truth_root, family="candidate_lineage_v1", trading_date=trading_date, run_id=run_id, filename="candidate_lineage.v1.json")


def candidate_consumption_audit_path_v1(*, truth_root: Path, trading_date: str, run_id: str) -> Path:
    return _artifact_path(
        truth_root=truth_root,
        family="candidate_consumption_audit_v1",
        trading_date=trading_date,
        run_id=run_id,
        filename="candidate_consumption_audit.v1.json",
    )


def promoted_sleeve_manifest_path_v1(*, truth_root: Path, trading_date: str, run_id: str) -> Path:
    return _artifact_path(truth_root=truth_root, family="promoted_sleeve_manifest_v1", trading_date=trading_date, run_id=run_id, filename="promoted_sleeve_manifest.v1.json")


def market_snapshot_authority_path_v1(*, truth_root: Path, trading_date: str, run_id: str) -> Path:
    return _artifact_path(truth_root=truth_root, family="market_snapshot_authority_v1", trading_date=trading_date, run_id=run_id, filename="market_snapshot_authority.v1.json")


def build_market_snapshot_authority_v1(
    *,
    truth_root: Path,
    trading_date: str,
    run_id: str,
    created_at_utc: str,
    required_symbols: list[str],
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    manifest_path = root / "market_data_snapshot_v1" / "dataset_manifest.json"
    required = sorted({str(symbol or "").strip().upper() for symbol in required_symbols if str(symbol or "").strip()})
    fresh: list[str] = []
    stale: list[str] = []
    missing: list[str] = []
    missing_bars: list[str] = []
    for symbol in required:
        symbol_path = root / "market_data_snapshot_v1" / symbol / f"{trading_date[:4]}.jsonl"
        if not symbol_path.exists():
            missing.append(symbol)
            missing_bars.append(f"{symbol}:{trading_date}")
            continue
        dates = _bar_dates(symbol_path)
        if trading_date in dates:
            fresh.append(symbol)
        else:
            stale.append(symbol)
            missing_bars.append(f"{symbol}:{trading_date}")
    if not manifest_path.exists():
        status = "MISSING"
    elif not required:
        status = "COHERENT"
    elif missing:
        status = "DATA_NOT_READY"
    elif stale:
        status = "PARTIAL"
    else:
        status = "COHERENT"
    payload = {
        "schema_id": "market_snapshot_authority",
        "schema_version": "v1",
        "artifact_id": "market_snapshot_authority_v1",
        "snapshot_id": f"market_snapshot:{trading_date}:{run_id}",
        "trading_date": trading_date,
        "run_id": run_id,
        "dataset_manifest_path": str(manifest_path),
        "required_symbols": required,
        "fresh_symbols": sorted(fresh),
        "stale_symbols": sorted(stale),
        "missing_symbols": sorted(missing),
        "missing_bars": sorted(missing_bars),
        "snapshot_status": status,
        "created_at_utc": created_at_utc,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_promoted_sleeve_manifest_v1(
    *,
    trading_date: str,
    run_id: str,
    generated_at_utc: str,
    promoted_sleeve_library: dict[str, Any] | None,
    source_artifact: str = "",
) -> dict[str, Any]:
    library = promoted_sleeve_library or {}
    sleeves = library.get("promoted_sleeves") if isinstance(library.get("promoted_sleeves"), list) else library.get("sleeves")
    rows: list[dict[str, Any]] = []
    for idx, sleeve in enumerate(sleeves if isinstance(sleeves, list) else [], start=1):
        if not isinstance(sleeve, dict):
            continue
        sleeve_id = _text(sleeve.get("sleeve_id"))
        rows.append(
            {
                "manifest_id": f"promoted_sleeve:{trading_date}:{sleeve_id or idx}",
                "effective_date": _text(sleeve.get("effective_date") or trading_date),
                "generated_at_utc": generated_at_utc,
                "sleeve_id": sleeve_id,
                "sleeve_version": _text(sleeve.get("sleeve_version") or sleeve.get("version") or "v1"),
                "promotion_status": _text(sleeve.get("promotion_status") or "unknown"),
                "allowed_symbols": _strings(sleeve.get("allowed_symbols") or sleeve.get("approved_symbols")),
                "account_mode": _text(sleeve.get("account_mode") or sleeve.get("environment") or "PAPER"),
                "expiry_date": _text(sleeve.get("expiry_date")),
                "promotion_reason": _text(sleeve.get("promotion_reason") or ",".join(_strings(sleeve.get("operational_constraints")))),
                "risk_approval_status": _text(sleeve.get("risk_approval_status") or sleeve.get("human_approval_status") or sleeve.get("approval_status")),
                "source_artifact": source_artifact,
                "checksum": canonical_hash_for_c2_artifact_v1({key: value for key, value in sleeve.items() if key != "canonical_json_hash"}),
            }
        )
    effective_rows = [row for row in rows if row["sleeve_id"] and row["promotion_status"].lower() == "promoted"]
    payload = {
        "schema_id": "promoted_sleeve_manifest",
        "schema_version": "v1",
        "artifact_id": "promoted_sleeve_manifest_v1",
        "manifest_id": f"promoted_sleeve_manifest:{trading_date}:{run_id}",
        "trading_date": trading_date,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "manifest_status": "EFFECTIVE" if effective_rows else "INEFFECTIVE",
        "promoted_sleeve_count": len(effective_rows),
        "promoted_sleeves": rows,
        "source_artifact": source_artifact,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_eod_input_contract_v1(
    *,
    trading_date: str,
    input_payload: dict[str, Any],
    market_snapshot_authority: dict[str, Any],
    promoted_sleeve_manifest: dict[str, Any],
    upstream_candidate_manifest_path: Path,
) -> dict[str, Any]:
    blockers: list[str] = []
    missing_artifacts: list[dict[str, str]] = []
    raw_absent_reason = _text(input_payload.get("raw_candidate_absent_reason"))
    candidate_input_path = _text(input_payload.get("candidate_input_path"))
    promoted_library_path = _text(input_payload.get("promoted_sleeve_library_path"))
    supplied_candidates = input_payload.get("candidates") if isinstance(input_payload.get("candidates"), list) else []
    raw_candidates = input_payload.get("raw_candidates") if isinstance(input_payload.get("raw_candidates"), list) else []
    raw_candidate_count = len(raw_candidates)
    artifact_inputs_required = bool(upstream_candidate_manifest_path.exists() or input_payload.get("enforce_eod_input_contract"))
    market_snapshot_required = bool(artifact_inputs_required or supplied_candidates)
    sleeve_eval_path = _text(input_payload.get("sleeve_eval_artifact_path")) or _default_sleeve_eval_path(input_payload, trading_date)
    readiness_path = _text(input_payload.get("readiness_artifact_path"))
    market_path = _text(market_snapshot_authority.get("dataset_manifest_path"))

    if market_snapshot_required:
        _require_path("market_snapshot_artifact_path", market_path, blockers, missing_artifacts, "MARKET_SNAPSHOT_MISSING")
    if artifact_inputs_required and not _path_exists(sleeve_eval_path) and not _text(input_payload.get("sleeve_eval_absent_reason")):
        blockers.append("SLEEVE_EVALUATION_INPUT_MISSING")
        missing_artifacts.append({"logical_name": "sleeve_eval_artifact_path", "path": sleeve_eval_path})
    if not supplied_candidates and raw_candidate_count > 0:
        blockers.append("NO_PROMOTABLE_CANDIDATES")
        if str(promoted_sleeve_manifest.get("manifest_status") or "") != "EFFECTIVE":
            blockers.append("MISSING_PROMOTION_APPROVAL")
    elif not supplied_candidates and not candidate_input_path and not raw_absent_reason:
        blockers.append("CANDIDATE_INPUT_MISSING")
        if upstream_candidate_manifest_path.exists():
            blockers.append("RAW_CANDIDATES_EXISTED_NOT_CONSUMED")
        missing_artifacts.append({"logical_name": "raw_candidate_artifact_path", "path": candidate_input_path})
    elif candidate_input_path and not _path_exists(candidate_input_path):
        blockers.append("CANDIDATE_INPUT_MISSING")
        missing_artifacts.append({"logical_name": "raw_candidate_artifact_path", "path": candidate_input_path})
    if not promoted_library_path and str(promoted_sleeve_manifest.get("manifest_status") or "") != "EFFECTIVE":
        blockers.append("PROMOTED_SLEEVE_LIBRARY_REQUIRED")
        missing_artifacts.append({"logical_name": "promoted_sleeve_manifest_path", "path": ""})
    elif promoted_library_path and not _path_exists(promoted_library_path):
        blockers.append("PROMOTED_SLEEVE_LIBRARY_REQUIRED")
        missing_artifacts.append({"logical_name": "promoted_sleeve_manifest_path", "path": promoted_library_path})
    if artifact_inputs_required and str(promoted_sleeve_manifest.get("manifest_status") or "") != "EFFECTIVE":
        blockers.append("PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE")
    if artifact_inputs_required and readiness_path and not _path_exists(readiness_path):
        blockers.append("READINESS_ARTIFACT_MISSING")
        missing_artifacts.append({"logical_name": "readiness_artifact_path", "path": readiness_path})
    elif artifact_inputs_required and not readiness_path and input_payload.get("manual_execution_expected", True):
        blockers.append("READINESS_ARTIFACT_MISSING")
        missing_artifacts.append({"logical_name": "readiness_artifact_path", "path": ""})
    snapshot_status = str(market_snapshot_authority.get("snapshot_status") or "MISSING")
    if market_snapshot_required and snapshot_status in {"PARTIAL", "STALE", "MISSING", "DATA_NOT_READY"}:
        blockers.append(f"MARKET_SNAPSHOT_{snapshot_status}")
    promotion_only_blockers = {"NO_PROMOTABLE_CANDIDATES", "MISSING_PROMOTION_APPROVAL", "PROMOTED_SLEEVE_LIBRARY_REQUIRED", "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE"}
    contract_status = "PASS" if not blockers else "NO_PROMOTABLE_CANDIDATES" if set(_dedupe(blockers)).issubset(promotion_only_blockers) and raw_candidate_count > 0 else EOD_STATUS_INPUT_CONTRACT_FAILED
    return {
        "schema_id": "eod_input_contract",
        "schema_version": "v1",
        "trading_date": trading_date,
        "status": contract_status,
        "blockers": _dedupe(blockers),
        "missing_artifacts": missing_artifacts,
        "candidate_input_path": candidate_input_path,
        "raw_candidate_absent_reason": raw_absent_reason,
        "raw_candidate_status": _text(input_payload.get("raw_candidate_status")),
        "raw_candidate_count": raw_candidate_count,
        "report_candidate_count": len(supplied_candidates),
        "upstream_candidate_manifest_path": str(upstream_candidate_manifest_path),
        "upstream_candidate_manifest_exists": upstream_candidate_manifest_path.exists(),
        "promoted_sleeve_library_path": promoted_library_path,
        "sleeve_eval_artifact_path": sleeve_eval_path,
        "readiness_artifact_path": readiness_path,
        "market_snapshot_artifact_path": market_path,
    }


def build_candidate_lineage_v1(
    *,
    trading_date: str,
    run_id: str,
    created_at_utc: str,
    input_payload: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    consumed_candidates: list[dict[str, Any]],
    promoted_sleeve_manifest_path: str,
    input_contract: dict[str, Any],
    readiness_status: str,
) -> dict[str, Any]:
    consumed_ids = {_text(row.get("candidate_id")) for row in consumed_candidates if isinstance(row, dict)}
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_rows, start=1):
        candidate_id = _text(raw.get("candidate_id")) or _text(raw.get("raw_intent_id")) or f"RAW_{idx:03d}"
        sleeve_id = _text(raw.get("sleeve_id") or raw.get("engine_id"))
        symbol = _text(raw.get("symbol") or raw.get("symbol_or_pair")).upper()
        block_reasons = _dedupe([*_strings(raw.get("reason_codes")), *_strings(raw.get("block_reasons")), *_strings(input_contract.get("blockers"))])
        consumed = candidate_id in consumed_ids
        has_raw_intent = bool(_text(raw.get("raw_intent_id")) or _text(raw.get("intent_id")) or _text(raw.get("candidate_id")))
        state = "RAW_CREATED" if has_raw_intent else "PROMOTION_SKIPPED"
        final_state = "NOT_CONSUMED_BY_EOD" if not consumed and has_raw_intent else ("ADVISORY_ONLY" if block_reasons else "PROMOTED")
        rows.append(
            {
                "candidate_id": candidate_id,
                "trading_date": trading_date,
                "sleeve_id": sleeve_id,
                "sleeve_version": _text(raw.get("sleeve_version") or "v1"),
                "symbol": symbol,
                "candidate_source": _text(raw.get("candidate_source") or raw.get("source") or "sleeve_evaluation_kernel_v1"),
                "raw_created": has_raw_intent,
                "raw_candidate_artifact_path": _text(raw.get("raw_intent_path") or raw.get("raw_candidate_artifact_path")),
                "promotion_status": _text(raw.get("promotion_status") or ("not_consumed" if not consumed else "promoted")),
                "promotion_reason": _text(raw.get("promotion_reason") or raw.get("rejection_reason") or ""),
                "promoted_sleeve_manifest_path": promoted_sleeve_manifest_path,
                "readiness_status": readiness_status,
                "execution_eligibility_status": "BLOCKED" if block_reasons else "UNKNOWN",
                "block_reasons": block_reasons,
                "final_state": final_state if final_state != "PROMOTED" else state,
                "consumed_by_eod": consumed,
                "shown_in_report": consumed,
                "created_at_utc": created_at_utc,
            }
        )
    payload = {
        "schema_id": "candidate_lineage",
        "schema_version": "v1",
        "artifact_id": "candidate_lineage_v1",
        "trading_date": trading_date,
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "candidate_count": len(rows),
        "lineage_rows": rows,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_candidate_consumption_audit_v1(
    *,
    trading_date: str,
    run_id: str,
    created_at_utc: str,
    raw_rows: list[dict[str, Any]],
    consumed_candidates: list[dict[str, Any]],
    promoted_sleeve_manifest: dict[str, Any],
    certified_symbols: list[str],
    certified_artifact_path: str,
    input_contract: dict[str, Any],
) -> dict[str, Any]:
    consumed_ids = {_text(row.get("candidate_id")) for row in consumed_candidates if isinstance(row, dict)}
    covered = {symbol.upper() for symbol in _strings(certified_symbols)}
    manifest_status = _text(promoted_sleeve_manifest.get("manifest_status")).upper()
    promoted_sleeves = promoted_sleeve_manifest.get("promoted_sleeves") if isinstance(promoted_sleeve_manifest.get("promoted_sleeves"), list) else []
    promoted_sleeve_ids = {
        _text(row.get("sleeve_id"))
        for row in promoted_sleeves
        if isinstance(row, dict) and _text(row.get("promotion_status")).lower() == "promoted"
    }
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_rows, start=1):
        if not isinstance(raw, dict):
            continue
        candidate_id = _text(raw.get("candidate_id")) or _text(raw.get("raw_intent_id")) or f"RAW_{idx:03d}"
        symbol = _text(raw.get("symbol") or raw.get("symbol_or_pair")).upper()
        sleeve_id = _text(raw.get("sleeve_id") or raw.get("engine_id"))
        source_status = _text(raw.get("status") or raw.get("candidate_status") or raw.get("final_state")).upper()
        raw_reasons = _dedupe([*_strings(raw.get("reason_codes")), *_strings(raw.get("block_reasons"))])
        category, reason = _candidate_consumption_category_v1(
            candidate_id=candidate_id,
            symbol=symbol,
            sleeve_id=sleeve_id,
            source_status=source_status,
            raw_reasons=raw_reasons,
            consumed_ids=consumed_ids,
            covered_symbols=covered,
            manifest_status=manifest_status,
            promoted_sleeve_ids=promoted_sleeve_ids,
            certified_artifact_path=certified_artifact_path,
        )
        rows.append(
            {
                "candidate_id": candidate_id,
                "symbol": symbol,
                "sleeve_id": sleeve_id,
                "source_status": source_status or "UNKNOWN",
                "consumption_category": category,
                "consumption_reason": reason,
                "covered_by_certified_eod": bool(symbol and symbol in covered),
                "promoted_sleeve_approved": bool(sleeve_id and sleeve_id in promoted_sleeve_ids),
                "raw_reason_codes": raw_reasons,
                "source_artifact_path": _text(raw.get("raw_intent_path") or raw.get("raw_candidate_artifact_path")),
            }
        )
    counts: dict[str, int] = {}
    for row in rows:
        category = str(row.get("consumption_category") or "UNKNOWN")
        counts[category] = counts.get(category, 0) + 1
    payload = {
        "schema_id": "candidate_consumption_audit",
        "schema_version": "v1",
        "artifact_id": "candidate_consumption_audit_v1",
        "trading_date": trading_date,
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "raw_candidate_count": len(rows),
        "promoted_candidate_count": counts.get("PROMOTED", 0),
        "excluded_candidate_count": len(rows) - counts.get("PROMOTED", 0),
        "consumption_counts": dict(sorted(counts.items())),
        "certified_universe_symbol_count": len(covered),
        "certified_universe_symbols": sorted(covered),
        "certified_artifact_path": certified_artifact_path,
        "promotion_contract": {
            "promoted_sleeve_manifest_status": manifest_status or "UNKNOWN",
            "promoted_sleeve_count": int(promoted_sleeve_manifest.get("promoted_sleeve_count") or 0),
            "promoted_sleeve_ids": sorted(promoted_sleeve_ids),
            "input_contract_status": _text(input_contract.get("status")),
            "input_contract_blockers": _strings(input_contract.get("blockers")),
        },
        "candidate_rows": rows,
        "normal_no_op": bool(rows and counts.get("PROMOTED", 0) == 0 and set(_strings(input_contract.get("blockers"))).issubset({"NO_PROMOTABLE_CANDIDATES", "MISSING_PROMOTION_APPROVAL", "PROMOTED_SLEEVE_LIBRARY_REQUIRED", "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE"})),
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _candidate_consumption_category_v1(
    *,
    candidate_id: str,
    symbol: str,
    sleeve_id: str,
    source_status: str,
    raw_reasons: list[str],
    consumed_ids: set[str],
    covered_symbols: set[str],
    manifest_status: str,
    promoted_sleeve_ids: set[str],
    certified_artifact_path: str,
) -> tuple[str, str]:
    reason_set = {reason.upper() for reason in raw_reasons}
    if candidate_id in consumed_ids:
        return "PROMOTED", "Candidate passed promotion governance and was consumed by the EOD report."
    if not certified_artifact_path:
        return "EXCLUDED_MISSING_CERTIFIED_DATA", "No certified EOD artifact was available for candidate coverage validation."
    if symbol and symbol not in covered_symbols:
        return "EXCLUDED_UNCOVERED_SYMBOL", f"{symbol} is outside the certified EOD universe used by the report."
    if source_status in {"NO_SIGNAL", "SUPPRESSED"} or reason_set.intersection({"NO_RAW_SIGNAL", "NO_INTENT_DECLARED", "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"}):
        return "EXCLUDED_LOW_SCORE", "Raw candidate did not present a promotable signal for the EOD report."
    if manifest_status != "EFFECTIVE":
        return "EXCLUDED_POLICY", "No effective promoted sleeve manifest exists for EOD report promotion."
    if sleeve_id and sleeve_id not in promoted_sleeve_ids:
        return "EXCLUDED_POLICY", "Candidate sleeve is not present in the approved promoted sleeve manifest."
    return "EXCLUDED_POLICY", "Candidate did not satisfy promotion policy for EOD report consumption."


def build_synthetic_advisory_rows_v1(*, lineage: dict[str, Any], input_contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in lineage.get("lineage_rows", []):
        if not isinstance(row, dict) or bool(row.get("consumed_by_eod")):
            continue
        rows.append(
            {
                "candidate_id": _text(row.get("candidate_id")),
                "candidate_source": _text(row.get("candidate_source")),
                "symbol": _text(row.get("symbol")),
                "sleeve_id": _text(row.get("sleeve_id")),
                "final_state": _text(row.get("final_state") or "NOT_CONSUMED_BY_EOD"),
                "reason_not_executable": ",".join(_strings(row.get("block_reasons"))) or "CANDIDATE_NOT_CONSUMED_BY_EOD",
                "reason_not_shown_as_manual_trade": ",".join(_strings(input_contract.get("blockers"))) or "NOT_IN_OPERATOR_QUEUE",
                "block_reasons": _strings(row.get("block_reasons")),
                "source_artifact_refs": [
                    artifact_ref_v1(row["raw_candidate_artifact_path"], artifact_type="raw_candidate_artifact")
                    for _ in [0]
                    if _text(row.get("raw_candidate_artifact_path"))
                ],
            }
        )
    return rows


def build_eod_run_manifest_v1(
    *,
    run_id: str,
    generated_at_utc: str,
    trading_date: str,
    release_id: str,
    git_commit: str,
    repo_dirty_status: str,
    active_release_repo_match: str,
    paths: dict[str, str],
    overall_status: str,
    blockers: list[str],
    advisory_only: bool,
    broker_transmit_control_touched: bool,
    ib_submit_automation_invoked: bool,
) -> dict[str, Any]:
    payload = {
        "schema_id": "eod_run_manifest",
        "schema_version": "v1",
        "artifact_id": "eod_run_manifest_v1",
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "trading_date": trading_date,
        "release_id": release_id,
        "git_commit": git_commit,
        "repo_dirty_status": repo_dirty_status,
        "active_release_repo_match": active_release_repo_match,
        "market_snapshot_artifact_path": paths.get("market_snapshot_artifact_path", ""),
        "sleeve_eval_artifact_path": paths.get("sleeve_eval_artifact_path", ""),
        "raw_candidate_artifact_path": paths.get("raw_candidate_artifact_path", ""),
        "promoted_sleeve_manifest_path": paths.get("promoted_sleeve_manifest_path", ""),
        "promoted_candidate_artifact_path": paths.get("promoted_candidate_artifact_path", ""),
        "readiness_artifact_path": paths.get("readiness_artifact_path", ""),
        "operator_queue_artifact_path": paths.get("operator_queue_artifact_path", ""),
        "report_artifact_path": paths.get("report_artifact_path", ""),
        "candidate_lineage_artifact_path": paths.get("candidate_lineage_artifact_path", ""),
        "overall_status": overall_status,
        "blockers": _dedupe(blockers),
        "advisory_only": bool(advisory_only),
        "broker_transmit_control_touched": bool(broker_transmit_control_touched),
        "ib_submit_automation_invoked": bool(ib_submit_automation_invoked),
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def write_market_snapshot_authority_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_against_repo_schema_v1(payload, REPO_ROOT, MARKET_SNAPSHOT_AUTHORITY_SCHEMA)
    path = market_snapshot_authority_path_v1(truth_root=truth_root, trading_date=str(payload["trading_date"]), run_id=str(payload["run_id"]))
    return _write_immutable_json(path, payload)


def write_promoted_sleeve_manifest_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_against_repo_schema_v1(payload, REPO_ROOT, PROMOTED_SLEEVE_MANIFEST_SCHEMA)
    path = promoted_sleeve_manifest_path_v1(truth_root=truth_root, trading_date=str(payload["trading_date"]), run_id=str(payload["run_id"]))
    return _write_immutable_json(path, payload)


def write_candidate_lineage_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_against_repo_schema_v1(payload, REPO_ROOT, CANDIDATE_LINEAGE_SCHEMA)
    path = candidate_lineage_path_v1(truth_root=truth_root, trading_date=str(payload["trading_date"]), run_id=str(payload["run_id"]))
    return _write_immutable_json(path, payload)


def write_candidate_consumption_audit_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_against_repo_schema_v1(payload, REPO_ROOT, CANDIDATE_CONSUMPTION_AUDIT_SCHEMA)
    path = candidate_consumption_audit_path_v1(truth_root=truth_root, trading_date=str(payload["trading_date"]), run_id=str(payload["run_id"]))
    return _write_immutable_json(path, payload)


def write_eod_run_manifest_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_against_repo_schema_v1(payload, REPO_ROOT, EOD_RUN_MANIFEST_SCHEMA)
    path = eod_run_manifest_path_v1(truth_root=truth_root, trading_date=str(payload["trading_date"]), run_id=str(payload["run_id"]))
    return _write_immutable_json(path, payload)


def load_upstream_candidate_rows_v1(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(payload, dict) and isinstance(payload.get("candidate_rows"), list):
        return [row for row in payload["candidate_rows"] if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("candidates"), list):
        return [row for row in payload["candidates"] if isinstance(row, dict)]
    return []


def overall_status_from_inputs_v1(*, input_contract: dict[str, Any], report: dict[str, Any], release_match_status: str) -> str:
    if str(input_contract.get("status") or "") == EOD_STATUS_INPUT_CONTRACT_FAILED:
        return EOD_STATUS_INPUT_CONTRACT_FAILED
    if str(release_match_status or "") == "MISMATCH":
        return EOD_STATUS_RELEASE_MISMATCH
    if str((report.get("data_freshness_status") or {}).get("status") or "") in {"BLOCKED", "STALE", "DATA_NOT_READY", "PARTIAL"}:
        return EOD_STATUS_DATA_NOT_READY
    if str(report.get("manual_execution_status") or "") == "NOT_READY" and report.get("selected_trade_candidates"):
        return EOD_STATUS_RUNTIME_NOT_READY
    if str(report.get("readiness_classification") or "") == "ADVISORY_ONLY":
        return EOD_STATUS_ADVISORY_ONLY
    if str(report.get("report_status") or "") == "BLOCKED":
        return EOD_STATUS_BLOCKED
    return EOD_STATUS_SUCCESS


def _default_sleeve_eval_path(input_payload: dict[str, Any], trading_date: str) -> str:
    source_path = _text(input_payload.get("source_rollup_path"))
    if source_path:
        return source_path
    return _text(input_payload.get("sleeve_evaluation_rollup_path"))


def _bar_dates(path: Path) -> set[str]:
    dates: set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                if isinstance(row, dict):
                    day = _text(row.get("day_utc") or row.get("date") or row.get("trading_day") or _text(row.get("timestamp_utc"))[:10])
                    if day:
                        dates.add(day)
    except (OSError, json.JSONDecodeError):
        return dates
    return dates


def _artifact_path(*, truth_root: Path, family: str, trading_date: str, run_id: str, filename: str) -> Path:
    return Path(truth_root).resolve() / "reports" / family / trading_date / _safe_run_id(run_id) / filename


def _write_immutable_json(path: Path, payload: dict[str, Any]) -> Path:
    data = canonical_json_bytes_v1(payload) + b"\n"
    if path.exists():
        if path.read_bytes() == data:
            return path
        content_hash = hashlib.sha256(data).hexdigest()[:16]
        versioned_parent = path.parent.parent / f"{path.parent.name}__{content_hash}"
        versioned_path = versioned_parent / path.name
        if versioned_path.exists():
            if versioned_path.read_bytes() == data:
                return versioned_path
            raise FileExistsError(f"REFUSE_OVERWRITE_EXISTING_ARTIFACT_VERSION_COLLISION:{versioned_path}")
        versioned_path.parent.mkdir(parents=True, exist_ok=True)
        versioned_path.write_bytes(data)
        return versioned_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _require_path(logical_name: str, path: str, blockers: list[str], missing: list[dict[str, str]], reason: str) -> None:
    if not path or not _path_exists(path):
        blockers.append(reason)
        missing.append({"logical_name": logical_name, "path": path})


def _path_exists(path: str) -> bool:
    return bool(path) and Path(path).expanduser().exists()


def _safe_run_id(run_id: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(run_id or "").strip())
    return cleaned or "aegis_eod_artifact_contract_v1"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    return [_text(value)] if _text(value) else []


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _text(value)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out
