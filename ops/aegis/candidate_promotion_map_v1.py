from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPORT_FAMILY = "candidate_promotion_map_v1"
SCHEMA_ID = "candidate_promotion_map"
SCHEMA_VERSION = "v1"

ACTIVE_STATUSES = {"CANDIDATE_CREATED", "INTENT_CREATED", "CAPTURE_READY", "ACTIVE_CURRENT"}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _sha(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _latest_candidate_manifest(root: Path, day: str) -> Path:
    base = root / "reports" / "candidate_generation_manifest_v1" / day
    paths = sorted(base.glob("*/candidate_generation_manifest.v1.json")) if base.exists() else []
    if not paths:
        return base / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    return max(paths, key=lambda p: (1 if p.parent.name.startswith(f"aegis_intraday_sleeves_now:{day}:") else 0, p.stat().st_mtime_ns, p.parent.name))


def _path(root: Path, day: str, family: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def candidate_promotion_map_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_promotion_map.v1.json"


def candidate_promotion_map_txt_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_promotion_map.v1.txt"


def _score_rows(scoring: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else []:
        if isinstance(row, dict) and str(row.get("intent_id") or ""):
            out[str(row.get("intent_id"))] = row
    coverage = scoring.get("candidate_coverage") if isinstance(scoring.get("candidate_coverage"), list) else []
    for row in coverage:
        if isinstance(row, dict) and str(row.get("raw_intent_id") or ""):
            out.setdefault(str(row.get("raw_intent_id")), row)
    return out


def _arbitration_rows(arbitration: dict[str, Any]) -> tuple[str, dict[str, dict[str, Any]]]:
    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    selected_id = str(selected.get("intent_id") or "")
    out: dict[str, dict[str, Any]] = {}
    for key in ("candidate_intents", "rejected_or_filtered_intents", "raw_candidate_intents"):
        rows = arbitration.get(key) if isinstance(arbitration.get(key), list) else []
        for row in rows:
            if isinstance(row, dict):
                intent_id = str(row.get("intent_id") or row.get("raw_intent_id") or "")
                if intent_id:
                    out[intent_id] = {**out.get(intent_id, {}), **row}
    if selected_id:
        out[selected_id] = {**out.get(selected_id, {}), **selected}
    return selected_id, out


def _promotion_blocker(promotion: dict[str, Any]) -> tuple[str, str]:
    status = str(promotion.get("status") or "").upper()
    if status in {"PROMOTED_TO_OPERATOR_REVIEW", "PASS", "VALID"}:
        return "", ""
    missing_values = promotion.get("missing_contract_fields") if isinstance(promotion.get("missing_contract_fields"), list) else []
    missing = [str(x) for x in missing_values]
    if any(item.startswith("market.price") for item in missing):
        return "SELECTED_SYMBOL_MARKET_DATA_MISSING", "Fetch current intraday market data for the selected symbol, then rerun promotion."
    if any(item.startswith("selected_intent") for item in missing):
        return "SELECTED_INTENT_CONTRACT_INVALID", "Regenerate selected intent arbitration from governed candidate rows."
    if missing:
        return "SELECTED_INTENT_PROMOTION_CONTRACT_FAILED", "Repair the failed selected-intent promotion contract fields."
    reason = str(promotion.get("rejection_reason") or "")
    if reason:
        return reason, "Inspect selected-intent promotion report and rerun the governed promotion chain."
    return "SELECTED_INTENT_PROMOTION_MISSING", "Build selected-intent promotion before manual capture readiness."


def _status_from_promotion(promotion: dict[str, Any]) -> str:
    return str(promotion.get("status") or "MISSING").upper()


def _ticket_statuses(root: Path, day: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    conversions: dict[str, dict[str, Any]] = {}
    for path in sorted((root / "reports" / "paper_conversion_evidence_v1" / day).glob("*/paper_conversion_evidence.v1.json")):
        payload = _read(path)
        for key in ("candidate_id", "intent_id", "exposure_id", "source_candidate_id"):
            value = str(payload.get(key) or "")
            if value:
                conversions[value] = {"path": str(path), "hash": _sha(path), "payload": payload}
    submits: dict[str, dict[str, Any]] = {}
    for path in sorted((root / "reports" / "submit_boundary_precheck_v1" / day).glob("*/submit_boundary_precheck.v1.json")):
        payload = _read(path)
        for key in ("candidate_id", "intent_id", "exposure_id", "ticket_id"):
            value = str(payload.get(key) or "")
            if value:
                submits[value] = {"path": str(path), "hash": _sha(path), "payload": payload}
    lineages: dict[str, dict[str, Any]] = {}
    for path in sorted((root / "reports" / "trade_ticket_lineage_v1" / day).glob("*/trade_ticket_lineage.v1.json")):
        payload = _read(path)
        for key in ("candidate_id", "intent_id", "exposure_id", "ticket_id"):
            value = str(payload.get(key) or "")
            if value:
                lineages[value] = {"path": str(path), "hash": _sha(path), "payload": payload}
    return conversions, submits, lineages


def _find_status(mapping: dict[str, dict[str, Any]], *ids: str) -> dict[str, Any]:
    for value in ids:
        if value and value in mapping:
            return mapping[value]
    return {}


def build_candidate_promotion_map_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    manifest_path = _latest_candidate_manifest(root, day_utc)
    scoring_path = _path(root, day_utc, "portfolio_scoring_v1", "portfolio_scoring.v1.json")
    arbitration_path = _path(root, day_utc, "intent_arbitration_v1", "intent_arbitration.v1.json")
    promotion_path = _path(root, day_utc, "aegis_selected_intent_promotion_v1", "selected_intent_promotion.v1.json")
    pointer_path = root / "pointers" / "selected_intent_pointer.v1.json"
    manifest = _read(manifest_path)
    scoring = _read(scoring_path)
    arbitration = _read(arbitration_path)
    promotion = _read(promotion_path)
    pointer = _read(pointer_path)
    score_by_intent = _score_rows(scoring)
    selected_id, arbitration_by_intent = _arbitration_rows(arbitration)
    conversions, submits, lineages = _ticket_statuses(root, day_utc)
    rows = manifest.get("candidate_rows") if isinstance(manifest.get("candidate_rows"), list) else []
    promotion_blocker, promotion_action = _promotion_blocker(promotion)
    manifest_lane = str(manifest.get("candidate_lane") or manifest.get("candidate_visibility_lane") or "").upper()
    manifest_certification_state = str(manifest.get("certification_state") or manifest.get("final_eod_certification_status") or "").upper()
    manifest_execution_eligible = bool(manifest.get("execution_eligible") is True and manifest_lane == "CERTIFIED" and manifest_certification_state == "CERTIFIED")
    manifest_input_snapshot_ids = manifest.get("input_market_data_snapshot_ids") if isinstance(manifest.get("input_market_data_snapshot_ids"), list) else []
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_intent_id = str(row.get("raw_intent_id") or row.get("intent_id") or "")
        candidate_id = str(row.get("candidate_id") or raw_intent_id or "")
        score = score_by_intent.get(raw_intent_id, {}) if raw_intent_id else {}
        arbitration_row = arbitration_by_intent.get(raw_intent_id, {}) if raw_intent_id else {}
        selected = bool(raw_intent_id and raw_intent_id == selected_id)
        conversion = _find_status(conversions, raw_intent_id, candidate_id)
        submit = _find_status(submits, raw_intent_id, candidate_id)
        lineage = _find_status(lineages, raw_intent_id, candidate_id)
        conversion_payload = conversion.get("payload") if isinstance(conversion.get("payload"), dict) else {}
        submit_payload = submit.get("payload") if isinstance(submit.get("payload"), dict) else {}
        lineage_payload = lineage.get("payload") if isinstance(lineage.get("payload"), dict) else {}
        conversion_status = str(conversion_payload.get("validation_status") or conversion_payload.get("status") or ("MISSING" if selected else "NOT_APPLICABLE")).upper()
        if selected and conversion_status in {"", "MISSING", "NOT_STARTED"}:
            submit = {}
            lineage = {}
            submit_payload = {}
            lineage_payload = {}
            submit_status = "NOT_STARTED"
            lineage_status = "NOT_STARTED"
        else:
            submit_status = str(submit_payload.get("validation_status") or submit_payload.get("status") or ("MISSING" if selected else "NOT_APPLICABLE")).upper()
            lineage_status = str(lineage_payload.get("lineage_status") or lineage_payload.get("status") or ("MISSING" if selected else "NOT_APPLICABLE")).upper()
        selected_pointer_status = "SELECTED" if selected and pointer.get("day_utc") == day_utc else ("NOT_SELECTED" if raw_intent_id else "NOT_APPLICABLE")
        promotion_status = _status_from_promotion(promotion) if selected else "NOT_APPLICABLE"
        construction_status = "NOT_STARTED" if selected and promotion_status != "PROMOTED_TO_OPERATOR_REVIEW" else ("PENDING" if selected else "NOT_APPLICABLE")
        if not manifest_execution_eligible and selected:
            submit = {}
            lineage = {}
            submit_payload = {}
            lineage_payload = {}
            submit_status = "REJECTED_NON_CERTIFIED_INPUT"
            lineage_status = "REJECTED_NON_CERTIFIED_INPUT"
            construction_status = "READ_ONLY_NON_CERTIFIED"
            conversion_status = "READ_ONLY_NON_CERTIFIED"
        capture_ready = manifest_execution_eligible and lineage_status in {"ACTIVE_CURRENT", "CAPTURE_READY"} and submit_status in {"VALIDATED", "VALID", "PASS"}
        if capture_ready:
            final_status = "CAPTURE_READY"
            blocker = ""
            next_action = "Record capture when fill details are available."
        elif selected and not manifest_execution_eligible:
            final_status = "SELECTED_READ_ONLY_NON_CERTIFIED"
            blocker = "NON_CERTIFIED_CANDIDATE_SNAPSHOT"
            next_action = "Wait for EOD certification promotion before any execution-facing workflow can consume this candidate."
        elif selected:
            final_status = "SELECTED_NOT_CAPTURE_READY"
            blocker = promotion_blocker or str(conversion_payload.get("blocker_code") or submit_payload.get("blocker_code") or lineage_payload.get("blocker_code") or "CONVERSION_MISSING")
            next_action = promotion_action or "Build conversion and submit-boundary evidence before capture."
        elif score:
            final_status = "SCORED_NOT_SELECTED"
            blocker = str(arbitration_row.get("rejection_reason") or score.get("score_unavailable_reason") or "NOT_SELECTED_BY_ARBITRATION")
            next_action = "No operator action required unless research review changes priority."
        else:
            final_status = "GENERATED_NOT_SCORED" if raw_intent_id else "GENERATED_NO_INTENT"
            blocker = str(score.get("score_unavailable_reason") or ("NO_INTENT_DECLARED" if not raw_intent_id else "SCORE_UNAVAILABLE"))
            next_action = "No operator action required." if not raw_intent_id else "Rerun governed scoring if this candidate should be eligible."
        out_rows.append({
            "candidate_id": candidate_id,
            "raw_intent_id": raw_intent_id,
            "symbol": str(row.get("symbol") or row.get("symbol_or_pair") or "").upper(),
            "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or ""),
            "score": score.get("score_total"),
            "rank": score.get("rank"),
            "candidate_status": str(row.get("status") or row.get("lifecycle_decision") or "").upper(),
            "selected_status": "SELECTED" if selected else ("NOT_SELECTED" if raw_intent_id else "NOT_APPLICABLE"),
            "selected_intent_pointer_status": selected_pointer_status,
            "promotion_status": promotion_status,
            "construction_status": construction_status,
            "allocation_status": "PENDING" if selected and promotion_status == "PROMOTED_TO_OPERATOR_REVIEW" else ("NOT_STARTED" if selected else "NOT_APPLICABLE"),
            "risk_status": "PENDING" if selected and promotion_status == "PROMOTED_TO_OPERATOR_REVIEW" else ("NOT_STARTED" if selected else "NOT_APPLICABLE"),
            "identity_status": "PENDING" if selected and promotion_status == "PROMOTED_TO_OPERATOR_REVIEW" else ("NOT_STARTED" if selected else "NOT_APPLICABLE"),
            "economic_state_status": "PENDING" if selected and promotion_status == "PROMOTED_TO_OPERATOR_REVIEW" else ("NOT_STARTED" if selected else "NOT_APPLICABLE"),
            "conversion_status": conversion_status,
            "submit_boundary_status": submit_status,
            "ticket_lineage_status": lineage_status,
            "manual_capture_status": "CAPTURE_READY" if capture_ready else "NOT_CAPTURE_READY",
            "candidate_lane": manifest_lane or str(row.get("candidate_lane") or ""),
            "certification_state": manifest_certification_state or str(row.get("certification_state") or ""),
            "certification_label": "CERTIFIED" if manifest_execution_eligible else "NON_CERTIFIED",
            "execution_eligible": manifest_execution_eligible,
            "read_only": not manifest_execution_eligible,
            "input_market_data_snapshot_ids": list(manifest_input_snapshot_ids),
            "execution_firewall_status": "PASS" if manifest_execution_eligible else "REJECTED_NON_CERTIFIED_INPUT",
            "final_operator_status": final_status,
            "exact_blocker": blocker,
            "next_safe_action": next_action,
            "artifact_paths": {
                "score": str(scoring_path) if score else "",
                "arbitration": str(arbitration_path) if raw_intent_id else "",
                "selected_intent_promotion": str(promotion_path) if selected else "",
                "conversion": str(conversion.get("path") or ""),
                "submit_boundary": str(submit.get("path") or ""),
                "ticket_lineage": str(lineage.get("path") or ""),
            },
            "artifact_hashes": {
                "score": _sha(scoring_path) if score else "",
                "arbitration": _sha(arbitration_path) if raw_intent_id else "",
                "selected_intent_promotion": _sha(promotion_path) if selected else "",
                "conversion": str(conversion.get("hash") or ""),
                "submit_boundary": str(submit.get("hash") or ""),
                "ticket_lineage": str(lineage.get("hash") or ""),
            },
        })
    selected_rows = [row for row in out_rows if row["selected_status"] == "SELECTED"]
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "status": "PASS" if out_rows else "NO_CANDIDATES",
        "candidate_count": len(out_rows),
        "candidate_lane": manifest_lane or "UNKNOWN",
        "certification_state": manifest_certification_state or "UNKNOWN",
        "execution_firewall_status": "PASS" if manifest_execution_eligible else "REJECTED_NON_CERTIFIED_INPUT",
        "selected_candidate_count": len(selected_rows),
        "capture_ready_count": len([row for row in out_rows if row["manual_capture_status"] == "CAPTURE_READY"]),
        "blocked_selected_count": len([row for row in selected_rows if row["manual_capture_status"] != "CAPTURE_READY"]),
        "candidate_rows": out_rows,
        "selected_candidates": selected_rows,
        "input_artifacts": {
            "candidate_generation_manifest": str(manifest_path),
            "portfolio_scoring": str(scoring_path),
            "intent_arbitration": str(arbitration_path),
            "selected_intent_pointer": str(pointer_path),
            "selected_intent_promotion": str(promotion_path),
        },
        "source_hashes": {str(path): _sha(path) for path in (manifest_path, scoring_path, arbitration_path, pointer_path, promotion_path)},
        "safety": {"broker_submit_transmit_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "trade_advice_enabled": False},
    }


def render_candidate_promotion_map_text_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS CANDIDATE PROMOTION MAP v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"candidate_count: {payload.get('candidate_count')}",
        f"selected_candidate_count: {payload.get('selected_candidate_count')}",
        f"capture_ready_count: {payload.get('capture_ready_count')}",
        f"blocked_selected_count: {payload.get('blocked_selected_count')}",
        "",
        "selected_candidates:",
    ]
    selected = payload.get("selected_candidates") if isinstance(payload.get("selected_candidates"), list) else []
    if not selected:
        lines.append("- none")
    for row in selected:
        lines.append(f"- {row.get('symbol')} / {row.get('sleeve_id')}: {row.get('final_operator_status')} blocker={row.get('exact_blocker') or 'none'} next={row.get('next_safe_action')}")
    return "\n".join(lines) + "\n"


def write_candidate_promotion_map_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    json_path = candidate_promotion_map_path_v1(truth_root=truth_root, day_utc=day_utc)
    txt_path = candidate_promotion_map_txt_path_v1(truth_root=truth_root, day_utc=day_utc)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    txt_path.write_text(render_candidate_promotion_map_text_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path)}
