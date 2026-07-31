from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from ops.aegis.candidate_contracts_v1 import build_candidate_contracts_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.paper_trade_golden_path_v1 import latest_paper_trade_golden_path_v1
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1


REPORT_FAMILY = "aegis_real_signal_death_report_v1"
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
RAW_SIGNAL_REQUIRED_FIELDS = ("raw_signal_id", "sleeve_id", "symbol", "evidence_path")
PAPER_CANDIDATE_REQUIRED_FIELDS = (
    "candidate_id",
    "raw_signal_id",
    "intent_id",
    "sleeve_id",
    "symbol",
    "direction",
    "instrument_type",
    "entry_reference_price",
    "risk_per_trade",
    "executable_status",
    "governance_status",
)
RUNTIME_BLOCK_REASONS = {
    "POSITION_STATE_STALE",
    "UNCHANGED_SIGNAL",
    "MARKET_DATA_SHA_MISMATCH",
    "SLEEVE_INPUT_REQUIREMENT_BLOCKED",
    "MISSING_REQUIRED_INPUTS",
    "SHA256_MISMATCH",
    "ALLOWED_SYMBOL_MISMATCH",
    "MARKET_SNAPSHOT_PARTIAL",
}


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _upper(value) in {"1", "TRUE", "YES", "ALLOW", "ALLOWED", "PASS"}


def _is_paper_rehearsal_ref(*values: Any) -> bool:
    return any("paper_rehearsal" in _text(value).lower() for value in values)


def _executed_sleeve(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    sleeve_id = _upper(row.get("sleeve_id") or row.get("engine_id"))
    if sleeve_id == SIMULATOR_ENGINE_ID:
        return False
    status = _upper(row.get("status") or row.get("current_status"))
    output_count = row.get("output_count")
    try:
        output_count_int = int(output_count or 0)
    except (TypeError, ValueError):
        output_count_int = 0
    signal_state = _upper((_safe_dict(row.get("signal_state"))).get("state"))
    return status in {"NO_INTENT", "INTENT_CREATED", "FILTERED_OUT"} or output_count_int > 0 or signal_state == "ACTIVE"


def _iter_sleeve_outcomes(root: Path, day_utc: str, rollup_payload: dict[str, Any]) -> list[dict[str, Any]]:
    outcomes = []
    for row in _safe_list(rollup_payload.get("outcomes")):
        if isinstance(row, dict):
            outcomes.append(row)
    known = {_upper(row.get("sleeve_id") or row.get("engine_id")) for row in outcomes}
    base = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc
    if base.exists():
        for child in sorted(base.iterdir()):
            if not child.is_dir():
                continue
            sleeve_id = child.name.strip().upper()
            if sleeve_id in known or sleeve_id == "":
                continue
            payload_path = child / "sleeve_evaluation.v1.json"
            if not payload_path.exists():
                continue
            try:
                import json
                payload = json.loads(payload_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(payload, dict):
                outcomes.append(payload)
    return outcomes


def _raw_entries_from_eval(row: dict[str, Any]) -> list[dict[str, Any]]:
    batch = _safe_dict(row.get("exposure_intent_batch"))
    candidates = []
    for source in (
        _safe_list(batch.get("output_intents")),
        _safe_list(row.get("output_intents")),
        _safe_list(row.get("intent_signature")),
    ):
        if source:
            candidates = source
            break
    out = []
    sleeve_id = _upper(row.get("sleeve_id") or row.get("engine_id") or "UNKNOWN") or "UNKNOWN"
    source_artifact = _text(row.get("artifact_path"))
    for item in candidates:
        if not isinstance(item, dict):
            continue
        raw_signal_id = _text(item.get("raw_signal_id") or item.get("intent_id") or item.get("intent_hash") or item.get("candidate_id"))
        symbol = _upper(item.get("symbol") or item.get("symbol_or_pair"))
        evidence_path = _text(item.get("intent_path") or item.get("raw_intent_path") or source_artifact)
        if not raw_signal_id or not symbol or _is_paper_rehearsal_ref(raw_signal_id, evidence_path):
            continue
        out.append(
            {
                "raw_signal_id": raw_signal_id,
                "sleeve_id": sleeve_id,
                "symbol": symbol,
                "signal_type": _text(item.get("schema_id") or row.get("expected_intent_type") or "EXPOSURE_INTENT"),
                "source_artifact": source_artifact,
                "evidence_path": evidence_path,
                "graph_linkage": f"sleeve_evaluation_kernel_v1.{sleeve_id}",
                "reason_codes": [str(code) for code in _safe_list(row.get("reason_codes")) if _text(code)],
                "lifecycle_reason_codes": [str(code) for code in _safe_list(row.get("lifecycle_reason_codes")) if _text(code)],
                "sleeve_status": _upper(row.get("status") or row.get("current_status")),
            }
        )
    return out


def _collect_real_raw_signals(root: Path, day_utc: str, sleeve_rollup_payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    signals: list[dict[str, Any]] = []
    sleeves_that_ran: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    for row in _iter_sleeve_outcomes(root, day_utc, sleeve_rollup_payload):
        sleeve_id = _upper(row.get("sleeve_id") or row.get("engine_id"))
        if not sleeve_id or sleeve_id == SIMULATOR_ENGINE_ID:
            continue
        if _executed_sleeve(row) and sleeve_id not in sleeves_that_ran:
            sleeves_that_ran.append(sleeve_id)
        for signal in _raw_entries_from_eval(row):
            key = (signal["sleeve_id"], signal["symbol"], signal["raw_signal_id"])
            if key in seen:
                continue
            seen.add(key)
            signals.append(signal)
    return signals, sleeves_that_ran


def _candidate_contract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in ("candidate_contracts", "rejected_raw_signals"):
        rows.extend(row for row in _safe_list(payload.get(key)) if isinstance(row, dict))
    return [row for row in rows if not _is_paper_rehearsal_ref(row.get("candidate_id"), row.get("raw_signal_id"), row.get("intent_id"), row.get("evidence_path"))]


def _candidate_row_for_signal(signal: dict[str, Any], contract_rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_id = signal["raw_signal_id"]
    sleeve_id = signal["sleeve_id"]
    symbol = signal["symbol"]
    exact = []
    fuzzy = []
    for row in contract_rows:
        row_sleeve = _upper(row.get("sleeve_id") or row.get("engine_id"))
        row_symbol = _upper(row.get("symbol") or row.get("symbol_or_pair"))
        if row_sleeve != sleeve_id or row_symbol != symbol:
            continue
        row_raw_id = _text(row.get("raw_signal_id") or row.get("intent_id") or row.get("intent_hash"))
        row_raw_hash = _text(row.get("intent_hash"))
        if row_raw_id == raw_id or row_raw_hash == raw_id:
            exact.append(row)
        else:
            fuzzy.append(row)
    return exact[0] if exact else (fuzzy[0] if fuzzy else {})


def _arbitration_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in ("candidate_intents", "raw_candidate_intents", "rejected_or_filtered_intents"):
        rows.extend(row for row in _safe_list(payload.get(key)) if isinstance(row, dict))
    selected = _safe_dict(payload.get("selected_intent"))
    if selected:
        rows.append(selected)
    return [row for row in rows if not _is_paper_rehearsal_ref(row.get("intent_id"), row.get("intent_path"))]


def _arbitration_row_for_signal(signal: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_id = signal["raw_signal_id"]
    sleeve_id = signal["sleeve_id"]
    symbol = signal["symbol"]
    exact = []
    fuzzy = []
    for row in rows:
        row_id = _text(row.get("intent_id") or row.get("raw_signal_id") or row.get("intent_hash"))
        row_sleeve = _upper(row.get("sleeve_id") or row.get("engine_id"))
        row_symbol = _upper(row.get("symbol") or row.get("symbol_or_pair"))
        if row_id == raw_id:
            exact.append(row)
        elif row_sleeve == sleeve_id and row_symbol == symbol:
            fuzzy.append(row)
    return exact[0] if exact else (fuzzy[0] if fuzzy else {})


def _passed_raw_signal_contract(signal: dict[str, Any]) -> bool:
    return all(_text(signal.get(field)) for field in RAW_SIGNAL_REQUIRED_FIELDS)


def _failed_candidate_contract_fields(signal: dict[str, Any], candidate_row: dict[str, Any]) -> list[str]:
    if not candidate_row:
        return [f"candidate.{field}" for field in PAPER_CANDIDATE_REQUIRED_FIELDS]
    missing = [str(item) for item in _safe_list(candidate_row.get("missing_contract_fields")) if _text(item)]
    if missing:
        return sorted(dict.fromkeys(missing))
    mapping = {
        "candidate_id": candidate_row.get("candidate_id"),
        "raw_signal_id": candidate_row.get("raw_signal_id") or candidate_row.get("intent_id") or candidate_row.get("intent_hash"),
        "intent_id": candidate_row.get("intent_id"),
        "sleeve_id": candidate_row.get("sleeve_id") or candidate_row.get("engine_id"),
        "symbol": candidate_row.get("symbol"),
        "direction": candidate_row.get("direction"),
        "instrument_type": candidate_row.get("instrument_type"),
        "entry_reference_price": candidate_row.get("entry_reference_price"),
        "risk_per_trade": candidate_row.get("risk_per_trade"),
        "executable_status": candidate_row.get("executable_status"),
        "governance_status": candidate_row.get("governance_status"),
    }
    failed = []
    for field in PAPER_CANDIDATE_REQUIRED_FIELDS:
        value = mapping.get(field)
        if value is None or _text(value) == "":
            failed.append(f"candidate.{field}")
    if _upper(mapping.get("symbol")) != signal["symbol"]:
        failed.append("candidate.symbol")
    if _upper(mapping.get("sleeve_id")) != signal["sleeve_id"]:
        failed.append("candidate.sleeve_id")
    return sorted(dict.fromkeys(failed))


def _failed_arbitration_fields(signal: dict[str, Any], arbitration_row: dict[str, Any], arbitration_payload: dict[str, Any]) -> list[str]:
    if arbitration_row:
        return []
    fields = ["intent_arbitration.intent_row"]
    if _upper(arbitration_payload.get("status")) != "SELECTED":
        fields.append("intent_arbitration.status")
    fields.append("intent_arbitration.portfolio_scoring_path")
    return fields


def _failed_promotion_fields(signal: dict[str, Any], arbitration_row: dict[str, Any], promotion_payload: dict[str, Any]) -> list[str]:
    promotion_status = _upper(promotion_payload.get("status") or promotion_payload.get("promotion_status"))
    if promotion_status in {"", "PROMOTED", "PROMOTED_TO_OPERATOR_REVIEW", "OK", "PASS"}:
        return []
    selected_id = _text((_safe_dict(promotion_payload.get("candidate"))).get("intent_id") or promotion_payload.get("selected_intent_id"))
    if selected_id and selected_id != signal["raw_signal_id"]:
        return []
    missing = [str(item) for item in _safe_list(promotion_payload.get("missing_contract_fields")) if _text(item)]
    return missing or ["selected_intent_promotion.status"]


def _failed_evidence_fields(signal: dict[str, Any], candidate_row: dict[str, Any], arbitration_row: dict[str, Any]) -> list[str]:
    failed = []
    if not _text(signal.get("evidence_path")):
        failed.append("raw_signal.evidence_path")
    if candidate_row:
        if not _safe_list(candidate_row.get("evidence_paths")) and not _text(candidate_row.get("evidence_path")):
            failed.append("candidate.evidence_paths")
        if not _text(candidate_row.get("raw_signal_id") or candidate_row.get("intent_id")):
            failed.append("candidate.raw_signal_id")
    else:
        failed.append("candidate.row")
    if arbitration_row:
        if not _text(arbitration_row.get("intent_path")):
            failed.append("intent_arbitration.intent_path")
    else:
        failed.append("intent_arbitration.row")
    return sorted(dict.fromkeys(failed))


def _signal_rejection_reason(signal: dict[str, Any], candidate_row: dict[str, Any], arbitration_row: dict[str, Any], promotion_payload: dict[str, Any]) -> str:
    for value in (
        arbitration_row.get("rejection_reason") if arbitration_row else "",
        candidate_row.get("rejection_reason") if candidate_row else "",
        candidate_row.get("score_unavailable_reason") if candidate_row else "",
        promotion_payload.get("rejection_reason"),
        promotion_payload.get("status"),
    ):
        text = _upper(value)
        if text and text not in {"BLOCKED", "CONTRACT_FAILED"}:
            return text
    reason_codes = [
        *[str(code) for code in _safe_list(signal.get("reason_codes")) if _text(code)],
        *[str(code) for code in _safe_list(signal.get("lifecycle_reason_codes")) if _text(code)],
        *[str(code) for code in _safe_list(candidate_row.get("reason_codes")) if _text(code)],
        *[str(code) for code in _safe_list(candidate_row.get("lifecycle_reason_codes")) if _text(code)],
    ]
    for code in reason_codes:
        if _upper(code):
            return _upper(code)
    if _upper(promotion_payload.get("status")) == "CONTRACT_FAILED":
        return "SELECTED_INTENT_PROMOTION_CONTRACT_FAILED"
    return "UNKNOWN"


def _rejection_stage(signal: dict[str, Any], candidate_row: dict[str, Any], arbitration_row: dict[str, Any], promotion_payload: dict[str, Any], failed_candidate_fields: list[str], failed_arbitration_fields: list[str], failed_promotion_fields: list[str]) -> str:
    if not _passed_raw_signal_contract(signal):
        return "SIGNAL_CONTRACT"
    if failed_candidate_fields:
        return "CANDIDATE_CONVERSION"
    if failed_arbitration_fields:
        return "INTENT_ARBITRATION"
    if any("portfolio_scoring" in field for field in failed_promotion_fields):
        return "PORTFOLIO_SCORING"
    if failed_promotion_fields:
        return "SELECTED_INTENT_PROMOTION"
    reason_pool = " ".join(
        [
            _upper(_signal_rejection_reason(signal, candidate_row, arbitration_row, promotion_payload)),
            " ".join(_upper(item) for item in _safe_list(signal.get("reason_codes"))),
            " ".join(_upper(item) for item in _safe_list(signal.get("lifecycle_reason_codes"))),
            " ".join(_upper(item) for item in _safe_list(candidate_row.get("reason_codes"))),
            " ".join(_upper(item) for item in _safe_list(candidate_row.get("lifecycle_reason_codes"))),
        ]
    )
    if any(token in reason_pool for token in RUNTIME_BLOCK_REASONS) or "VIX" in reason_pool:
        return "RUNTIME_TRUTH_GATE"
    if any("research" in field for field in failed_candidate_fields + failed_arbitration_fields):
        return "RESEARCH_GATE"
    return "UNKNOWN"


def _next_repair_action(stage: str, reason: str, failed_candidate_fields: list[str], failed_arbitration_fields: list[str], failed_promotion_fields: list[str]) -> str:
    if stage == "RUNTIME_TRUTH_GATE":
        if "VIX" in reason:
            return "Restore current canonical VIX evidence, then rerun candidate diagnostics and the signal death report."
        return "Resolve the runtime blocker in sleeve evaluation evidence, then rerun candidate diagnostics and the signal death report."
    if stage == "CANDIDATE_CONVERSION":
        if reason == "ENTRY_REFERENCE_PRICE_MISSING":
            return "Publish a certified current-session market.price registry row for the symbol, then rerun candidate contracts, candidate diagnostics, and the signal death report."
        if reason == "ENTRY_REFERENCE_PRICE_STALE":
            return "Refresh certified current-session market data for the symbol, then rerun candidate contracts, candidate diagnostics, and the signal death report."
        if reason == "ENTRY_REFERENCE_PRICE_UNCERTIFIED":
            return "Repair the data-registry certification chain for the symbol so entry price evidence is VALID and hashed, then rerun candidate contracts, candidate diagnostics, and the signal death report."
        if reason == "ENTRY_REFERENCE_PRICE_SYMBOL_MISMATCH":
            return "Repair the market-price symbol mapping so the certified source matches the raw-signal symbol, then rerun candidate contracts, candidate diagnostics, and the signal death report."
        if any(field.endswith("raw_signal_id") or field.endswith("intent_id") for field in failed_candidate_fields):
            return "Repair candidate-contract linkage so every real raw signal retains raw_signal_id, intent_id, and evidence lineage through conversion."
        return "Repair candidate conversion so real raw signals produce complete candidate contract rows."
    if stage == "INTENT_ARBITRATION":
        return "Repair intent arbitration ingestion so converted raw signals appear in arbitration evidence."
    if stage == "PORTFOLIO_SCORING":
        return "Repair portfolio scoring evidence for converted intents; do not bypass the gate."
    if stage == "SELECTED_INTENT_PROMOTION":
        return "Repair selected intent promotion contract inputs; do not fabricate a promoted candidate."
    if stage == "SIGNAL_CONTRACT":
        return "Repair the sleeve raw-signal contract so raw_signal_id, sleeve_id, symbol, and evidence_path are always emitted."
    return "Inspect sleeve evaluation, candidate manifest, intent arbitration, and selected intent promotion artifacts together."


def _paper_golden_summary(paper_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_rehearsal_lifecycle_proven": bool(paper_payload.get("paper_rehearsal_lifecycle_proven")) if isinstance(paper_payload, dict) else False,
        "paper_raw_signal_count": int((paper_payload or {}).get("raw_signal_count") or 0),
        "paper_candidate_count": int((paper_payload or {}).get("candidate_count") or 0),
        "paper_mode": _text((paper_payload or {}).get("mode")),
        "paper_receipt_type": _text((paper_payload or {}).get("receipt_type")),
        "paper_candidate_required_fields": list(PAPER_CANDIDATE_REQUIRED_FIELDS),
        "paper_raw_signal_required_fields": list(RAW_SIGNAL_REQUIRED_FIELDS),
    }


def _real_candidate_count(candidate_contracts_payload: dict[str, Any]) -> int:
    return int(_safe_dict(candidate_contracts_payload).get("candidates_created") or 0)


def _signal_graph_row(signal: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    raw_id = _text(signal.get("raw_signal_id"))
    sleeve_id = _upper(signal.get("sleeve_id"))
    symbol = _upper(signal.get("symbol"))
    for row in _safe_list(payload.get("signals")):
        if not isinstance(row, dict):
            continue
        if _text(row.get("raw_signal_id")) == raw_id:
            return row
        if _upper(row.get("sleeve_id")) == sleeve_id and _upper(row.get("symbol")) == symbol:
            return row
    return {}


def build_real_signal_death_report_v1(*, truth_root: Path | str, day_utc: str, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    sleeve_rollup_path, sleeve_rollup_payload = latest_json_v1(root, "sleeve_evaluation_kernel_v1", day_utc, "sleeve_evaluation_rollup.v1.json")
    manifest_path, manifest_payload = latest_json_v1(root, "candidate_generation_manifest_v1", day_utc, "candidate_generation_manifest.v1.json")
    lifecycle_path, lifecycle_payload = latest_json_v1(root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json")
    signal_graph_path, signal_graph_payload = latest_json_v1(root, "aegis_signal_evidence_graph_v1", day_utc, "signal_evidence_graph.v1.json")
    candidate_contracts_path, candidate_contracts_payload = latest_json_v1(root, "aegis_candidate_contracts_v1", day_utc, "candidate_contracts.v1.json")
    arbitration_path, arbitration_payload = latest_json_v1(root, "intent_arbitration_v1", day_utc, "intent_arbitration.v1.json")
    promotion_path, promotion_payload = latest_json_v1(root, "aegis_selected_intent_promotion_v1", day_utc, "selected_intent_promotion.v1.json")
    runtime_path, runtime_payload = latest_json_v1(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    paper_path, paper_payload = latest_paper_trade_golden_path_v1(truth_root=root, day_utc=day_utc)
    if not signal_graph_payload:
        signal_graph_payload = build_signal_evidence_graph_v1(truth_root=root, day_utc=day_utc, repo_root=repo_root)
        signal_graph_path = root / "reports" / "aegis_signal_evidence_graph_v1" / day_utc / "signal_evidence_graph.v1.json"
    if not candidate_contracts_payload:
        candidate_contracts_payload = build_candidate_contracts_v1(truth_root=root, day_utc=day_utc, repo_root=repo_root)

    signals, sleeves_that_ran = _collect_real_raw_signals(root, day_utc, _safe_dict(sleeve_rollup_payload))
    contract_rows = _candidate_contract_rows(_safe_dict(candidate_contracts_payload))
    arbitration_rows = _arbitration_rows(_safe_dict(arbitration_payload))
    real_candidate_count = _real_candidate_count(_safe_dict(candidate_contracts_payload))
    signal_rows: list[dict[str, Any]] = []
    stage_counts: Counter[str] = Counter()
    missing_field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    next_actions: list[str] = []

    for signal in signals:
        graph_row = _signal_graph_row(signal, _safe_dict(signal_graph_payload))
        candidate_row = _candidate_row_for_signal(signal, contract_rows)
        arbitration_row = _arbitration_row_for_signal(signal, arbitration_rows)
        failed_candidate_fields = _failed_candidate_contract_fields(signal, candidate_row)
        failed_arbitration_fields = _failed_arbitration_fields(signal, arbitration_row, _safe_dict(arbitration_payload))
        failed_promotion_fields = _failed_promotion_fields(signal, arbitration_row, _safe_dict(promotion_payload))
        failed_evidence_fields = _failed_evidence_fields(signal, candidate_row, arbitration_row)
        reason = _signal_rejection_reason(signal, candidate_row, arbitration_row, _safe_dict(promotion_payload))
        stage = _rejection_stage(signal, candidate_row, arbitration_row, _safe_dict(promotion_payload), failed_candidate_fields, failed_arbitration_fields, failed_promotion_fields)
        action = _next_repair_action(stage, reason, failed_candidate_fields, failed_arbitration_fields, failed_promotion_fields)
        price_evidence = next((item for item in _safe_list(graph_row.get("required_evidence")) if _text(item.get("purpose")) == "ENTRY_REFERENCE_PRICE"), {})
        row = {
            "raw_signal_id": signal["raw_signal_id"],
            "sleeve_id": signal["sleeve_id"],
            "symbol": signal["symbol"],
            "signal_type": signal["signal_type"],
            "source_artifact": signal["source_artifact"],
            "evidence_path": signal["evidence_path"],
            "graph_linkage": signal["graph_linkage"],
            "passed_raw_signal_contract": _passed_raw_signal_contract(signal),
            "failed_candidate_contract_fields": failed_candidate_fields,
            "failed_arbitration_fields": failed_arbitration_fields,
            "failed_promotion_fields": failed_promotion_fields,
            "failed_evidence_fields": failed_evidence_fields,
            "rejection_stage": _text(graph_row.get("rejection_stage")) or stage,
            "rejection_reason": _text(candidate_row.get("rejection_reason")) if (_text(graph_row.get("rejection_stage")) or stage) == "CANDIDATE_CONVERSION" and _text(candidate_row.get("rejection_reason")) else (_text(graph_row.get("rejection_reason")) or reason),
            "next_repair_action": _text(graph_row.get("next_repair_action")) or action,
            "candidate_contract_status": _upper(candidate_row.get("contract_validation_status") or candidate_row.get("status") or graph_row.get("candidate_contract_status")) if candidate_row or graph_row else "",
            "candidate_contract_path": _text(candidate_contracts_path or ""),
            "signal_evidence_graph_path": _text(signal_graph_path or ""),
            "required_evidence": _safe_list(graph_row.get("required_evidence")),
            "fulfillment_status": _text(graph_row.get("fulfillment_status")),
            "certification_status": _text(graph_row.get("certification_status")),
            "entry_reference_price": _text(candidate_row.get("entry_reference_price") or next((item.get("value") for item in _safe_list(graph_row.get("required_evidence")) if _text(item.get("purpose")) == "ENTRY_REFERENCE_PRICE"), "")) if candidate_row or graph_row else "",
            "entry_reference_price_status": _text(candidate_row.get("entry_reference_price_status")) if candidate_row else "",
            "entry_reference_price_missing_symbol": _text(candidate_row.get("entry_reference_price_missing_symbol") or graph_row.get("symbol")) if candidate_row or graph_row else "",
            "entry_reference_price_source_path": _text(candidate_row.get("entry_reference_price_source_path")) if candidate_row else "",
            "entry_reference_price_source_hash": _text(candidate_row.get("entry_reference_price_source_hash")) if candidate_row else "",
            "entry_reference_price_provider": _text(candidate_row.get("entry_reference_price_provider")) if candidate_row else "",
            "entry_reference_price_timestamp_utc": _text(candidate_row.get("entry_reference_price_timestamp_utc")) if candidate_row else "",
            "entry_reference_price_session_date": _text(candidate_row.get("entry_reference_price_session_date")) if candidate_row else "",
            "entry_reference_price_checked_evidence_paths": _safe_list(candidate_row.get("entry_reference_price_checked_evidence_paths")) if candidate_row else [],
            "entry_reference_price_certification_status": _text(candidate_row.get("entry_reference_price_certification_status") or price_evidence.get("entry_reference_price_certification_status")) if candidate_row or price_evidence else "",
            "entry_reference_price_certification_reason_codes": _safe_list(candidate_row.get("entry_reference_price_certification_reason_codes")) or _safe_list(price_evidence.get("entry_reference_price_certification_reason_codes")),
            "entry_reference_price_certification_id": _text(candidate_row.get("entry_reference_price_certification_id") or price_evidence.get("entry_reference_price_certification_id")) if candidate_row or price_evidence else "",
            "entry_reference_price_certification_detail_status": _text(candidate_row.get("entry_reference_price_certification_detail_status") or price_evidence.get("entry_reference_price_certification_status")) if candidate_row or price_evidence else "",
            "entry_reference_price_safe_repair_available": bool(candidate_row.get("entry_reference_price_safe_repair_available") or price_evidence.get("safe_repair_available")) if candidate_row or price_evidence else False,
            "detail_reason_codes": _safe_list(candidate_row.get("detail_reason_codes")) or _safe_list(candidate_row.get("entry_reference_price_certification_reason_codes")) or _safe_list(price_evidence.get("entry_reference_price_certification_reason_codes")),
            "intent_arbitration_status": _upper(_safe_dict(arbitration_payload).get("status")),
            "selected_intent_promotion_status": _upper(_safe_dict(promotion_payload).get("status") or _safe_dict(promotion_payload).get("promotion_status")),
        }
        signal_rows.append(row)
        stage_counts[stage] += 1
        reason_counts[reason] += 1
        for field in failed_candidate_fields + failed_arbitration_fields + failed_promotion_fields + failed_evidence_fields:
            missing_field_counts[field] += 1
        if action and action not in next_actions:
            next_actions.append(action)

    comparison = {
        **_paper_golden_summary(_safe_dict(paper_payload)),
        "real_raw_signal_count": len(signal_rows),
        "real_candidate_count": real_candidate_count,
        "paper_rehearsal_excluded_from_real_totals": True,
        "real_missing_stages_vs_paper": [
            stage
            for stage in ["candidate", "operator_execution_queue", "manual_trade_packet", "manual_execution_receipt", "outcome"]
            if real_candidate_count == 0
        ],
        "summary": (
            f"Paper rehearsal proves raw signal to promoted candidate with {int((_safe_dict(paper_payload)).get('candidate_count') or 0)} candidate; "
            f"real path has {len(signal_rows)} raw signals and {real_candidate_count} real candidate contracts."
        ),
    }

    report = {
        "schema_id": "aegis_real_signal_death_report",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": now_utc_v1(),
        "total_raw_signals": len(signal_rows),
        "total_candidates_created": real_candidate_count,
        "sleeves_that_ran": sleeves_that_ran,
        "signals_by_sleeve": dict(sorted(Counter(row["sleeve_id"] for row in signal_rows).items())),
        "signals_by_symbol": dict(sorted(Counter(row["symbol"] for row in signal_rows).items())),
        "rejection_stage_counts": dict(sorted(stage_counts.items())),
        "top_missing_contract_fields": [
            {"field": field, "count": count}
            for field, count in missing_field_counts.most_common(10)
        ],
        "top_rejection_reasons": [
            {"reason": reason, "count": count}
            for reason, count in reason_counts.most_common(10)
        ],
        "comparison_to_paper_golden_path": comparison,
        "exact_next_repair_actions": next_actions,
        "signals": signal_rows,
        "input_artifacts": {
            "sleeve_evaluation_rollup": str(sleeve_rollup_path or ""),
            "candidate_generation_manifest": str(manifest_path or ""),
            "signal_evidence_graph": str(signal_graph_path or ""),
            "candidate_contracts": str(candidate_contracts_path or ""),
            "candidate_lifecycle": str(lifecycle_path or ""),
            "intent_arbitration": str(arbitration_path or ""),
            "selected_intent_promotion": str(promotion_path or ""),
            "runtime_truth": str(runtime_path or ""),
            "paper_trade_golden_path": str(paper_path or ""),
        },
        "runtime_truth": {
            "classification": _upper(_safe_dict(runtime_payload).get("runtime_truth_classification")),
            "trade_advice_allowed": _bool(_safe_dict(runtime_payload).get("trade_advice_allowed")),
            "autonomous_execution_allowed": _bool(_safe_dict(runtime_payload).get("autonomous_execution_allowed")),
            "manual_trade_capture_allowed": _bool(_safe_dict(runtime_payload).get("manual_trade_capture_allowed")),
        },
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "manual_trade_capture_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "paper_rehearsal_excluded_from_real_totals": True,
            "simulated_paper_counts_as_real_readiness": False,
        },
    }
    return report


def write_real_signal_death_report_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "real_signal_death_report.v1.json", payload)
    return {"json": str(json_path)}
