from __future__ import annotations

from typing import Any


EXECUTION_STATUSES = {
    "NOT_RUN",
    "BLOCKED",
    "EXECUTED_NO_SIGNALS",
    "EXECUTED_REJECTED",
    "EXECUTED_WITH_CANDIDATES",
}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _evidence_path_for_sleeve(row: dict[str, Any], diagnostics: dict[str, Any]) -> str:
    return _first_text(
        row.get("evaluation_artifact_path"),
        row.get("source_artifact_path"),
        _safe_dict(diagnostics.get("input_artifacts")).get("sleeve_evaluation_rollup"),
        _safe_dict(diagnostics.get("input_artifacts")).get("candidate_generation_manifest"),
        _safe_dict(diagnostics.get("input_artifacts")).get("runtime_truth"),
    )


def _graph_linkage_for_sleeve(row: dict[str, Any]) -> str:
    sleeve_id = _first_text(row.get("sleeve_id"), "UNKNOWN")
    return f"aegis_candidate_generation_diagnostics_v1.sleeves[{sleeve_id}]"


def _symbols_evaluated_count(row: dict[str, Any], *, executed: bool) -> int:
    if not executed:
        return 0
    for key in ("symbols_evaluated_count", "evaluated_symbol_count", "symbol_count"):
        if row.get(key) is not None:
            return _safe_int(row.get(key))
    for key in ("symbols_evaluated", "evaluated_symbols", "scanned_symbols", "allowed_symbols"):
        values = _safe_list(row.get(key))
        if values:
            return len({str(item).strip().upper() for item in values if str(item).strip()})
    symbol = _first_text(row.get("symbol"), row.get("symbol_or_pair"))
    return 1 if symbol else 0


def _execution_status(row: dict[str, Any]) -> str:
    run_status = _upper(row.get("run_status"))
    evaluation_status = _upper(row.get("evaluation_status"))
    raw_signals = _safe_int(row.get("raw_signal_count"))
    candidates = _safe_int(row.get("candidate_count"))
    rejected = _safe_int(row.get("rejected_count") or row.get("rejected_candidate_count"))
    blocker = _first_text(row.get("canonical_blocker"), row.get("reason_no_candidate"))
    if run_status in {"", "NOT_RUN", "SKIPPED"} and evaluation_status not in {"BLOCKED", "NO_INTENT"}:
        return "NOT_RUN"
    if run_status in {"BLOCKED", "FAILED"} or evaluation_status == "BLOCKED" or blocker in {"MISSING_REQUIRED_INPUTS", "SLEEVE_INPUT_REQUIREMENT_BLOCKED"}:
        return "BLOCKED"
    if candidates > 0:
        return "EXECUTED_WITH_CANDIDATES"
    if rejected > 0:
        return "EXECUTED_REJECTED"
    if run_status == "RAN" or evaluation_status in {"NO_INTENT", "RAN", "COMPLETE"}:
        return "EXECUTED_NO_SIGNALS"
    if raw_signals > 0:
        return "EXECUTED_REJECTED"
    return "NOT_RUN"


def _primary_blocker(row: dict[str, Any]) -> str:
    reasons = _safe_list(row.get("reason_codes")) or _safe_list(row.get("rejection_reasons"))
    return _first_text(
        row.get("canonical_blocker"),
        ", ".join(str(item) for item in reasons if str(item).strip()),
        row.get("reason_no_candidate"),
        ", ".join(str(item) for item in _safe_list(row.get("blocking_inputs")) if str(item).strip()),
        ", ".join(str(item) for item in _safe_list(row.get("missing_inputs")) if str(item).strip()),
        "none",
    )


def build_sleeve_execution_summary_v1(diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics = _safe_dict(diagnostics)
    rows: list[dict[str, Any]] = []
    for row in _safe_list(diagnostics.get("sleeves")):
        if not isinstance(row, dict):
            continue
        execution_status = _execution_status(row)
        executed = execution_status.startswith("EXECUTED")
        raw_signals = _safe_int(row.get("raw_signal_count"))
        candidates = _safe_int(row.get("candidate_count"))
        rejected = _safe_int(row.get("rejected_count") or row.get("rejected_candidate_count"))
        rows.append(
            {
                "sleeve_id": _first_text(row.get("sleeve_id"), "UNKNOWN"),
                "execution_status": execution_status,
                "symbols_evaluated_count": _symbols_evaluated_count(row, executed=executed),
                "raw_signals_count": raw_signals,
                "candidates_generated_count": candidates,
                "candidates_rejected_count": rejected,
                "primary_blocker_reason": _primary_blocker(row) if execution_status == "BLOCKED" or rejected else "none",
                "evidence_path": _evidence_path_for_sleeve(row, diagnostics),
                "graph_linkage": _graph_linkage_for_sleeve(row),
                "run_status": row.get("run_status") or "",
                "readiness": row.get("readiness") or "",
                "reason_codes": _safe_list(row.get("reason_codes")),
                "entry_reference_price_detail_reason_codes": _safe_list(row.get("entry_reference_price_detail_reason_codes")),
                "entry_reference_price_certification_statuses": _safe_list(row.get("entry_reference_price_certification_statuses")),
                "entry_reference_price_safe_repair_available": bool(row.get("entry_reference_price_safe_repair_available")),
                "allowed_symbols": _safe_list(row.get("allowed_symbols")),
            }
        )
    return rows


def build_execution_coverage_v1(diagnostics: dict[str, Any], sleeve_rows: list[dict[str, Any]]) -> dict[str, Any]:
    diagnostics = _safe_dict(diagnostics)
    expected = _safe_int(diagnostics.get("total_sleeves_expected") or diagnostics.get("total_sleeves_expected_today") or len(sleeve_rows))
    blocked = sum(1 for row in sleeve_rows if row.get("execution_status") == "BLOCKED")
    successfully_executed = sum(1 for row in sleeve_rows if str(row.get("execution_status") or "").startswith("EXECUTED"))
    attempted = sum(1 for row in sleeve_rows if row.get("execution_status") != "NOT_RUN")
    producing_signals = sum(1 for row in sleeve_rows if _safe_int(row.get("raw_signals_count")) > 0)
    return {
        "expected_sleeves": expected,
        "sleeves_attempted": attempted,
        "sleeves_successfully_executed": successfully_executed,
        "sleeves_blocked": blocked,
        "sleeves_producing_signals": producing_signals,
    }


def _symbol_rows_for_sleeve(row: dict[str, Any]) -> list[str]:
    symbols = _safe_list(row.get("symbols_evaluated")) or _safe_list(row.get("evaluated_symbols")) or _safe_list(row.get("scanned_symbols")) or _safe_list(row.get("allowed_symbols"))
    return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})


def build_rejected_candidate_visibility_v1(diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics = _safe_dict(diagnostics)
    rows: list[dict[str, Any]] = []
    for row in _safe_list(diagnostics.get("raw_signal_rejections")) + _safe_list(diagnostics.get("rejected_raw_signal_details")):
        if not isinstance(row, dict):
            continue
        evidence_paths = _safe_list(row.get("source_artifact_paths"))
        evidence_path = _first_text(
            row.get("evidence_path"),
            evidence_paths[0] if evidence_paths else "",
            row.get("portfolio_scoring_path"),
            _safe_dict(diagnostics.get("input_artifacts")).get("intent_arbitration"),
            _safe_dict(diagnostics.get("input_artifacts")).get("candidate_generation_manifest"),
        )
        unique_key = (
            _first_text(row.get("sleeve_id"), "UNKNOWN"),
            _first_text(row.get("symbol"), "UNKNOWN"),
            _first_text(row.get("raw_signal_id"), row.get("candidate_id"), row.get("rejection_reason")),
        )
        projected = {
            "symbol": unique_key[1],
            "rejection_reason": _first_text(row.get("rejection_reason"), row.get("reason"), "UNKNOWN"),
            "rejection_stage": _first_text(row.get("rejection_stage"), "UNKNOWN"),
            "sleeve_id": unique_key[0],
            "evidence_path": evidence_path,
            "graph_linkage": f"aegis_candidate_generation_diagnostics_v1.raw_signal_rejections[{unique_key[0]}:{unique_key[1]}]",
            "candidate_id": _first_text(row.get("candidate_id"), row.get("raw_signal_id")),
            "human_readable_explanation": _first_text(row.get("human_readable_explanation")),
            "required_next_action": _first_text(row.get("required_next_action")),
            "rejection_classification": _first_text(row.get("rejection_classification"), "UNKNOWN"),
            "detail_reason_codes": _safe_list(row.get("detail_reason_codes")) or _safe_list(row.get("entry_reference_price_certification_reason_codes")),
            "entry_reference_price_certification_status": _first_text(row.get("entry_reference_price_certification_status")),
            "entry_reference_price_certification_reason_codes": _safe_list(row.get("entry_reference_price_certification_reason_codes")),
            "entry_reference_price_timestamp_utc": _first_text(row.get("entry_reference_price_timestamp_utc")),
            "entry_reference_price_source_path": _first_text(row.get("entry_reference_price_source_path")),
            "entry_reference_price_safe_repair_available": bool(row.get("entry_reference_price_safe_repair_available")),
            "candidate_gate_failed": bool(row.get("candidate_gate_failed")),
            "promotion_gate_failed": bool(row.get("promotion_gate_failed")),
            "selected_intent_promotion_status": _first_text(row.get("selected_intent_promotion_status")),
            "selected_intent_promotion_missing_contract_fields": _safe_list(row.get("selected_intent_promotion_missing_contract_fields")),
            "raw_signal_id": _first_text(row.get("raw_signal_id")),
            "rejected_count": 1,
        }
        if projected not in rows:
            rows.append(projected)
    if rows:
        return rows
    for row in _safe_list(diagnostics.get("sleeves")):
        if not isinstance(row, dict):
            continue
        rejected = _safe_int(row.get("rejected_count") or row.get("rejected_candidate_count"))
        if rejected <= 0:
            continue
        reason = _primary_blocker(row)
        stage = "CONTRACT_OR_EVIDENCE" if _execution_status(row) == "BLOCKED" else "SLEEVE_FILTER"
        symbols = _symbol_rows_for_sleeve(row) or ["UNKNOWN"]
        evidence_path = _evidence_path_for_sleeve(row, diagnostics)
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol,
                    "rejection_reason": reason,
                    "rejection_stage": stage,
                    "sleeve_id": _first_text(row.get("sleeve_id"), "UNKNOWN"),
                    "evidence_path": evidence_path,
                    "graph_linkage": _graph_linkage_for_sleeve(row),
                    "candidate_id": "",
                    "human_readable_explanation": _first_text(row.get("reason_no_candidate"), reason),
                }
            )
    return rows


def build_no_candidate_explanations_v1(diagnostics: dict[str, Any], sleeve_rows: list[dict[str, Any]], rejected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    diagnostics = _safe_dict(diagnostics)
    input_artifacts = _safe_dict(diagnostics.get("input_artifacts"))
    rows: list[dict[str, Any]] = []

    def add(code: str, explanation: str, evidence_path: str = "", graph_linkage: str = "") -> None:
        if not any(row["code"] == code for row in rows):
            rows.append(
                {
                    "code": code,
                    "explanation": explanation,
                    "evidence_path": evidence_path or input_artifacts.get("candidate_generation_manifest") or input_artifacts.get("runtime_truth") or "",
                    "graph_linkage": graph_linkage or f"aegis_candidate_generation_diagnostics_v1.no_candidate_explanations[{code}]",
                }
            )

    total_candidates = _safe_int(diagnostics.get("total_candidates_generated"))
    if total_candidates > 0:
        return []
    scanned_symbols = sum(_safe_int(row.get("symbols_evaluated_count")) for row in sleeve_rows if str(row.get("execution_status") or "").startswith("EXECUTED"))
    if scanned_symbols == 0:
        add("NO_SYMBOLS_SCANNED", "No executed sleeve reported evaluated symbols.")
    if any(row.get("execution_status") == "EXECUTED_NO_SIGNALS" and _safe_int(row.get("symbols_evaluated_count")) > 0 for row in sleeve_rows):
        add("THRESHOLDS_NOT_MET", "At least one sleeve scanned symbols but found no qualifying setup.")
    if any(row.get("execution_status") == "BLOCKED" for row in sleeve_rows):
        add("CONTRACTS_FAILED", "One or more sleeves were blocked by contract, hash, allowed-symbol, or runtime precondition checks.")
    missing_artifacts = _safe_list(diagnostics.get("missing_input_artifacts")) + _safe_list(diagnostics.get("stale_input_artifacts"))
    missing_text = " ".join(str(item) for item in missing_artifacts)
    if missing_artifacts or "MISSING" in missing_text or "STALE" in missing_text:
        add("EVIDENCE_MISSING", "Required evidence or current market/runtime inputs are missing or stale.")
    status_text = " ".join(
        [
            _upper(diagnostics.get("candidate_generation_status")),
            _upper(diagnostics.get("operator_interpretation")),
            _upper(diagnostics.get("exact_blocker")),
            missing_text.upper(),
        ]
    )
    if "RUNTIME" in status_text or "BLOCKED" in status_text:
        add("RUNTIME_BLOCKED", "Runtime or upstream readiness prevented full candidate-generation coverage.")
    all_reason_text = " ".join(
        [
            " ".join(str(item) for item in row.get("reason_codes", []))
            + " "
            + str(row.get("primary_blocker_reason") or "")
            for row in sleeve_rows
        ]
        + [str(row.get("rejection_reason") or "") for row in rejected_rows]
    ).upper()
    if "VIX" in all_reason_text or "VOLATILITY" in all_reason_text:
        add("VOLATILITY_FILTER_FAILED", "Volatility context failed or was unavailable, including VIX-dependent sleeve filters.")
    promotion = _safe_dict(diagnostics.get("selected_intent_promotion"))
    promotion_status = _upper(promotion.get("status") or promotion.get("promotion_status"))
    if promotion and promotion_status not in {"", "PROMOTED", "PROMOTED_TO_OPERATOR_REVIEW", "OK", "PASS"}:
        add("PROMOTION_BLOCKED", "Selected intent promotion did not produce a reviewable promoted candidate.")
    if not rows:
        add("NO_CANDIDATE_REASON_UNSPECIFIED", _first_text(diagnostics.get("zero_candidate_explanation"), "Diagnostics reported zero candidates without a more specific categorized reason."))
    return rows


def build_candidate_generation_visibility_v1(diagnostics: dict[str, Any]) -> dict[str, Any]:
    diagnostics = _safe_dict(diagnostics)
    sleeve_rows = build_sleeve_execution_summary_v1(diagnostics)
    rejected_rows = build_rejected_candidate_visibility_v1(diagnostics)
    death_report = _safe_dict(diagnostics.get("real_signal_death_report"))
    candidate_contracts = _safe_dict(diagnostics.get("candidate_contracts"))
    signal_evidence_graph = _safe_dict(diagnostics.get("signal_evidence_graph"))
    return {
        "schema_id": "candidate_generation_visibility",
        "schema_version": "v1",
        "execution_coverage": build_execution_coverage_v1(diagnostics, sleeve_rows),
        "sleeve_execution_summary": sleeve_rows,
        "rejected_candidate_visibility": rejected_rows,
        "no_candidate_explanations": build_no_candidate_explanations_v1(diagnostics, sleeve_rows, rejected_rows),
        "candidate_contracts": candidate_contracts,
        "signal_evidence_graph": signal_evidence_graph,
        "signal_evidence_rows": _safe_list(signal_evidence_graph.get("signals")),
        "real_candidate_contracts": _safe_list(diagnostics.get("real_candidate_contracts")) or _safe_list(candidate_contracts.get("candidate_contracts")),
        "real_signal_death_report": death_report,
        "real_raw_signals": _safe_list(death_report.get("signals")),
        "rejection_stage_counts": _safe_dict(diagnostics.get("rejection_stage_counts")) or _safe_dict(death_report.get("rejection_stage_counts")),
        "top_missing_candidate_fields": _safe_list(diagnostics.get("top_missing_candidate_fields")) or _safe_list(death_report.get("top_missing_contract_fields")),
        "paper_golden_path_comparison_summary": _safe_dict(diagnostics.get("paper_golden_path_comparison_summary")) or _safe_dict(death_report.get("comparison_to_paper_golden_path")),
        "exact_next_repair_actions": _safe_list(death_report.get("exact_next_repair_actions")),
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "manual_capture_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }
