from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1

CONTEXT_FAMILY = "aegis_position_review_context_v1"
CONTEXT_FILENAME = "position_review_context.v1.json"
SCORE_FAMILY = "aegis_position_review_score_v1"
SCORE_FILENAME = "position_review_score.v1.json"
BRIEF_FAMILY = "aegis_position_review_brief_v1"
BRIEF_FILENAME = "position_review_brief.v1.json"

PROMPT_VERSION = "position_review_brief_prompt_v1"
MODEL_NAME = "DETERMINISTIC_CONSTRAINED_BRIEF_GENERATOR"
MODEL_VERSION = "phase1_no_live_llm"
TEMPERATURE = 0

INTERNAL_LABEL_TRANSLATIONS = {
    "CERTIFIED": "Supporting evidence confirmed.",
    "FULFILLED": "Required condition remains satisfied.",
    "PROMOTION_BLOCKED": "Evidence is not strong enough for higher-conviction promotion.",
    "REVIEW_ONLY": "Monitoring signal only; no action implied.",
    "EVIDENCE_NOT_DEMANDED": "Additional evidence exists but was not required for this position.",
    "EVIDENCE_UNCONSUMED": "Evidence was available but not used in the active decision path.",
    "LIFECYCLE_STATE_UNAVAILABLE": "Lifecycle state was not available in the review context.",
    "SIGNAL_EVIDENCE_UNAVAILABLE": "Signal evidence was not available in the review context.",
}

INTERNAL_LABELS = tuple(INTERNAL_LABEL_TRANSLATIONS.keys())

SAFETY = {
    "paper_only": True,
    "read_only": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
    "execution_recommendations_allowed": False,
}

ALLOWED_AI_SCOPE = [
    "explain",
    "summarize",
    "compare",
    "diagnose",
    "narrate",
]

FORBIDDEN_PATTERNS = [
    re.compile(r"\bbuy\b", re.IGNORECASE),
    re.compile(r"\bsell\b", re.IGNORECASE),
    re.compile(r"\bexit\s+now\b", re.IGNORECASE),
    re.compile(r"\bincrease\s+size\b", re.IGNORECASE),
    re.compile(r"\breduce\s+size\b", re.IGNORECASE),
    re.compile(r"\bexecute\b", re.IGNORECASE),
]


def position_review_context_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / CONTEXT_FAMILY / str(day_utc) / CONTEXT_FILENAME


def position_review_score_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / SCORE_FAMILY / str(day_utc) / SCORE_FILENAME


def position_review_brief_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / BRIEF_FAMILY / str(day_utc) / BRIEF_FILENAME


def build_position_review_context_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    source_paths = _source_paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in source_paths.items()}
    source_hashes = {key: _file_hash(path) for key, path in source_paths.items() if path.exists()}
    ledger = payloads.get("paper_position_ledger") or {}
    pnl = payloads.get("paper_pnl_report") or {}
    entries = _rows(payloads.get("entry_receipts") or {}, "receipts", "entry_receipts", "rows")
    exits = _rows(payloads.get("exit_receipts") or {}, "receipts", "exit_receipts", "rows")
    sleeve_analytics = payloads.get("sleeve_analytics") or {}
    candidate_lifecycle = payloads.get("candidate_lifecycle") or {}
    signal_boundary = payloads.get("signal_evidence_boundary") or {}
    signal_graph = payloads.get("signal_evidence_graph") or {}
    contracts = payloads.get("candidate_contracts") or {}
    market_data = payloads.get("market_data") or {}
    candidate_state = payloads.get("candidate_state") or {}
    duplicate_candidate = payloads.get("duplicate_candidate") or {}
    construction_boundary = payloads.get("construction_boundary") or {}
    contract_boundary = payloads.get("contract_boundary") or {}
    sleeve_evaluation = payloads.get("sleeve_evaluation") or {}
    sleeve_condition_audit = payloads.get("sleeve_condition_audit") or {}
    regime_bucket_ranking = payloads.get("regime_bucket_ranking") or {}
    pnl_by_position = {str(row.get("position_id") or ""): row for row in _rows(pnl, "open_positions")}
    entry_by_candidate = _index_rows(entries, "candidate_id", "candidate_contract_id")
    exit_by_position = _index_rows(exits, "position_id", "candidate_id")
    lifecycle_by_candidate = _index_rows(_rows(candidate_lifecycle, "current_session_candidates"), "candidate_id", "candidate_contract_id")
    signal_boundary_by_candidate = _index_rows(_rows(signal_boundary, "boundary_rows", "rows", "candidates"), "candidate_id", "candidate_contract_id")
    contract_by_candidate = _index_rows(_rows(contracts, "contracts", "rows", "candidate_contracts"), "candidate_id", "candidate_contract_id")
    candidate_state_by_candidate = _index_rows(_rows(candidate_state, "candidates", "active_candidates", "rows"), "candidate_id", "candidate_contract_id")
    duplicate_by_candidate = _index_rows(_rows(duplicate_candidate, "duplicate_rows", "rows", "candidates"), "candidate_id", "candidate_contract_id")
    construction_by_candidate = _index_rows(_rows(construction_boundary, "boundary_rows", "rows", "candidates"), "candidate_id", "candidate_contract_id")
    contract_boundary_by_candidate = _index_rows(_rows(contract_boundary, "boundary_rows", "rows", "candidates"), "candidate_id", "candidate_contract_id")
    signal_graph_rows = _rows(signal_graph, "signals")
    signal_graph_by_raw = _index_rows(signal_graph_rows, "raw_signal_id", "intent_id")
    signal_graph_by_symbol_sleeve = {
        f"{str(row.get('symbol') or '').upper()}|{str(row.get('sleeve_id') or '')}": row
        for row in signal_graph_rows
        if str(row.get("symbol") or "") and str(row.get("sleeve_id") or "")
    }
    sleeve_by_id = {str(row.get("sleeve_id") or ""): row for row in _rows(sleeve_analytics, "sleeves")}
    sleeve_evaluation_by_id = {str(row.get("sleeve_id") or ""): row for row in _rows(sleeve_evaluation, "sleeves")}
    sleeve_condition_by_id = {str(row.get("sleeve_id") or ""): row for row in _rows(sleeve_condition_audit, "sleeves")}
    market_by_symbol = _market_rows_by_symbol(market_data)

    contexts: list[dict[str, Any]] = []
    for position in _rows(ledger, "open_positions"):
        position_id = str(position.get("position_id") or "")
        candidate_id = str(position.get("candidate_id") or "")
        symbol = str(position.get("symbol") or "").upper()
        pnl_row = pnl_by_position.get(position_id) or {}
        entry = entry_by_candidate.get(candidate_id) or {}
        exit_row = exit_by_position.get(position_id) or exit_by_position.get(candidate_id) or {}
        lifecycle = lifecycle_by_candidate.get(candidate_id) or {}
        signal_boundary_row = signal_boundary_by_candidate.get(candidate_id) or {}
        contract = contract_by_candidate.get(candidate_id) or {}
        candidate_state_row = candidate_state_by_candidate.get(candidate_id) or {}
        duplicate_row = duplicate_by_candidate.get(candidate_id) or {}
        construction_row = construction_by_candidate.get(candidate_id) or {}
        contract_boundary_row = contract_boundary_by_candidate.get(candidate_id) or {}
        sleeve_id = str(pnl_row.get("sleeve_id") or position.get("sleeve_id") or ((position.get("candidate_lineage") or {}) if isinstance(position.get("candidate_lineage"), dict) else {}).get("sleeve_id") or candidate_state_row.get("sleeve_id") or contract.get("sleeve_id") or "UNKNOWN")
        sleeve = sleeve_by_id.get(sleeve_id) or {}
        market = market_by_symbol.get(symbol) or {}
        ledger_lineage = position.get("candidate_lineage") if isinstance(position.get("candidate_lineage"), dict) else {}
        raw_signal_id = str(candidate_state_row.get("raw_signal_id") or ledger_lineage.get("raw_signal_id") or contract.get("raw_signal_id") or signal_boundary_row.get("raw_signal_id") or "")
        intent_id = str(candidate_state_row.get("intent_id") or ledger_lineage.get("intent_id") or contract.get("intent_id") or raw_signal_id or "")
        signal_graph_row = signal_graph_by_raw.get(raw_signal_id) or signal_graph_by_raw.get(intent_id) or signal_graph_by_symbol_sleeve.get(f"{symbol}|{sleeve_id}") or {}
        dynamic_paths = _candidate_evidence_paths(candidate_state_row, contract, signal_graph_row)
        dynamic_payloads = _read_dynamic_payloads(dynamic_paths)
        dynamic_hashes = {str(path): _file_hash(Path(path)) for path in dynamic_paths if Path(path).exists()}
        intent_snapshot = _first_dynamic_payload(dynamic_payloads, "exposure_intent")
        sleeve_run_evidence = _first_dynamic_payload(dynamic_payloads, "sleeve_evaluation")
        sleeve_eval = sleeve_run_evidence or sleeve_evaluation_by_id.get(sleeve_id) or {}
        sleeve_condition = sleeve_condition_by_id.get(sleeve_id) or {}
        signal_evidence = _signal_evidence_v1(
            signal_graph=signal_graph_row,
            signal_boundary=signal_boundary_row,
            candidate_state=candidate_state_row,
            contract=contract,
            intent_snapshot=intent_snapshot,
            raw_signal_id=raw_signal_id,
            intent_id=intent_id,
        )
        sleeve_evidence = _sleeve_evidence_v1(sleeve=sleeve, sleeve_evaluation=sleeve_eval, sleeve_condition=sleeve_condition)
        validation_evidence = _validation_evidence_v1(
            candidate_state=candidate_state_row,
            lifecycle=lifecycle,
            contract=contract,
            contract_boundary=contract_boundary_row,
            construction_boundary=construction_row,
            duplicate=duplicate_row,
            entry=entry,
        )
        market_regime_evidence = _market_regime_evidence_v1(
            signal_graph=signal_graph_row,
            sleeve_evaluation=sleeve_eval,
            sleeve_condition=sleeve_condition,
            regime_bucket_ranking=regime_bucket_ranking,
        )
        included = {
            "position": _compact_dict(position, [
                "position_id",
                "candidate_id",
                "symbol",
                "side",
                "quantity",
                "entry_price",
                "entry_time",
                "current_status",
                "mark_price",
                "current_certified_mark",
                "mark_certification_status",
                "mark_freshness_status",
                "mark_market_session_date",
                "unrealized_pnl",
                "unrealized_pnl_status",
            ]),
            "entry_receipt": _compact_dict(entry, [
                "receipt_id",
                "candidate_id",
                "candidate_contract_id",
                "paper_session_id",
                "symbol",
                "direction",
                "planned_entry",
                "actual_entry",
                "planned_stop",
                "actual_stop",
                "quantity",
                "timestamp_utc",
                "notes",
            ]),
            "exit_receipt": _compact_dict(exit_row, ["receipt_id", "position_id", "candidate_id", "symbol", "exit_price", "exit_timestamp_utc", "exit_reason"]),
            "paper_pnl": _compact_dict(pnl_row, [
                "position_id",
                "symbol",
                "sleeve_id",
                "quantity",
                "entry_price",
                "current_certified_mark",
                "mark_price",
                "unrealized_pnl",
                "unrealized_pnl_status",
                "mark_certification_status",
                "mark_freshness_status",
                "mark_timestamp_utc",
            ]),
            "sleeve": _compact_dict(sleeve, ["sleeve_id", "sleeve_name", "status", "open_positions", "market_value", "unrealized_pnl", "total_pnl", "mark_coverage_pct", "data_quality_status"]),
            "sleeve_evidence": sleeve_evidence,
            "signal_evidence": signal_evidence,
            "validation_evidence": validation_evidence,
            "market_regime_evidence": market_regime_evidence,
            "candidate_lineage": {
                "ledger_candidate_lineage": ledger_lineage,
                "candidate_state": _compact_dict(candidate_state_row, ["candidate_id", "symbol", "sleeve_id", "raw_signal_id", "review_status", "paper_position_status", "originating_day", "thesis_reason_codes", "warnings", "entry_reference_price"]),
                "candidate_lifecycle": _compact_dict(lifecycle, ["candidate_id", "candidate_contract_id", "paper_session_id", "symbol", "direction", "candidate_lifecycle_state", "status_message"]),
                "candidate_contract": _compact_dict(contract, ["candidate_id", "candidate_contract_id", "paper_session_id", "symbol", "direction", "sleeve", "strategy", "contract_status", "eligibility_status"]),
                "signal_evidence_boundary": _compact_dict(signal_boundary_row, ["symbol", "candidate_id", "paper_session_id", "boundary_status", "boundary_reason", "in_signal_evidence_graph", "in_output_intents"]),
            },
            "market_mark": _compact_dict(market, ["symbol", "last_price", "close", "freshness_status", "market_session_date", "provider", "source", "source_hash", "source_timestamp_utc"]),
            "source_lineage": {
                "dynamic_source_artifacts": dynamic_paths,
                "dynamic_source_artifact_hashes": dynamic_hashes,
            },
        }
        evidence_counts = _evidence_counts(included)
        null_reasons = _null_reasons(included)
        excluded = _excluded_evidence(included)
        quality = "PASS" if not null_reasons else "PARTIAL"
        context_seed = {
            "domain": "position",
            "target_id": position_id,
            "day_utc": day,
            "source_artifact_hashes": source_hashes,
            "included_evidence": included,
            "excluded_evidence": excluded,
            "null_reasons": null_reasons,
            "allowed_ai_scope": ALLOWED_AI_SCOPE,
        }
        context_hash = _stable_hash(context_seed)
        contexts.append({
            "context_id": f"position-review-context:{_short_hash(position_id or candidate_id or symbol)}",
            "domain": "position",
            "target_id": position_id,
            "position_id": position_id,
            "candidate_id": candidate_id,
            "symbol": symbol,
            "generated_at": generated_at,
            "as_of": str(pnl.get("generated_at_utc") or ledger.get("generated_at_utc") or generated_at),
            "source_artifacts": {key: str(path) for key, path in source_paths.items()},
            "source_artifact_hashes": source_hashes,
            "position_source_artifact_hashes": {**source_hashes, **dynamic_hashes},
            "input_context_hash": context_hash,
            "data_quality_status": quality,
            "evidence_counts": evidence_counts,
            "included_evidence": included,
            "excluded_evidence": excluded,
            "null_reasons": null_reasons,
            "allowed_ai_scope": list(ALLOWED_AI_SCOPE),
        })
    payload = {
        "schema_id": "aegis_position_review_context",
        "schema_version": "v1",
        "artifact_id": CONTEXT_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "as_of": str((payloads.get("paper_pnl_report") or {}).get("generated_at_utc") or generated_at),
        "summary": {
            "open_position_count": len(_rows(ledger, "open_positions")),
            "context_count": len(contexts),
            "partial_context_count": sum(1 for row in contexts if row.get("data_quality_status") != "PASS"),
            "evidence_count": sum((row.get("evidence_counts") or {}).get("evidence_count", 0) for row in contexts),
            "sleeve_evidence_count": sum((row.get("evidence_counts") or {}).get("sleeve_evidence_count", 0) for row in contexts),
            "signal_evidence_count": sum((row.get("evidence_counts") or {}).get("signal_evidence_count", 0) for row in contexts),
            "validation_evidence_count": sum((row.get("evidence_counts") or {}).get("validation_evidence_count", 0) for row in contexts),
            "market_regime_evidence_count": sum((row.get("evidence_counts") or {}).get("market_regime_evidence_count", 0) for row in contexts),
        },
        "contexts": contexts,
        "source_artifacts": {key: str(path) for key, path in source_paths.items()},
        "source_artifact_hashes": source_hashes,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at": "", "as_of": "", "content_hash": ""})
    return payload


def write_position_review_context_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_position_review_context_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(position_review_context_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def build_position_review_score_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    context_path = position_review_context_path_v1(truth_root=root, day_utc=day)
    context_payload = read_json_v1(context_path)
    if not context_payload:
        context_payload = build_position_review_context_v1(truth_root=root, day_utc=day)
    generated_at = now_utc_v1()
    scores = [_score_for_context(context, generated_at=generated_at) for context in _rows(context_payload, "contexts")]
    payload = {
        "schema_id": "aegis_position_review_score",
        "schema_version": "v1",
        "artifact_id": SCORE_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "as_of": str(context_payload.get("as_of") or generated_at),
        "source_context_path": str(context_path),
        "source_context_hash": str(context_payload.get("content_hash") or _file_hash(context_path)),
        "score_version": "position_review_score_v1",
        "summary": {
            "open_position_count": (context_payload.get("summary") or {}).get("open_position_count"),
            "score_count": len(scores),
            "improving_count": sum(1 for row in scores if row.get("position_health") == "IMPROVING"),
            "stable_count": sum(1 for row in scores if row.get("position_health") == "STABLE"),
            "deteriorating_count": sum(1 for row in scores if row.get("position_health") == "DETERIORATING"),
            "strengthened_count": sum(1 for row in scores if row.get("thesis_status") == "THESIS_STRENGTHENED"),
            "unchanged_count": sum(1 for row in scores if row.get("thesis_status") == "THESIS_UNCHANGED"),
            "weakened_count": sum(1 for row in scores if row.get("thesis_status") == "THESIS_WEAKENED"),
        },
        "scores": scores,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at": "", "as_of": "", "content_hash": ""})
    return payload


def write_position_review_score_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_position_review_score_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(position_review_score_path_v1(truth_root=truth_root, day_utc=day_utc), body)



def build_position_review_brief_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    context_path = position_review_context_path_v1(truth_root=root, day_utc=day)
    score_path = position_review_score_path_v1(truth_root=root, day_utc=day)
    context_payload = read_json_v1(context_path)
    if not context_payload:
        context_payload = build_position_review_context_v1(truth_root=root, day_utc=day)
    score_payload = read_json_v1(score_path)
    if not score_payload:
        score_payload = build_position_review_score_v1(truth_root=root, day_utc=day)
    scores_by_context = {str(row.get("context_id") or ""): row for row in _rows(score_payload, "scores")}
    generated_at = now_utc_v1()
    briefs = [_brief_for_context(context, generated_at=generated_at, score=scores_by_context.get(str(context.get("context_id") or "")) or {}) for context in _rows(context_payload, "contexts")]
    unsupported_count = sum(len(row.get("unsupported_claims") or []) for row in briefs)
    forbidden_count = sum(len(_forbidden_language_hits(row)) for row in briefs)
    internal_label_violation_count = sum(1 for row in briefs if _internal_label_hits(row) or _internal_label_ratio(row) > 0.30)
    payload = {
        "schema_id": "aegis_position_review_brief",
        "schema_version": "v1",
        "artifact_id": BRIEF_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "as_of": str(context_payload.get("as_of") or generated_at),
        "source_context_path": str(context_path),
        "source_context_hash": str(context_payload.get("content_hash") or _file_hash(context_path)),
        "source_score_path": str(score_path),
        "source_score_hash": str(score_payload.get("content_hash") or _file_hash(score_path)),
        "summary": {
            "open_position_count": (context_payload.get("summary") or {}).get("open_position_count"),
            "brief_count": len(briefs),
            "unsupported_claims_count": unsupported_count,
            "forbidden_language_violation_count": forbidden_count,
            "internal_label_translation_violation_count": internal_label_violation_count,
            "missing_position_health_count": sum(1 for row in briefs if not row.get("position_health")),
            "missing_thesis_status_count": sum(1 for row in briefs if not row.get("thesis_status")),
            "missing_key_insight_count": sum(1 for row in briefs if not row.get("key_insight")),
        },
        "briefs": briefs,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["status"] = "BLOCKED" if forbidden_count or internal_label_violation_count else ("PARTIAL" if unsupported_count else "CANONICAL")
    payload["content_hash"] = _stable_hash({**payload, "generated_at": "", "as_of": "", "content_hash": ""})
    return payload


def write_position_review_brief_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_position_review_brief_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(position_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def load_position_review_brief_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return read_json_v1(position_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc))


def self_check_position_review_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    context_path = position_review_context_path_v1(truth_root=root, day_utc=day)
    score_path = position_review_score_path_v1(truth_root=root, day_utc=day)
    brief_path = position_review_brief_path_v1(truth_root=root, day_utc=day)
    context = read_json_v1(context_path)
    score = read_json_v1(score_path)
    brief = read_json_v1(brief_path)
    failures: list[str] = []
    if not context:
        failures.append(f"missing context artifact: {context_path}")
    if not score:
        failures.append(f"missing score artifact: {score_path}")
    if not brief:
        failures.append(f"missing brief artifact: {brief_path}")
    contexts = {str(row.get("context_id") or ""): row for row in _rows(context, "contexts")}
    scores = {str(row.get("context_id") or ""): row for row in _rows(score, "scores")}
    briefs = _rows(brief, "briefs")
    for row in contexts.values():
        hashes = row.get("source_artifact_hashes") if isinstance(row.get("source_artifact_hashes"), dict) else {}
        if not hashes:
            failures.append(f"source artifact hash missing for context {row.get('context_id')}")
    for row in briefs:
        context_id = str(row.get("context_id") or "")
        matching = contexts.get(context_id)
        if not matching:
            failures.append(f"brief exists without matching context: {row.get('brief_id')}")
            continue
        if row.get("input_context_hash") != matching.get("input_context_hash"):
            failures.append(f"context hash mismatch for brief {row.get('brief_id')}")
        score_row = scores.get(context_id)
        if not score_row:
            failures.append(f"brief exists without matching deterministic score: {row.get('brief_id')}")
        elif row.get("score_id") != score_row.get("score_id"):
            failures.append(f"score id mismatch for brief {row.get('brief_id')}")
        if row.get("position_health") not in {"IMPROVING", "STABLE", "DETERIORATING"}:
            failures.append(f"position_health missing for brief {row.get('brief_id')}")
        if row.get("thesis_status") not in {"THESIS_STRENGTHENED", "THESIS_UNCHANGED", "THESIS_WEAKENED"}:
            failures.append(f"thesis_status missing for brief {row.get('brief_id')}")
        if not row.get("key_insight"):
            failures.append(f"Key Insight missing for brief {row.get('brief_id')}")
        if not _evidence_is_ranked(row.get("ranked_evidence")):
            failures.append(f"evidence not ranked for brief {row.get('brief_id')}")
        if not row.get("most_important_risk"):
            failures.append(f"primary risk missing for brief {row.get('brief_id')}")
        if not row.get("most_important_confirmation"):
            failures.append(f"primary confirmation missing for brief {row.get('brief_id')}")
        hits = _forbidden_language_hits(row)
        if hits:
            failures.append(f"forbidden trade advice language in brief {row.get('brief_id')}: {', '.join(hits)}")
        if row.get("unsupported_claims") and row.get("status") == "CANONICAL":
            failures.append(f"unsupported claims present but status canonical: {row.get('brief_id')}")
        if _brief_lacks_linked_evidence(row):
            failures.append(f"brief does not consume linked context evidence: {row.get('brief_id')}")
        label_hits = _internal_label_hits(row)
        if label_hits:
            failures.append(f"internal labels appear without translation in main brief {row.get('brief_id')}: {', '.join(label_hits)}")
        ratio = _internal_label_ratio(row)
        if ratio > 0.30:
            failures.append(f"visible brief text is mostly internal labels for {row.get('brief_id')}: {ratio:.2%}")
    pages_path = Path(__file__).resolve().parents[2] / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    pages = pages_path.read_text(encoding="utf-8") if pages_path.exists() else ""
    if "fetchAegisPositionReviewBrief" not in pages or "/api/aegis/position-review/latest" not in pages:
        failures.append("UI does not read the position review brief endpoint")
    if "OpenAI(" in pages or "responses.create" in pages or "chat.completions" in pages:
        failures.append("browser generates or calls AI content directly")
    result = {
        "ok": not failures,
        "day_utc": day,
        "context_path": str(context_path),
        "score_path": str(score_path),
        "brief_path": str(brief_path),
        "position_context_count": len(_rows(context, "contexts")),
        "position_score_count": len(_rows(score, "scores")),
        "position_brief_count": len(briefs),
        "unsupported_claims_count": sum(len(row.get("unsupported_claims") or []) for row in briefs),
        "forbidden_language_violation_count": sum(len(_forbidden_language_hits(row)) for row in briefs),
        "internal_label_translation_violation_count": sum(1 for row in briefs if _internal_label_hits(row) or _internal_label_ratio(row) > 0.30),
        "failure_count": len(failures),
        "failures": failures,
        **SAFETY,
    }
    return result


def _brief_for_context(context: Mapping[str, Any], *, generated_at: str, score: Mapping[str, Any] | None = None) -> dict[str, Any]:
    score = score or _score_for_context(context, generated_at=generated_at)
    evidence = context.get("included_evidence") if isinstance(context.get("included_evidence"), dict) else {}
    position = evidence.get("position") if isinstance(evidence.get("position"), dict) else {}
    entry = evidence.get("entry_receipt") if isinstance(evidence.get("entry_receipt"), dict) else {}
    pnl = evidence.get("paper_pnl") if isinstance(evidence.get("paper_pnl"), dict) else {}
    sleeve = evidence.get("sleeve") if isinstance(evidence.get("sleeve"), dict) else {}
    signal_evidence = evidence.get("signal_evidence") if isinstance(evidence.get("signal_evidence"), dict) else {}
    sleeve_evidence = evidence.get("sleeve_evidence") if isinstance(evidence.get("sleeve_evidence"), dict) else {}
    validation = evidence.get("validation_evidence") if isinstance(evidence.get("validation_evidence"), dict) else {}
    market_regime = evidence.get("market_regime_evidence") if isinstance(evidence.get("market_regime_evidence"), dict) else {}
    symbol = str(context.get("symbol") or position.get("symbol") or entry.get("symbol") or "Position").upper()
    sleeve_id = str(pnl.get("sleeve_id") or sleeve.get("sleeve_id") or signal_evidence.get("sleeve_id") or "UNKNOWN")
    entry_price = _first(position.get("entry_price"), entry.get("actual_entry"), entry.get("planned_entry"))
    mark = _first(position.get("current_certified_mark"), position.get("mark_price"), pnl.get("current_certified_mark"), pnl.get("mark_price"))
    pnl_value = _first(position.get("unrealized_pnl"), pnl.get("unrealized_pnl"))
    entry_thesis = _entry_thesis(symbol=symbol, entry=entry, sleeve_id=sleeve_id, signal=signal_evidence, validation=validation)
    supporting = _supporting_evidence(symbol=symbol, position=position, pnl=pnl, sleeve_id=sleeve_id, signal=signal_evidence, validation=validation, sleeve_evidence=sleeve_evidence)
    contradicting = _contradicting_evidence(symbol=symbol, position=position, pnl=pnl, sleeve_id=sleeve_id, signal=signal_evidence, validation=validation, market_regime=market_regime)
    validation_points = _validation_points(validation=validation, signal=signal_evidence)
    market_regime_points = _market_regime_points(market_regime=market_regime, signal=signal_evidence)
    risks = _risk_points(context=context, signal=signal_evidence, validation=validation, market_regime=market_regime)
    monitoring = _monitoring_points(entry_price=entry_price, mark=mark, pnl_value=pnl_value, signal=signal_evidence, validation=validation, market_regime=market_regime)
    data_quality = _brief_data_quality(context=context, signal=signal_evidence, validation=validation, market_regime=market_regime)
    ranked_evidence = score.get("evidence_rankings") if isinstance(score.get("evidence_rankings"), list) else []
    scored_supporting = [str(item.get("text") or "") for item in score.get("most_important_supporting_evidence", []) if isinstance(item, Mapping) and item.get("text")]
    scored_contradicting = [str(item.get("text") or "") for item in score.get("most_important_contradicting_evidence", []) if isinstance(item, Mapping) and item.get("text")]
    supporting = scored_supporting[:3] or supporting[:3]
    contradicting = scored_contradicting[:3] or contradicting[:3]
    risks = [str(score.get("most_important_risk") or "")] + [item for item in risks if item != score.get("most_important_risk")]
    risks = [item for item in risks if item][:3]
    monitoring = _dedupe_texts([str(score.get("most_important_confirmation") or "")] + monitoring)[:5]
    unsupported: list[str] = []
    if not supporting:
        unsupported.append("No supporting evidence rows were present in the deterministic context.")
    if not validation_points:
        unsupported.append("No validation evidence rows were present in the deterministic context.")
    if not market_regime_points:
        unsupported.append("No market or regime context rows were present in the deterministic context.")
    status = "PARTIAL" if unsupported or context.get("data_quality_status") != "PASS" else "CANONICAL"
    brief = {
        "brief_id": f"position-review-brief:{_short_hash(str(context.get('target_id') or context.get('context_id') or symbol))}",
        "context_id": str(context.get("context_id") or ""),
        "position_id": str(context.get("position_id") or context.get("target_id") or ""),
        "target_id": str(context.get("target_id") or ""),
        "symbol": symbol,
        "sleeve_id": sleeve_id,
        "input_context_hash": str(context.get("input_context_hash") or ""),
        "prompt_version": PROMPT_VERSION,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "temperature": TEMPERATURE,
        "schema_version": "v1",
        "generated_at": generated_at,
        "status": status,
        "score_id": str(score.get("score_id") or ""),
        "score_version": str(score.get("score_version") or "position_review_score_v1"),
        "position_health": str(score.get("position_health") or "STABLE"),
        "position_health_explanation": str(score.get("position_health_explanation") or "Position health was computed from deterministic evidence."),
        "thesis_status": str(score.get("thesis_status") or "THESIS_UNCHANGED"),
        "thesis_status_explanation": str(score.get("thesis_status_explanation") or "Thesis status was computed from deterministic evidence."),
        "confidence": str(score.get("confidence") or "LOW"),
        "key_insight": _key_insight_from_score(symbol=symbol, score=score),
        "key_insight_inputs": score.get("key_insight_inputs") if isinstance(score.get("key_insight_inputs"), list) else [],
        "ranked_evidence": ranked_evidence,
        "most_important_supporting_evidence": score.get("most_important_supporting_evidence") if isinstance(score.get("most_important_supporting_evidence"), list) else [],
        "most_important_contradicting_evidence": score.get("most_important_contradicting_evidence") if isinstance(score.get("most_important_contradicting_evidence"), list) else [],
        "most_important_risk": str(score.get("most_important_risk") or "Risk to watch: context evidence may change after the next canonical refresh."),
        "most_important_confirmation": str(score.get("most_important_confirmation") or "Confirmation to monitor: next canonical evidence refresh."),
        "conclusion": _conclusion(symbol=symbol, sleeve_id=sleeve_id, signal=signal_evidence, validation=validation, pnl_value=pnl_value),
        "entry_thesis": entry_thesis,
        "thesis_summary": entry_thesis,
        "what_changed_since_entry": _what_changed(entry_price=entry_price, mark=mark, pnl_value=pnl_value, validation=validation, market_regime=market_regime),
        "supporting_evidence": supporting,
        "contradicting_evidence": contradicting,
        "validation_evidence": validation_points,
        "market_regime_context": market_regime_points,
        "risks": risks,
        "monitoring_points": monitoring,
        "data_quality": data_quality,
        "unsupported_claims": unsupported,
        "source_references": [
            {"artifact": "aegis_position_review_context_v1", "context_id": str(context.get("context_id") or ""), "input_context_hash": str(context.get("input_context_hash") or "")},
            {"artifact": "aegis_position_review_score_v1", "score_id": str(score.get("score_id") or ""), "score_version": str(score.get("score_version") or "position_review_score_v1")},
            {"artifact": "aegis_paper_position_ledger_v1", "position_id": str(context.get("target_id") or "")},
            {"artifact": "aegis_paper_pnl_report_v1", "position_id": str(context.get("target_id") or "")},
            {"artifact": "aegis_signal_evidence_graph_v1", "raw_signal_id": str(signal_evidence.get("raw_signal_id") or "")},
            {"artifact": "aegis_candidate_state_v1", "review_status": str(((validation.get("candidate_state") or {}) if isinstance(validation.get("candidate_state"), dict) else {}).get("review_status") or "")},
        ],
        "data_quality_status": str(context.get("data_quality_status") or "UNKNOWN"),
        "null_reasons": context.get("null_reasons") if isinstance(context.get("null_reasons"), dict) else {},
        "safety": dict(SAFETY),
    }
    hits = _forbidden_language_hits(brief)
    if hits:
        brief["status"] = "BLOCKED"
        brief["unsupported_claims"] = sorted(set(list(brief["unsupported_claims"]) + [f"FORBIDDEN_LANGUAGE:{hit}" for hit in hits]))
    return brief


def _score_for_context(context: Mapping[str, Any], *, generated_at: str) -> dict[str, Any]:
    evidence = context.get("included_evidence") if isinstance(context.get("included_evidence"), dict) else {}
    position = evidence.get("position") if isinstance(evidence.get("position"), dict) else {}
    entry = evidence.get("entry_receipt") if isinstance(evidence.get("entry_receipt"), dict) else {}
    pnl = evidence.get("paper_pnl") if isinstance(evidence.get("paper_pnl"), dict) else {}
    sleeve = evidence.get("sleeve") if isinstance(evidence.get("sleeve"), dict) else {}
    signal = evidence.get("signal_evidence") if isinstance(evidence.get("signal_evidence"), dict) else {}
    validation = evidence.get("validation_evidence") if isinstance(evidence.get("validation_evidence"), dict) else {}
    market_regime = evidence.get("market_regime_evidence") if isinstance(evidence.get("market_regime_evidence"), dict) else {}
    symbol = str(context.get("symbol") or position.get("symbol") or entry.get("symbol") or "Position").upper()
    sleeve_id = str(pnl.get("sleeve_id") or sleeve.get("sleeve_id") or signal.get("sleeve_id") or "UNKNOWN")
    entry_price = _first(position.get("entry_price"), entry.get("actual_entry"), entry.get("planned_entry"))
    mark = _first(position.get("current_certified_mark"), position.get("mark_price"), pnl.get("current_certified_mark"), pnl.get("mark_price"))
    pnl_value = _to_float(_first(position.get("unrealized_pnl"), pnl.get("unrealized_pnl")))
    rankings = _ranked_evidence_for_context(
        context=context,
        symbol=symbol,
        sleeve_id=sleeve_id,
        position=position,
        pnl=pnl,
        signal=signal,
        validation=validation,
        market_regime=market_regime,
    )
    supporting = [row for row in rankings if row.get("stance") == "SUPPORTING"]
    contradicting = [row for row in rankings if row.get("stance") == "CONTRADICTING"]
    support_score = sum(_priority_weight(row.get("priority")) for row in supporting[:6])
    contradict_score = sum(_priority_weight(row.get("priority")) for row in contradicting[:6])
    if pnl_value is not None and pnl_value > 0:
        support_score += 1
    elif pnl_value is not None and pnl_value < 0:
        contradict_score += 1
    if support_score - contradict_score >= 4:
        position_health = "IMPROVING"
        thesis_status = "THESIS_STRENGTHENED"
    elif contradict_score - support_score >= 3:
        position_health = "DETERIORATING"
        thesis_status = "THESIS_WEAKENED"
    else:
        position_health = "STABLE"
        thesis_status = "THESIS_UNCHANGED"
    counts = context.get("evidence_counts") if isinstance(context.get("evidence_counts"), dict) else {}
    has_core_evidence = all(int(counts.get(key) or 0) > 0 for key in ("signal_evidence_count", "validation_evidence_count", "market_regime_evidence_count"))
    if context.get("data_quality_status") == "PASS" and has_core_evidence and supporting:
        confidence = "HIGH" if len(contradicting) <= 2 else "MEDIUM"
    elif supporting and has_core_evidence:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"
    risk = _primary_risk(context=context, signal=signal, validation=validation, market_regime=market_regime, contradicting=contradicting)
    confirmation = _primary_confirmation(signal=signal, validation=validation, market_regime=market_regime, supporting=supporting)
    key_inputs = _dedupe_texts([
        f"position_health={position_health}",
        f"thesis_status={thesis_status}",
        f"confidence={confidence}",
        f"support_score={support_score}",
        f"contradicting_score={contradict_score}",
        f"unrealized_pnl={pnl_value if pnl_value is not None else 'not available'}",
        supporting[0].get("text") if supporting else "No high-priority supporting evidence was ranked.",
        contradicting[0].get("text") if contradicting else "No high-priority contradicting evidence was ranked.",
        risk,
        confirmation,
    ])
    return {
        "score_id": f"position-review-score:{_short_hash(str(context.get('target_id') or context.get('context_id') or symbol))}",
        "context_id": str(context.get("context_id") or ""),
        "target_id": str(context.get("target_id") or ""),
        "position_id": str(context.get("position_id") or context.get("target_id") or ""),
        "candidate_id": str(context.get("candidate_id") or ""),
        "symbol": symbol,
        "sleeve_id": sleeve_id,
        "generated_at": generated_at,
        "as_of": str(context.get("as_of") or generated_at),
        "input_context_hash": str(context.get("input_context_hash") or ""),
        "score_version": "position_review_score_v1",
        "position_health": position_health,
        "position_health_explanation": _position_health_explanation(position_health, support_score, contradict_score, pnl_value),
        "thesis_status": thesis_status,
        "thesis_status_explanation": _thesis_status_explanation(thesis_status, supporting, contradicting),
        "evidence_rankings": rankings,
        "confidence": confidence,
        "key_insight_inputs": key_inputs,
        "most_important_supporting_evidence": supporting[:3],
        "most_important_contradicting_evidence": contradicting[:3],
        "most_important_risk": risk,
        "most_important_confirmation": confirmation,
        "planned_entry": entry_price,
        "current_mark": mark,
        "unrealized_pnl": pnl_value,
        "data_quality_status": str(context.get("data_quality_status") or "UNKNOWN"),
        "safety": dict(SAFETY),
    }


def _ranked_evidence_for_context(*, context: Mapping[str, Any], symbol: str, sleeve_id: str, position: Mapping[str, Any], pnl: Mapping[str, Any], signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    def add(stance: str, priority: str, category: str, text: str, source: str) -> None:
        if text:
            rows.append({"stance": stance, "priority": priority, "category": category, "text": text, "source": source})
    certified_mark = position.get("mark_certification_status") == "CERTIFIED" or pnl.get("mark_certification_status") == "CERTIFIED"
    add("SUPPORTING", "HIGH", "Market Evidence", f"{symbol} has a certified current market mark for the review date.", "market_mark") if certified_mark else add("CONTRADICTING", "HIGH", "Market Evidence", "Current market mark is missing or not certified.", "market_mark")
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    certified = int(req.get("certified_count") or 0)
    total = int(req.get("required_count") or 0)
    if total and certified >= total:
        add("SUPPORTING", "HIGH", "Signal Evidence", "Required signal evidence coverage is complete.", "signal_evidence")
    elif certified > 0:
        add("SUPPORTING", "HIGH", "Signal Evidence", "Supporting signal evidence exists, but coverage remains limited.", "signal_evidence")
        add("CONTRADICTING", "HIGH", "Signal Evidence", "Signal evidence coverage is incomplete.", "signal_evidence")
    else:
        add("CONTRADICTING", "HIGH", "Signal Evidence", "Signal evidence coverage is missing or unavailable.", "signal_evidence")
    if signal.get("certification_status") == "CERTIFIED":
        add("SUPPORTING", "HIGH", "Signal Evidence", INTERNAL_LABEL_TRANSLATIONS["CERTIFIED"], "signal_evidence")
    if signal.get("fulfillment_status") == "FULFILLED":
        add("SUPPORTING", "HIGH", "Signal Evidence", INTERNAL_LABEL_TRANSLATIONS["FULFILLED"], "signal_evidence")
    failures = req.get("failure_reasons") if isinstance(req.get("failure_reasons"), list) else []
    for phrase in _translate_labels(failures):
        add("CONTRADICTING", "HIGH", "Signal Evidence", phrase, "signal_evidence")
    regime_rows = market_regime.get("signal_required_regime_evidence") if isinstance(market_regime.get("signal_required_regime_evidence"), list) else []
    if regime_rows:
        add("SUPPORTING", "HIGH", "Market Evidence", f"Market/regime evidence linked {len(regime_rows)} rows to this position review.", "market_regime_evidence")
    elif market_regime:
        add("SUPPORTING", "HIGH", "Market Evidence", "Market/regime context is present for this review.", "market_regime_evidence")
    contract = validation.get("contract") if isinstance(validation.get("contract"), dict) else {}
    construction = validation.get("construction_boundary") if isinstance(validation.get("construction_boundary"), dict) else {}
    lifecycle = validation.get("lifecycle") if isinstance(validation.get("lifecycle"), dict) else {}
    receipt = validation.get("operator_entry_receipt") if isinstance(validation.get("operator_entry_receipt"), dict) else {}
    if contract.get("contract_validation_status") == "VALID":
        add("SUPPORTING", "MEDIUM", "Validation Evidence", "Candidate contract checks passed.", "candidate_contract")
    elif contract:
        add("CONTRADICTING", "MEDIUM", "Validation Evidence", "Candidate contract evidence has limitations noted in diagnostics.", "candidate_contract")
    if construction.get("construction_result") == "CONSTRUCTED":
        add("SUPPORTING", "MEDIUM", "Validation Evidence", "Entry, stop, quantity, and risk construction evidence is present.", "construction_boundary")
    elif construction:
        add("CONTRADICTING", "MEDIUM", "Validation Evidence", "Paper construction evidence is incomplete or limited.", "construction_boundary")
    if lifecycle.get("candidate_lifecycle_state"):
        add("SUPPORTING", "MEDIUM", "Validation Evidence", _lifecycle_phrase(lifecycle.get("candidate_lifecycle_state"), detail=lifecycle.get("status_message")), "candidate_lifecycle")
    if receipt:
        add("SUPPORTING", "LOW", "Process Evidence", "The operator-recorded paper entry receipt is linked to this review.", "entry_receipt")
    if sleeve_id and sleeve_id != "UNKNOWN":
        add("SUPPORTING", "LOW", "Process Evidence", f"Sleeve attribution is recovered as {sleeve_id}.", "sleeve_attribution")
    else:
        add("CONTRADICTING", "LOW", "Process Evidence", "Sleeve attribution is not fully recovered.", "sleeve_attribution")
    duplicate = validation.get("duplicate_policy") if isinstance(validation.get("duplicate_policy"), dict) else {}
    if duplicate.get("suppressed") is True:
        add("CONTRADICTING", "MEDIUM", "Validation Evidence", _operator_duplicate_message(str(duplicate.get("operator_message") or "Refreshed signal has no material change.")), "duplicate_policy")
    if context.get("data_quality_status") != "PASS":
        add("CONTRADICTING", "MEDIUM", "Validation Evidence", "Review context has missing fields listed in null reasons.", "position_review_context")
    return _rank_and_dedupe_evidence(rows)


def _rank_and_dedupe_evidence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    ordered = sorted(rows, key=lambda row: (-_priority_weight(row.get("priority")), str(row.get("category") or ""), str(row.get("text") or "")))
    out = []
    for row in ordered:
        key = (str(row.get("stance") or ""), str(row.get("text") or ""))
        if key in seen:
            continue
        seen.add(key)
        ranked = dict(row)
        ranked["rank"] = len(out) + 1
        out.append(ranked)
    return out


def _priority_weight(priority: Any) -> int:
    return {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(str(priority or "").upper(), 0)


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, "", [], {}, "NOT_CANONICAL"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _position_health_explanation(health: str, support_score: int, contradict_score: int, pnl_value: float | None) -> str:
    pnl_text = "unavailable" if pnl_value is None else f"{pnl_value:.2f}"
    if health == "IMPROVING":
        return f"Supporting evidence outweighs contradicting evidence ({support_score} vs {contradict_score}) with unrealized paper P&L {pnl_text}."
    if health == "DETERIORATING":
        return f"Contradicting evidence outweighs supporting evidence ({contradict_score} vs {support_score}) with unrealized paper P&L {pnl_text}."
    return f"Supporting and contradicting evidence are balanced ({support_score} vs {contradict_score}) with unrealized paper P&L {pnl_text}."


def _thesis_status_explanation(status: str, supporting: list[Mapping[str, Any]], contradicting: list[Mapping[str, Any]]) -> str:
    top_support = str((supporting[0] or {}).get("text") if supporting else "supporting evidence is limited")
    top_contra = str((contradicting[0] or {}).get("text") if contradicting else "no major contradicting evidence was ranked")
    if status == "THESIS_STRENGTHENED":
        return f"The thesis strengthened because {top_support}"
    if status == "THESIS_WEAKENED":
        return f"The thesis weakened because {top_contra}"
    return f"The thesis is unchanged: {top_support}; {top_contra}."


def _primary_risk(*, context: Mapping[str, Any], signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any], contradicting: list[Mapping[str, Any]]) -> str:
    if contradicting:
        return f"Risk to watch: {str(contradicting[0].get('text') or '').rstrip('.')} .".replace(' .', '.')
    if context.get("data_quality_status") != "PASS":
        return "Risk to watch: review context has missing fields listed in null reasons."
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    if req.get("failure_reasons"):
        return "Risk to watch: some evidence categories were not required or used in the active decision path."
    return "Risk to watch: market marks and paper P&L can change after the next canonical refresh."


def _primary_confirmation(*, signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any], supporting: list[Mapping[str, Any]]) -> str:
    for row in supporting:
        if row.get("priority") == "HIGH":
            return f"Confirmation to monitor: {str(row.get('text') or '').rstrip('.')} .".replace(' .', '.')
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    if req:
        return f"Confirmation to monitor: signal evidence remains {int(req.get('certified_count') or 0)}/{int(req.get('required_count') or 0)} confirmed."
    return "Confirmation to monitor: next canonical evidence refresh preserves the linked signal and market context."


def _key_insight_from_score(*, symbol: str, score: Mapping[str, Any]) -> str:
    health = str(score.get("position_health") or "STABLE").replace("_", " ").lower()
    thesis = str(score.get("thesis_status") or "THESIS_UNCHANGED").replace("THESIS_", "").replace("_", " ").lower()
    support = "supporting evidence is limited"
    contra = "no major contradicting evidence was ranked"
    support_rows = score.get("most_important_supporting_evidence") if isinstance(score.get("most_important_supporting_evidence"), list) else []
    contra_rows = score.get("most_important_contradicting_evidence") if isinstance(score.get("most_important_contradicting_evidence"), list) else []
    if support_rows and isinstance(support_rows[0], Mapping):
        support = str(support_rows[0].get("text") or support)
    if contra_rows and isinstance(contra_rows[0], Mapping):
        contra = str(contra_rows[0].get("text") or contra)
    return f"{symbol} is {health} and the thesis is {thesis} because {support} Although {contra}"


def _evidence_is_ranked(value: Any) -> bool:
    rows = value if isinstance(value, list) else []
    if not rows:
        return False
    priorities = {"HIGH", "MEDIUM", "LOW"}
    for row in rows:
        if not isinstance(row, Mapping):
            return False
        if row.get("priority") not in priorities or not row.get("rank") or not row.get("text"):
            return False
    return True


def _dedupe_texts(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out



def _conclusion(*, symbol: str, sleeve_id: str, signal: Mapping[str, Any], validation: Mapping[str, Any], pnl_value: Any) -> str:
    lifecycle = (validation.get("lifecycle") or {}) if isinstance(validation.get("lifecycle"), dict) else {}
    lifecycle_text = _lifecycle_phrase(lifecycle.get("candidate_lifecycle_state"))
    signal_text = "the signal evidence is linked to the review context" if signal else INTERNAL_LABEL_TRANSLATIONS["SIGNAL_EVIDENCE_UNAVAILABLE"]
    return f"{symbol} is a read-only paper position review for {sleeve_id}. {signal_text}; {lifecycle_text}. Unrealized paper P&L is {pnl_value or 'not available'}."


def _entry_thesis(*, symbol: str, entry: Mapping[str, Any], sleeve_id: str, signal: Mapping[str, Any], validation: Mapping[str, Any]) -> str:
    parts = []
    intent = signal.get("intent_snapshot") if isinstance(signal.get("intent_snapshot"), dict) else {}
    underlying = intent.get("underlying") if isinstance(intent.get("underlying"), dict) else {}
    signal_type = _signal_type_phrase(_first(signal.get("signal_type"), intent.get("exposure_type")))
    risk_class = _risk_class_phrase(_first(signal.get("risk_class"), intent.get("risk_class")))
    if signal_type:
        parts.append(f"The entry thesis was a {signal_type} for {underlying.get('symbol') or symbol}.")
    if risk_class:
        parts.append(f"The position belongs to the {risk_class} sleeve theme.")
    if entry.get("planned_entry") or entry.get("planned_stop"):
        parts.append(f"The operator recorded planned entry {entry.get('planned_entry') or 'not available'} and planned stop {entry.get('planned_stop') or 'not available'}.")
    if sleeve_id and sleeve_id != "UNKNOWN":
        parts.append(f"Sleeve attribution is {sleeve_id}.")
    contract = validation.get("contract") if isinstance(validation.get("contract"), dict) else {}
    if contract:
        parts.append("This is a monitored paper position; no trade action is implied by the review.")
    return " ".join(parts) or "Entry thesis evidence is limited in the deterministic context."


def _supporting_evidence(*, symbol: str, position: Mapping[str, Any], pnl: Mapping[str, Any], sleeve_id: str, signal: Mapping[str, Any], validation: Mapping[str, Any], sleeve_evidence: Mapping[str, Any]) -> list[str]:
    out = [
        _sentence(f"{symbol} has a current market mark confirmed for the review date.", position.get("mark_certification_status") == "CERTIFIED" or pnl.get("mark_certification_status") == "CERTIFIED"),
        _sentence(f"The position remains attributed to {sleeve_id}.", bool(sleeve_id and sleeve_id != "UNKNOWN")),
        _sentence("Signal evidence is present for this position.", bool(signal.get("boundary_status"))),
        _sentence(INTERNAL_LABEL_TRANSLATIONS["CERTIFIED"], signal.get("certification_status") == "CERTIFIED"),
        _sentence(INTERNAL_LABEL_TRANSLATIONS["FULFILLED"], signal.get("fulfillment_status") == "FULFILLED"),
    ]
    contract = validation.get("contract") if isinstance(validation.get("contract"), dict) else {}
    construction = validation.get("construction_boundary") if isinstance(validation.get("construction_boundary"), dict) else {}
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    latest_run = sleeve_evidence.get("latest_run") if isinstance(sleeve_evidence.get("latest_run"), dict) else {}
    out.extend([
        _sentence("Candidate contract checks passed.", contract.get("contract_validation_status") == "VALID"),
        _sentence("Entry, stop, and quantity construction checks are present.", bool(construction.get("construction_result") or construction.get("boundary_status"))),
        _sentence(_coverage_phrase(req), req.get("required_count") not in (None, "", 0)),
        _sentence("The sleeve run produced usable review evidence for this position.", bool(latest_run.get("current_status") or latest_run.get("status"))),
    ])
    return [item for item in out if item]


def _contradicting_evidence(*, symbol: str, position: Mapping[str, Any], pnl: Mapping[str, Any], sleeve_id: str, signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any]) -> list[str]:
    out = [
        _sentence("Sleeve attribution is not fully recovered.", sleeve_id == "UNKNOWN"),
        _sentence("Current market mark is missing or not confirmed.", not (position.get("mark_certification_status") == "CERTIFIED" or pnl.get("mark_certification_status") == "CERTIFIED")),
    ]
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    failures = req.get("failure_reasons") if isinstance(req.get("failure_reasons"), list) else []
    translated_failures = _translate_labels(failures)
    if translated_failures:
        out.append(" ".join(translated_failures))
    duplicate = validation.get("duplicate_policy") if isinstance(validation.get("duplicate_policy"), dict) else {}
    if duplicate.get("suppressed") is True:
        out.append(_operator_duplicate_message(str(duplicate.get("operator_message") or "")))
    candidate_state = validation.get("candidate_state") if isinstance(validation.get("candidate_state"), dict) else {}
    warnings = candidate_state.get("warnings") if isinstance(candidate_state.get("warnings"), list) else []
    warning_text = _operator_warning_message(warnings)
    if warning_text:
        out.append(warning_text)
    regime_rows = market_regime.get("signal_required_regime_evidence") if isinstance(market_regime.get("signal_required_regime_evidence"), list) else []
    blocked_regime = [row for row in regime_rows if isinstance(row, Mapping) and row.get("failure_reason")]
    if blocked_regime:
        out.append("Market-regime evidence was available as a context category, but it was not required for this position's active decision path.")
    return [item for item in out if item]


def _validation_points(*, validation: Mapping[str, Any], signal: Mapping[str, Any]) -> list[str]:
    out = []
    lifecycle = validation.get("lifecycle") if isinstance(validation.get("lifecycle"), dict) else {}
    if lifecycle:
        out.append(_lifecycle_phrase(lifecycle.get("candidate_lifecycle_state"), detail=lifecycle.get("status_message")))
    contract_boundary = validation.get("contract_boundary") if isinstance(validation.get("contract_boundary"), dict) else {}
    if contract_boundary:
        out.append("Contract evidence is present and matched to this position." if contract_boundary.get("contract_outcome") == "VALID_CONTRACT" else "Contract evidence is present with limitations noted in diagnostics.")
    construction = validation.get("construction_boundary") if isinstance(validation.get("construction_boundary"), dict) else {}
    if construction:
        out.append("Paper construction evidence is present for entry, stop, quantity, and risk review." if construction.get("construction_result") == "CONSTRUCTED" else "Paper construction evidence is incomplete or limited.")
    duplicate = validation.get("duplicate_policy") if isinstance(validation.get("duplicate_policy"), dict) else {}
    if duplicate:
        out.append(_operator_duplicate_message(str(duplicate.get("operator_message") or "Signal refresh evidence was reviewed.")))
    receipt = validation.get("operator_entry_receipt") if isinstance(validation.get("operator_entry_receipt"), dict) else {}
    if receipt:
        out.append("The operator-recorded paper entry receipt is linked to this review.")
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    if req:
        out.append(_coverage_phrase(req))
    return out


def _market_regime_points(*, market_regime: Mapping[str, Any], signal: Mapping[str, Any]) -> list[str]:
    out = []
    context = market_regime.get("regime_bucket_context") if isinstance(market_regime.get("regime_bucket_context"), dict) else {}
    if context:
        top = context.get("top_ranked_candidate_id") or "not available"
        out.append(f"Market/regime context was available for this review; the diagnostic top-ranked candidate reference is {top}.")
    sleeve_context = market_regime.get("sleeve_market_context") if isinstance(market_regime.get("sleeve_market_context"), dict) else {}
    if sleeve_context:
        out.append(f"The sleeve market context covered {sleeve_context.get('resolved_symbol_count') or 'an unknown number of'} symbols for this review.")
    required = market_regime.get("signal_required_regime_evidence") if isinstance(market_regime.get("signal_required_regime_evidence"), list) else []
    if required:
        failures = sorted({str(row.get("failure_reason")) for row in required if isinstance(row, Mapping) and row.get("failure_reason")})
        translated = _translate_labels(failures)
        out.append(f"Market/regime evidence rows linked: {len(required)}. {' '.join(translated) if translated else 'No market/regime evidence limitations were reported.'}")
    elif signal.get("required_evidence_summary"):
        out.append("No separate market-regime evidence row was required beyond the signal evidence summary.")
    return out


def _risk_points(*, context: Mapping[str, Any], signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any]) -> list[str]:
    risks = [
        "Risk to watch: market marks and paper P&L can change after the next canonical refresh.",
        "Risk to watch: sleeve interpretation depends on recovered candidate and signal lineage.",
    ]
    duplicate = validation.get("duplicate_policy") if isinstance(validation.get("duplicate_policy"), dict) else {}
    if duplicate.get("suppressed") is True:
        risks.append("Risk to watch: this may be a refreshed signal rather than a materially different setup.")
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    if req.get("failure_reasons"):
        risks.append("Risk to watch: some additional evidence categories were available but not required or used in the active decision path.")
    if context.get("data_quality_status") != "PASS":
        risks.append("Risk to watch: the context has missing fields listed in null reasons.")
    return risks


def _monitoring_points(*, entry_price: Any, mark: Any, pnl_value: Any, signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any]) -> list[str]:
    points = [
        f"Monitoring point: compare recorded entry {entry_price or 'not available'} with the next confirmed market mark, currently {mark or 'not available'}.",
        f"Monitoring point: compare unrealized paper P&L {pnl_value or 'not available'} with sleeve-level paper results after each canonical refresh.",
    ]
    duplicate = validation.get("duplicate_policy") if isinstance(validation.get("duplicate_policy"), dict) else {}
    if duplicate:
        points.append("Monitoring point: watch for material improvement before treating refreshed evidence as a distinct new setup.")
    req = signal.get("required_evidence_summary") if isinstance(signal.get("required_evidence_summary"), dict) else {}
    if req:
        certified = req.get("certified_count", 0)
        total = req.get("required_count", 0)
        points.append(f"Monitoring point: signal evidence coverage is {certified}/{total} confirmed.")
    context = market_regime.get("regime_bucket_context") if isinstance(market_regime.get("regime_bucket_context"), dict) else {}
    if context.get("status"):
        points.append("Monitoring point: refresh market/regime context with the next governed evidence update.")
    return points


def _brief_data_quality(*, context: Mapping[str, Any], signal: Mapping[str, Any], validation: Mapping[str, Any], market_regime: Mapping[str, Any]) -> dict[str, Any]:
    counts = context.get("evidence_counts") if isinstance(context.get("evidence_counts"), dict) else {}
    return {
        "status": str(context.get("data_quality_status") or "UNKNOWN"),
        "summary": "Review context has linked signal, validation, and market/regime evidence." if signal and validation and market_regime else "Review context has missing linked evidence; see null reasons and source references.",
        "null_reasons": context.get("null_reasons") if isinstance(context.get("null_reasons"), dict) else {},
        "evidence_counts": counts,
        "has_signal_evidence": bool(signal),
        "has_validation_evidence": bool(validation),
        "has_market_regime_evidence": bool(market_regime),
    }


def _what_changed(*, entry_price: Any, mark: Any, pnl_value: Any, validation: Mapping[str, Any] | None = None, market_regime: Mapping[str, Any] | None = None) -> str:
    validation = validation or {}
    market_regime = market_regime or {}
    lifecycle = validation.get("lifecycle") if isinstance(validation.get("lifecycle"), dict) else {}
    duplicate = validation.get("duplicate_policy") if isinstance(validation.get("duplicate_policy"), dict) else {}
    context = market_regime.get("regime_bucket_context") if isinstance(market_regime.get("regime_bucket_context"), dict) else {}
    parts = [f"The confirmed market mark is {mark or 'not available'} versus recorded entry {entry_price or 'not available'}, with unrealized paper P&L {pnl_value or 'not available'}."]
    if lifecycle.get("candidate_lifecycle_state"):
        parts.append(_lifecycle_phrase(lifecycle.get("candidate_lifecycle_state")))
    if duplicate.get("operator_message"):
        parts.append(_operator_duplicate_message(str(duplicate.get("operator_message") or "")))
    if context.get("status"):
        parts.append("Market/regime context is available for this review.")
    return " ".join(parts)

def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json",
        "entry_receipts": root / "reports" / "aegis_paper_entry_receipts_v1" / day / "paper_entry_receipts.v1.json",
        "exit_receipts": root / "reports" / "aegis_paper_exit_receipts_v1" / day / "paper_exit_receipts.v1.json",
        "market_data": root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json",
        "signal_evidence_boundary": root / "reports" / "aegis_signal_evidence_boundary_v1" / day / "signal_evidence_boundary.v1.json",
        "candidate_lifecycle": root / "reports" / "aegis_candidate_lifecycle_projection_v1" / day / "candidate_lifecycle_projection.v1.json",
        "candidate_contracts": root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json",
        "paper_pnl_report": root / "reports" / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json",
        "sleeve_analytics": root / "reports" / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json",
        "signal_evidence_graph": root / "reports" / "aegis_signal_evidence_graph_v1" / day / "signal_evidence_graph.v1.json",
        "candidate_state": root / "reports" / "aegis_candidate_state_v1" / day / "candidate_state.v1.json",
        "duplicate_candidate": root / "reports" / "aegis_duplicate_candidate_v1" / day / "duplicate_candidate.v1.json",
        "construction_boundary": root / "reports" / "aegis_construction_input_boundary_v1" / day / "construction_input_boundary.v1.json",
        "contract_boundary": root / "reports" / "aegis_contract_generation_boundary_v1" / day / "contract_generation_boundary.v1.json",
        "sleeve_evaluation": root / "reports" / "aegis_sleeve_evaluation_v1" / day / "sleeve_evaluation.v1.json",
        "sleeve_condition_audit": root / "reports" / "aegis_sleeve_condition_audit_v1" / day / "sleeve_condition_audit.v1.json",
        "regime_bucket_ranking": root / "reports" / "regime_bucket_candidate_ranking_v1" / day / "regime_bucket_candidate_ranking.v1.json",
    }


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _index_rows(rows: list[dict[str, Any]], *keys: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in keys:
            value = str(row.get(key) or "")
            if value and value not in out:
                out[value] = row
    return out


def _market_rows_by_symbol(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("symbol_rows", "symbols", "rows"):
        value = payload.get(key)
        if isinstance(value, dict):
            rows.extend([dict(row) for row in value.values() if isinstance(row, Mapping)])
        elif isinstance(value, list):
            rows.extend([dict(row) for row in value if isinstance(row, Mapping)])
    return {str(row.get("symbol") or row.get("canonical_symbol") or "").upper(): row for row in rows if str(row.get("symbol") or row.get("canonical_symbol") or "")}


def _compact_dict(row: Mapping[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: row.get(key) for key in keys if row.get(key) not in (None, "", [], {})}


def _null_reasons(included: Mapping[str, Any]) -> dict[str, str]:
    reasons: dict[str, str] = {}
    required = {
        "position.entry_price": ((included.get("position") or {}) if isinstance(included.get("position"), dict) else {}).get("entry_price"),
        "position.quantity": ((included.get("position") or {}) if isinstance(included.get("position"), dict) else {}).get("quantity"),
        "paper_pnl.unrealized_pnl": ((included.get("paper_pnl") or {}) if isinstance(included.get("paper_pnl"), dict) else {}).get("unrealized_pnl"),
        "paper_pnl.sleeve_id": ((included.get("paper_pnl") or {}) if isinstance(included.get("paper_pnl"), dict) else {}).get("sleeve_id"),
        "market_mark.last_price_or_close": _first(((included.get("market_mark") or {}) if isinstance(included.get("market_mark"), dict) else {}).get("last_price"), ((included.get("market_mark") or {}) if isinstance(included.get("market_mark"), dict) else {}).get("close")),
    }
    for key, value in required.items():
        if value in (None, "", [], {}, "NOT_CANONICAL"):
            reasons[key] = "MISSING_FROM_DETERMINISTIC_CONTEXT"
    return reasons


def _excluded_evidence(included: Mapping[str, Any]) -> list[dict[str, str]]:
    excluded = []
    if not included.get("exit_receipt"):
        excluded.append({"evidence": "exit_receipt", "reason": "NO_EXIT_RECEIPT_FOR_OPEN_POSITION"})
    lineage = included.get("candidate_lineage") if isinstance(included.get("candidate_lineage"), dict) else {}
    if not lineage.get("signal_evidence_boundary"):
        excluded.append({"evidence": "signal_evidence_boundary", "reason": "NO_MATCHING_SIGNAL_BOUNDARY_ROW"})
    return excluded



def _candidate_evidence_paths(*rows: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        for key in ("evidence_paths", "source_artifacts"):
            value = row.get(key)
            if isinstance(value, list):
                paths.extend(str(item) for item in value if item)
            elif isinstance(value, dict):
                paths.extend(str(item) for item in value.values() if item)
        for key in (
            "evidence_path",
            "source_artifact",
            "source_artifact_path",
            "output_intent_path",
            "intent_path",
            "signal_evidence_graph_path",
        ):
            value = row.get(key)
            if value:
                paths.append(str(value))
    return _unique_existing_paths(paths)[:12]


def _unique_existing_paths(paths: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        if not raw:
            continue
        path = str(Path(raw).expanduser())
        if path in seen or not Path(path).exists():
            continue
        seen.add(path)
        out.append(path)
    return out


def _read_dynamic_payloads(paths: list[str]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for raw in paths:
        path = Path(raw)
        if path.suffix.lower() != ".json":
            continue
        payload = read_json_v1(path)
        if isinstance(payload, dict) and payload:
            payloads.append(payload)
    return payloads


def _first_dynamic_payload(payloads: list[dict[str, Any]], schema_id: str) -> dict[str, Any]:
    for payload in payloads:
        if str(payload.get("schema_id") or "") == schema_id:
            return payload
    return {}


def _signal_evidence_v1(
    *,
    signal_graph: Mapping[str, Any],
    signal_boundary: Mapping[str, Any],
    candidate_state: Mapping[str, Any],
    contract: Mapping[str, Any],
    intent_snapshot: Mapping[str, Any],
    raw_signal_id: str,
    intent_id: str,
) -> dict[str, Any]:
    required = signal_graph.get("required_evidence") if isinstance(signal_graph.get("required_evidence"), list) else []
    return _drop_empty({
        "raw_signal_id": raw_signal_id,
        "intent_id": intent_id,
        "signal_type": _first(signal_graph.get("signal_type"), contract.get("signal_type")),
        "direction": _first(signal_graph.get("direction"), contract.get("direction")),
        "sleeve_id": _first(signal_graph.get("sleeve_id"), candidate_state.get("sleeve_id"), contract.get("sleeve_id")),
        "governance_status": signal_graph.get("governance_status"),
        "fulfillment_status": signal_graph.get("fulfillment_status"),
        "certification_status": signal_graph.get("certification_status"),
        "boundary_status": signal_boundary.get("boundary_status"),
        "boundary_reason": signal_boundary.get("boundary_reason"),
        "candidate_review_status": candidate_state.get("review_status"),
        "thesis_reason_codes": candidate_state.get("thesis_reason_codes"),
        "warnings": candidate_state.get("warnings"),
        "intent_snapshot": _compact_dict(intent_snapshot, ["intent_id", "underlying", "engine", "exposure_type", "risk_class", "target_notional_pct", "expected_holding_days", "constraints"]),
        "required_evidence_summary": _required_evidence_summary(required),
        "required_evidence": [_compact_dict(row, ["evidence_id", "purpose", "data_item_id", "certified", "consumed", "failure_reason", "provider", "value", "timestamp_utc", "source_artifact_path", "source_hash"]) for row in required[:8] if isinstance(row, Mapping)],
    })


def _sleeve_evidence_v1(*, sleeve: Mapping[str, Any], sleeve_evaluation: Mapping[str, Any], sleeve_condition: Mapping[str, Any]) -> dict[str, Any]:
    return _drop_empty({
        "analytics": _compact_dict(sleeve, ["sleeve_id", "sleeve_name", "status", "open_positions", "market_value", "unrealized_pnl", "total_pnl", "mark_coverage_pct", "data_quality_status"]),
        "latest_run": _compact_dict(sleeve_evaluation, ["sleeve_id", "status", "signal_state", "current_status", "activation_status", "lifecycle_decision", "lifecycle_reason_codes", "reason_codes", "output_count", "output_intent_count", "rejected_count", "candidate_count", "canonical_blocker", "operator_next_action", "market_data_mode", "market_data_manifest_check", "nearest_miss_telemetry"]),
        "condition_audit": _compact_dict(sleeve_condition, ["sleeve_id", "current_classification", "condition_audit_conclusion", "latest_evaluation", "near_miss_analysis", "recommendation", "recommendation_reason"]),
    })


def _validation_evidence_v1(
    *,
    candidate_state: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    contract: Mapping[str, Any],
    contract_boundary: Mapping[str, Any],
    construction_boundary: Mapping[str, Any],
    duplicate: Mapping[str, Any],
    entry: Mapping[str, Any],
) -> dict[str, Any]:
    return _drop_empty({
        "candidate_state": _compact_dict(candidate_state, ["current_state", "review_status", "paper_position_status", "paper_trade_eligible", "operator_review_required", "operator_decision_required", "rollover_status", "warnings", "invalidation_reason"]),
        "lifecycle": _compact_dict(lifecycle, ["candidate_lifecycle_state", "candidate_readiness_status", "construction_present", "active_review_present", "status_message", "missing_required_fields"]),
        "contract": _compact_dict(contract, ["contract_validation_status", "market_data_status", "entry_reference_price_status", "signal_evidence_graph_status", "governance_status", "executable_status", "review_only"]),
        "contract_boundary": _compact_dict(contract_boundary, ["boundary_status", "boundary_reason", "contract_outcome", "market_data_valid", "market_data_reason", "in_signal_evidence_graph"]),
        "construction_boundary": _compact_dict(construction_boundary, ["boundary_status", "boundary_reason", "construction_attempted", "construction_result", "market_data_valid", "candidate_contract_present"]),
        "duplicate_policy": _compact_dict(duplicate, ["duplicate_classification", "duplicate_scope", "operator_message", "suppressed", "reviewable", "prior_candidate_id", "prior_position_id", "improvement_score", "improvement_reasons"]),
        "operator_entry_receipt": _compact_dict(entry, ["receipt_id", "event_type", "operator_entered", "receipt_type", "timestamp_utc", "planned_entry", "planned_stop", "actual_entry", "actual_stop", "quantity"]),
    })


def _market_regime_evidence_v1(
    *,
    signal_graph: Mapping[str, Any],
    sleeve_evaluation: Mapping[str, Any],
    sleeve_condition: Mapping[str, Any],
    regime_bucket_ranking: Mapping[str, Any],
) -> dict[str, Any]:
    required = signal_graph.get("required_evidence") if isinstance(signal_graph.get("required_evidence"), list) else []
    regime_rows = [row for row in required if isinstance(row, Mapping) and ("REGIME" in str(row.get("evidence_id") or "") or "regime" in str(row.get("data_item_id") or "").lower())]
    return _drop_empty({
        "signal_required_regime_evidence": [_compact_dict(row, ["evidence_id", "purpose", "data_item_id", "certified", "consumed", "failure_reason", "value", "timestamp_utc"]) for row in regime_rows],
        "sleeve_market_context": _compact_dict(sleeve_evaluation, ["market_data_mode", "market_data_manifest_check", "symbol_source", "resolved_symbol_count", "active_symbol_universe", "allowed_symbols"]),
        "condition_audit_latest_evaluation": (sleeve_condition.get("latest_evaluation") if isinstance(sleeve_condition.get("latest_evaluation"), dict) else {}),
        "regime_bucket_context": _compact_dict(regime_bucket_ranking, ["status", "recommendation", "selected_candidate_id", "top_ranked_candidate_id", "scoring_context", "missing_scoring_inputs"]),
    })


def _required_evidence_summary(rows: list[Any]) -> dict[str, Any]:
    valid_rows = [row for row in rows if isinstance(row, Mapping)]
    return {
        "required_count": len(valid_rows),
        "certified_count": sum(1 for row in valid_rows if row.get("certified") is True),
        "consumed_count": sum(1 for row in valid_rows if row.get("consumed") is True),
        "failure_reasons": sorted({str(row.get("failure_reason")) for row in valid_rows if row.get("failure_reason")}),
    }


def _evidence_counts(included: Mapping[str, Any]) -> dict[str, int]:
    signal_count = _count_evidence(included.get("signal_evidence"))
    sleeve_count = _count_evidence(included.get("sleeve_evidence"))
    validation_count = _count_evidence(included.get("validation_evidence"))
    market_regime_count = _count_evidence(included.get("market_regime_evidence"))
    base_count = sum(1 for key in ("position", "entry_receipt", "exit_receipt", "paper_pnl", "market_mark", "candidate_lineage") if included.get(key))
    return {
        "evidence_count": base_count + signal_count + sleeve_count + validation_count + market_regime_count,
        "sleeve_evidence_count": sleeve_count,
        "signal_evidence_count": signal_count,
        "validation_evidence_count": validation_count,
        "market_regime_evidence_count": market_regime_count,
    }


def _count_evidence(value: Any) -> int:
    if isinstance(value, Mapping):
        return sum(1 for item in value.values() if item not in (None, "", [], {}))
    if isinstance(value, list):
        return len([item for item in value if item not in (None, "", [], {})])
    return 1 if value not in (None, "", [], {}) else 0


def _drop_empty(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if value not in (None, "", [], {})}

def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _short_hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:20]


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}, "NOT_CANONICAL"):
            return value
    return None


def _sentence(text: str, condition: bool) -> str:
    return text if condition else ""




def _translate_label(value: Any) -> str:
    text = str(value or "")
    return INTERNAL_LABEL_TRANSLATIONS.get(text, text.replace("_", " ").lower())


def _translate_labels(values: list[Any]) -> list[str]:
    out: list[str] = []
    for value in values:
        phrase = _translate_label(value)
        if phrase and phrase not in out:
            out.append(phrase)
    return out


def _signal_type_phrase(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"EXPOSURE_INTENT", "LONG_EQUITY"}:
        return "long exposure thesis"
    if text:
        return text.replace("_", " ").lower()
    return ""


def _risk_class_phrase(value: Any) -> str:
    text = str(value or "").upper()
    if text == "CROSS_ASSET_TREND":
        return "cross-asset trend"
    if text == "LONG_EQUITY":
        return "long equity"
    if text:
        return text.replace("_", " ").lower()
    return ""


def _lifecycle_phrase(value: Any, *, detail: Any = None) -> str:
    text = str(value or "")
    if not text:
        return INTERNAL_LABEL_TRANSLATIONS["LIFECYCLE_STATE_UNAVAILABLE"]
    phrases = {
        "POSITION_OPEN": "The paper position is currently open",
        "PAPER_POSITION_OPEN": "The paper position is currently open",
        "POSITION_CLOSED": "The paper position is closed",
        "GENERATED": "The candidate was generated but no paper entry was recorded",
        "APPROVED_FOR_PAPER": "The candidate was approved for paper tracking",
        "REJECTED": "The candidate was marked not captured",
        "DEFERRED": "The candidate was deferred",
        "FAILED": "The candidate has a failed processing state",
    }
    phrase = phrases.get(text, text.replace("_", " ").lower())
    if detail:
        clean_detail = _operator_duplicate_message(str(detail))
        return f"{phrase}; {clean_detail}"
    return phrase


def _coverage_phrase(summary: Mapping[str, Any]) -> str:
    certified = int(summary.get("certified_count") or 0)
    total = int(summary.get("required_count") or 0)
    if total <= 0:
        return "Signal evidence coverage was not reported."
    if certified >= total:
        return "Supporting signal evidence coverage is complete."
    if certified > 0:
        return "Supporting signal evidence exists, but evidence coverage remains limited."
    return "Signal evidence coverage is limited."


def _operator_warning_message(warnings: list[Any]) -> str:
    labels = {str(item) for item in warnings}
    if "PROMOTION_BLOCKED" in labels and "REVIEW_ONLY" in labels:
        return "The position remains in monitoring mode because the evidence did not meet the threshold for stronger conviction."
    translated = _translate_labels(list(labels))
    return " ".join(translated)


def _operator_duplicate_message(message: str) -> str:
    if not message:
        return ""
    return message.replace("Signal refresh only — no material change requiring action.", "This appears to be a refreshed signal with no material change requiring a new operator action.").replace("Signal refresh only - no material change requiring action.", "This appears to be a refreshed signal with no material change requiring a new operator action.")


def _visible_brief_text(brief: Mapping[str, Any]) -> str:
    return json.dumps({
        "key_insight": brief.get("key_insight"),
        "position_health": brief.get("position_health"),
        "position_health_explanation": brief.get("position_health_explanation"),
        "thesis_status": brief.get("thesis_status"),
        "thesis_status_explanation": brief.get("thesis_status_explanation"),
        "conclusion": brief.get("conclusion"),
        "entry_thesis": brief.get("entry_thesis"),
        "thesis_summary": brief.get("thesis_summary"),
        "what_changed_since_entry": brief.get("what_changed_since_entry"),
        "supporting_evidence": brief.get("supporting_evidence"),
        "contradicting_evidence": brief.get("contradicting_evidence"),
        "validation_evidence": brief.get("validation_evidence"),
        "market_regime_context": brief.get("market_regime_context"),
        "most_important_risk": brief.get("most_important_risk"),
        "most_important_confirmation": brief.get("most_important_confirmation"),
        "risks": brief.get("risks"),
        "monitoring_points": brief.get("monitoring_points"),
        "most_important_risk": brief.get("most_important_risk"),
        "most_important_confirmation": brief.get("most_important_confirmation"),
        "data_quality": brief.get("data_quality"),
    }, sort_keys=True, ensure_ascii=True)


def _internal_label_hits(brief: Mapping[str, Any]) -> list[str]:
    text = _visible_brief_text(brief)
    return sorted({label for label in INTERNAL_LABELS if label in text})


def _internal_label_ratio(brief: Mapping[str, Any]) -> float:
    text = _visible_brief_text(brief)
    tokens = re.findall(r"[A-Za-z_]+", text)
    if not tokens:
        return 0.0
    internal = sum(1 for token in tokens if token in INTERNAL_LABELS or ("_" in token and token.isupper()))
    return internal / len(tokens)

def _brief_lacks_linked_evidence(brief: Mapping[str, Any]) -> bool:
    required_fields = [
        "key_insight",
        "position_health",
        "thesis_status",
        "entry_thesis",
        "supporting_evidence",
        "contradicting_evidence",
        "validation_evidence",
        "market_regime_context",
        "most_important_risk",
        "most_important_confirmation",
        "monitoring_points",
        "data_quality",
    ]
    for field in required_fields:
        value = brief.get(field)
        if value in (None, "", [], {}):
            return True
    text_fields = [
        brief.get("conclusion"),
        brief.get("entry_thesis"),
        brief.get("what_changed_since_entry"),
        brief.get("supporting_evidence"),
        brief.get("contradicting_evidence"),
        brief.get("validation_evidence"),
        brief.get("market_regime_context"),
        brief.get("monitoring_points"),
    ]
    text = json.dumps(text_fields, sort_keys=True, ensure_ascii=True).lower()
    linked_terms = ["signal", "contract", "construction", "lifecycle", "duplicate", "regime", "sleeve"]
    if sum(1 for term in linked_terms if term in text) < 4:
        return True
    price_only_terms = ["entry", "current mark", "paper p&l"]
    return all(term in text for term in price_only_terms) and not any(term in text for term in ["signal", "validation", "regime", "construction", "duplicate"])

def _forbidden_language_hits(brief: Mapping[str, Any]) -> list[str]:
    text = json.dumps({
        "key_insight": brief.get("key_insight"),
        "position_health_explanation": brief.get("position_health_explanation"),
        "thesis_status_explanation": brief.get("thesis_status_explanation"),
        "conclusion": brief.get("conclusion"),
        "thesis_summary": brief.get("thesis_summary"),
        "what_changed_since_entry": brief.get("what_changed_since_entry"),
        "supporting_evidence": brief.get("supporting_evidence"),
        "contradicting_evidence": brief.get("contradicting_evidence"),
        "most_important_risk": brief.get("most_important_risk"),
        "most_important_confirmation": brief.get("most_important_confirmation"),
        "risks": brief.get("risks"),
        "monitoring_points": brief.get("monitoring_points"),
    }, sort_keys=True, ensure_ascii=True)
    hits: list[str] = []
    for pattern in FORBIDDEN_PATTERNS:
        if pattern.search(text):
            hits.append(pattern.pattern)
    return hits
