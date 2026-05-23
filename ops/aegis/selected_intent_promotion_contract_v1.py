from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_eod_v1 import artifact_ref_v1
from constellation_2.common.aegis_lite_promoted_candidates_v1 import (
    build_promoted_candidate_set_v1,
    write_promoted_candidate_set_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_selected_intent_promotion_v1"
RUN_ID = "selected_intent_promotion_v1"
PROMOTION_POLICY_ID = "selected_scored_intent_operator_review_promotion_v1"


def build_selected_intent_promotion_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    arbitration_path, arbitration = latest_json_v1(root, "intent_arbitration_v1", day_utc, "intent_arbitration.v1.json")
    scoring_path, scoring = latest_json_v1(root, "portfolio_scoring_v1", day_utc, "portfolio_scoring.v1.json")
    market_path, market_data = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    registry_path, data_registry = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    readiness_path, readiness = latest_json_v1(root, "aegis_sleeve_readiness_v1", day_utc, "sleeve_readiness.v1.json")

    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    intent_path = Path(str(selected.get("intent_path") or ""))
    intent = read_json_v1(intent_path) if str(intent_path) else {}
    symbol = str(selected.get("symbol") or (intent.get("underlying") or {}).get("symbol") or "").strip().upper()
    sleeve_id = str(selected.get("sleeve_id") or selected.get("engine_id") or "").strip()
    market_symbol = _market_symbol(market_data, data_registry, symbol)
    registry_item = _registry_item(data_registry, f"market.price.{symbol}")
    contract_checks = _contract_checks(
        day_utc=day_utc,
        arbitration=arbitration,
        selected=selected,
        scoring=scoring,
        intent=intent,
        symbol=symbol,
        sleeve_id=sleeve_id,
        market_symbol=market_symbol if isinstance(market_symbol, dict) else {},
        registry_item=registry_item,
        readiness=readiness,
    )
    missing = [row for row in contract_checks if row["status"] != "PASS" and row["required"]]
    generated_at = now_utc_v1()
    candidate = _candidate_from_selected(
        day_utc=day_utc,
        generated_at=generated_at,
        selected=selected,
        intent=intent,
        market_symbol=market_symbol if isinstance(market_symbol, dict) else {},
        source_paths=[str(path or "") for path in (arbitration_path, scoring_path, market_path, registry_path, readiness_path, intent_path) if str(path or "")],
    ) if not missing else {}
    candidate_set_path = ""
    if candidate:
        candidate_set = build_promoted_candidate_set_v1(
            day_utc=day_utc,
            run_id=f"{RUN_ID}:{day_utc}",
            generated_at_utc=generated_at,
            candidates=[candidate],
        )
        candidate_set_path = str(write_promoted_candidate_set_v1(truth_root=root, payload=candidate_set))
    status = "PROMOTED_TO_OPERATOR_REVIEW" if candidate else "CONTRACT_FAILED"
    payload = {
        "schema_id": "aegis_selected_intent_promotion",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "status": status,
        "promotion_policy_id": PROMOTION_POLICY_ID,
        "selected_intent_id": str(selected.get("intent_id") or ""),
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "contract_checks": contract_checks,
        "missing_contract_fields": [row["field"] for row in missing],
        "rejection_reason": "" if candidate else "SELECTED_INTENT_PROMOTION_CONTRACT_FAILED",
        "promotion_status": "operator_reviewable" if candidate else "blocked",
        "operator_review_required": True,
        "automatic_approval_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_called": False,
        "promoted_candidate_count": 1 if candidate else 0,
        "candidate": candidate,
        "promoted_candidate_set_path": candidate_set_path,
        "input_artifacts": {
            "intent_arbitration": str(arbitration_path or ""),
            "portfolio_scoring": str(scoring_path or ""),
            "selected_intent": str(intent_path) if str(intent_path) else "",
            "market_data": str(market_path or ""),
            "data_registry": str(registry_path or ""),
            "sleeve_readiness": str(readiness_path or ""),
        },
        "source_hashes": _source_hashes([path for path in (arbitration_path, scoring_path, market_path, registry_path, readiness_path, intent_path) if path]),
        "safety": {
            "review_only": True,
            "human_review_required": True,
            "fake_opportunity_created": False,
            "fake_score_created": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
            "manual_approval_bypass_allowed": False,
        },
    }
    return payload


def write_selected_intent_promotion_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "selected_intent_promotion.v1.json", payload)
    summary_path = out_dir / "selected_intent_promotion.summary.txt"
    matrix_path = out_dir / "selected_intent_promotion.matrix.csv"
    summary_path.write_text(render_selected_intent_promotion_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_selected_intent_promotion_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_selected_intent_promotion_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS SELECTED INTENT PROMOTION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"selected_intent_id: {payload.get('selected_intent_id')}",
        f"sleeve_id: {payload.get('sleeve_id')}",
        f"symbol: {payload.get('symbol')}",
        f"promoted_candidate_count: {payload.get('promoted_candidate_count')}",
        "missing_contract_fields:",
    ]
    for field in payload.get("missing_contract_fields") or []:
        lines.append(f"- {field}")
    lines.extend(["", "broker_execution_allowed: false", "autonomous_execution_allowed: false", "automatic_approval_allowed: false", ""])
    return "\n".join(lines)


def render_selected_intent_promotion_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["field", "required", "status", "value", "reason"])
    writer.writeheader()
    for row in payload.get("contract_checks") or []:
        if isinstance(row, dict):
            writer.writerow(
                {
                    "field": row.get("field", ""),
                    "required": row.get("required", ""),
                    "status": row.get("status", ""),
                    "value": row.get("value", ""),
                    "reason": row.get("reason", ""),
                }
            )
    return out.getvalue()


def _contract_checks(
    *,
    day_utc: str,
    arbitration: dict[str, Any],
    selected: dict[str, Any],
    scoring: dict[str, Any],
    intent: dict[str, Any],
    symbol: str,
    sleeve_id: str,
    market_symbol: dict[str, Any],
    registry_item: dict[str, Any],
    readiness: dict[str, Any],
) -> list[dict[str, Any]]:
    score_row = _score_row(scoring, str(selected.get("intent_id") or ""))
    return [
        _check("intent_arbitration.status", str(arbitration.get("status") or ""), "SELECTED", "Selected intent arbitration must have selected this intent."),
        _check_present("selected_intent.intent_id", selected.get("intent_id"), "Selected intent id is required."),
        _check_present("selected_intent.sleeve_id", sleeve_id, "Selected intent sleeve id is required."),
        _check_present("selected_intent.symbol", symbol, "Selected intent symbol is required."),
        _check_intent_symbol("selected_intent.symbol_matches_intent", symbol, intent),
        _check_intent_engine("selected_intent.sleeve_matches_intent", sleeve_id, intent),
        _check("portfolio_scoring.status", str(selected.get("portfolio_scoring_status") or ""), "SCORED", "Selected intent must have a real portfolio score."),
        _check_present("portfolio_scoring.score_total", selected.get("portfolio_score_total"), "Score total is required."),
        _check_rank("portfolio_scoring.rank", selected.get("portfolio_score_rank")),
        _check_bool("portfolio_scoring.executable_eligible", selected.get("executable_eligible"), True, "Scoring must mark the selected intent eligible for review promotion."),
        _check("portfolio_scoring.ranked_intent_present", "YES" if score_row else "NO", "YES", "Scoring artifact must contain the selected intent row."),
        _check_present("intent.path", selected.get("intent_path"), "Selected intent artifact path is required."),
        _check("intent.schema_id", str(intent.get("schema_id") or ""), "exposure_intent", "Selected artifact must be an exposure intent."),
        _check("intent.exposure_type", str(intent.get("exposure_type") or ""), "LONG_EQUITY", "Only long-equity review candidates are supported by this v1 contract."),
        _check_present("intent.target_notional_pct", intent.get("target_notional_pct"), "Target notional percentage is required as review context."),
        _check_present("intent.constraints.max_risk_pct", (intent.get("constraints") or {}).get("max_risk_pct") if isinstance(intent.get("constraints"), dict) else "", "Max risk percentage is required as review context."),
        _check("market.price.status", str(market_symbol.get("freshness_status") or registry_item.get("status") or ""), "CURRENT", "Current selected-symbol price is required."),
        _check("market.price.session_date", str(market_symbol.get("market_session_date") or registry_item.get("market_session_date") or ""), day_utc, "Selected-symbol market data must match the target session."),
        _check_present("market.price.last_price", market_symbol.get("last_price") or market_symbol.get("last") or market_symbol.get("value") or market_symbol.get("close") or registry_item.get("value"), "A real selected-symbol entry reference price is required."),
        _check_sleeve_ready(readiness, sleeve_id),
        _check("safety.operator_review_required", "true", "true", "Promotion output must require operator review."),
        _check("safety.broker_execution_allowed", "false", "false", "Promotion output must not allow broker execution."),
        _check("safety.autonomous_execution_allowed", "false", "false", "Promotion output must not allow autonomous execution."),
        _check("safety.automatic_approval_allowed", "false", "false", "Promotion output must not allow automatic approval."),
    ]


def _candidate_from_selected(
    *,
    day_utc: str,
    generated_at: str,
    selected: dict[str, Any],
    intent: dict[str, Any],
    market_symbol: dict[str, Any],
    source_paths: list[str],
) -> dict[str, Any]:
    intent_id = str(selected.get("intent_id") or "")
    sleeve_id = str(selected.get("sleeve_id") or "")
    symbol = str(selected.get("symbol") or (intent.get("underlying") or {}).get("symbol") or "").upper()
    score = str(selected.get("portfolio_score_total") or "")
    rank = str(selected.get("portfolio_score_rank") or "")
    candidate_id = "review_" + canonical_hash_for_c2_artifact_v1({"day_utc": day_utc, "intent_id": intent_id, "policy": PROMOTION_POLICY_ID})[:20]
    source_refs = [artifact_ref_v1(path, artifact_type="selected_intent_promotion_input") for path in source_paths if path]
    return {
        "candidate_id": candidate_id,
        "raw_signal_id": intent_id,
        "intent_id": intent_id,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": str(market_symbol.get("last_price") or market_symbol.get("last") or market_symbol.get("value") or market_symbol.get("close") or ""),
        "suggested_quantity": 0,
        "sizing_guidance": "Operator review only; no shares authorized; no broker execution permitted.",
        "stop_price": "",
        "stop_logic": "NOT_SUPPLIED_BY_INTENT_REVIEW_ONLY",
        "risk_per_trade": str((intent.get("constraints") or {}).get("max_risk_pct") or ""),
        "sleeve_ownership": sleeve_id,
        "confidence": f"PORTFOLIO_SCORE_{score}" if score else "SCORED",
        "conviction": f"RANK_{rank}" if rank else "SCORED",
        "reason_codes": [
            "SELECTED_BY_INTENT_ARBITRATION",
            "PORTFOLIO_SCORING_EXECUTABLE_ELIGIBLE",
            "OPERATOR_REVIEW_REQUIRED",
            "NON_EXECUTABLE_REVIEW_ONLY",
            "BROKER_EXECUTION_DISABLED",
            "AUTONOMOUS_EXECUTION_DISABLED",
        ],
        "edge_family": sleeve_id,
        "thesis_id": intent_id,
        "shared_risk_tags": ["US_EQUITY_BETA", sleeve_id],
        "correlated_symbols": [],
        "regime_dependency": "TREND_OR_BOOTSTRAP_ACCEPTED",
        "macro_sensitivity": "EQUITY_BETA",
        "volatility_liquidity_dependency": "ETF_LIQUIDITY",
        "promotion_status": "promoted",
        "review_only": True,
        "human_review_required": True,
        "operator_review_status": "AWAITING_OPERATOR_REVIEW",
        "automatic_approval_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "dry_run_only": False,
        "demo_mode": False,
        "generated_at": generated_at,
        "source_run_id": f"{RUN_ID}:{day_utc}",
        "source_promotion_refs": source_refs,
        "source_artifact_refs": source_refs,
        "promotion_contract": {
            "policy_id": PROMOTION_POLICY_ID,
            "status": "PASS",
            "selected_intent_id": intent_id,
            "portfolio_score_total": score,
            "portfolio_score_rank": rank,
            "review_only": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
        },
    }


def _score_row(scoring: dict[str, Any], intent_id: str) -> dict[str, Any]:
    rows = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict) and str(row.get("intent_id") or "") == intent_id:
            return row
    return {}



def _data_items(data_registry: dict[str, Any]) -> list[dict[str, Any]]:
    rows = data_registry.get("data_items") if isinstance(data_registry.get("data_items"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _market_symbol(market_data: dict[str, Any], data_registry: dict[str, Any], symbol: str) -> dict[str, Any]:
    symbol = str(symbol or "").strip().upper()
    symbols = market_data.get("symbols") if isinstance(market_data.get("symbols"), dict) else {}
    row = symbols.get(symbol) if isinstance(symbols.get(symbol), dict) else {}
    if row:
        return dict(row)
    for record in market_data.get("normalized_records") if isinstance(market_data.get("normalized_records"), list) else []:
        if not isinstance(record, dict):
            continue
        record_symbol = str(record.get("canonical_symbol") or record.get("symbol") or "").strip().upper()
        if record_symbol == symbol:
            return {
                **record,
                "freshness_status": str(record.get("freshness_status") or record.get("status") or ""),
                "last_price": record.get("last") or record.get("last_price") or record.get("value") or record.get("close"),
            }
    registry_item = _registry_item(data_registry, f"market.price.{symbol}")
    if registry_item:
        return {
            **registry_item,
            "freshness_status": str(registry_item.get("status") or ""),
            "last_price": registry_item.get("value") or registry_item.get("last_price") or registry_item.get("close"),
        }
    return {}


def _intent_symbol(intent: dict[str, Any]) -> str:
    underlying = intent.get("underlying") if isinstance(intent.get("underlying"), dict) else {}
    return str(intent.get("symbol") or underlying.get("symbol") or "").strip().upper()


def _intent_engine(intent: dict[str, Any]) -> str:
    engine = intent.get("engine") if isinstance(intent.get("engine"), dict) else {}
    return str(intent.get("sleeve_id") or intent.get("engine_id") or engine.get("engine_id") or "").strip()


def _check_intent_symbol(field: str, symbol: str, intent: dict[str, Any]) -> dict[str, Any]:
    expected = _intent_symbol(intent)
    actual = str(symbol or "").strip().upper()
    ok = bool(actual and (not expected or actual == expected))
    return {"field": field, "required": True, "status": "PASS" if ok else "FAIL", "value": actual, "expected": expected or "present", "reason": "Selected symbol must match the selected exposure intent."}


def _check_intent_engine(field: str, sleeve_id: str, intent: dict[str, Any]) -> dict[str, Any]:
    expected = _intent_engine(intent)
    actual = str(sleeve_id or "").strip()
    ok = bool(actual and (not expected or actual == expected))
    return {"field": field, "required": True, "status": "PASS" if ok else "FAIL", "value": actual, "expected": expected or "present", "reason": "Selected sleeve must match the selected exposure intent."}

def _registry_item(data_registry: dict[str, Any], data_item_id: str) -> dict[str, Any]:
    for row in _data_items(data_registry):
        if str(row.get("data_item_id") or "") == data_item_id:
            return row
    return {}


def _check(field: str, value: Any, expected: str, reason: str) -> dict[str, Any]:
    text = str(value or "")
    return {"field": field, "required": True, "status": "PASS" if text == expected else "FAIL", "value": text, "expected": expected, "reason": reason}


def _check_present(field: str, value: Any, reason: str) -> dict[str, Any]:
    text = str(value or "")
    return {"field": field, "required": True, "status": "PASS" if text else "FAIL", "value": text, "expected": "present", "reason": reason}


def _check_bool(field: str, value: Any, expected: bool, reason: str) -> dict[str, Any]:
    actual = bool(value)
    return {"field": field, "required": True, "status": "PASS" if actual is expected else "FAIL", "value": str(actual).lower(), "expected": str(expected).lower(), "reason": reason}


def _check_rank(field: str, value: Any) -> dict[str, Any]:
    try:
        rank = int(value)
    except Exception:
        rank = 0
    return {"field": field, "required": True, "status": "PASS" if rank > 0 else "FAIL", "value": str(value or ""), "expected": ">0", "reason": "Selected intent must have a deterministic portfolio rank."}


def _check_sleeve_ready(readiness: dict[str, Any], sleeve_id: str) -> dict[str, Any]:
    state = ""
    for row in readiness.get("sleeves") if isinstance(readiness.get("sleeves"), list) else []:
        if isinstance(row, dict) and str(row.get("sleeve_id") or "") == sleeve_id:
            state = str(row.get("readiness") or "")
            break
    return {"field": "sleeve_readiness.readiness", "required": True, "status": "PASS" if state in {"READY", "READY_WITH_WARNINGS"} else "FAIL", "value": state, "expected": "READY or READY_WITH_WARNINGS", "reason": "Only ready sleeves can promote selected intents."}


def _source_hashes(paths: list[Path | None]) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in paths:
        if not path:
            continue
        resolved = Path(path)
        try:
            out[str(resolved)] = artifact_ref_v1(resolved, artifact_type="source").get("sha256", "")
        except Exception:
            out[str(resolved)] = ""
    return out
