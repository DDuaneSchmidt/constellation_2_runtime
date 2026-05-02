#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1, git_commit_v1, git_dirty_status_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1


PRODUCER = "ops/tools/run_sleeve_intent_quality_diagnostics_v1.py"
SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_intent_quality_diagnostics.v1.schema.json"
SLEEVES = (
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
)


def diagnostics_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_intent_quality_diagnostics_v1" / day_utc / "sleeve_intent_quality_diagnostics.v1.json"


def _report_path(root: Path, family: str, day: str, filename: str) -> Path:
    return Path(root).resolve() / "reports" / family / day / filename


def _read_inputs(root: Path, day: str) -> tuple[dict[str, Path], dict[str, dict[str, Any]]]:
    paths = {
        "sleeve_evaluation_rollup": _report_path(root, "sleeve_evaluation_kernel_v1", day, "sleeve_evaluation_rollup.v1.json"),
        "portfolio_scoring": _report_path(root, "portfolio_scoring_v1", day, "portfolio_scoring.v1.json"),
        "risk_sizing_authority": _report_path(root, "risk_sizing_authority_v1", day, "risk_sizing_authority.v1.json"),
        "market_data_authority": _report_path(root, "market_data_authority_v1", day, "market_data_authority.v1.json"),
        "portfolio_account_authority": _report_path(root, "portfolio_account_authority_v1", day, "portfolio_account_authority.v1.json"),
    }
    return paths, {name: read_json_v1(path) for name, path in paths.items()}


def _canon_sleeve(value: Any) -> str:
    text = str(value or "").strip().upper()
    aliases = {
        "C2_TREND_EQ_PRIMARY": "C2_TREND_EQ_PRIMARY_V1",
        "C2_MEAN_REVERSION_EQ": "C2_MEAN_REVERSION_EQ_V1",
        "C2_MARKET_NEUTRAL_SPREAD": "C2_MARKET_NEUTRAL_SPREAD_V1",
        "C2_CROSS_ASSET_TREND": "C2_CROSS_ASSET_TREND_V1",
        "C2_EVENT_DISLOCATION": "C2_EVENT_DISLOCATION_V1",
        "C2_DEFENSIVE_TAIL": "C2_DEFENSIVE_TAIL_V1",
        "C2_VOL_INCOME_DEFINED_RISK": "C2_VOL_INCOME_DEFINED_RISK_V1",
    }
    return aliases.get(text, text)


def _rows_by_sleeve(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("outcomes", []) if isinstance(payload.get("outcomes"), list) else []:
        if isinstance(row, dict):
            out[_canon_sleeve(row.get("sleeve_id") or row.get("engine_id"))] = row
    return out


def _scoring_by_intent(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("rankings") if isinstance(payload.get("rankings"), list) else payload.get("ranked_intents")
    return {
        str(row.get("intent_id") or ""): row
        for row in rows or []
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    }


def _risk_by_intent(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("intent_id") or ""): row
        for row in payload.get("sizing_decisions", []) if isinstance(payload.get("sizing_decisions"), list)
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    }


def _input_paths(*items: Any) -> list[str]:
    paths: list[str] = []
    for item in items:
        if isinstance(item, str) and item:
            paths.append(item)
        elif isinstance(item, dict):
            path = str(item.get("path") or item.get("artifact_path") or "").strip()
            if path:
                paths.append(path)
        elif isinstance(item, list):
            paths.extend(_input_paths(*item))
    return sorted(set(paths))


def _code_text(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(code).strip() for code in value if str(code).strip())
    return str(value or "").strip()


def _missing_data_reason(outcome: dict[str, Any], market: dict[str, Any]) -> str:
    manifest = outcome.get("market_data_manifest_check") if isinstance(outcome.get("market_data_manifest_check"), dict) else {}
    blocker = str(manifest.get("canonical_blocker") or "").strip().upper()
    if blocker:
        return blocker
    if str(market.get("status") or "").upper() == "FAIL":
        return str(market.get("first_blocker") or market.get("market_data_state") or "MARKET_DATA_AUTHORITY_FAIL").strip().upper()
    return ""


def _build_sleeve_rows(outcomes: dict[str, dict[str, Any]], scoring_by_intent: dict[str, dict[str, Any]], risk_by_intent: dict[str, dict[str, Any]], market: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve_id in SLEEVES:
        outcome = outcomes.get(sleeve_id, {})
        output_intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
        rejected = outcome.get("rejected_intents") if isinstance(outcome.get("rejected_intents"), list) else []
        requested_symbols = outcome.get("producer_requested_symbols") if isinstance(outcome.get("producer_requested_symbols"), list) else []
        intent_ids = [str(row.get("intent_id") or "") for row in output_intents if isinstance(row, dict)]
        downstream = next((str(risk_by_intent.get(intent_id, {}).get("reason_code") or "") for intent_id in intent_ids if risk_by_intent.get(intent_id, {}).get("reason_code")), "")
        if not downstream:
            downstream = next((_code_text(scoring_by_intent.get(intent_id, {}).get("reason_codes")) for intent_id in intent_ids if scoring_by_intent.get(intent_id)), "")
        rows.append(
            {
                "sleeve_id": sleeve_id,
                "status": str(outcome.get("status") or "MISSING").strip().upper(),
                "evaluated_universe_count": len(requested_symbols),
                "trigger_count": len(output_intents),
                "rejected_candidate_count": len(rejected),
                "no_intent_reason": ",".join(str(code) for code in outcome.get("reason_codes", []) if str(code)) if str(outcome.get("status") or "").upper() == "NO_INTENT" else "",
                "missing_data_reason": _missing_data_reason(outcome, market),
                "downstream_blocker": downstream,
                "intent_ids": intent_ids,
            }
        )
    return rows


def _build_intent_rows(outcomes: dict[str, dict[str, Any]], scoring_by_intent: dict[str, dict[str, Any]], risk_by_intent: dict[str, dict[str, Any]], market: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve_id, outcome in sorted(outcomes.items()):
        output_intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
        for intent in output_intents:
            if not isinstance(intent, dict):
                continue
            intent_id = str(intent.get("intent_id") or "").strip()
            score = scoring_by_intent.get(intent_id, {})
            risk = risk_by_intent.get(intent_id, {})
            downstream = str(risk.get("reason_code") or "").strip().upper()
            if not downstream and score:
                downstream = ",".join(str(code) for code in score.get("reason_codes", []) if str(code))
            rows.append(
                {
                    "sleeve_id": sleeve_id,
                    "intent_id": intent_id,
                    "signal_id": f"{sleeve_id}:{intent.get('symbol') or outcome.get('intent_symbol') or ''}",
                    "signal_trigger_reason": ",".join(str(code) for code in outcome.get("reason_codes", []) if str(code)),
                    "inputs_used": _input_paths(outcome.get("input_artifacts"), score.get("evidence_paths")),
                    "missing_inputs": [str(_missing_data_reason(outcome, market))] if _missing_data_reason(outcome, market) else [],
                    "trigger_threshold": "ENGINE_DEFINED_NOT_EXPORTED",
                    "rejection_reason": "" if output_intents else ",".join(str(code) for code in outcome.get("reason_codes", []) if str(code)),
                    "downstream_blocker": downstream,
                    "intent_reached_risk_sizing": bool(risk),
                    "symbol": str(intent.get("symbol") or outcome.get("intent_symbol") or "").strip().upper(),
                    "score_total": score.get("score_total"),
                    "score_components": score.get("score_components") if isinstance(score.get("score_components"), dict) else {},
                }
            )
    return rows


def _differentiation(intent_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in intent_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            by_symbol.setdefault(symbol, []).append(row)
    groups = [
        {
            "symbol": symbol,
            "sleeves": sorted({str(row.get("sleeve_id") or "") for row in rows}),
            "intent_ids": sorted(str(row.get("intent_id") or "") for row in rows),
            "diagnostic": "POSSIBLE_HIDDEN_CORRELATION_SAME_SYMBOL",
        }
        for symbol, rows in sorted(by_symbol.items())
        if len({str(row.get("sleeve_id") or "") for row in rows}) > 1
    ]
    return {"possible_hidden_correlation": bool(groups), "overlapping_symbol_groups": groups}


def _risk_diag(paths: dict[str, Path], payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    risk = payloads["risk_sizing_authority"]
    account = payloads["portfolio_account_authority"]
    status = str(risk.get("status") or "MISSING").strip().upper()
    risk_env = risk.get("risk_envelope") if isinstance(risk.get("risk_envelope"), dict) else {}
    account_values = account.get("account_values") if isinstance(account.get("account_values"), dict) else {}
    nav = account_values.get("net_liquidation_cents") or risk_env.get("nav_total_cents")
    root_cause = ""
    correction = ""
    if not risk:
        root_cause = "RISK_SIZING_AUTHORITY_MISSING"
        correction = "Run governed risk sizing with the truth_root and execution_root that contain capital envelope and account authority."
    elif str(risk.get("first_blocker") or "").upper() == "ACCOUNT_DATA_MISSING":
        root_cause = "ACCOUNT_NET_LIQUIDATION_MISSING"
        correction = "Provide governed broker account snapshot or operator statement with net_liquidation_cents; do not fabricate NAV."
    elif str(risk.get("first_blocker") or "").upper() == "CAPITAL_RISK_ENVELOPE_NOT_PASS":
        root_cause = "CAPITAL_RISK_ENVELOPE_NOT_PASS_OR_STALE_RISK_SIZING_ARTIFACT"
        correction = "Rerun governed risk sizing with the matching execution_root; if envelope still fails, fix governed NAV/account evidence."
    elif status == "FAIL":
        root_cause = str(risk.get("first_blocker") or risk.get("risk_sizing_state") or "RISK_SIZING_FAIL").upper()
        correction = "Resolve the governed risk sizing blocker before allocation."
    else:
        root_cause = "NONE"
        correction = ""
    if nav in (None, "", 0, "0"):
        if root_cause == "NONE":
            root_cause = "NAV_ZERO_OR_UNAVAILABLE"
        correction = correction or "Provide governed account/NAV evidence before risk sizing."
    return {
        "status": status,
        "risk_sizing_state": str(risk.get("risk_sizing_state") or "MISSING"),
        "first_blocker": str(risk.get("first_blocker") or ""),
        "nav_value": nav,
        "root_cause": root_cause,
        "correction_path": correction,
        "evidence_paths": [str(paths["risk_sizing_authority"]), str(paths["portfolio_account_authority"])],
    }


def build_sleeve_intent_quality_diagnostics_v1(*, day_utc: str, truth_root: Path, runtime_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    runtime = Path(runtime_root or truth_root).resolve()
    paths, payloads = _read_inputs(root, day_utc)
    outcomes = _rows_by_sleeve(payloads["sleeve_evaluation_rollup"])
    scoring = _scoring_by_intent(payloads["portfolio_scoring"])
    risk = _risk_by_intent(payloads["risk_sizing_authority"])
    sleeve_rows = _build_sleeve_rows(outcomes, scoring, risk, payloads["market_data_authority"])
    intent_rows = _build_intent_rows(outcomes, scoring, risk, payloads["market_data_authority"])
    return {
        "schema_id": "sleeve_intent_quality_diagnostics",
        "schema_version": "sleeve_intent_quality_diagnostics.v1",
        "day_utc": day_utc,
        "generated_at": now_iso_v1(),
        "git_commit": git_commit_v1(),
        "git_dirty_status": git_dirty_status_v1(),
        "truth_root": str(root),
        "runtime_root": str(runtime),
        "producer": PRODUCER,
        "authority": "DIAGNOSTIC_ONLY",
        "readiness_effect": "NONE",
        "submit_effect": "NONE",
        "allocation_effect": "NONE",
        "sleeve_diagnostics": sleeve_rows,
        "intent_diagnostics": intent_rows,
        "differentiation_diagnostics": _differentiation(intent_rows),
        "risk_sizing_diagnostics": _risk_diag(paths, payloads),
    }


def _validate(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("SLEEVE_INTENT_QUALITY_DIAGNOSTICS_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def write_diagnostics_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any], command: str) -> Path:
    path = diagnostics_path(truth_root=truth_root, day_utc=day_utc)
    input_paths, _ = _read_inputs(truth_root, day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name=PRODUCER,
        producer_command=command,
        input_artifacts=input_paths.values(),
        output_artifacts=[path],
        schema_versions={"sleeve_intent_quality_diagnostics_v1": "sleeve_intent_quality_diagnostics.v1"},
    )
    _validate(payload)
    write_json_v1(path, payload)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_intent_quality_diagnostics_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    args = parser.parse_args(argv)
    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else truth_root
    payload = build_sleeve_intent_quality_diagnostics_v1(day_utc=day, truth_root=truth_root, runtime_root=runtime_root)
    command = f"PYTHONPATH=\"$PWD\" python3 {PRODUCER} --day_utc {day} --truth_root {truth_root}"
    path = write_diagnostics_v1(truth_root=truth_root, day_utc=day, payload=payload, command=command)
    print(json.dumps({"status": "PASS", "path": str(path), "intent_count": len(payload["intent_diagnostics"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
