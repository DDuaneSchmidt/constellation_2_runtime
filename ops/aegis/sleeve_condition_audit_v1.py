from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.sleeve_evaluation_v1 import sleeve_evaluation_path_v1


REPORT_FAMILY = "aegis_sleeve_condition_audit_v1"
REPORT_FILENAME = "sleeve_condition_audit.v1.json"

AUDITED_SLEEVES = [
    "C2_DEFENSIVE_TAIL_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
]

SAFETY = {
    "read_only": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
    "autonomous_live_trading_allowed": False,
    "candidate_creation_allowed": False,
    "sleeve_logic_modification_allowed": False,
    "threshold_modification_allowed": False,
    "universe_modification_allowed": False,
}

CONDITION_LIBRARY: dict[str, dict[str, Any]] = {
    "C2_DEFENSIVE_TAIL_V1": {
        "intended_purpose": {
            "market_condition": "Defensive/tail overlay for abnormal regime, high cross-asset correlation, or meaningful account drawdown.",
            "reasonable_trigger_frequency": "Infrequent; expected to be quiet during normal regimes and flat drawdown days.",
            "expected_quiet_most_days": True,
        },
        "current_trigger_conditions": {
            "thresholds_and_filters": [
                "Trigger if regime snapshot is not NORMAL.",
                "Trigger if max_pairwise_corr > 0.70.",
                "Trigger if account drawdown_pct < -0.05.",
            ],
            "required_data_inputs": [
                "market_data_inputs_v1 / market_data_snapshot_v1 for SPY context",
                "accounting_v1/nav drawdown snapshot",
                "positions_snapshot_v2",
                "correlation matrix/regime snapshot",
            ],
            "exclusion_rules": [
                "No hedge intent when regime is NORMAL, max correlation is at or below 0.70, and drawdown is at or above -5%.",
                "Missing required inputs fail closed as data-blocked rather than producing an intent.",
            ],
            "risk_gates": [
                "Target notional percent 0.05.",
                "Max risk percent 0.05.",
                "Paper/read-model diagnostics only; no broker execution.",
            ],
        },
    },
    "C2_EVENT_DISLOCATION_V1": {
        "intended_purpose": {
            "market_condition": "Tactical response to abnormal daily gap or intraday range dislocation.",
            "reasonable_trigger_frequency": "Event-driven and episodic; expected to be quiet unless large gaps or ranges appear.",
            "expected_quiet_most_days": True,
        },
        "current_trigger_conditions": {
            "thresholds_and_filters": [
                "Trigger if abs(gap_pct) >= 0.02.",
                "Trigger if range_pct >= 0.03.",
            ],
            "required_data_inputs": ["market_data_snapshot_v1 manifest and OHLCV JSONL day bars for the requested symbol universe"],
            "exclusion_rules": [
                "Symbols missing a target-day bar or required history do not emit intents.",
                "No intent when neither gap nor range threshold is reached.",
            ],
            "risk_gates": [
                "Target notional percent 0.05.",
                "Max risk percent 0.01.",
                "Expected holding period 2 days.",
                "Paper/read-model diagnostics only; no broker execution.",
            ],
        },
    },
    "C2_MARKET_NEUTRAL_SPREAD_V1": {
        "intended_purpose": {
            "market_condition": "ETF pair spread mean reversion when a spread z-score reaches an extreme.",
            "reasonable_trigger_frequency": "Sparse; expected to be quiet unless a configured pair spread is near a two-standard-deviation move.",
            "expected_quiet_most_days": True,
        },
        "current_trigger_conditions": {
            "thresholds_and_filters": [
                "Evaluate configured pairs SPY:QQQ, IWM:SPY, and HYG:LQD.",
                "Use lookback 60.",
                "Trigger when abs(z) > 2.0.",
                "Select the pair with the strongest absolute z-score when more than one pair qualifies.",
            ],
            "required_data_inputs": ["Market bars for HYG, IWM, LQD, QQQ, and SPY"],
            "exclusion_rules": [
                "No intent when all configured pairs have abs(z) <= 2.0.",
                "Missing pair history blocks the affected pair.",
            ],
            "risk_gates": [
                "Target notional percent 0.05.",
                "Max risk percent 0.01.",
                "Paper/read-model diagnostics only; no broker execution.",
            ],
        },
    },
    "C2_MEAN_REVERSION_EQ_V1": {
        "intended_purpose": {
            "market_condition": "Equity mean reversion after a materially oversold move relative to recent history.",
            "reasonable_trigger_frequency": "Quiet in normal/uptrend conditions; should trigger only on pronounced pullbacks.",
            "expected_quiet_most_days": True,
        },
        "current_trigger_conditions": {
            "thresholds_and_filters": [
                "Use window_days 20.",
                "Trigger when z <= -2.0.",
            ],
            "required_data_inputs": ["market_data_snapshot_v1 manifest and OHLCV JSONL history for the requested equity universe"],
            "exclusion_rules": [
                "Symbols missing target-day bars or sufficient history do not emit intents.",
                "No intent when z is above -2.0.",
            ],
            "risk_gates": [
                "Target notional percent 0.20.",
                "Max risk percent 0.02.",
                "Expected holding period 3 days.",
                "Paper/read-model diagnostics only; no broker execution.",
            ],
        },
    },
    "C2_VOL_INCOME_DEFINED_RISK_V1": {
        "intended_purpose": {
            "market_condition": "Defined-risk volatility-income candidate when recent realized volatility is elevated and trend filter passes.",
            "reasonable_trigger_frequency": "Occasional; expected to be quiet when realized-volatility percentile is low.",
            "expected_quiet_most_days": True,
        },
        "current_trigger_conditions": {
            "thresholds_and_filters": [
                "Use stdev_window 3.",
                "Use percentile_window 7.",
                "Trigger when realized-vol percentile >= 0.75.",
                "Require close >= SMA trend filter.",
            ],
            "required_data_inputs": ["market_data_snapshot_v1 manifest and OHLCV JSONL bars for GLD, HYG, IWM, QQQ, SPY, and TLT"],
            "exclusion_rules": [
                "No intent when realized-vol percentile is below 0.75 or trend filter fails.",
                "Symbols missing target-day bars or sufficient history do not emit intents.",
            ],
            "risk_gates": [
                "Target notional percent 0.01.",
                "Max risk percent 0.01.",
                "Paper/read-model diagnostics only; no broker execution.",
            ],
        },
    },
}


def sleeve_condition_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_sleeve_condition_audit_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _source_paths(root, day)
    sleeve_evaluation = _read_json(paths["aegis_sleeve_evaluation_v1"])
    rollup = _read_json(paths["sleeve_evaluation_kernel_v1"])
    eval_rows = {str(row.get("sleeve_id") or ""): row for row in _rows(sleeve_evaluation, "sleeves")}
    rollup_rows = {str(row.get("sleeve_id") or ""): row for row in _rows(rollup, "outcomes")}
    history = _historical_sanity(root, day, AUDITED_SLEEVES)

    sleeves = []
    for sleeve_id in AUDITED_SLEEVES:
        eval_row = eval_rows.get(sleeve_id, {})
        rollup_row = rollup_rows.get(sleeve_id, {})
        direct_row = _read_json(root / "reports" / "sleeve_evaluation_kernel_v1" / day / sleeve_id / "sleeve_evaluation.v1.json")
        sleeves.append(
            _build_audit_row(
                root=root,
                day=day,
                sleeve_id=sleeve_id,
                eval_row=eval_row,
                rollup_row=rollup_row,
                direct_row=direct_row,
                history=history.get(sleeve_id, {}),
            )
        )

    recommendation_counts = Counter(str(row.get("recommendation") or "") for row in sleeves)
    summary = {
        "audited_sleeve_count": len(sleeves),
        "correctly_silent_count": sum(1 for row in sleeves if bool(row.get("latest_evaluation", {}).get("silence_correct"))),
        "modification_review_warranted_count": sum(1 for row in sleeves if bool(row.get("modification_review_warranted"))),
        "recommendation_counts": dict(sorted(recommendation_counts.items())),
        "missing_nearest_miss_telemetry_count": sum(1 for row in sleeves if not row.get("near_miss_analysis", {}).get("nearest_miss_data_available")),
        "runtime_failure_count": sum(1 for row in sleeves if row.get("current_classification") == "SILENT_RUNTIME_FAILURE"),
        "data_blocked_count": sum(1 for row in sleeves if row.get("current_classification") == "SILENT_DATA_BLOCKED"),
    }
    payload = {
        "schema_id": "aegis_sleeve_condition_audit",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at": _now(),
        "status": "READY",
        "summary": summary,
        "sleeves": sleeves,
        "source_artifact_paths": {name: str(path) for name, path in paths.items() if path.exists()},
        "source_hashes": {name: _sha256(path) for name, path in paths.items() if path.exists()},
        "notes": [
            "This artifact audits silent sleeve trigger conditions from existing runtime evidence only.",
            "It does not modify sleeve thresholds, sleeve logic, universes, candidate generation, or trading safety gates.",
        ],
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_sleeve_condition_audit_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_sleeve_condition_audit_v1(truth_root=root, day_utc=day_utc))
    path = sleeve_condition_audit_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _build_audit_row(*, root: Path, day: str, sleeve_id: str, eval_row: Mapping[str, Any], rollup_row: Mapping[str, Any], direct_row: Mapping[str, Any], history: Mapping[str, Any]) -> dict[str, Any]:
    library = CONDITION_LIBRARY[sleeve_id]
    parsed = _parse_latest_conditions(sleeve_id=sleeve_id, root=root, day=day, eval_row=eval_row, rollup_row=rollup_row, direct_row=direct_row)
    current_classification = str(eval_row.get("classification") or _classification_from_rollup(rollup_row) or "UNKNOWN")
    latest = _latest_evaluation(eval_row=eval_row, rollup_row=rollup_row, direct_row=direct_row, parsed=parsed)
    near_miss = _near_miss_analysis(sleeve_id=sleeve_id, eval_row=eval_row, rollup_row=rollup_row, direct_row=direct_row, parsed=parsed)
    recommendation = _recommendation(sleeve_id=sleeve_id, current_classification=current_classification, latest=latest, near_miss=near_miss, history=history)
    return {
        "sleeve_id": sleeve_id,
        "current_classification": current_classification,
        "intended_purpose": library["intended_purpose"],
        "current_trigger_conditions": library["current_trigger_conditions"],
        "latest_evaluation": latest,
        "near_miss_analysis": near_miss,
        "historical_sanity_check": dict(history),
        "recommendation": recommendation["classification"],
        "modification_review_warranted": recommendation["modification_review_warranted"],
        "condition_audit_conclusion": recommendation["conclusion"],
        "recommendation_reason": recommendation["reason"],
        "logic_changed": False,
        "thresholds_changed": False,
        "universe_changed": False,
    }


def _latest_evaluation(*, eval_row: Mapping[str, Any], rollup_row: Mapping[str, Any], direct_row: Mapping[str, Any], parsed: Mapping[str, Any]) -> dict[str, Any]:
    status = _first_text(rollup_row.get("status"), direct_row.get("status"), eval_row.get("status"), eval_row.get("evaluation_status")) or "UNKNOWN"
    blocker_reason = _first_text(eval_row.get("blocker_reason"), rollup_row.get("canonical_blocker"), "; ".join(_as_list(rollup_row.get("reason_codes"))))
    failed_conditions = list(parsed.get("conditions_failed") or [])
    passed_conditions = list(parsed.get("conditions_passed") or [])
    if str(blocker_reason).upper() == "NON_TRADING_DAY_NO_SLEEVE_RUN_EXPECTED":
        return {
            "did_required_inputs_exist": True,
            "did_sleeve_run": False,
            "run_status": "NOT_EXPECTED_NON_TRADING_DAY",
            "conditions_passed": ["Target day is not a trading session; sleeve run is not expected."],
            "conditions_failed": failed_conditions,
            "was_silence_correct": True,
            "silence_correct": True,
            "blocker_reason": blocker_reason,
            "evidence": parsed.get("evidence") or {},
        }
    did_run = status.upper() in {"NO_INTENT", "INTENT_CREATED", "FILTERED_OUT"} and not _has_runtime_failure(eval_row, rollup_row, direct_row)
    required_inputs_exist = _required_inputs_exist(eval_row, rollup_row, direct_row)
    if not failed_conditions and status.upper() == "NO_INTENT":
        failed_conditions.append("Producer declared no intent; no structured condition-level failure was emitted.")
    silence_correct = bool(did_run and required_inputs_exist and status.upper() == "NO_INTENT" and not _has_runtime_failure(eval_row, rollup_row, direct_row))
    return {
        "did_required_inputs_exist": required_inputs_exist,
        "did_sleeve_run": did_run,
        "run_status": status,
        "conditions_passed": passed_conditions,
        "conditions_failed": failed_conditions,
        "was_silence_correct": silence_correct,
        "silence_correct": silence_correct,
        "blocker_reason": blocker_reason,
        "evidence": parsed.get("evidence") or {},
    }


def _required_inputs_exist(eval_row: Mapping[str, Any], rollup_row: Mapping[str, Any], direct_row: Mapping[str, Any]) -> bool:
    missing = _as_list(eval_row.get("missing_input_artifacts")) + _as_list(rollup_row.get("missing_inputs")) + _as_list(direct_row.get("missing_inputs"))
    status = str(eval_row.get("market_data_status") or rollup_row.get("data_status") or "").upper()
    manifest = rollup_row.get("market_data_manifest_check") if isinstance(rollup_row.get("market_data_manifest_check"), Mapping) else {}
    manifest_status = str(manifest.get("status") or "").upper()
    if missing:
        return False
    if status in {"MISSING", "BLOCKED"}:
        return False
    if manifest_status and manifest_status != "PASS":
        return False
    return True


def _has_runtime_failure(*rows: Mapping[str, Any]) -> bool:
    joined = " ".join(str(row.get(key) or "") for row in rows for key in ("canonical_blocker", "blocker_reason", "status", "classification"))
    return any(token in joined.upper() for token in ("PRODUCER_NONZERO_RC", "ENGINE_INVOCATION_TIMEOUT", "SILENT_RUNTIME_FAILURE"))


def _parse_latest_conditions(*, sleeve_id: str, root: Path, day: str, eval_row: Mapping[str, Any], rollup_row: Mapping[str, Any], direct_row: Mapping[str, Any]) -> dict[str, Any]:
    if sleeve_id == "C2_DEFENSIVE_TAIL_V1":
        return _parse_defensive(root, day, rollup_row)
    stdout = _first_text(direct_row.get("stdout_summary"), rollup_row.get("stdout_summary"), eval_row.get("stdout_summary"))
    if sleeve_id == "C2_MARKET_NEUTRAL_SPREAD_V1":
        return _parse_market_neutral(stdout)
    if sleeve_id == "C2_VOL_INCOME_DEFINED_RISK_V1":
        return _parse_vol_income(stdout, rollup_row)
    if sleeve_id == "C2_EVENT_DISLOCATION_V1":
        return _parse_event_dislocation(stdout, rollup_row)
    if sleeve_id == "C2_MEAN_REVERSION_EQ_V1":
        return _parse_mean_reversion(stdout, rollup_row)
    return {"conditions_passed": [], "conditions_failed": [], "evidence": {}}


def _parse_defensive(root: Path, day: str, rollup_row: Mapping[str, Any]) -> dict[str, Any]:
    nav = _read_json(root / "truth_sleeves" / "PRIMARY" / "PAPER" / "accounting_v1" / "nav" / day / "nav_snapshot.v1.json")
    regime = _read_json(root / "truth_sleeves" / "PRIMARY" / "PAPER" / "monitoring_v1" / "regime_snapshot_v2" / day / "regime_snapshot.v2.json")
    stdout = _first_text(rollup_row.get("stdout_summary"))
    stdout_json = _first_json_object(stdout)
    drawdown = _first_number(nav.get("drawdown_pct"), (nav.get("history") or {}).get("drawdown_pct")) or 0.0
    regime_value = _first_text(regime.get("regime"), regime.get("regime_state"), regime.get("state"), stdout_json.get("regime"), "NORMAL")
    max_corr = _first_number(stdout_json.get("max_pairwise_corr"), rollup_row.get("max_pairwise_corr")) or 0.0
    failed = []
    passed = []
    if regime_value.upper() != "NORMAL":
        passed.append(f"regime != NORMAL ({regime_value})")
    else:
        failed.append("regime is NORMAL")
    if max_corr > 0.70:
        passed.append(f"max_pairwise_corr {max_corr:.4f} > 0.70")
    else:
        failed.append(f"max_pairwise_corr {max_corr:.4f} <= 0.70")
    if drawdown < -0.05:
        passed.append(f"drawdown_pct {drawdown:.4f} < -0.05")
    else:
        failed.append(f"drawdown_pct {drawdown:.4f} >= -0.05")
    return {
        "conditions_passed": passed,
        "conditions_failed": failed,
        "evidence": {"regime": regime_value, "max_pairwise_corr": max_corr, "drawdown_pct": drawdown},
    }


def _parse_market_neutral(stdout: str) -> dict[str, Any]:
    body = _first_json_object(stdout)
    rows = [row for row in body.get("evaluations", []) if isinstance(row, Mapping)]
    passed = []
    failed = []
    near = []
    z_enter = _first_number(body.get("z_enter")) or 2.0
    for row in rows:
        z = _first_number(row.get("z"))
        pair = str(row.get("pair") or "UNKNOWN")
        if z is None:
            continue
        distance = max(0.0, z_enter - abs(z))
        item = {"pair": pair, "z": z, "abs_z": abs(z), "threshold": z_enter, "distance_to_threshold": distance, "threshold_ratio": abs(z) / z_enter if z_enter else None}
        near.append(item)
        if abs(z) > z_enter:
            passed.append(f"{pair} abs(z) {abs(z):.4f} > {z_enter:.2f}")
        else:
            failed.append(f"{pair} abs(z) {abs(z):.4f} <= {z_enter:.2f}; distance {distance:.4f}")
    near.sort(key=lambda item: float(item.get("distance_to_threshold") or 999.0))
    return {"conditions_passed": passed, "conditions_failed": failed, "evidence": {"z_enter": z_enter, "pair_evaluations": rows, "computed_near_misses": near[:5]}}


def _parse_vol_income(stdout: str, rollup_row: Mapping[str, Any]) -> dict[str, Any]:
    records = _json_objects_from_stdout(stdout)
    requested = _requested_symbols(rollup_row)
    threshold = 0.75
    passed = []
    failed = []
    near = []
    for row in records:
        symbol = str(row.get("symbol") or "UNKNOWN")
        pct = _first_number(row.get("stdev_percentile"), row.get("pct_rank"))
        trend_ok = row.get("trend_ok")
        if pct is None:
            continue
        dist = max(0.0, threshold - pct)
        near.append({"symbol": symbol, "stdev_percentile": pct, "threshold": threshold, "distance_to_threshold": dist, "trend_ok": trend_ok})
        if pct >= threshold and trend_ok is True:
            passed.append(f"{symbol} percentile {pct:.4f} >= 0.75 and trend_ok true")
        elif pct < threshold:
            failed.append(f"{symbol} percentile {pct:.4f} < 0.75")
        elif trend_ok is not True:
            failed.append(f"{symbol} trend filter did not pass")
    near.sort(key=lambda item: float(item.get("distance_to_threshold") or 999.0))
    missing = sorted(set(requested) - {str(row.get("symbol") or "") for row in records}) if requested else []
    if missing:
        failed.append(f"Structured near-miss telemetry missing for requested symbols: {', '.join(missing[:10])}")
    return {"conditions_passed": passed, "conditions_failed": failed, "evidence": {"enter_percentile": threshold, "parsed_symbol_count": len(records), "requested_symbols": requested, "missing_parsed_symbols": missing, "computed_near_misses": near[:5]}}


def _parse_event_dislocation(stdout: str, rollup_row: Mapping[str, Any]) -> dict[str, Any]:
    records = _json_objects_from_stdout(stdout)
    requested = _requested_symbols(rollup_row)
    missing_bar = [str(row.get("symbol") or "UNKNOWN") for row in records if str(row.get("status") or "").upper() == "MISSING_BAR_FOR_DAY"]
    gap_rows = []
    failed = []
    passed = []
    for row in records:
        symbol = str(row.get("symbol") or "UNKNOWN")
        gap = _first_number(row.get("gap_pct"))
        range_pct = _first_number(row.get("range_pct"))
        if gap is None and range_pct is None:
            continue
        gap_dist = max(0.0, 0.02 - abs(gap or 0.0))
        range_dist = max(0.0, 0.03 - (range_pct or 0.0))
        gap_rows.append({"symbol": symbol, "gap_pct": gap, "range_pct": range_pct, "gap_distance": gap_dist, "range_distance": range_dist})
        if gap is not None and abs(gap) >= 0.02:
            passed.append(f"{symbol} abs(gap_pct) {abs(gap):.4f} >= 0.02")
        if range_pct is not None and range_pct >= 0.03:
            passed.append(f"{symbol} range_pct {range_pct:.4f} >= 0.03")
        if (gap is None or abs(gap) < 0.02) and (range_pct is None or range_pct < 0.03):
            failed.append(f"{symbol} did not reach gap/range thresholds")
    if missing_bar:
        failed.append(f"Missing target-day bars in parsed telemetry for {len(missing_bar)} symbols")
    missing_parsed = sorted(set(requested) - {str(row.get("symbol") or "") for row in records}) if requested else []
    if missing_parsed:
        failed.append("Producer stdout is truncated or lacks aggregate near-miss rows for the full requested universe")
    gap_rows.sort(key=lambda row: min(float(row.get("gap_distance") or 999.0), float(row.get("range_distance") or 999.0)))
    return {"conditions_passed": passed, "conditions_failed": failed, "evidence": {"gap_abs_enter": 0.02, "range_enter": 0.03, "parsed_symbol_count": len(records), "requested_symbols": requested, "missing_parsed_symbols": missing_parsed[:25], "missing_bar_count": len(missing_bar), "computed_near_misses": gap_rows[:5]}}


def _parse_mean_reversion(stdout: str, rollup_row: Mapping[str, Any]) -> dict[str, Any]:
    records = _json_objects_from_stdout(stdout)
    requested = _requested_symbols(rollup_row)
    threshold = -2.0
    passed = []
    failed = []
    near = []
    for row in records:
        symbol = str(row.get("symbol") or "UNKNOWN")
        z = _first_number(row.get("z"))
        if z is None:
            continue
        distance = max(0.0, z - threshold)
        near.append({"symbol": symbol, "z": z, "threshold": threshold, "distance_to_threshold": distance})
        if z <= threshold:
            passed.append(f"{symbol} z {z:.4f} <= -2.0")
        else:
            failed.append(f"{symbol} z {z:.4f} > -2.0")
    missing = sorted(set(requested) - {str(row.get("symbol") or "") for row in records}) if requested else []
    if missing:
        failed.append("Producer stdout is truncated or lacks aggregate near-miss rows for the full requested universe")
    near.sort(key=lambda row: float(row.get("distance_to_threshold") or 999.0))
    return {"conditions_passed": passed, "conditions_failed": failed, "evidence": {"z_enter": threshold, "parsed_symbol_count": len(records), "requested_symbols": requested, "missing_parsed_symbols": missing[:25], "computed_near_misses": near[:5]}}


def _near_miss_analysis(*, sleeve_id: str, eval_row: Mapping[str, Any], rollup_row: Mapping[str, Any], direct_row: Mapping[str, Any], parsed: Mapping[str, Any]) -> dict[str, Any]:
    telemetry = direct_row.get("nearest_miss_telemetry") if isinstance(direct_row.get("nearest_miss_telemetry"), Mapping) else rollup_row.get("nearest_miss_telemetry") if isinstance(rollup_row.get("nearest_miss_telemetry"), Mapping) else {}
    if telemetry and str(telemetry.get("status") or "").upper() != "NOT_APPLICABLE":
        top = [dict(row) for row in telemetry.get("top_nearest_misses") or [] if isinstance(row, Mapping)]
        missing = [str(item) for item in telemetry.get("missing_telemetry_symbols") or [] if str(item).strip()]
        complete = str(telemetry.get("status") or "").upper() == "COMPLETE" and not missing
        return {
            "nearest_miss_data_available": complete,
            "nearest_miss_count": int(telemetry.get("nearest_miss_count") or len(top)),
            "top_nearest_misses": top[:5],
            "missing_nearest_miss_telemetry": None if complete else "Nearest-miss telemetry is present but incomplete for some requested symbols.",
            "producer_must_emit": None if complete else "Ensure the sleeve runner emits parseable per-symbol telemetry for every requested symbol.",
            "telemetry_status": str(telemetry.get("status") or ""),
            "requested_symbol_count": int(telemetry.get("requested_symbol_count") or 0),
            "evaluated_symbol_count": int(telemetry.get("evaluated_symbol_count") or 0),
            "blocked_symbol_count": int(telemetry.get("blocked_symbol_count") or 0),
            "missing_telemetry_symbols": missing,
        }
    computed = list((parsed.get("evidence") or {}).get("computed_near_misses") or [])
    requested = list((parsed.get("evidence") or {}).get("requested_symbols") or [])
    missing_parsed = list((parsed.get("evidence") or {}).get("missing_parsed_symbols") or [])
    eval_nearest = _as_list(eval_row.get("top_nearest_misses"))
    if computed:
        available = not missing_parsed
        reason = "Computed from structured producer stdout."
        if missing_parsed:
            reason = "Partial only: producer stdout is truncated or lacks aggregate near-miss rows for the full universe."
        return {
            "nearest_miss_data_available": available,
            "nearest_miss_count": len(computed),
            "top_nearest_misses": computed[:5],
            "missing_nearest_miss_telemetry": None if available else reason,
            "producer_must_emit": None if available else "Emit a full structured nearest_miss summary artifact or untruncated aggregate row for every requested symbol/pair.",
        }
    if eval_nearest and eval_nearest[0] != {"status": "NEAREST_MISS_DATA_MISSING", "detail": "No rejected-intent or nearest-miss rows were present in source artifacts."}:
        return {
            "nearest_miss_data_available": True,
            "nearest_miss_count": int(eval_row.get("nearest_miss_count") or len(eval_nearest)),
            "top_nearest_misses": eval_nearest[:5],
            "missing_nearest_miss_telemetry": None,
            "producer_must_emit": None,
        }
    if sleeve_id == "C2_DEFENSIVE_TAIL_V1":
        return {
            "nearest_miss_data_available": True,
            "nearest_miss_count": 3,
            "top_nearest_misses": [
                {"condition": item} for item in (parsed.get("conditions_failed") or [])[:5]
            ],
            "missing_nearest_miss_telemetry": None,
            "producer_must_emit": None,
        }
    return {
        "nearest_miss_data_available": False,
        "nearest_miss_count": 0,
        "top_nearest_misses": [],
        "missing_nearest_miss_telemetry": "No complete structured nearest-miss rows were present in sleeve evaluation or kernel stdout.",
        "producer_must_emit": "The sleeve producer or kernel wrapper must emit full per-symbol/pair nearest-miss telemetry, not only truncated stdout.",
        "requested_symbol_count": len(requested),
    }


def _recommendation(*, sleeve_id: str, current_classification: str, latest: Mapping[str, Any], near_miss: Mapping[str, Any], history: Mapping[str, Any]) -> dict[str, Any]:
    if _first_text(latest.get("blocker_reason")).upper() == "NON_TRADING_DAY_NO_SLEEVE_RUN_EXPECTED":
        return _rec("KEEP_AS_IS", False, "Sleeve silence is correct for a non-trading day.", "No sleeve run is expected because the target day is not a trading session; no threshold, universe, or logic review is warranted.")
    if current_classification == "SILENT_RUNTIME_FAILURE" or _first_text(latest.get("blocker_reason")).upper() in {"PRODUCER_NONZERO_RC", "ENGINE_INVOCATION_TIMEOUT"}:
        return _rec("IMPLEMENTATION_REVIEW_REQUIRED", True, "Runtime failure must be repaired before judging trigger logic.", "Sleeve silence is caused by a runtime failure, not validated market conditions.")
    if current_classification == "SILENT_DATA_BLOCKED" or not latest.get("did_required_inputs_exist"):
        return _rec("REVIEW_DATA_INPUTS", True, "Required data inputs are missing or blocked.", "Repair data/input binding before reviewing thresholds or universe.")
    if sleeve_id == "C2_MARKET_NEUTRAL_SPREAD_V1":
        top = (near_miss.get("top_nearest_misses") or [{}])[0]
        ratio = _first_number(top.get("threshold_ratio"))
        if ratio is not None and ratio >= 0.95:
            return _rec("REVIEW_THRESHOLD", True, "Nearest pair is within 5% of the configured z-score threshold.", f"{top.get('pair')} reached {ratio:.1%} of the trigger threshold; this supports threshold review, not a direct logic change.")
    if not near_miss.get("nearest_miss_data_available"):
        return _rec("NEEDS_NEAR_MISS_TELEMETRY", False, "Silence is explained only at run level; full nearest-miss data is missing.", str(near_miss.get("missing_nearest_miss_telemetry") or "Nearest-miss telemetry unavailable."))
    if int(history.get("last_60_trading_days_trigger_count") or 0) == 0 and int(history.get("days_with_observations_60") or 0) >= 20:
        return _rec("NEEDS_MORE_HISTORY", False, "No recent triggers were found in available history.", "Keep logic unchanged, but continue monitoring trigger frequency before judging thresholds.")
    return _rec("KEEP_AS_IS", False, "Current evidence supports correct silence.", "Required inputs existed, the sleeve ran, and explicit trigger conditions did not pass.")


def _rec(classification: str, warranted: bool, conclusion: str, reason: str) -> dict[str, Any]:
    return {"classification": classification, "modification_review_warranted": warranted, "conclusion": conclusion, "reason": reason}


def _historical_sanity(root: Path, day: str, sleeve_ids: list[str]) -> dict[str, dict[str, Any]]:
    dates = sorted(p.name for p in (root / "reports" / "sleeve_evaluation_kernel_v1").glob("20??-??-??") if p.name <= day)
    windows = {"30": dates[-30:], "60": dates[-60:]}
    out: dict[str, dict[str, Any]] = {}
    for sleeve_id in sleeve_ids:
        row: dict[str, Any] = {}
        for label, window in windows.items():
            stats = _history_stats(root, window, sleeve_id)
            row[f"last_{label}_trading_days_trigger_count"] = stats["trigger_count"]
            row[f"last_{label}_trading_days_blocked_by_missing_data"] = stats["blocked_by_missing_data"]
            row[f"last_{label}_trading_days_correctly_silent"] = stats["correctly_silent"]
            row[f"last_{label}_trading_days_runtime_errors"] = stats["runtime_errors"]
            row[f"days_with_observations_{label}"] = stats["observations"]
        row["last_candidate_date"] = _last_candidate_date(root, day, sleeve_id)
        out[sleeve_id] = row
    return out


def _history_stats(root: Path, dates: list[str], sleeve_id: str) -> dict[str, int]:
    stats = Counter()
    for path_day in dates:
        rollup = _read_json(root / "reports" / "sleeve_evaluation_kernel_v1" / path_day / "sleeve_evaluation_rollup.v1.json")
        rows = {str(row.get("sleeve_id") or ""): row for row in _rows(rollup, "outcomes")}
        row = rows.get(sleeve_id)
        if not row:
            direct = _read_json(root / "reports" / "sleeve_evaluation_kernel_v1" / path_day / sleeve_id / "sleeve_evaluation.v1.json")
            row = direct if direct else None
        if not row:
            continue
        stats["observations"] += 1
        status = str(row.get("status") or "").upper()
        blocker = str(row.get("canonical_blocker") or "").upper()
        output_count = int(_first_number(row.get("output_intents"), row.get("output_count"), 0) or 0)
        if output_count > 0 or status in {"INTENT_CREATED", "OUTPUT_PRODUCED"}:
            stats["trigger_count"] += 1
        elif blocker in {"MISSING_REQUIRED_INPUTS", "SLEEVE_INPUT_REQUIREMENT_BLOCKED"} or str(row.get("data_status") or "").upper() in {"MISSING", "BLOCKED"}:
            stats["blocked_by_missing_data"] += 1
        elif blocker in {"PRODUCER_NONZERO_RC", "ENGINE_INVOCATION_TIMEOUT"} or status in {"ERROR", "FAILED"}:
            stats["runtime_errors"] += 1
        elif status in {"NO_INTENT", "FILTERED_OUT"}:
            stats["correctly_silent"] += 1
    return {key: int(stats.get(key, 0)) for key in ("observations", "trigger_count", "blocked_by_missing_data", "correctly_silent", "runtime_errors")}


def _last_candidate_date(root: Path, day: str, sleeve_id: str) -> str | None:
    base = root / "reports" / "aegis_candidate_contracts_v1"
    last = None
    for path in sorted(base.glob("*/candidate_contracts.v1.json")):
        path_day = path.parent.name
        if path_day > day:
            continue
        payload = _read_json(path)
        if any(str(row.get("sleeve_id") or "") == sleeve_id for row in _rows(payload, "candidate_contracts", "contracts", "candidates")):
            last = path_day
    return last


def _classification_from_rollup(row: Mapping[str, Any]) -> str:
    status = str(row.get("status") or "").upper()
    blocker = str(row.get("canonical_blocker") or "").upper()
    if blocker in {"PRODUCER_NONZERO_RC", "ENGINE_INVOCATION_TIMEOUT"}:
        return "SILENT_RUNTIME_FAILURE"
    if blocker in {"MISSING_REQUIRED_INPUTS", "SLEEVE_INPUT_REQUIREMENT_BLOCKED"} or str(row.get("data_status") or "").upper() in {"MISSING", "BLOCKED"}:
        return "SILENT_DATA_BLOCKED"
    if status == "NO_INTENT":
        return "SILENT_MARKET_CONDITION_NOT_MET"
    return status


def _requested_symbols(row: Mapping[str, Any]) -> list[str]:
    manifest = row.get("market_data_manifest_check") if isinstance(row.get("market_data_manifest_check"), Mapping) else {}
    values = _as_list(row.get("producer_requested_symbols")) or _as_list(manifest.get("requested_symbols")) or _as_list(row.get("requested_symbols"))
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _json_objects_from_stdout(text: str) -> list[dict[str, Any]]:
    rows = []
    for line in str(text or "").splitlines():
        obj = _first_json_object(line)
        if obj:
            rows.append(obj)
    if not rows:
        obj = _first_json_object(text)
        if obj:
            rows.append(obj)
    return rows


def _first_json_object(text: str) -> dict[str, Any]:
    text = str(text or "")
    start = text.find("{")
    if start < 0:
        return {}
    candidate = text[start:]
    for end in range(len(candidate), 0, -1):
        chunk = candidate[:end].strip()
        if not chunk.endswith("}"):
            continue
        try:
            value = json.loads(chunk)
        except Exception:
            continue
        return value if isinstance(value, dict) else {}
    match = re.search(r"\{.*\}", text)
    if match:
        try:
            value = json.loads(match.group(0))
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}
    return {}


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "aegis_sleeve_evaluation_v1": sleeve_evaluation_path_v1(truth_root=root, day_utc=day),
        "sleeve_evaluation_kernel_v1": root / "reports" / "sleeve_evaluation_kernel_v1" / day / "sleeve_evaluation_rollup.v1.json",
        "candidate_generation_diagnostics_v1": root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value in (None, "", "n/a") or isinstance(value, bool):
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return None


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
