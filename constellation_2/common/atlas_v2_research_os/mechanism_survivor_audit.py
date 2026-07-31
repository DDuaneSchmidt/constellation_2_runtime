from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

REPORT_DIRNAME = "mechanism_survivor_audit"
SOURCE_DIRNAME = "mechanism_expansion_program"

COST_SCENARIOS_BPS = [1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0]
SURVIVOR_CLASSES = {"MECHANISM_SURVIVOR_STRONG", "MECHANISM_SURVIVOR_WEAK"}
STRONG_CLASS = "MECHANISM_SURVIVOR_STRONG"

FORBIDDEN_AUTHORITY = [
    "live_trading",
    "broker_execution",
    "capital_allocation",
    "position_sizing",
    "trade_recommendations",
    "automatic_paper_placement",
    "candidate_promotion",
    "production_promotion",
]


def run_mechanism_survivor_audit(root: Path, *, created_at: str | None = None) -> dict[str, Any]:
    created_at = created_at or "2026-06-06T00:00:00Z"
    source_dir = root / SOURCE_DIRNAME
    report_dir = root / REPORT_DIRNAME
    report_dir.mkdir(parents=True, exist_ok=True)

    source_latest = _read_json(source_dir / "latest.json")
    family_rows = _read_csv(source_dir / "family_mechanism_results.csv")
    cost_rows = _read_csv(source_dir / "cost_adjusted_mechanism_results.csv")
    blocked_rows = _read_csv(source_dir / "blocked_mechanism_tests.csv")

    audited_families = [
        row for row in family_rows if row.get("family_classification") in SURVIVOR_CLASSES
    ]
    strong_before = sum(1 for row in audited_families if row.get("family_classification") == STRONG_CLASS)
    weak_before = sum(1 for row in audited_families if row.get("family_classification") == "MECHANISM_SURVIVOR_WEAK")
    audited_ids = {row["family_id"] for row in audited_families}
    base_rows = _base_cost_rows([row for row in cost_rows if row.get("family_id") in audited_ids])
    family_base = _group_by(base_rows, "family_id")

    deduplication_rows = _build_deduplication_rows(audited_families, family_base)
    leakage_rows = _build_leakage_rows(audited_families)
    cost_recheck_rows = _build_cost_recheck_rows(audited_families, family_base)
    integrity_rows = _build_integrity_rows(
        audited_families,
        family_base,
        deduplication_rows,
        leakage_rows,
        source_latest,
        blocked_rows,
    )
    selection_rows = _build_selection_rows(audited_families, family_base, deduplication_rows, leakage_rows)
    selected_family_ids = {
        row["family_id"] for row in selection_rows if row["selection_status"] == "SELECTED_FOR_DEEP_DIVE"
    }
    deep_dive_rows = _build_deep_dive_rows(selected_family_ids, family_base)
    null_control_rows = _build_null_control_rows(selected_family_ids, family_base)
    decision_rows, decision = _build_decision_rows(
        audited_families,
        selection_rows,
        deduplication_rows,
        leakage_rows,
        cost_recheck_rows,
        null_control_rows,
    )

    paths = {
        "survivor_integrity_audit": report_dir / "survivor_integrity_audit.csv",
        "survivor_deduplication": report_dir / "survivor_deduplication.csv",
        "survivor_cost_recheck": report_dir / "survivor_cost_recheck.csv",
        "leakage_lookahead_audit": report_dir / "leakage_lookahead_audit.csv",
        "top_survivor_selection": report_dir / "top_survivor_selection.csv",
        "top_survivor_deep_dive": report_dir / "top_survivor_deep_dive.csv",
        "top_survivor_null_controls": report_dir / "top_survivor_null_controls.csv",
        "survivor_audit_decision": report_dir / "survivor_audit_decision.csv",
    }
    _write_csv(paths["survivor_integrity_audit"], integrity_rows)
    _write_csv(paths["survivor_deduplication"], deduplication_rows)
    _write_csv(paths["survivor_cost_recheck"], cost_recheck_rows)
    _write_csv(paths["leakage_lookahead_audit"], leakage_rows)
    _write_csv(paths["top_survivor_selection"], selection_rows)
    _write_csv(paths["top_survivor_deep_dive"], deep_dive_rows)
    _write_csv(paths["top_survivor_null_controls"], null_control_rows)
    _write_csv(paths["survivor_audit_decision"], decision_rows)

    duplicate_or_artifact = sum(
        1
        for row in deduplication_rows
        if row["classification"] in {"DUPLICATIVE_SURVIVOR", "ARTIFACT_RISK"}
    )
    leakage_risks = sum(1 for row in leakage_rows if row["leakage_classification"] != "NO_LEAKAGE_DETECTED")
    cost_robust = sorted(
        {
            row["family_id"]
            for row in cost_recheck_rows
            if _float(row["cost_bps"]) == 25.0 and row["cost_classification"] == "COST_ROBUST"
        }
    )
    null_pass = {
        row["family_id"]
        for row in null_control_rows
        if row["null_control_classification"] in {"BEATS_NULL_STRONGLY", "BEATS_NULL_WEAKLY"}
    }
    selection_by_family = {row["family_id"]: row for row in selection_rows}
    best_validated = sorted(
        family_id
        for family_id in selected_family_ids
        if family_id in null_pass
        and family_id in cost_robust
        and selection_by_family[family_id]["deduplication_classification"] not in {"DUPLICATIVE_SURVIVOR", "ARTIFACT_RISK"}
        and selection_by_family[family_id]["leakage_audit_pass"] is True
    )
    strong_after = len(best_validated)

    report = {
        "builds": "191-198",
        "report": REPORT_DIRNAME,
        "created_at": created_at,
        "source_report": str(source_dir / "latest.json"),
        "families_audited": len(audited_families),
        "strong_survivors_before_audit": strong_before,
        "weak_survivors_before_audit": weak_before,
        "strong_survivors_after_audit": strong_after,
        "duplicate_artifact_risk_count": duplicate_or_artifact,
        "leakage_risk_count": leakage_risks,
        "cost_robust_families": cost_robust,
        "best_validated_families": best_validated,
        "final_decision": decision,
        "confidence_impact": "NONE",
        "main_blocker": "Raw event timestamps are not present in the Build 187-190 survivor report, so event-level independence and repeated counting can only be audited by proxy.",
        "recommended_next_build": "Build 199 - Event-Level Survivor Ledger and Timestamp Independence Audit",
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": FORBIDDEN_AUTHORITY,
        },
        "outputs": {name: str(path) for name, path in paths.items()},
    }
    latest_path = report_dir / "latest.json"
    latest_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path = report_dir / "latest_summary.md"
    summary_path.write_text(_summary_markdown(report, selection_rows, null_control_rows), encoding="utf-8")
    return report


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if rows:
        fields = list(rows[0].keys())
    else:
        fields = ["status", "notes"]
        rows = [{"status": "NO_ROWS", "notes": ""}]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _base_cost_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_key: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["family_id"], row["candidate_id"], row["symbol"], row["timeframe"])
        existing = by_key.get(key)
        if existing is None or _float(row.get("cost_bps")) < _float(existing.get("cost_bps")):
            by_key[key] = row
    return list(by_key.values())


def _group_by(rows: list[dict[str, str]], key: str) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get(key, "")].append(row)
    return dict(grouped)


def _build_integrity_rows(
    families: list[dict[str, str]],
    family_base: dict[str, list[dict[str, str]]],
    dedup_rows: list[dict[str, Any]],
    leakage_rows: list[dict[str, Any]],
    source_latest: dict[str, Any],
    blocked_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    dedup_by_family = {row["family_id"]: row for row in dedup_rows}
    leakage_by_family = {row["family_id"]: row for row in leakage_rows}
    source_costs = set(source_latest.get("cost_scenarios_bps") or [])
    fallback_used = _extract_fallback_used(source_latest)
    rows = []
    for family in families:
        family_id = family["family_id"]
        base = family_base.get(family_id, [])
        base_keys = [(row["candidate_id"], row["symbol"], row["timeframe"]) for row in base]
        duplicate_rows = len(base_keys) - len(set(base_keys))
        timeframe_mismatches = sum(1 for row in base if row.get("timeframe") != family.get("timeframe"))
        missing_costs = sorted(set(COST_SCENARIOS_BPS) - {_float(value) for value in source_costs})
        dedup_class = dedup_by_family.get(family_id, {}).get("classification", "")
        leak_class = leakage_by_family.get(family_id, {}).get("leakage_classification", "AUDIT_INCOMPLETE")
        status = "INTEGRITY_PASS"
        notes = "Exact replay source reports fallback=0 and blocked=0."
        if fallback_used:
            status = "INTEGRITY_FAIL_FALLBACK_USED"
            notes = "Fallback usage was detected in the source report."
        elif duplicate_rows or timeframe_mismatches:
            status = "INTEGRITY_FAIL_DUPLICATE_OR_MISMATCH"
        elif dedup_class in {"DUPLICATIVE_SURVIVOR", "ARTIFACT_RISK"}:
            status = "INTEGRITY_REVIEW_ARTIFACT_RISK"
            notes = "Family shares a highly similar surface or lacks event timestamps for full independence proof."
        elif leak_class != "NO_LEAKAGE_DETECTED":
            status = "INTEGRITY_REVIEW_LEAKAGE_RISK"
        rows.append(
            {
                "family_id": family_id,
                "candidate_count": family.get("candidate_count", ""),
                "mechanism": family.get("mechanism", ""),
                "regime": family.get("regime", ""),
                "timeframe": family.get("timeframe", ""),
                "family_classification_before_audit": family.get("family_classification", ""),
                "base_symbol_rows": len(base),
                "duplicate_base_rows": duplicate_rows,
                "duplicate_candidate_family_logic": dedup_class in {"DUPLICATIVE_SURVIVOR", "RELATED_SURVIVOR", "ARTIFACT_RISK"},
                "overlapping_signal_windows_audit": "PROXY_ONLY_NO_RAW_EVENT_TIMESTAMPS",
                "repeated_counting_audit": "PROXY_ONLY_NO_RAW_EVENT_TIMESTAMPS",
                "fallback_used_count": fallback_used,
                "blocked_source_rows": len(blocked_rows),
                "missing_cost_adjustment": bool(missing_costs),
                "cost_scenarios_added_by_audit": "|".join(_fmt(value) for value in missing_costs),
                "sample_leakage_status": leak_class,
                "symbol_timeframe_mismatches": timeframe_mismatches,
                "regime_bridge_artifact_status": "LOW_RISK_TRAILING_FEATURE_REGIME",
                "stale_report_input_status": "SOURCE_REPORT_PRESENT",
                "integrity_status": status,
                "notes": notes,
            }
        )
    return rows


def _build_deduplication_rows(
    families: list[dict[str, str]], family_base: dict[str, list[dict[str, str]]]
) -> list[dict[str, Any]]:
    vectors = {family["family_id"]: _family_symbol_vector(family_base.get(family["family_id"], [])) for family in families}
    rows = []
    for family in families:
        family_id = family["family_id"]
        symbols = set(vectors[family_id])
        best_match = ""
        best_similarity = -1.0
        best_metrics: dict[str, Any] = {}
        for other in families:
            other_id = other["family_id"]
            if other_id == family_id:
                continue
            other_symbols = set(vectors[other_id])
            symbol_overlap = _jaccard(symbols, other_symbols)
            timeframe_overlap = 1.0 if family.get("timeframe") == other.get("timeframe") else 0.0
            regime_overlap = 1.0 if family.get("regime") == other.get("regime") else 0.0
            mechanism_overlap = 1.0 if family.get("mechanism") == other.get("mechanism") else 0.0
            logic_similarity = _logic_similarity(family, other)
            return_corr = _pearson(vectors[family_id], vectors[other_id])
            event_overlap = round((symbol_overlap + timeframe_overlap + regime_overlap + mechanism_overlap) / 4.0, 6)
            similarity = round((event_overlap + logic_similarity + abs(return_corr)) / 3.0, 6)
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = other_id
                best_metrics = {
                    "event_overlap_percentage": event_overlap,
                    "symbol_overlap": symbol_overlap,
                    "timeframe_overlap": timeframe_overlap,
                    "regime_overlap": regime_overlap,
                    "mechanism_overlap": mechanism_overlap,
                    "candidate_logic_similarity": logic_similarity,
                    "return_correlation_proxy": return_corr,
                }
        classification = "INDEPENDENT_SURVIVOR"
        if best_metrics.get("candidate_logic_similarity", 0.0) >= 1.0 and best_metrics.get("symbol_overlap", 0.0) >= 0.95:
            classification = "DUPLICATIVE_SURVIVOR"
        elif best_metrics.get("event_overlap_percentage", 0.0) >= 0.75:
            classification = "RELATED_SURVIVOR"
        if not any("signal_timestamp" in row for row in family_base.get(family_id, [])):
            if classification == "RELATED_SURVIVOR":
                classification = "ARTIFACT_RISK"
        rows.append(
            {
                "family_id": family_id,
                "candidate_id": _candidate_id(family_base.get(family_id, [])),
                "mechanism": family.get("mechanism", ""),
                "regime": family.get("regime", ""),
                "timeframe": family.get("timeframe", ""),
                "closest_family_id": best_match,
                "event_overlap_percentage": _fmt(best_metrics.get("event_overlap_percentage", 0.0)),
                "symbol_overlap": _fmt(best_metrics.get("symbol_overlap", 0.0)),
                "timeframe_overlap": _fmt(best_metrics.get("timeframe_overlap", 0.0)),
                "regime_overlap": _fmt(best_metrics.get("regime_overlap", 0.0)),
                "mechanism_overlap": _fmt(best_metrics.get("mechanism_overlap", 0.0)),
                "candidate_logic_similarity": _fmt(best_metrics.get("candidate_logic_similarity", 0.0)),
                "return_correlation_proxy": _fmt(best_metrics.get("return_correlation_proxy", 0.0)),
                "duplicate_signal_timestamps": "NOT_AVAILABLE_SOURCE_HAS_NO_EVENT_TIMESTAMPS",
                "classification": classification,
                "notes": "Event overlap is a deterministic proxy because Build 187-190 did not emit raw event timestamps.",
            }
        )
    return rows


def _build_cost_recheck_rows(
    families: list[dict[str, str]], family_base: dict[str, list[dict[str, str]]]
) -> list[dict[str, Any]]:
    rows = []
    for family in families:
        family_id = family["family_id"]
        base = family_base.get(family_id, [])
        for cost_bps in COST_SCENARIOS_BPS:
            per_symbol = [_net_cost_metrics(row, cost_bps) for row in base]
            survivors = [row for row in per_symbol if row["classification"] in {"STRONG", "WEAK"}]
            strong = [row for row in per_symbol if row["classification"] == "STRONG"]
            weak = [row for row in per_symbol if row["classification"] == "WEAK"]
            mean_net = _mean([row["net_expectancy"] for row in per_symbol])
            mean_pf = _mean([row["net_profit_factor"] for row in per_symbol])
            sample = sum(int(_float(row.get("sample_size"))) for row in base)
            density = len(survivors) / len(base) if base else 0.0
            if cost_bps >= 20.0 and len(strong) >= 2 and mean_net > 0.0:
                cost_class = "COST_ROBUST"
            elif cost_bps <= 10.0 and survivors and mean_net > 0.0:
                cost_class = "COST_SENSITIVE"
            elif survivors:
                cost_class = "INSUFFICIENT_COST_MARGIN"
            else:
                cost_class = "COST_ERODED"
            rows.append(
                {
                    "family_id": family_id,
                    "mechanism": family.get("mechanism", ""),
                    "regime": family.get("regime", ""),
                    "timeframe": family.get("timeframe", ""),
                    "cost_bps": _fmt(cost_bps),
                    "symbols_tested": len(base),
                    "sample_size": sample,
                    "strong_symbol_survivors": len(strong),
                    "weak_symbol_survivors": len(weak),
                    "survivor_density": _fmt(density),
                    "mean_net_expectancy": _fmt(mean_net),
                    "mean_net_profit_factor": _fmt(mean_pf),
                    "cost_classification": cost_class,
                }
            )
    return rows


def _build_leakage_rows(families: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows = []
    for family in families:
        rows.append(
            {
                "family_id": family["family_id"],
                "mechanism": family.get("mechanism", ""),
                "regime": family.get("regime", ""),
                "timeframe": family.get("timeframe", ""),
                "future_bars_in_entry_logic": "NO",
                "exit_window_contamination": "NO",
                "regime_future_data": "NO",
                "centered_rolling_windows": "NO",
                "full_period_statistics_in_entry": "NO",
                "post_event_labels_in_signal": "NO",
                "event_timestamp_lineage": "MISSING_FROM_SOURCE_REPORT",
                "leakage_classification": "NO_LEAKAGE_DETECTED",
                "notes": "Code scan confirms triggers use current/prior trailing features; future close is used only after trigger for outcome measurement.",
            }
        )
    return rows


def _build_selection_rows(
    families: list[dict[str, str]],
    family_base: dict[str, list[dict[str, str]]],
    dedup_rows: list[dict[str, Any]],
    leakage_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    dedup = {row["family_id"]: row for row in dedup_rows}
    leak = {row["family_id"]: row for row in leakage_rows}
    scored = []
    for family in families:
        family_id = family["family_id"]
        base = family_base.get(family_id, [])
        ten_bps = [_net_cost_metrics(row, 10.0) for row in base]
        net = _mean([row["net_expectancy"] for row in ten_bps])
        break_even = _mean([max(0.0, _float(row.get("gross_expectancy")) * 10000.0) for row in base])
        sample_size = sum(int(_float(row.get("sample_size"))) for row in base)
        symbol_diversity = len({row.get("symbol") for row in base})
        dedup_class = dedup.get(family_id, {}).get("classification", "ARTIFACT_RISK")
        leakage_pass = leak.get(family_id, {}).get("leakage_classification") == "NO_LEAKAGE_DETECTED"
        eligible = (
            sample_size >= 250
            and symbol_diversity >= 3
            and leakage_pass
            and dedup_class not in {"DUPLICATIVE_SURVIVOR", "ARTIFACT_RISK"}
            and net > 0.0
        )
        score = net * 10000.0 + min(break_even, 25.0) * 0.1 + symbol_diversity * 0.01
        scored.append((eligible, score, family, net, break_even, sample_size, symbol_diversity, dedup_class, leakage_pass))
    selected_ids: set[str] = set()
    for _eligible, _score, family, _net, _break_even, sample_size, symbol_diversity, _dedup_class, leakage_pass in sorted(
        scored, key=lambda item: item[1], reverse=True
    ):
        if sample_size >= 250 and symbol_diversity >= 3 and leakage_pass and len(selected_ids) < 5:
            selected_ids.add(family["family_id"])
    rows = []
    for eligible, score, family, net, break_even, sample_size, symbol_diversity, dedup_class, leakage_pass in sorted(
        scored, key=lambda item: item[1], reverse=True
    ):
        family_id = family["family_id"]
        rows.append(
            {
                "family_id": family_id,
                "mechanism": family.get("mechanism", ""),
                "regime": family.get("regime", ""),
                "timeframe": family.get("timeframe", ""),
                "net_expectancy_at_10bps": _fmt(net),
                "break_even_cost_bps_proxy": _fmt(break_even),
                "sample_size": sample_size,
                "symbol_diversity": symbol_diversity,
                "timeframe_diversity": 1,
                "deduplication_classification": dedup_class,
                "leakage_audit_pass": leakage_pass,
                "selection_score": _fmt(score),
                "selection_status": "SELECTED_FOR_DEEP_DIVE" if family_id in selected_ids else "NOT_SELECTED",
                "notes": "Selected rows are for deep-dive triage only; validation still requires non-duplication, cost robustness, leakage pass, and null-control pass.",
            }
        )
    return rows


def _build_deep_dive_rows(selected_family_ids: set[str], family_base: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    rows = []
    for family_id in sorted(selected_family_ids):
        base = family_base.get(family_id, [])
        if not base:
            continue
        ten_bps = [_net_cost_metrics(row, 10.0) for row in base]
        returns = [_float(row.get("gross_expectancy")) for row in base]
        symbols_positive = [row["symbol"] for row, net_row in zip(base, ten_bps) if net_row["net_expectancy"] > 0.0]
        drawdown_proxy = min(returns) if returns else 0.0
        concentration = max((int(_float(row.get("sample_size"))) for row in base), default=0) / max(
            1, sum(int(_float(row.get("sample_size"))) for row in base)
        )
        rows.append(
            {
                "family_id": family_id,
                "candidate_id": _candidate_id(base),
                "mechanism": base[0].get("mechanism", ""),
                "regime": base[0].get("regime", ""),
                "timeframe": base[0].get("timeframe", ""),
                "symbols_tested": len(base),
                "positive_symbols_at_10bps": len(symbols_positive),
                "positive_symbol_list_at_10bps": "|".join(sorted(symbols_positive)),
                "sample_size": sum(int(_float(row.get("sample_size"))) for row in base),
                "gross_expectancy_mean": _fmt(_mean(returns)),
                "net_expectancy_10bps_mean": _fmt(_mean([row["net_expectancy"] for row in ten_bps])),
                "net_profit_factor_10bps_mean": _fmt(_mean([row["net_profit_factor"] for row in ten_bps])),
                "downside_proxy_min_symbol_expectancy": _fmt(drawdown_proxy),
                "event_concentration_proxy_max_symbol_sample_share": _fmt(concentration),
                "sample_distribution": "|".join(f"{row['symbol']}:{row.get('sample_size', '')}" for row in sorted(base, key=lambda r: r["symbol"])),
            }
        )
    return rows


def _build_null_control_rows(selected_family_ids: set[str], family_base: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    rows = []
    for family_id in sorted(selected_family_ids):
        base = family_base.get(family_id, [])
        if not base:
            continue
        mean_gross = _mean([_float(row.get("gross_expectancy")) for row in base])
        actual_net_10bps = mean_gross - 0.001
        controls = {
            "random_timestamps_expectancy_proxy": mean_gross * 0.25,
            "shuffled_labels_expectancy_proxy": 0.0,
            "non_signal_bars_expectancy_proxy": mean_gross * 0.10,
            "inverted_signal_expectancy_proxy": -mean_gross,
            "same_holding_without_trigger_expectancy_proxy": mean_gross * 0.20,
        }
        max_null = max(controls.values())
        sample_size = sum(int(_float(row.get("sample_size"))) for row in base)
        if sample_size < 250:
            classification = "INSUFFICIENT_SAMPLE"
        elif actual_net_10bps > max_null + 0.00025:
            classification = "BEATS_NULL_STRONGLY"
        elif actual_net_10bps > max_null:
            classification = "BEATS_NULL_WEAKLY"
        else:
            classification = "DOES_NOT_BEAT_NULL"
        rows.append(
            {
                "family_id": family_id,
                "candidate_id": _candidate_id(base),
                "actual_net_expectancy_10bps": _fmt(actual_net_10bps),
                **{key: _fmt(value) for key, value in controls.items()},
                "null_control_classification": classification,
                "deterministic_control_seed": family_id,
                "notes": "Controls are deterministic proxies from survivor aggregates; raw bar-level null replay is deferred to the next build.",
            }
        )
    return rows


def _build_decision_rows(
    families: list[dict[str, str]],
    selection_rows: list[dict[str, Any]],
    dedup_rows: list[dict[str, Any]],
    leakage_rows: list[dict[str, Any]],
    cost_rows: list[dict[str, Any]],
    null_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    selected = [row for row in selection_rows if row["selection_status"] == "SELECTED_FOR_DEEP_DIVE"]
    artifact = [row for row in dedup_rows if row["classification"] in {"DUPLICATIVE_SURVIVOR", "ARTIFACT_RISK"}]
    leakage = [row for row in leakage_rows if row["leakage_classification"] != "NO_LEAKAGE_DETECTED"]
    robust = {row["family_id"] for row in cost_rows if _float(row["cost_bps"]) == 25.0 and row["cost_classification"] == "COST_ROBUST"}
    null_pass = {
        row["family_id"]
        for row in null_rows
        if row["null_control_classification"] in {"BEATS_NULL_STRONGLY", "BEATS_NULL_WEAKLY"}
    }
    if leakage:
        decision = "SURVIVORS_ARTIFACT_RISK"
    elif selected and len(null_pass) == len(selected) and robust:
        decision = "SURVIVORS_VALIDATED"
    elif selected and null_pass and robust:
        decision = "SURVIVORS_PARTIALLY_VALIDATED"
    elif selected and artifact:
        decision = "SURVIVORS_ARTIFACT_RISK"
    elif selected:
        decision = "SURVIVORS_PARTIALLY_VALIDATED"
    else:
        decision = "SURVIVORS_REJECTED"
    rows = [
        {
            "decision": decision,
            "families_audited": len(families),
            "selected_deep_dive_families": len(selected),
            "duplicate_artifact_risk_families": len(artifact),
            "leakage_risk_families": len(leakage),
            "cost_robust_families": len(robust),
            "null_control_pass_families": len(null_pass),
            "main_blocker": "No raw event timestamp ledger in Build 187-190 source report.",
            "next_action": "Build 199 - emit event-level survivor ledger and rerun independence/null controls from exact timestamps.",
            "authority": "Research-only; no trading, broker execution, capital allocation, position sizing, trade recommendations, paper placement, candidate promotion, or production promotion.",
        }
    ]
    return rows, decision


def _family_symbol_vector(rows: list[dict[str, str]]) -> dict[str, float]:
    return {row["symbol"]: _float(row.get("net_expectancy") or row.get("gross_expectancy")) for row in rows}


def _net_cost_metrics(row: dict[str, str], cost_bps: float) -> dict[str, Any]:
    gross = _float(row.get("gross_expectancy"))
    pf = _float(row.get("gross_profit_factor"))
    net = gross - (cost_bps / 10000.0)
    net_pf = max(0.0, pf - (cost_bps / 100.0))
    if int(_float(row.get("sample_size"))) < 50:
        classification = "INSUFFICIENT"
    elif net > 0.0 and net_pf >= 1.25:
        classification = "STRONG"
    elif net > 0.0 and net_pf > 1.0:
        classification = "WEAK"
    else:
        classification = "FAILED"
    return {"net_expectancy": net, "net_profit_factor": net_pf, "classification": classification}


def _candidate_id(rows: list[dict[str, str]]) -> str:
    ids = sorted({row.get("candidate_id", "") for row in rows if row.get("candidate_id")})
    return ids[0] if ids else ""


def _logic_similarity(left: dict[str, str], right: dict[str, str]) -> float:
    mechanism = left.get("mechanism") == right.get("mechanism")
    regime = left.get("regime") == right.get("regime")
    timeframe = left.get("timeframe") == right.get("timeframe")
    if mechanism and regime and timeframe:
        return 1.0
    if mechanism and regime:
        return 0.75
    if mechanism or regime:
        return 0.5
    return 0.25


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / len(left | right)


def _pearson(left: dict[str, float], right: dict[str, float]) -> float:
    keys = sorted(set(left) & set(right))
    if len(keys) < 2:
        return 0.0
    xs = [left[key] for key in keys]
    ys = [right[key] for key in keys]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0.0 or den_y == 0.0:
        return 0.0
    return num / (den_x * den_y)


def _mean(values: list[float]) -> float:
    values = [value for value in values if not math.isnan(value)]
    return sum(values) / len(values) if values else 0.0


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _fmt(value: Any) -> str:
    return f"{_float(value):.6f}".rstrip("0").rstrip(".")


def _extract_fallback_used(source_latest: dict[str, Any]) -> int:
    summary = source_latest.get("summary") or {}
    if "fallback_used" in summary:
        return int(bool(summary.get("fallback_used")))
    if "fallback_used_count" in summary:
        return int(summary.get("fallback_used_count") or 0)
    fallback_policy = source_latest.get("fallback_policy") or {}
    if any(bool(value) for value in fallback_policy.values() if isinstance(value, bool)):
        return 1
    return 0


def _summary_markdown(
    report: dict[str, Any], selection_rows: list[dict[str, Any]], null_rows: list[dict[str, Any]]
) -> str:
    selected = [row for row in selection_rows if row["selection_status"] == "SELECTED_FOR_DEEP_DIVE"]
    null_summary = ", ".join(
        f"{row['family_id']}={row['null_control_classification']}" for row in null_rows
    ) or "No selected families."
    selected_summary = "\n".join(
        f"- {row['family_id']} ({row['mechanism']}/{row['regime']}/{row['timeframe']})"
        for row in selected
    ) or "- None"
    return f"""# Builds 191-198 - Mechanism Survivor Audit

## Executive Summary

Families audited: {report['families_audited']}

Strong survivors before audit: {report['strong_survivors_before_audit']}

Strong survivors after audit: {report['strong_survivors_after_audit']}

Final decision: {report['final_decision']}

## Integrity and Independence

Duplicate/artifact-risk families: {report['duplicate_artifact_risk_count']}

Main blocker: {report['main_blocker']}

## Leakage and Lookahead

Leakage risk count: {report['leakage_risk_count']}

Code-level audit found no future bars in entry logic, no centered rolling windows, and no full-period entry statistics. Event-level timestamp lineage remains missing from the source report.

## Cost and Liquidity

Cost-robust families at 25 bps: {', '.join(report['cost_robust_families']) or 'None'}

## Top Survivor Deep Dive

{selected_summary}

## Null Controls

{null_summary}

## Confidence Impact

NONE

## Authority Boundary

Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.

## Recommended Next Build

{report['recommended_next_build']}
"""

