from __future__ import annotations

import csv
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1, write_candidate_lifecycle_reports_v1
from ops.aegis.candidate_ranking_explanation_v1 import build_candidate_ranking_v1, write_candidate_ranking_reports_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_candidate_portfolio_selection_v1"
REPORT_FILENAME = "candidate_portfolio_selection.v1.json"
POLICY_NAME = "DEFAULT_ONE_PER_SLEEVE_WITH_DISTINCT_EXPOSURE_EXCEPTION"
DEFAULT_POLICY = {
    "default_max_per_sleeve_per_run": 1,
    "max_per_sleeve_if_distinct_exposure": 2,
    "absolute_max_per_sleeve_per_run": 3,
    "require_distinct_exposure_cluster_for_extra_candidates": True,
    "minimum_confidence_for_extra_candidate": "HIGH",
    "minimum_score_percentile_for_extra_candidate": 80,
    "suppress_same_cluster_lower_ranked": True,
}
RISK_BUDGET_STATUS = "WITHIN_LIMIT_FOR_V1_METADATA_POLICY"
CONCENTRATION_STATUS = "WITHIN_LIMIT_FOR_V1_METADATA_POLICY"


EXPOSURE_MAP: dict[str, dict[str, str]] = {
    "NVDA": {"exposure_cluster": "AI_SEMIS", "risk_theme": "AI_SEMIS", "sector": "Technology", "asset_class": "EQUITY", "correlation_proxy": "AI semiconductor equity beta"},
    "AMD": {"exposure_cluster": "AI_SEMIS", "risk_theme": "AI_SEMIS", "sector": "Technology", "asset_class": "EQUITY", "correlation_proxy": "AI semiconductor equity beta"},
    "AVGO": {"exposure_cluster": "AI_SEMIS", "risk_theme": "AI_SEMIS", "sector": "Technology", "asset_class": "EQUITY", "correlation_proxy": "AI semiconductor equity beta"},
    "SMH": {"exposure_cluster": "AI_SEMIS", "risk_theme": "AI_SEMIS", "sector": "Technology", "asset_class": "ETF", "correlation_proxy": "Semiconductor ETF"},
    "SPY": {"exposure_cluster": "BROAD_EQUITY", "risk_theme": "BROAD_MARKET", "sector": "Broad Market", "asset_class": "ETF", "correlation_proxy": "US large-cap equity beta"},
    "QQQ": {"exposure_cluster": "BROAD_EQUITY", "risk_theme": "BROAD_MARKET", "sector": "Broad Market", "asset_class": "ETF", "correlation_proxy": "US growth equity beta"},
    "IWM": {"exposure_cluster": "BROAD_EQUITY", "risk_theme": "SMALL_CAP", "sector": "Broad Market", "asset_class": "ETF", "correlation_proxy": "US small-cap equity beta"},
    "HYG": {"exposure_cluster": "CREDIT", "risk_theme": "CREDIT", "sector": "Credit", "asset_class": "ETF", "correlation_proxy": "High-yield credit spread beta"},
    "LQD": {"exposure_cluster": "CREDIT", "risk_theme": "CREDIT", "sector": "Credit", "asset_class": "ETF", "correlation_proxy": "Investment-grade credit spread beta"},
    "TLT": {"exposure_cluster": "RATES", "risk_theme": "RATES", "sector": "Rates", "asset_class": "ETF", "correlation_proxy": "Long-duration Treasury rates"},
    "IEF": {"exposure_cluster": "RATES", "risk_theme": "RATES", "sector": "Rates", "asset_class": "ETF", "correlation_proxy": "Intermediate Treasury rates"},
    "GLD": {"exposure_cluster": "MACRO_DEFENSIVE", "risk_theme": "COMMODITY", "sector": "Commodity", "asset_class": "ETF", "correlation_proxy": "Gold defensive macro exposure"},
    "DBC": {"exposure_cluster": "COMMODITY", "risk_theme": "COMMODITY", "sector": "Commodity", "asset_class": "ETF", "correlation_proxy": "Broad commodity basket"},
    "UUP": {"exposure_cluster": "USD", "risk_theme": "USD", "sector": "Currency", "asset_class": "ETF", "correlation_proxy": "US dollar basket"},
    "XBI": {"exposure_cluster": "BIOTECH", "risk_theme": "BIOTECH", "sector": "Biotechnology", "asset_class": "ETF", "correlation_proxy": "Biotech equity beta"},
}


def build_candidate_portfolio_selection_v1(*, truth_root: Path, day_utc: str, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    policy = {**DEFAULT_POLICY, **(policy or {})}
    write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    ranking_path, ranking = latest_json_v1(root, "aegis_candidate_ranking_v1", day_utc, "candidate_ranking.v1.json")
    if not ranking:
        ranking = build_candidate_ranking_v1(truth_root=root, day_utc=day_utc)
        ranking_paths = write_candidate_ranking_reports_v1(truth_root=root, day_utc=day_utc, payload=ranking)
        ranking_path = Path(ranking_paths["ranking"])
    lifecycle_by_id = {
        str(row.get("candidate_id")): row
        for row in lifecycle.get("candidates", [])
        if isinstance(row, dict) and row.get("candidate_id")
    }
    ranking_rows = [row for row in ranking.get("ranked_candidates", []) if isinstance(row, dict)]
    enriched = _enriched_candidates(ranking_rows, lifecycle_by_id=lifecycle_by_id, day_utc=day_utc)
    selected: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    watchlist: list[dict[str, Any]] = []
    same_sleeve_summary: list[dict[str, Any]] = []
    for group_key, group_rows in _group_by_sleeve_run(enriched).items():
        group_rows = sorted(group_rows, key=lambda row: (int(row.get("rank_within_sleeve") or 9999), -float(row.get("score") or 0.0), str(row.get("candidate_id") or "")))
        group_selected, group_suppressed, group_watch = _select_group(group_rows, policy=policy)
        selected.extend(group_selected)
        suppressed.extend(group_suppressed)
        watchlist.extend(group_watch)
        sleeve_id, run_id = group_key
        same_sleeve_summary.append(
            {
                "sleeve_id": sleeve_id,
                "run_id": run_id,
                "same_sleeve_candidate_count": len(group_rows),
                "selected_count": len(group_selected),
                "suppressed_count": len(group_suppressed),
                "watchlist_count": len(group_watch),
                "selected_symbols": [row.get("symbol") for row in group_selected],
                "suppression_reason_codes": sorted({str(row.get("suppression_reason_code") or "") for row in group_suppressed if row.get("suppression_reason_code")}),
                "policy": POLICY_NAME,
            }
        )
    candidates = sorted(selected + suppressed + watchlist, key=lambda row: (str(row.get("selection_status") != "SELECTED"), int(row.get("rank") or 9999), str(row.get("candidate_id") or "")))
    return {
        "schema_id": "aegis_candidate_portfolio_selection",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_portfolio_selection_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "selection_policy_name": POLICY_NAME,
        "portfolio_selection_policy": policy,
        "candidate_count": len(candidates),
        "selected_count": len(selected),
        "suppressed_count": len(suppressed),
        "watchlist_count": len(watchlist),
        "selected_candidates": sorted(selected, key=lambda row: (int(row.get("rank") or 9999), str(row.get("candidate_id") or ""))),
        "suppressed_candidates": sorted(suppressed, key=lambda row: (int(row.get("rank") or 9999), str(row.get("candidate_id") or ""))),
        "watchlist_candidates": sorted(watchlist, key=lambda row: (int(row.get("rank") or 9999), str(row.get("candidate_id") or ""))),
        "candidates": candidates,
        "same_sleeve_selection_summary": sorted(same_sleeve_summary, key=lambda row: (str(row.get("sleeve_id") or ""), str(row.get("run_id") or ""))),
        "exposure_cluster_summary": _exposure_cluster_summary(candidates),
        "learning_inputs": _learning_inputs(candidates),
        "input_artifacts": {
            "candidate_lifecycle": str(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_lifecycle.v1.json"),
            "candidate_ranking": str(ranking_path or ""),
        },
        "safety": _safety_flags(),
        **_safety_flags(),
    }


def write_candidate_portfolio_selection_reports_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / REPORT_FILENAME, payload)
    summary_path = out_dir / "candidate_portfolio_selection.summary.txt"
    matrix_path = out_dir / "candidate_portfolio_selection.matrix.csv"
    summary_path.write_text(render_candidate_portfolio_selection_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_candidate_portfolio_selection_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_candidate_portfolio_selection_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS CANDIDATE PORTFOLIO SELECTION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"policy: {payload.get('selection_policy_name')}",
        f"candidate_count: {payload.get('candidate_count')}",
        f"selected_count: {payload.get('selected_count')}",
        f"suppressed_count: {payload.get('suppressed_count')}",
        f"watchlist_count: {payload.get('watchlist_count')}",
        "selected:",
    ]
    for row in payload.get("selected_candidates") or []:
        lines.append(f"- {row.get('symbol')} sleeve={row.get('sleeve_id')} cluster={row.get('exposure_cluster')} reason={row.get('operator_explanation')}")
    lines.append("suppressed:")
    for row in (payload.get("suppressed_candidates") or [])[:20]:
        lines.append(f"- {row.get('symbol')} sleeve={row.get('sleeve_id')} code={row.get('suppression_reason_code')} reason={row.get('operator_explanation')}")
    lines.extend(["", "broker_execution_allowed: false", "autonomous_execution_allowed: false", "automatic_approval_allowed: false", ""])
    return "\n".join(lines)


def render_candidate_portfolio_selection_matrix_csv_v1(payload: dict[str, Any]) -> str:
    buffer = StringIO()
    fieldnames = [
        "candidate_id",
        "symbol",
        "sleeve_id",
        "run_id",
        "rank_within_sleeve",
        "score_band",
        "confidence",
        "exposure_cluster",
        "selection_status",
        "suppression_reason_code",
        "operator_explanation",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in payload.get("candidates") or []:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return buffer.getvalue()


def exposure_metadata_v1(symbol: str, candidate: dict[str, Any] | None = None) -> dict[str, str]:
    candidate = candidate if isinstance(candidate, dict) else {}
    explicit_cluster = str(candidate.get("exposure_cluster") or "").strip().upper()
    explicit_theme = str(candidate.get("risk_theme") or "").strip().upper()
    sym = str(symbol or candidate.get("symbol") or "").strip().upper()
    mapped = dict(EXPOSURE_MAP.get(sym) or {})
    if explicit_cluster:
        mapped["exposure_cluster"] = explicit_cluster
    if explicit_theme:
        mapped["risk_theme"] = explicit_theme
    if not mapped:
        mapped = {
            "exposure_cluster": explicit_cluster or f"SYMBOL_{sym}" if sym else "UNKNOWN",
            "risk_theme": explicit_theme or "UNKNOWN",
            "sector": str(candidate.get("sector") or "UNKNOWN"),
            "asset_class": str(candidate.get("asset_class") or "UNKNOWN"),
            "correlation_proxy": str(candidate.get("correlation_proxy") or "deterministic symbol metadata unavailable"),
        }
    mapped.setdefault("exposure_cluster", "UNKNOWN")
    mapped.setdefault("risk_theme", "UNKNOWN")
    mapped.setdefault("sector", str(candidate.get("sector") or "UNKNOWN"))
    mapped.setdefault("asset_class", str(candidate.get("asset_class") or "UNKNOWN"))
    mapped.setdefault("correlation_proxy", str(candidate.get("correlation_proxy") or mapped.get("exposure_cluster") or "UNKNOWN"))
    mapped["exposure_cluster_reason"] = (
        f"{sym} mapped by deterministic v1 metadata."
        if sym in EXPOSURE_MAP
        else "No deterministic mapping found; using candidate metadata or UNKNOWN cluster."
    )
    return mapped


def _enriched_candidates(ranking_rows: list[dict[str, Any]], *, lifecycle_by_id: dict[str, dict[str, Any]], day_utc: str) -> list[dict[str, Any]]:
    raw_scores = [_score(row) for row in ranking_rows]
    max_score = max(raw_scores) if raw_scores else 0.0
    min_score = min(raw_scores) if raw_scores else 0.0
    by_sleeve: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    enriched: list[dict[str, Any]] = []
    for row in ranking_rows:
        candidate_id = str(row.get("candidate_id") or "")
        lifecycle = lifecycle_by_id.get(candidate_id, {})
        merged = {**lifecycle, **row}
        symbol = str(merged.get("symbol") or "").upper()
        run_id = _run_id(merged, day_utc)
        sleeve_id = str(merged.get("sleeve_id") or "UNKNOWN")
        score = _score(merged)
        score_percentile = _score_percentile(score, min_score=min_score, max_score=max_score)
        exposure = exposure_metadata_v1(symbol, merged)
        item = {
            **merged,
            **exposure,
            "candidate_id": candidate_id,
            "symbol": symbol or merged.get("symbol") or "UNKNOWN",
            "sleeve_id": sleeve_id,
            "run_id": run_id,
            "rank": int(row.get("rank") or 9999),
            "rank_within_sleeve": 0,
            "score": score,
            "score_percentile": score_percentile,
            "score_band": _score_band(score_percentile),
            "confidence": _confidence_label(merged.get("confidence")),
            "selection_status": "WATCHLIST",
            "selection_reason": "",
            "suppression_reason_code": "",
            "operator_explanation": "",
            "suggested_operator_action": "Review manually; no broker execution.",
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
        }
        by_sleeve[(sleeve_id, run_id)].append(item)
        enriched.append(item)
    for (_sleeve, _run), rows in by_sleeve.items():
        for index, row in enumerate(sorted(rows, key=lambda item: (int(item.get("rank") or 9999), -float(item.get("score") or 0.0))), start=1):
            row["rank_within_sleeve"] = index
    return enriched


def _group_by_sleeve_run(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(str(row.get("sleeve_id") or "UNKNOWN"), str(row.get("run_id") or "UNKNOWN"))].append(row)
    return groups


def _select_group(rows: list[dict[str, Any]], *, policy: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    watchlist: list[dict[str, Any]] = []
    selected_clusters: set[str] = set()
    absolute_max = int(policy.get("absolute_max_per_sleeve_per_run") or 3)
    distinct_max = int(policy.get("max_per_sleeve_if_distinct_exposure") or 2)
    for row in rows:
        cluster = str(row.get("exposure_cluster") or "UNKNOWN")
        if not selected:
            selected.append(_selected_row(row, f"Selected {row.get('symbol')} as the strongest candidate from this sleeve run."))
            selected_clusters.add(cluster)
            continue
        if len(selected) >= absolute_max:
            suppressed.append(_suppressed_row(row, "LOWER_RANKED_SAME_SLEEVE", f"Suppressed {row.get('symbol')} because the sleeve already reached the absolute per-run candidate limit."))
            continue
        if len(selected) >= distinct_max:
            suppressed.append(_suppressed_row(row, "LOWER_RANKED_SAME_SLEEVE", f"Suppressed {row.get('symbol')} because the distinct-exposure exception is already full for this sleeve run."))
            continue
        if bool(policy.get("require_distinct_exposure_cluster_for_extra_candidates", True)) and cluster in selected_clusters:
            suppressed.append(_suppressed_row(row, "DUPLICATE_EXPOSURE_CLUSTER", f"Suppressed {row.get('symbol')} because a stronger same-sleeve candidate already represents {cluster} exposure."))
            continue
        eligible, reason_code = _extra_candidate_eligible(row, policy=policy)
        if not eligible:
            suppressed.append(_suppressed_row(row, reason_code, _threshold_explanation(row, reason_code)))
            continue
        selected.append(_selected_row(row, f"Selected {row.get('symbol')} in addition to stronger same-sleeve candidates because it is a distinct exposure cluster and within v1 risk-budget limits."))
        selected_clusters.add(cluster)
    selected_count = len(selected)
    return _with_group_context(selected, rows, selected_count), _with_group_context(suppressed, rows, selected_count), _with_group_context(watchlist, rows, selected_count)


def _selected_row(row: dict[str, Any], explanation: str) -> dict[str, Any]:
    return {
        **row,
        "selection_status": "SELECTED",
        "selection_reason": "SELECTED_BY_PORTFOLIO_SELECTION_POLICY",
        "suppression_reason_code": "",
        "operator_explanation": explanation,
        "why_selected": explanation,
        "why_not": "No broker execution, autonomous execution, or automatic approval is allowed.",
        "risk_budget_status": RISK_BUDGET_STATUS,
        "portfolio_concentration_status": CONCENTRATION_STATUS,
    }


def _suppressed_row(row: dict[str, Any], code: str, explanation: str) -> dict[str, Any]:
    return {
        **row,
        "selection_status": "SUPPRESSED",
        "selection_reason": "",
        "suppression_reason_code": code,
        "operator_explanation": explanation,
        "why_selected": "",
        "why_not": explanation,
        "risk_budget_status": RISK_BUDGET_STATUS,
        "portfolio_concentration_status": CONCENTRATION_STATUS,
    }


def _with_group_context(rows: list[dict[str, Any]], group_rows: list[dict[str, Any]], selected_count: int) -> list[dict[str, Any]]:
    for row in rows:
        row["same_sleeve_group"] = {
            "same_sleeve_candidate_count": len(group_rows),
            "selected_count": selected_count,
            "policy": POLICY_NAME,
        }
    return rows


def _extra_candidate_eligible(row: dict[str, Any], *, policy: dict[str, Any]) -> tuple[bool, str]:
    confidence = str(row.get("confidence") or "UNKNOWN").upper()
    score_percentile = float(row.get("score_percentile") or 0.0)
    min_conf = str(policy.get("minimum_confidence_for_extra_candidate") or "HIGH").upper()
    min_score = float(policy.get("minimum_score_percentile_for_extra_candidate") or 80)
    if _confidence_rank(confidence) >= _confidence_rank(min_conf):
        return True, ""
    if score_percentile >= min_score:
        return True, ""
    if confidence not in {"HIGH", "MEDIUM", "LOW"}:
        return False, "CONFIDENCE_TOO_LOW"
    return False, "SCORE_BELOW_EXTRA_CANDIDATE_THRESHOLD"


def _threshold_explanation(row: dict[str, Any], code: str) -> str:
    if code == "CONFIDENCE_TOO_LOW":
        return f"Suppressed {row.get('symbol')} because confidence is too low or unknown for an extra same-sleeve candidate."
    return f"Suppressed {row.get('symbol')} because it did not clear the extra same-sleeve candidate score threshold."


def _score(row: dict[str, Any]) -> float:
    for key in ("ranking_score", "score", "pre_suppression_score_total"):
        try:
            value = row.get(key)
            if value is not None and value != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _score_percentile(score: float, *, min_score: float, max_score: float) -> float:
    if max_score <= min_score:
        return 100.0 if score >= max_score else 0.0
    return round(((score - min_score) / (max_score - min_score)) * 100.0, 2)


def _score_band(percentile: float) -> str:
    if percentile >= 80:
        return "HIGH"
    if percentile >= 40:
        return "MEDIUM"
    return "LOW"


def _confidence_label(value: Any) -> str:
    if isinstance(value, (int, float)):
        if float(value) >= 0.75:
            return "HIGH"
        if float(value) >= 0.45:
            return "MEDIUM"
        return "LOW"
    text = str(value or "UNKNOWN").upper()
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    if "HIGH" in text or "STRONG" in text:
        return "HIGH"
    if "MEDIUM" in text or "MODERATE" in text:
        return "MEDIUM"
    if "LOW" in text or "WEAK" in text:
        return "LOW"
    return "UNKNOWN"


def _confidence_rank(value: str) -> int:
    return {"UNKNOWN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(str(value or "UNKNOWN").upper(), 0)


def _run_id(row: dict[str, Any], day_utc: str) -> str:
    for key in ("run_id", "source_run_id", "triggered_run_id", "trigger_id", "raw_signal_id"):
        value = row.get(key)
        if value:
            return str(value)
    return f"aegis:{day_utc}"


def _exposure_cluster_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for row in rows:
        cluster = str(row.get("exposure_cluster") or "UNKNOWN")
        item = summary.setdefault(cluster, {"exposure_cluster": cluster, "candidate_count": 0, "selected_count": 0, "suppressed_count": 0, "symbols": [], "risk_theme": row.get("risk_theme") or "UNKNOWN"})
        item["candidate_count"] += 1
        if row.get("selection_status") == "SELECTED":
            item["selected_count"] += 1
        if row.get("selection_status") == "SUPPRESSED":
            item["suppressed_count"] += 1
        if row.get("symbol") not in item["symbols"]:
            item["symbols"].append(row.get("symbol"))
    return sorted(summary.values(), key=lambda row: (-int(row["candidate_count"]), str(row["exposure_cluster"])))


def _learning_inputs(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [row for row in rows if row.get("selection_status") == "SELECTED"]
    suppressed = [row for row in rows if row.get("selection_status") == "SUPPRESSED"]
    return {
        "selected_vs_suppressed_outcome": "INSUFFICIENT_DATA",
        "suppressed_candidate_opportunity_cost": "INSUFFICIENT_DATA",
        "same_sleeve_selection_accuracy": "INSUFFICIENT_DATA",
        "exposure_cluster_selection_quality": "INSUFFICIENT_DATA",
        "selected_candidate_ids": [row.get("candidate_id") for row in selected],
        "suppressed_candidate_ids": [row.get("candidate_id") for row in suppressed],
    }


def _safety_flags() -> dict[str, bool]:
    return {
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_order_placement_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }
