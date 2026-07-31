from __future__ import annotations

import json
import resource
import time
from datetime import date
from pathlib import Path
from typing import Any

from .bulk_observation_import import EXPANDED_SYMBOLS, stable_id, structured_observation_rows, observation_record_from_raw
from .candidate_backtests import SUPPORTED_CLASSIFICATION, _load_local_spy_data
from .historical_replay_engine import now_utc
from .observation_deduplication import cluster_observations, deduplicate_observations
from .observation_import_governance import validate_observation_import_allowed
from .observation_trial import (
    AUTHORITY_BOUNDARY,
    AUTHORITY_STATEMENT,
    _run_cluster_trial,
    _validate_authority,
)

REPORT_ROOT = Path("reports/atlas_v2_research_os/observation_scale_stress")
DEFAULT_LEVELS = (5_000, 10_000, 25_000, 50_000, 100_000)
STOP_DUPLICATE_RATE = 0.90
STOP_PAPER_FORWARD_READY_COLLAPSE = 0.75


def build_observation_scale_stress_report(
    *,
    levels: tuple[int, ...] = DEFAULT_LEVELS,
    day: str | None = None,
    created_at: str | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Any]:
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    data_rows, data_meta, data_limitations = _load_local_spy_data(Path(data_path) if data_path else None)
    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    previous_clean: dict[str, Any] | None = None
    previous_ready_rate: float | None = None
    stopped = False

    for level in levels:
        if stopped:
            results.append(_skipped_level(level, "STOP_RULE_ALREADY_TRIGGERED"))
            continue
        if level >= 100_000 and not (previous_clean and previous_clean.get("level") == 50_000 and previous_clean.get("clean") is True):
            results.append(_skipped_level(level, "STOP_BEFORE_100000_UNLESS_50000_COMPLETES_CLEANLY"))
            stopped = True
            continue
        try:
            result = _run_level(level, created_at=created, data_rows=data_rows, data_meta=data_meta)
        except Exception as exc:
            result = _failed_level(level, "RUNTIME_ERROR", str(exc))
            failures.append({"level": level, "failure_type": "RUNTIME_ERROR", "message": str(exc)})
            stopped = True
            results.append(result)
            continue

        stop_reasons = list(result.get("stop_reasons", []))
        duplicate_rate = float(result.get("duplicate_rate") or 0.0)
        ready_rate = float(result.get("paper_forward_ready_rate") or 0.0)
        if duplicate_rate > STOP_DUPLICATE_RATE:
            stop_reasons.append("DUPLICATE_RATE_GT_90_PERCENT")
        if previous_ready_rate is not None and previous_ready_rate > 0:
            collapse = (previous_ready_rate - ready_rate) / previous_ready_rate
            result["paper_forward_ready_rate_collapse_from_previous"] = round(collapse, 6)
            if collapse > STOP_PAPER_FORWARD_READY_COLLAPSE:
                stop_reasons.append("PAPER_FORWARD_READY_RATE_COLLAPSE_GT_75_PERCENT")
        else:
            result["paper_forward_ready_rate_collapse_from_previous"] = 0.0
        if result.get("governance_certification", {}).get("status") != "PASS":
            stop_reasons.append("GOVERNANCE_OR_CERTIFICATION_FAILED")

        result["stop_reasons"] = sorted(set(stop_reasons))
        result["clean"] = not result["stop_reasons"] and not result.get("failures")
        results.append(result)
        if result["clean"]:
            previous_clean = result
        if result.get("historical_replays_executed", 0) > 0:
            previous_ready_rate = ready_rate
        if result["stop_reasons"]:
            stopped = True

    report = {
        "schema_id": "atlas_v2_research_os_observation_scale_stress_report_v1",
        "schema_version": "v1",
        "report_type": "OBSERVATION_SCALE_STRESS",
        "report_id": stable_id("obs_scale_stress", [created, levels, [row.get("level") for row in results]]),
        "created_at": created,
        "day": day_value,
        "levels_requested": list(levels),
        "levels_executed": [row["level"] for row in results if row.get("status") == "EXECUTED"],
        "levels_skipped": [row for row in results if row.get("status") == "SKIPPED"],
        "stop_rules": {
            "runtime_errors": "stop",
            "duplicate_rate_gt_90_percent": "stop",
            "paper_forward_ready_rate_collapse_gt_75_percent": "stop",
            "governance_or_certification_failure": "stop",
            "before_100000": "100000 only runs after 50000 completes cleanly",
        },
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "results": results,
        "failures": failures + [failure for row in results for failure in row.get("failures", [])],
        "authority_boundary": {
            **dict(AUTHORITY_BOUNDARY),
            "automatic_paper_placement_authorized": False,
        },
        "confirmations": {
            "no_live_trading": True,
            "no_broker_execution": True,
            "no_capital_authority": True,
            "no_automatic_paper_placement": True,
            "no_position_sizing": True,
            "paper_forward_ready_is_human_review_only": True,
        },
        "limitations": [
            AUTHORITY_STATEMENT,
            "Stress observations are deterministic generated research inputs and are not imported into operational Atlas memory or candidate queues.",
            "Paper-forward-ready means human-review-only candidate evidence intersection, not paper placement or broker action.",
        ],
    }
    _validate_authority(report)
    return report


def write_observation_scale_stress_report(
    *,
    report_root: str | Path = REPORT_ROOT,
    levels: tuple[int, ...] = DEFAULT_LEVELS,
    day: str | None = None,
    created_at: str | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Path]:
    report = build_observation_scale_stress_report(levels=levels, day=day, created_at=created_at, data_path=data_path)
    day_value = day or report["day"]
    out_root = Path(report_root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "observation_scale_stress_report.json"
    summary_path = out_dir / "observation_scale_stress_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_observation_scale_stress_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_observation_scale_stress_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Observation Scale Stress",
        "",
        f"Created: {report.get('created_at')}",
        f"Levels executed: {', '.join(str(value) for value in report.get('levels_executed', [])) or 'none'}",
        "",
        "| Level | Status | Observations | Clusters | Claims | Hypotheses | Replays | Positive replay rate | Eligible | Backtest-supported | Paper-forward-ready | Duplicates skipped | Duration s | Quality/1k | Failures |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in report.get("results", []):
        lines.append(
            "| {level} | {status} | {obs} | {clusters} | {claims} | {hyp} | {replays} | {pos} | {eligible} | {supported} | {ready} | {dups} | {duration} | {quality} | {failures} |".format(
                level=row.get("level"),
                status=row.get("status"),
                obs=row.get("observations_generated", 0),
                clusters=row.get("clusters_created", 0),
                claims=row.get("claims_generated", 0),
                hyp=row.get("hypotheses_generated", 0),
                replays=row.get("historical_replays_executed", 0),
                pos=row.get("positive_replay_rate", 0.0),
                eligible=row.get("eligible_candidates", 0),
                supported=row.get("backtest_supported_candidates", 0),
                ready=row.get("paper_forward_ready_candidates", 0),
                dups=row.get("duplicates_skipped", 0),
                duration=row.get("runtime_duration_seconds", 0.0),
                quality=row.get("quality_per_1000_observations", {}).get("paper_forward_ready_candidates_per_1000", 0.0),
                failures="; ".join(row.get("stop_reasons", []) + [item.get("message", "") for item in row.get("failures", [])]) or "none",
            )
        )
    lines.extend(["", "## Memory/File Size Issues", ""])
    for row in report.get("results", []):
        if row.get("status") != "EXECUTED":
            continue
        issues = row.get("memory_file_size_issues", [])
        lines.append(f"- {row.get('level')}: {', '.join(issues) if issues else 'none'}")
    lines.extend(
        [
            "",
            "## Confirmations",
            "",
            "- No live trading.",
            "- No broker execution.",
            "- No capital authority.",
            "- No automatic paper placement.",
            "- No position sizing.",
            "",
        ]
    )
    return "\n".join(lines)


def _run_level(level: int, *, created_at: str, data_rows: list[dict[str, Any]], data_meta: dict[str, Any]) -> dict[str, Any]:
    start = time.perf_counter()
    rows = structured_observation_rows(count=level, profile_name=f"OBSERVATION_SCALE_STRESS_{level}", symbol_universe=EXPANDED_SYMBOLS)
    records: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for index, raw in enumerate(rows):
        try:
            record = observation_record_from_raw(raw, index=index, created_at=created_at)
            validate_observation_import_allowed(record)
            records.append(record)
        except Exception as exc:
            invalid.append({"row_index": index, "error": str(exc)})
    unique, duplicates = deduplicate_observations(records)
    clusters = cluster_observations(unique)
    trials: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index, cluster in enumerate(clusters):
        try:
            trials.append(_run_cluster_trial(cluster, index=index, created_at=created_at, data_rows=data_rows, data_meta=data_meta))
        except Exception as exc:
            failures.append({"cluster_id": cluster.get("cluster_id"), "failure_type": "CLUSTER_TRIAL_ERROR", "message": str(exc)})
            break

    metrics = _metrics(level, records, invalid, unique, duplicates, clusters, trials)
    duration = round(time.perf_counter() - start, 6)
    approx_input_bytes = len(json.dumps(rows[: min(len(rows), 1000)], sort_keys=True).encode("utf-8"))
    estimated_input_bytes = int((approx_input_bytes / max(1, min(len(rows), 1000))) * len(rows))
    peak_rss_kb = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    result = {
        "level": level,
        "status": "EXECUTED",
        **metrics,
        "runtime_duration_seconds": duration,
        "memory": {"peak_rss_kb": peak_rss_kb, "peak_rss_mb": round(peak_rss_kb / 1024, 3)},
        "file_size": {"estimated_generated_observations_json_bytes": estimated_input_bytes, "estimated_generated_observations_json_mb": round(estimated_input_bytes / 1_000_000, 3)},
        "memory_file_size_issues": _memory_file_issues(peak_rss_kb=peak_rss_kb, estimated_input_bytes=estimated_input_bytes),
        "failures": failures,
        "sample_trials": _sample_trials(trials),
        "governance_certification": _governance_certification(failures),
        "stop_reasons": ["RUNTIME_ERROR"] if failures else [],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }
    _validate_authority({"authority_boundary": result["authority_boundary"]})
    return result


def _metrics(
    level: int,
    records: list[dict[str, Any]],
    invalid: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    duplicates: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
    trials: list[dict[str, Any]],
) -> dict[str, Any]:
    positives = [row for row in trials if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    eligible = [row for row in trials if row.get("edge_qualification", {}).get("eligible") is True]
    supported = [row for row in trials if row.get("candidate_backtest", {}).get("classification") == SUPPORTED_CLASSIFICATION]
    ready = [row for row in trials if row.get("paper_forward_ready") is True]
    claims = len(clusters)
    return {
        "observations_generated": level,
        "observations_imported": len(unique),
        "invalid_observations": len(invalid),
        "clusters_created": len(clusters),
        "claims_generated": claims,
        "hypotheses_generated": len(trials),
        "historical_replays_executed": len(trials),
        "positive_replays": len(positives),
        "positive_replay_rate": round(len(positives) / len(trials), 6) if trials else 0.0,
        "eligible_candidates": len(eligible),
        "backtest_supported_candidates": len(supported),
        "paper_forward_ready_candidates": len(ready),
        "duplicates_skipped": len(duplicates),
        "duplicate_rate": round(len(duplicates) / len(records), 6) if records else 0.0,
        "paper_forward_ready_rate": round(len(ready) / len(trials), 6) if trials else 0.0,
        "quality_per_1000_observations": {
            "clusters_per_1000": _per_1000(len(clusters), level),
            "claims_per_1000": _per_1000(claims, level),
            "hypotheses_per_1000": _per_1000(len(trials), level),
            "positive_replays_per_1000": _per_1000(len(positives), level),
            "eligible_candidates_per_1000": _per_1000(len(eligible), level),
            "backtest_supported_candidates_per_1000": _per_1000(len(supported), level),
            "paper_forward_ready_candidates_per_1000": _per_1000(len(ready), level),
        },
    }


def _governance_certification(failures: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "PASS" if not failures else "FAIL",
        "research_only": True,
        "live_trading_authorized": False,
        "broker_execution_authorized": False,
        "capital_authorized": False,
        "automatic_paper_trade_placement_authorized": False,
        "position_sizing_authorized": False,
        "failures": failures,
    }


def _memory_file_issues(*, peak_rss_kb: int, estimated_input_bytes: int) -> list[str]:
    issues: list[str] = []
    if peak_rss_kb > 1_048_576:
        issues.append("peak_rss_gt_1gb")
    if estimated_input_bytes > 250_000_000:
        issues.append("estimated_generated_observation_json_gt_250mb")
    return issues


def _sample_trials(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sample = trials[:5]
    return [
        {
            "cluster_id": row.get("cluster_id"),
            "hypothesis_id": row.get("hypothesis_id"),
            "candidate_id": row.get("candidate_id"),
            "historical_replay": row.get("historical_replay"),
            "edge_qualification": row.get("edge_qualification"),
            "candidate_backtest": row.get("candidate_backtest"),
            "paper_forward_ready": row.get("paper_forward_ready"),
        }
        for row in sample
    ]


def _skipped_level(level: int, reason: str) -> dict[str, Any]:
    return {"level": level, "status": "SKIPPED", "skip_reason": reason, "stop_reasons": [reason], "clean": False}


def _failed_level(level: int, failure_type: str, message: str) -> dict[str, Any]:
    return {
        "level": level,
        "status": "FAILED",
        "failures": [{"failure_type": failure_type, "message": message}],
        "stop_reasons": [failure_type],
        "clean": False,
    }


def _per_1000(value: int, observations: int) -> float:
    return round((value / observations) * 1000, 6) if observations else 0.0
