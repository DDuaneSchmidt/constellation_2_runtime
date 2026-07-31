from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .historical_replay_engine import create_historical_replay_request, run_historical_replay
from .historical_replay_governance import validate_historical_replay_allowed
from .historical_replay_results import route_historical_replay_backlog_items

HISTORICAL_REPLAY_REPORT_ROOT = Path("reports/atlas_v2_research_os/historical_replay")


def build_historical_replay_report(replay_results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    results = list(replay_results or demo_historical_replay_results())
    for result in results:
        validate_historical_replay_allowed(result)
    status_counts = Counter(result.get("certification", {}).get("status", "INSUFFICIENT_SAMPLE") for result in results)
    supported = Counter()
    failed = Counter()
    reviews: list[str] = []
    analyses: list[str] = []
    for result in results:
        status = result.get("certification", {}).get("status")
        tags = result.get("mechanism_tags", []) or ["UNKNOWN"]
        if status == "REPLAY_POSITIVE":
            supported.update(tags)
            reviews.append(result["replay_id"])
        if status == "REPLAY_NEGATIVE":
            failed.update(tags)
            analyses.append(result["replay_id"])
    return {
        "schema_id": "atlas_v2_research_os_historical_replay_report_v1",
        "schema_version": "v1",
        "day": date.today().isoformat(),
        "replays_executed": len(results),
        "positive_replays": status_counts.get("REPLAY_POSITIVE", 0),
        "neutral_replays": status_counts.get("REPLAY_NEUTRAL", 0),
        "negative_replays": status_counts.get("REPLAY_NEGATIVE", 0),
        "insufficient_sample": status_counts.get("INSUFFICIENT_SAMPLE", 0),
        "top_supported_mechanisms": _top_counter(supported),
        "top_failed_mechanisms": _top_counter(failed),
        "recommended_candidate_reviews": reviews,
        "recommended_failure_analyses": analyses,
        "results": results,
        "authority_boundary": {
            "evidence_only": True,
            "trading_authorized": False,
            "capital_authorized": False,
            "candidate_promotion_authorized": False,
            "paper_trade_placement_authorized": False,
        },
    }


def write_historical_replay_report(
    replay_results: list[dict[str, Any]] | None = None,
    *,
    root: str | Path = HISTORICAL_REPLAY_REPORT_ROOT,
    day: str | None = None,
    backlog_root: str | Path | None = None,
) -> dict[str, Path]:
    report = build_historical_replay_report(replay_results)
    day_value = day or report["day"]
    out_root = Path(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    if backlog_root is not None:
        routed = []
        for result in report["results"]:
            routed.extend(route_historical_replay_backlog_items(result, root=backlog_root))
        report["routed_backlog_items"] = routed
    json_path = out_dir / "historical_replay_report.json"
    md_path = out_dir / "historical_replay_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_historical_replay_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def audit_historical_replay_report(root: str | Path = HISTORICAL_REPLAY_REPORT_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root)
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                for result in payload.get("results", []):
                    validate_historical_replay_allowed(result)
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"historical_replay_audit_ok": not failures, "historical_replay_audit_failures": failures}


def render_historical_replay_summary(report: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Atlas Historical Replay Summary",
            "",
            f"Replays executed: {report['replays_executed']}",
            f"Positive replays: {report['positive_replays']}",
            f"Neutral replays: {report['neutral_replays']}",
            f"Negative replays: {report['negative_replays']}",
            f"Insufficient sample: {report['insufficient_sample']}",
            f"Top supported mechanisms: {json.dumps(report['top_supported_mechanisms'], sort_keys=True)}",
            f"Top failed mechanisms: {json.dumps(report['top_failed_mechanisms'], sort_keys=True)}",
            f"Recommended candidate reviews: {json.dumps(report['recommended_candidate_reviews'], sort_keys=True)}",
            f"Recommended failure analyses: {json.dumps(report['recommended_failure_analyses'], sort_keys=True)}",
            "",
            "Historical replay is evidence generation only and does not authorize trading, capital, promotion, or paper placement.",
            "",
        ]
    )


def demo_historical_replay_results() -> list[dict[str, Any]]:
    positive_request = create_historical_replay_request(
        hypothesis_id="hyp-demo-opening-range",
        mechanism_tags=["OPENING_RANGE", "VOLATILITY_EXPANSION"],
        regime_context={"label": "OPENING_SESSION_HIGH_VOLATILITY"},
        time_window="1y",
        source_artifact_ids=["artifact-demo-opening-range"],
        replay_id="hist-replay-demo-positive",
        created_at="2026-06-05T00:00:00Z",
    )
    negative_request = create_historical_replay_request(
        hypothesis_id="hyp-demo-midday-chop",
        mechanism_tags=["MEAN_REVERSION"],
        regime_context={"label": "MIDDAY_SESSION_CHOP"},
        time_window="1y",
        source_artifact_ids=["artifact-demo-midday-chop"],
        replay_id="hist-replay-demo-negative",
        created_at="2026-06-05T00:00:00Z",
    )
    positive_samples = [{"return": value, "regime": "OPENING_SESSION_HIGH_VOLATILITY"} for value in [0.018, 0.012, -0.004, 0.021, 0.009, 0.016, -0.003, 0.014, 0.011, 0.020]]
    negative_samples = [{"return": value, "regime": "MIDDAY_SESSION_CHOP"} for value in [-0.012, -0.009, 0.002, -0.010, -0.006, 0.001, -0.011, -0.004]]
    return [
        run_historical_replay(positive_request, positive_samples, created_at="2026-06-05T00:00:00Z"),
        run_historical_replay(negative_request, negative_samples, created_at="2026-06-05T00:00:00Z"),
    ]


def _top_counter(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    return [{"mechanism": key, "count": value} for key, value in counter.most_common(limit)]
