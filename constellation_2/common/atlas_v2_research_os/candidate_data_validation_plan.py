from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_symbol_attribution import apply_symbol_attribution
from .final_candidate_ranking import AUTHORITY_BOUNDARY

DATA_PLAN_DIRNAME = "candidate_data_validation_plan"
APPROVAL_CHECKLIST_DIRNAME = "paper_forward_approval_checklist"


def build_candidate_data_validation_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    campaign = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    candidates = list(campaign.get("campaign_candidates") or [])
    plans = [_candidate_data_plan(row) for row in candidates]
    return {
        "schema_id": "atlas_v2_research_os_candidate_data_validation_plan",
        "schema_version": "1.0",
        "report_type": "CANDIDATE_DATA_VALIDATION_PLAN",
        "created_at": created_at or _now(),
        "day": (created_at or _now())[:10],
        "source_report": str(root_path / "focused_observation_campaign" / "latest.json"),
        "summary": {
            "candidates_covered": len(plans),
            "direct_symbol_data_required": sum(plan["direct_symbol_or_universe_required"] for plan in plans),
            "intraday_data_required": sum(plan["intraday_required"] for plan in plans),
            "daily_data_required": sum(plan["daily_data_required"] for plan in plans),
            "mechanism_distribution": dict(Counter(plan["mechanism"] for plan in plans)),
            "regime_distribution": dict(Counter(plan["regime"] for plan in plans)),
            "primary_blocker": "Candidate-specific symbol/universe data is missing; current evidence depends on SPY daily proxy.",
        },
        "candidate_data_validation_plans": plans,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": _guardrails(),
    }


def build_paper_forward_approval_checklist(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    data_plan = _read_json(root_path / DATA_PLAN_DIRNAME / "latest.json", {})
    if not data_plan:
        data_plan = build_candidate_data_validation_plan(root_path, created_at=created_at)
    checklist = [_approval_checklist(row) for row in data_plan.get("candidate_data_validation_plans", [])]
    return {
        "schema_id": "atlas_v2_research_os_paper_forward_approval_checklist",
        "schema_version": "1.0",
        "report_type": "PAPER_FORWARD_APPROVAL_CHECKLIST",
        "created_at": created_at or _now(),
        "day": (created_at or _now())[:10],
        "source_report": str(root_path / DATA_PLAN_DIRNAME / "latest.json"),
        "summary": {
            "candidates_covered": len(checklist),
            "approval_ready_after_data_validation": sum(row["approval_recommendation"] == "APPROVE_AFTER_DATA_VALIDATION" for row in checklist),
            "human_review_required": len(checklist),
            "authority": "paper-forward observation approval checklist only",
        },
        "candidate_checklists": checklist,
        "global_approval_gates": [
            "Confirm candidate-specific symbol or universe before observation.",
            "Confirm observation rule is unambiguous and measurable.",
            "Confirm no position size, capital allocation, broker instruction, or trade recommendation is present.",
            "Confirm paper-forward observation is human-approved and manually supervised.",
            "Confirm failure/invalidating conditions are recorded before observation starts.",
        ],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": _guardrails(),
    }


def write_candidate_data_validation_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Path]:
    report = build_candidate_data_validation_plan(root, created_at=created_at)
    return _write(report, Path(root) / DATA_PLAN_DIRNAME, "candidate_data_validation_plan.json", "candidate_data_validation_plan_summary.md", render_candidate_data_validation_plan_summary(report))


def write_paper_forward_approval_checklist(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Path]:
    report = build_paper_forward_approval_checklist(root, created_at=created_at)
    return _write(report, Path(root) / APPROVAL_CHECKLIST_DIRNAME, "paper_forward_approval_checklist.json", "paper_forward_approval_checklist_summary.md", render_paper_forward_approval_checklist_summary(report))


def render_candidate_data_validation_plan_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Candidate Data Validation Plan",
        "",
        f"Candidates covered: {report.get('summary', {}).get('candidates_covered')}",
        f"Primary blocker: {report.get('summary', {}).get('primary_blocker')}",
        "",
        "## Candidate Plans",
    ]
    for row in report.get("candidate_data_validation_plans", []):
        lines.append(f"- {row['candidate_id']} {row['mechanism']} / {row['regime']}: symbols={row['required_symbols_or_universe']}; timeframe={row['required_timeframe']}; min_sample={row['minimum_sample_size']}")
        lines.append(f"  blocker: {row['missing_data_blockers'][0] if row['missing_data_blockers'] else 'none'}")
    lines.extend(["", "Authority: data validation only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def render_paper_forward_approval_checklist_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Paper-Forward Observation Approval Checklist",
        "",
        f"Candidates covered: {report.get('summary', {}).get('candidates_covered')}",
        "",
        "## Candidate Checklist",
    ]
    for row in report.get("candidate_checklists", []):
        lines.append(f"- {row['candidate_id']}: {row['approval_recommendation']}")
        for item in row["checklist_items"][:5]:
            lines.append(f"  - {item['status']}: {item['item']}")
    lines.extend(["", "Authority: checklist only; no automatic paper placement, live trading, broker execution, capital allocation, or position sizing.", ""])
    return "\n".join(lines)


def _candidate_data_plan(candidate: dict[str, Any]) -> dict[str, Any]:
    candidate = apply_symbol_attribution(candidate)
    mechanism = str(candidate.get("mechanism") or "UNKNOWN").upper()
    regime = str(candidate.get("regime") or "UNKNOWN").upper()
    intraday_required = mechanism in {"OPENING_RANGE", "SESSION_TIMING", "VWAP_OR_AVERAGE_RECLAIM", "LIQUIDITY_SWEEP", "EVENT_REACTION"}
    attributed_symbols = list(candidate.get("candidate_symbols") or [])
    direct_universe = attributed_symbols or _symbol_requirement(mechanism)
    return {
        "candidate_id": candidate.get("candidate_id"),
        "campaign_rank": candidate.get("campaign_rank"),
        "mechanism": mechanism,
        "regime": regime,
        "final_score": candidate.get("final_score"),
        "expectancy": candidate.get("expectancy"),
        "profit_factor": candidate.get("profit_factor"),
        "sample_size": candidate.get("sample_size"),
        "max_drawdown": candidate.get("max_drawdown"),
        "required_symbols_or_universe": direct_universe,
        "candidate_symbols": attributed_symbols,
        "candidate_universe_symbols": list(candidate.get("candidate_universe_symbols") or []),
        "candidate_timeframes": list(candidate.get("candidate_timeframes") or []),
        "candidate_source_observation_ids": list(candidate.get("candidate_source_observation_ids") or []),
        "symbol_attribution_confidence": candidate.get("symbol_attribution_confidence", 0.0),
        "symbol_attribution_method": candidate.get("symbol_attribution_method", "UNKNOWN"),
        "direct_symbol_or_universe_required": True,
        "required_timeframe": "intraday plus daily confirmation" if intraday_required else "daily bars plus optional intraday confirmation",
        "intraday_required": intraday_required,
        "daily_data_required": True,
        "needed_indicators": _indicators(mechanism),
        "minimum_lookback": "5 years preferred; 2 years minimum for paper-forward validation context",
        "minimum_sample_size": max(100, int(candidate.get("sample_size") or 0)),
        "slippage_cost_assumptions_needed_later": [
            "Define only after human approval for paper-forward observation.",
            "Do not use for live trading, capital allocation, or position sizing.",
        ],
        "missing_data_blockers": _missing_data_blockers(attributed_symbols, intraday_required),
        "validation_steps": [
            "Resolve candidate-specific symbol or universe.",
            "Replay candidate on direct data with the same observation rule.",
            "Compare direct-data expectancy, profit factor, sample size, drawdown, and replay/backtest consistency against proxy result.",
            "Confirm no forbidden authority fields are introduced.",
        ],
    }


def _missing_data_blockers(attributed_symbols: list[str], intraday_required: bool) -> list[str]:
    blockers = []
    if not attributed_symbols:
        blockers.append("Candidate currently lacks direct symbol/universe attribution and uses SPY daily proxy evidence.")
    else:
        blockers.append("Candidate-specific symbols are attributed; confirm local direct CSV coverage before replay.")
    if intraday_required:
        blockers.append("Intraday event timing data required before observation rule can be considered fully validated.")
    return blockers


def _approval_checklist(plan: dict[str, Any]) -> dict[str, Any]:
    items = [
        ("Candidate-specific symbol/universe identified", "PENDING"),
        ("Direct-data replay completed", "PENDING"),
        ("Observation entry condition is measurable", "PENDING"),
        ("Observation exit condition is measurable", "PENDING"),
        ("Invalidation condition is explicit", "PENDING"),
        ("Minimum sample size target accepted", "PENDING"),
        ("Human reviewer confirms paper-forward only", "PENDING"),
        ("No live trading, capital, broker, or position-sizing language present", "PASS"),
    ]
    return {
        "candidate_id": plan["candidate_id"],
        "campaign_rank": plan.get("campaign_rank"),
        "mechanism": plan["mechanism"],
        "regime": plan["regime"],
        "approval_recommendation": "APPROVE_AFTER_DATA_VALIDATION",
        "human_review_required": True,
        "checklist_items": [{"item": item, "status": status} for item, status in items],
        "blocking_open_items": [item for item, status in items if status == "PENDING"],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def _symbol_requirement(mechanism: str) -> str:
    if mechanism in {"BREAKOUT", "MEAN_REVERSION", "REVERSAL"}:
        return "Candidate-specific equity/ETF universe used by the source observation; SPY proxy is insufficient for final validation."
    if mechanism == "EVENT_REACTION":
        return "Candidate-specific event universe plus benchmark ETF for context."
    return "Candidate-specific symbol or explicitly defined tradable universe."


def _indicators(mechanism: str) -> list[str]:
    base = ["open", "high", "low", "close", "volume", "regime label"]
    extra = {
        "BREAKOUT": ["prior range high/low", "volume expansion", "trend filter"],
        "MEAN_REVERSION": ["distance from moving average", "volatility band", "reversion horizon"],
        "EVENT_REACTION": ["event timestamp", "pre-event range", "post-event drift window"],
        "REVERSAL": ["exhaustion move", "reversal confirmation", "volatility context"],
    }
    return base + extra.get(mechanism, [])


def _write(report: dict[str, Any], root: Path, filename: str, summary_filename: str, summary: str) -> dict[str, Path]:
    day = str(report.get("day") or _today())
    out_dir = root / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / filename
    summary_path = out_dir / summary_filename
    latest_json = root / "latest.json"
    latest_summary = root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _guardrails() -> list[str]:
    return [
        "Paper-forward observation planning only.",
        "No live trading.",
        "No broker execution.",
        "No capital allocation.",
        "No position sizing.",
        "No automatic paper trade placement.",
        "No candidate production promotion.",
    ]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
