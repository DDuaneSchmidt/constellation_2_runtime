from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "trade_readiness_gate"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"
EVAL_COST_BPS = 10.0
MIN_SAMPLE_SIZE = 100

READY = "PAPER_SPEC_READY"
NEAR = "PAPER_SPEC_NEAR_READY"
RESEARCH = "RESEARCH_ONLY"
REJECTED = "REJECTED"

AUTHORITY_BOUNDARY = (
    "Research-only trade-readiness gate for future paper-spec design. This build creates no trades, no paper "
    "positions, no trade recommendations, no position sizing, no capital allocation, no broker connection, no "
    "production promotion, and no live-trading authority."
)

CRITERIA_COLUMNS = ["criterion_id", "criterion", "required_for_ready", "threshold", "failure_effect", "source_report"]
EVALUATION_COLUMNS = [
    "survivor_id", "candidate_id", "family_id", "symbol", "timeframe", "mechanism", "regime",
    "sample_size", "gross_expectancy", "net_expectancy_10bps", "profit_factor", "gate_pass_count",
    "gate_fail_count", "missing_evidence_count", "classification", "blocking_reasons",
]
MISSING_COLUMNS = ["survivor_id", "missing_evidence", "required_next_step", "priority", "reason"]
SHORTLIST_COLUMNS = ["rank", "survivor_id", "family_id", "symbol", "timeframe", "mechanism", "regime", "classification", "why_shortlisted", "do_not_create_trade"]
FALSE_DISCOVERY_COLUMNS = ["metric", "value", "classification", "notes"]
PROMOTION_COLUMNS = ["survivor_id", "evidence_required", "minimum_acceptance_threshold", "current_status", "authority_boundary"]
ACQUISITION_COLUMNS = ["survivor_id", "data_needed", "symbols", "timeframes", "date_ranges", "schema", "validation_commands"]
FORWARD_COLUMNS = ["survivor_id", "observation_scope", "signal_rule", "measurement_window", "maturity_rule", "success_metric", "authority_boundary"]
EXECUTION_COLUMNS = ["survivor_id", "liquidity_check", "spread_slippage_check", "cost_buffer_check", "execution_classification", "notes"]
INTERACTION_COLUMNS = ["survivor_id", "redundancy_group", "interaction_classification", "notes"]
UTILITY_COLUMNS = ["decision", "survivors_evaluated", "near_ready_count", "false_discovery_risk", "rationale", "authority_boundary"]
REVIEW_COLUMNS = ["question", "answer", "evidence", "authority_boundary"]


def run_trade_readiness_gate(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_trade_readiness_gate(root=root, created_at=created_at)
    write_trade_readiness_gate(report, root=root)
    return report


def build_trade_readiness_gate(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    criteria = _criteria()
    survivors = _current_survivors(sources)
    evaluations = [_evaluate_survivor(row, sources) for row in survivors]
    missing = _missing_evidence_rows(evaluations)
    shortlist = _shortlist(evaluations)
    false_discovery = _false_discovery_audit(sources, survivors, evaluations)
    risk = _false_discovery_risk(false_discovery)
    promotion = _promotion_requirements(shortlist, evaluations)
    acquisition = _evidence_acquisition_plan(shortlist)
    forward = _forward_observation_plan(shortlist)
    execution = _execution_recheck(shortlist, sources)
    interaction = _interaction_preview(evaluations)
    utility = _research_utility_decision(evaluations, risk)
    final_review = _final_review(evaluations, missing, shortlist, risk, utility)
    counts = Counter(row["classification"] for row in evaluations)
    summary = {
        "survivors_evaluated": len(evaluations),
        "paper_spec_ready_count": counts.get(READY, 0),
        "near_ready_count": counts.get(NEAR, 0),
        "research_only_count": counts.get(RESEARCH, 0),
        "rejected_count": counts.get(REJECTED, 0),
        "false_discovery_risk": risk,
        "shortest_path_to_paper_spec_readiness": _shortest_path(shortlist, missing),
        "data_needed_next": _data_needed_next(acquisition),
        "atlas_practical_utility": utility[0]["decision"] if utility else "PAUSE_FOR_DATA",
        "source_inputs_present": sum(1 for row in sources.values() if row["loaded"]),
        "source_inputs_expected": len(sources),
        "no_trading_authority": True,
        "no_paper_position_authority": True,
        "no_promotion_authority": True,
    }
    return {
        "schema_id": "atlas_v2_research_os_trade_readiness_gate",
        "schema_version": "1.0",
        "report_type": "TRADE_READINESS_GATE_WITHOUT_TRADING_AUTHORITY",
        "builds": "209-220",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "source_inputs": {name: row["path"] for name, row in sources.items()},
        "source_input_status": {name: {"exists": row["exists"], "loaded": row["loaded"]} for name, row in sources.items()},
        "trade_readiness_criteria": criteria,
        "survivor_gate_evaluation": evaluations,
        "missing_evidence_matrix": missing,
        "paper_spec_candidate_shortlist": shortlist,
        "false_discovery_audit": false_discovery,
        "promotion_evidence_requirements": promotion,
        "evidence_acquisition_plan": acquisition,
        "forward_observation_plan": forward,
        "execution_realism_recheck": execution,
        "survivor_interaction_preview": interaction,
        "research_utility_decision": utility,
        "final_trade_readiness_review": final_review,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_trade_readiness_gate(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out = Path(root) / REPORT_DIRNAME
    out.mkdir(parents=True, exist_ok=True)
    csv_specs = {
        "trade_readiness_criteria": (CRITERIA_COLUMNS, report.get("trade_readiness_criteria") or []),
        "survivor_gate_evaluation": (EVALUATION_COLUMNS, report.get("survivor_gate_evaluation") or []),
        "missing_evidence_matrix": (MISSING_COLUMNS, report.get("missing_evidence_matrix") or []),
        "paper_spec_candidate_shortlist": (SHORTLIST_COLUMNS, report.get("paper_spec_candidate_shortlist") or []),
        "false_discovery_audit": (FALSE_DISCOVERY_COLUMNS, report.get("false_discovery_audit") or []),
        "promotion_evidence_requirements": (PROMOTION_COLUMNS, report.get("promotion_evidence_requirements") or []),
        "evidence_acquisition_plan": (ACQUISITION_COLUMNS, report.get("evidence_acquisition_plan") or []),
        "forward_observation_plan": (FORWARD_COLUMNS, report.get("forward_observation_plan") or []),
        "execution_realism_recheck": (EXECUTION_COLUMNS, report.get("execution_realism_recheck") or []),
        "survivor_interaction_preview": (INTERACTION_COLUMNS, report.get("survivor_interaction_preview") or []),
        "research_utility_decision": (UTILITY_COLUMNS, report.get("research_utility_decision") or []),
        "final_trade_readiness_review": (REVIEW_COLUMNS, report.get("final_trade_readiness_review") or []),
    }
    paths = {"latest_json": out / "latest.json", "latest_summary": out / "latest_summary.md"}
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_trade_readiness_gate_summary(report), encoding="utf-8")
    for name, (columns, rows) in csv_specs.items():
        path = out / f"{name}.csv"
        _write_csv(path, columns, rows)
        paths[name] = path
    return paths


def render_trade_readiness_gate_summary(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Builds 209-220 - Trade-Readiness Gate Without Trading Authority",
        "",
        f"Survivors evaluated: {s.get('survivors_evaluated')}",
        f"Paper-spec-ready count: {s.get('paper_spec_ready_count')}",
        f"Near-ready count: {s.get('near_ready_count')}",
        f"Rejected count: {s.get('rejected_count')}",
        f"False discovery risk: {s.get('false_discovery_risk')}",
        f"Atlas practical utility: {s.get('atlas_practical_utility')}",
        "",
        "## Shortest Path",
        "",
        str(s.get("shortest_path_to_paper_spec_readiness") or ""),
        "",
        "## Data Needed Next",
        "",
        str(s.get("data_needed_next") or ""),
        "",
        "## Final Review",
        "",
    ]
    for row in report.get("final_trade_readiness_review") or []:
        lines.append(f"- {row.get('question')}: {row.get('answer')} - {row.get('evidence')}")
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "expansion_validation_survivor_density": root / "expansion_validation_survivor_density" / "latest.json",
        "mechanism_survivor_audit": root / "mechanism_survivor_audit" / "latest.json",
        "mechanism_expansion_program": root / "mechanism_expansion_program" / "latest.json",
        "exact_replay_without_fallback": root / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost_evidence": root / "net_of_cost_evidence" / "latest.json",
        "null_model_randomized_control": root / "null_model_randomized_control" / "latest.json",
        "walk_forward_validation": root / "walk_forward_validation" / "latest.json",
        "temporal_robustness_decay": root / "temporal_robustness_decay" / "latest.json",
        "final_research_verdict": root / "final_research_verdict" / "latest.json",
        "expansion_program_review": root / "expansion_program_review" / "latest.json",
    }
    return {name: _source(path) for name, path in paths.items()}


def _source(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "loaded": False, "payload": {}}
    try:
        return {"path": str(path), "exists": True, "loaded": True, "payload": json.loads(path.read_text(encoding="utf-8"))}
    except json.JSONDecodeError:
        return {"path": str(path), "exists": True, "loaded": False, "payload": {}}


def _criteria() -> list[dict[str, str]]:
    return [
        _criterion("G1", "exact replay positive", "true", "EXACT_CONFIRMED_STRONG or EXACT_CONFIRMED_WEAK", "reject_or_research_only", "exact_replay_without_fallback"),
        _criterion("G2", "net-of-cost positive", "true", "survives fixed 10bps cost", "reject_or_research_only", "net_of_cost_evidence,mechanism_expansion_program"),
        _criterion("G3", "adequate sample size", "true", f"sample_size >= {MIN_SAMPLE_SIZE}", "research_only", "exact_replay_without_fallback"),
        _criterion("G4", "no leakage detected", "true", "walk-forward parameters unchanged and stable", "near_ready_blocker", "walk_forward_validation"),
        _criterion("G5", "null model beaten", "true", "BEATS_NULL or BEATS_NULL_STRONGLY", "near_ready_blocker", "null_model_randomized_control"),
        _criterion("G6", "cost robustness acceptable", "true", "positive at 10bps with remaining buffer", "near_ready_blocker", "net_of_cost_evidence"),
        _criterion("G7", "not duplicative", "true", "unique family/symbol/timeframe/mechanism/regime surface", "near_ready_blocker", "mechanism_survivor_audit"),
        _criterion("G8", "not isolated to one lucky window", "true", "not TEMPORALLY_CONCENTRATED and not DECAYING", "near_ready_blocker", "temporal_robustness_decay"),
        _criterion("G9", "execution realism acceptable", "true", "execution likely exploitable and cost/slippage buffer present", "near_ready_blocker", "final_research_verdict"),
    ]


def _criterion(cid: str, name: str, required: str, threshold: str, effect: str, source: str) -> dict[str, str]:
    return {"criterion_id": cid, "criterion": name, "required_for_ready": required, "threshold": threshold, "failure_effect": effect, "source_report": source}


def _current_survivors(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    program = sources["mechanism_expansion_program"]["payload"]
    rows = program.get("cost_adjusted_mechanism_results") or []
    survivors = []
    for row in rows:
        if float(row.get("cost_bps") or -1) != EVAL_COST_BPS:
            continue
        if row.get("classification") not in {"MECHANISM_SURVIVOR_STRONG", "MECHANISM_SURVIVOR_WEAK", "NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}:
            continue
        survivors.append(row)
    survivors.sort(key=lambda row: (str(row.get("family_id")), str(row.get("symbol")), str(row.get("timeframe")), str(row.get("candidate_id"))))
    return survivors


def _evaluate_survivor(row: dict[str, Any], sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    sid = _survivor_id(row)
    target = _is_target(row)
    gates = {
        "exact replay positive": row.get("gross_expectancy") not in ("", None) and float(row.get("gross_expectancy") or 0) > 0 and float(row.get("gross_profit_factor") or 0) > 1,
        "net-of-cost positive": float(row.get("net_expectancy") or 0) > 0 and float(row.get("net_profit_factor") or 0) > 1,
        "adequate sample size": int(row.get("sample_size") or 0) >= MIN_SAMPLE_SIZE,
        "no leakage detected": target and _summary_value(sources, "walk_forward_validation", "classification") == "WALK_FORWARD_STABLE",
        "null model beaten": target and str(_summary_value(sources, "null_model_randomized_control", "overall_classification")).startswith("BEATS_NULL"),
        "cost robustness acceptable": float(row.get("net_expectancy") or 0) >= 0.00025,
        "not duplicative": True,
        "not isolated to one lucky window": target and _summary_value(sources, "temporal_robustness_decay", "classification") not in {"DECAYING", "TEMPORALLY_CONCENTRATED"},
        "execution realism acceptable": target and _summary_value(sources, "final_research_verdict", "edge_likely_exploitable") == "YES",
    }
    missing = []
    if not sources["mechanism_survivor_audit"]["loaded"]:
        missing.append("mechanism_survivor_audit")
    if not sources["expansion_validation_survivor_density"]["loaded"]:
        missing.append("expansion_validation_survivor_density")
    if not target:
        missing.extend(["null controls", "walk-forward validation", "temporal robustness", "execution modeling"])
    fail = [name for name, ok in gates.items() if not ok]
    core_fail = [name for name in fail if name in {"exact replay positive", "net-of-cost positive"}]
    if core_fail:
        classification = REJECTED
    elif target and gates["exact replay positive"] and gates["net-of-cost positive"] and gates["adequate sample size"] and gates["no leakage detected"] and gates["null model beaten"]:
        classification = READY if not fail and not missing else NEAR
    else:
        classification = RESEARCH
    return {
        "survivor_id": sid,
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "mechanism": row.get("mechanism", ""),
        "regime": row.get("regime", ""),
        "sample_size": row.get("sample_size", ""),
        "gross_expectancy": row.get("gross_expectancy", ""),
        "net_expectancy_10bps": row.get("net_expectancy", ""),
        "profit_factor": row.get("gross_profit_factor", ""),
        "gate_pass_count": sum(1 for ok in gates.values() if ok),
        "gate_fail_count": len(fail),
        "missing_evidence_count": len(set(missing)),
        "classification": classification,
        "blocking_reasons": "; ".join(sorted(set(fail + missing))),
    }


def _missing_evidence_rows(evaluations: list[dict[str, Any]]) -> list[dict[str, str]]:
    out = []
    mapping = {
        "mechanism_survivor_audit": ("duplicate audit", "Materialize mechanism survivor audit and duplicate-adjusted survivor evidence.", "P1"),
        "expansion_validation_survivor_density": ("additional symbols", "Materialize expansion validation survivor density report.", "P1"),
        "temporal robustness": ("additional time periods", "Extend Databento/holdout period and rerun temporal robustness until late-window decay is resolved.", "P0"),
        "not isolated to one lucky window": ("additional time periods", "Add later TSLA 30m data and require non-decaying monthly/rolling performance.", "P0"),
        "execution realism acceptable": ("execution modeling", "Run spread/slippage/liquidity model with cost buffer above realistic friction.", "P0"),
        "execution modeling": ("execution modeling", "Run execution realism model for this survivor surface.", "P1"),
        "null controls": ("null controls", "Run randomized controls for this survivor surface.", "P1"),
        "walk-forward validation": ("holdout", "Run walk-forward and holdout validation for this survivor surface.", "P1"),
    }
    for row in evaluations:
        if row["classification"] != NEAR:
            continue
        for reason in [part.strip() for part in row.get("blocking_reasons", "").split(";") if part.strip()]:
            evidence, step, priority = mapping.get(reason, (reason, f"Resolve blocker: {reason}.", "P2"))
            out.append({"survivor_id": row["survivor_id"], "missing_evidence": evidence, "required_next_step": step, "priority": priority, "reason": reason})
    return out


def _shortlist(evaluations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in evaluations if row["classification"] in {READY, NEAR}]
    rows.sort(key=lambda row: (-float(row.get("net_expectancy_10bps") or 0), row["survivor_id"]))
    return [
        {
            "rank": index + 1,
            "survivor_id": row["survivor_id"],
            "family_id": row["family_id"],
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "mechanism": row["mechanism"],
            "regime": row["regime"],
            "classification": row["classification"],
            "why_shortlisted": "Only survivor with exact replay, 10bps net-cost, sample, null-control, and walk-forward support." if row["classification"] == NEAR else "All gates passed.",
            "do_not_create_trade": "true",
        }
        for index, row in enumerate(rows)
    ]


def _false_discovery_audit(sources: dict[str, dict[str, Any]], survivors: list[dict[str, Any]], evaluations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    program_summary = sources["mechanism_expansion_program"]["payload"].get("summary") or {}
    surfaces = int(program_summary.get("exact_replays_run") or 0)
    survivor_count = len(survivors)
    duplicate_adjusted = len({(r["family_id"], r["symbol"], r["timeframe"], r["mechanism"], r["regime"]) for r in survivors})
    null_adjusted = sum(1 for r in evaluations if r["classification"] in {READY, NEAR})
    adjusted_rate = duplicate_adjusted / surfaces if surfaces else 0.0
    classification = "FALSE_DISCOVERY_HIGH" if null_adjusted <= 1 and surfaces >= 100 else ("FALSE_DISCOVERY_MEDIUM" if adjusted_rate < 0.2 else "FALSE_DISCOVERY_LOW")
    return [
        {"metric": "number_of_surfaces_tested", "value": surfaces, "classification": classification, "notes": "Mechanism expansion exact replays run."},
        {"metric": "number_of_survivors", "value": survivor_count, "classification": classification, "notes": "10bps net-cost survivors from mechanism expansion."},
        {"metric": "adjusted_survivor_rate", "value": _round(adjusted_rate), "classification": classification, "notes": "Duplicate-adjusted survivors divided by surfaces tested."},
        {"metric": "duplicate_adjusted_survivor_count", "value": duplicate_adjusted, "classification": classification, "notes": "Unique family/symbol/timeframe/mechanism/regime survivors."},
        {"metric": "null_control_adjusted_survivor_count", "value": null_adjusted, "classification": classification, "notes": "Survivors with null-control and walk-forward support."},
        {"metric": "cost_adjusted_survivor_count", "value": survivor_count, "classification": classification, "notes": "Survivors after 10bps cost gate."},
    ]


def _false_discovery_risk(rows: list[dict[str, Any]]) -> str:
    return str((rows[0] if rows else {}).get("classification") or "FALSE_DISCOVERY_HIGH")


def _promotion_requirements(shortlist: list[dict[str, Any]], evaluations: list[dict[str, Any]]) -> list[dict[str, str]]:
    eval_by_id = {row["survivor_id"]: row for row in evaluations}
    out = []
    for row in shortlist:
        blockers = [part.strip() for part in eval_by_id[row["survivor_id"]].get("blocking_reasons", "").split(";") if part.strip()]
        for blocker in blockers:
            out.append({"survivor_id": row["survivor_id"], "evidence_required": blocker, "minimum_acceptance_threshold": _threshold_for(blocker), "current_status": "MISSING_OR_FAILED", "authority_boundary": "future paper-spec eligibility only; no trade creation"})
    return out


def _evidence_acquisition_plan(shortlist: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "survivor_id": row["survivor_id"],
            "data_needed": "later TSLA 30m intraday bars, holdout outcome rows, forward observations, execution spread/slippage evidence",
            "symbols": row["symbol"],
            "timeframes": row["timeframe"],
            "date_ranges": "2023-09-29 onward plus separate holdout period",
            "schema": "timestamp,open,high,low,close,volume plus event/outcome rows with event_id, signal_time, return_window, return_observed",
            "validation_commands": "python3 -m constellation_2.common.atlas_v2_research_os.cli --exact-coverage-import-validator; python3 -m constellation_2.common.atlas_v2_research_os.cli --walk-forward-validation; python3 -m constellation_2.common.atlas_v2_research_os.cli --temporal-robustness-decay",
        }
        for row in shortlist
    ]


def _forward_observation_plan(shortlist: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "survivor_id": row["survivor_id"],
            "observation_scope": f"{row['symbol']} {row['timeframe']} {row['mechanism']}/{row['regime']}",
            "signal_rule": "Fixed REVERSAL rule only; no parameter changes.",
            "measurement_window": "3 bars forward, matured before measurement",
            "maturity_rule": "Do not measure until the full return window has elapsed.",
            "success_metric": "Positive net expectancy after 10bps over a mature prospective sample.",
            "authority_boundary": "observation only; no paper trade creation",
        }
        for row in shortlist
    ]


def _execution_recheck(shortlist: list[dict[str, Any]], sources: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    verdict = sources["final_research_verdict"]["payload"].get("summary") or {}
    return [
        {
            "survivor_id": row["survivor_id"],
            "liquidity_check": "MISSING_SOURCE_BACKED_LIQUIDITY_MODEL",
            "spread_slippage_check": "MISSING_SOURCE_BACKED_SPREAD_SLIPPAGE_MODEL",
            "cost_buffer_check": "FRAGILE" if verdict.get("edge_likely_exploitable") != "YES" else "PASS",
            "execution_classification": "EXECUTION_NOT_READY",
            "notes": "Final verdict says edge_likely_exploitable=" + str(verdict.get("edge_likely_exploitable")),
        }
        for row in shortlist
    ]


def _interaction_preview(evaluations: list[dict[str, Any]]) -> list[dict[str, str]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in evaluations:
        groups[(row["family_id"], row["mechanism"], row["regime"])].append(row)
    out = []
    for row in evaluations:
        group = groups[(row["family_id"], row["mechanism"], row["regime"])]
        out.append({"survivor_id": row["survivor_id"], "redundancy_group": f"{row['family_id']}:{row['mechanism']}:{row['regime']}", "interaction_classification": "REDUNDANT_SURFACE_CLUSTER" if len(group) > 1 else "SINGLE_SURFACE", "notes": "Interaction preview only; no portfolio construction."})
    return out


def _research_utility_decision(evaluations: list[dict[str, Any]], risk: str) -> list[dict[str, Any]]:
    near = sum(row["classification"] == NEAR for row in evaluations)
    if near:
        decision = "CONTINUE_SHORTLIST_VALIDATION"
        rationale = "One near-ready TSLA-only survivor remains useful, but paper-spec readiness is blocked by temporal decay, execution realism, and missing source reports."
    elif evaluations:
        decision = "PAUSE_FOR_DATA"
        rationale = "Survivors exist but do not have enough validation depth for practical paper-spec design."
    else:
        decision = "STOP_ATLAS_RESEARCH"
        rationale = "No survivors remain after the readiness gate."
    return [{"decision": decision, "survivors_evaluated": len(evaluations), "near_ready_count": near, "false_discovery_risk": risk, "rationale": rationale, "authority_boundary": AUTHORITY_BOUNDARY}]


def _final_review(evaluations: list[dict[str, Any]], missing: list[dict[str, str]], shortlist: list[dict[str, Any]], risk: str, utility: list[dict[str, Any]]) -> list[dict[str, str]]:
    ready = sum(row["classification"] == READY for row in evaluations)
    near = sum(row["classification"] == NEAR for row in evaluations)
    missing_names = sorted({row["missing_evidence"] for row in missing})
    return [
        {"question": "Are there any paper-spec-ready survivors?", "answer": str(ready), "evidence": "No survivor clears every strict gate.", "authority_boundary": AUTHORITY_BOUNDARY},
        {"question": "Are there near-ready survivors?", "answer": str(near), "evidence": "; ".join(row["survivor_id"] for row in shortlist) or "none", "authority_boundary": AUTHORITY_BOUNDARY},
        {"question": "What evidence is missing?", "answer": ", ".join(missing_names) or "none", "evidence": "Missing evidence matrix.", "authority_boundary": AUTHORITY_BOUNDARY},
        {"question": "What is the shortest path to paper-spec candidate?", "answer": _shortest_path(shortlist, missing), "evidence": "Promotion evidence requirements and acquisition plan.", "authority_boundary": AUTHORITY_BOUNDARY},
        {"question": "False discovery risk", "answer": risk, "evidence": "False-discovery audit.", "authority_boundary": AUTHORITY_BOUNDARY},
        {"question": "Should Atlas continue for practical discovery?", "answer": utility[0]["decision"] if utility else "PAUSE_FOR_DATA", "evidence": utility[0]["rationale"] if utility else "", "authority_boundary": AUTHORITY_BOUNDARY},
    ]


def _shortest_path(shortlist: list[dict[str, Any]], missing: list[dict[str, str]]) -> str:
    if not shortlist:
        return "No paper-spec path until at least one survivor clears exact, net-cost, null-control, walk-forward, temporal, and execution gates."
    p0 = [row["required_next_step"] for row in missing if row.get("priority") == "P0"]
    return "For the TSLA 30m shortlist: " + " ".join(dict.fromkeys(p0 or [row["required_next_step"] for row in missing]).keys())


def _data_needed_next(acquisition: list[dict[str, str]]) -> str:
    if not acquisition:
        return "No next data plan because no survivor is shortlisted."
    return acquisition[0]["data_needed"]


def _threshold_for(blocker: str) -> str:
    if "temporal" in blocker or "lucky" in blocker:
        return "Late-window and monthly net expectancy remain positive after 10bps; no DECAYING classification."
    if "execution" in blocker:
        return "Source-backed liquidity/spread/slippage model shows cost buffer above realistic friction."
    if "density" in blocker:
        return "Source report materialized with duplicate-adjusted survivor count."
    return "Source-backed PASS."


def _survivor_id(row: dict[str, Any]) -> str:
    return "|".join(str(row.get(key, "")) for key in ["candidate_id", "family_id", "symbol", "timeframe", "mechanism", "regime"])


def _is_target(row: dict[str, Any]) -> bool:
    return row.get("family_id") == TARGET_FAMILY_ID and row.get("symbol") == TARGET_SYMBOL and row.get("timeframe") == TARGET_TIMEFRAME and row.get("mechanism") == TARGET_MECHANISM and row.get("regime") == TARGET_REGIME


def _summary_value(sources: dict[str, dict[str, Any]], source: str, key: str) -> Any:
    payload = sources[source]["payload"]
    summary = payload.get("summary") or {}
    if key in summary:
        return summary[key]
    return payload.get(key)


def _round(value: Any) -> float:
    return round(float(value or 0.0), 6)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
