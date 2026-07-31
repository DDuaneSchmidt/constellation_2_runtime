from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.research_lab.research_console_v1 import research_console_v1
from ops.aegis.research_lab.research_run_ledger_v1 import read_research_run_ledger_v1
from ops.aegis.research_lab.research_store_reader import default_research_store_root


REPORT_FAMILY = "aegis_research_review_brief_v1"
REPORT_FILENAME = "research_review_brief.v1.json"
REQUIRED_BRIEF_FIELDS = {
    "conclusion",
    "confidence",
    "evidence_summary",
    "key_evidence",
    "risks",
    "decision_needed",
    "allowed_actions",
}
ALLOWED_ACTIONS = [
    "REVIEW_BRIEF",
    "MONITOR",
    "DISMISS",
    "ARCHIVE",
    "PROMOTE_TO_WATCHLIST",
    "REQUEST_MORE_RESEARCH",
    "OPEN_DIAGNOSTICS",
]


def research_review_brief_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def _latest_run_for_hypothesis(runs: list[dict[str, Any]], hypothesis_id: str) -> dict[str, Any]:
    matching = [row for row in runs if str(row.get("hypothesis_id") or "") == hypothesis_id]
    if not matching:
        return {}
    return sorted(
        matching,
        key=lambda row: (
            str(row.get("started_at") or ""),
            str(row.get("queued_at") or ""),
            str(row.get("requested_at") or ""),
            str(row.get("completed_at") or ""),
            str(row.get("research_run_id") or ""),
        ),
    )[-1]


def _confidence_for(row: dict[str, Any]) -> tuple[str, str]:
    status_text = " ".join(
        str(row.get(key) or "")
        for key in ["test_status", "latest_result", "data_status", "latest_result_summary"]
    ).upper()
    sample_size = row.get("sample_size")
    minimum = row.get("minimum_sample_size")
    if "INCONCLUSIVE" in status_text:
        return "LOW", f"Evidence is inconclusive; sample size is {sample_size or 0}/{minimum or 'required'}."
    if "NOT_IMPLEMENTED" in status_text:
        return "LOW", "Required research execution has not produced a completed study result yet."
    if "SUCCEEDED" in status_text or "READY" in status_text:
        return "MEDIUM", "Structured research output is available for operator review."
    return "LOW", "Aegis has enough structure to create a review brief, but confidence remains limited."


def _safe_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value in (None, ""):
        return []
    return [str(value)]


def _brief_status(brief: dict[str, Any]) -> tuple[str, list[str]]:
    missing: list[str] = []
    for field in REQUIRED_BRIEF_FIELDS:
        value = brief.get(field)
        if value in (None, "", [], {}):
            missing.append(field)
    return ("BLOCKED" if missing else str(brief.get("status") or "RECOMMENDATION_READY")), missing


def _latest_test_result_for_hypothesis(*, truth_root: Path, day_utc: str, hypothesis_id: str) -> dict[str, Any]:
    path = Path(truth_root).expanduser().resolve() / "reports" / "aegis_research_test_results_v1" / day_utc / hypothesis_id / "research_test_result.v1.json"
    return read_json_v1(path)


def _row_with_latest_test_result(row: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    if not result:
        return dict(row)
    merged = dict(row)
    status = str(result.get("test_status") or result.get("latest_result") or "")
    summary = str(result.get("result_summary") or "")
    if status:
        merged["test_status"] = status
        merged["latest_result"] = status
        merged["data_status"] = status
    if summary:
        merged["latest_result_summary"] = summary
        merged["result_summary"] = summary
        merged["user_facing_explanation"] = summary
    for key in ["sample_size", "minimum_sample_size", "required_symbols", "blocker"]:
        if result.get(key) is not None:
            merged[key] = result.get(key)
    if result.get("required_symbols") and not merged.get("symbols"):
        merged["symbols"] = result.get("required_symbols")
    merged["latest_research_test_result_path"] = str(Path("reports") / "aegis_research_test_results_v1" / str(result.get("day_utc") or "") / str(result.get("hypothesis_id") or "") / "research_test_result.v1.json")
    return merged


def _validation_ui_for(row: dict[str, Any]) -> dict[str, Any]:
    test_status = str(row.get("test_status") or row.get("latest_result") or row.get("data_status") or "").upper()
    blocker = str(row.get("blocker") or "").upper()
    if test_status == "INCONCLUSIVE_SAMPLE_SIZE" and blocker == "INSUFFICIENT_FORWARD_RETURN_SAMPLE":
        current = int(row.get("sample_size") or 0)
        required = int(row.get("minimum_sample_size") or 20)
        missing = max(0, required - current)
        return {
            "state_label": "Collecting Evidence",
            "classification": "MORE_RESEARCH_REQUIRED",
            "message": "Research is active. Aegis needs more forward-return observations before this hypothesis can qualify for paper validation.",
            "required_samples": required,
            "current_samples": current,
            "missing_samples": missing,
            "sample_count_label": f"{current}/{required}",
            "next_sample_expected_at": "next market close",
            "estimated_completion_date": f"after {missing} valid observations" if missing else "complete after current observation set",
            "expected_trading_days_remaining": missing,
            "next_action": "Collect forward-return observations",
            "operator_action_required": False,
            "paper_testing_sleeve": "Not created yet",
            "reason": "Qualification requires more evidence",
            "blocker": "INSUFFICIENT_FORWARD_RETURN_SAMPLE",
        }
    return {}


def _review_brief_for(row: dict[str, Any], *, latest_run: dict[str, Any], generated_at: str) -> dict[str, Any]:
    hypothesis_id = str(row.get("hypothesis_id") or row.get("hypothesis_proposal_id") or row.get("item_id") or "")
    title = str(row.get("title") or hypothesis_id or "Research finding")
    symbols = _safe_list(row.get("symbols") or row.get("required_symbols") or row.get("related_symbols"))
    latest_summary = str(row.get("latest_result_summary") or row.get("result_summary") or row.get("user_facing_explanation") or "").strip()
    test_status = str(row.get("test_status") or row.get("latest_result") or row.get("data_status") or "").strip()
    validation_ui = _validation_ui_for(row)
    confidence, confidence_reason = _confidence_for(row)
    conclusion = latest_summary or f"{title} produced a research finding that requires operator review."
    if test_status and test_status not in conclusion:
        evidence_summary = f"{test_status}: {latest_summary or row.get('next_action') or 'Research finding is ready for review.'}"
    else:
        evidence_summary = latest_summary or str(row.get("next_action") or "Research finding is ready for review.")
    why_it_matters = (
        f"This finding may affect monitoring decisions for {', '.join(symbols)}."
        if symbols
        else "This finding may affect research monitoring decisions."
    )
    key_evidence = [
        item
        for item in [
            latest_summary,
            f"Result status: {test_status}" if test_status else "",
            f"Sample size: {row.get('sample_size')}/{row.get('minimum_sample_size')}" if row.get("sample_size") is not None or row.get("minimum_sample_size") is not None else "",
        ]
        if item
    ]
    risks = [
        "Research findings are evidence for operator judgment, not trade advice.",
        "Open or incomplete sample evidence may not generalize.",
    ]
    if "INCONCLUSIVE" in test_status.upper():
        risks.append("Sample size is below the configured minimum.")
    if "NOT_IMPLEMENTED" in test_status.upper():
        risks.append("The research runner has not produced a completed test result.")
    decision_needed = "Monitor, dismiss, archive, promote to watchlist, or request more research."
    allowed_actions = list(ALLOWED_ACTIONS)
    if validation_ui:
        decision_needed = "No operator action required. Aegis is collecting forward-return observations."
        allowed_actions = ["OPEN_DIAGNOSTICS"]
    brief = {
        "hypothesis_id": hypothesis_id,
        "research_run_id": str(latest_run.get("research_run_id") or "NO_RESEARCH_RUN_RECORDED"),
        "title": title,
        "status": "RECOMMENDATION_READY",
        "conclusion": conclusion,
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "why_it_matters": why_it_matters,
        "affected_symbols": symbols,
        "evidence_summary": evidence_summary,
        "key_evidence": key_evidence,
        "counter_evidence": [],
        "risks": risks,
        "decision_needed": decision_needed,
        "allowed_actions": allowed_actions,
        "validation_ui": validation_ui,
        "staleness": {
            "status": "CURRENT",
            "as_of": str(row.get("updated_at") or generated_at),
        },
        "source_artifacts": [
            {
                "artifact_id": "research_console_v1",
                "description": "Research console normalized hypothesis row.",
                "hypothesis_id": hypothesis_id,
            },
            {
                "artifact_id": "research_run_ledger.v1",
                "description": "Latest research run ledger entry if present.",
                "research_run_id": str(latest_run.get("research_run_id") or ""),
            },
        ],
        "generated_at": generated_at,
        "as_of": str(row.get("updated_at") or generated_at),
        "diagnostics": {
            "raw_status": str(row.get("user_facing_status") or ""),
            "raw_result_status": test_status,
            "operator_action_required": bool(validation_ui.get("operator_action_required")) if validation_ui else bool(row.get("operator_action_required")),
        },
    }
    status, missing = _brief_status(brief)
    brief["status"] = status
    if missing:
        brief["blocked_reason"] = "Review brief is missing required structured fields."
        brief["missing_required_fields"] = missing
    return brief


def build_research_review_brief_v1(
    *,
    truth_root: Path,
    day_utc: str,
    store_root: Path | None = None,
) -> dict[str, Any]:
    generated_at = now_utc_v1()
    store = store_root or default_research_store_root()
    console = research_console_v1(store_root=store)
    rows = [row for row in console.get("all_hypotheses") or [] if str(row.get("user_facing_status") or "") in {"Recommendation Ready", "Collecting Evidence"}]
    runs = read_research_run_ledger_v1(store_root=store)
    enriched_rows = []
    for row in rows:
        hypothesis_id = str(row.get("hypothesis_id") or "")
        latest_result = _latest_test_result_for_hypothesis(truth_root=truth_root, day_utc=day_utc, hypothesis_id=hypothesis_id)
        enriched_rows.append(_row_with_latest_test_result(row, latest_result))
    briefs = [
        _review_brief_for(row, latest_run=_latest_run_for_hypothesis(runs, str(row.get("hypothesis_id") or "")), generated_at=generated_at)
        for row in enriched_rows
    ]
    counts = Counter(str(brief.get("status") or "UNKNOWN") for brief in briefs)
    diagnostics = []
    for brief in briefs:
        if brief.get("status") == "BLOCKED":
            diagnostics.append(
                {
                    "hypothesis_id": brief.get("hypothesis_id"),
                    "message": brief.get("blocked_reason"),
                    "missing_required_fields": brief.get("missing_required_fields") or [],
                }
            )
    status = "CANONICAL"
    if diagnostics:
        status = "PARTIAL"
    payload = {
        "schema_id": "aegis_research_review_brief",
        "schema_version": "v1",
        "artifact_id": "aegis_research_review_brief_v1",
        "day_utc": day_utc,
        "as_of": generated_at,
        "generated_at": generated_at,
        "status": status,
        "summary": {
            "recommendation_ready_count": len(rows),
            "review_brief_count": len(briefs),
            "blocked_brief_count": counts.get("BLOCKED", 0),
            "stale_brief_count": counts.get("STALE", 0),
            "operator_action_required_count": sum(1 for brief in briefs if (brief.get("diagnostics") or {}).get("operator_action_required")),
            "collecting_evidence_count": sum(1 for brief in briefs if (brief.get("validation_ui") or {}).get("state_label") == "Collecting Evidence"),
        },
        "briefs": briefs,
        "source_artifacts": [
            {
                "artifact_id": "research_console_v1",
                "row_count": len(console.get("all_hypotheses") or []),
                "freshness_status": "CURRENT",
            },
            {
                "artifact_id": "research_run_ledger.v1",
                "row_count": len(runs),
                "freshness_status": "CURRENT",
            },
        ],
        "diagnostics": diagnostics,
        "safety": {
            "research_only": True,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_order_submission_allowed": False,
            "live_trading_allowed": False,
            "autonomous_trading_allowed": False,
        },
    }
    return payload


def write_research_review_brief_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(research_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def load_research_review_brief_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return read_json_v1(research_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc))


def self_check_research_review_brief_v1(*, truth_root: Path, day_utc: str, store_root: Path | None = None) -> dict[str, Any]:
    payload = load_research_review_brief_v1(truth_root=truth_root, day_utc=day_utc)
    console = research_console_v1(store_root=store_root or default_research_store_root())
    rec_ready_ids = sorted(
        str(row.get("hypothesis_id") or "")
        for row in console.get("all_hypotheses") or []
        if str(row.get("user_facing_status") or "") == "Recommendation Ready"
    )
    briefs = payload.get("briefs") if isinstance(payload.get("briefs"), list) else []
    brief_by_id = {str(brief.get("hypothesis_id") or ""): brief for brief in briefs}
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, details: dict[str, Any] | None = None) -> None:
        checks.append({"check": name, "ok": bool(ok), "details": details or {}})

    add_check("artifact exists", bool(payload), {"path": str(research_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc))})
    missing_briefs = [hypothesis_id for hypothesis_id in rec_ready_ids if hypothesis_id not in brief_by_id]
    add_check("every Recommendation Ready hypothesis has a review brief", not missing_briefs, {"missing_hypothesis_ids": missing_briefs})
    for field in ["conclusion", "confidence", "evidence_summary", "decision_needed"]:
        missing = [hypothesis_id for hypothesis_id, brief in brief_by_id.items() if not brief.get(field)]
        add_check(f"review brief includes {field}", not missing, {"missing_hypothesis_ids": missing})
    missing_evidence = [hypothesis_id for hypothesis_id, brief in brief_by_id.items() if not brief.get("key_evidence")]
    add_check("review brief includes key evidence", not missing_evidence, {"missing_hypothesis_ids": missing_evidence})
    stale_unmarked = [
        hypothesis_id
        for hypothesis_id, brief in brief_by_id.items()
        if str((brief.get("staleness") or {}).get("status") or "") == "STALE" and brief.get("status") != "STALE"
    ]
    add_check("stale findings are marked stale", not stale_unmarked, {"hypothesis_ids": stale_unmarked})
    ui_source = (Path("constellation_2/phaseL/ui/static/operator_shell/pages/index.js")).read_text(encoding="utf-8")
    add_check("UI exposes Recommendation Ready with Review Brief content", "renderResearchReviewPage" in ui_source and "fetchResearchReviewBrief" in ui_source)
    ok = all(check["ok"] for check in checks)
    return {
        "ok": ok,
        "day_utc": day_utc,
        "artifact_path": str(research_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc)),
        "recommendation_ready_count": len(rec_ready_ids),
        "review_brief_count": len(briefs),
        "checks": checks,
    }
