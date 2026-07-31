from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.research_lab.research_review_brief_v1 import (
    load_research_review_brief_v1,
    research_review_brief_path_v1,
)
from ops.aegis.research_lab.research_console_v1 import research_console_v1
from ops.aegis.research_lab.research_validation_samples_v1 import build_research_validation_samples_v1
from ops.aegis.research_lab.research_store_reader import default_research_store_root


QUALIFICATION_FAMILY = "aegis_hypothesis_qualification_v1"
QUALIFICATION_FILENAME = "hypothesis_qualification.v1.json"
SLEEVE_FAMILY = "aegis_paper_testing_sleeve_v1"
SLEEVE_FILENAME = "paper_testing_sleeve.v1.json"

QUALIFIED = "QUALIFIED_FOR_PAPER_VALIDATION"
NOT_QUALIFIED = "NOT_QUALIFIED"
BLOCKED = "QUALIFICATION_BLOCKED"
MORE_RESEARCH = "MORE_RESEARCH_REQUIRED"
INCONCLUSIVE = "INCONCLUSIVE"


def hypothesis_qualification_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / QUALIFICATION_FAMILY / day_utc / QUALIFICATION_FILENAME


def paper_testing_sleeve_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / SLEEVE_FAMILY / day_utc / SLEEVE_FILENAME


def _stable_id(prefix: str, *parts: str) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return f"{prefix}-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"


def _safe_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value in (None, ""):
        return []
    return [str(value)]


def _qualification_for_brief(brief: dict[str, Any], *, generated_at: str, review_path: Path) -> dict[str, Any]:
    hypothesis_id = str(brief.get("hypothesis_id") or "")
    title = str(brief.get("title") or hypothesis_id)
    confidence = str(brief.get("confidence") or "LOW").upper()
    evidence = _safe_list(brief.get("key_evidence"))
    affected_symbols = sorted({symbol.upper() for symbol in _safe_list(brief.get("affected_symbols"))})
    result_text = " ".join(
        [
            str(brief.get("status") or ""),
            str(brief.get("evidence_summary") or ""),
            " ".join(evidence),
            str((brief.get("diagnostics") or {}).get("raw_result_status") or ""),
        ]
    ).upper()
    blockers: list[str] = []
    required_follow_up: list[str] = []
    if str(brief.get("status") or "") not in {"RECOMMENDATION_READY", "MONITORING"}:
        blockers.append("Review brief is not recommendation-ready.")
    if not affected_symbols:
        blockers.append("No affected symbols were identified.")
    if not evidence:
        blockers.append("No structured key evidence was recorded.")
    if "TEST_NOT_IMPLEMENTED" in result_text:
        blockers.append("Research test execution is not implemented for this hypothesis.")
        required_follow_up.append("Implement or run the governed research test before paper validation.")
    if "INCONCLUSIVE" in result_text:
        required_follow_up.append("Collect more evidence until sample-size and sufficiency thresholds are met.")
    if "DATA_NEEDED" in result_text or "MISSING_INPUT" in result_text:
        required_follow_up.append("Refresh or bind required research inputs, then rerun the governed research test.")

    evidence_sufficiency = "SUFFICIENT" if confidence in {"MEDIUM", "HIGH"} and evidence and "INCONCLUSIVE" not in result_text and "DATA_NEEDED" not in result_text else "INSUFFICIENT"
    data_freshness = "CURRENT" if str((brief.get("staleness") or {}).get("status") or "CURRENT").upper() == "CURRENT" else "STALE"
    testable_signal_definition = "PRESENT" if evidence and "TEST_NOT_IMPLEMENTED" not in result_text else "MISSING_OR_INCOMPLETE"
    paper_validation_allowed = False
    qualification_status = NOT_QUALIFIED
    if blockers:
        qualification_status = BLOCKED
    elif evidence_sufficiency == "INSUFFICIENT":
        qualification_status = MORE_RESEARCH if required_follow_up else INCONCLUSIVE
    elif data_freshness != "CURRENT":
        qualification_status = BLOCKED
        blockers.append("Research evidence is stale.")
        required_follow_up.append("Refresh source evidence before paper validation.")
    elif testable_signal_definition != "PRESENT":
        qualification_status = BLOCKED
        blockers.append("No testable signal definition is available.")
    else:
        qualification_status = QUALIFIED
        paper_validation_allowed = True

    score = 0
    score += 25 if affected_symbols else 0
    score += 25 if evidence else 0
    score += 25 if confidence in {"MEDIUM", "HIGH"} else 0
    score += 25 if data_freshness == "CURRENT" and testable_signal_definition == "PRESENT" and not blockers else 0
    return {
        "hypothesis_id": hypothesis_id,
        "research_run_id": str(brief.get("research_run_id") or ""),
        "title": title,
        "qualification_status": qualification_status,
        "qualification_score": score,
        "confidence": confidence,
        "evidence_sufficiency": evidence_sufficiency,
        "data_freshness": data_freshness,
        "affected_symbols": affected_symbols,
        "testable_signal_definition": testable_signal_definition,
        "paper_validation_allowed": paper_validation_allowed,
        "blocker_reasons": blockers,
        "required_follow_up": required_follow_up,
        "generated_at": generated_at,
        "source_artifacts": [
            {
                "artifact_id": "aegis_research_review_brief_v1",
                "path": str(review_path),
                "hypothesis_id": hypothesis_id,
            }
        ],
        "safety_gates_checked": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }


def build_hypothesis_qualification_v1(
    *,
    truth_root: Path,
    day_utc: str,
    review_brief_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at = now_utc_v1()
    review_path = research_review_brief_path_v1(truth_root=truth_root, day_utc=day_utc)
    review = review_brief_payload if review_brief_payload is not None else load_research_review_brief_v1(truth_root=truth_root, day_utc=day_utc)
    briefs = review.get("briefs") if isinstance(review, dict) and isinstance(review.get("briefs"), list) else []
    rows = [_qualification_for_brief(brief, generated_at=generated_at, review_path=review_path) for brief in briefs if isinstance(brief, dict)]
    counts = Counter(row["qualification_status"] for row in rows)
    return {
        "schema_id": "aegis_hypothesis_qualification",
        "schema_version": "v1",
        "artifact_id": "aegis_hypothesis_qualification_v1",
        "day_utc": day_utc,
        "generated_at": generated_at,
        "status": "CANONICAL",
        "summary": {
            "total_hypotheses": len(rows),
            "qualified": counts.get(QUALIFIED, 0),
            "not_qualified": counts.get(NOT_QUALIFIED, 0),
            "qualification_blocked": counts.get(BLOCKED, 0),
            "more_research_required": counts.get(MORE_RESEARCH, 0),
            "inconclusive": counts.get(INCONCLUSIVE, 0),
            "paper_validation_allowed": sum(1 for row in rows if row.get("paper_validation_allowed")),
        },
        "qualifications": rows,
        "source_artifacts": [
            {
                "artifact_id": "aegis_research_review_brief_v1",
                "path": str(review_path),
                "row_count": len(review.get("briefs") or []) if isinstance(review, dict) else 0,
            }
        ],
        "safety": {
            "research_only": True,
            "paper_testing_sleeves_allowed": True,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }


def write_hypothesis_qualification_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(hypothesis_qualification_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def load_hypothesis_qualification_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return read_json_v1(hypothesis_qualification_path_v1(truth_root=truth_root, day_utc=day_utc))


def _symbol_link(symbol: str, qualification: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "role": "AFFECTED_SYMBOL",
        "reason_linked": "Symbol appeared in qualified research review affected_symbols.",
        "evidence_reference": {
            "hypothesis_id": qualification.get("hypothesis_id"),
            "research_run_id": qualification.get("research_run_id"),
            "source_artifacts": qualification.get("source_artifacts") or [],
        },
        "validation_eligibility": "ELIGIBLE",
        "paper_candidate_allowed": True,
        "link_status": "LINKED",
        "exclusion_reason": "",
    }


def build_paper_testing_sleeve_v1(
    *,
    truth_root: Path,
    day_utc: str,
    qualification_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at = now_utc_v1()
    qualification = qualification_payload or load_hypothesis_qualification_v1(truth_root=truth_root, day_utc=day_utc)
    rows = qualification.get("qualifications") if isinstance(qualification, dict) else []
    sleeves: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if row.get("qualification_status") != QUALIFIED or not row.get("paper_validation_allowed"):
            excluded.append(
                {
                    "hypothesis_id": row.get("hypothesis_id"),
                    "qualification_status": row.get("qualification_status"),
                    "reason": "; ".join(_safe_list(row.get("blocker_reasons")) or _safe_list(row.get("required_follow_up")) or ["Not qualified for paper validation."]),
                }
            )
            continue
        hypothesis_id = str(row.get("hypothesis_id") or "")
        research_run_id = str(row.get("research_run_id") or "")
        sleeve_id = _stable_id("PTS", day_utc, hypothesis_id, research_run_id)
        linked_symbols = [_symbol_link(symbol, row) for symbol in _safe_list(row.get("affected_symbols"))]
        sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "hypothesis_id": hypothesis_id,
                "research_run_id": research_run_id,
                "sleeve_name": f"Paper Test - {row.get('title') or hypothesis_id}",
                "sleeve_type": "PAPER_TESTING",
                "lifecycle_state": "PAPER_VALIDATION_ACTIVE" if linked_symbols else "QUALIFICATION_BLOCKED",
                "linked_symbols": linked_symbols,
                "test_start_date": day_utc,
                "test_end_date": None,
                "review_after_days": 20,
                "validation_rules": {
                    "minimum_observations": 20,
                    "outcome_states": ["VALIDATED", "REJECTED", "INCONCLUSIVE", "RETIRED"],
                },
                "entry_rules": {
                    "source": "paper_validation_candidate_generation",
                    "requires_current_market_data": True,
                    "requires_valid_construction_inputs": True,
                },
                "exit_rules": {
                    "source": "paper_validation_outcome_tracking",
                    "manual_live_execution_allowed": False,
                },
                "risk_limits": {
                    "paper_only": True,
                    "live_broker_order_allowed": False,
                    "max_validation_candidate_notional": None,
                },
                "duplicate_policy": {
                    "enforced_by": "AEGIS_DUPLICATE_CANDIDATE_REQUIREMENTS.md",
                    "same_symbol_requires_explicit_classification": True,
                },
                "source_artifacts": row.get("source_artifacts") or [],
                "generated_at": generated_at,
                "actor": "SYSTEM",
                "safety_gates_checked": row.get("safety_gates_checked") or {},
            }
        )
    return {
        "schema_id": "aegis_paper_testing_sleeve",
        "schema_version": "v1",
        "artifact_id": "aegis_paper_testing_sleeve_v1",
        "day_utc": day_utc,
        "generated_at": generated_at,
        "status": "CANONICAL",
        "summary": {
            "qualified_hypotheses": sum(1 for row in rows or [] if isinstance(row, dict) and row.get("qualification_status") == QUALIFIED),
            "paper_testing_sleeves_active": len(sleeves),
            "linked_symbols": sum(len(sleeve.get("linked_symbols") or []) for sleeve in sleeves),
            "excluded_hypotheses": len(excluded),
        },
        "sleeves": sleeves,
        "excluded_hypotheses": excluded,
        "source_artifacts": [
            {
                "artifact_id": "aegis_hypothesis_qualification_v1",
                "path": str(hypothesis_qualification_path_v1(truth_root=truth_root, day_utc=day_utc)),
                "row_count": len(rows or []),
            }
        ],
        "safety": {
            "research_only": True,
            "paper_testing_sleeves_allowed": True,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }


def write_paper_testing_sleeve_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(paper_testing_sleeve_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def load_paper_testing_sleeve_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return read_json_v1(paper_testing_sleeve_path_v1(truth_root=truth_root, day_utc=day_utc))


def hypothesis_validation_status_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    qualification = load_hypothesis_qualification_v1(truth_root=truth_root, day_utc=day_utc)
    sleeves = load_paper_testing_sleeve_v1(truth_root=truth_root, day_utc=day_utc)
    q_rows = qualification.get("qualifications") if isinstance(qualification, dict) else []
    sleeve_rows = sleeves.get("sleeves") if isinstance(sleeves, dict) else []
    counts = Counter(str(row.get("qualification_status") or "UNKNOWN") for row in q_rows or [] if isinstance(row, dict))
    return {
        "ok": True,
        "day_utc": day_utc,
        "total_hypotheses": len(q_rows or []),
        "researching": 0,
        "qualified": counts.get(QUALIFIED, 0),
        "not_qualified": counts.get(NOT_QUALIFIED, 0),
        "more_research_required": counts.get(MORE_RESEARCH, 0),
        "qualification_blocked": counts.get(BLOCKED, 0),
        "inconclusive": counts.get(INCONCLUSIVE, 0),
        "paper_testing_sleeves_active": len(sleeve_rows or []),
        "linked_symbols": sum(len(row.get("linked_symbols") or []) for row in sleeve_rows or [] if isinstance(row, dict)),
        "validation_candidates_generated": 0,
        "blocked_reasons": [
            {
                "hypothesis_id": row.get("hypothesis_id"),
                "qualification_status": row.get("qualification_status"),
                "blocker_reasons": row.get("blocker_reasons") or [],
                "required_follow_up": row.get("required_follow_up") or [],
            }
            for row in q_rows or []
            if isinstance(row, dict) and row.get("qualification_status") != QUALIFIED
        ],
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }


def hypothesis_validation_self_check_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    qualification = load_hypothesis_qualification_v1(truth_root=truth_root, day_utc=day_utc)
    sleeves = load_paper_testing_sleeve_v1(truth_root=truth_root, day_utc=day_utc)
    q_rows = qualification.get("qualifications") if isinstance(qualification, dict) else []
    sleeve_rows = sleeves.get("sleeves") if isinstance(sleeves, dict) else []
    sleeves_by_hypothesis = {str(row.get("hypothesis_id") or ""): row for row in sleeve_rows or [] if isinstance(row, dict)}
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, details: dict[str, Any] | None = None) -> None:
        checks.append({"check": name, "ok": bool(ok), "details": details or {}})

    qualified_ids = [str(row.get("hypothesis_id") or "") for row in q_rows or [] if isinstance(row, dict) and row.get("qualification_status") == QUALIFIED]
    missing_sleeves = [hypothesis_id for hypothesis_id in qualified_ids if hypothesis_id not in sleeves_by_hypothesis]
    add_check("qualified hypothesis has paper-testing sleeve", not missing_sleeves, {"missing_hypothesis_ids": missing_sleeves})
    no_symbols = [row.get("sleeve_id") for row in sleeve_rows or [] if isinstance(row, dict) and not row.get("linked_symbols")]
    add_check("paper-testing sleeve has linked symbols", not no_symbols, {"sleeve_ids": no_symbols})
    missing_evidence = [
        {"sleeve_id": row.get("sleeve_id"), "symbol": link.get("symbol")}
        for row in sleeve_rows or []
        if isinstance(row, dict)
        for link in row.get("linked_symbols") or []
        if not link.get("evidence_reference")
    ]
    add_check("linked symbol has evidence reference", not missing_evidence, {"missing": missing_evidence})
    ambiguous = [row.get("hypothesis_id") for row in q_rows or [] if isinstance(row, dict) and row.get("qualification_status") not in {QUALIFIED, NOT_QUALIFIED, BLOCKED, MORE_RESEARCH, INCONCLUSIVE}]
    add_check("validation state is explicit", not ambiguous, {"hypothesis_ids": ambiguous})
    terminal_rec_ready = [
        row.get("hypothesis_id")
        for row in q_rows or []
        if isinstance(row, dict) and row.get("qualification_status") == QUALIFIED and not row.get("paper_validation_allowed")
    ]
    add_check("Recommendation Ready is not terminal for qualified hypothesis", not terminal_rec_ready, {"hypothesis_ids": terminal_rec_ready})
    unsafe = any((qualification.get("safety") or {}).get(key) for key in ["trade_advice_allowed", "broker_execution_allowed", "broker_submit_transmit_allowed", "live_trading_allowed", "autonomous_live_trading_allowed"]) or any((sleeves.get("safety") or {}).get(key) for key in ["trade_advice_allowed", "broker_execution_allowed", "broker_submit_transmit_allowed", "live_trading_allowed", "autonomous_live_trading_allowed"])
    add_check("live broker and trade-advice gates remain disabled", not unsafe)
    ok = all(check["ok"] for check in checks)
    return {
        "ok": ok,
        "day_utc": day_utc,
        "checks": checks,
        "qualified": len(qualified_ids),
        "paper_testing_sleeves_active": len(sleeve_rows or []),
        "linked_symbols": sum(len(row.get("linked_symbols") or []) for row in sleeve_rows or [] if isinstance(row, dict)),
    }



def research_validation_self_check_v1(*, truth_root: Path, day_utc: str, store_root: Path | None = None) -> dict[str, Any]:
    review = load_research_review_brief_v1(truth_root=truth_root, day_utc=day_utc)
    sleeves = load_paper_testing_sleeve_v1(truth_root=truth_root, day_utc=day_utc)
    console = research_console_v1(store_root=store_root or default_research_store_root())
    briefs = review.get("briefs") if isinstance(review, dict) and isinstance(review.get("briefs"), list) else []
    sleeve_rows = sleeves.get("sleeves") if isinstance(sleeves, dict) and isinstance(sleeves.get("sleeves"), list) else []
    console_rows = console.get("all_hypotheses") if isinstance(console, dict) and isinstance(console.get("all_hypotheses"), list) else []
    console_by_id = {str(row.get("hypothesis_id") or ""): row for row in console_rows if isinstance(row, dict)}
    sleeve_hypothesis_ids = {str(row.get("hypothesis_id") or "") for row in sleeve_rows if isinstance(row, dict)}
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, details: dict[str, Any] | None = None) -> None:
        checks.append({"check": name, "ok": bool(ok), "details": details or {}})

    collecting = [brief for brief in briefs if isinstance(brief, dict) and (brief.get("validation_ui") or {}).get("state_label") == "Collecting Evidence"]
    missing_counts: list[str] = []
    missing_next_sample: list[str] = []
    operator_required: list[str] = []
    sleeve_created_too_early: list[str] = []
    ambiguous: list[str] = []
    for brief in collecting:
        hypothesis_id = str(brief.get("hypothesis_id") or "")
        validation = brief.get("validation_ui") if isinstance(brief.get("validation_ui"), dict) else {}
        if validation.get("required_samples") is None or validation.get("current_samples") is None or validation.get("missing_samples") is None:
            missing_counts.append(hypothesis_id)
        if not validation.get("next_sample_expected_at"):
            missing_next_sample.append(hypothesis_id)
        if validation.get("operator_action_required") is not False or (brief.get("diagnostics") or {}).get("operator_action_required") is True:
            operator_required.append(hypothesis_id)
        if hypothesis_id in sleeve_hypothesis_ids:
            sleeve_created_too_early.append(hypothesis_id)
        console_row = console_by_id.get(hypothesis_id, {})
        status_text = " ".join([
            str(console_row.get("user_facing_status") or ""),
            str(console_row.get("user_facing_explanation") or ""),
            str(brief.get("decision_needed") or ""),
        ]).lower()
        if "waiting" in status_text or "manual review" in status_text:
            ambiguous.append(hypothesis_id)
    sample_report = build_research_validation_samples_v1(truth_root=truth_root, day_utc=day_utc)
    valid_observations_exist = int(sample_report.get("current_samples") or 0) > 0
    stale_zero = bool(collecting) and valid_observations_exist and any((brief.get("validation_ui") or {}).get("current_samples") == 0 for brief in collecting if isinstance(brief, dict))
    threshold_met_without_rerun = int(sample_report.get("current_samples") or 0) >= int(sample_report.get("required_samples") or 20) and not sample_report.get("should_rerun_qualification")
    add_check("Collecting Evidence has sample counts", not missing_counts, {"hypothesis_ids": missing_counts})
    add_check("Collecting Evidence has next sample timing", not missing_next_sample, {"hypothesis_ids": missing_next_sample})
    add_check("Collecting Evidence does not require operator action", not operator_required, {"hypothesis_ids": operator_required})
    add_check("paper-testing sleeve is not created before qualification", not sleeve_created_too_early, {"hypothesis_ids": sleeve_created_too_early})
    add_check("Collecting Evidence is not ambiguous Waiting or Manual Review", not ambiguous, {"hypothesis_ids": ambiguous})
    add_check("Collecting Evidence sample count advances when valid observations exist", not stale_zero, {"current_samples": sample_report.get("current_samples"), "exclusion_reasons": sample_report.get("exclusion_reasons")})
    add_check("qualification rerun is requested when sample threshold is met", not threshold_met_without_rerun, {"current_samples": sample_report.get("current_samples"), "required_samples": sample_report.get("required_samples")})
    ok = all(check["ok"] for check in checks)
    return {
        "ok": ok,
        "day_utc": day_utc,
        "collecting_evidence_count": len(collecting),
        "paper_testing_sleeves_active": len(sleeve_rows),
        "research_validation_samples": {
            "current_samples": sample_report.get("current_samples"),
            "required_samples": sample_report.get("required_samples"),
            "missing_samples": sample_report.get("missing_samples"),
            "next_sample_expected_at": sample_report.get("next_sample_expected_at"),
            "exclusion_reasons": sample_report.get("exclusion_reasons"),
            "should_rerun_qualification": sample_report.get("should_rerun_qualification"),
        },
        "checks": checks,
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }
