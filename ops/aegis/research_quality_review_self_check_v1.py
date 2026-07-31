from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DOCS = {
    "quality_review": Path("docs/aegis_thesis_hypothesis_quality_review_v1.md"),
    "inactive_sleeve_review": Path("docs/aegis_inactive_sleeve_explanation_review_v1.md"),
    "retirement_policy": Path("docs/aegis_retirement_criteria_policy_v1.md"),
    "program_grouping_review": Path("docs/aegis_research_program_grouping_review_v1.md"),
    "regime_taxonomy": Path("docs/aegis_regime_tag_taxonomy_v1.md"),
}


def build_research_quality_review_self_check_v1(*, repo_root: Path | str = Path("."), truth_root: Path | str = "/home/node/constellation_runtime_data/truth", day_utc: str) -> dict[str, Any]:
    repo = Path(repo_root).resolve()
    truth = Path(truth_root).resolve()
    failures: list[dict[str, Any]] = []
    texts: dict[str, str] = {}
    for name, rel in DOCS.items():
        path = repo / rel
        if not path.exists():
            failures.append(_failure("MISSING_DOC", name, str(path)))
            texts[name] = ""
        else:
            texts[name] = path.read_text(encoding="utf-8")
    portfolio_path = truth / "reports" / "aegis_research_portfolio_v1" / day_utc / "research_portfolio.v1.json"
    portfolio = _read_json(portfolio_path)
    hypotheses = [str(row.get("hypothesis_id") or "") for row in portfolio.get("hypotheses") or [] if row.get("hypothesis_id")]
    theses = [str(row.get("thesis_id") or "") for row in portfolio.get("theses") or [] if row.get("thesis_id")]
    programs_path = truth / "reports" / "aegis_research_capital_allocation_v1" / day_utc / "research_capital_allocation.v1.json"
    allocation = _read_json(programs_path)
    programs = [str(row.get("research_program_id") or "") for row in allocation.get("programs") or [] if row.get("research_program_id")]
    inactive_required = [
        "C2_DEFENSIVE_TAIL_V1",
        "C2_EVENT_DISLOCATION_V1",
        "C2_MARKET_NEUTRAL_SPREAD_V1",
        "C2_MEAN_REVERSION_EQ_V1",
        "C2_VOL_INCOME_DEFINED_RISK_V1",
    ]
    for thesis_id in theses:
        if thesis_id not in texts.get("quality_review", ""):
            failures.append(_failure("THESIS_NOT_REVIEWED", thesis_id, str(DOCS["quality_review"])))
    for hypothesis_id in hypotheses:
        if hypothesis_id not in texts.get("quality_review", ""):
            failures.append(_failure("HYPOTHESIS_NOT_REVIEWED", hypothesis_id, str(DOCS["quality_review"])))
    for sleeve_id in inactive_required:
        if sleeve_id not in texts.get("inactive_sleeve_review", ""):
            failures.append(_failure("INACTIVE_SLEEVE_NOT_CLASSIFIED", sleeve_id, str(DOCS["inactive_sleeve_review"])))
    for program_id in programs:
        if program_id not in texts.get("program_grouping_review", ""):
            failures.append(_failure("RESEARCH_PROGRAM_NOT_REVIEWED", program_id, str(DOCS["program_grouping_review"])))
    for required in ["Hypothesis Retirement Criteria", "Sleeve Retirement Criteria", "Research Program Retirement Criteria", "manual_review_required_before_retire"]:
        if required not in texts.get("retirement_policy", ""):
            failures.append(_failure("RETIREMENT_POLICY_SECTION_MISSING", required, str(DOCS["retirement_policy"])))
    for tag in ["RISK_ON", "RISK_OFF", "LOW_VOL", "HIGH_VOL", "RATES_RISING", "DOLLAR_STRENGTH", "BROAD_PARTICIPATION", "CROSS_ASSET_CONFIRMATION", "EARNINGS_SEASON", "OPTIONS_EXPIRATION_WEEK"]:
        if tag not in texts.get("regime_taxonomy", ""):
            failures.append(_failure("REGIME_TAG_MISSING", tag, str(DOCS["regime_taxonomy"])))
    return {
        "schema_id": "aegis_research_quality_review_self_check",
        "schema_version": "v1",
        "day_utc": day_utc,
        "generated_at": _now(),
        "ok": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "summary": {
            "theses_expected": len(theses),
            "hypotheses_expected": len(hypotheses),
            "inactive_sleeves_expected": len(inactive_required),
            "research_programs_expected": len(programs),
            "docs_checked": len(DOCS),
        },
        "docs": {name: str((repo / rel).resolve()) for name, rel in DOCS.items()},
    }


def write_research_quality_review_self_check_v1(*, repo_root: Path | str = Path("."), truth_root: Path | str = "/home/node/constellation_runtime_data/truth", day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    truth = Path(truth_root).resolve()
    path = truth / "reports" / "aegis_research_quality_review_self_check_v1" / day_utc / "self_check.v1.json"
    body = payload or build_research_quality_review_self_check_v1(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _failure(code: str, item: str, path: str) -> dict[str, str]:
    return {"failure_code": code, "item": item, "path": path}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
