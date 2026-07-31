from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "family_observation_log"
FAMILY_ROBUSTNESS_REVIEW_DIRNAME = "family_robustness_review"
CSV_FIELDS = [
    "date",
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "symbol_or_universe",
    "observation_condition_met",
    "invalidation_condition_met",
    "paper_outcome",
    "return_observed",
    "notes",
    "reviewer",
]
WEEKLY_REVIEW_FIELDS = [
    "observations logged",
    "sample size by family",
    "supporting observations",
    "invalidating observations",
    "families strengthened",
    "families weakened",
    "families needing data",
    "families to retire",
]
MANUAL_OBSERVATION_LIMITATION = (
    "Manual family-level observation logging only. This workflow does not authorize trade execution, "
    "broker actions, automatic paper trade placement, capital authority, position sizing, trade recommendations, "
    "or intraday data purchases."
)


def build_family_observation_log_template(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    day: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    source = root_path / FAMILY_ROBUSTNESS_REVIEW_DIRNAME / "latest.json"
    review = _read_json(source, {})
    families = list(review.get("family_reviews", []))
    families.sort(key=_family_sort_key)
    if limit is not None:
        families = families[:limit]
    rows = [_template_row(family) for family in families]
    payload = {
        "schema_id": "atlas_v2_research_os_family_observation_log_template_v1",
        "schema_version": "v1",
        "day": day or str(review.get("day") or _today()),
        "created_at": _now(),
        "source_family_robustness_review_report": source.as_posix(),
        "fields": list(CSV_FIELDS),
        "weekly_review_fields": list(WEEKLY_REVIEW_FIELDS),
        "family_count": len(rows),
        "rows": rows,
        "manual_workflow": [
            "Open the CSV template and record only forward observations that were manually checked.",
            "Use true, false, or blank for observation_condition_met and invalidation_condition_met.",
            "Use supporting, invalidating, no_setup, inconclusive, needs_data, retire, or blank for paper_outcome.",
            "Use return_observed only for manually observed paper-forward outcomes, never for allocation or sizing.",
            "Do not place trades, broker orders, automatic paper trades, allocations, or position sizes from this log.",
            "Do not purchase intraday data for this workflow.",
        ],
        "authority_boundary": _authority_boundary(),
        "human_review_required": True,
        "artifact_types": ["FamilyObservationLogTemplate", "WeeklyFamilyObservationReviewTemplate"],
        "metadata": {"research_only": True, "manual_observation_only": True},
        "limitations": [MANUAL_OBSERVATION_LIMITATION],
    }
    _validate_authority_boundary(payload)
    return payload


def write_family_observation_log_template(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    day: str | None = None,
    limit: int | None = None,
) -> dict[str, Path]:
    root_path = Path(root)
    payload = build_family_observation_log_template(root_path, day=day, limit=limit)
    day_value = day or payload["day"]
    out_root = root_path / REPORT_DIRNAME
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "family_observation_log_template.csv"
    instructions_path = out_dir / "family_observation_log_instructions.md"
    weekly_template_path = out_dir / "weekly_family_observation_review_template.md"
    latest_csv = out_root / "latest_template.csv"
    latest_instructions = out_root / "latest_instructions.md"
    _write_csv(csv_path, payload["rows"])
    _write_csv(latest_csv, payload["rows"])
    instructions = render_family_observation_log_instructions(payload)
    weekly_template = render_weekly_family_observation_review_template(payload)
    instructions_path.write_text(instructions, encoding="utf-8")
    weekly_template_path.write_text(weekly_template, encoding="utf-8")
    latest_instructions.write_text(instructions, encoding="utf-8")
    return {
        "csv": csv_path,
        "instructions": instructions_path,
        "weekly_review_template": weekly_template_path,
        "latest_csv": latest_csv,
        "latest_instructions": latest_instructions,
    }


def render_family_observation_log_instructions(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Family Observation Log Instructions",
            "",
            f"Day: {payload['day']}",
            f"Family rows: {payload['family_count']}",
            "",
            "## Manual Logging Workflow",
            "",
            "Use this CSV as a manual research log for family-level forward evidence. Fill a row only when a family condition is manually observed or manually invalidated. Leave fields blank when the evidence is unavailable.",
            "",
            "Fields:",
            ", ".join(payload["fields"]),
            "",
            "`observation_condition_met` and `invalidation_condition_met` should be true, false, or blank. `paper_outcome` should be supporting, invalidating, no_setup, inconclusive, needs_data, retire, or blank. `return_observed` is optional measured paper-forward evidence only; it is not a sizing, allocation, or trade input.",
            "",
            "## Weekly Review Must Summarize",
            "",
            "\n".join(f"- {field}" for field in payload["weekly_review_fields"]),
            "",
            "## Boundaries",
            "",
            "Manual observation only.",
            "No intraday data purchases are required or authorized.",
            "No trade execution.",
            "No broker actions.",
            "No automatic paper trade placement.",
            "No capital authority.",
            "No position sizing.",
            "No trade recommendations.",
            "",
            "This workflow is a paper-forward observation evidence surface only. It is not an order ticket, paper trade ticket, execution plan, allocation instruction, or live trading workflow.",
            "",
        ]
    )


def render_weekly_family_observation_review_template(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Weekly Family Observation Review Template",
            "",
            f"Week ending: {payload['day']}",
            "",
            "## Observations Logged",
            "",
            "- Total:",
            "- Notes:",
            "",
            "## Sample Size By Family",
            "",
            "| family_id | observations | supporting | invalidating | needs_data |",
            "| --- | ---: | ---: | ---: | ---: |",
            "",
            "## Supporting Observations",
            "",
            "- ",
            "",
            "## Invalidating Observations",
            "",
            "- ",
            "",
            "## Families Strengthened",
            "",
            "- ",
            "",
            "## Families Weakened",
            "",
            "- ",
            "",
            "## Families Needing Data",
            "",
            "- ",
            "",
            "## Families To Retire",
            "",
            "- ",
            "",
            "## Boundaries",
            "",
            "Manual observation only. No trade execution, broker actions, automatic paper trade placement, capital authority, position sizing, trade recommendations, or intraday data purchases.",
            "",
        ]
    )


def audit_family_observation_log_templates(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root) / REPORT_DIRNAME
    if root_path.exists():
        for path in root_path.rglob("family_observation_log_template.csv"):
            try:
                with path.open(newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    if reader.fieldnames != CSV_FIELDS:
                        raise ValueError("family observation log fields do not match required fields")
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
        for path in root_path.rglob("*instructions.md"):
            text = path.read_text(encoding="utf-8")
            for required in ("Manual observation only.", "No trade execution.", "No broker actions.", "No position sizing."):
                if required not in text:
                    failures.append(f"{path.as_posix()}: missing guardrail {required}")
    return {
        "family_observation_log_audit_ok": not failures,
        "family_observation_log_audit_failures": failures,
    }


def _template_row(family: dict[str, Any]) -> dict[str, str]:
    mechanism, regime = _mechanism_and_regime(family)
    return {
        "date": "",
        "family_id": str(family.get("family_id") or ""),
        "mechanism": mechanism,
        "regime": regime,
        "timeframe": _joined(family.get("timeframe_concentration", {}).get("timeframes")),
        "symbol_or_universe": _joined(family.get("symbol_universe_concentration", {}).get("symbols")),
        "observation_condition_met": "",
        "invalidation_condition_met": "",
        "paper_outcome": "",
        "return_observed": "",
        "notes": "",
        "reviewer": "",
    }


def _mechanism_and_regime(family: dict[str, Any]) -> tuple[str, str]:
    family_name = str(family.get("family_name") or "")
    parts = [part.strip() for part in family_name.split("/") if part.strip()]
    mechanism = str(family.get("mechanism") or (parts[0] if parts else ""))
    regime = str(family.get("regime") or (parts[1] if len(parts) > 1 else ""))
    return mechanism, regime


def _family_sort_key(family: dict[str, Any]) -> tuple[int, int, str]:
    priority = {
        "ROBUST_ENOUGH_TO_OBSERVE": 0,
        "PROMISING_BUT_DATA_BLOCKED": 1,
        "DUPLICATIVE": 2,
        "FRAGILE": 3,
        "REJECT_FOR_NOW": 4,
    }
    classification = str(family.get("classification") or "")
    rank = family.get("best_rank")
    try:
        numeric_rank = int(rank)
    except (TypeError, ValueError):
        numeric_rank = 999999
    return priority.get(classification, 9), numeric_rank, str(family.get("family_id") or "")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def _joined(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if str(item))
    if value is None:
        return ""
    return str(value)


def _authority_boundary() -> dict[str, Any]:
    return {
        "manual_observation_only": True,
        "intraday_data_purchase_required": False,
        "paper_forward_observation_only": True,
        "trade_execution_authorized": False,
        "broker_execution_authorized": False,
        "automatic_paper_trade_placement_authorized": False,
        "capital_authorized": False,
        "position_sizing_authorized": False,
        "trade_recommendation_authorized": False,
    }


def _validate_authority_boundary(payload: dict[str, Any]) -> None:
    boundary = payload.get("authority_boundary", {})
    forbidden = [
        "trade_execution_authorized",
        "broker_execution_authorized",
        "automatic_paper_trade_placement_authorized",
        "capital_authorized",
        "position_sizing_authorized",
        "trade_recommendation_authorized",
    ]
    for key in forbidden:
        if boundary.get(key) is not False:
            raise ValueError(f"family observation log authority boundary violated: {key}")
    if boundary.get("manual_observation_only") is not True:
        raise ValueError("family observation log must remain manual observation only")


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
