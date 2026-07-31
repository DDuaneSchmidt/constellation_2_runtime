from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .paper_forward_observation_governance import validate_paper_forward_observation_allowed

REPORT_DIRNAME = "paper_forward_observation_log"
DEFAULT_FIELDS = [
    "date",
    "candidate_id",
    "mechanism",
    "ticker_or_symbol",
    "regime_observed",
    "entry_condition_met",
    "exit_condition_met",
    "invalidating_condition_met",
    "paper_result",
    "return_observed",
    "notes",
    "reviewer",
]
MANUAL_LOG_LIMITATION = (
    "Manual paper-forward observation logging only. This template does not authorize broker execution, "
    "automatic paper trade placement, live trading, capital authority, position sizing, or trade recommendations."
)


def build_observation_log_template(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, limit: int = 12) -> dict[str, Any]:
    root_path = Path(root)
    source = root_path / "candidate_review" / "latest.json"
    review = _read_json(source, {})
    candidates = list(review.get("top_candidates", []))[:limit]
    rows = [_template_row(candidate) for candidate in candidates]
    payload = {
        "schema_id": "atlas_v2_research_os_paper_forward_observation_log_template_v1",
        "schema_version": "v1",
        "day": day or str(review.get("day") or _today()),
        "created_at": _now(),
        "source_candidate_review_report": source.as_posix(),
        "fields": list(DEFAULT_FIELDS),
        "candidate_count": len(rows),
        "rows": rows,
        "manual_workflow": [
            "Open the CSV or JSON template.",
            "For each reviewed candidate, fill only the observation fields that were actually observed.",
            "Use true, false, or blank for condition fields.",
            "Record paper_result as observed, invalidated, no_setup, inconclusive, or blank.",
            "Do not place trades, broker orders, automatic paper trades, allocations, or position sizes from this log.",
        ],
        "authority_boundary": _authority_boundary(),
        "human_review_required": True,
        "artifact_types": ["PaperForwardObservationLogTemplate"],
        "metadata": {"research_only": True, "manual_logging_only": True},
        "limitations": [MANUAL_LOG_LIMITATION],
    }
    validate_paper_forward_observation_allowed(payload)
    return payload


def write_observation_log_template(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    day: str | None = None,
    limit: int = 12,
) -> dict[str, Path]:
    root_path = Path(root)
    payload = build_observation_log_template(root_path, day=day, limit=limit)
    day_value = day or payload["day"]
    out_root = root_path / REPORT_DIRNAME
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "observation_log_template.csv"
    json_path = out_dir / "observation_log_template.json"
    instructions_path = out_dir / "observation_log_instructions.md"
    latest_csv = out_root / "latest_template.csv"
    latest_instructions = out_root / "latest_instructions.md"
    _write_csv(csv_path, payload["rows"])
    _write_csv(latest_csv, payload["rows"])
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    instructions = render_observation_log_instructions(payload)
    instructions_path.write_text(instructions, encoding="utf-8")
    latest_instructions.write_text(instructions, encoding="utf-8")
    return {
        "csv": csv_path,
        "json": json_path,
        "instructions": instructions_path,
        "latest_csv": latest_csv,
        "latest_instructions": latest_instructions,
    }


def render_observation_log_instructions(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Paper-Forward Observation Log Template",
            "",
            f"Day: {payload['day']}",
            f"Candidate rows: {payload['candidate_count']}",
            "",
            "## How David Should Log Observations",
            "",
            "Use this as a manual research log. Fill one row only when a reviewed candidate setup is observed in the market context. Leave fields blank when the observation is not available or cannot be reconstructed from evidence.",
            "",
            "Fields:",
            ", ".join(payload["fields"]),
            "",
            "Condition fields should be true, false, or blank. `paper_result` should be observed, invalidated, no_setup, inconclusive, or blank. `return_observed` is optional and should be a measured paper-forward outcome only, not a sizing or allocation input.",
            "",
            "## Boundaries",
            "",
            "No broker execution.",
            "No automatic paper trade placement.",
            "No live trading.",
            "No capital authority.",
            "No position sizing.",
            "No trade recommendations.",
            "",
            "This template is for paper-forward observation notes only. It is not an order ticket, paper trade ticket, execution plan, allocation instruction, or live trading workflow.",
            "",
        ]
    )


def audit_observation_log_templates(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root) / REPORT_DIRNAME
    if root_path.exists():
        for path in root_path.rglob("observation_log_template.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("fields") != DEFAULT_FIELDS:
                    raise ValueError("observation log template fields do not match required fields")
                validate_paper_forward_observation_allowed(payload)
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"paper_forward_observation_log_audit_ok": not failures, "paper_forward_observation_log_audit_failures": failures}


def _template_row(candidate: dict[str, Any]) -> dict[str, str]:
    return {
        "date": "",
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "mechanism": str(candidate.get("mechanism") or ""),
        "ticker_or_symbol": "",
        "regime_observed": "",
        "entry_condition_met": "",
        "exit_condition_met": "",
        "invalidating_condition_met": "",
        "paper_result": "",
        "return_observed": "",
        "notes": "",
        "reviewer": "",
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=DEFAULT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in DEFAULT_FIELDS})


def _authority_boundary() -> dict[str, Any]:
    return {
        "manual_logging_only": True,
        "paper_forward_observation_only": True,
        "broker_execution_authorized": False,
        "automatic_paper_trade_placement_authorized": False,
        "live_trading_authorized": False,
        "capital_authorized": False,
        "position_sizing_authorized": False,
        "trade_recommendation_authorized": False,
    }


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
