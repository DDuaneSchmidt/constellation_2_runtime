from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1, write_candidate_lifecycle_reports_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_regime_outcome_memory_v1"


def build_regime_outcome_memory_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    regime_path, regime = latest_json_v1(root, "regime_context_v1", day_utc, "regime_context.v1.json")
    classifications = regime.get("regime_classifications") if isinstance(regime.get("regime_classifications"), dict) else {}
    rows = []
    for candidate in lifecycle.get("candidates", []):
        if not isinstance(candidate, dict):
            continue
        rows.append(
            {
                "candidate_id": candidate.get("candidate_id"),
                "sleeve_id": candidate.get("sleeve_id"),
                "operator_decision": candidate.get("operator_decision"),
                "outcome_status": candidate.get("outcome_status"),
                "regime_context": classifications or candidate.get("regime_context") or {},
                "evidence_quality": "LOW" if candidate.get("evidence_artifacts") else "UNKNOWN",
            }
        )
    quality = _quality(rows)
    payload = {
        "schema_id": "aegis_regime_outcome_memory",
        "schema_version": "v1",
        "artifact_id": "aegis_regime_outcome_memory_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "input_artifacts": {
            "candidate_lifecycle": str(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_lifecycle.v1.json"),
            "regime_context": str(regime_path or ""),
        },
        "candidate_outcomes_by_regime": rows,
        "sleeve_outcomes_by_regime": quality,
        "ignored_candidate_outcomes_by_regime": [row for row in rows if row.get("operator_decision") == "IGNORED"],
        "failure_patterns_by_regime": _failure_patterns(rows),
        "research_ideas_generated_from_regime_failures": _research_ideas(rows),
        "sleeves_favored_by_regime": [],
        "sleeves_impaired_by_regime": [],
        "evidence_quality": "LOW" if rows else "INSUFFICIENT_DATA",
        "unknowns": ["longitudinal_regime_history"] if len(rows) < 20 else [],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "safety": {"broker_execution_allowed": False, "autonomous_execution_allowed": False},
    }
    return payload


def write_regime_outcome_memory_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    memory_path = write_json_v1(out_dir / "regime_outcome_memory.v1.json", payload)
    quality_path = write_json_v1(
        out_dir / "regime_sleeve_candidate_quality.v1.json",
        {
            "schema_id": "regime_sleeve_candidate_quality",
            "schema_version": "v1",
            "day_utc": day_utc,
            "sleeve_outcomes_by_regime": payload["sleeve_outcomes_by_regime"],
            "evidence_quality": payload["evidence_quality"],
        },
    )
    summary_path = out_dir / "regime_lessons.summary.txt"
    summary_path.write_text(render_regime_lessons_summary_v1(payload), encoding="utf-8")
    return {"memory": str(memory_path), "quality": str(quality_path), "summary": str(summary_path)}


def render_regime_lessons_summary_v1(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "AEGIS REGIME OUTCOME MEMORY v1",
            f"day_utc: {payload.get('day_utc')}",
            f"candidate_rows: {len(payload.get('candidate_outcomes_by_regime') or [])}",
            f"evidence_quality: {payload.get('evidence_quality')}",
            "broker_execution_allowed: false",
            "autonomous_execution_allowed: false",
            "",
        ]
    )


def _quality(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_sleeve: dict[str, dict[str, Any]] = {}
    for row in rows:
        sleeve = str(row.get("sleeve_id") or "UNKNOWN")
        bucket = by_sleeve.setdefault(sleeve, {"sleeve_id": sleeve, "candidate_count": 0, "known_outcome_count": 0, "ignored_count": 0})
        bucket["candidate_count"] += 1
        if row.get("operator_decision") == "IGNORED":
            bucket["ignored_count"] += 1
        if row.get("outcome_status") in {"OUTCOME_WON", "OUTCOME_LOST", "OUTCOME_FLAT"}:
            bucket["known_outcome_count"] += 1
    return list(by_sleeve.values())


def _failure_patterns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"pattern": "candidate_loss", "candidate_id": row.get("candidate_id"), "sleeve_id": row.get("sleeve_id"), "confidence": "LOW"}
        for row in rows
        if row.get("outcome_status") == "OUTCOME_LOST"
    ]


def _research_ideas(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "idea_id": f"regime-failure:{row.get('candidate_id')}",
            "source_candidate_id": row.get("candidate_id"),
            "title": "Investigate regime-specific candidate failure",
            "human_approval_required": True,
        }
        for row in rows
        if row.get("outcome_status") == "OUTCOME_LOST"
    ]
