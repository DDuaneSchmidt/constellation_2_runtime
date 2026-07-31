from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_mapping_rules_v1 import SLEEVE_RULES, THESIS_RULES, declared_or_legacy_mapping_v1, iter_report_days_v1, report_path_v1, source_ref_v1, stable_hash_v1, text_v1

REPORT_FAMILY = "aegis_research_thesis_registry_v1"
REPORT_FILENAME = "thesis_registry.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def thesis_registry_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def observed_sleeve_ids_v1(*, truth_root: Path | str, day_utc: str) -> list[str]:
    sleeves: set[str] = set()
    for _, payload in iter_report_days_v1(truth_root, "sleeve_evaluation_kernel_v1", "sleeve_evaluation.v1.json", day_utc) or []:
        sleeve = text_v1(payload.get("sleeve_id") or payload.get("sleeve"))
        if not sleeve:
            # sleeve id is often the parent directory for this family
            pass
    base = Path(truth_root).expanduser().resolve() / "reports" / "sleeve_evaluation_kernel_v1"
    if base.exists():
        for path in sorted(base.glob("*/*/sleeve_evaluation.v1.json")):
            if path.parts[-3] <= str(day_utc) and path.parts[-2] != "UNKNOWN":
                sleeves.add(path.parts[-2])
    for family, filename in [("aegis_sleeve_analytics_v1", "sleeve_analytics.v1.json"), ("aegis_sleeve_performance_truth_v1", "sleeve_performance_truth.v1.json")]:
        for path, payload in iter_report_days_v1(truth_root, family, filename, day_utc) or []:
            for row in payload.get("sleeves") or []:
                sleeve_id = text_v1(row.get("sleeve_id")) if isinstance(row, dict) else ""
                if sleeve_id and sleeve_id != "UNKNOWN":
                    sleeves.add(sleeve_id)
    for path, payload in iter_report_days_v1(truth_root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json", day_utc) or []:
        for key in ("candidate_contracts", "rejected_raw_signals"):
            for row in payload.get(key) or []:
                sleeve_id = text_v1(row.get("sleeve_id")) if isinstance(row, dict) else ""
                if sleeve_id and sleeve_id != "UNKNOWN":
                    sleeves.add(sleeve_id)
    return sorted(sleeves)


def build_research_thesis_registry_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    sleeves = observed_sleeve_ids_v1(truth_root=truth_root, day_utc=day_utc)
    thesis_to_hypotheses: dict[str, set[str]] = {}
    thesis_to_sleeves: dict[str, set[str]] = {}
    unmapped: list[str] = []
    for sleeve in sleeves:
        mapping = declared_or_legacy_mapping_v1({"sleeve_id": sleeve})
        thesis_id = text_v1(mapping.get("thesis_id"))
        hypothesis_id = text_v1(mapping.get("hypothesis_id"))
        if not thesis_id or not hypothesis_id:
            unmapped.append(sleeve)
            continue
        thesis_to_hypotheses.setdefault(thesis_id, set()).add(hypothesis_id)
        thesis_to_sleeves.setdefault(thesis_id, set()).add(sleeve)
    theses = []
    for thesis_id in sorted(thesis_to_hypotheses):
        rule = THESIS_RULES.get(thesis_id, {})
        related = sorted(thesis_to_hypotheses.get(thesis_id, set()))
        sleeves_for_thesis = sorted(thesis_to_sleeves.get(thesis_id, set()))
        status = "ACTIVE" if sleeves_for_thesis else "PROPOSED"
        theses.append({
            "thesis_id": thesis_id,
            "name": rule.get("name") or thesis_id,
            "description": rule.get("description") or "Legacy-inferred research thesis.",
            "rationale": rule.get("rationale") or "Created by deterministic legacy migration from sleeve metadata.",
            "source": "LEGACY_SLEEVE_MIGRATION_RULES_V1",
            "created_at": f"{day_utc}T00:00:00Z",
            "status": status,
            "owner_type": "SYSTEM",
            "related_hypotheses": related,
            "linked_sleeves": sleeves_for_thesis,
            "evidence_summary": {"linked_hypothesis_count": len(related), "linked_sleeve_count": len(sleeves_for_thesis)},
            "allocation_score": None,
            "mapping_confidence": "LEGACY_INFERRED",
        })
    payload = {
        "schema_id": "aegis_research_thesis_registry",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "theses": theses,
        "summary": {"thesis_count": len(theses), "active_theses": sum(1 for row in theses if row["status"] == "ACTIVE"), "unmapped_sleeves": len(unmapped)},
        "unmapped_sleeves": unmapped,
        "source_artifacts": {"sleeve_evaluation_kernel_v1": str(Path(truth_root).expanduser().resolve() / "reports" / "sleeve_evaluation_kernel_v1")},
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_research_thesis_registry_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_research_thesis_registry_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(thesis_registry_path_v1(truth_root=truth_root, day_utc=day_utc), body)
