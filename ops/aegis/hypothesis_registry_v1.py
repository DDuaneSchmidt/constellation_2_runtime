from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_mapping_rules_v1 import SLEEVE_RULES, THESIS_RULES, declared_or_legacy_mapping_v1, iter_report_days_v1, report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_thesis_registry_v1 import observed_sleeve_ids_v1

REPORT_FAMILY = "aegis_hypothesis_registry_v1"
REPORT_FILENAME = "hypothesis_registry.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def hypothesis_registry_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def candidate_rows_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path, payload in iter_report_days_v1(truth_root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json", day_utc) or []:
        for key in ("candidate_contracts", "rejected_raw_signals"):
            for row in payload.get(key) or []:
                if not isinstance(row, Mapping):
                    continue
                candidate_id = text_v1(row.get("candidate_id") or row.get("candidate_contract_id"))
                if candidate_id:
                    rows[candidate_id] = {**rows.get(candidate_id, {}), **dict(row), "source_artifact": str(path)}
    return list(rows.values())


def paper_position_rows_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    path = report_path_v1(truth_root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json")
    from ops.aegis.intelligence_common_v1 import read_json_v1
    payload = read_json_v1(path)
    rows = []
    for key in ("positions", "open_positions", "closed_positions"):
        for row in payload.get(key) or []:
            if isinstance(row, Mapping):
                item = dict(row)
                item["source_artifact"] = str(path)
                rows.append(item)
    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        position_id = text_v1(row.get("position_id") or row.get("paper_position_id") or row.get("candidate_id"))
        if position_id:
            dedup[position_id] = {**dedup.get(position_id, {}), **row}
    return list(dedup.values())


def validation_sample_rows_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    from ops.aegis.intelligence_common_v1 import read_json_v1
    path = report_path_v1(truth_root, "aegis_research_validation_samples_v1", day_utc, "research_validation_samples.v1.json")
    payload = read_json_v1(path)
    return [{**dict(row), "source_artifact": str(path)} for row in payload.get("samples") or [] if isinstance(row, Mapping)]


def validation_result_rows_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    from ops.aegis.intelligence_common_v1 import read_json_v1
    path = report_path_v1(truth_root, "aegis_research_validation_result_v1", day_utc, "research_validation_result.v1.json")
    payload = read_json_v1(path)
    return [{**dict(row), "source_artifact": str(path)} for row in payload.get("results") or [] if isinstance(row, Mapping)]


def sleeve_mapping_rows_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    rows = []
    for sleeve_id in observed_sleeve_ids_v1(truth_root=truth_root, day_utc=day_utc):
        mapping = declared_or_legacy_mapping_v1({"sleeve_id": sleeve_id})
        rows.append({
            "sleeve_id": sleeve_id,
            "hypothesis_id": mapping.get("hypothesis_id", ""),
            "thesis_id": mapping.get("thesis_id", ""),
            "implementation_version": "v1",
            "candidate_generation_rules": f"legacy sleeve candidate generation rules for {sleeve_id}",
            "validation_rules": "validation grouped by hypothesis first, sleeve second",
            "paper_trade_rules": "manual-only paper workflow; no broker execution",
            "relationship_state": "PAPER_TESTING",
            "mapping_confidence": mapping.get("mapping_confidence"),
            "mapping_source": mapping.get("mapping_source"),
        })
    return rows


def mapping_for_candidate_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return declared_or_legacy_mapping_v1(row, sleeve_id=text_v1(row.get("sleeve_id")))


def mapping_for_position_v1(row: Mapping[str, Any], candidate_index: Mapping[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
    direct = declared_or_legacy_mapping_v1(row, sleeve_id=text_v1(row.get("sleeve_id")))
    if direct.get("hypothesis_id"):
        return direct
    lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}
    direct = declared_or_legacy_mapping_v1(lineage, sleeve_id=text_v1(lineage.get("sleeve_id")))
    if direct.get("hypothesis_id"):
        return direct
    candidate_id = text_v1(row.get("candidate_id") or lineage.get("candidate_id"))
    candidate = (candidate_index or {}).get(candidate_id) or {}
    return mapping_for_candidate_v1(candidate)


def build_hypothesis_registry_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    sleeves = sleeve_mapping_rows_v1(truth_root=truth_root, day_utc=day_utc)
    candidates = candidate_rows_v1(truth_root=truth_root, day_utc=day_utc)
    candidate_index = {text_v1(row.get("candidate_id") or row.get("candidate_contract_id")): row for row in candidates if text_v1(row.get("candidate_id") or row.get("candidate_contract_id"))}
    positions = paper_position_rows_v1(truth_root=truth_root, day_utc=day_utc)
    samples = validation_sample_rows_v1(truth_root=truth_root, day_utc=day_utc)
    results = validation_result_rows_v1(truth_root=truth_root, day_utc=day_utc)

    buckets: dict[str, dict[str, Any]] = {}
    def ensure(mapping: Mapping[str, Any]) -> dict[str, Any] | None:
        hypothesis_id = text_v1(mapping.get("hypothesis_id"))
        thesis_id = text_v1(mapping.get("thesis_id"))
        if not hypothesis_id or not thesis_id:
            return None
        if hypothesis_id not in buckets:
            rule = next((r for r in SLEEVE_RULES.values() if r["hypothesis_id"] == hypothesis_id), {})
            buckets[hypothesis_id] = {
                "hypothesis_id": hypothesis_id,
                "thesis_id": thesis_id,
                "name": rule.get("hypothesis_name") or hypothesis_id,
                "formal_claim": rule.get("formal_claim") or "Legacy-inferred testable claim.",
                "market_universe": rule.get("market_universe") or "Unspecified legacy universe",
                "signal_definition": rule.get("signal_definition") or "Legacy sleeve signal definition",
                "expected_behavior": rule.get("expected_behavior") or "Positive evidence accumulation under validation.",
                "invalidation_criteria": rule.get("invalidation_criteria") or "Failed validation or persistent negative expectancy.",
                "status": "INVESTIGATING",
                "validation_state": "UNVALIDATED",
                "evidence_score": 0.0,
                "confidence_score": 0.0,
                "linked_sleeves": [],
                "linked_candidates": [],
                "linked_paper_positions": [],
                "linked_outcomes": [],
                "linked_validation_samples": [],
                "linked_validation_results": [],
                "evidence_objects": [],
                "mapping_confidence": mapping.get("mapping_confidence") or "LEGACY_INFERRED",
            }
        return buckets[hypothesis_id]

    for sleeve in sleeves:
        bucket = ensure(sleeve)
        if bucket is not None:
            bucket["linked_sleeves"].append(sleeve["sleeve_id"])
            bucket["evidence_objects"].append(_evidence("sleeve_mapping", sleeve, day_utc, sleeve_id=sleeve["sleeve_id"]))
    for row in candidates:
        mapping = mapping_for_candidate_v1(row)
        bucket = ensure(mapping)
        if bucket is not None:
            cid = text_v1(row.get("candidate_id") or row.get("candidate_contract_id"))
            bucket["linked_candidates"].append(cid)
            bucket["evidence_objects"].append(_evidence("candidate_contract", row, day_utc, sleeve_id=text_v1(row.get("sleeve_id")), candidate_id=cid))
    for row in positions:
        mapping = mapping_for_position_v1(row, candidate_index)
        bucket = ensure(mapping)
        if bucket is not None:
            pid = text_v1(row.get("position_id") or row.get("paper_position_id") or row.get("candidate_id"))
            bucket["linked_paper_positions"].append(pid)
            if text_v1(row.get("realized_pnl")) or text_v1(row.get("exit_time")):
                bucket["linked_outcomes"].append(pid)
            bucket["evidence_objects"].append(_evidence("paper_position", row, day_utc, sleeve_id=text_v1(row.get("sleeve_id")), candidate_id=text_v1(row.get("candidate_id")), position_id=pid))
    for row in samples:
        mapping = declared_or_legacy_mapping_v1(row, sleeve_id=text_v1(row.get("sleeve_id")))
        bucket = ensure(mapping)
        if bucket is not None:
            sid = text_v1(row.get("sample_id") or row.get("source_test_result_path"))
            bucket["linked_validation_samples"].append(sid)
            bucket["evidence_objects"].append(_evidence("validation_sample", row, day_utc, sleeve_id=text_v1(row.get("sleeve_id")), candidate_id=text_v1(row.get("candidate_id"))))
    for row in results:
        mapping = declared_or_legacy_mapping_v1(row, sleeve_id=text_v1(row.get("sleeve_id")))
        if not mapping.get("hypothesis_id") and text_v1(row.get("hypothesis_id")) in buckets:
            mapping = {"hypothesis_id": text_v1(row.get("hypothesis_id")), "thesis_id": buckets[text_v1(row.get("hypothesis_id"))]["thesis_id"], "mapping_confidence": "SOURCE_DECLARED", "mapping_source": "validation_result.hypothesis_id"}
        bucket = ensure(mapping)
        if bucket is not None:
            rid = text_v1(row.get("result_id") or row.get("hypothesis_id"))
            bucket["linked_validation_results"].append(rid)
            bucket["evidence_objects"].append(_evidence("validation_result", row, day_utc))
            if text_v1(row.get("validation_status") or row.get("status")).upper() in {"SUPPORTED", "VALIDATED", "PASS"}:
                bucket["validation_state"] = "SUPPORTED"
    hypotheses = []
    for row in buckets.values():
        for key in ("linked_sleeves", "linked_candidates", "linked_paper_positions", "linked_outcomes", "linked_validation_samples", "linked_validation_results"):
            row[key] = sorted(set(x for x in row[key] if x))
        sample_count = len(row["linked_validation_samples"]) + len(row["linked_validation_results"])
        evidence_count = len(row["evidence_objects"])
        row["evidence_score"] = min(100.0, round(evidence_count * 4.0, 6))
        row["confidence_score"] = min(100.0, round(sample_count * 12.5 + len(row["linked_paper_positions"]) * 1.5, 6))
        hypotheses.append(row)
    hypotheses.sort(key=lambda row: row["hypothesis_id"])
    payload = {
        "schema_id": "aegis_hypothesis_registry",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "hypotheses": hypotheses,
        "sleeve_implementations": sleeves,
        "candidate_hypothesis_links": _candidate_links(candidates),
        "paper_position_hypothesis_links": _position_links(positions, candidate_index),
        "summary": {"hypothesis_count": len(hypotheses), "sleeve_mapping_count": len(sleeves), "candidate_count": len(candidates), "paper_position_count": len(positions)},
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_hypothesis_registry_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(hypothesis_registry_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_hypothesis_registry_v1(truth_root=truth_root, day_utc=day_utc))


def _candidate_links(candidates: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out=[]
    for row in candidates:
        mapping=mapping_for_candidate_v1(row)
        out.append({"candidate_id": text_v1(row.get("candidate_id") or row.get("candidate_contract_id")), "sleeve_id": text_v1(row.get("sleeve_id")), **mapping})
    return out


def _position_links(positions: list[Mapping[str, Any]], candidate_index: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    out=[]
    for row in positions:
        mapping=mapping_for_position_v1(row, candidate_index)
        out.append({"position_id": text_v1(row.get("position_id") or row.get("paper_position_id") or row.get("candidate_id")), "candidate_id": text_v1(row.get("candidate_id")), "sleeve_id": text_v1(row.get("sleeve_id")), **mapping})
    return out


def _evidence(evidence_type: str, row: Mapping[str, Any], day_utc: str, *, sleeve_id: str = "", candidate_id: str = "", position_id: str = "") -> dict[str, Any]:
    mapping = declared_or_legacy_mapping_v1(row, sleeve_id=sleeve_id or text_v1(row.get("sleeve_id")))
    source = text_v1(row.get("source_artifact") or row.get("lineage_source_path") or row.get("evidence_path"))
    eid = stable_hash_v1({"type": evidence_type, "hypothesis": mapping.get("hypothesis_id"), "sleeve": sleeve_id, "candidate": candidate_id, "position": position_id, "source": source})[:24]
    return {
        "evidence_id": f"evidence_{eid}",
        "hypothesis_id": mapping.get("hypothesis_id", ""),
        "sleeve_id": sleeve_id,
        "candidate_id": candidate_id,
        "position_id": position_id,
        "evidence_type": evidence_type,
        "source_artifact": source,
        "source_hash": "",
        "day_utc": str(day_utc),
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "validity_status": "VALID" if mapping.get("hypothesis_id") else "UNMAPPED",
        "notes": mapping.get("mapping_source", ""),
    }
