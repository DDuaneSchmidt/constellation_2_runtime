from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_integrity_latest_summary_v1,
    research_lab_integrity_report_latest_v1,
    research_lab_integrity_report_v1,
    research_lab_integrity_reports_v1,
)
from research_lab.challengers.challenger_comparison import write_challenger_comparison_report
from research_lab.challengers.challenger_evidence import write_challenger_evidence_batch
from research_lab.challengers.human_review_decision import write_human_review_decision
from research_lab.challengers.human_review_dossier import write_human_review_dossier
from research_lab.integrity.research_store_integrity import (
    build_research_store_integrity_report,
    write_research_store_integrity_report,
)
from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.tests.test_challenger_evidence_batch import _write_fixture


def _fixture_store(store: Path) -> dict[str, str]:
    track_id = _write_fixture(store)
    batch = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)["batch"]
    report = write_challenger_comparison_report(challenger_evidence_batch_id=batch["challenger_evidence_batch_id"], store_root=store)["report"]
    dossier = write_human_review_dossier(challenger_comparison_report_id=report["challenger_comparison_report_id"], store_root=store)["dossier"]
    decision = write_human_review_decision(
        human_review_dossier_id=dossier["human_review_dossier_id"],
        challenger_id=(dossier["review_candidates"] or dossier["blocked_or_excluded_items"])[0]["challenger_hypothesis_id"],
        decision="request_more_challenger_evidence",
        decided_by="david",
        rationale="Need more regime-specific evidence before opening paper trial.",
        store_root=store,
    )["decision"]
    return {
        "track_id": track_id,
        "batch_id": batch["challenger_evidence_batch_id"],
        "comparison_id": report["challenger_comparison_report_id"],
        "dossier_id": dossier["human_review_dossier_id"],
        "decision_id": decision["human_review_decision_id"],
    }


def _count_errors(report: dict) -> int:
    return sum(
        1
        for field in ["missing_references", "hash_mismatches", "schema_validation_failures", "lineage_breaks", "duplicate_registry_entries", "mutation_boundary_violations"]
        for row in report[field]
        if row.get("severity") == "ERROR"
    )


def test_clean_store_pass_or_expected_warnings_and_blocker_info(tmp_path: Path) -> None:
    ids = _fixture_store(tmp_path / "store")

    report = build_research_store_integrity_report(store_root=tmp_path / "store")

    assert report["overall_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert _count_errors(report) == 0
    assert any(row.get("severity") == "INFO" and row.get("reason") == "cost_sensitivity_requires_supported_cost_model_snapshot" for row in report["unresolved_blockers"])
    assert ids["batch_id"] in report["source_registry_ids"]["challenger_evidence_batches.jsonl"]


def test_missing_reference_and_registry_missing_artifact_errors(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _fixture_store(store)
    batch_path = store / "challenger_evidence_batches" / f"{ids['batch_id']}.json"
    batch = read_json(batch_path)
    batch["challenger_evidence_items"][0]["event_study_evidence_id"] = "missing_event_study"
    batch["content_hash"] = content_hash(batch, exclude={"generated_at"}, sort_lists=True)
    write_json(batch_path, batch, overwrite=True)
    append_jsonl(store / "registries" / "challenger_variants.jsonl", {"challenger_variant_id": "missing_variant"})

    report = build_research_store_integrity_report(store_root=store)

    assert report["overall_status"] == "FAIL"
    assert any(row.get("artifact_id") == "missing_event_study" for row in report["missing_references"])
    assert any(row.get("registry_entry_id") == "missing_variant" for row in report["missing_references"])


def test_orphan_hash_schema_audit_duplicate_and_mutation_findings(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _fixture_store(store)
    write_json(store / "challenger_variants" / "orphan_variant.json", {"challenger_variant_id": "orphan_variant", "research_label": "RESEARCH_ONLY", "immutable_hash": "abc123abc123"}, overwrite=False)
    decision_path = store / "human_review_decisions" / f"{ids['decision_id']}.json"
    decision = read_json(decision_path)
    decision["rationale"] = ""
    decision["governance_constraints"]["paper_trial_created"] = True
    write_json(decision_path, decision, overwrite=True)
    registry = store / "registries" / "human_review_decision_registry.json"
    append_jsonl(registry, read_jsonl(registry)[0])
    audit_path = store / "audit_log" / "audit_events.jsonl"
    audit_rows = [row for row in read_jsonl(audit_path) if row.get("entity_id") != ids["decision_id"]]
    audit_path.write_text("\n".join(__import__("json").dumps(row, sort_keys=True, separators=(",", ":")) for row in audit_rows) + "\n", encoding="utf-8")

    report = build_research_store_integrity_report(store_root=store)

    assert report["overall_status"] == "FAIL"
    assert report["orphaned_artifacts"]
    assert report["hash_mismatches"]
    assert report["schema_validation_failures"]
    assert report["duplicate_registry_entries"]
    assert report["mutation_boundary_violations"]
    assert any(row.get("required_action") == "human_review_decision_recorded" for row in report["warnings"])


def test_lineage_completeness_error(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _fixture_store(store)
    batch_path = store / "challenger_evidence_batches" / f"{ids['batch_id']}.json"
    batch = read_json(batch_path)
    batch["challenger_evidence_items"][0]["longitudinal_run_id"] = ""
    batch["content_hash"] = content_hash(batch, exclude={"generated_at"}, sort_lists=True)
    write_json(batch_path, batch, overwrite=True)

    report = build_research_store_integrity_report(store_root=store)

    assert any(row.get("field") == "longitudinal_run_id" for row in report["lineage_breaks"])


def test_persistence_registry_audit_api_and_determinism(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _fixture_store(store)

    first = build_research_store_integrity_report(store_root=store)
    second = build_research_store_integrity_report(store_root=store)
    result = write_research_store_integrity_report(store_root=store)

    assert first["immutable_hash"] == second["immutable_hash"]
    assert read_jsonl(store / "registries" / "integrity_report_registry.json")[-1]["integrity_report_id"] == result["report"]["integrity_report_id"]
    assert any(row["action"] == "research_store_integrity_report_generated" and row["entity_id"] == result["report"]["integrity_report_id"] for row in read_jsonl(store / "audit_log" / "audit_events.jsonl"))
    listing = research_lab_integrity_reports_v1(store_root=store)
    latest = research_lab_integrity_report_latest_v1(store_root=store)
    detail = research_lab_integrity_report_v1(integrity_report_id=result["report"]["integrity_report_id"], store_root=store)
    summary = research_lab_integrity_latest_summary_v1(store_root=store)
    assert listing["read_only"] is True
    assert latest["integrity_report"]["integrity_report_id"] == result["report"]["integrity_report_id"]
    assert detail["integrity_report"]["integrity_report_id"] == result["report"]["integrity_report_id"]
    assert summary["integrity_report_id"] == result["report"]["integrity_report_id"]
