from __future__ import annotations

from pathlib import Path

from research_lab.event_intake.event_intake_registry import (
    capture_event_observation,
    create_event_cluster,
    generate_hypothesis_proposal,
    generate_intent_candidate,
    seed_event_families,
)
from research_lab.recovery.hypothesis_recovery import build_forensic_inventory, recover_hypotheses
from research_lab.storage.manifest_io import read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


def _canonical_chain(store: Path) -> str:
    seed_event_families(store_root=store, actor="test")
    obs = capture_event_observation(
        store_root=store,
        actor="test",
        event_family_id="oil_shock",
        title="Oil shock observation",
        description="Oil shock reversal research idea",
        source_type="operator_observation",
        source_ref="test",
        symbols_mentioned=["USO", "XLE"],
        suspected_mechanism="oil shock reversal",
        confidence_level="low",
        research_priority="watchlist",
        notes="test",
    )["event_observation"]
    cluster = create_event_cluster(event_family_id="oil_shock", observation_ids=[obs["event_observation_id"]], cluster_title="Oil shock cluster", store_root=store, actor="test")["event_cluster"]
    intent = generate_intent_candidate(event_cluster_id=cluster["event_cluster_id"], store_root=store, actor="test")["intent_candidate"]
    proposal = generate_hypothesis_proposal(intent_candidate_id=intent["intent_candidate_id"], store_root=store, actor="test")["hypothesis_proposal"]
    return proposal["hypothesis_proposal_id"]


def test_forensic_scanner_finds_canonical_orphan_and_text(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path / "store")
    _canonical_chain(store)
    fixture = tmp_path / "operator.log"
    fixture.write_text("Research idea: breadth collapse recovery after weak breadth. Hypothesis proposal only.\n", encoding="utf-8")
    orphan = capture_event_observation(
        store_root=store,
        actor="test",
        event_family_id="gap_event",
        title="Gap fill orphan observation",
        description="Gap-fill after large overnight gaps",
        source_type="operator_observation",
        source_ref="test",
        symbols_mentioned=["SPY"],
        suspected_mechanism="gap fill",
        confidence_level="low",
        research_priority="watchlist",
        notes="orphan",
    )["event_observation"]
    items, _ = build_forensic_inventory(store_root=store, scan_roots=[store, fixture])
    assert any(item["source_type"] == "canonical_proposal" for item in items)
    assert any(item["source_type"] == "event_observation" and item["raw_title"] == orphan["title"] for item in items)
    assert any("breadth collapse recovery" in item["raw_text"].lower() for item in items)


def test_existing_canonical_proposal_is_not_duplicated(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path / "store")
    _canonical_chain(store)
    result = recover_hypotheses(dry_run=False, seed_defaults=False, store_root=store, scan_roots=[store], actor="test")
    assert result["report"]["canonical_before_count"] == 1
    assert result["report"]["canonical_after_count"] == 1
    assert result["report"]["recovered_count"] == 0


def test_orphan_observation_recovers_into_full_proposal_chain(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path / "store")
    seed_event_families(store_root=store, actor="test")
    capture_event_observation(
        store_root=store,
        actor="test",
        event_family_id="gap_event",
        title="Gap fill orphan observation",
        description="Gap-fill after large overnight gaps",
        source_type="operator_observation",
        source_ref="test",
        symbols_mentioned=["SPY"],
        suspected_mechanism="gap fill",
        confidence_level="low",
        research_priority="watchlist",
        notes="orphan",
    )
    result = recover_hypotheses(dry_run=False, seed_defaults=False, store_root=store, scan_roots=[store], actor="test")
    assert result["report"]["recovered_count"] == 1
    proposal_id = result["report"]["recovered_proposals"][0]["hypothesis_proposal_id"]
    assert (store / "event_intake" / "hypothesis_proposals" / f"{proposal_id}.json").exists()
    assert read_jsonl(store / "registries" / "research_readiness_assessments.jsonl")
    assert read_jsonl(store / "registries" / "proposal_priority_scores.jsonl")
    assert read_jsonl(store / "registries" / "research_intake_dossiers.jsonl")


def test_ambiguous_operator_note_recovers_as_watchlist_needs_operator_review(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path / "store")
    note = tmp_path / "operator_note.log"
    note.write_text("Possible hypothesis: lottery macro/geopolitical dislocation. Unverified claim, proposal only.\n", encoding="utf-8")
    result = recover_hypotheses(dry_run=False, seed_defaults=False, store_root=store, scan_roots=[note], actor="test")
    assert result["report"]["recovered_count"] == 1
    proposal_id = result["report"]["recovered_proposals"][0]["hypothesis_proposal_id"]
    proposal = read_json(store / "event_intake" / "hypothesis_proposals" / f"{proposal_id}.json")
    assert proposal["proposal_status"] == "watchlist"
    assert proposal["recovery_metadata"]["needs_operator_review"] is True


def test_default_seed_list_is_idempotent_and_projection_includes_all(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path / "store")
    first = recover_hypotheses(dry_run=False, seed_defaults=True, store_root=store, scan_roots=[], actor="test")
    second = recover_hypotheses(dry_run=False, seed_defaults=True, store_root=store, scan_roots=[], actor="test")
    assert first["report"]["seeded_default_count"] == 20
    assert second["report"]["seeded_default_count"] == 0
    assert second["report"]["canonical_after_count"] == 20
    projection = read_json(store / "projections" / "hypothesis_queue" / "latest.json")
    assert len(projection["items"]) == 20
    assert any("missing_breadth_snapshot" in item["blocking_items"] for item in projection["items"])
    oil = [item for item in projection["items"] if item["provenance"]["event_family_id"] == "oil_shock"]
    assert oil and "missing_required_symbols" in oil[0]["blocking_items"]


def test_recovery_report_counts_and_no_auto_research_plan_or_sleeve(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path / "store")
    result = recover_hypotheses(dry_run=False, seed_defaults=True, store_root=store, scan_roots=[], actor="test")
    report = result["report"]
    assert report["canonical_before_count"] == 0
    assert report["canonical_after_count"] == 20
    assert report["projection_integrity_status"] == "ok"
    assert (store / "recovery" / "hypothesis_recovery" / "hypothesis_recovery_report.json").exists()
    assert read_jsonl(store / "registries" / "research_plans.jsonl") == []
    assert read_jsonl(store / "registries" / "sleeve_definitions.jsonl") == []
