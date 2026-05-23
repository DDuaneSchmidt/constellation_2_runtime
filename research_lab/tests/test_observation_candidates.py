from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_observation_candidate_batch_latest_v1,
    research_lab_observation_candidate_batch_v1,
    research_lab_observation_candidate_batches_v1,
    research_lab_observation_candidate_latest_v1,
    research_lab_observation_candidate_v1,
    research_lab_observation_candidates_v1,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.observations.observation_batch import build_observation_candidate_batch, write_observation_candidate_batch
from research_lab.observations.observation_scanner import run_observation_scanners
from research_lab.status.research_os_status import write_research_os_status_report
from research_lab.storage.manifest_io import read_jsonl
from research_lab.tests.test_research_os_status_report import _failed_integrity_fixture


def _counts(store: Path) -> dict[str, int]:
    return {
        "hypotheses": len(list((store / "research_plans").rglob("*.json"))) if (store / "research_plans").exists() else 0,
        "challengers": len(list((store / "challenger_tracks").glob("*.json"))) if (store / "challenger_tracks").exists() else 0,
        "sleeves": len(list((store / "sleeves").rglob("*.json"))) if (store / "sleeves").exists() else 0,
        "paper_trials": len(list((store / "paper_trials").rglob("*.json"))) if (store / "paper_trials").exists() else 0,
        "paper_trial_proposals": len(list((store / "paper_trial_proposals").glob("*.json"))) if (store / "paper_trial_proposals").exists() else 0,
        "candidate_batches": len(list((store / "candidate_batches").rglob("*.json"))) if (store / "candidate_batches").exists() else 0,
        "observations": len(list((store / "observation_candidates").glob("*.json"))) if (store / "observation_candidates").exists() else 0,
        "batches": len(list((store / "observation_candidate_batches").glob("*.json"))) if (store / "observation_candidate_batches").exists() else 0,
        "audit": len(read_jsonl(store / "audit_log" / "audit_events.jsonl")),
    }


def test_research_store_scanner_emits_red_status_and_duplicate_observations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")

    observations, statuses = run_observation_scanners(store_root=store, scanners=["research_store"], generated_at="2026-05-19T00:00:00Z")

    assert any(row["status"] == "completed" for row in statuses)
    assert any(row["observation_type"] == "research_store_anomaly" and row["actionability_status"] == "non_actionable_research_only" for row in observations)
    assert any("Research OS is RED" in row["observation_summary"] for row in observations)
    assert any("Duplicate registry entries" in row["observation_summary"] for row in observations)
    assert any(any(blocker.get("registry_name") == "paper_trials.jsonl" for blocker in row["blockers"]) for row in observations)
    for row in observations:
        validate_contract("observation_candidate", row)


def test_challenger_scanner_and_market_unavailable_do_not_fail(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")

    observations, statuses = run_observation_scanners(store_root=store, scanners=["challenger", "market"], generated_at="2026-05-19T00:00:00Z")

    assert any(row["scanner"] == "market" and row["status"] == "market_data_unavailable" for row in statuses)
    assert any(row["observation_family"] == "research_quality" for row in observations)
    assert all("buy" not in row["observation_summary"].lower() for row in observations)


def test_market_threshold_scanner_emits_price_and_volume_observations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")
    market_dir = store / "market_data"
    market_dir.mkdir(parents=True)
    lines = ["symbol,date,open,high,low,close,volume"]
    close = 100.0
    for idx in range(25):
        date = f"2026-04-{idx + 1:02d}"
        close = close * (1.001 if idx % 2 == 0 else 0.999)
        volume = 1000
        if idx == 24:
            close = 112.0
            volume = 4000
        lines.append(f"XYZ,{date},{close - 0.1},{close + 0.2},{close - 0.2},{close},{volume}")
    (market_dir / "ohlcv.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")

    observations, statuses = run_observation_scanners(store_root=store, scanners=["market"], generated_at="2026-05-19T00:00:00Z")

    assert any(row["scanner"] == "market" and row["status"] == "completed" for row in statuses)
    assert any(row["observation_type"] == "abnormal_price_move" for row in observations)
    assert any(row["observation_type"] == "abnormal_volume" for row in observations)


def test_observation_batch_persistence_registry_audit_api_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")
    before = _counts(store)

    result = write_observation_candidate_batch(store_root=store, scanners=["all"], generated_at="2026-05-19T00:00:00Z")
    after = _counts(store)

    batch = result["batch"]
    validate_contract("observation_candidate_batch", batch)
    assert batch["observation_count"] > 0
    assert batch["non_actionable_research_only_assertion"]["paper_trial_created"] is False
    assert after["hypotheses"] == before["hypotheses"]
    assert after["challengers"] == before["challengers"]
    assert after["sleeves"] == before["sleeves"]
    assert after["paper_trials"] == before["paper_trials"]
    assert after["paper_trial_proposals"] == before["paper_trial_proposals"]
    assert after["candidate_batches"] == before["candidate_batches"]
    assert after["observations"] == before.get("observations", 0) + batch["observation_count"]
    assert after["batches"] == before.get("batches", 0) + 1
    assert after["audit"] == before["audit"] + batch["observation_count"] + 1
    assert read_jsonl(store / "registries" / "observation_candidate_batch_registry.json")[-1]["observation_candidate_batch_id"] == batch["observation_candidate_batch_id"]
    assert len(read_jsonl(store / "registries" / "observation_candidate_registry.json")) == batch["observation_count"]

    listing = research_lab_observation_candidates_v1(store_root=store)
    latest_candidate = research_lab_observation_candidate_latest_v1(store_root=store)
    candidate_detail = research_lab_observation_candidate_v1(observation_candidate_id=latest_candidate["observation_candidate"]["observation_candidate_id"], store_root=store)
    batch_listing = research_lab_observation_candidate_batches_v1(store_root=store)
    latest_batch = research_lab_observation_candidate_batch_latest_v1(store_root=store)
    batch_detail = research_lab_observation_candidate_batch_v1(observation_candidate_batch_id=batch["observation_candidate_batch_id"], store_root=store)
    assert listing["read_only"] is True
    assert candidate_detail["read_only"] is True
    assert batch_listing["read_only"] is True
    assert latest_batch["observation_candidate_batch"]["observation_candidate_batch_id"] == batch["observation_candidate_batch_id"]
    assert batch_detail["observation_candidate_batch"]["observation_candidate_batch_id"] == batch["observation_candidate_batch_id"]


def test_observation_batch_fingerprint_is_deterministic_except_generated_ids(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")

    first_obs, first_statuses = run_observation_scanners(store_root=store, scanners=["research_store"], generated_at="2026-05-19T00:00:00Z")
    second_obs, second_statuses = run_observation_scanners(store_root=store, scanners=["research_store"], generated_at="2026-05-19T00:01:00Z")
    first = build_observation_candidate_batch(observations=first_obs, scanner_statuses=first_statuses, generated_at="2026-05-19T00:00:00Z", scanner_run_id="run_a")
    second = build_observation_candidate_batch(observations=second_obs, scanner_statuses=second_statuses, generated_at="2026-05-19T00:01:00Z", scanner_run_id="run_b")

    assert first["observation_candidate_batch_id"] != second["observation_candidate_batch_id"]
    assert first["immutable_hash"] == second["immutable_hash"]
