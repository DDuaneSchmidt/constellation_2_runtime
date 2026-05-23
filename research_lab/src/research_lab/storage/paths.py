from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def research_lab_root() -> Path:
    return project_root() / "research_lab"


def research_store_root(root: Path | None = None) -> Path:
    return (root or research_lab_root() / "research_store").resolve()


def ensure_store_layout(root: Path | None = None) -> Path:
    store = research_store_root(root)
    for rel in [
        "universes",
        "datasets",
        "evidence_packages",
        "regimes",
        "cost_models",
        "candidate_batches",
        "operator_decisions",
        "outcome_records",
        "sleeves",
        "longitudinal_runs",
        "paper_trials",
        "portfolio_reports/evidence_inventory",
        "portfolio_reports/paper_trial_inventory",
        "portfolio_reports/sleeve_comparison",
        "portfolio_reports/backlog_priority",
        "registries",
        "audit_log",
        "event_intake/event_families",
        "event_intake/observations",
        "event_intake/clusters",
        "event_intake/intents",
        "event_intake/hypothesis_proposals",
        "research_intake/readiness_assessments",
        "research_intake/priority_scores",
        "research_intake/dossiers",
        "research_intake/reviews",
        "breadth",
        "macro_events",
        "projections/hypothesis_queue/builds",
        "projections/operator_home/builds",
        "projections/projection_health",
        "tmp",
    ]:
        (store / rel).mkdir(parents=True, exist_ok=True)
    return store


def resolve_research_uri(uri: str, *, store_root: Path | None = None) -> Path:
    store = research_store_root(store_root)
    text = str(uri or "").strip()
    if text.startswith("research://universes/"):
        return store / "universes" / text.removeprefix("research://universes/")
    if text.startswith("research://datasets/"):
        return store / "datasets" / text.removeprefix("research://datasets/")
    if text.startswith("research://evidence/"):
        return store / "evidence_packages" / text.removeprefix("research://evidence/")
    if text.startswith("research://regimes/"):
        return store / "regimes" / text.removeprefix("research://regimes/")
    if text.startswith("research://cost_models/"):
        return store / "cost_models" / text.removeprefix("research://cost_models/")
    if text.startswith("research://candidate_batches/"):
        return store / "candidate_batches" / text.removeprefix("research://candidate_batches/")
    if text.startswith("research://sleeves/"):
        return store / "sleeves" / text.removeprefix("research://sleeves/")
    if text.startswith("research://research_plans/"):
        return store / "research_plans" / text.removeprefix("research://research_plans/")
    if text == "research://registries/datasets":
        return store / "registries" / "dataset_snapshots.jsonl"
    if text == "research://registries/universes":
        return store / "registries" / "universe_snapshots.jsonl"
    if text == "research://registries/evidence":
        return store / "registries" / "evidence_packages.jsonl"
    if text.startswith("research://audit/"):
        return store / "audit_log" / "audit_events.jsonl"
    raise ValueError(f"Unsupported research URI: {uri}")


def universe_uri(universe_snapshot_id: str) -> str:
    return f"research://universes/{universe_snapshot_id}"


def dataset_uri(dataset_snapshot_id: str) -> str:
    return f"research://datasets/{dataset_snapshot_id}"


def evidence_uri(evidence_package_id: str) -> str:
    return f"research://evidence/{evidence_package_id}"


def regime_uri(regime_snapshot_id: str) -> str:
    return f"research://regimes/{regime_snapshot_id}"


def cost_model_uri(cost_model_snapshot_id: str) -> str:
    return f"research://cost_models/{cost_model_snapshot_id}"


def candidate_batch_uri(candidate_batch_id: str) -> str:
    return f"research://candidate_batches/{candidate_batch_id}"


def sleeve_uri(sleeve_id: str) -> str:
    return f"research://sleeves/{sleeve_id}"
