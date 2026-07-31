from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .edge_qualification import qualify_edge
from .edge_qualification_models import EdgeQualificationInput
from .historical_replay_engine import create_historical_replay_request, now_utc, run_historical_replay, stable_id
from .historical_replay_results import edge_input_with_historical_replay, route_historical_replay_backlog_items
from .mechanism_search_governance import validate_mechanism_hypothesis, validate_mechanism_search_request, validate_mechanism_search_run
from .mechanism_search_models import MechanismHypothesis, MechanismSearchRun
from .mechanism_search_space import iter_search_space
from .paper_trade_candidate_reports import build_paper_trade_candidate_report


def generate_mechanism_hypotheses(
    *,
    limit: int,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str | None = None,
    write_artifacts: bool = True,
    replay_limit: int = 10,
) -> dict[str, Any]:
    validate_mechanism_search_request(limit=limit)
    created = created_at or now_utc()
    run_id = stable_id("mechanism_search", [created, limit])
    store = ArtifactStore(root)
    source_id = f"question-{run_id}"
    if write_artifacts and not store.exists(source_id):
        store.create_artifact(
            artifact_id=source_id,
            artifact_type=ArtifactType.QUESTION.value,
            created_at=created,
            created_by="atlas_mechanism_search",
            confidence=0.25,
            evidence_level="GENERATED_ONLY",
            labels=["mechanism_search", "research_only"],
            metadata={"question": "Which bounded technical mechanism hypotheses should be replayed first?", "research_only": True},
            is_root=True,
        )

    hypotheses: list[dict[str, Any]] = []
    pipeline_results: list[dict[str, Any]] = []
    search_space = iter_search_space()
    for index in range(limit):
        row = dict(search_space[index % len(search_space)])
        variation_index = index // len(search_space)
        if variation_index:
            row["source_search_config"] = {
                **dict(row["source_search_config"]),
                "metadata": {
                    **dict(row["source_search_config"].get("metadata", {})),
                    "bounded_variation_index": variation_index,
                },
            }
        hypothesis = _build_hypothesis(row, index=index, created_at=created, source_artifact_ids=[source_id] if write_artifacts else [])
        validate_mechanism_hypothesis(hypothesis)
        hypotheses.append(hypothesis)
        if write_artifacts and not store.exists(hypothesis["hypothesis_id"]):
            store.create_artifact(
                artifact_id=hypothesis["hypothesis_id"],
                artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value,
                created_at=created,
                created_by="atlas_mechanism_search",
                source_artifact_ids=[source_id],
                confidence=0.30,
                evidence_level="GENERATED_ONLY",
                lifecycle_state="NEW",
                labels=["mechanism_search", hypothesis["mechanism"], "research_only"],
                metadata=hypothesis,
            )
        if index < replay_limit:
            pipeline_results.append(_run_research_pipeline(hypothesis, root=root, created_at=created))

    run = MechanismSearchRun(
        run_id=run_id,
        created_at=created,
        requested_limit=limit,
        emitted_count=len(hypotheses),
        hypotheses=hypotheses,
        governance_result={"status": "PENDING"},
        pipeline_results=pipeline_results,
        metadata={
            "research_only": True,
            "write_artifacts": write_artifacts,
            "pipeline": ["ResearchHypothesis", "HistoricalReplay", "EdgeQualification", "PaperTradeCandidate gate"],
            "pipeline_replay_limit": replay_limit,
        },
    ).to_dict()
    run["governance_result"] = validate_mechanism_search_run(run)
    return run


def generate_mechanism_search_small(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    return generate_mechanism_hypotheses(limit=10, root=root, created_at=created_at, replay_limit=10)


def generate_mechanism_search_1000(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    return generate_mechanism_hypotheses(limit=1000, root=root, created_at=created_at, replay_limit=25)


def _build_hypothesis(row: dict[str, Any], *, index: int, created_at: str, source_artifact_ids: list[str]) -> dict[str, Any]:
    hypothesis_id = stable_id(
        "hyp_mechanism",
        [row["mechanism"], row["conditions"], row["regime"], row["timeframe"], index],
    )
    hypothesis = MechanismHypothesis(
        hypothesis_id=hypothesis_id,
        mechanism=row["mechanism"],
        conditions=list(row["conditions"]),
        regime=row["regime"],
        timeframe=row["timeframe"],
        entry_observation_rule=row["entry_observation_rule"],
        exit_observation_rule=row["exit_observation_rule"],
        invalidation_rule=row["invalidation_rule"],
        complexity_score=float(row["complexity_score"]),
        source_search_config=dict(row["source_search_config"]),
        source_artifact_ids=list(source_artifact_ids),
        created_at=created_at,
        metadata={"research_only": True, "bounded_generation_index": index},
    ).to_dict()
    return hypothesis


def _run_research_pipeline(hypothesis: dict[str, Any], *, root: str | Path, created_at: str) -> dict[str, Any]:
    request = create_historical_replay_request(
        hypothesis_id=hypothesis["hypothesis_id"],
        mechanism_tags=[hypothesis["mechanism"]],
        regime_context={"label": hypothesis["regime"]},
        time_window=_time_window_for_timeframe(hypothesis["timeframe"]),
        source_artifact_ids=hypothesis.get("source_artifact_ids", []),
        created_at=created_at,
        metadata={"mechanism_search_hypothesis_id": hypothesis["hypothesis_id"]},
    )
    replay = run_historical_replay(request, _synthetic_replay_samples(hypothesis), created_at=created_at)
    routed_items = route_historical_replay_backlog_items(replay, root=root, created_at=created_at)
    edge_input = EdgeQualificationInput(
        source_artifact_ids=list(hypothesis.get("source_artifact_ids", [])) + [replay["replay_id"]],
        source_hypothesis_ids=[hypothesis["hypothesis_id"]],
        evidence_maturity=0.35,
        research_effectiveness=0.45,
        hypothesis_survival=0.45,
        failure_history=0.25,
        duplicate_risk=0.20,
        regime_coverage=0.40,
        candidate_quality_trend=0.40,
        learning_validation_trend=0.40,
        lineage_complete=bool(hypothesis.get("source_artifact_ids")),
        governance_pass=True,
        generated_only=True,
        mechanism_tags=[hypothesis["mechanism"]],
        regime_context={"label": hypothesis["regime"]},
        evidence_level="GENERATED_ONLY",
        lifecycle_state="NEW",
        metadata={"mechanism_search": True},
    )
    replay_edge_input = edge_input_with_historical_replay(edge_input, replay)
    qualification = qualify_edge(replay_edge_input, created_at=created_at)
    candidate_report = build_paper_trade_candidate_report(replay_edge_input, candidate_id=f"ptc-{hypothesis['hypothesis_id']}", created_at=created_at)
    return {
        "hypothesis_id": hypothesis["hypothesis_id"],
        "historical_replay": {
            "replay_id": replay["replay_id"],
            "status": replay["certification"]["status"],
            "score": replay["metrics"]["historical_replay_score"],
            "sample_size": replay["sample_size"],
            "metrics": dict(replay.get("metrics") or {}),
        },
        "edge_qualification": {
            "qualification_id": qualification["qualification_id"],
            "eligible": qualification["eligible"],
            "edge_score": qualification["score"]["edge_score"],
            "disqualification_reasons": qualification["disqualification_reasons"],
        },
        "paper_trade_candidate_gate": {
            "candidate_id": candidate_report["candidate"]["candidate_id"],
            "certification_status": candidate_report["certification"]["status"],
            "paper_trade_eligible": candidate_report["paper_trade_eligible"],
            "human_review_required": candidate_report["human_review_required"],
        },
        "routed_backlog_items": routed_items,
    }


def _time_window_for_timeframe(timeframe: str) -> str:
    if timeframe in {"1m", "5m", "15m"}:
        return "90d"
    if timeframe == "1h":
        return "180d"
    return "1y"


def _synthetic_replay_samples(hypothesis: dict[str, Any]) -> list[dict[str, Any]]:
    seed = sum(ord(char) for char in hypothesis["hypothesis_id"])
    complexity = float(hypothesis["complexity_score"])
    base = 0.004 + (seed % 7) * 0.001 - complexity * 0.004
    samples: list[dict[str, Any]] = []
    for index in range(12):
        sign = -1 if (seed + index) % 5 == 0 else 1
        value = round(sign * (base + (index % 4) * 0.0015), 6)
        samples.append({"return": value, "regime": hypothesis["regime"]})
    return samples
