from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType, EvidenceLevel
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .backlog_seed_governance import validate_seed_item_governance
from .backlog_seed_reports import write_seed_report
from .backlog_seed_sources import collect_seed_sources, source_inventory_summary
from .backlog_seeding_models import DEFAULT_SEED_PROFILE, MAX_SEED_ITEMS, MECHANISM_FAMILIES, PROFILE_LIMITS, VARIATION_DIMENSIONS, BacklogSeedItem, BacklogSeedingResult
from .research_backlog import ResearchBacklog
from .semantic_deduplication import normalize_research_text

CREATED_BY = "atlas_v2_research_os_backlog_seeding"
AUTHORITY_BOUNDARY = "RESEARCH_ONLY_NO_AUTHORITY_EXPANSION"


def generate_seed_backlog_items(root: str | Path = DEFAULT_STORE_ROOT, *, profile: str = DEFAULT_SEED_PROFILE, limit: int | None = None, write: bool = True, day: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created_at = _now()
    requested_limit = _profile_limit(profile, limit)
    sources = collect_seed_sources(root_path)
    candidates: list[BacklogSeedItem] = []
    candidates.extend(generate_failure_analysis_items(sources, created_at=created_at))
    candidates.extend(generate_regime_gap_items(sources, created_at=created_at))
    candidates.extend(generate_duplicate_review_items(sources, created_at=created_at))
    candidates.extend(generate_edge_qualification_review_items(sources, created_at=created_at))
    candidates.extend(_artifact_followup_items(sources, created_at=created_at))
    candidates.extend(generate_mechanism_variation_items(sources, limit=requested_limit, created_at=created_at))
    deduped, duplicate_seed_ids = deduplicate_seed_items(candidates, root_path)
    selected = deduped[:requested_limit]
    store = ArtifactStore(root_path)
    backlog = ResearchBacklog(root_path)
    written_ids: list[str] = []
    blocked_ids: list[str] = []
    governance_violations: list[str] = []
    if write:
        for seed in selected:
            payload = seed.to_dict()
            gate = validate_seed_item_governance(payload)
            if gate["status"] != "PASS":
                governance_violations.extend(gate["violations"])
                blocked_ids.append(seed.seed_id)
                continue
            question_id = _ensure_seed_question_artifact(store, seed, created_at=created_at)
            source_ids = list(dict.fromkeys([*seed.source_artifact_ids, question_id]))
            if backlog.get(seed.backlog_item_id, required=False):
                duplicate_seed_ids.append(seed.seed_id)
                continue
            state = seed.state
            blocked_reason = seed.blocked_reason
            if not source_ids:
                state = "BLOCKED"
                blocked_reason = "MISSING_REQUIRED_SOURCE_LINEAGE"
                blocked_ids.append(seed.seed_id)
            item = backlog.create_backlog_item(
                backlog_item_id=seed.backlog_item_id,
                item_type=seed.item_type,
                title=seed.title,
                description=seed.description,
                created_at=created_at,
                created_by=CREATED_BY,
                source_artifact_ids=source_ids,
                state=state,
                blocked_reason=blocked_reason,
                expected_learning_value=seed.expected_learning_value,
                novelty_score=seed.novelty_score,
                candidate_impact_estimate=seed.candidate_impact_estimate,
                failure_reduction_score=seed.failure_reduction_score,
                evidence_gap_score=seed.evidence_gap_score,
                regime_gap_score=seed.regime_gap_score,
                cost_estimate=seed.cost_estimate,
                mechanism_tags=seed.mechanism_tags,
                regime_context=seed.regime_context,
                source_memory_ids=seed.source_memory_ids,
                metadata={
                    "seed_id": seed.seed_id,
                    "seed_source": seed.source,
                    "duplicate_risk": seed.duplicate_risk,
                    "research_only": True,
                    "authority_boundary": AUTHORITY_BOUNDARY,
                    **seed.metadata,
                },
            )
            if item["state"] == "READY":
                written_ids.append(item["backlog_item_id"])
    ready = backlog.get_ready_items()
    result = BacklogSeedingResult(
        seeding_run_id=f"backlog-seeding-{_stamp(created_at)}-{_hash([created_at, profile, requested_limit])}",
        created_at=created_at,
        profile=profile,
        requested_limit=requested_limit,
        generated_count=len(candidates),
        written_count=len(written_ids),
        duplicate_count=len(duplicate_seed_ids),
        blocked_count=len(blocked_ids),
        ready_count_after_seeding=len(ready),
        ready_count_by_type=dict(sorted(Counter(row.get("item_type", "") for row in ready).items())),
        ready_count_by_mechanism=_ready_by_mechanism(ready),
        written_backlog_item_ids=written_ids,
        duplicate_seed_ids=sorted(set(duplicate_seed_ids)),
        blocked_seed_ids=sorted(set(blocked_ids)),
        governance_result={"status": "PASS" if not governance_violations else "FAIL", "violations": governance_violations, "warnings": []},
        metadata={"source_inventory": source_inventory_summary(sources), "write": write},
    ).to_dict()
    paths = write_seed_report(result, root_path, day=day)
    result["report_paths"] = {key: str(value) for key, value in paths.items()}
    for path_key, path_value in result["report_paths"].items():
        if path_key.endswith("json") or path_key == "json":
            Path(path_value).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def generate_mechanism_variation_items(sources: dict[str, Any] | None = None, *, limit: int = 100, created_at: str | None = None) -> list[BacklogSeedItem]:
    rows: list[BacklogSeedItem] = []
    source_artifact_ids = _source_artifact_pool(sources or {})
    idx = 0
    for mechanism in MECHANISM_FAMILIES:
        for regime in VARIATION_DIMENSIONS["regime"]:
            for session in VARIATION_DIMENSIONS["session"]:
                failure = VARIATION_DIMENSIONS["failure_pattern"][idx % len(VARIATION_DIMENSIONS["failure_pattern"])]
                confirmation = VARIATION_DIMENSIONS["confirmation_filter"][idx % len(VARIATION_DIMENSIONS["confirmation_filter"])]
                invalidating = VARIATION_DIMENSIONS["invalidating_condition"][idx % len(VARIATION_DIMENSIONS["invalidating_condition"])]
                evidence_gap = VARIATION_DIMENSIONS["evidence_gap"][idx % len(VARIATION_DIMENSIONS["evidence_gap"])]
                timeframe = VARIATION_DIMENSIONS["timeframe"][idx % len(VARIATION_DIMENSIONS["timeframe"])]
                title = f"Research {mechanism} variation in {regime} during {session}"
                description = f"Investigate {mechanism} as research-only workload across {timeframe}; compare {confirmation}, failure pattern {failure}, invalidating condition {invalidating}, and evidence gap {evidence_gap}."
                rows.append(_seed(
                    item_type="MECHANISM_VARIATION",
                    title=title,
                    description=description,
                    source="MechanismFamilies",
                    mechanism_tags=[mechanism],
                    regime_context=regime,
                    source_artifact_ids=source_artifact_ids[:1],
                    expected_learning_value=0.55,
                    novelty_score=0.25,
                    evidence_gap_score=0.2 if "gap" in evidence_gap else 0.1,
                    regime_gap_score=0.25 if regime == "UNKNOWN" else 0.1,
                    cost_estimate=0.08,
                    metadata={"session": session, "timeframe": timeframe, "failure_pattern": failure, "confirmation_filter": confirmation, "invalidating_condition": invalidating, "evidence_gap": evidence_gap},
                ))
                idx += 1
                if len(rows) >= min(limit, MAX_SEED_ITEMS):
                    return rows
    return rows


def generate_failure_analysis_items(sources: dict[str, Any], *, created_at: str | None = None) -> list[BacklogSeedItem]:
    rows = []
    failures = list(sources.get("repeated_failures") or sources.get("failure_patterns") or [])
    for failure in failures:
        mechanism_tags = list(failure.get("mechanism_tags") or ["MEAN_REVERSION"])
        failure_type = str(failure.get("failure_type") or failure.get("error_type") or "UNKNOWN_FAILURE")
        repetition = int(failure.get("repetition_count") or failure.get("count") or 1)
        title = f"Analyze repeated {failure_type} failure for {', '.join(mechanism_tags)}"
        rows.append(_seed(
            item_type="FAILURE_ANALYSIS",
            title=title,
            description=f"Research why repeated failure pattern {failure_type} persists and identify paper-forward observations needed before any authority change.",
            source="FailurePatterns",
            source_artifact_ids=list(failure.get("source_artifact_ids") or []),
            mechanism_tags=mechanism_tags,
            regime_context=",".join(failure.get("regime_context_ids") or []) or "UNKNOWN",
            expected_learning_value=0.65,
            failure_reduction_score=min(1.0, 0.2 + repetition / 10.0),
            evidence_gap_score=0.2,
            cost_estimate=0.1,
            metadata={"failure_id": failure.get("failure_id") or failure.get("failure_ids"), "failure_type": failure_type, "repetition_count": repetition},
        ))
    return rows


def generate_regime_gap_items(sources: dict[str, Any], *, created_at: str | None = None) -> list[BacklogSeedItem]:
    rows = []
    contexts = {row.get("regime_context_id"): row for row in sources.get("regime_contexts", [])}
    for memory in sources.get("research_memory", []):
        ctx_rows = [contexts.get(ctx, {}) for ctx in memory.get("regime_context_ids", [])]
        missing = not ctx_rows or any("UNKNOWN" in ctx.get("labels", []) for ctx in ctx_rows)
        if not missing:
            continue
        mechanisms = list(memory.get("mechanism_tags") or ["MEAN_REVERSION"])
        rows.append(_seed(
            item_type="REGIME_GAP",
            title=f"Resolve regime gap for {memory.get('memory_id')}",
            description="Research missing or UNKNOWN regime context before future evidence is treated as mature.",
            source="ResearchMemory",
            source_artifact_ids=list(memory.get("source_artifact_ids") or []),
            source_memory_ids=[str(memory.get("memory_id"))],
            mechanism_tags=mechanisms,
            regime_context="UNKNOWN",
            expected_learning_value=0.5,
            regime_gap_score=0.6,
            evidence_gap_score=0.2,
            cost_estimate=0.08,
            metadata={"memory_id": memory.get("memory_id")},
        ))
    return rows


def generate_duplicate_review_items(sources: dict[str, Any], *, created_at: str | None = None) -> list[BacklogSeedItem]:
    rows = []
    for duplicate in list(sources.get("potential_duplicates") or []) + list(sources.get("duplicate_links") or []):
        left = duplicate.get("left_memory_id")
        right = duplicate.get("right_memory_id")
        if not left or not right:
            continue
        score = float(duplicate.get("duplicate_score") or 0.75)
        rows.append(_seed(
            item_type="DUPLICATE_REVIEW",
            title=f"Review duplicate research memory {left} vs {right}",
            description="Research whether duplicate memory should remain separate; do not delete artifacts automatically.",
            source="DuplicateLinks",
            source_memory_ids=[str(left), str(right)],
            mechanism_tags=list(duplicate.get("mechanism_tags") or ["MEAN_REVERSION"]),
            expected_learning_value=0.35,
            novelty_score=0.05,
            duplicate_risk=score,
            cost_estimate=0.05,
            metadata={"duplicate_score": score, "duplicate_status": duplicate.get("status")},
        ))
    return rows


def generate_edge_qualification_review_items(sources: dict[str, Any], *, created_at: str | None = None) -> list[BacklogSeedItem]:
    rows = []
    for candidate in sources.get("paper_trade_candidates", []):
        candidate_id = str(candidate.get("candidate_id") or candidate.get("id") or "candidate")
        rows.append(_seed(
            item_type="PAPER_TRADE_CANDIDATE_REVIEW",
            title=f"Review paper-trade candidate evidence for {candidate_id}",
            description="Review PaperTradeCandidate evidence for human-reviewed paper testing consideration only.",
            source="PaperTradeCandidates",
            source_artifact_ids=list(candidate.get("source_artifact_ids") or []),
            source_memory_ids=list(candidate.get("source_memory_ids") or []),
            mechanism_tags=list(candidate.get("mechanism_tags") or ["MEAN_REVERSION"]),
            regime_context=str(candidate.get("regime_context") or "UNKNOWN"),
            expected_learning_value=0.45,
            candidate_impact_estimate=0.35,
            evidence_gap_score=0.15,
            cost_estimate=0.1,
            metadata={"candidate_id": candidate_id},
        ))
    for queue_item in sources.get("paper_trading_queue", []):
        queue_id = str(queue_item.get("queue_item_id") or queue_item.get("candidate_id") or "queue")
        rows.append(_seed(
            item_type="EDGE_QUALIFICATION_REVIEW",
            title=f"Review edge qualification inputs for {queue_id}",
            description="Review edge qualification inputs feeding the human-reviewed paper testing queue only.",
            source="PaperTradingQueue",
            source_artifact_ids=list(queue_item.get("source_artifact_ids") or []),
            mechanism_tags=list((queue_item.get("test_plan") or {}).get("mechanism_tags") or ["MEAN_REVERSION"]),
            expected_learning_value=0.45,
            candidate_impact_estimate=0.3,
            evidence_gap_score=0.2,
            cost_estimate=0.1,
            metadata={"queue_item_id": queue_id},
        ))
    return rows


def deduplicate_seed_items(items: list[BacklogSeedItem], root: str | Path = DEFAULT_STORE_ROOT) -> tuple[list[BacklogSeedItem], list[str]]:
    unresolved = [row for row in ResearchBacklog(root).list_backlog_items() if row.get("state") not in {"COMPLETED", "REJECTED", "RETIRED"}]
    seen = {_signature(row) for row in unresolved}
    emitted = set()
    kept: list[BacklogSeedItem] = []
    duplicates: list[str] = []
    for item in items:
        sig = _signature(item.to_dict())
        if sig in seen or sig in emitted:
            duplicates.append(item.seed_id)
            continue
        emitted.add(sig)
        kept.append(item)
    return kept, duplicates


def _artifact_followup_items(sources: dict[str, Any], *, created_at: str | None = None) -> list[BacklogSeedItem]:
    rows = []
    for claim in sources.get("generated_research_claims", [])[:20]:
        rows.append(_seed(item_type="HYPOTHESIS_VALIDATION", title=f"Generate hypothesis from claim {claim.get('artifact_id')}", description="Turn prior GeneratedResearchClaim into a testable ResearchHypothesis.", source="PriorGeneratedResearchClaims", source_artifact_ids=[claim["artifact_id"]], mechanism_tags=_artifact_mechanisms(claim), expected_learning_value=0.55, novelty_score=0.1, evidence_gap_score=0.15, cost_estimate=0.08))
    for hyp in sources.get("research_hypotheses", [])[:20]:
        rows.append(_seed(item_type="EVIDENCE_GAP", title=f"Design cheap experiment for hypothesis {hyp.get('artifact_id')}", description="Design CheapExperimentSpec for prior ResearchHypothesis; no execution requested.", source="PriorResearchHypotheses", source_artifact_ids=[hyp["artifact_id"]], mechanism_tags=_artifact_mechanisms(hyp), expected_learning_value=0.55, novelty_score=0.1, evidence_gap_score=0.35, cost_estimate=0.08))
    return rows


def _ensure_seed_question_artifact(store: ArtifactStore, seed: BacklogSeedItem, *, created_at: str) -> str:
    artifact_id = f"q-{seed.backlog_item_id}"
    if store.exists(artifact_id):
        return artifact_id
    store.create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.QUESTION.value,
        created_at=created_at,
        created_by=CREATED_BY,
        source_artifact_ids=list(seed.source_artifact_ids),
        confidence=0.35,
        evidence_level=EvidenceLevel.GENERATED_ONLY.value,
        labels=["research_os", "backlog_seed", "generated_question"],
        metadata={
            "question": seed.title,
            "description": seed.description,
            "seed_id": seed.seed_id,
            "seed_source": seed.source,
            "mechanism_tags": seed.mechanism_tags,
            "regime_context": seed.regime_context,
            "research_only": True,
            "authority_boundary": AUTHORITY_BOUNDARY,
        },
        is_root=not bool(seed.source_artifact_ids),
    )
    return artifact_id


def _seed(**kwargs: Any) -> BacklogSeedItem:
    material = [kwargs.get("item_type"), kwargs.get("title"), kwargs.get("description"), kwargs.get("mechanism_tags", []), kwargs.get("regime_context", "UNKNOWN"), kwargs.get("source_artifact_ids", []), kwargs.get("source_memory_ids", [])]
    digest = _hash(material)
    metadata = {"research_only": True, "authority_boundary": AUTHORITY_BOUNDARY, **dict(kwargs.pop("metadata", {}) or {})}
    return BacklogSeedItem(seed_id=f"seed-{digest}", backlog_item_id=f"seed-{digest}", metadata=metadata, **kwargs)


def _signature(row: dict[str, Any]) -> str:
    metadata = row.get("metadata", {}) if isinstance(row.get("metadata"), dict) else {}
    material = {
        "item_type": row.get("item_type"),
        "title": normalize_research_text(str(row.get("title") or "")),
        "description": normalize_research_text(str(row.get("description") or "")),
        "mechanism_tags": sorted(row.get("mechanism_tags") or metadata.get("mechanism_tags") or []),
        "source_artifact_ids": sorted(row.get("source_artifact_ids") or []),
        "regime_context": row.get("regime_context") or metadata.get("regime_context") or "UNKNOWN",
    }
    return _hash(material)


def _ready_by_mechanism(ready: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in ready:
        metadata = row.get("metadata", {}) if isinstance(row.get("metadata"), dict) else {}
        tags = row.get("mechanism_tags") or metadata.get("mechanism_tags") or ["UNKNOWN"]
        for tag in tags:
            counts[str(tag)] += 1
    return dict(sorted(counts.items()))


def _source_artifact_pool(sources: dict[str, Any]) -> list[str]:
    ids = []
    for key in ("generated_research_claims", "research_hypotheses", "cheap_experiment_specs"):
        ids.extend(str(row.get("artifact_id")) for row in sources.get(key, []) if row.get("artifact_id"))
    for memory in sources.get("research_memory", []):
        ids.extend(str(item) for item in memory.get("source_artifact_ids", []))
    return list(dict.fromkeys(ids))


def _artifact_mechanisms(artifact: dict[str, Any]) -> list[str]:
    metadata = artifact.get("metadata", {}) if isinstance(artifact.get("metadata"), dict) else {}
    tags = metadata.get("mechanism_tags") or metadata.get("mechanism_family") or []
    if isinstance(tags, str):
        return [tags]
    return list(tags or ["MEAN_REVERSION"])


def _profile_limit(profile: str, limit: int | None) -> int:
    value = limit if limit is not None else PROFILE_LIMITS.get(profile, PROFILE_LIMITS[DEFAULT_SEED_PROFILE])
    if value > MAX_SEED_ITEMS:
        raise ValueError("backlog seeding cannot exceed 300 items without an explicit future build")
    if value <= 0:
        raise ValueError("backlog seeding limit must be positive")
    return int(value)


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").replace("Z", "Z")[:15]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
