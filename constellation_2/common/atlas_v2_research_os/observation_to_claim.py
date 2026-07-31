from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType, EvidenceLevel
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .observation_import_governance import validate_observation_import_allowed
from .observation_models import ObservationClaimSeed
from .research_backlog import ResearchBacklog
from .session_context import normalize_session_context
from .regime_expansion import normalize_regime


def convert_observation_cluster_to_claim(cluster: dict[str, Any]) -> dict[str, Any]:
    validate_observation_import_allowed({"metadata": cluster.get("metadata", {})})
    mechanism = str(cluster.get("mechanism", "UNKNOWN")).upper()
    market_structure = str(cluster.get("market_structure") or cluster.get("metadata", {}).get("market_structure") or "UNKNOWN").upper()
    regime = normalize_regime(cluster.get("regime"))
    symbols = list(cluster.get("symbols", []))
    timeframes = list(cluster.get("timeframes", []))
    subject = mechanism.replace("_", " ").lower()
    session_context = normalize_session_context((cluster.get("metadata") or {}).get("session_context"))
    regime_phrase = "UNKNOWN regimes" if regime == "UNKNOWN" else f"{regime} regimes"
    timeframe_phrase = "/".join(timeframes) if timeframes else "observed"
    symbol_phrase = ", ".join(symbols[:5]) if symbols else "tracked symbols"
    structure_phrase = market_structure.replace("_", " ").lower()
    claim_text = f"{mechanism} observations with {market_structure} structure may show repeatable {subject} behavior in {regime_phrase} across {symbol_phrase} on {timeframe_phrase} timeframes."
    return ObservationClaimSeed(
        claim_seed_id=cluster.get("claim_seed_id") or cluster["cluster_id"].replace("obs_cluster", "obs_claim", 1),
        source_observation_ids=list(cluster.get("source_observation_ids", [])),
        mechanism=mechanism,
        market_structure=market_structure,
        regime=regime,
        symbols=symbols,
        timeframes=timeframes,
        cluster_summary=str(cluster.get("cluster_summary", "")),
        claim_text=claim_text,
        confidence=float(cluster.get("confidence", 0.0)),
        metadata={
            "source_cluster_id": cluster.get("cluster_id"),
            "market_structure": market_structure,
            "session_context": (cluster.get("metadata", {}) or {}).get("session_context", "UNKNOWN"),
            "session_context_counts": dict((cluster.get("metadata", {}) or {}).get("session_context_counts", {}) if isinstance((cluster.get("metadata", {}) or {}).get("session_context_counts", {}), dict) else {}),
            "cluster_metadata": dict(cluster.get("metadata", {}) if isinstance(cluster.get("metadata", {}), dict) else {}),
            "split_dimensions": dict((cluster.get("metadata", {}) or {}).get("split_dimensions", {}) if isinstance((cluster.get("metadata", {}) or {}).get("split_dimensions", {}), dict) else {}),
            "symbol_groups": list(cluster.get("symbol_groups", [])),
            "source_types": list(cluster.get("source_types", [])),
            "symbol_counts": dict(cluster.get("symbol_counts", {})),
            "timeframe_counts": dict(cluster.get("timeframe_counts", {})),
            "source_type_counts": dict(cluster.get("source_type_counts", {}) if isinstance(cluster.get("source_type_counts", {}), dict) else {}),
            "market_structure_counts": dict(cluster.get("market_structure_counts", {}) if isinstance(cluster.get("market_structure_counts", {}), dict) else {}),
            "measurement_only": True,
            "research_only": True,
            "authority": "CLAIM_INVESTIGATION_BACKLOG_ONLY",
        },
    ).to_dict()


def create_backlog_items_from_observations(
    claim_seeds: list[dict[str, Any]],
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str,
) -> list[dict[str, Any]]:
    backlog = ResearchBacklog(root)
    store = ArtifactStore(root)
    created: list[dict[str, Any]] = []
    existing_by_claim = {
        row.get("metadata", {}).get("claim_seed_id"): row
        for row in backlog.list_backlog_items(item_type="CLAIM_INVESTIGATION")
    }
    for claim in claim_seeds:
        validate_observation_import_allowed({"metadata": claim.get("metadata", {})})
        claim_seed_id = claim["claim_seed_id"]
        if claim_seed_id in existing_by_claim:
            created.append(existing_by_claim[claim_seed_id])
            continue
        claim_seed_artifact = ensure_observation_claim_seed_artifact(
            claim, root=root, created_at=created_at, artifact_store=store
        )
        item = backlog.create_backlog_item(
            backlog_item_id=f"obs-claim-{claim_seed_id[-12:]}",
            item_type="CLAIM_INVESTIGATION",
            title=f"Investigate observation claim: {claim['mechanism']} in {claim['regime']}",
            description=claim["claim_text"],
            created_at=created_at,
            created_by="atlas_observation_import",
            source_artifact_ids=[claim_seed_artifact["artifact_id"]],
            state="READY",
            expected_learning_value=0.65,
            novelty_score=0.25,
            evidence_gap_score=0.35,
            regime_gap_score=0.35 if claim.get("regime") == "UNKNOWN" else 0.1,
            cost_estimate=0.15,
            mechanism_tags=[claim["mechanism"]],
            regime_context=str(claim.get("regime") or "UNKNOWN"),
            metadata={
                "claim_seed_id": claim_seed_id,
                "session_context": normalize_session_context((claim.get("metadata") or {}).get("session_context")),
                "claim_seed_artifact_id": claim_seed_artifact["artifact_id"],
                "market_structure": claim.get("market_structure") or (claim.get("metadata", {}) or {}).get("market_structure") or "UNKNOWN",
                "observation_claim_seed": claim,
                "source_observation_ids": list(claim.get("source_observation_ids", [])),
                "measurement_only": True,
                "authority": "CLAIM_INVESTIGATION_ONLY",
                "flow": "ObservationRecord->ObservationCluster->CLAIM_INVESTIGATION->Claim->Hypothesis->HistoricalReplay",
            },
        )
        created.append(item)
    return created


def ensure_observation_claim_seed_artifact(
    claim: dict[str, Any],
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str,
    artifact_store: ArtifactStore | None = None,
) -> dict[str, Any]:
    validate_observation_import_allowed({"metadata": claim.get("metadata", {})})
    store = artifact_store or ArtifactStore(root)
    artifact_id = claim["claim_seed_id"]
    if store.exists(artifact_id):
        return store.get_artifact(artifact_id)
    artifact = store.create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.QUESTION.value,
        created_at=created_at,
        created_by="atlas_observation_import",
        confidence=float(claim.get("confidence", 0.0)),
        evidence_level=EvidenceLevel.GENERATED_ONLY.value,
        labels=["observation_import", "claim_investigation_seed", "research_only"],
        metadata={
            "question": claim["claim_text"],
            "description": claim.get("cluster_summary", ""),
            "claim_text": claim["claim_text"],
            "mechanism_family": claim.get("mechanism") or "UNKNOWN",
            "market_structure": claim.get("market_structure") or (claim.get("metadata", {}) or {}).get("market_structure") or "UNKNOWN",
            "session_context": (claim.get("metadata", {}) or {}).get("session_context", "UNKNOWN"),
            "regime_context": claim.get("regime") or "UNKNOWN",
            "session_context": normalize_session_context((claim.get("metadata") or {}).get("session_context")),
            "symbols": list(claim.get("symbols", [])),
            "timeframes": list(claim.get("timeframes", [])),
            "source_observation_ids": list(claim.get("source_observation_ids", [])),
            "source_types": list((claim.get("metadata", {}) or {}).get("source_types", [])),
            "symbol_counts": dict((claim.get("metadata", {}) or {}).get("symbol_counts", {})),
            "timeframe_counts": dict((claim.get("metadata", {}) or {}).get("timeframe_counts", {})),
            "source_type_counts": dict((claim.get("metadata", {}) or {}).get("source_type_counts", {})),
            "market_structure_counts": dict((claim.get("metadata", {}) or {}).get("market_structure_counts", {})),
            "observation_claim_seed": claim,
            "observation_record_only": False,
            "observations_are_not_claims": True,
            "claims_are_not_recommendations": True,
            "measurement_only": True,
            "research_only": True,
            "authority": "CLAIM_INVESTIGATION_BACKLOG_ONLY",
            "flow": "ObservationRecord->ObservationCluster->CLAIM_INVESTIGATION->Claim->Hypothesis->HistoricalReplay",
        },
        is_root=True,
    ).to_dict()
    validate_observation_import_allowed(artifact)
    return artifact
