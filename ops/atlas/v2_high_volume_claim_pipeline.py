from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_external_strategy_claim_extractor import (
    classify_mechanism_text,
    fragility_conditions_for_failure_modes,
    infer_primary_failure_modes,
    normalize_text,
)

PIPELINE_VERSION = "atlas_v2_high_volume_claim_pipeline_v1"
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
MAX_CLAIMS = 10000
DEFAULT_MAX_CHEAP_EXPERIMENT_CANDIDATES = 50
ALLOWED_SOURCE_TYPES = (
    "TRANSCRIPT_INTAKE",
    "MANUAL_CLAIM_ENTRY",
    "HISTORICAL_RESEARCH_ARTIFACTS",
    "EXTERNAL_STRATEGY_CLAIMS",
)
FORBIDDEN_OUTPUT_OBJECT_TYPES = {
    "Candidate",
    "Sleeve",
    "PaperPosition",
    "Recommendation",
    "Validation",
    "Trade",
}

OPENING_RANGE_ALIASES = (
    "orb",
    "opening range",
    "opening-range",
    "opening range breakout",
    "opening range retest",
    "initial balance",
    "sneaky pivot",
    "first 5 minute range",
    "first 15 minute range",
)

CLAIM_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "if", "in", "into", "is", "it",
    "of", "on", "or", "the", "then", "to", "use", "using", "when", "with", "strategy", "setup", "claim",
}


@dataclass(frozen=True)
class NormalizedClaim:
    claim_id: str
    source_type: str
    source_id: str
    claim_text: str
    normalized_text: str
    mechanism_family: str
    mechanism_confidence: float
    claim_fingerprint: str
    mechanism_fingerprint: str
    contrarian_theory: str
    fragility_conditions: tuple[str, ...]


@dataclass(frozen=True)
class HighVolumeClaimPipelineResult:
    batch: dict[str, Any]
    metrics: dict[str, Any]
    mechanism_registry: list[dict[str, Any]]
    mechanism_clusters: list[dict[str, Any]]
    cluster_summary: dict[str, Any]
    normalized_claims: tuple[NormalizedClaim, ...]

    def report(self) -> dict[str, Any]:
        return {
            "total_claims": self.metrics["total_claims"],
            "unique_claims": self.metrics["unique_claims"],
            "unique_mechanisms": self.metrics["unique_mechanisms"],
            "duplicate_ratio": self.metrics["duplicate_ratio"],
            "mechanism_distribution": self.metrics["mechanism_distribution"],
            "cheap_experiment_candidates": self.metrics["cheap_experiment_candidates"],
        }


class AtlasV2HighVolumeClaimPipeline:
    def __init__(self, ledger: AtlasV2Ledger | None = None, *, max_cheap_experiment_candidates: int = DEFAULT_MAX_CHEAP_EXPERIMENT_CANDIDATES) -> None:
        if max_cheap_experiment_candidates < 0:
            raise AtlasV2ValidationError("max_cheap_experiment_candidates must be non-negative")
        self.ledger = ledger
        self.max_cheap_experiment_candidates = max_cheap_experiment_candidates

    def process_claim_batch(self, *, batch_id: str, claims: Iterable[str | dict[str, Any]], created_at: str = DEFAULT_CREATED_AT) -> HighVolumeClaimPipelineResult:
        normalized = tuple(self._normalize_claim(index, raw) for index, raw in enumerate(claims))
        total = len(normalized)
        if total == 0:
            raise AtlasV2ValidationError("ClaimBatch requires at least one externally supplied claim")
        if total > MAX_CLAIMS:
            raise AtlasV2ValidationError("ClaimBatch cannot exceed 10000 claims")

        first_seen: dict[str, NormalizedClaim] = {}
        for claim in normalized:
            first_seen.setdefault(claim.claim_fingerprint, claim)
        unique_claims = len(first_seen)

        by_family: dict[str, list[NormalizedClaim]] = {}
        source_by_family: dict[str, set[str]] = {}
        contrarian_by_family: dict[str, int] = {}
        for claim in normalized:
            by_family.setdefault(claim.mechanism_family, []).append(claim)
            source_by_family.setdefault(claim.mechanism_family, set()).add(claim.source_type)
            if claim.contrarian_theory:
                contrarian_by_family[claim.mechanism_family] = contrarian_by_family.get(claim.mechanism_family, 0) + 1

        clusters = [self._cluster_record(batch_id, family, items, source_by_family[family], created_at) for family, items in sorted(by_family.items())]
        registry = [self._registry_record(family, items, source_by_family[family], contrarian_by_family.get(family, 0), created_at) for family, items in sorted(by_family.items())]
        cheap_candidates = self._cheap_experiment_candidates(registry, clusters)
        cheap_families = {item["mechanism_family"] for item in cheap_candidates}
        for row in registry:
            row["cheap_experiment_count"] = 1 if row["mechanism_family"] in cheap_families else 0

        duplicate_count = total - unique_claims
        unique_mechanisms = len(by_family)
        source_types_present = sorted({claim.source_type for claim in normalized})
        batch = {
            "batch_id": batch_id,
            "created_at": created_at,
            "pipeline_version": PIPELINE_VERSION,
            "claim_count": total,
            "batch_size_tier": batch_size_tier(total),
            "allowed_source_types": list(ALLOWED_SOURCE_TYPES),
            "source_types_present": source_types_present,
            "claim_ids": [claim.claim_id for claim in normalized],
            "authority_boundary_acknowledged": True,
            "status": "INGESTED_CLASSIFIED_DEDUPED_ROUTED_TO_CHEAP_EXPERIMENT_SUMMARY",
        }
        metrics = {
            "metrics_id": f"claim-batch-metrics-{short_hash(batch_id)}",
            "batch_id": batch_id,
            "created_at": created_at,
            "total_claims": total,
            "unique_claims": unique_claims,
            "unique_mechanisms": unique_mechanisms,
            "duplicate_ratio": round(duplicate_count / total, 6),
            "mechanism_distribution": {family: len(items) for family, items in sorted(by_family.items())},
            "cheap_experiment_candidates": cheap_candidates,
            "claims_ingested": total,
            "claims_deduped": duplicate_count,
            "mechanisms_discovered": unique_mechanisms,
            "mechanisms_reused": max(0, total - unique_mechanisms),
            "contrarian_coverage": round(sum(1 for claim in normalized if claim.contrarian_theory) / total, 6),
            "cheap_experiment_coverage": round(len(cheap_candidates) / unique_mechanisms, 6) if unique_mechanisms else 0.0,
            "authority_boundary_acknowledged": True,
        }
        summary = {
            "summary_id": f"claim-cluster-summary-{short_hash(batch_id)}",
            "batch_id": batch_id,
            "created_at": created_at,
            "cluster_count": len(clusters),
            "clusters": [{"cluster_id": row["cluster_id"], "mechanism_family": row["mechanism_family"], "claim_count": row["claim_count"]} for row in clusters],
            "dedupe_strategy": "claim fingerprint plus mechanism-family clustering; duplicate claims do not create independent experiments",
            "authority_boundary_acknowledged": True,
        }

        if self.ledger is not None:
            batch = self.ledger.create_record("ClaimBatch", batch, reason="high-volume claim batch ingested from externally supplied claims", triggering_object=PIPELINE_VERSION)
            registry = [self.ledger.create_record("MechanismRegistry", row, reason="mechanism registry summarized for high-volume claim batch", triggering_object=f"ClaimBatch:{batch_id}") for row in registry]
            clusters = [self.ledger.create_record("MechanismCluster", row, reason="mechanism cluster built before cheap experiment routing", triggering_object=f"ClaimBatch:{batch_id}") for row in clusters]
            metrics = self.ledger.create_record("ClaimBatchMetrics", metrics, reason="high-volume claim metrics computed after deduplication", triggering_object=f"ClaimBatch:{batch_id}")
            summary = self.ledger.create_record("ClaimClusterSummary", summary, reason="claim cluster summary emitted after bounded routing", triggering_object=f"ClaimBatch:{batch_id}")

        self._assert_no_forbidden_outputs(batch, metrics, registry, clusters, summary)
        return HighVolumeClaimPipelineResult(batch=batch, metrics=metrics, mechanism_registry=registry, mechanism_clusters=clusters, cluster_summary=summary, normalized_claims=normalized)

    def _normalize_claim(self, index: int, raw: str | dict[str, Any]) -> NormalizedClaim:
        if isinstance(raw, str):
            payload: dict[str, Any] = {"claim_text": raw, "source_type": "MANUAL_CLAIM_ENTRY"}
        elif isinstance(raw, dict):
            payload = raw
        else:
            raise AtlasV2ValidationError("claims must be strings or objects")
        text = str(payload.get("claim_text") or payload.get("text") or "").strip()
        if not text:
            raise AtlasV2ValidationError("each externally supplied claim requires claim_text")
        source_type = normalize_source_type(str(payload.get("source_type") or payload.get("origin") or "MANUAL_CLAIM_ENTRY"))
        source_id = str(payload.get("source_id") or f"source-{source_type.lower()}")
        claim_id = str(payload.get("claim_id") or f"hvc-{index:05d}-{short_hash(source_type + source_id + text)}")
        family, confidence = classify_high_volume_mechanism(text)
        fingerprint = claim_fingerprint(text, family)
        mechanism_fp = mechanism_fingerprint(text, family)
        failure_modes, _, _ = infer_primary_failure_modes({"claim_text": text, "extraction_status": "EXTRACTED"}, {"mechanism_family": family})
        fragilities = tuple(fragility_conditions_for_failure_modes(failure_modes))
        return NormalizedClaim(
            claim_id=claim_id,
            source_type=source_type,
            source_id=source_id,
            claim_text=text,
            normalized_text=normalize_text(text),
            mechanism_family=family,
            mechanism_confidence=confidence,
            claim_fingerprint=fingerprint,
            mechanism_fingerprint=mechanism_fp,
            contrarian_theory=contrarian_theory_statement(family, failure_modes),
            fragility_conditions=fragilities,
        )

    def _registry_record(self, family: str, claims: list[NormalizedClaim], sources: set[str], contrarian_count: int, created_at: str) -> dict[str, Any]:
        return {
            "mechanism_id": f"mechanism-registry-{family.lower()}",
            "created_at": created_at,
            "mechanism_family": family,
            "claim_count": len(claims),
            "source_count": len(sources),
            "contrarian_count": contrarian_count,
            "cheap_experiment_count": 0,
            "last_updated": created_at,
        }

    def _cluster_record(self, batch_id: str, family: str, claims: list[NormalizedClaim], sources: set[str], created_at: str) -> dict[str, Any]:
        confidence = round(sum(claim.mechanism_confidence for claim in claims) / len(claims), 6)
        return {
            "cluster_id": f"mechanism-cluster-{short_hash(batch_id + family)}",
            "created_at": created_at,
            "mechanism_family": family,
            "claim_ids": [claim.claim_id for claim in claims],
            "claim_count": len(claims),
            "source_diversity": len(sources),
            "confidence": confidence,
            "status": "RETAINED_NOT_PROMOTED" if family == "UNKNOWN" else "CLUSTERED",
        }

    def _cheap_experiment_candidates(self, registry: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cluster_by_family = {row["mechanism_family"]: row for row in clusters}
        eligible = [row for row in registry if row["mechanism_family"] != "UNKNOWN" and row["contrarian_count"] > 0]
        eligible.sort(key=lambda row: (-int(row["claim_count"]), str(row["mechanism_family"])))
        bounded = eligible[: self.max_cheap_experiment_candidates]
        return [
            {
                "mechanism_family": row["mechanism_family"],
                "cluster_id": cluster_by_family[row["mechanism_family"]]["cluster_id"],
                "claim_count": row["claim_count"],
                "source_count": row["source_count"],
                "route": "TIER_1_SANITY_SUMMARY_ONLY",
                "reason": "One bounded cheap experiment route per mechanism cluster; duplicate claims do not create independent experiments.",
            }
            for row in bounded
        ]

    def _assert_no_forbidden_outputs(self, *objects: Any) -> None:
        encoded = json.dumps(objects, sort_keys=True).lower()
        forbidden_terms = (
            '"object_type": "candidate"',
            '"object_type": "sleeve"',
            '"object_type": "paperposition"',
            '"object_type": "recommendation"',
            '"object_type": "validation"',
            '"object_type": "trade"',
            'broker_order',
            'autonomous_execution_allowed": true',
            'broker_execution_allowed": true',
            'trade_advice_allowed": true',
            'capital_allocation',
        )
        if any(term in encoded for term in forbidden_terms):
            raise AtlasV2ValidationError("high-volume claim pipeline attempted forbidden authority output")


def batch_size_tier(total: int) -> str:
    if total <= 1000:
        return "UP_TO_1000"
    if total <= 5000:
        return "UP_TO_5000"
    if total <= 10000:
        return "UP_TO_10000"
    raise AtlasV2ValidationError("ClaimBatch cannot exceed 10000 claims")


def normalize_source_type(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip().upper()).strip("_")
    aliases = {
        "TRANSCRIPT": "TRANSCRIPT_INTAKE",
        "TRANSCRIPT_INTAKE": "TRANSCRIPT_INTAKE",
        "MANUAL": "MANUAL_CLAIM_ENTRY",
        "MANUAL_CLAIM": "MANUAL_CLAIM_ENTRY",
        "MANUAL_CLAIM_ENTRY": "MANUAL_CLAIM_ENTRY",
        "HISTORICAL": "HISTORICAL_RESEARCH_ARTIFACTS",
        "HISTORICAL_RESEARCH_ARTIFACT": "HISTORICAL_RESEARCH_ARTIFACTS",
        "HISTORICAL_RESEARCH_ARTIFACTS": "HISTORICAL_RESEARCH_ARTIFACTS",
        "EXTERNAL": "EXTERNAL_STRATEGY_CLAIMS",
        "EXTERNAL_STRATEGY": "EXTERNAL_STRATEGY_CLAIMS",
        "EXTERNAL_STRATEGY_CLAIMS": "EXTERNAL_STRATEGY_CLAIMS",
    }
    source_type = aliases.get(normalized, normalized)
    if source_type not in ALLOWED_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"unsupported high-volume claim source_type: {value}")
    return source_type


def classify_high_volume_mechanism(text: str) -> tuple[str, float]:
    lowered = text.lower()
    if any(alias in lowered for alias in OPENING_RANGE_ALIASES):
        return "OPENING_RANGE", 0.9
    family, confidence, _ = classify_mechanism_text(text)
    return family, confidence


def claim_fingerprint(text: str, family: str) -> str:
    normalized = normalize_text(text)
    tokens = [token for token in normalized.split() if token not in CLAIM_STOPWORDS]
    return sha256_text(" ".join([family.lower(), *tokens]))


def mechanism_fingerprint(text: str, family: str) -> str:
    normalized = normalize_text(text)
    if family == "OPENING_RANGE":
        return sha256_text("opening_range")
    tokens = sorted(set(token for token in normalized.split() if token not in CLAIM_STOPWORDS))[:12]
    return sha256_text(" ".join([family.lower(), *tokens]))


def contrarian_theory_statement(family: str, failure_modes: list[str]) -> str:
    if family == "UNKNOWN":
        return "Insufficient supplied mechanism detail; route to clarification only, not experiment expansion."
    modes = ", ".join(failure_modes[:3])
    return f"Treat the supplied {family} claim as fragile to {modes}; require cheap falsification before any learning interpretation."


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _read_claims(args: argparse.Namespace) -> list[dict[str, Any] | str]:
    if args.input_file:
        payload = json.loads(Path(args.input_file).read_text(encoding="utf-8"))
    else:
        payload = json.loads(sys.stdin.read())
    if isinstance(payload, dict):
        claims = payload.get("claims")
    else:
        claims = payload
    if not isinstance(claims, list):
        raise AtlasV2ValidationError("input must be a claim list or an object with claims")
    return claims


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 high-volume claim intake pipeline V1")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--input-file")
    parser.add_argument("--ledger-root")
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--max-cheap-experiment-candidates", type=int, default=DEFAULT_MAX_CHEAP_EXPERIMENT_CANDIDATES)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    ledger = AtlasV2Ledger(Path(args.ledger_root)) if args.ledger_root else None
    pipeline = AtlasV2HighVolumeClaimPipeline(ledger, max_cheap_experiment_candidates=args.max_cheap_experiment_candidates)
    result = pipeline.process_claim_batch(batch_id=args.batch_id, claims=_read_claims(args), created_at=args.created_at)
    print(json.dumps(result.report(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
