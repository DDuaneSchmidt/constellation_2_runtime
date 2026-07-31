from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import (
    AtlasV2Ledger,
    AtlasV2ValidationError,
    HIGH_VOLUME_CLAIM_BATCH_TIERS,
    HIGH_VOLUME_CLAIM_SOURCE_TYPES,
)
from ops.atlas.v2_external_strategy_claim_extractor import (
    DEFAULT_CREATED_AT,
    ALLOWED_HANDOFF_TIERS,
    classify_mechanism_text,
    claim_fingerprint,
    extract_rule_fields,
    fragility_conditions_for_failure_modes,
    infer_primary_failure_modes,
    mechanism_fingerprint,
    opposite_hypothesis_for_failure_modes,
    falsification_tests_for_failure_modes,
    sanitize_claim_text,
    sha256_text,
    short_hash,
)

PROCESSOR_VERSION = "atlas_v2_claim_batch_processor_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_claim_batch_processor_v1_ledgers")
PIPELINE_STEPS = [
    "CLAIMS",
    "DEDUPE",
    "MECHANISM_CLASSIFICATION",
    "CONTRARIAN_THEORY",
    "MECHANISM_REGISTRY",
    "CHEAP_EXPERIMENT_ELIGIBILITY",
]


@dataclass(frozen=True)
class ClaimBatchProcessorResult:
    batch: dict[str, Any]
    run: dict[str, Any]
    metrics: dict[str, Any]
    mechanism_records: list[dict[str, Any]]
    registry_records: list[dict[str, Any]]
    dedupe_records: list[dict[str, Any]]
    contrarian_records: list[dict[str, Any]]
    handoff_records: list[dict[str, Any]]

    def summary(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch["batch_id"],
            "run_id": self.run["run_id"],
            "claim_count": self.batch["claim_count"],
            "unique_claims": self.metrics["unique_claims"],
            "unique_mechanisms": self.metrics["unique_mechanisms"],
            "cheap_experiment_candidates": len(self.metrics["cheap_experiment_candidates"]),
            "duplicate_ratio": self.metrics["duplicate_ratio"],
            "status": self.run["status"],
        }


class AtlasV2ClaimBatchProcessor:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def process(
        self,
        claims: list[dict[str, Any] | str],
        *,
        batch_id: str | None = None,
        run_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> ClaimBatchProcessorResult:
        normalized = [normalize_claim_input(index, item, created_at=created_at) for index, item in enumerate(claims, start=1)]
        claim_count = len(normalized)
        if claim_count > 10000:
            raise AtlasV2ValidationError("ClaimBatchProcessor V1 accepts at most 10000 claims per batch")
        source_types_present = sorted({item["source_type"] for item in normalized})
        batch_record_id = batch_id or f"clb-{short_hash(batch_fingerprint(normalized))}"
        run_record_id = run_id or f"cbr-{short_hash(batch_record_id + ':' + PROCESSOR_VERSION)}"

        batch = self.ledger.create_record(
            "ClaimBatch",
            {
                "batch_id": batch_record_id,
                "created_at": created_at,
                "pipeline_version": PROCESSOR_VERSION,
                "claim_count": claim_count,
                "batch_size_tier": batch_size_tier(claim_count),
                "allowed_source_types": sorted(HIGH_VOLUME_CLAIM_SOURCE_TYPES),
                "source_types_present": source_types_present,
                "claim_ids": [item["claim_id"] for item in normalized],
                "authority_boundary_acknowledged": True,
                "status": "RECORDED",
            },
            reason="high-volume Atlas V2 claim batch recorded without experiment fanout",
            triggering_object=PROCESSOR_VERSION,
        )

        claim_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        mechanism_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in normalized:
            claim_payload = claim_payload_for_item(item)
            mechanism_payload = mechanism_payload_for_claim(claim_payload, created_at=created_at)
            claim_fp = claim_fingerprint(claim_payload)
            mechanism_fp = mechanism_fingerprint(claim_payload, mechanism_payload)
            enriched = {
                **item,
                "claim_payload": claim_payload,
                "mechanism_payload": mechanism_payload,
                "claim_fingerprint": claim_fp,
                "mechanism_fingerprint": mechanism_fp,
                "mechanism_key": f"{mechanism_payload['mechanism_family']}:{mechanism_fp}",
            }
            claim_groups[claim_fp].append(enriched)
            mechanism_groups[enriched["mechanism_key"]].append(enriched)

        mechanism_records: list[dict[str, Any]] = []
        registry_records: list[dict[str, Any]] = []
        dedupe_records: list[dict[str, Any]] = []
        contrarian_records: list[dict[str, Any]] = []
        handoff_records: list[dict[str, Any]] = []

        for mechanism_key in sorted(mechanism_groups):
            group = mechanism_groups[mechanism_key]
            representative = group[0]
            claim_payload = representative["claim_payload"]
            mechanism_payload = representative["mechanism_payload"]
            family = mechanism_payload["mechanism_family"]
            mechanism_id = f"cbm-{short_hash(batch_record_id + mechanism_key)}"
            mechanism_record = self.ledger.create_record(
                "ExternalStrategyMechanism",
                {
                    **mechanism_payload,
                    "mechanism_id": mechanism_id,
                    "claim_id": claim_payload["claim_id"],
                    "created_at": created_at,
                },
                reason="batch representative mechanism classified after in-memory dedupe",
                triggering_object=f"ClaimBatch:{batch_record_id}",
            )
            mechanism_records.append(mechanism_record)

            duplicate_count = len(group) - 1
            dedupe_record = self.ledger.create_record(
                "ExternalStrategyDeduplicationResult",
                {
                    "dedupe_id": f"cbd-{short_hash(batch_record_id + mechanism_key)}",
                    "claim_id": claim_payload["claim_id"],
                    "claim_fingerprint": representative["claim_fingerprint"],
                    "mechanism_fingerprint": representative["mechanism_fingerprint"],
                    "is_duplicate": duplicate_count > 0,
                    "duplicate_reason": "BATCH_MECHANISM_FINGERPRINT_MATCH" if duplicate_count else "",
                    "status": "DUPLICATE_CLAIM" if duplicate_count else "UNIQUE_OR_FIRST_SEEN",
                    "created_at": created_at,
                },
                reason="batch claim group deduped by claim and mechanism fingerprints",
                triggering_object=f"ExternalStrategyMechanism:{mechanism_id}",
            )
            dedupe_records.append(dedupe_record)

            failure_modes, confidence, status = infer_primary_failure_modes(claim_payload, mechanism_record)
            contrarian_record = self.ledger.create_record(
                "ExternalStrategyContrarianTheory",
                {
                    "contrarian_id": f"cbc-{short_hash(batch_record_id + mechanism_key)}",
                    "claim_id": claim_payload["claim_id"],
                    "mechanism_id": mechanism_id,
                    "primary_failure_modes": failure_modes,
                    "opposite_hypothesis": opposite_hypothesis_for_failure_modes(failure_modes, mechanism_record),
                    "fragility_conditions": fragility_conditions_for_failure_modes(failure_modes),
                    "required_falsification_tests": falsification_tests_for_failure_modes(failure_modes),
                    "prior_failure_matches": [],
                    "contrarian_confidence": confidence,
                    "status": status,
                    "created_at": created_at,
                },
                reason="batch contrarian theory generated for compressed mechanism",
                triggering_object=f"ExternalStrategyDeduplicationResult:{dedupe_record['dedupe_id']}",
            )
            contrarian_records.append(contrarian_record)

            eligible = claim_payload["extraction_status"] == "EXTRACTED" and family != "UNKNOWN"
            tier = "TIER_1_SANITY" if eligible else "TIER_0_DEDUPE"
            if tier not in ALLOWED_HANDOFF_TIERS:
                raise AtlasV2ValidationError(f"ClaimBatchProcessor produced disallowed handoff tier: {tier}")
            handoff_payload: dict[str, Any] = {
                "handoff_id": f"cbh-{short_hash(batch_record_id + mechanism_key)}",
                "claim_id": claim_payload["claim_id"],
                "mechanism_id": mechanism_id,
                "dedupe_id": dedupe_record["dedupe_id"],
                "eligible_for_cheap_experiment": eligible,
                "recommended_tier": tier,
                "reason": "One cheap-test eligibility record per compressed mechanism, not per raw claim.",
                "authority_boundary_acknowledged": True,
                "status": "READY_FOR_CHEAP_EXPERIMENT_HANDOFF" if eligible else "DEDUPE_ONLY",
                "created_at": created_at,
            }
            if tier in {"TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION"}:
                handoff_payload["contrarian_id"] = contrarian_record["contrarian_id"]
            handoff_record = self.ledger.create_record(
                "ExternalStrategyCheapExperimentHandoff",
                handoff_payload,
                reason="batch cheap experiment eligibility emitted at mechanism granularity only",
                triggering_object=f"ExternalStrategyContrarianTheory:{contrarian_record['contrarian_id']}",
            )
            handoff_records.append(handoff_record)

            registry_records.append(
                self.ledger.create_record(
                    "MechanismRegistry",
                    {
                        "mechanism_id": f"mrg-{short_hash(batch_record_id + mechanism_key)}",
                        "created_at": created_at,
                        "mechanism_family": family,
                        "claim_count": len(group),
                        "source_count": len({item["source_id"] for item in group}),
                        "contrarian_count": 1,
                        "cheap_experiment_count": 1 if eligible else 0,
                        "last_updated": created_at,
                    },
                    reason="batch mechanism inventory updated without strategy-name authority expansion",
                    triggering_object=f"ExternalStrategyCheapExperimentHandoff:{handoff_record['handoff_id']}",
                )
            )

        mechanism_distribution = dict(sorted(Counter(record["mechanism_family"] for record in mechanism_records).items()))
        unique_claims = len(claim_groups)
        unique_mechanisms = len(mechanism_groups)
        cheap_candidates = [record["mechanism_id"] for record in handoff_records if record["eligible_for_cheap_experiment"]]
        metrics = self.ledger.create_record(
            "ClaimBatchMetrics",
            {
                "metrics_id": f"cbx-{short_hash(batch_record_id + run_record_id)}",
                "batch_id": batch_record_id,
                "created_at": created_at,
                "total_claims": claim_count,
                "unique_claims": unique_claims,
                "unique_mechanisms": unique_mechanisms,
                "duplicate_ratio": round(1 - (unique_claims / claim_count), 6) if claim_count else 0.0,
                "mechanism_distribution": mechanism_distribution,
                "cheap_experiment_candidates": cheap_candidates,
                "claims_ingested": claim_count,
                "claims_deduped": claim_count - unique_claims,
                "mechanisms_discovered": unique_mechanisms,
                "mechanisms_reused": claim_count - unique_mechanisms,
                "contrarian_coverage": round(len(contrarian_records) / unique_mechanisms, 6) if unique_mechanisms else 0.0,
                "cheap_experiment_coverage": round(len(cheap_candidates) / unique_mechanisms, 6) if unique_mechanisms else 0.0,
                "authority_boundary_acknowledged": True,
            },
            reason="batch compression metrics recorded for Atlas V2 Phase 1",
            triggering_object=f"ClaimBatch:{batch_record_id}",
        )

        run = self.ledger.create_record(
            "ClaimBatchRun",
            {
                "run_id": run_record_id,
                "batch_id": batch_record_id,
                "created_at": created_at,
                "pipeline_version": PROCESSOR_VERSION,
                "pipeline_steps": PIPELINE_STEPS,
                "input_claim_count": claim_count,
                "processed_claim_count": claim_count,
                "emitted_record_counts": {
                    "ExternalStrategyMechanism": len(mechanism_records),
                    "MechanismRegistry": len(registry_records),
                    "ExternalStrategyDeduplicationResult": len(dedupe_records),
                    "ExternalStrategyContrarianTheory": len(contrarian_records),
                    "ExternalStrategyCheapExperimentHandoff": len(handoff_records),
                    "ClaimBatchMetrics": 1,
                },
                "mechanism_record_ids": [record["mechanism_id"] for record in mechanism_records],
                "dedupe_record_ids": [record["dedupe_id"] for record in dedupe_records],
                "contrarian_record_ids": [record["contrarian_id"] for record in contrarian_records],
                "handoff_record_ids": [record["handoff_id"] for record in handoff_records],
                "authority_boundary_acknowledged": True,
                "status": "COMPLETED",
            },
            reason="claim batch processor completed without experiment fanout",
            triggering_object=f"ClaimBatchMetrics:{metrics['metrics_id']}",
        )

        return ClaimBatchProcessorResult(
            batch=batch,
            run=run,
            metrics=metrics,
            mechanism_records=mechanism_records,
            registry_records=registry_records,
            dedupe_records=dedupe_records,
            contrarian_records=contrarian_records,
            handoff_records=handoff_records,
        )


def normalize_claim_input(index: int, item: dict[str, Any] | str, *, created_at: str) -> dict[str, Any]:
    if isinstance(item, str):
        payload: dict[str, Any] = {"claim_text": item, "source_type": "MANUAL_CLAIM_ENTRY"}
    elif isinstance(item, dict):
        payload = dict(item)
    else:
        raise AtlasV2ValidationError("ClaimBatchProcessor claims must be strings or objects")
    source_type = str(payload.get("source_type") or "MANUAL_CLAIM_ENTRY").strip().upper()
    if source_type not in HIGH_VOLUME_CLAIM_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"unsupported high-volume claim source_type: {source_type}")
    text = sanitize_claim_text(str(payload.get("claim_text") or payload.get("text") or payload.get("input_text") or ""))
    if not text:
        raise AtlasV2ValidationError("ClaimBatchProcessor claim text is required")
    source_id = str(payload.get("source_id") or f"cbs-{short_hash(source_type + ':' + str(index) + ':' + sha256_text(text))}")
    claim_id = str(payload.get("claim_id") or f"bcl-{short_hash(source_id + ':' + sha256_text(text))}")
    return {
        "claim_id": claim_id,
        "source_id": source_id,
        "source_type": source_type,
        "source_title": str(payload.get("source_title") or source_type.lower()),
        "claim_text": text,
        "created_at": str(payload.get("created_at") or created_at),
    }


def claim_payload_for_item(item: dict[str, Any]) -> dict[str, Any]:
    fields = extract_rule_fields(item["claim_text"])
    enough_rule_detail = bool(fields.get("entry_rule") and (fields.get("exit_rule") or fields.get("risk_rule") or fields.get("filter_rule")))
    payload: dict[str, Any] = {
        "claim_id": item["claim_id"],
        "source_id": item["source_id"],
        "claim_text": item["claim_text"],
        "confidence": 0.72 if enough_rule_detail else 0.25,
        "extraction_status": "EXTRACTED" if enough_rule_detail else "INSUFFICIENT_RULE_DETAIL",
        "created_at": item["created_at"],
    }
    payload.update({key: value for key, value in fields.items() if value})
    return payload


def mechanism_payload_for_claim(claim: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "market_context", "timeframe"))
    family, confidence, description = classify_mechanism_text(text)
    return {
        "mechanism_id": f"tmp-{short_hash(str(claim['claim_id']) + family)}",
        "claim_id": claim["claim_id"],
        "mechanism_family": family,
        "mechanism_description": description,
        "classification_confidence": confidence,
        "created_at": created_at,
    }


def batch_size_tier(claim_count: int) -> str:
    if claim_count <= 1000:
        return "UP_TO_1000"
    if claim_count <= 5000:
        return "UP_TO_5000"
    if claim_count <= 10000:
        return "UP_TO_10000"
    raise AtlasV2ValidationError(f"ClaimBatch count exceeds supported tiers: {claim_count}")


def batch_fingerprint(items: list[dict[str, Any]]) -> str:
    material = json.dumps(
        [{"claim_id": item["claim_id"], "source_type": item["source_type"], "claim_text_hash": sha256_text(item["claim_text"])} for item in items],
        sort_keys=True,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _read_claims(args: argparse.Namespace) -> list[dict[str, Any] | str]:
    if args.input_file:
        raw = Path(args.input_file).read_text(encoding="utf-8")
    elif not sys.stdin.isatty():
        raw = sys.stdin.read()
    else:
        raw = "[]"
    data = json.loads(raw)
    if not isinstance(data, list):
        raise AtlasV2ValidationError("ClaimBatchProcessor input must be a JSON array")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 Claim Batch Processor V1")
    parser.add_argument("--input-file")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2ClaimBatchProcessor(AtlasV2Ledger(Path(args.ledger_root))).process(
        _read_claims(args),
        batch_id=args.batch_id,
        run_id=args.run_id,
        created_at=args.created_at,
    )
    print(json.dumps(result.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
