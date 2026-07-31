from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_claim_batch_processor import DEFAULT_LEDGER_ROOT as CLAIM_BATCH_DEFAULT_LEDGER_ROOT
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, short_hash
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError

MECHANISM_REGISTRY_VERSION = "atlas_v2_mechanism_registry_v1"
DEFAULT_LEDGER_ROOT = CLAIM_BATCH_DEFAULT_LEDGER_ROOT


@dataclass(frozen=True)
class MechanismRegistryResult:
    registry_entries: list[dict[str, Any]]
    clusters: list[dict[str, Any]]
    lineages: list[dict[str, Any]]
    metrics: list[dict[str, Any]]

    def summary(self) -> dict[str, Any]:
        return {
            "registry_entries": len(self.registry_entries),
            "clusters": len(self.clusters),
            "lineages": len(self.lineages),
            "metrics": len(self.metrics),
            "mechanism_families": [record["mechanism_family"] for record in self.registry_entries],
            "unknown_retained_not_promoted": any(record["mechanism_family"] == "UNKNOWN" and record["status"] == "RETAINED_NOT_PROMOTED" for record in self.registry_entries),
        }


class AtlasV2MechanismRegistry:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def build_from_claim_batch(
        self,
        *,
        batch_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> MechanismRegistryResult:
        batch = select_batch(self.ledger, batch_id)
        batch_id_value = str(batch["batch_id"])
        run = select_claim_batch_run(self.ledger, batch_id_value)
        mechanism_records = records_by_ids(self.ledger.records("ExternalStrategyMechanism"), "mechanism_id", run.get("mechanism_record_ids", []))
        contrarian_records = records_by_ids(self.ledger.records("ExternalStrategyContrarianTheory"), "contrarian_id", run.get("contrarian_record_ids", []))
        handoff_records = records_by_ids(self.ledger.records("ExternalStrategyCheapExperimentHandoff"), "handoff_id", run.get("handoff_record_ids", []))
        phase1_registry_records = registry_records_for_handoffs(self.ledger.records("MechanismRegistry"), handoff_records)
        if not phase1_registry_records and mechanism_records:
            phase1_registry_records = synthesize_registry_from_mechanisms(mechanism_records, contrarian_records, handoff_records, created_at=created_at)
        if not phase1_registry_records:
            raise AtlasV2ValidationError(f"No Claim Batch Processor mechanism records found for batch: {batch_id_value}")

        grouped = group_phase1_mechanisms(phase1_registry_records, mechanism_records, contrarian_records, handoff_records)
        registry_entries: list[dict[str, Any]] = []
        clusters: list[dict[str, Any]] = []
        lineages: list[dict[str, Any]] = []
        metrics: list[dict[str, Any]] = []

        for family in sorted(grouped):
            group = grouped[family]
            mechanism_id = f"mre-{short_hash(family)}"
            status = "RETAINED_NOT_PROMOTED" if family == "UNKNOWN" else "ACTIVE_RESEARCH"
            claim_count = group["claim_count"]
            source_count = group["source_count"]
            contrarian_count = group["contrarian_count"]
            cheap_count = group["cheap_experiment_eligibility_count"] if family != "UNKNOWN" else 0
            learning_event_count = count_learning_events_for_family(self.ledger, family)
            registry_entries.append(
                self.ledger.create_record(
                    "MechanismRegistryEntry",
                    {
                        "mechanism_id": mechanism_id,
                        "mechanism_family": family,
                        "created_at": created_at,
                        "updated_at": created_at,
                        "claim_count": claim_count,
                        "source_count": source_count,
                        "contrarian_count": contrarian_count,
                        "cheap_experiment_eligibility_count": cheap_count,
                        "learning_event_count": learning_event_count,
                        "status": status,
                    },
                    reason="mechanism registry entry appended from Claim Batch Processor output",
                    triggering_object=f"ClaimBatch:{batch_id_value}",
                )
            )
            representative_claim_ids = sorted(group["claim_ids"]) or [f"mechanism-family:{family}"]
            clusters.append(
                self.ledger.create_record(
                    "MechanismCluster",
                    {
                        "cluster_id": f"mcl-{short_hash(batch_id_value + ':' + family)}",
                        "created_at": created_at,
                        "mechanism_family": family,
                        "claim_ids": representative_claim_ids,
                        "claim_count": claim_count,
                        "source_diversity": source_count,
                        "confidence": group["confidence"],
                        "status": "RETAINED_NOT_PROMOTED" if family == "UNKNOWN" else "CLUSTERED",
                    },
                    reason="mechanism cluster appended at family granularity",
                    triggering_object=f"MechanismRegistryEntry:{mechanism_id}",
                )
            )
            lineages.append(
                self.ledger.create_record(
                    "MechanismLineage",
                    {
                        "lineage_id": f"mln-{short_hash(mechanism_id + ':' + batch_id_value)}",
                        "mechanism_id": mechanism_id,
                        "variant_description": "Root mechanism family observed from supplied claim batch." if family != "UNKNOWN" else "Unknown mechanism retained for clarification only.",
                        "lineage_reason": "Initial durable mechanism registry entry from Claim Batch Processor V1 output.",
                        "status": "ROOT_RECORDED" if family != "UNKNOWN" else "UNKNOWN_RETAINED",
                    },
                    reason="mechanism lineage appended without variant generation or promotion",
                    triggering_object=f"MechanismCluster:{clusters[-1]['cluster_id']}",
                )
            )
            unique_mechanism_count = max(1, group["unique_mechanism_count"])
            metrics.append(
                self.ledger.create_record(
                    "MechanismMetrics",
                    {
                        "metrics_id": f"mmt-{short_hash(mechanism_id + ':' + batch_id_value + ':' + str(len(self.ledger.records('MechanismMetrics'))))}",
                        "mechanism_id": mechanism_id,
                        "claim_count": claim_count,
                        "duplicate_ratio": round(1 - (unique_mechanism_count / claim_count), 6) if claim_count else 0.0,
                        "contrarian_coverage": round(contrarian_count / unique_mechanism_count, 6),
                        "cheap_experiment_coverage": round(cheap_count / unique_mechanism_count, 6),
                        "learning_event_count": learning_event_count,
                        "last_updated": created_at,
                    },
                    reason="mechanism metrics appended without overwriting prior counts",
                    triggering_object=f"MechanismLineage:{lineages[-1]['lineage_id']}",
                )
            )
        return MechanismRegistryResult(registry_entries=registry_entries, clusters=clusters, lineages=lineages, metrics=metrics)


def select_batch(ledger: AtlasV2Ledger, batch_id: str | None) -> dict[str, Any]:
    batches = ledger.records("ClaimBatch")
    if batch_id:
        for batch in reversed(batches):
            if batch.get("batch_id") == batch_id:
                return batch
        raise AtlasV2ValidationError(f"ClaimBatch not found: {batch_id}")
    if not batches:
        raise AtlasV2ValidationError("Mechanism Registry V1 requires ClaimBatchProcessor output")
    return batches[-1]




def select_claim_batch_run(ledger: AtlasV2Ledger, batch_id: str) -> dict[str, Any]:
    for run in reversed(ledger.records("ClaimBatchRun")):
        if run.get("batch_id") == batch_id:
            return run
    raise AtlasV2ValidationError(f"ClaimBatchRun not found for batch: {batch_id}")


def records_by_ids(records: list[dict[str, Any]], id_field: str, ids: list[Any]) -> list[dict[str, Any]]:
    wanted = {str(item) for item in ids}
    return [record for record in records if str(record.get(id_field)) in wanted]


def records_for_batch(records: list[dict[str, Any]], batch_id: str) -> list[dict[str, Any]]:
    marker = f"ClaimBatch:{batch_id}"
    result: list[dict[str, Any]] = []
    for record in records:
        history = record.get("transition_history") or []
        if any(isinstance(item, dict) and item.get("triggering_object") == marker for item in history):
            result.append(record)
    return result




def registry_records_for_handoffs(records: list[dict[str, Any]], handoff_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    handoff_markers = {f"ExternalStrategyCheapExperimentHandoff:{record.get('handoff_id')}" for record in handoff_records}
    result: list[dict[str, Any]] = []
    for record in records:
        history = record.get("transition_history") or []
        if any(isinstance(item, dict) and item.get("triggering_object") in handoff_markers for item in history):
            result.append(record)
    return result


def synthesize_registry_from_mechanisms(
    mechanism_records: list[dict[str, Any]],
    contrarian_records: list[dict[str, Any]],
    handoff_records: list[dict[str, Any]],
    *,
    created_at: str,
) -> list[dict[str, Any]]:
    synthetic: list[dict[str, Any]] = []
    for mechanism in mechanism_records:
        mechanism_id = str(mechanism["mechanism_id"])
        synthetic.append({
            "mechanism_id": mechanism_id,
            "created_at": created_at,
            "mechanism_family": mechanism["mechanism_family"],
            "claim_count": 1,
            "source_count": 1,
            "contrarian_count": sum(1 for record in contrarian_records if record.get("mechanism_id") == mechanism_id),
            "cheap_experiment_count": sum(1 for record in handoff_records if record.get("mechanism_id") == mechanism_id and record.get("eligible_for_cheap_experiment") is True),
            "last_updated": created_at,
        })
    return synthetic


def group_phase1_mechanisms(
    phase1_registry_records: list[dict[str, Any]],
    mechanism_records: list[dict[str, Any]],
    contrarian_records: list[dict[str, Any]],
    handoff_records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    mechanism_by_id = {str(record.get("mechanism_id")): record for record in mechanism_records}
    contrarian_by_mechanism = defaultdict(int)
    for record in contrarian_records:
        contrarian_by_mechanism[str(record.get("mechanism_id"))] += 1
    eligible_by_mechanism = defaultdict(int)
    for record in handoff_records:
        if record.get("eligible_for_cheap_experiment") is True:
            eligible_by_mechanism[str(record.get("mechanism_id"))] += 1
    grouped: dict[str, dict[str, Any]] = {}
    for record in phase1_registry_records:
        family = str(record.get("mechanism_family") or "UNKNOWN")
        mechanism_id = str(record.get("mechanism_id") or "")
        group = grouped.setdefault(family, {
            "claim_count": 0,
            "source_count": 0,
            "contrarian_count": 0,
            "cheap_experiment_eligibility_count": 0,
            "unique_mechanism_count": 0,
            "claim_ids": set(),
            "confidence_values": [],
        })
        group["claim_count"] += int(record.get("claim_count") or 0)
        group["source_count"] += int(record.get("source_count") or 0)
        group["contrarian_count"] += int(record.get("contrarian_count") or contrarian_by_mechanism.get(mechanism_id, 0))
        group["cheap_experiment_eligibility_count"] += int(record.get("cheap_experiment_count") or eligible_by_mechanism.get(mechanism_id, 0))
        group["unique_mechanism_count"] += 1
        mechanism = mechanism_by_id.get(mechanism_id)
        if mechanism and mechanism.get("claim_id"):
            group["claim_ids"].add(str(mechanism["claim_id"]))
            confidence = mechanism.get("classification_confidence")
            if isinstance(confidence, (int, float)):
                group["confidence_values"].append(float(confidence))
    for group in grouped.values():
        values = group.pop("confidence_values")
        group["confidence"] = round(sum(values) / len(values), 6) if values else 0.0
    return grouped


def count_learning_events_for_family(ledger: AtlasV2Ledger, family: str) -> int:
    count = 0
    family_lower = family.lower()
    for object_type in ("LearningEstimate", "AttentionSignal", "ExperienceEvent", "LearningVelocityMetric"):
        for record in ledger.records(object_type):
            material = json.dumps(record, sort_keys=True).lower()
            if family_lower in material:
                count += 1
    return count


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 Mechanism Registry V1")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--batch-id")
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2MechanismRegistry(AtlasV2Ledger(Path(args.ledger_root))).build_from_claim_batch(
        batch_id=args.batch_id,
        created_at=args.created_at,
    )
    print(json.dumps(result.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
