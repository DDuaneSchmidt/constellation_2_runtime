from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import ai_evidence_v1


LAYERS = {"FACT", "METRIC", "INTERPRETATION", "RECOMMENDATION", "ACTION"}


def file_hash_v1(path: str | Path) -> str:
    p = Path(path)
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()
    except OSError:
        return ""


def object_hash_v1(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def evidence_chain_item_v1(
    *,
    conclusion_id: str,
    layer: str,
    source_artifacts: list[str],
    formula_or_rule: str | None = None,
    sample_size: int | None = None,
    minimum_sample_size: int | None = None,
    evidence_quality: str = "UNKNOWN",
    confidence: str = "UNKNOWN",
    repo_root: Path,
) -> dict[str, Any]:
    ai = ai_evidence_v1(repo_root)
    layer = layer.upper()
    if layer not in LAYERS:
        raise ValueError(f"unknown evidence layer: {layer}")
    return {
        "conclusion_id": conclusion_id,
        "layer": layer,
        "source_artifacts": source_artifacts,
        "source_hashes": [file_hash_v1(path) for path in source_artifacts],
        "formula_or_rule": formula_or_rule,
        "sample_size": sample_size,
        "minimum_sample_size": minimum_sample_size,
        "evidence_quality": evidence_quality,
        "confidence": _bounded_confidence(confidence, evidence_quality, sample_size, minimum_sample_size),
        "ai_used": bool(ai["ai_used"] and layer in {"INTERPRETATION", "RECOMMENDATION"}),
        "deterministic_fallback": bool(ai["deterministic_fallback"]),
        "human_approval_required": layer in {"RECOMMENDATION", "ACTION"},
        "automated_change_allowed": False,
        "runtime_truth_mutation_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def validate_evidence_chain_v1(chain: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    layers = {str(item.get("layer") or "") for item in chain if isinstance(item, dict)}
    for item in chain:
        if not isinstance(item, dict):
            issues.append("CHAIN_ITEM_NOT_OBJECT")
            continue
        layer = str(item.get("layer") or "")
        cid = str(item.get("conclusion_id") or "UNKNOWN")
        if layer == "FACT" and not item.get("source_artifacts"):
            issues.append(f"{cid}:FACT_REQUIRES_SOURCE_ARTIFACT")
        if layer == "METRIC":
            if not item.get("formula_or_rule"):
                issues.append(f"{cid}:METRIC_REQUIRES_FORMULA")
            if item.get("sample_size") is None:
                issues.append(f"{cid}:METRIC_REQUIRES_SAMPLE_SIZE")
        if layer == "INTERPRETATION" and not ({"FACT", "METRIC"} & layers):
            issues.append(f"{cid}:INTERPRETATION_REQUIRES_FACT_OR_METRIC")
        if layer == "RECOMMENDATION" and "INTERPRETATION" not in layers:
            issues.append(f"{cid}:RECOMMENDATION_REQUIRES_INTERPRETATION")
        if bool(item.get("ai_used")) and layer not in {"INTERPRETATION", "RECOMMENDATION"}:
            issues.append(f"{cid}:AI_NOT_ALLOWED_FOR_{layer}")
        for field in ("automated_change_allowed", "runtime_truth_mutation_allowed", "broker_execution_allowed", "autonomous_execution_allowed"):
            if bool(item.get(field)):
                issues.append(f"{cid}:{field.upper()}_FORBIDDEN")
    return issues


def _bounded_confidence(confidence: str, quality: str, sample_size: int | None, minimum: int | None) -> str:
    confidence = str(confidence or "UNKNOWN").upper()
    quality = str(quality or "UNKNOWN").upper()
    if quality in {"INSUFFICIENT", "UNKNOWN"}:
        return "UNKNOWN" if confidence == "HIGH" else confidence
    if sample_size is not None and minimum is not None and sample_size < minimum:
        return "LOW" if confidence in {"HIGH", "MEDIUM"} else confidence
    if quality == "LOW" and confidence == "HIGH":
        return "LOW"
    return confidence if confidence in {"HIGH", "MEDIUM", "LOW", "UNKNOWN"} else "UNKNOWN"
