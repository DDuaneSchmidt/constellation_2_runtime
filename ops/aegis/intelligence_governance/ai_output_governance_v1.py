from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import ai_evidence_v1
from ops.aegis.intelligence_governance.evidence_chain_v1 import file_hash_v1, object_hash_v1


ALLOWED_AI_OUTPUT_TYPES = {"INTERPRETATION", "RECOMMENDATION", "SUMMARY"}
FORBIDDEN_AI_OUTPUT_TYPES = {"FACT", "METRIC", "APPROVAL", "RUNTIME_PERMISSION", "EXECUTION_INSTRUCTION"}


def govern_ai_output_v1(
    *,
    repo_root: Path,
    output_type: str,
    prompt: str,
    input_artifacts: list[str],
    output_payload: dict[str, Any],
    evidence_citations: list[str],
) -> dict[str, Any]:
    ai = ai_evidence_v1(repo_root)
    output_type = output_type.upper()
    prohibited = []
    if output_type in FORBIDDEN_AI_OUTPUT_TYPES:
        prohibited.append(f"AI_OUTPUT_TYPE_FORBIDDEN:{output_type}")
    if output_type not in ALLOWED_AI_OUTPUT_TYPES:
        prohibited.append(f"AI_OUTPUT_TYPE_NOT_ALLOWED:{output_type}")
    return {
        "output_type": output_type,
        "model_provider": "" if not ai["ai_used"] else ai.get("model_used", "UNKNOWN"),
        "prompt_hash": object_hash_v1({"prompt": prompt}) if prompt else "",
        "input_artifact_hashes": [file_hash_v1(path) for path in input_artifacts],
        "output_hash": object_hash_v1(output_payload),
        "ai_used": bool(ai["ai_used"]),
        "deterministic_fallback": bool(ai["deterministic_fallback"]),
        "evidence_citations": evidence_citations,
        "prohibited_claim_check": "PASS" if not prohibited else "FAIL",
        "prohibited_claim_violations": prohibited,
        "human_approval_required": True,
        "automated_change_allowed": False,
    }
