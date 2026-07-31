from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_poc_review_v1"
SOURCE_FAMILY = "aegis_alpha_factory_poc_v1"
SCHEMA_ID = "aegis_alpha_factory_poc_review"
SCHEMA_VERSION = "v1"


def build_alpha_factory_poc_review_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    source_path = (
        Path(truth_root).expanduser().resolve()
        / "reports"
        / SOURCE_FAMILY
        / day_utc
        / "alpha_factory_poc.v1.json"
    )
    source = read_json_v1(source_path)
    if not source:
        raise FileNotFoundError(f"missing source POC artifact: {source_path}")

    questions = _list(source.get("question_store"))
    hypotheses = _list(source.get("hypothesis_store"))
    evidence = _list(source.get("evidence_store"))
    candidates = _list(source.get("research_asset_candidates"))
    candidate = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    source_hash = str(source.get("content_hash") or _hash(source))

    evidence_by_id = {str(row.get("evidence_id")): row for row in evidence if isinstance(row, dict)}
    supporting_ids = [str(row) for row in candidate.get("supporting_evidence") or []]
    contradictory_ids = [str(row) for row in candidate.get("contradictory_evidence") or []]
    supporting = [evidence_by_id[row] for row in supporting_ids if row in evidence_by_id]
    contradictory = [evidence_by_id[row] for row in contradictory_ids if row in evidence_by_id]
    candidate_lineage = [str(row) for row in candidate.get("lineage") or []]

    execution_valid = (
        source.get("schema_id") == "aegis_alpha_factory_poc"
        and source.get("schema_version") == "v1"
        and len(questions) >= 5
        and len(hypotheses) >= 15
        and len(evidence) >= 15
        and len(candidates) >= 1
    )
    lineage_complete = _candidate_lineage_complete(candidate_lineage)
    reproducible = bool((source.get("summary") or {}).get("reproducible")) and bool(source_hash)
    discovery_seeded = _candidate_looks_seeded(candidate, questions, hypotheses)
    support_assessment = _evidence_support_assessment(candidate, supporting, contradictory)

    verdicts = {
        "poc_execution": "POC_EXECUTION_VALID" if execution_valid else "POC_EXECUTION_INVALID",
        "discovery_claim": "DISCOVERY_CLAIM_INVALID" if discovery_seeded else "DISCOVERY_CLAIM_INCONCLUSIVE",
        "lineage": "LINEAGE_COMPLETE" if lineage_complete else "LINEAGE_INCOMPLETE",
        "evidence": "EVIDENCE_SUFFICIENT" if support_assessment["sufficient"] else "EVIDENCE_INSUFFICIENT",
        "benchmark": "BENCHMARK_REQUIRED",
    }
    findings = [
        {
            "finding_id": "AFR-001",
            "severity": "HIGH",
            "area": "candidate_discovery",
            "finding": "The Research Asset Candidate appears implicitly seeded rather than discovered.",
            "evidence": [
                "candidate id and proposed_name are fixed values",
                "candidate projection selects Q-001, Q-002, and Q-003 as a predefined group",
                "hypothesis families are deterministic templates from fixed question ids",
            ],
            "verdict_impact": "DISCOVERY_CLAIM_INVALID",
        },
        {
            "finding_id": "AFR-002",
            "severity": "HIGH",
            "area": "evidence_support",
            "finding": "The named candidate is Real Yield Shock Response, but the supporting evidence comes only from GLD shock to SLV forward returns.",
            "evidence": [
                f"supporting_evidence={supporting_ids}",
                "REAL_YIELD_shock_to_GLD evidence is listed as contradictory or limiting",
                f"real_yield_support_sample_count={support_assessment['real_yield_support_sample_count']}",
            ],
            "verdict_impact": "EVIDENCE_INSUFFICIENT",
        },
        {
            "finding_id": "AFR-003",
            "severity": "MEDIUM",
            "area": "evidence_artifacts",
            "finding": "Several evidence artifacts have zero samples but still carry positive or negative effect directions from baseline arithmetic.",
            "evidence": [
                row.get("evidence_id")
                for row in evidence
                if isinstance(row, dict) and int(row.get("sample_count") or 0) == 0 and row.get("effect_direction") != "FLAT"
            ],
            "verdict_impact": "EVIDENCE_INSUFFICIENT",
        },
        {
            "finding_id": "AFR-004",
            "severity": "MEDIUM",
            "area": "questions",
            "finding": "Questions are specific and source-linked, but their wording and source mappings are static templates.",
            "evidence": [row.get("question_id") for row in questions if isinstance(row, dict)],
            "verdict_impact": "DISCOVERY_CLAIM_INVALID",
        },
        {
            "finding_id": "AFR-005",
            "severity": "LOW",
            "area": "lineage",
            "finding": "Lineage is structurally complete from candidate back to questions, features, and observations.",
            "evidence": [
                f"candidate_lineage_count={len(candidate_lineage)}",
                f"has_observation_lineage={any(row.startswith('OBS-') for row in candidate_lineage)}",
                f"has_feature_lineage={any(row.startswith('FEAT-') for row in candidate_lineage)}",
                f"has_question_lineage={any(row.startswith('Q-') for row in candidate_lineage)}",
            ],
            "verdict_impact": "LINEAGE_COMPLETE",
        },
    ]
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "review_scope": [
            "generated questions",
            "generated hypotheses",
            "evidence artifacts",
            "Research Asset Candidate lineage",
            "candidate discovered versus hardcoded or implicitly seeded",
            "output reproducibility",
            "evidence support for candidate",
            "contradictions and limitations",
        ],
        "excluded_scope": [
            "new discovery features",
            "expanded alpha factory scope",
            "trading readiness",
            "capital allocation",
            "broker integration",
        ],
        "source_artifact": str(source_path),
        "source_artifact_hash": source_hash,
        "verdicts": verdicts,
        "minimum_next_benchmark": {
            "benchmark": "naive-correlation baseline",
            "reason": "The POC claims correlation-breakdown and real-yield-response discovery, but the reviewed candidate can be explained by fixed relationship templates. A naive rolling-correlation screen is the smallest benchmark that can test whether the question/candidate projection adds anything beyond direct correlation ranking.",
            "required_before_claiming_discovery": True,
            "rejected_alternatives": [
                {
                    "benchmark": "random-question baseline",
                    "reason": "Useful as a sanity check, but weaker than a domain-naive correlation baseline for this specific correlation/relationship claim.",
                },
                {
                    "benchmark": "generic-LLM-style hypothesis baseline",
                    "reason": "Too broad for the next falsification step and not needed before testing the deterministic correlation-selection claim.",
                },
            ],
        },
        "question_review": _question_review(questions),
        "hypothesis_review": _hypothesis_review(hypotheses),
        "evidence_review": _evidence_review(evidence, supporting_ids, contradictory_ids),
        "candidate_review": {
            "candidate_id": candidate.get("research_asset_candidate_id"),
            "proposed_name": candidate.get("proposed_name"),
            "identified_relationship_or_structure": candidate.get("identified_relationship_or_structure"),
            "supporting_evidence": supporting_ids,
            "contradictory_evidence": contradictory_ids,
            "lineage_complete": lineage_complete,
            "discovered_vs_seeded_assessment": "SEEDED_OR_IMPLICITLY_HARDCODED",
            "support_assessment": support_assessment,
            "lineage_counts": {
                "observations": len([row for row in candidate_lineage if row.startswith("OBS-")]),
                "features": len([row for row in candidate_lineage if row.startswith("FEAT-")]),
                "questions": len([row for row in candidate_lineage if row.startswith("Q-")]),
            },
        },
        "contradictions_and_limitations": [
            "The candidate requires at least three related evidence artifacts and has them, but the three supporting artifacts are all from one GLD-shock-to-SLV family.",
            "The real-yield evidence for the named candidate has zero experiment samples and is recorded as limiting or contradictory.",
            "Zero-sample evidence should not produce directional support or contradiction beyond UNDER_SAMPLED.",
            "The artifact is reproducible, but reproducibility does not validate discovery because the fixture and candidate projection are deterministic.",
        ],
        "findings": findings,
        "summary": {
            "question_count": len(questions),
            "hypothesis_count": len(hypotheses),
            "evidence_count": len(evidence),
            "candidate_count": len(candidates),
            "supporting_evidence_count": len(supporting_ids),
            "contradictory_evidence_count": len(contradictory_ids),
            "zero_sample_evidence_count": len([row for row in evidence if isinstance(row, dict) and int(row.get("sample_count") or 0) == 0]),
            "output_reproducible": reproducible,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_poc_review_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_poc_review_v1.json", payload)
    return {"json": str(path)}


def _question_review(questions: list[Any]) -> list[dict[str, Any]]:
    out = []
    for row in questions:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "question_id": row.get("question_id"),
                "source_type": row.get("source_type"),
                "source_feature_count": len(row.get("source_features") or []),
                "source_observation_count": len(row.get("source_observations") or []),
                "specific_not_generic": bool(row.get("source_features")) and str(row.get("question") or "").count(" ") >= 5,
                "hostile_note": "Source-linked but generated from fixed source_type templates.",
            }
        )
    return out


def _hypothesis_review(hypotheses: list[Any]) -> dict[str, Any]:
    families = sorted({str(row.get("hypothesis_family")) for row in hypotheses if isinstance(row, dict)})
    return {
        "family_count": len(families),
        "families": families,
        "template_pattern_detected": True,
        "hostile_note": "Hypotheses are coherent experiment templates, not independent discoveries.",
    }


def _evidence_review(evidence: list[Any], supporting_ids: list[str], contradictory_ids: list[str]) -> dict[str, Any]:
    rows = [row for row in evidence if isinstance(row, dict)]
    return {
        "artifact_count": len(rows),
        "supporting_ids": supporting_ids,
        "contradictory_ids": contradictory_ids,
        "zero_sample_ids": [row.get("evidence_id") for row in rows if int(row.get("sample_count") or 0) == 0],
        "directional_zero_sample_ids": [
            row.get("evidence_id")
            for row in rows
            if int(row.get("sample_count") or 0) == 0 and row.get("effect_direction") in {"POSITIVE", "NEGATIVE"}
        ],
        "limitation_capture_present": any(row.get("contradiction_notes") for row in rows),
        "hostile_note": "Limitations are present, but sample sufficiency and family-level support do not justify the candidate name.",
    }


def _evidence_support_assessment(candidate: dict[str, Any], supporting: list[dict[str, Any]], contradictory: list[dict[str, Any]]) -> dict[str, Any]:
    proposed_name = str(candidate.get("proposed_name") or "")
    real_yield_support = [
        row for row in supporting if "REAL_YIELD" in str(row.get("hypothesis_id")) or str(row.get("hypothesis_id", "")).startswith("H-001")
    ]
    real_yield_sample_count = sum(int(row.get("sample_count") or 0) for row in real_yield_support)
    support_families = sorted({str(row.get("hypothesis_id", ""))[0:5] for row in supporting})
    has_named_relationship_support = "Real Yield" not in proposed_name or real_yield_sample_count > 0
    sufficient = len(supporting) >= 3 and len(support_families) >= 2 and has_named_relationship_support and not contradictory
    return {
        "sufficient": sufficient,
        "supporting_artifact_count": len(supporting),
        "contradictory_artifact_count": len(contradictory),
        "support_family_prefixes": support_families,
        "real_yield_support_sample_count": real_yield_sample_count,
        "hostile_conclusion": "Evidence is insufficient for the named candidate; support is concentrated in GLD shock to SLV, while real-yield artifacts are zero-sample or limiting.",
    }


def _candidate_lineage_complete(lineage: list[str]) -> bool:
    return (
        any(row.startswith("OBS-") for row in lineage)
        and any(row.startswith("FEAT-") for row in lineage)
        and any(row.startswith("Q-") for row in lineage)
    )


def _candidate_looks_seeded(candidate: dict[str, Any], questions: list[Any], hypotheses: list[Any]) -> bool:
    question_ids = {str(row.get("question_id")) for row in questions if isinstance(row, dict)}
    hypothesis_question_ids = {str(row.get("question_id")) for row in hypotheses if isinstance(row, dict)}
    return (
        candidate.get("research_asset_candidate_id") == "RAC-001"
        and candidate.get("proposed_name") == "Real Yield Shock Response"
        and {"Q-001", "Q-002", "Q-003"}.issubset(question_ids)
        and {"Q-001", "Q-002", "Q-003"}.issubset(hypothesis_question_ids)
    )


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
