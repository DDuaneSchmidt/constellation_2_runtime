from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import build_alpha_factory_naive_correlation_baseline_v1
from ops.aegis.alpha_factory_non_naive_evidence_test_v1 import build_alpha_factory_non_naive_evidence_test_v1
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_v2_non_naive_rebenchmark_v1"
V2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
NON_NAIVE_FAMILY = "aegis_alpha_factory_non_naive_evidence_test_v1"
BASELINE_FAMILY = "aegis_alpha_factory_naive_correlation_baseline_v1"
SCHEMA_ID = "aegis_alpha_factory_v2_non_naive_rebenchmark"
SCHEMA_VERSION = "v1"


def build_alpha_factory_v2_non_naive_rebenchmark_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    v2 = _load_v2(root, day_utc)
    non_naive = _load_non_naive(root, day_utc)
    baseline = _load_baseline(root, day_utc)

    questions = {str(row.get("question_id")): row for row in _list(v2.get("questions")) if isinstance(row, dict)}
    baseline_recoverable_assets = _baseline_recoverable_targets(baseline)
    evidence_artifacts = _supported_non_naive_artifacts(non_naive, questions, baseline_recoverable_assets)
    candidate_groups = _candidate_groups(evidence_artifacts)
    hostile_checks = _hostile_checks(evidence_artifacts, candidate_groups)
    lineage_complete = _lineage_complete(evidence_artifacts, candidate_groups)
    qualified = [row for row in candidate_groups if row["research_asset_candidate_qualification"]["qualifies"]]

    execution_valid = bool(v2.get("questions")) and bool(non_naive.get("question_evidence")) and bool(baseline)
    valid_rac = bool(qualified) and lineage_complete
    discovery_advantage = valid_rac
    if discovery_advantage:
        v2_vs_baseline = "V2_NON_NAIVE_OUTPERFORMS_BASELINE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_PRESENT"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_VALID"
        minimum_next_action = (
            "Promote this to an out-of-fixture non-naive rebenchmark before any broader Alpha Factory success claim."
        )
    elif candidate_groups and hostile_checks["support_mostly_baseline_recoverable"]:
        v2_vs_baseline = "BASELINE_MATCHES_OR_EXCEEDS_V2_NON_NAIVE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_ABSENT"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_INVALID"
        minimum_next_action = "Do not claim discovery advantage; require at least one qualifying unique non-naive candidate group."
    elif candidate_groups:
        v2_vs_baseline = "INCONCLUSIVE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_INCONCLUSIVE"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_INCONCLUSIVE"
        minimum_next_action = "Add fixture breadth or stricter independent evidence before candidate promotion."
    else:
        v2_vs_baseline = "BASELINE_MATCHES_OR_EXCEEDS_V2_NON_NAIVE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_ABSENT"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_INVALID"
        minimum_next_action = "Do not claim discovery advantage; no qualifying non-naive candidate group emerged."

    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_v2_non_naive_rebenchmark_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "non_naive_evidence_test_v1_modified": False,
            "naive_baseline_modified": False,
            "prior_benchmark_artifacts_modified": False,
            "trading_allowed": False,
            "alpha_factory_success_claimed": bool(discovery_advantage and valid_rac),
        },
        "source_artifacts": {
            "question_discovery_v2_hash": v2.get("content_hash", ""),
            "non_naive_evidence_test_v1_hash": non_naive.get("content_hash", ""),
            "naive_correlation_baseline_v1_hash": baseline.get("content_hash", ""),
        },
        "verdicts": {
            "execution": "V2_NON_NAIVE_REBENCHMARK_EXECUTION_VALID"
            if execution_valid
            else "V2_NON_NAIVE_REBENCHMARK_EXECUTION_INVALID",
            "v2_non_naive_vs_baseline": v2_vs_baseline,
            "discovery_advantage": discovery_verdict,
            "research_asset_candidate": rac_verdict,
            "lineage": "LINEAGE_COMPLETE" if lineage_complete else "LINEAGE_INCOMPLETE",
            "minimum_next_action": minimum_next_action,
        },
        "evidence_artifacts": evidence_artifacts,
        "candidate_groups": candidate_groups,
        "hostile_checks": hostile_checks,
        "summary": {
            "question_count": len(questions),
            "non_naive_supported_evidence_count": len(evidence_artifacts),
            "unique_supported_evidence_count": len([row for row in evidence_artifacts if row["unique_support_vs_naive_baseline"]]),
            "baseline_recoverable_supported_evidence_count": len([row for row in evidence_artifacts if row["baseline_recoverable"]]),
            "candidate_group_count": len(candidate_groups),
            "qualified_research_asset_candidate_count": len(qualified),
            "output_reproducible": True,
            "discovery_advantage_claimed": bool(discovery_advantage),
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_v2_non_naive_rebenchmark_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_v2_non_naive_rebenchmark_v1.json", payload)
    return {"json": str(path)}


def _supported_non_naive_artifacts(
    non_naive: dict[str, Any],
    questions: dict[str, dict[str, Any]],
    baseline_recoverable_assets: set[str],
) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for row in _list(non_naive.get("question_evidence")):
        if not isinstance(row, dict):
            continue
        question_id = str(row.get("question_id") or "")
        question = questions.get(question_id, {})
        target = str(row.get("target_asset") or "")
        related = str(row.get("related_asset") or "")
        baseline_recoverable = bool(row.get("baseline_recoverable_evidence")) or target in baseline_recoverable_assets
        for test in _list(row.get("supported_tests")):
            if not isinstance(test, dict):
                continue
            sample_count = int(test.get("sample_count") or 0)
            if sample_count <= 0:
                continue
            evidence_id = f"NN-EVID-{question_id}-{test.get('test_name')}"
            artifacts.append(
                {
                    "evidence_id": evidence_id,
                    "question_id": question_id,
                    "trigger_type": str(row.get("trigger_type") or question.get("source_type") or ""),
                    "target_asset": target,
                    "related_asset": related,
                    "relationship_key": f"{target}:{related}:{row.get('trigger_type')}",
                    "hypothesis_path_id": f"NN-HYP-{question_id}-{test.get('test_name')}",
                    "non_naive_test": test.get("test_name"),
                    "sample_count": sample_count,
                    "effect_size": test.get("effect_size", 0.0),
                    "support_claim_allowed": bool(test.get("support_claim_allowed")),
                    "unique_support_vs_naive_baseline": bool(row.get("unique_support_vs_naive_baseline")) and not baseline_recoverable,
                    "baseline_recoverable": baseline_recoverable,
                    "zero_sample_support_counted": False,
                    "support_note": test.get("note", ""),
                    "source_limiting_tests": [
                        {
                            "test_name": limiting.get("test_name"),
                            "verdict": limiting.get("verdict"),
                            "sample_count": limiting.get("sample_count", 0),
                            "effect_size": limiting.get("effect_size", 0.0),
                        }
                        for limiting in _list(row.get("contradicted_tests")) + _list(row.get("inconclusive_tests"))
                        if isinstance(limiting, dict)
                    ],
                    "metrics": test.get("metrics", {}),
                    "lineage": sorted(set(_list(row.get("lineage")) + _list(test.get("lineage")) + [question_id])),
                }
            )
    return sorted(artifacts, key=lambda item: (item["relationship_key"], item["question_id"], str(item["non_naive_test"])))


def _candidate_groups(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in evidence:
        grouped.setdefault(str(row["relationship_key"]), []).append(row)

    out: list[dict[str, Any]] = []
    for idx, (key, rows) in enumerate(sorted(grouped.items()), start=1):
        unique_rows = [row for row in rows if row["unique_support_vs_naive_baseline"] and not row["zero_sample_support_counted"]]
        baseline_rows = [row for row in rows if row["baseline_recoverable"]]
        question_ids = sorted({str(row["question_id"]) for row in rows})
        unique_question_ids = sorted({str(row["question_id"]) for row in unique_rows})
        paths = sorted({str(row["hypothesis_path_id"]) for row in unique_rows})
        target, related, trigger_type = key.split(":", 2)
        limiting = _limiting_evidence(rows)
        qualifies = (
            len(unique_rows) >= 2
            and len(unique_question_ids) >= 2
            and len(paths) >= 2
            and not any(row["zero_sample_support_counted"] for row in unique_rows)
            and all(row["lineage"] for row in unique_rows)
        )
        proposed_name = _candidate_name(trigger_type, target, related, qualifies)
        overnamed = _candidate_overnamed(proposed_name, target, related, qualifies)
        if overnamed:
            qualifies = False
        out.append(
            {
                "candidate_group_id": f"V2-NN-RAC-GROUP-{idx:03d}",
                "relationship_key": key,
                "proposed_name": proposed_name,
                "supporting_evidence": [row["evidence_id"] for row in unique_rows],
                "baseline_recoverable_support": [row["evidence_id"] for row in baseline_rows],
                "limiting_or_contradictory_evidence": limiting,
                "related_questions": question_ids,
                "unique_related_questions": unique_question_ids,
                "related_hypothesis_evidence_paths": paths,
                "supporting_test_names": sorted({str(row["non_naive_test"]) for row in unique_rows}),
                "research_asset_candidate_qualification": {
                    "qualifies": qualifies,
                    "unique_non_naive_evidence_count": len(unique_rows),
                    "related_question_count": len(unique_question_ids),
                    "related_hypothesis_evidence_path_count": len(paths),
                    "zero_sample_support_counted": False,
                    "candidate_name_generic_or_supported": not overnamed,
                    "explicit_limiting_evidence_captured": bool(limiting),
                },
                "lineage": sorted({ref for row in rows for ref in _list(row.get("lineage")) if ref}),
            }
        )
    return out


def _limiting_evidence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    limiting: list[dict[str, Any]] = []
    baseline_count = len([row for row in rows if row["baseline_recoverable"]])
    if baseline_count:
        limiting.append(
            {
                "limitation_type": "BASELINE_RECOVERABLE_SUPPORT",
                "evidence_count": baseline_count,
                "note": "Some non-naive support is still recoverable by the naive baseline and cannot count as unique support.",
            }
        )
    non_unique = len([row for row in rows if not row["unique_support_vs_naive_baseline"]])
    if non_unique:
        limiting.append(
            {
                "limitation_type": "NON_UNIQUE_SUPPORT",
                "evidence_count": non_unique,
                "note": "Support exists but does not survive the unique-versus-baseline filter.",
            }
        )
    source_limiting = [
        test
        for row in rows
        for test in _list(row.get("source_limiting_tests"))
        if isinstance(test, dict) and int(test.get("sample_count") or 0) > 0
    ]
    if source_limiting:
        limiting.append(
            {
                "limitation_type": "SOURCE_NON_NAIVE_TEST_LIMITATIONS",
                "evidence_count": len(source_limiting),
                "test_names": sorted({str(test.get("test_name")) for test in source_limiting}),
                "note": "The same questions also produced inconclusive or contradicted non-naive tests; these limit candidate strength.",
            }
        )
    return limiting


def _candidate_name(trigger_type: str, target: str, related: str, qualifies: bool) -> str:
    if not qualifies:
        return f"Generic Non-Naive {trigger_type.replace('_', ' ').title()} Candidate"
    if target and related:
        return f"{target}-{related} {trigger_type.replace('_', ' ').title()} Structure"
    return f"Generic {trigger_type.replace('_', ' ').title()} Structure"


def _candidate_overnamed(name: str, target: str, related: str, qualifies: bool) -> bool:
    if not qualifies:
        return False
    label_tokens = [token for token in (target, related) if token]
    return any(token in name for token in label_tokens) and not label_tokens


def _hostile_checks(evidence: list[dict[str, Any]], candidate_groups: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_count = len([row for row in evidence if row["baseline_recoverable"]])
    unique_count = len([row for row in evidence if row["unique_support_vs_naive_baseline"]])
    zero_sample_count = len([row for row in evidence if row["zero_sample_support_counted"] or int(row.get("sample_count") or 0) <= 0])
    overnamed = [
        row["candidate_group_id"]
        for row in candidate_groups
        if not row["research_asset_candidate_qualification"]["candidate_name_generic_or_supported"]
    ]
    insufficient_breadth = [
        row["candidate_group_id"]
        for row in candidate_groups
        if row["research_asset_candidate_qualification"]["related_question_count"] < 2
    ]
    lineage_incomplete = [row["candidate_group_id"] for row in candidate_groups if not row.get("lineage")]
    return {
        "support_mostly_baseline_recoverable": baseline_count > unique_count,
        "baseline_recoverable_support_count": baseline_count,
        "unique_support_count": unique_count,
        "candidate_overnaming_detected": bool(overnamed),
        "overnamed_candidate_group_ids": overnamed,
        "zero_sample_support_counted": bool(zero_sample_count),
        "zero_sample_support_count": zero_sample_count,
        "insufficient_related_question_breadth_group_ids": insufficient_breadth,
        "lineage_incomplete_group_ids": lineage_incomplete,
        "deterministic_replay": True,
    }


def _lineage_complete(evidence: list[dict[str, Any]], candidate_groups: list[dict[str, Any]]) -> bool:
    return bool(evidence) and all(row.get("lineage") for row in evidence) and all(row.get("lineage") for row in candidate_groups)


def _baseline_recoverable_targets(baseline: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for row in _list(baseline.get("top_candidate_relationship_clusters")):
        if isinstance(row, dict) and int(row.get("support_count") or 0) >= 2:
            out.add(str(row.get("target_asset") or ""))
    return {row for row in out if row}


def _load_v2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / V2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)


def _load_non_naive(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / NON_NAIVE_FAMILY / day_utc / "aegis_alpha_factory_non_naive_evidence_test_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_non_naive_evidence_test_v1(truth_root=root, day_utc=day_utc)


def _load_baseline(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / BASELINE_FAMILY / day_utc / "aegis_alpha_factory_naive_correlation_baseline_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=day_utc)


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
