from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.alpha_factory_question_discovery_v2_rebenchmark_v1 import (
    build_alpha_factory_question_discovery_v2_rebenchmark_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_question_productivity_diagnostic_v1"
V2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
REBENCHMARK_FAMILY = "aegis_alpha_factory_question_discovery_v2_rebenchmark_v1"
SCHEMA_ID = "aegis_alpha_factory_question_productivity_diagnostic"
SCHEMA_VERSION = "v1"


def build_alpha_factory_question_productivity_diagnostic_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    v2 = _load_v2(root, day_utc)
    rebenchmark = _load_rebenchmark(root, day_utc)
    question_rows = _question_rows(v2=v2, rebenchmark=rebenchmark)
    trigger_aggregates = _trigger_aggregates(question_rows)
    failure_analysis = _failure_analysis(question_rows, rebenchmark)
    hostile_checks = _hostile_checks(question_rows, trigger_aggregates, rebenchmark)
    productive_unique_count = len([row for row in question_rows if row["classification"] == "PRODUCTIVE_UNIQUE"])
    productive_count = len([row for row in question_rows if row["classification"].startswith("PRODUCTIVE")])
    productivity_sufficient = productive_unique_count >= 2 and not hostile_checks["evidence_tests_too_similar_to_naive_baseline"]
    execution_valid = bool(question_rows) and bool(rebenchmark.get("evidence")) and bool(rebenchmark.get("hypotheses"))
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_question_productivity_diagnostic_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "v2_rebenchmark_modified": False,
            "naive_baseline_modified": False,
            "candidate_naming_rules_modified": False,
            "trading_allowed": False,
            "question_discovery_v3_built": False,
        },
        "source_artifacts": {
            "question_discovery_v2_hash": v2.get("content_hash"),
            "v2_rebenchmark_hash": rebenchmark.get("content_hash"),
        },
        "verdicts": {
            "execution": "DIAGNOSTIC_EXECUTION_VALID" if execution_valid else "DIAGNOSTIC_EXECUTION_INVALID",
            "question_productivity": "QUESTION_PRODUCTIVITY_SUFFICIENT"
            if productivity_sufficient
            else "QUESTION_PRODUCTIVITY_INSUFFICIENT",
            "primary_failure_mode": failure_analysis["primary_failure_mode"],
            "v3_decision": "V3_REQUIRED" if failure_analysis["primary_failure_mode"] != "UNKNOWN" else "DO_NOT_BUILD_V3_YET",
            "minimum_next_action": failure_analysis["minimum_next_action"],
        },
        "question_diagnostics": question_rows,
        "trigger_type_aggregates": trigger_aggregates,
        "failure_analysis": failure_analysis,
        "hostile_checks": hostile_checks,
        "summary": {
            "question_count": len(question_rows),
            "productive_question_count": productive_count,
            "productive_unique_question_count": productive_unique_count,
            "baseline_recoverable_question_count": len([row for row in question_rows if row["baseline_recoverable"]]),
            "candidate_group_question_count": len([row for row in question_rows if row["candidate_group_created"]]),
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_question_productivity_diagnostic_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_question_productivity_diagnostic_v1.json", payload)
    return {"json": str(path)}


def _load_v2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / V2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)


def _load_rebenchmark(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / REBENCHMARK_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2_rebenchmark_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=root, day_utc=day_utc)


def _question_rows(*, v2: dict[str, Any], rebenchmark: dict[str, Any]) -> list[dict[str, Any]]:
    hypotheses_by_question: dict[str, list[dict[str, Any]]] = {}
    for hypothesis in _list(rebenchmark.get("hypotheses")):
        if isinstance(hypothesis, dict):
            hypotheses_by_question.setdefault(str(hypothesis.get("question_id")), []).append(hypothesis)
    evidence_by_question: dict[str, list[dict[str, Any]]] = {}
    for evidence in _list(rebenchmark.get("evidence")):
        if isinstance(evidence, dict):
            evidence_by_question.setdefault(str(evidence.get("question_id")), []).append(evidence)
    candidate_by_question = _candidate_groups_by_question(rebenchmark)
    baseline_recovered_candidates = {
        str(row.get("candidate_group_id"))
        for match in _list((rebenchmark.get("baseline_comparison") or {}).get("matched_candidate_groups"))
        if isinstance(match, dict)
        for row in [{"candidate_group_id": match.get("candidate_group_id")}]
    }
    rows: list[dict[str, Any]] = []
    for question in _list(v2.get("questions")):
        if not isinstance(question, dict):
            continue
        qid = str(question.get("question_id"))
        hypotheses = hypotheses_by_question.get(qid, [])
        evidence = evidence_by_question.get(qid, [])
        supported = [
            row
            for row in evidence
            if int(row.get("sample_count") or 0) >= 3
            and abs(float(row.get("effect_size") or 0.0)) > 0.0005
            and str(row.get("effect_direction")) in {"POSITIVE", "NEGATIVE"}
        ]
        limiting = [row for row in evidence if row not in supported]
        candidate_groups = candidate_by_question.get(qid, [])
        recovered_candidate_ids = [
            row.get("candidate_group_id")
            for row in candidate_groups
            if str(row.get("candidate_group_id")) in baseline_recovered_candidates
        ]
        baseline_recoverable = bool(recovered_candidate_ids)
        unique_vs_baseline = bool(supported) and not baseline_recoverable
        candidate_group_created = bool(candidate_groups)
        classification = _classify_question(
            evidence_count=len(evidence),
            supported_count=len(supported),
            baseline_recoverable=baseline_recoverable,
            candidate_group_created=candidate_group_created,
        )
        rows.append(
            {
                "question_id": qid,
                "trigger_type": str(question.get("source_type") or ""),
                "question": question.get("question"),
                "source_relationship_refs": _list(question.get("source_relationship_refs")),
                "source_feature_refs": _list(question.get("source_feature_refs")),
                "downstream_hypothesis_count": len(hypotheses),
                "downstream_evidence_count": len(evidence),
                "supported_evidence_count": len(supported),
                "contradicted_or_limiting_evidence_count": len(limiting),
                "baseline_recoverable": baseline_recoverable,
                "unique_vs_baseline": unique_vs_baseline,
                "candidate_group_created": candidate_group_created,
                "candidate_group_ids": [row.get("candidate_group_id") for row in candidate_groups],
                "baseline_recovered_candidate_group_ids": recovered_candidate_ids,
                "classification": classification,
                "lineage": _list(question.get("lineage")),
            }
        )
    return rows


def _candidate_groups_by_question(rebenchmark: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for candidate in _list(rebenchmark.get("candidate_groups")):
        if not isinstance(candidate, dict):
            continue
        qids = sorted({str(item) for item in _list(candidate.get("lineage")) if str(item).startswith("QDV2-") and not str(item).startswith("QDV2-TRIG-")})
        for qid in qids:
            out.setdefault(qid, []).append(candidate)
    return out


def _classify_question(
    *,
    evidence_count: int,
    supported_count: int,
    baseline_recoverable: bool,
    candidate_group_created: bool,
) -> str:
    if evidence_count == 0:
        return "UNPRODUCTIVE_NO_EVIDENCE"
    if supported_count == 0:
        return "REDUNDANT_WITH_BASELINE" if baseline_recoverable else "UNPRODUCTIVE_WEAK_EVIDENCE"
    if baseline_recoverable:
        return "PRODUCTIVE_BASELINE_RECOVERABLE" if candidate_group_created else "REDUNDANT_WITH_BASELINE"
    return "PRODUCTIVE_UNIQUE"


def _trigger_aggregates(question_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in question_rows:
        grouped.setdefault(str(row["trigger_type"]), []).append(row)
    out: list[dict[str, Any]] = []
    for trigger_type, rows in sorted(grouped.items()):
        out.append(
            {
                "trigger_type": trigger_type,
                "display_name": trigger_type.lower().replace("_", " "),
                "question_count": len(rows),
                "productive_unique_count": len([row for row in rows if row["classification"] == "PRODUCTIVE_UNIQUE"]),
                "productive_baseline_recoverable_count": len([row for row in rows if row["classification"] == "PRODUCTIVE_BASELINE_RECOVERABLE"]),
                "unproductive_no_evidence_count": len([row for row in rows if row["classification"] == "UNPRODUCTIVE_NO_EVIDENCE"]),
                "unproductive_weak_evidence_count": len([row for row in rows if row["classification"] == "UNPRODUCTIVE_WEAK_EVIDENCE"]),
                "redundant_with_baseline_count": len([row for row in rows if row["classification"] == "REDUNDANT_WITH_BASELINE"]),
                "supported_evidence_count": sum(int(row["supported_evidence_count"]) for row in rows),
                "candidate_group_count": len([row for row in rows if row["candidate_group_created"]]),
                "zero_unique_productivity": not any(row["classification"] == "PRODUCTIVE_UNIQUE" for row in rows),
            }
        )
    return out


def _failure_analysis(question_rows: list[dict[str, Any]], rebenchmark: dict[str, Any]) -> dict[str, Any]:
    no_evidence = len([row for row in question_rows if row["downstream_evidence_count"] == 0])
    weak = len([row for row in question_rows if row["downstream_evidence_count"] > 0 and row["supported_evidence_count"] == 0])
    baseline_recoverable = len([row for row in question_rows if row["baseline_recoverable"]])
    unique = len([row for row in question_rows if row["unique_vs_baseline"]])
    candidate_groups = _list(rebenchmark.get("candidate_groups"))
    out_of_fixture = rebenchmark.get("out_of_fixture_context") if isinstance(rebenchmark.get("out_of_fixture_context"), dict) else {}
    if baseline_recoverable and unique == 0:
        primary = "BASELINE_REDUNDANCY"
        minimum = "Do not build v3 around the current forward-return evidence test. First design a non-naive evidence test or require out-of-fixture unique support."
    elif weak >= max(1, len(question_rows) // 2):
        primary = "WEAK_EVIDENCE_TESTS"
        minimum = "Tighten evidence tests before v3; current triggers mostly fail to produce supported evidence."
    elif no_evidence:
        primary = "WEAK_HYPOTHESIS_GENERATION"
        minimum = "Repair hypothesis generation coverage before changing question discovery."
    elif out_of_fixture.get("prior_poc_generalization") == "POC_DOES_NOT_GENERALIZE":
        primary = "INSUFFICIENT_FIXTURE_DIVERSITY"
        minimum = "Run v2 productivity diagnostics on the alternate fixture before changing question discovery."
    elif not candidate_groups:
        primary = "WEAK_TRIGGER_SELECTION"
        minimum = "Inspect trigger thresholds before v3; no candidate groups were created."
    else:
        primary = "UNKNOWN"
        minimum = "Do not build v3 yet; collect another deterministic diagnostic run."
    return {
        "primary_failure_mode": primary,
        "failure_mode_counts": {
            "weak_trigger_selection": 1 if primary == "WEAK_TRIGGER_SELECTION" else 0,
            "weak_hypothesis_generation": no_evidence,
            "weak_evidence_tests": weak,
            "baseline_redundancy": baseline_recoverable,
            "insufficient_fixture_diversity": 1 if out_of_fixture else 0,
        },
        "candidate_group_count": len(candidate_groups),
        "unique_question_count": unique,
        "minimum_next_action": minimum,
    }


def _hostile_checks(
    question_rows: list[dict[str, Any]],
    trigger_aggregates: list[dict[str, Any]],
    rebenchmark: dict[str, Any],
) -> dict[str, Any]:
    candidate_groups = _list(rebenchmark.get("candidate_groups"))
    unsupported_candidates = [
        row.get("candidate_group_id")
        for row in candidate_groups
        if isinstance(row, dict) and (not row.get("supporting_evidence") or int(row.get("supporting_sample_count") or 0) <= 0)
    ]
    return {
        "questions_that_generate_no_evidence": [row["question_id"] for row in question_rows if row["downstream_evidence_count"] == 0],
        "questions_whose_evidence_is_fully_baseline_recoverable": [
            row["question_id"]
            for row in question_rows
            if row["supported_evidence_count"] > 0 and row["baseline_recoverable"] and not row["unique_vs_baseline"]
        ],
        "trigger_types_with_zero_unique_productivity": [
            row["trigger_type"] for row in trigger_aggregates if row["zero_unique_productivity"]
        ],
        "candidate_groups_unsupported_by_evidence": unsupported_candidates,
        "evidence_tests_too_similar_to_naive_baseline": bool(
            (rebenchmark.get("baseline_comparison") or {}).get("baseline_recovers_any_candidate")
        ),
    }


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
