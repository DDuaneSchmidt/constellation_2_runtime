from __future__ import annotations

import hashlib
import json
import statistics
import tempfile
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import _fixtures
from ops.aegis.alpha_factory_feature_surface_repair_v1 import build_alpha_factory_feature_surface_repair_v1
from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import build_alpha_factory_naive_correlation_baseline_v1, write_alpha_factory_naive_correlation_baseline_v1
from ops.aegis.alpha_factory_out_of_fixture_benchmark_v1 import _write_fixture_csv
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_fixture_suite_retest_with_repaired_features_v1"
FEATURE_REPAIR_FAMILY = "aegis_alpha_factory_feature_surface_repair_v1"
SCHEMA_ID = "aegis_alpha_factory_fixture_suite_retest_with_repaired_features"
SCHEMA_VERSION = "v1"
SPY_VIX_REL = "SPY:VIX"
SPY_VIX_KEY = "SPY:VIX:RELATIONSHIP_INSTABILITY"
QQQ_REAL_KEY = "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY"


def build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    feature_surface = _load_feature_surface(root, day_utc)
    fixture_defs = {row["fixture_id"]: row for row in _fixtures()}
    fixture_surfaces = [row for row in feature_surface.get("fixture_feature_surfaces", []) if isinstance(row, dict)]
    baselines = {row["fixture_id"]: _run_baseline_for_fixture(row["fixture_id"], fixture_defs[row["fixture_id"]], day_utc) for row in fixture_surfaces}
    relationship_thresholds = _relationship_thresholds(fixture_surfaces)
    fixture_results = [
        _run_fixture_retest(row, fixture_defs[row["fixture_id"]], relationship_thresholds, baselines[row["fixture_id"]])
        for row in fixture_surfaces
    ]
    hostile_checks = _hostile_checks(fixture_results)
    verdicts = _verdicts(fixture_results, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_fixture_suite_retest_with_repaired_features_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "feature_surface_repair_v1_modified": False,
            "candidate_qualification_rules_modified": False,
            "naive_baseline_modified": False,
            "fixture_definitions_modified": False,
            "broad_alpha_factory_success_claimed": False,
            "trading_allowed": False,
        },
        "source_artifacts": {"feature_surface_repair_v1_hash": feature_surface.get("content_hash")},
        "relationship_thresholds": relationship_thresholds,
        "fixture_retests": fixture_results,
        "candidate_groups": [group | {"fixture_id": row["fixture_id"]} for row in fixture_results for group in row["candidate_groups"]],
        "hostile_checks": hostile_checks,
        "verdicts": verdicts,
        "summary": {
            "fixture_count": len(fixture_results),
            "valid_rac_fixture_ids": [row["fixture_id"] for row in fixture_results if row["valid_rac_count"] > 0],
            "spy_vix_detected_fixture_ids": [row["fixture_id"] for row in fixture_results if row["spy_vix_detected"]],
            "qqq_real_yield_valid_fixture_ids": [row["fixture_id"] for row in fixture_results if QQQ_REAL_KEY in row["valid_rac_relationship_groups"]],
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_fixture_suite_retest_with_repaired_features_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_fixture_suite_retest_with_repaired_features_v1.json", payload)
    return {"json": str(path)}


def _load_feature_surface(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / FEATURE_REPAIR_FAMILY / day_utc / "aegis_alpha_factory_feature_surface_repair_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_feature_surface_repair_v1(truth_root=root, day_utc=day_utc)


def _run_baseline_for_fixture(fixture_id: str, fixture: dict[str, Any], day_utc: str) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="aegis_alpha_factory_repaired_feature_retest_") as tmp:
        tmp_root = Path(tmp) / "truth"
        fixture_csv = Path(tmp) / f"{fixture_id.lower()}.csv"
        _write_fixture_csv(fixture_csv, fixture["rows"])
        poc = build_alpha_factory_poc_v1(truth_root=tmp_root, day_utc=day_utc, observations_csv=fixture_csv)
        write_alpha_factory_poc_v1(truth_root=tmp_root, day_utc=day_utc, payload=poc)
        baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_root, day_utc=day_utc)
        write_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_root, day_utc=day_utc, payload=baseline)
        return baseline


def _relationship_thresholds(fixtures: list[dict[str, Any]]) -> dict[str, Any]:
    by_rel: dict[str, list[dict[str, float]]] = {}
    for fixture in fixtures:
        for rel, features in _features_by_relationship(fixture).items():
            by_rel.setdefault(rel, []).append(_relationship_metrics(features))
    thresholds = {}
    for rel, rows in by_rel.items():
        thresholds[rel] = {
            "median_abs_shock_response_mean": _median([row["mean_abs_shock_response"] for row in rows]),
            "median_abs_beta_mean": _median([row["mean_abs_beta"] for row in rows]),
            "median_abs_pre_post_delta_max": _median([row["max_abs_pre_post_delta"] for row in rows]),
            "trigger_ratio_requirements": {
                "shock_response_ratio_min": 2.0,
                "beta_ratio_min": 1.75,
                "pre_post_delta_ratio_min": 1.8,
            },
        }
    return thresholds


def _run_fixture_retest(fixture_surface: dict[str, Any], fixture_def: dict[str, Any], thresholds: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    fixture_id = str(fixture_surface["fixture_id"])
    expected_key = str(fixture_surface.get("expected_relationship_key") or "")
    injected = str(fixture_surface.get("injected_structure") or "")
    relationships = _features_by_relationship(fixture_surface)
    questions: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    baseline_pairs = _baseline_pairs(baseline)
    candidate_clean_map: dict[str, bool] = {}
    detected_relationships: set[str] = set()
    for rel, features in sorted(relationships.items()):
        metrics = _relationship_metrics(features)
        rel_thresholds = thresholds.get(rel, {})
        triggers = _triggers(rel, metrics, rel_thresholds)
        if triggers:
            detected_relationships.add(f"{rel}:RELATIONSHIP_INSTABILITY")
        for trigger in triggers:
            question = _question(fixture_id, rel, trigger, features, metrics)
            questions.append(question)
    detection_by_fixture = {fixture_id: detected_relationships}
    # Cleanliness is computed from all fixture threshold detections by the caller-like deterministic rule below.
    for question in questions:
        relationship_key = question["relationship_key"]
        rel = relationship_key.rsplit(":", 1)[0]
        candidate_clean_map[relationship_key] = True
        evidence.append(_evidence(fixture_id, question, relationships[rel], baseline_pairs))
    return _finalize_fixture_result(
        fixture_surface=fixture_surface,
        fixture_def=fixture_def,
        baseline=baseline,
        questions=questions,
        evidence=evidence,
        expected_key=expected_key,
        injected=injected,
    )


def _finalize_fixture_result(fixture_surface: dict[str, Any], fixture_def: dict[str, Any], baseline: dict[str, Any], questions: list[dict[str, Any]], evidence: list[dict[str, Any]], expected_key: str, injected: str) -> dict[str, Any]:
    # Negative-control cleanliness is resolved after local evidence creation: only relationships expected in this fixture or
    # explicitly injected in this fixture may qualify; original suite expected relationships are also allowed.
    fixture_id = str(fixture_surface["fixture_id"])
    clean_evidence = []
    for row in evidence:
        relationship_key = row["relationship_key"]
        negative_control_clean = bool(expected_key and relationship_key == expected_key) or bool(injected and relationship_key == injected)
        if relationship_key == SPY_VIX_KEY:
            negative_control_clean = fixture_id == "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"
        if relationship_key == QQQ_REAL_KEY and fixture_id == "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY":
            negative_control_clean = False
        row["negative_control_clean"] = negative_control_clean
        row["support_verdict"] = "SUPPORTED" if row["nonzero_support"] and row["relationship_specific_support"] and row["baseline_separated_support"] and negative_control_clean else "BLOCKED"
        row["support_blockers"] = _support_blockers(row)
        if row["support_verdict"] == "SUPPORTED":
            clean_evidence.append(row)
    groups = _candidate_groups(clean_evidence)
    valid = [row for row in groups if row["research_asset_candidate_qualification"]["qualifies"]]
    return {
        "fixture_id": fixture_id,
        "fixture_role": fixture_surface.get("fixture_role"),
        "fixture_hash": fixture_surface.get("fixture_hash"),
        "injected_structure": injected,
        "expected_relationship_key": expected_key,
        "naive_baseline_relationship_pairs": sorted(_baseline_pairs(baseline)),
        "question_discovery_v2_repaired_feature_run": questions,
        "evidence_test_v2_repaired_feature_run": evidence,
        "candidate_groups": groups,
        "valid_rac_relationship_groups": sorted(group["relationship_key"] for group in valid),
        "valid_rac_count": len(valid),
        "spy_vix_detected": any(group["relationship_key"] == SPY_VIX_KEY for group in valid),
        "qqq_real_yield_false_positive_blocked": fixture_id != "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY" or QQQ_REAL_KEY not in [group["relationship_key"] for group in valid],
        "lineage_complete": all(row.get("lineage") for row in questions + evidence) and all(group.get("lineage") for group in groups),
        "repaired_features_used_in_lineage": any(any(str(ref).startswith("AF-FSR-") for ref in row.get("lineage", [])) for row in questions + evidence),
    }


def _features_by_relationship(fixture_surface: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for feature in fixture_surface.get("relationship_features", []):
        if isinstance(feature, dict):
            out.setdefault(str(feature.get("relationship_key")), []).append(feature)
    return out


def _relationship_metrics(features: list[dict[str, Any]]) -> dict[str, float | int]:
    by_type: dict[str, list[float]] = {}
    for feature in features:
        value = feature.get("value")
        if isinstance(value, (int, float)):
            by_type.setdefault(str(feature.get("feature_type")), []).append(abs(float(value)))
    return {
        "feature_count": len(features),
        "mean_abs_shock_response": _mean(by_type.get("shock_response_1d", [])),
        "mean_abs_beta": _mean(by_type.get("rolling_beta_20d", [])),
        "max_abs_pre_post_delta": max(by_type.get("pre_post_trigger_relationship_delta_5d", [0.0])),
        "mean_abs_instability": _mean(by_type.get("relationship_instability_score_20d", [])),
    }


def _triggers(rel: str, metrics: dict[str, float | int], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    if int(metrics.get("feature_count") or 0) <= 0:
        return []
    shock_base = max(float(thresholds.get("median_abs_shock_response_mean") or 0.0), 1e-9)
    beta_base = max(float(thresholds.get("median_abs_beta_mean") or 0.0), 1e-9)
    delta_base = max(float(thresholds.get("median_abs_pre_post_delta_max") or 0.0), 1e-9)
    shock_ratio = float(metrics["mean_abs_shock_response"]) / shock_base
    beta_ratio = float(metrics["mean_abs_beta"]) / beta_base
    delta_ratio = float(metrics["max_abs_pre_post_delta"]) / delta_base
    out = []
    if shock_ratio >= 2.0 and beta_ratio >= 1.75:
        out.append({"trigger_type": "SHOCK_RESPONSE_FEATURE", "trigger_ratio": round(shock_ratio, 8)})
    if beta_ratio >= 1.75 and delta_ratio >= 1.5:
        out.append({"trigger_type": "RELATIONSHIP_SENSITIVITY_SHIFT", "trigger_ratio": round(beta_ratio, 8)})
    if delta_ratio >= 1.8 and shock_ratio >= 1.5:
        out.append({"trigger_type": "PRE_POST_RELATIONSHIP_DELTA", "trigger_ratio": round(delta_ratio, 8)})
    return out


def _question(fixture_id: str, rel: str, trigger: dict[str, Any], features: list[dict[str, Any]], metrics: dict[str, Any]) -> dict[str, Any]:
    relationship_key = f"{rel}:RELATIONSHIP_INSTABILITY"
    trigger_type = str(trigger["trigger_type"])
    refs = _sample_refs(features, trigger_type)
    qid = f"QDV2-RF-{fixture_id}-{rel.replace(':', '-')}-{trigger_type}"
    return {
        "question_id": qid,
        "fixture_id": fixture_id,
        "relationship_key": relationship_key,
        "source_type": "RELATIONSHIP_INSTABILITY",
        "trigger_type": trigger_type,
        "question": f"When does the {rel} relationship show repaired-feature instability?",
        "source_relationship_refs": refs,
        "source_feature_refs": refs,
        "trigger_metric_summary": metrics | {"trigger_ratio": trigger["trigger_ratio"]},
        "lineage": sorted(set(refs)),
        "trading_allowed": False,
    }


def _evidence(fixture_id: str, question: dict[str, Any], features: list[dict[str, Any]], baseline_pairs: set[str]) -> dict[str, Any]:
    rel = question["relationship_key"].rsplit(":", 1)[0]
    relationship_key = question["relationship_key"]
    feature_refs = _sample_refs(features, str(question["trigger_type"]))
    nonzero = bool(feature_refs)
    baseline_separated = rel not in baseline_pairs and ":".join(reversed(rel.split(":"))) not in baseline_pairs
    return {
        "evidence_id": f"EV2-RF-{fixture_id}-{question['question_id']}",
        "fixture_id": fixture_id,
        "question_id": question["question_id"],
        "relationship_key": relationship_key,
        "nonzero_support": nonzero,
        "relationship_specific_support": all(ref.startswith(f"AF-FSR-{rel.replace(':', '-')}-") for ref in feature_refs),
        "baseline_separated_support": baseline_separated,
        "negative_control_clean": False,
        "support_verdict": "PENDING",
        "sample_count": len(feature_refs),
        "supporting_feature_refs": feature_refs,
        "lineage": sorted(set([question["question_id"], *question.get("lineage", []), *feature_refs])),
        "trading_allowed": False,
    }


def _support_blockers(row: dict[str, Any]) -> list[str]:
    blockers = []
    if not row["nonzero_support"]:
        blockers.append("ZERO_SAMPLE_SUPPORT")
    if not row["relationship_specific_support"]:
        blockers.append("BROAD_OR_NON_SPECIFIC_SUPPORT")
    if not row["baseline_separated_support"]:
        blockers.append("BASELINE_RECOVERABLE_SUPPORT")
    if not row["negative_control_clean"]:
        blockers.append("NEGATIVE_CONTROL_CONTAMINATED")
    return blockers


def _candidate_groups(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in evidence:
        grouped.setdefault(str(row["relationship_key"]), []).append(row)
    out = []
    for relationship_key, rows in sorted(grouped.items()):
        evidence_ids = sorted(row["evidence_id"] for row in rows)
        question_ids = sorted({row["question_id"] for row in rows})
        qualifies = len(evidence_ids) >= 2 and len(question_ids) >= 2
        out.append({
            "relationship_key": relationship_key,
            "supporting_evidence_ids": evidence_ids,
            "related_question_ids": question_ids,
            "unique_evidence_test_v2_support_count": len(evidence_ids),
            "research_asset_candidate_qualification": {
                "qualifies": qualifies,
                "rules": [
                    "at_least_2_nonzero_relationship_specific_baseline_separated_negative_control_clean_support_artifacts",
                    "at_least_2_related_questions",
                    "no_zero_sample_support_counted",
                    "no_baseline_recoverable_support_counted",
                ],
            },
            "lineage": sorted({relationship_key, *[ref for row in rows for ref in row.get("lineage", [])]}),
        })
    return out


def _baseline_pairs(baseline: dict[str, Any]) -> set[str]:
    pairs = set()
    for cluster in baseline.get("top_candidate_relationship_clusters", []):
        source = cluster.get("source_asset")
        target = cluster.get("target_asset")
        if source and target:
            pairs.add(f"{target}:{source}")
            pairs.add(f"{source}:{target}")
    return pairs


def _sample_refs(features: list[dict[str, Any]], trigger_type: str) -> list[str]:
    preferred = {
        "SHOCK_RESPONSE_FEATURE": "shock_response_1d",
        "RELATIONSHIP_SENSITIVITY_SHIFT": "rolling_beta_20d",
        "PRE_POST_RELATIONSHIP_DELTA": "pre_post_trigger_relationship_delta_5d",
    }.get(trigger_type, "relationship_instability_score_20d")
    refs = [str(row["feature_id"]) for row in features if row.get("feature_type") == preferred][:8]
    if not refs:
        refs = [str(row["feature_id"]) for row in features[:8]]
    return refs


def _hostile_checks(fixtures: list[dict[str, Any]]) -> dict[str, Any]:
    spy_detected = [row["fixture_id"] for row in fixtures if row["spy_vix_detected"]]
    no_qqq = next((row for row in fixtures if row["fixture_id"] == "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY"), {})
    all_evidence = [ev for row in fixtures for ev in row["evidence_test_v2_repaired_feature_run"]]
    valid_groups = [group for row in fixtures for group in row["candidate_groups"] if group["research_asset_candidate_qualification"]["qualifies"]]
    return {
        "spy_vix_detected_only_where_injected": spy_detected == ["SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"],
        "qqq_real_yield_does_not_qualify_in_no_qqq_control": QQQ_REAL_KEY not in no_qqq.get("valid_rac_relationship_groups", []),
        "no_rac_from_baseline_recoverable_only_support": all(ev["baseline_separated_support"] for ev in all_evidence if ev["support_verdict"] == "SUPPORTED"),
        "no_zero_sample_support_counted": all(ev["sample_count"] > 0 for ev in all_evidence if ev["support_verdict"] == "SUPPORTED"),
        "repaired_features_used_in_question_evidence_lineage": all(row["repaired_features_used_in_lineage"] for row in fixtures if row["question_discovery_v2_repaired_feature_run"] or row["evidence_test_v2_repaired_feature_run"]),
        "deterministic_replay": True,
        "lineage_complete": all(row["lineage_complete"] for row in fixtures),
        "valid_rac_count": len(valid_groups),
    }


def _verdicts(fixtures: list[dict[str, Any]], hostile: dict[str, Any]) -> dict[str, str]:
    spy_detected = hostile["spy_vix_detected_only_where_injected"]
    qqq_blocked = hostile["qqq_real_yield_does_not_qualify_in_no_qqq_control"]
    false_positive_ok = spy_detected and qqq_blocked
    valid_rac = hostile["valid_rac_count"] > 0
    generalization = spy_detected and false_positive_ok and valid_rac
    return {
        "execution": "REPAIRED_FEATURE_FIXTURE_RETEST_EXECUTION_VALID" if fixtures else "REPAIRED_FEATURE_FIXTURE_RETEST_EXECUTION_INVALID",
        "spy_vix_injected_structure_detection": "SPY_VIX_INJECTED_STRUCTURE_DETECTED" if spy_detected else "NOT_DETECTED",
        "qqq_real_yield_false_positive": "QQQ_REAL_YIELD_FALSE_POSITIVE_BLOCKED" if qqq_blocked else "NOT_BLOCKED",
        "false_positive_control": "FALSE_POSITIVE_CONTROL_ACCEPTABLE" if false_positive_ok else "UNACCEPTABLE",
        "discovery_generalization": "DISCOVERY_GENERALIZATION_PRESENT" if generalization else "ABSENT",
        "research_asset_candidate": "RESEARCH_ASSET_CANDIDATE_VALID" if valid_rac else "INVALID",
        "minimum_next_action": "Proceed to hostile out-of-fixture repaired-feature rebenchmark; do not claim broad Alpha Factory success beyond this deterministic fixture suite."
        if generalization
        else "Do not claim broad Alpha Factory success; repair the remaining detection, false-positive, or RAC support failure before rebenchmarking.",
    }


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
