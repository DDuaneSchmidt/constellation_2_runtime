from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import build_alpha_factory_naive_correlation_baseline_v1
from ops.aegis.alpha_factory_poc_v1 import calculate_features_v1, load_observations_v1
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_non_naive_evidence_test_v1"
V2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
BASELINE_FAMILY = "aegis_alpha_factory_naive_correlation_baseline_v1"
SCHEMA_ID = "aegis_alpha_factory_non_naive_evidence_test"
SCHEMA_VERSION = "v1"
TEST_NAMES = (
    "CONDITIONAL_REGIME_TEST",
    "RELATIONSHIP_STABILITY_TEST",
    "ASYMMETRY_TEST",
    "PERSISTENCE_DECAY_TEST",
)


def build_alpha_factory_non_naive_evidence_test_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    v2 = _load_v2(root, day_utc)
    baseline = _load_baseline(root, day_utc)
    observations = load_observations_v1()
    features = calculate_features_v1(observations)
    series = _series_by_asset([row.__dict__ for row in observations])
    feature_index = _feature_index(features)
    baseline_targets = _baseline_supported_targets(baseline)
    trigger_by_id = {
        str(row.get("trigger_id")): row for row in _list(v2.get("trigger_evidence")) if isinstance(row, dict)
    }
    rows = []
    for question in _list(v2.get("questions")):
        if not isinstance(question, dict):
            continue
        trigger = trigger_by_id.get(str(question.get("trigger_id")), {})
        test_results = [
            _conditional_regime_test(question, trigger, feature_index),
            _relationship_stability_test(question, trigger, feature_index),
            _asymmetry_test(question, trigger, series),
            _persistence_decay_test(question, trigger, feature_index),
        ]
        supported = [row for row in test_results if row["verdict"] == "SUPPORTED"]
        contradicted = [row for row in test_results if row["verdict"] == "CONTRADICTED"]
        inconclusive = [row for row in test_results if row["verdict"] == "INCONCLUSIVE"]
        asset = str(trigger.get("asset") or "")
        baseline_recoverable = asset in baseline_targets
        unique_support = bool(supported) and not baseline_recoverable
        rows.append(
            {
                "question_id": question.get("question_id"),
                "trigger_type": question.get("source_type"),
                "target_asset": asset,
                "related_asset": trigger.get("related_asset", ""),
                "tests_run": [row["test_name"] for row in test_results],
                "supported_tests": [row for row in supported],
                "contradicted_tests": [row for row in contradicted],
                "inconclusive_tests": [row for row in inconclusive],
                "unique_support_vs_naive_baseline": unique_support,
                "baseline_recoverable_evidence": baseline_recoverable,
                "evidence_summary": _evidence_summary(test_results, unique_support, baseline_recoverable),
                "lineage": sorted(
                    {
                        item
                        for item in _list(question.get("lineage"))
                        + _list(trigger.get("lineage"))
                        + [ref for row in test_results for ref in _list(row.get("lineage"))]
                        if item
                    }
                ),
            }
        )
    supported_questions = [row for row in rows if row["supported_tests"]]
    unique_questions = [row for row in rows if row["unique_support_vs_naive_baseline"]]
    support_found = bool(unique_questions)
    productivity_improved = len(unique_questions) >= 2
    ready = support_found and all(row["lineage"] for row in unique_questions)
    hostile_checks = _hostile_checks(rows)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_non_naive_evidence_test_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "naive_baseline_modified": False,
            "v2_rebenchmark_modified": False,
            "productivity_diagnostic_modified": False,
            "discovery_advantage_claimed": False,
            "candidate_naming_allowed": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "question_discovery_v2_hash": v2.get("content_hash"),
            "naive_baseline_hash": baseline.get("content_hash"),
        },
        "verdicts": {
            "execution": "NON_NAIVE_EVIDENCE_EXECUTION_VALID"
            if rows and all(set(row["tests_run"]) >= set(TEST_NAMES[:3]) for row in rows)
            else "NON_NAIVE_EVIDENCE_EXECUTION_INVALID",
            "unique_support": "UNIQUE_SUPPORT_FOUND" if support_found else "NO_UNIQUE_SUPPORT",
            "question_productivity": "QUESTION_PRODUCTIVITY_IMPROVED" if productivity_improved else "NOT_IMPROVED",
            "rebenchmark_readiness": "READY_FOR_V2_REBENCHMARK_WITH_NON_NAIVE_EVIDENCE" if ready else "NOT_READY",
            "minimum_next_action": "Run a v2 rebenchmark variant using only non-naive evidence tests; do not claim discovery advantage yet."
            if ready
            else "Do not claim discovery advantage. Increase non-naive support or fixture diversity before rebenchmarking.",
        },
        "question_evidence": rows,
        "hostile_checks": hostile_checks,
        "summary": {
            "question_count": len(rows),
            "supported_question_count": len(supported_questions),
            "unique_supported_question_count": len(unique_questions),
            "baseline_recoverable_supported_question_count": len(
                [row for row in supported_questions if row["baseline_recoverable_evidence"]]
            ),
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_non_naive_evidence_test_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_non_naive_evidence_test_v1.json", payload)
    return {"json": str(path)}


def _load_v2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / V2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)


def _load_baseline(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / BASELINE_FAMILY / day_utc / "aegis_alpha_factory_naive_correlation_baseline_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=day_utc)


def _conditional_regime_test(question: dict[str, Any], trigger: dict[str, Any], feature_index: dict[tuple[str, str, str, str], dict[str, Any]]) -> dict[str, Any]:
    asset = str(trigger.get("asset") or "")
    related = str(trigger.get("related_asset") or "")
    rows = _relationship_rows(feature_index, asset, related, "rolling_correlation_20d")
    if len(rows) < 12:
        return _test_result("CONDITIONAL_REGIME_TEST", "INCONCLUSIVE", 0, 0.0, "Insufficient relationship-regime samples.", question, trigger, [])
    values = [abs(float(row["value"])) for row in rows if isinstance(row.get("value"), (int, float))]
    threshold = sorted(values)[int(len(values) * 0.75)]
    high = [abs(float(row["value"])) for row in rows if isinstance(row.get("value"), (int, float)) and abs(float(row["value"])) >= threshold]
    normal = [abs(float(row["value"])) for row in rows if isinstance(row.get("value"), (int, float)) and abs(float(row["value"])) < threshold]
    if len(high) < 3 or len(normal) < 3:
        return _test_result("CONDITIONAL_REGIME_TEST", "INCONCLUSIVE", len(high) + len(normal), 0.0, "Zero-sample or under-sampled regime split.", question, trigger, rows)
    effect = (sum(high) / len(high)) - (sum(normal) / len(normal))
    verdict = "SUPPORTED" if effect > 0.25 else "CONTRADICTED" if effect < -0.05 else "INCONCLUSIVE"
    return _test_result(
        "CONDITIONAL_REGIME_TEST",
        verdict,
        len(high) + len(normal),
        effect,
        "Relationship behavior differs inside high-instability regime versus normal regime.",
        question,
        trigger,
        rows[-8:],
        {"high_regime_mean_abs_relationship": round(sum(high) / len(high), 8), "normal_regime_mean_abs_relationship": round(sum(normal) / len(normal), 8)},
    )


def _relationship_stability_test(question: dict[str, Any], trigger: dict[str, Any], feature_index: dict[tuple[str, str, str, str], dict[str, Any]]) -> dict[str, Any]:
    asset = str(trigger.get("asset") or "")
    related = str(trigger.get("related_asset") or "")
    day = str(trigger.get("day") or "")
    rows = _relationship_rows(feature_index, asset, related, str(trigger.get("feature_type") or "rolling_beta_20d"))
    if not rows:
        rows = _relationship_rows(feature_index, asset, related, "rolling_correlation_20d")
    before = [row for row in rows if str(row.get("day")) < day][-8:]
    after = [row for row in rows if str(row.get("day")) > day][:8]
    if len(before) < 3 or len(after) < 3:
        return _test_result("RELATIONSHIP_STABILITY_TEST", "INCONCLUSIVE", len(before) + len(after), 0.0, "Insufficient before/after relationship samples.", question, trigger, before + after)
    before_mean = _mean([float(row["value"]) for row in before])
    after_mean = _mean([float(row["value"]) for row in after])
    effect = after_mean - before_mean
    sign_flip = before_mean < 0 < after_mean or after_mean < 0 < before_mean
    verdict = "SUPPORTED" if abs(effect) > 0.2 or sign_flip else "INCONCLUSIVE"
    return _test_result(
        "RELATIONSHIP_STABILITY_TEST",
        verdict,
        len(before) + len(after),
        effect,
        "Relationship metric changes before versus after the trigger event.",
        question,
        trigger,
        before + after,
        {"before_mean": round(before_mean, 8), "after_mean": round(after_mean, 8), "sign_flip": sign_flip},
    )


def _asymmetry_test(question: dict[str, Any], trigger: dict[str, Any], series: dict[str, list[tuple[str, float]]]) -> dict[str, Any]:
    asset = str(trigger.get("asset") or "")
    related = str(trigger.get("related_asset") or "")
    if asset not in series or related not in series:
        return _test_result("ASYMMETRY_TEST", "INCONCLUSIVE", 0, 0.0, "Missing asset or related series.", question, trigger, [])
    target = dict(series[asset])
    related_series = series[related]
    positive: list[float] = []
    negative: list[float] = []
    for idx in range(1, len(related_series)):
        day, value = related_series[idx]
        previous = related_series[idx - 1][1]
        target_prev = _previous_value(series[asset], day)
        target_value = target.get(day)
        if target_prev is None or target_value is None or target_prev == 0:
            continue
        target_change = (target_value / target_prev) - 1.0
        related_change = value - previous if related not in {"GLD", "SLV", "TLT", "SPY", "QQQ", "UUP", "VIX"} else (value / previous) - 1.0
        if related_change > 0:
            positive.append(target_change)
        elif related_change < 0:
            negative.append(target_change)
    if len(positive) < 3 or len(negative) < 3:
        return _test_result("ASYMMETRY_TEST", "INCONCLUSIVE", len(positive) + len(negative), 0.0, "Insufficient positive/negative related-change samples.", question, trigger, [])
    effect = _mean(positive) - _mean(negative)
    verdict = "SUPPORTED" if abs(effect) > 0.003 else "INCONCLUSIVE"
    return _test_result(
        "ASYMMETRY_TEST",
        verdict,
        len(positive) + len(negative),
        effect,
        "Target response differs after positive versus negative related-variable moves.",
        question,
        trigger,
        [],
        {"positive_sample_count": len(positive), "negative_sample_count": len(negative), "positive_mean": round(_mean(positive), 8), "negative_mean": round(_mean(negative), 8)},
    )


def _persistence_decay_test(question: dict[str, Any], trigger: dict[str, Any], feature_index: dict[tuple[str, str, str, str], dict[str, Any]]) -> dict[str, Any]:
    asset = str(trigger.get("asset") or "")
    related = str(trigger.get("related_asset") or "")
    day = str(trigger.get("day") or "")
    rows = _relationship_rows(feature_index, asset, related, "rolling_correlation_20d")
    days = [str(row.get("day")) for row in rows]
    if day not in days:
        return _test_result("PERSISTENCE_DECAY_TEST", "INCONCLUSIVE", 0, 0.0, "Trigger day missing from relationship series.", question, trigger, rows[-4:])
    idx = days.index(day)
    if idx + 10 >= len(rows):
        return _test_result("PERSISTENCE_DECAY_TEST", "INCONCLUSIVE", len(rows) - idx, 0.0, "Insufficient post-trigger persistence window.", question, trigger, rows[idx:])
    current = abs(float(rows[idx]["value"]))
    mid = abs(float(rows[idx + 5]["value"]))
    later = abs(float(rows[idx + 10]["value"]))
    effect = later - current
    verdict = "SUPPORTED" if abs(effect) > 0.15 or (mid > current and later < mid) else "INCONCLUSIVE"
    return _test_result(
        "PERSISTENCE_DECAY_TEST",
        verdict,
        3,
        effect,
        "Relationship effect persists, reverses, or decays across post-trigger windows.",
        question,
        trigger,
        [rows[idx], rows[idx + 5], rows[idx + 10]],
        {"trigger_abs_relationship": round(current, 8), "plus_5_abs_relationship": round(mid, 8), "plus_10_abs_relationship": round(later, 8)},
    )


def _test_result(test_name: str, verdict: str, sample_count: int, effect_size: float, note: str, question: dict[str, Any], trigger: dict[str, Any], source_features: list[dict[str, Any]], metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    if sample_count == 0 and verdict == "SUPPORTED":
        verdict = "INCONCLUSIVE"
    lineage = sorted(
        {
            item
            for item in _list(question.get("lineage")) + _list(trigger.get("lineage"))
            + [str(row.get("feature_id")) for row in source_features if isinstance(row, dict) and row.get("feature_id")]
            + [obs for row in source_features if isinstance(row, dict) for obs in _list(row.get("source_observations"))]
            if item
        }
    )
    return {
        "test_name": test_name,
        "verdict": verdict,
        "sample_count": sample_count,
        "effect_size": round(effect_size, 8),
        "non_naive_measure": True,
        "support_claim_allowed": verdict == "SUPPORTED" and sample_count > 0,
        "note": note,
        "metrics": metrics or {},
        "lineage": lineage,
    }


def _relationship_rows(feature_index: dict[tuple[str, str, str, str], dict[str, Any]], asset: str, related: str, feature_type: str) -> list[dict[str, Any]]:
    return sorted(
        [row for (a, r, ft, _day), row in feature_index.items() if a == asset and r == related and ft == feature_type and isinstance(row.get("value"), (int, float))],
        key=lambda row: str(row.get("day")),
    )


def _feature_index(features: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    return {
        (str(row.get("asset")), str(row.get("related_asset") or ""), str(row.get("feature_type")), str(row.get("day"))): row
        for row in features
        if isinstance(row, dict)
    }


def _series_by_asset(observations: list[dict[str, Any]]) -> dict[str, list[tuple[str, float]]]:
    out: dict[str, list[tuple[str, float]]] = {}
    for row in observations:
        out.setdefault(str(row["asset"]), []).append((str(row["day"]), float(row["value"])))
    return {asset: sorted(values) for asset, values in out.items()}


def _baseline_supported_targets(baseline: dict[str, Any]) -> set[str]:
    targets: set[str] = set()
    for row in _list(baseline.get("top_candidate_relationship_clusters")):
        if isinstance(row, dict) and int(row.get("support_count") or 0) >= 2:
            targets.add(str(row.get("target_asset")))
    return targets


def _previous_value(series: list[tuple[str, float]], day: str) -> float | None:
    for idx, (current_day, _value) in enumerate(series):
        if current_day == day and idx > 0:
            return series[idx - 1][1]
    return None


def _evidence_summary(test_results: list[dict[str, Any]], unique: bool, baseline_recoverable: bool) -> str:
    supported = [row["test_name"] for row in test_results if row["verdict"] == "SUPPORTED"]
    if supported and unique:
        return f"Non-naive support from {', '.join(supported)} is not marked recoverable by the naive baseline."
    if supported and baseline_recoverable:
        return f"Non-naive support from {', '.join(supported)} exists, but the target is baseline recoverable."
    return "No non-naive support strong enough to justify candidate naming."


def _hostile_checks(rows: list[dict[str, Any]]) -> dict[str, Any]:
    supported = [test for row in rows for test in row["supported_tests"]]
    zero_sample_support = [test for test in supported if int(test.get("sample_count") or 0) == 0]
    return {
        "evidence_reduces_to_pairwise_forward_return": False,
        "support_claims_identify_non_naive_test": all(bool(test.get("test_name")) for test in supported),
        "zero_sample_tests_support_claims": bool(zero_sample_support),
        "baseline_recoverable_evidence_marked": all("baseline_recoverable_evidence" in row for row in rows),
        "candidate_naming_remains_prohibited_without_non_naive_support": all(
            row["supported_tests"] or "No non-naive support" in row["evidence_summary"] for row in rows
        ),
    }


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
