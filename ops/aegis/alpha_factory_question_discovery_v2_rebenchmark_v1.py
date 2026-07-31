from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import (
    build_alpha_factory_naive_correlation_baseline_v1,
    write_alpha_factory_naive_correlation_baseline_v1,
)
from ops.aegis.alpha_factory_poc_v1 import HORIZONS, RETURN_ASSETS, load_observations_v1
from ops.aegis.alpha_factory_question_discovery_v2 import (
    FORBIDDEN_ANSWER_LABELS,
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_question_discovery_v2_rebenchmark_v1"
V2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
BASELINE_FAMILY = "aegis_alpha_factory_naive_correlation_baseline_v1"
OUT_OF_FIXTURE_FAMILY = "aegis_alpha_factory_out_of_fixture_benchmark_v1"
SCHEMA_ID = "aegis_alpha_factory_question_discovery_v2_rebenchmark"
SCHEMA_VERSION = "v1"


def build_alpha_factory_question_discovery_v2_rebenchmark_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    v2 = _load_or_build_v2(root, day_utc)
    baseline = _load_or_build_baseline(root, day_utc)
    out_of_fixture = _load_out_of_fixture(root, day_utc)
    observations = load_observations_v1()
    series = _series_by_asset([row.__dict__ for row in observations])
    obs_id = {(row.asset, row.day): row.observation_id for row in observations}

    hypotheses = _hypotheses_from_v2(v2)
    evidence = _evidence_from_hypotheses(hypotheses=hypotheses, series=series, obs_id=obs_id)
    candidate_groups = _candidate_groups(evidence=evidence, hypotheses=hypotheses)
    baseline_comparison = _compare_to_baseline(candidate_groups, baseline)
    hostile_checks = _hostile_checks(v2=v2, candidate_groups=candidate_groups, baseline_comparison=baseline_comparison)
    lineage_complete = _lineage_complete(hypotheses, evidence, candidate_groups)
    productivity = _question_productivity(v2, hypotheses, evidence)
    candidate_naming = _candidate_naming_verdict(candidate_groups, hostile_checks)

    v2_outperforms = (
        bool(candidate_groups)
        and not baseline_comparison["baseline_recovers_any_candidate"]
        and productivity["supported_question_count"] >= 2
        and candidate_naming == "CANDIDATE_NAMING_VALID"
    )
    if v2_outperforms:
        poc_vs_baseline = "V2_OUTPERFORMS_BASELINE"
        discovery_advantage = "DISCOVERY_ADVANTAGE_PRESENT"
        minimum_next_action = "Run the same v2 rebenchmark out of fixture before making any Alpha Factory success claim."
    elif baseline_comparison["baseline_recovers_any_candidate"] or not candidate_groups:
        poc_vs_baseline = "BASELINE_MATCHES_OR_EXCEEDS_V2"
        discovery_advantage = "DISCOVERY_ADVANTAGE_ABSENT"
        minimum_next_action = (
            "Do not claim Alpha Factory success. Re-run v2 on the out-of-fixture dataset and require candidate support "
            "that is not recoverable by the naive-correlation baseline."
        )
    else:
        poc_vs_baseline = "INCONCLUSIVE"
        discovery_advantage = "DISCOVERY_ADVANTAGE_INCONCLUSIVE"
        minimum_next_action = "Add a larger deterministic fixture suite before making a discovery-advantage claim."

    execution_valid = bool(v2.get("questions")) and bool(hypotheses) and bool(evidence) and lineage_complete
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_question_discovery_v2_rebenchmark_against_naive_baseline",
        "constraints": {
            "question_discovery_v2_modified": False,
            "naive_baseline_modified": False,
            "evidence_standards_modified": False,
            "review_verdict_definitions_modified": False,
            "alpha_factory_success_claimed": v2_outperforms and discovery_advantage == "DISCOVERY_ADVANTAGE_PRESENT",
            "trading_allowed": False,
        },
        "source_artifacts": {
            "question_discovery_v2_hash": v2.get("content_hash"),
            "naive_baseline_hash": baseline.get("content_hash"),
            "out_of_fixture_hash": out_of_fixture.get("content_hash") if out_of_fixture else "",
        },
        "verdicts": {
            "execution": "V2_REBENCHMARK_EXECUTION_VALID" if execution_valid else "V2_REBENCHMARK_EXECUTION_INVALID",
            "v2_vs_baseline": poc_vs_baseline,
            "discovery_advantage": discovery_advantage,
            "candidate_naming": candidate_naming,
            "lineage": "LINEAGE_COMPLETE" if lineage_complete else "LINEAGE_INCOMPLETE",
            "minimum_next_action": minimum_next_action,
        },
        "question_productivity": productivity,
        "hypotheses": hypotheses,
        "evidence": evidence,
        "candidate_groups": candidate_groups,
        "baseline_comparison": baseline_comparison,
        "out_of_fixture_context": _out_of_fixture_context(out_of_fixture),
        "hostile_checks": hostile_checks,
        "summary": {
            "question_count": len(v2.get("questions") or []),
            "hypothesis_count": len(hypotheses),
            "evidence_count": len(evidence),
            "candidate_group_count": len(candidate_groups),
            "output_reproducible": True,
            "lineage_complete": lineage_complete,
            "discovery_advantage_claimed": v2_outperforms and discovery_advantage == "DISCOVERY_ADVANTAGE_PRESENT",
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_question_discovery_v2_rebenchmark_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_question_discovery_v2_rebenchmark_v1.json", payload)
    return {"json": str(path)}


def _load_or_build_v2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / V2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    if payload:
        return payload
    payload = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc, payload=payload)
    return payload


def _load_or_build_baseline(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / BASELINE_FAMILY / day_utc / "aegis_alpha_factory_naive_correlation_baseline_v1.json"
    payload = read_json_v1(path)
    if payload:
        return payload
    payload = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=day_utc)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=day_utc, payload=payload)
    return payload


def _load_out_of_fixture(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / OUT_OF_FIXTURE_FAMILY / day_utc / "aegis_alpha_factory_out_of_fixture_benchmark_v1.json"
    payload = read_json_v1(path)
    return payload if isinstance(payload, dict) else {}


def _hypotheses_from_v2(v2: dict[str, Any]) -> list[dict[str, Any]]:
    triggers = {row.get("trigger_id"): row for row in _list(v2.get("trigger_evidence")) if isinstance(row, dict)}
    hypotheses: list[dict[str, Any]] = []
    for question in _list(v2.get("questions")):
        if not isinstance(question, dict):
            continue
        trigger = triggers.get(question.get("trigger_id"), {})
        asset = str(trigger.get("asset") or _extract_asset_from_question(question) or "")
        related = str(trigger.get("related_asset") or "")
        if asset not in RETURN_ASSETS:
            continue
        source_type = str(question.get("source_type") or trigger.get("trigger_type") or "")
        for horizon in HORIZONS:
            hypothesis_id = f"H-V2-{question['question_id']}-{horizon:02d}"
            hypotheses.append(
                {
                    "hypothesis_id": hypothesis_id,
                    "question_id": question["question_id"],
                    "trigger_id": question.get("trigger_id"),
                    "hypothesis_family": f"{source_type}_{asset}_{related or 'UNRELATED'}_forward_return",
                    "statement": (
                        f"If the data-derived {source_type} trigger for {asset}"
                        f"{' versus ' + related if related else ''} occurs, does {asset} show abnormal forward return "
                        f"over {horizon} trading day horizon?"
                    ),
                    "trigger_type": source_type,
                    "target_asset": asset,
                    "related_asset": related,
                    "horizon_days": horizon,
                    "lineage": sorted(set(_list(question.get("lineage")) + [str(question["question_id"])])),
                    "trading_allowed": False,
                }
            )
    return hypotheses


def _evidence_from_hypotheses(
    *,
    hypotheses: list[dict[str, Any]],
    series: dict[str, list[tuple[str, float]]],
    obs_id: dict[tuple[str, str], str],
) -> list[dict[str, Any]]:
    grouped_days: dict[tuple[str, str, str], list[str]] = {}
    for hypothesis in hypotheses:
        key = (str(hypothesis["trigger_type"]), str(hypothesis["target_asset"]), str(hypothesis.get("related_asset") or ""))
        day_refs = [item for item in hypothesis.get("lineage", []) if isinstance(item, str) and item.startswith("QDV2-TRIG-")]
        for ref in day_refs:
            day = _day_from_trigger_id(ref)
            if day:
                grouped_days.setdefault(key, []).append(day)

    evidence: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        target = str(hypothesis["target_asset"])
        horizon = int(hypothesis["horizon_days"])
        key = (str(hypothesis["trigger_type"]), target, str(hypothesis.get("related_asset") or ""))
        event_days = sorted(set(grouped_days.get(key, [])))
        target_series = series[target]
        index_by_day = {day: idx for idx, (day, _value) in enumerate(target_series)}
        returns: list[float] = []
        lineage = list(hypothesis["lineage"])
        used_days: list[str] = []
        for day in event_days:
            idx = index_by_day.get(day)
            if idx is None or idx + horizon >= len(target_series):
                continue
            start = target_series[idx][1]
            end = target_series[idx + horizon][1]
            if not start:
                continue
            returns.append((end / start) - 1.0)
            used_days.append(day)
            lineage.append(obs_id.get((target, day), ""))
            lineage.append(obs_id.get((target, target_series[idx + horizon][0]), ""))
        baseline_values = [
            (target_series[idx + horizon][1] / target_series[idx][1]) - 1.0
            for idx in range(0, len(target_series) - horizon)
            if target_series[idx][1]
        ]
        shock_mean = sum(returns) / len(returns) if returns else 0.0
        baseline_mean = sum(baseline_values) / len(baseline_values) if baseline_values else 0.0
        effect = shock_mean - baseline_mean
        direction = "POSITIVE" if effect > 0.0005 else "NEGATIVE" if effect < -0.0005 else "FLAT"
        consistency = _direction_consistency(returns)
        evidence_id = hypothesis["hypothesis_id"].replace("H-", "E-")
        evidence.append(
            {
                "evidence_id": evidence_id,
                "hypothesis_id": hypothesis["hypothesis_id"],
                "question_id": hypothesis["question_id"],
                "test_window": {
                    "start_day": target_series[0][0],
                    "end_day": target_series[-1][0],
                    "horizon_days": horizon,
                },
                "sample_count": len(returns),
                "effect_direction": direction,
                "effect_size": round(effect, 8),
                "direction_consistency": round(consistency, 8),
                "baseline_comparison": {
                    "trigger_forward_return_mean": round(shock_mean, 8),
                    "all_days_forward_return_mean": round(baseline_mean, 8),
                },
                "event_days": used_days,
                "contradiction_notes": "Under-sampled, zero-sample, or flat effect; blocks candidate naming."
                if len(returns) < 3 or direction == "FLAT"
                else "",
                "lineage": sorted({row for row in lineage if row}),
                "trading_allowed": False,
            }
        )
    return evidence


def _candidate_groups(evidence: list[dict[str, Any]], hypotheses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hyp_by_id = {row["hypothesis_id"]: row for row in hypotheses}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in evidence:
        hyp = hyp_by_id.get(row["hypothesis_id"], {})
        grouped.setdefault(str(hyp.get("hypothesis_family") or ""), []).append(row)
    candidates: list[dict[str, Any]] = []
    for family, rows in sorted(grouped.items()):
        supporting = [
            row
            for row in rows
            if int(row.get("sample_count") or 0) >= 3
            and abs(float(row.get("effect_size") or 0.0)) > 0.0005
            and str(row.get("effect_direction")) in {"POSITIVE", "NEGATIVE"}
        ]
        if len(supporting) < 3:
            continue
        hyp_ids = sorted({str(row["hypothesis_id"]) for row in supporting})
        if len(hyp_ids) < 2:
            continue
        first_hyp = hyp_by_id[hyp_ids[0]]
        target = str(first_hyp.get("target_asset") or "")
        trigger_type = str(first_hyp.get("trigger_type") or "").replace("_", " ").title()
        proposed_name = f"{target} {trigger_type} Forward Return Response"
        zero_sample = any(int(row.get("sample_count") or 0) == 0 for row in rows)
        candidates.append(
            {
                "candidate_group_id": f"V2-CG-{len(candidates) + 1:03d}",
                "proposed_name": proposed_name,
                "hypothesis_family": family,
                "supporting_evidence": [row["evidence_id"] for row in supporting],
                "limiting_evidence": [row["evidence_id"] for row in rows if row not in supporting],
                "supporting_sample_count": sum(int(row.get("sample_count") or 0) for row in supporting),
                "max_abs_effect_size": round(max(abs(float(row.get("effect_size") or 0.0)) for row in supporting), 8),
                "zero_sample_support_present": zero_sample,
                "naming_blocked_by_zero_sample": zero_sample,
                "lineage": sorted({item for row in rows for item in row.get("lineage", [])} | set(hyp_ids)),
                "trading_allowed": False,
            }
        )
    return candidates


def _compare_to_baseline(candidate_groups: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    clusters = [row for row in _list(baseline.get("top_candidate_relationship_clusters")) if isinstance(row, dict)]
    relationships = [row for row in _list(baseline.get("top_relationships")) if isinstance(row, dict)]
    matched: list[dict[str, Any]] = []
    for candidate in candidate_groups:
        tokens = str(candidate.get("proposed_name") or "").split()
        target = tokens[0] if tokens else ""
        candidate_match = [
            row
            for row in clusters
            if row.get("target_asset") == target and int(row.get("support_count") or 0) >= 2
        ]
        if candidate_match:
            matched.append(
                {
                    "candidate_group_id": candidate["candidate_group_id"],
                    "candidate_name": candidate["proposed_name"],
                    "matched_naive_clusters": candidate_match[:5],
                }
            )
    return {
        "baseline_verdicts": baseline.get("verdicts", {}),
        "top_baseline_cluster": clusters[0] if clusters else {},
        "top_baseline_relationship": relationships[0] if relationships else {},
        "matched_candidate_groups": matched,
        "baseline_recovers_any_candidate": bool(matched),
        "relationship_count": baseline.get("summary", {}).get("relationship_count", len(relationships)),
        "cluster_count": baseline.get("summary", {}).get("cluster_count", len(clusters)),
    }


def _hostile_checks(
    *,
    v2: dict[str, Any],
    candidate_groups: list[dict[str, Any]],
    baseline_comparison: dict[str, Any],
) -> dict[str, Any]:
    question_text = "\n".join(str(row.get("question") or "") for row in _list(v2.get("questions")) if isinstance(row, dict))
    forbidden_hits = [
        label for label in FORBIDDEN_ANSWER_LABELS if label.lower() in question_text.lower()
    ]
    candidate_label_support = all(
        row.get("supporting_evidence") and int(row.get("supporting_sample_count") or 0) > 0 for row in candidate_groups
    )
    zero_sample_blocks = all(not row.get("zero_sample_support_present") for row in candidate_groups)
    return {
        "fixed_template_leakage_reintroduced": bool(forbidden_hits),
        "pre_labeled_research_asset_names_before_grouping": bool(forbidden_hits),
        "forbidden_label_hits": forbidden_hits,
        "evidence_supports_candidate_labels": candidate_label_support,
        "baseline_recoverability_measured": "baseline_recovers_any_candidate" in baseline_comparison,
        "baseline_recovers_any_candidate": bool(baseline_comparison.get("baseline_recovers_any_candidate")),
        "zero_sample_support_blocks_candidate_naming": zero_sample_blocks,
    }


def _candidate_naming_verdict(candidate_groups: list[dict[str, Any]], hostile_checks: dict[str, Any]) -> str:
    if hostile_checks["fixed_template_leakage_reintroduced"]:
        return "OVERNAMED"
    if not candidate_groups:
        return "INCONCLUSIVE"
    if hostile_checks["evidence_supports_candidate_labels"] and hostile_checks["zero_sample_support_blocks_candidate_naming"]:
        return "CANDIDATE_NAMING_VALID"
    return "OVERNAMED"


def _question_productivity(v2: dict[str, Any], hypotheses: list[dict[str, Any]], evidence: list[dict[str, Any]]) -> dict[str, Any]:
    supported_questions = sorted(
        {
            str(row.get("question_id"))
            for row in evidence
            if int(row.get("sample_count") or 0) >= 3 and abs(float(row.get("effect_size") or 0.0)) > 0.0005
        }
    )
    return {
        "question_count": len(_list(v2.get("questions"))),
        "hypothesis_count": len(hypotheses),
        "evidence_count": len(evidence),
        "supported_question_count": len(supported_questions),
        "supported_question_ids": supported_questions,
        "zero_sample_evidence_count": len([row for row in evidence if int(row.get("sample_count") or 0) == 0]),
        "under_sampled_evidence_count": len([row for row in evidence if int(row.get("sample_count") or 0) < 3]),
    }


def _out_of_fixture_context(out_of_fixture: dict[str, Any]) -> dict[str, Any]:
    if not out_of_fixture:
        return {"available": False}
    verdicts = out_of_fixture.get("verdicts", {})
    hostile = out_of_fixture.get("hostile_checks", {})
    return {
        "available": True,
        "verdicts": verdicts,
        "prior_poc_generalization": verdicts.get("poc_generalization"),
        "prior_baseline_recoverability": hostile.get("naive_baseline_recoverability"),
        "prior_candidate_naming": verdicts.get("candidate_naming"),
    }


def _lineage_complete(
    hypotheses: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> bool:
    if not hypotheses or not evidence:
        return False
    if any(not row.get("lineage") for row in hypotheses):
        return False
    if any(not row.get("lineage") for row in evidence):
        return False
    if candidates and any(not row.get("lineage") for row in candidates):
        return False
    return True


def _series_by_asset(observations: list[dict[str, Any]]) -> dict[str, list[tuple[str, float]]]:
    out: dict[str, list[tuple[str, float]]] = {}
    for row in observations:
        out.setdefault(str(row["asset"]), []).append((str(row["day"]), float(row["value"])))
    return {asset: sorted(values) for asset, values in out.items()}


def _direction_consistency(values: list[float]) -> float:
    non_zero = [row for row in values if abs(row) > 0.0000001]
    if not non_zero:
        return 0.0
    positive = len([row for row in non_zero if row > 0])
    negative = len([row for row in non_zero if row < 0])
    return max(positive, negative) / len(non_zero)


def _day_from_trigger_id(trigger_id: str) -> str:
    raw = trigger_id.rsplit("-", 1)[-1]
    if len(raw) != 8 or not raw.isdigit():
        return ""
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"


def _extract_asset_from_question(question: dict[str, Any]) -> str:
    text = str(question.get("question") or "")
    for asset in RETURN_ASSETS:
        if asset in text:
            return asset
    return ""


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
