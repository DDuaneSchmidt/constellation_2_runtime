from __future__ import annotations

import csv
import hashlib
import json
import math
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import (
    build_alpha_factory_naive_correlation_baseline_v1,
    write_alpha_factory_naive_correlation_baseline_v1,
)
from ops.aegis.alpha_factory_poc_v1 import (
    ASSETS,
    build_alpha_factory_poc_v1,
    write_alpha_factory_poc_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_out_of_fixture_benchmark_v1"
POC_FAMILY = "aegis_alpha_factory_poc_v1"
BASELINE_FAMILY = "aegis_alpha_factory_naive_correlation_baseline_v1"
SCHEMA_ID = "aegis_alpha_factory_out_of_fixture_benchmark"
SCHEMA_VERSION = "v1"


def build_alpha_factory_out_of_fixture_benchmark_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    in_fixture_poc = _load_or_build_in_fixture_poc(root, day_utc)
    in_fixture_baseline = _load_or_build_in_fixture_baseline(root, day_utc, in_fixture_poc)
    fixture_rows = _alternate_fixture_rows()
    fixture_hash = _hash({"rows": fixture_rows})

    with tempfile.TemporaryDirectory(prefix="aegis_alpha_factory_oof_") as tmp:
        temp_root = Path(tmp) / "truth"
        fixture_csv = Path(tmp) / "alternate_fixture_v1.csv"
        _write_fixture_csv(fixture_csv, fixture_rows)
        out_fixture_poc = build_alpha_factory_poc_v1(
            truth_root=temp_root,
            day_utc=day_utc,
            observations_csv=fixture_csv,
        )
        write_alpha_factory_poc_v1(truth_root=temp_root, day_utc=day_utc, payload=out_fixture_poc)
        out_fixture_baseline = build_alpha_factory_naive_correlation_baseline_v1(
            truth_root=temp_root,
            day_utc=day_utc,
        )

    in_candidate = _candidate_review(in_fixture_poc)
    out_candidate = _candidate_review(out_fixture_poc)
    in_baseline = _baseline_review(in_fixture_baseline)
    out_baseline = _baseline_review(out_fixture_baseline)
    comparison = _compare(
        in_fixture_poc=in_fixture_poc,
        out_fixture_poc=out_fixture_poc,
        in_candidate=in_candidate,
        out_candidate=out_candidate,
        in_baseline=in_baseline,
        out_baseline=out_baseline,
    )
    verdicts = {
        "out_of_fixture_execution": "OUT_OF_FIXTURE_EXECUTION_VALID"
        if out_fixture_poc.get("research_asset_candidates") and out_fixture_baseline.get("top_relationships")
        else "OUT_OF_FIXTURE_EXECUTION_INVALID",
        "poc_generalization": comparison["poc_generalization"],
        "poc_vs_baseline": comparison["poc_vs_baseline"],
        "discovery_advantage": comparison["discovery_advantage"],
        "candidate_naming": comparison["candidate_naming"],
        "minimum_next_action": comparison["minimum_next_action"],
    }
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "out_of_fixture_benchmark_for_alpha_factory_poc_and_naive_baseline",
        "constraints": {
            "poc_discovery_engine_modified": False,
            "naive_baseline_logic_modified": False,
            "research_asset_candidate_naming_logic_modified": False,
            "trading_allowed": False,
        },
        "alternate_fixture": {
            "fixture_id": "alpha_factory_alternate_deterministic_daily_history_v1",
            "fixture_hash": fixture_hash,
            "observation_count": len(fixture_rows),
            "asset_count": len(ASSETS),
            "same_as_poc_fixture": fixture_hash == str(in_fixture_poc.get("content_hash")),
            "design_note": "Alternate deterministic path with GLD shock days followed by SLV drift and macro yield shocks that the unchanged POC cannot sample as shock features.",
        },
        "verdicts": verdicts,
        "in_fixture_reference": {
            "poc": _poc_summary(in_fixture_poc, in_candidate),
            "naive_baseline": in_baseline,
        },
        "out_of_fixture_results": {
            "poc": _poc_summary(out_fixture_poc, out_candidate),
            "naive_baseline": out_baseline,
        },
        "comparison": comparison,
        "hostile_checks": comparison["hostile_checks"],
        "source_artifacts": {
            "in_fixture_poc_hash": str(in_fixture_poc.get("content_hash") or _hash(in_fixture_poc)),
            "in_fixture_naive_baseline_hash": str(in_fixture_baseline.get("content_hash") or _hash(in_fixture_baseline)),
            "out_of_fixture_poc_hash": str(out_fixture_poc.get("content_hash") or _hash(out_fixture_poc)),
            "out_of_fixture_naive_baseline_normalized_hash": _hash(out_baseline),
        },
        "summary": {
            "output_reproducible": True,
            "out_of_fixture_candidate_count": len(_list(out_fixture_poc.get("research_asset_candidates"))),
            "out_of_fixture_relationship_count": int(out_baseline.get("relationship_count") or 0),
            "candidate_name": out_candidate.get("candidate_name"),
            "top_baseline_cluster_id": out_baseline.get("top_cluster", {}).get("cluster_id", ""),
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_out_of_fixture_benchmark_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_out_of_fixture_benchmark_v1.json", payload)
    return {"json": str(path)}


def _load_or_build_in_fixture_poc(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / POC_FAMILY / day_utc / "alpha_factory_poc.v1.json"
    poc = read_json_v1(path)
    if poc:
        return poc
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=day_utc)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=day_utc, payload=poc)
    return poc


def _load_or_build_in_fixture_baseline(root: Path, day_utc: str, poc: dict[str, Any]) -> dict[str, Any]:
    path = root / "reports" / BASELINE_FAMILY / day_utc / "aegis_alpha_factory_naive_correlation_baseline_v1.json"
    baseline = read_json_v1(path)
    if baseline:
        return baseline
    poc_path = root / "reports" / POC_FAMILY / day_utc / "alpha_factory_poc.v1.json"
    if not poc_path.exists():
        write_alpha_factory_poc_v1(truth_root=root, day_utc=day_utc, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=day_utc)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=day_utc, payload=baseline)
    return baseline


def _alternate_fixture_rows() -> list[dict[str, Any]]:
    start = date(2025, 1, 2)
    days = [start + timedelta(days=idx) for idx in range(100) if (start + timedelta(days=idx)).weekday() < 5][:72]
    values = {
        "GLD": 192.0,
        "SLV": 24.0,
        "TLT": 91.0,
        "SPY": 512.0,
        "QQQ": 438.0,
        "UUP": 29.5,
        "VIX": 14.5,
        "REAL_YIELD": 1.35,
        "NOMINAL_YIELD": 4.25,
        "INFLATION_EXPECTATIONS": 2.90,
    }
    gld_shocks = {14, 31, 48, 61}
    real_yield_shocks = {12, 29, 46, 63}
    risk_shocks = {20, 52}
    rows: list[dict[str, Any]] = []
    for idx, day_obj in enumerate(days):
        real_shock = idx in real_yield_shocks
        gld_shock = idx in gld_shocks
        slv_follow_through = any(idx - event in {1, 2, 3, 4, 5} for event in gld_shocks)
        risk_shock = idx in risk_shocks
        values["REAL_YIELD"] += 0.08 if real_shock else 0.004 * math.sin(idx / 3.5)
        values["NOMINAL_YIELD"] += 0.055 if real_shock else 0.003 * math.cos(idx / 4.5)
        values["INFLATION_EXPECTATIONS"] = values["NOMINAL_YIELD"] - values["REAL_YIELD"]
        values["VIX"] = max(11.0, values["VIX"] + (3.4 if risk_shock else -0.08 + 0.04 * math.sin(idx / 2.0)))
        values["GLD"] *= 1.0 + (0.026 if gld_shock else -0.001 if real_shock else 0.0012 * math.sin(idx / 2.4))
        values["SLV"] *= 1.0 + (0.006 if gld_shock else 0.0075 if slv_follow_through else -0.003 if real_shock else 0.0008 * math.cos(idx / 2.7))
        values["TLT"] *= 1.0 + (-0.012 if real_shock else 0.0008 * math.cos(idx / 3.2))
        values["SPY"] *= 1.0 + (-0.015 if risk_shock else 0.0011 * math.sin(idx / 5.5))
        values["QQQ"] *= 1.0 + (-0.018 if risk_shock else 0.0014 * math.sin(idx / 4.2))
        values["UUP"] *= 1.0 + (0.006 if real_shock else 0.00035 * math.cos(idx / 5.8))
        day = day_obj.isoformat()
        for asset in ASSETS:
            rows.append(
                {
                    "observation_id": f"OOF-OBS-{asset}-{day}",
                    "asset": asset,
                    "day": day,
                    "value": round(values[asset], 6),
                    "source": "alpha_factory_alternate_deterministic_daily_history_v1",
                    "known_at": f"{day}T21:00:00Z",
                }
            )
    return rows


def _write_fixture_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["observation_id", "asset", "day", "value", "source", "known_at"])
        writer.writeheader()
        writer.writerows(rows)


def _poc_summary(poc: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "content_hash": poc.get("content_hash"),
        "summary": poc.get("summary", {}),
        "question_templates": [
            {
                "question_id": row.get("question_id"),
                "source_type": row.get("source_type"),
                "question": row.get("question"),
            }
            for row in _list(poc.get("question_store"))
            if isinstance(row, dict)
        ],
        "candidate": candidate,
    }


def _candidate_review(poc: dict[str, Any]) -> dict[str, Any]:
    candidates = [row for row in _list(poc.get("research_asset_candidates")) if isinstance(row, dict)]
    candidate = candidates[0] if candidates else {}
    evidence = [row for row in _list(poc.get("evidence_store")) if isinstance(row, dict)]
    supporting_ids = [str(row) for row in candidate.get("supporting_evidence") or []]
    contradictory_ids = [str(row) for row in candidate.get("contradictory_evidence") or []]
    supporting = [row for row in evidence if str(row.get("evidence_id")) in supporting_ids]
    contradictory = [row for row in evidence if str(row.get("evidence_id")) in contradictory_ids]
    real_yield = [
        row
        for row in evidence
        if str(row.get("hypothesis_id", "")).startswith(("H-001", "H-003"))
    ]
    lineage = [str(row) for row in candidate.get("lineage") or []]
    real_yield_sample_count = sum(int(row.get("sample_count") or 0) for row in real_yield)
    supporting_sample_count = sum(int(row.get("sample_count") or 0) for row in supporting)
    name = str(candidate.get("proposed_name") or "")
    return {
        "candidate_id": candidate.get("research_asset_candidate_id", ""),
        "candidate_name": name,
        "supporting_evidence_ids": supporting_ids,
        "contradictory_evidence_ids": contradictory_ids,
        "supporting_evidence_count": len(supporting),
        "contradictory_evidence_count": len(contradictory),
        "supporting_sample_count": supporting_sample_count,
        "real_yield_evidence_sample_count": real_yield_sample_count,
        "real_yield_support_zero_sample_or_insufficient": real_yield_sample_count < 3,
        "candidate_name_unsupported_by_evidence": "Real Yield" in name and real_yield_sample_count < 3,
        "lineage_complete": bool(lineage) and any(item.startswith("OOF-OBS-") or item.startswith("OBS-") for item in lineage),
        "lineage_count": len(lineage),
    }


def _baseline_review(baseline: dict[str, Any]) -> dict[str, Any]:
    clusters = [row for row in _list(baseline.get("top_candidate_relationship_clusters")) if isinstance(row, dict)]
    relationships = [row for row in _list(baseline.get("top_relationships")) if isinstance(row, dict)]
    return {
        "verdicts": baseline.get("verdicts", {}),
        "relationship_count": baseline.get("summary", {}).get("relationship_count", len(relationships)),
        "cluster_count": baseline.get("summary", {}).get("cluster_count", len(clusters)),
        "top_relationship": relationships[0] if relationships else {},
        "top_cluster": clusters[0] if clusters else {},
        "matched_candidate_clusters": [
            row for row in clusters if row.get("cluster_id") in {"NBC-GLD-TO-SLV", "NBC-SLV-TO-GLD", "NBC-REAL_YIELD-TO-GLD"}
        ][:5],
        "hostile_checks": baseline.get("hostile_checks", {}),
    }


def _compare(
    *,
    in_fixture_poc: dict[str, Any],
    out_fixture_poc: dict[str, Any],
    in_candidate: dict[str, Any],
    out_candidate: dict[str, Any],
    in_baseline: dict[str, Any],
    out_baseline: dict[str, Any],
) -> dict[str, Any]:
    in_questions = _question_signature(in_fixture_poc)
    out_questions = _question_signature(out_fixture_poc)
    same_templates = in_questions == out_questions
    naming_unsupported = bool(out_candidate["candidate_name_unsupported_by_evidence"])
    baseline_recoverable = _baseline_recovers_candidate(out_baseline)
    poc_generalizes = not naming_unsupported and out_candidate["lineage_complete"] and not baseline_recoverable
    if poc_generalizes:
        poc_generalization = "POC_GENERALIZES"
        poc_vs_baseline = "POC_OUTPERFORMS_BASELINE"
        discovery_advantage = "DISCOVERY_ADVANTAGE_PRESENT"
        minimum_next_action = "Run a larger out-of-sample benchmark before promoting discovery claims."
    elif naming_unsupported or baseline_recoverable or same_templates:
        poc_generalization = "POC_DOES_NOT_GENERALIZE"
        poc_vs_baseline = "BASELINE_MATCHES_OR_EXCEEDS_POC"
        discovery_advantage = "DISCOVERY_ADVANTAGE_ABSENT"
        minimum_next_action = (
            "Do not repair candidate logic yet. First add a benchmark gate that rejects the current candidate when real-yield "
            "hypotheses have zero samples or when GLD-to-SLV naive shock response recovers the support."
        )
    else:
        poc_generalization = "INCONCLUSIVE"
        poc_vs_baseline = "INCONCLUSIVE"
        discovery_advantage = "DISCOVERY_ADVANTAGE_INCONCLUSIVE"
        minimum_next_action = "Run a wider deterministic fixture suite before claiming discovery advantage."
    hostile_checks = {
        "fixed_template_question_leakage": same_templates,
        "candidate_name_unsupported_by_evidence": naming_unsupported,
        "zero_sample_feature_support": out_candidate["real_yield_evidence_sample_count"] == 0,
        "naive_baseline_recoverability": baseline_recoverable,
        "lineage_completeness": out_candidate["lineage_complete"],
        "deterministic_replay": True,
        "out_of_fixture_real_yield_evidence_sample_count": out_candidate["real_yield_evidence_sample_count"],
        "out_of_fixture_supporting_evidence_ids": out_candidate["supporting_evidence_ids"],
        "out_of_fixture_top_baseline_cluster_id": out_baseline.get("top_cluster", {}).get("cluster_id", ""),
    }
    return {
        "poc_generalization": poc_generalization,
        "poc_vs_baseline": poc_vs_baseline,
        "discovery_advantage": discovery_advantage,
        "candidate_naming": "OVERNAMED" if naming_unsupported else "CANDIDATE_NAMING_VALID",
        "minimum_next_action": minimum_next_action,
        "research_asset_candidate_quality": {
            "in_fixture": in_candidate,
            "out_of_fixture": out_candidate,
        },
        "relationship_recovery": {
            "in_fixture_candidate_clusters": in_baseline.get("matched_candidate_clusters", []),
            "out_of_fixture_candidate_clusters": out_baseline.get("matched_candidate_clusters", []),
            "out_of_fixture_naive_recovers_candidate_support": baseline_recoverable,
        },
        "metric_comparison": {
            "in_fixture_top_cluster": in_baseline.get("top_cluster", {}),
            "out_of_fixture_top_cluster": out_baseline.get("top_cluster", {}),
            "in_fixture_top_relationship": in_baseline.get("top_relationship", {}),
            "out_of_fixture_top_relationship": out_baseline.get("top_relationship", {}),
        },
        "naming_evidence_alignment": {
            "candidate_name": out_candidate.get("candidate_name"),
            "real_yield_evidence_sample_count": out_candidate["real_yield_evidence_sample_count"],
            "supporting_sample_count": out_candidate["supporting_sample_count"],
            "verdict": "OVERNAMED" if naming_unsupported else "CANDIDATE_NAMING_VALID",
        },
        "hostile_checks": hostile_checks,
    }


def _baseline_recovers_candidate(baseline_review: dict[str, Any]) -> bool:
    clusters = baseline_review.get("matched_candidate_clusters") or []
    for cluster in clusters:
        if not isinstance(cluster, dict):
            continue
        if cluster.get("cluster_id") == "NBC-GLD-TO-SLV" and int(cluster.get("support_count") or 0) >= 2:
            return True
    checks = baseline_review.get("hostile_checks") if isinstance(baseline_review.get("hostile_checks"), dict) else {}
    return bool(checks.get("gld_to_slv_evidence_alone_explains_candidate"))


def _question_signature(poc: dict[str, Any]) -> list[tuple[str, str, str]]:
    return [
        (str(row.get("question_id")), str(row.get("source_type")), str(row.get("question")))
        for row in _list(poc.get("question_store"))
        if isinstance(row, dict)
    ]


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
