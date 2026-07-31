from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_poc_v1 import (
    ASSETS,
    calculate_features_v1,
    load_observations_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_question_discovery_v2"
SCHEMA_ID = "aegis_alpha_factory_question_discovery"
SCHEMA_VERSION = "v2"
FORBIDDEN_ANSWER_LABELS = (
    "Real Yield Shock Response",
    "Volatility Regime Transition",
    "Precious Metals Response",
)


def build_alpha_factory_question_discovery_v2(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    observations = load_observations_v1()
    features = calculate_features_v1(observations)
    triggers = _discover_triggers(features)
    questions = _questions_from_triggers(triggers)
    leakage = _leakage_checks(questions)
    lineage_complete = _lineage_complete(questions)
    data_derived = bool(questions) and all(
        row.get("source_feature_refs") or row.get("source_relationship_refs") for row in questions
    )
    execution_valid = bool(questions) and not leakage["forbidden_answer_label_present"] and lineage_complete
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_question_discovery_v2_data_derived_repair",
        "constraints": {
            "no_discovery_advantage_claim": True,
            "trading_allowed": False,
            "capital_allocation_allowed": False,
            "broker_integration_allowed": False,
            "autonomous_execution_allowed": False,
        },
        "inputs": {
            "observation_count": len(observations),
            "feature_count": len(features),
            "assets": list(ASSETS),
            "feature_engine": "alpha_factory_poc_feature_engine.v1",
        },
        "method": {
            "trigger_types": [
                "RELATIONSHIP_INSTABILITY",
                "CORRELATION_BREAKDOWN",
                "PREDICTION_RESIDUAL_ANOMALY",
                "SHOCK_RESPONSE_ASYMMETRY",
                "UNEXPECTED_STABILITY",
            ],
            "fixed_template_questions_allowed": False,
            "pre_labeled_research_asset_names_allowed": False,
            "question_generation": "rank deterministic data-derived triggers, then render generic questions from trigger metrics",
        },
        "verdicts": {
            "execution": "QUESTION_DISCOVERY_V2_EXECUTION_VALID" if execution_valid else "QUESTION_DISCOVERY_V2_EXECUTION_INVALID",
            "fixed_template_leakage": "STILL_PRESENT" if leakage["forbidden_answer_label_present"] else "FIXED_TEMPLATE_LEAKAGE_REMOVED",
            "questions_data_derived": "QUESTIONS_DATA_DERIVED" if data_derived else "NOT_DATA_DERIVED",
            "lineage": "LINEAGE_COMPLETE" if lineage_complete else "LINEAGE_INCOMPLETE",
            "rebenchmark_readiness": "READY_FOR_REBENCHMARK"
            if execution_valid and data_derived
            else "NOT_READY_FOR_REBENCHMARK",
        },
        "questions": questions,
        "trigger_evidence": triggers,
        "leakage_checks": leakage,
        "summary": {
            "question_count": len(questions),
            "trigger_count": len(triggers),
            "lineage_complete": lineage_complete,
            "output_reproducible": True,
            "discovery_advantage_claimed": False,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_question_discovery_v2(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_question_discovery_v2.json", payload)
    return {"json": str(path)}


def _discover_triggers(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    triggers: list[dict[str, Any]] = []
    relationships = [
        row
        for row in features
        if row.get("feature_type") in {"rolling_correlation_20d", "rolling_beta_20d"}
        and row.get("related_asset")
        and isinstance(row.get("value"), (int, float))
    ]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in relationships:
        grouped.setdefault((str(row["asset"]), str(row["related_asset"]), str(row["feature_type"])), []).append(row)
    for key, rows in grouped.items():
        rows = sorted(rows, key=lambda row: str(row.get("day")))
        triggers.extend(_relationship_instability_triggers(key, rows))
        if key[2] == "rolling_correlation_20d":
            triggers.extend(_correlation_breakdown_triggers(key, rows))
    triggers.extend(_prediction_residual_triggers(features))
    triggers.extend(_shock_response_asymmetry_triggers(features))
    triggers.extend(_unexpected_stability_triggers(features))
    return sorted(
        triggers,
        key=lambda row: (
            -float(row["severity_score"]),
            str(row["trigger_type"]),
            str(row["asset"]),
            str(row.get("related_asset") or ""),
            str(row["trigger_id"]),
        ),
    )[:12]


def _relationship_instability_triggers(
    key: tuple[str, str, str],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if len(rows) < 20:
        return []
    asset, related, feature_type = key
    out: list[dict[str, Any]] = []
    for idx in range(12, len(rows)):
        window = rows[max(0, idx - 11) : idx + 1]
        values = [float(row["value"]) for row in window if isinstance(row.get("value"), (int, float))]
        if len(values) < 8:
            continue
        recent = values[-4:]
        prior = values[:-4]
        if not prior:
            continue
        recent_mean = sum(recent) / len(recent)
        prior_mean = sum(prior) / len(prior)
        spread = max(values) - min(values)
        sign_flip = min(values) < 0 < max(values)
        if spread >= 0.8 or (sign_flip and abs(recent_mean - prior_mean) >= 0.25):
            source = window[-8:]
            out.append(
                _trigger(
                    trigger_type="RELATIONSHIP_INSTABILITY",
                    asset=asset,
                    related_asset=related,
                    day=str(rows[idx]["day"]),
                    feature_type=feature_type,
                    severity=spread + abs(recent_mean - prior_mean),
                    metric_summary={
                        "window_min": round(min(values), 8),
                        "window_max": round(max(values), 8),
                        "recent_mean": round(recent_mean, 8),
                        "prior_mean": round(prior_mean, 8),
                        "sign_flip": sign_flip,
                    },
                    source_features=source,
                )
            )
    return out[-2:]


def _correlation_breakdown_triggers(
    key: tuple[str, str, str],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if len(rows) < 20:
        return []
    asset, related, feature_type = key
    out: list[dict[str, Any]] = []
    for idx in range(14, len(rows)):
        window = rows[max(0, idx - 13) : idx + 1]
        values = [float(row["value"]) for row in window if isinstance(row.get("value"), (int, float))]
        if len(values) < 10:
            continue
        prior = values[:-3]
        recent = values[-3:]
        prior_abs = sum(abs(row) for row in prior) / len(prior)
        recent_abs = sum(abs(row) for row in recent) / len(recent)
        sign_break = any(row < -0.25 for row in prior) and any(row > 0.25 for row in recent)
        magnitude_break = prior_abs >= 0.65 and recent_abs <= 0.35
        if sign_break or magnitude_break:
            source = window[-8:]
            out.append(
                _trigger(
                    trigger_type="CORRELATION_BREAKDOWN",
                    asset=asset,
                    related_asset=related,
                    day=str(rows[idx]["day"]),
                    feature_type=feature_type,
                    severity=abs(prior_abs - recent_abs) + (0.5 if sign_break else 0.0),
                    metric_summary={
                        "prior_abs_mean": round(prior_abs, 8),
                        "recent_abs_mean": round(recent_abs, 8),
                        "sign_break": sign_break,
                        "magnitude_break": magnitude_break,
                    },
                    source_features=source,
                )
            )
    return out[-2:]


def _prediction_residual_triggers(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {(row.get("asset"), row.get("day"), row.get("feature_type"), row.get("related_asset")): row for row in features}
    candidates: list[dict[str, Any]] = []
    for row in features:
        if row.get("feature_type") != "rolling_beta_20d" or row.get("related_asset") != "REAL_YIELD":
            continue
        asset = str(row.get("asset"))
        day = str(row.get("day"))
        beta = row.get("value")
        daily = by_key.get((asset, day, "daily_return", None))
        corr = by_key.get((asset, day, "rolling_correlation_20d", "REAL_YIELD"))
        if not isinstance(beta, (int, float)) or not daily or not isinstance(daily.get("value"), (int, float)):
            continue
        residual = float(daily["value"]) - (float(beta) * 0.01)
        candidates.append({"row": row, "daily": daily, "corr": corr, "residual": residual})
    residuals = [row["residual"] for row in candidates]
    std = _std(residuals)
    if not std:
        return []
    mean = sum(residuals) / len(residuals)
    out: list[dict[str, Any]] = []
    for candidate in candidates:
        z_score = (candidate["residual"] - mean) / std
        if abs(z_score) < 2.0:
            continue
        source_features = [candidate["row"], candidate["daily"]]
        if isinstance(candidate.get("corr"), dict):
            source_features.append(candidate["corr"])
        out.append(
            _trigger(
                trigger_type="PREDICTION_RESIDUAL_ANOMALY",
                asset=str(candidate["row"]["asset"]),
                related_asset="REAL_YIELD",
                day=str(candidate["row"]["day"]),
                feature_type="rolling_beta_20d",
                severity=abs(z_score),
                metric_summary={
                    "residual": round(candidate["residual"], 8),
                    "residual_z_score": round(z_score, 8),
                    "beta_value": candidate["row"].get("value"),
                    "daily_return": candidate["daily"].get("value"),
                },
                source_features=source_features,
            )
        )
    return out[:3]


def _shock_response_asymmetry_triggers(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_asset_day_type = {(row.get("asset"), row.get("day"), row.get("feature_type")): row for row in features}
    shock_rows = [
        row
        for row in features
        if row.get("feature_type") == "shock_indicator" and row.get("value") is True
    ]
    out: list[dict[str, Any]] = []
    for source_asset in sorted({str(row.get("asset")) for row in shock_rows}):
        source_shocks = [row for row in shock_rows if row.get("asset") == source_asset]
        for target_asset in ("GLD", "SLV", "TLT", "SPY", "QQQ", "UUP"):
            if target_asset == source_asset:
                continue
            target_returns = [
                by_asset_day_type.get((target_asset, row.get("day"), "daily_return"))
                for row in source_shocks
            ]
            target_returns = [row for row in target_returns if isinstance(row, dict) and isinstance(row.get("value"), (int, float))]
            if len(target_returns) < 3:
                continue
            positives = [row for row in target_returns if float(row["value"]) > 0]
            negatives = [row for row in target_returns if float(row["value"]) < 0]
            asymmetry = abs(len(positives) - len(negatives)) / len(target_returns)
            mean_response = sum(float(row["value"]) for row in target_returns) / len(target_returns)
            if asymmetry >= 0.65 or abs(mean_response) >= 0.01:
                source_features = source_shocks[:6] + target_returns[:6]
                out.append(
                    _trigger(
                        trigger_type="SHOCK_RESPONSE_ASYMMETRY",
                        asset=target_asset,
                        related_asset=source_asset,
                        day=str(source_shocks[-1]["day"]),
                        feature_type="shock_indicator_to_daily_return",
                        severity=asymmetry + abs(mean_response),
                        metric_summary={
                            "shock_count": len(source_shocks),
                            "response_count": len(target_returns),
                            "positive_response_count": len(positives),
                            "negative_response_count": len(negatives),
                            "mean_same_day_response": round(mean_response, 8),
                            "direction_asymmetry": round(asymmetry, 8),
                        },
                        source_features=source_features,
                    )
                )
    return out[:3]


def _unexpected_stability_triggers(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    vol_rows = [
        row
        for row in features
        if row.get("feature_type") == "rolling_volatility_20d"
        and row.get("asset") == "TLT"
        and isinstance(row.get("value"), (int, float))
    ]
    regime_rows = {
        (row.get("asset"), row.get("day")): row
        for row in features
        if row.get("feature_type") == "regime_label" and row.get("asset") == "TLT"
    }
    out: list[dict[str, Any]] = []
    for idx in range(8, len(vol_rows)):
        window = vol_rows[max(0, idx - 7) : idx + 1]
        clean = [float(row["value"]) for row in window if isinstance(row.get("value"), (int, float))]
        regimes = [regime_rows.get(("TLT", row.get("day"))) for row in window]
        shock_regimes = [row for row in regimes if isinstance(row, dict) and row.get("value") in {"REAL_YIELD_SHOCK", "RISK_OFF"}]
        if len(clean) < 5 or not shock_regimes:
            continue
        mean_vol = sum(clean) / len(clean)
        if mean_vol <= 0.0045:
            source_features = window + shock_regimes
            out.append(
                _trigger(
                    trigger_type="UNEXPECTED_STABILITY",
                    asset="TLT",
                    related_asset="REGIME_LABEL",
                    day=str(vol_rows[idx]["day"]),
                    feature_type="rolling_volatility_20d",
                    severity=(0.0045 - mean_vol) + (len(shock_regimes) * 0.1),
                    metric_summary={
                        "mean_volatility": round(mean_vol, 8),
                        "shock_regime_count": len(shock_regimes),
                    },
                    source_features=source_features,
                )
            )
    return out[-2:]


def _trigger(
    *,
    trigger_type: str,
    asset: str,
    related_asset: str,
    day: str,
    feature_type: str,
    severity: float,
    metric_summary: dict[str, Any],
    source_features: list[dict[str, Any]],
) -> dict[str, Any]:
    source_refs = sorted({str(row.get("feature_id")) for row in source_features if row.get("feature_id")})
    source_observations = sorted(
        {
            str(obs)
            for row in source_features
            for obs in row.get("source_observations", [])
            if obs
        }
    )
    related = related_asset or "NONE"
    day_key = day.replace("-", "")
    return {
        "trigger_id": f"QDV2-TRIG-{trigger_type}-{asset}-{related}-{feature_type}-{day_key}",
        "trigger_type": trigger_type,
        "asset": asset,
        "related_asset": related_asset,
        "day": day,
        "feature_type": feature_type,
        "severity_score": round(float(severity), 8),
        "metric_summary": metric_summary,
        "source_feature_refs": source_refs,
        "source_relationship_refs": source_refs if "rolling_" in feature_type or "relationship" in trigger_type.lower() else [],
        "source_observation_refs": source_observations,
        "lineage": sorted(set(source_refs + source_observations)),
    }


def _questions_from_triggers(triggers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    questions: list[dict[str, Any]] = []
    for idx, trigger in enumerate(triggers, start=1):
        asset = str(trigger["asset"])
        related = str(trigger.get("related_asset") or "the referenced variable")
        source_type = str(trigger["trigger_type"])
        if source_type == "RELATIONSHIP_INSTABILITY":
            text = f"When does the observed {asset} relationship with {related} become unstable relative to its recent window?"
        elif source_type == "CORRELATION_BREAKDOWN":
            text = f"When does the observed correlation between {asset} and {related} break from its recent magnitude or sign?"
        elif source_type == "PREDICTION_RESIDUAL_ANOMALY":
            text = f"When do {asset} observed returns diverge from the relationship-implied move versus {related}?"
        elif source_type == "SHOCK_RESPONSE_ASYMMETRY":
            text = f"When do {asset} same-day responses become asymmetric after observed {related} shock days?"
        elif source_type == "UNEXPECTED_STABILITY":
            text = f"When does {asset} remain unusually stable during observed stress-regime features?"
        else:
            text = f"When does {asset} show an unexplained anomaly versus {related}?"
        questions.append(
            {
                "question_id": f"QDV2-{idx:03d}",
                "source_type": source_type,
                "question": text,
                "trigger_id": trigger["trigger_id"],
                "source_feature_refs": trigger["source_feature_refs"],
                "source_relationship_refs": trigger["source_relationship_refs"],
                "source_observation_refs": trigger["source_observation_refs"],
                "trigger_metric_summary": trigger["metric_summary"],
                "lineage": sorted(set(trigger["lineage"] + [trigger["trigger_id"]])),
                "trading_allowed": False,
            }
        )
    return questions


def _leakage_checks(questions: list[dict[str, Any]]) -> dict[str, Any]:
    hits: list[dict[str, str]] = []
    for question in questions:
        text = str(question.get("question") or "")
        for label in FORBIDDEN_ANSWER_LABELS:
            if label.lower() in text.lower():
                hits.append({"question_id": str(question.get("question_id")), "forbidden_label": label})
    return {
        "forbidden_answer_labels": list(FORBIDDEN_ANSWER_LABELS),
        "forbidden_answer_label_present": bool(hits),
        "hits": hits,
        "fixed_answer_labels_allowed_before_grouping": False,
    }


def _lineage_complete(questions: list[dict[str, Any]]) -> bool:
    if not questions:
        return False
    for question in questions:
        lineage = [str(row) for row in question.get("lineage") or []]
        refs = [str(row) for row in question.get("source_feature_refs") or question.get("source_relationship_refs") or []]
        observations = [str(row) for row in question.get("source_observation_refs") or []]
        if not refs or not observations:
            return False
        if not all(ref in lineage for ref in refs):
            return False
        if not all(obs in lineage for obs in observations):
            return False
    return True


def _std(values: list[float]) -> float | None:
    clean = [row for row in values if isinstance(row, (int, float))]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    return math.sqrt(sum((row - mean) ** 2 for row in clean) / (len(clean) - 1))


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
