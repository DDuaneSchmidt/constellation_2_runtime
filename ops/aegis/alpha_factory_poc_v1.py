from __future__ import annotations

import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_poc_v1"
SCHEMA_ID = "aegis_alpha_factory_poc"
SCHEMA_VERSION = "v1"
CALCULATION_VERSION = "alpha_factory_poc_feature_engine.v1"
KNOWN_AT_RULE = "daily_close_known_after_21_00_utc_next_calendar_day"
ASSETS = ("GLD", "SLV", "TLT", "SPY", "QQQ", "UUP", "VIX", "REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS")
RETURN_ASSETS = ("GLD", "SLV", "TLT", "SPY", "QQQ", "UUP")
HORIZONS = (1, 5, 20)


@dataclass(frozen=True)
class Observation:
    observation_id: str
    asset: str
    day: str
    value: float
    source: str
    known_at: str


def build_alpha_factory_poc_v1(*, truth_root: Path, day_utc: str, observations_csv: Path | None = None) -> dict[str, Any]:
    observations = load_observations_v1(observations_csv=observations_csv)
    events: list[dict[str, Any]] = []
    _emit(events, "OBSERVATION_LOADED", "OBS", [row.observation_id for row in observations])
    features = calculate_features_v1(observations)
    _emit(events, "FEATURE_CALCULATED", "FEATURES", [row["feature_id"] for row in features])
    questions = discover_questions_v1(observations=observations, features=features)
    for question in questions:
        _emit(events, "QUESTION_GENERATED", question["question_id"], question["lineage"])
    hypotheses = generate_hypotheses_v1(questions)
    for hypothesis in hypotheses:
        _emit(events, "HYPOTHESIS_GENERATED", hypothesis["hypothesis_id"], hypothesis["lineage"])
    evidence = run_experiments_v1(observations=observations, features=features, hypotheses=hypotheses)
    for artifact in evidence:
        _emit(events, "EXPERIMENT_RUN", artifact["experiment_id"], artifact["lineage"])
        _emit(events, "EVIDENCE_CREATED", artifact["evidence_id"], artifact["lineage"])
    candidates = project_research_asset_candidates_v1(questions=questions, hypotheses=hypotheses, evidence=evidence)
    for candidate in candidates:
        _emit(events, "RESEARCH_ASSET_CANDIDATE_CREATED", candidate["research_asset_candidate_id"], candidate["lineage"])

    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "minimal_alpha_factory_proof_of_concept",
        "safety": {
            "trading_allowed": False,
            "capital_allocation_allowed": False,
            "live_broker_integration_allowed": False,
            "autonomous_execution_allowed": False,
            "mutation_engine_allowed": False,
        },
        "observation_store": [row.__dict__ for row in observations],
        "feature_store": features,
        "question_store": questions,
        "hypothesis_store": hypotheses,
        "evidence_store": evidence,
        "research_asset_candidates": candidates,
        "event_log": events,
        "summary": {
            "observation_count": len(observations),
            "feature_count": len(features),
            "question_count": len(questions),
            "hypothesis_count": len(hypotheses),
            "evidence_count": len(evidence),
            "research_asset_candidate_count": len(candidates),
            "lineage_complete": _lineage_complete(candidates),
            "reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_poc_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "alpha_factory_poc.v1.json", payload)
    event_path = out_dir / "alpha_factory_events.v1.jsonl"
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with event_path.open("a", encoding="utf-8") as handle:
        for event in payload.get("event_log") or []:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
    summary_path = out_dir / "alpha_factory_poc.summary.txt"
    summary_path.write_text(render_alpha_factory_summary_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "events": str(event_path), "summary": str(summary_path)}


def render_alpha_factory_summary_v1(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return "\n".join(
        [
            "AEGIS ALPHA FACTORY POC v1",
            f"day_utc: {payload.get('day_utc')}",
            f"observations: {summary.get('observation_count')}",
            f"features: {summary.get('feature_count')}",
            f"questions: {summary.get('question_count')}",
            f"hypotheses: {summary.get('hypothesis_count')}",
            f"evidence: {summary.get('evidence_count')}",
            f"research_asset_candidates: {summary.get('research_asset_candidate_count')}",
            f"lineage_complete: {str(summary.get('lineage_complete')).lower()}",
            "trading_allowed: false",
            "capital_allocation_allowed: false",
            "",
        ]
    )


def load_observations_v1(*, observations_csv: Path | None = None) -> list[Observation]:
    if observations_csv:
        return _read_observations_csv(observations_csv)
    return _fixture_observations()


def calculate_features_v1(observations: list[Observation]) -> list[dict[str, Any]]:
    by_asset = _series_by_asset(observations)
    obs_id = {(row.asset, row.day): row.observation_id for row in observations}
    features: list[dict[str, Any]] = []
    for asset in RETURN_ASSETS:
        series = by_asset[asset]
        for idx, (day, value) in enumerate(series):
            source = [obs_id[(asset, day)]]
            previous = series[idx - 1][1] if idx >= 1 else None
            daily = None if previous in (None, 0) else (value / previous) - 1.0
            five = _return_over(series, idx, 5)
            twenty = _return_over(series, idx, 20)
            volatility = _std([_daily_return(series, j) for j in range(max(1, idx - 19), idx + 1)])
            z_score = _z_score([row[1] for row in series[max(0, idx - 19) : idx + 1]], value)
            shock = abs(daily or 0.0) >= 0.018 or abs(z_score or 0.0) >= 2.0
            regime = _regime_label(by_asset, idx)
            feature_values = {
                "daily_return": daily,
                "return_5d": five,
                "return_20d": twenty,
                "rolling_volatility_20d": volatility,
                "z_score_20d": z_score,
                "shock_indicator": shock,
                "regime_label": regime,
            }
            for name, feature_value in feature_values.items():
                features.append(_feature(asset, day, name, feature_value, source))
        features.extend(_relationship_features(asset, by_asset, obs_id))
    return features


def discover_questions_v1(*, observations: list[Observation], features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    feature_by_type = _features_by_type(features)
    gld_real_corr = [row for row in feature_by_type["rolling_correlation_20d"] if row["asset"] == "GLD" and row["related_asset"] == "REAL_YIELD"]
    gld_beta = [row for row in feature_by_type["rolling_beta_20d"] if row["asset"] == "GLD" and row["related_asset"] == "REAL_YIELD"]
    gld_shocks = [row for row in feature_by_type["shock_indicator"] if row["asset"] == "GLD" and row["value"] is True]
    slv_shocks = [row for row in feature_by_type["shock_indicator"] if row["asset"] == "SLV" and row["value"] is True]
    tlt_stable = [row for row in feature_by_type["rolling_volatility_20d"] if row["asset"] == "TLT" and row["value"] is not None and row["value"] < 0.004]
    return [
        _question("Q-001", "RELATIONSHIP_BREAKDOWN", "When does GLD stop behaving consistently with real yield shocks?", gld_real_corr[-6:] + gld_beta[-6:], observations),
        _question("Q-002", "SHOCK_RESPONSE_ANOMALY", "Which gold and silver shock days fail to mean revert over short horizons?", gld_shocks[-4:] + slv_shocks[-4:], observations),
        _question("Q-003", "PREDICTION_FAILURE", "When do GLD real-yield beta signs predict the wrong next move?", gld_beta[-10:], observations),
        _question("Q-004", "UNEXPECTED_STABILITY", "When does TLT stay stable despite yield and volatility shocks?", tlt_stable[-8:], observations),
        _question("Q-005", "CORRELATION_BREAKDOWN", "When do GLD and SLV stop moving as the same precious-metals complex?", [row for row in feature_by_type["rolling_correlation_20d"] if row["asset"] == "GLD" and row["related_asset"] == "SLV"][-8:], observations),
    ]


def generate_hypotheses_v1(questions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assets_by_question = {
        "Q-001": ("REAL_YIELD", "GLD"),
        "Q-002": ("GLD", "SLV"),
        "Q-003": ("REAL_YIELD", "GLD"),
        "Q-004": ("NOMINAL_YIELD", "TLT"),
        "Q-005": ("SLV", "GLD"),
    }
    hypotheses: list[dict[str, Any]] = []
    for question in questions:
        shock_asset, target_asset = assets_by_question[question["question_id"]]
        for horizon in HORIZONS:
            hypothesis_id = f"H-{question['question_id'][2:]}-{horizon:02d}"
            hypotheses.append(
                {
                    "hypothesis_id": hypothesis_id,
                    "question_id": question["question_id"],
                    "hypothesis_family": f"{shock_asset}_shock_to_{target_asset}_forward_return",
                    "statement": f"If {shock_asset} shock occurs, does {target_asset} show abnormal forward return over {horizon} trading day horizon?",
                    "shock_feature": f"{shock_asset}:shock_indicator",
                    "target_asset": target_asset,
                    "horizon_days": horizon,
                    "lineage": [question["question_id"], *question["lineage"]],
                    "trading_allowed": False,
                }
            )
    return hypotheses


def run_experiments_v1(*, observations: list[Observation], features: list[dict[str, Any]], hypotheses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_asset = _series_by_asset(observations)
    shock_days = {
        (row["asset"], row["day"]): row
        for row in features
        if row["feature_type"] == "shock_indicator" and row.get("value") is True
    }
    daily_features = {(row["asset"], row["day"], row["feature_type"]): row for row in features}
    evidence: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        shock_asset = hypothesis["shock_feature"].split(":", 1)[0]
        target = hypothesis["target_asset"]
        horizon = int(hypothesis["horizon_days"])
        target_series = by_asset[target]
        index_by_day = {day: idx for idx, (day, _value) in enumerate(target_series)}
        returns: list[float] = []
        lineage = list(hypothesis["lineage"])
        for (asset, day), shock_feature in sorted(shock_days.items()):
            if asset != shock_asset or day not in index_by_day:
                continue
            idx = index_by_day[day]
            if idx + horizon >= len(target_series):
                continue
            start = target_series[idx][1]
            end = target_series[idx + horizon][1]
            if start:
                returns.append((end / start) - 1.0)
                lineage.append(shock_feature["feature_id"])
                maybe_target_feature = daily_features.get((target, day, "daily_return"))
                if maybe_target_feature:
                    lineage.append(maybe_target_feature["feature_id"])
        baseline = [_return_over(target_series, idx, horizon) for idx in range(0, len(target_series) - horizon)]
        baseline_values = [row for row in baseline if row is not None]
        effect = sum(returns) / len(returns) if returns else 0.0
        base = sum(baseline_values) / len(baseline_values) if baseline_values else 0.0
        diff = effect - base
        direction = "POSITIVE" if diff > 0.0005 else "NEGATIVE" if diff < -0.0005 else "FLAT"
        evidence_id = hypothesis["hypothesis_id"].replace("H-", "E-")
        evidence.append(
            {
                "evidence_id": evidence_id,
                "experiment_id": evidence_id.replace("E-", "X-"),
                "hypothesis_id": hypothesis["hypothesis_id"],
                "test_window": {"start_day": target_series[0][0], "end_day": target_series[-1][0], "horizon_days": horizon},
                "sample_count": len(returns),
                "effect_direction": direction,
                "effect_size": round(diff, 8),
                "baseline_comparison": {"shock_forward_return_mean": round(effect, 8), "all_days_forward_return_mean": round(base, 8)},
                "contradiction_notes": "Under-sampled or flat effect; retain as limiting evidence." if len(returns) < 3 or direction == "FLAT" else "",
                "lineage": sorted(set(lineage)),
                "trading_allowed": False,
            }
        )
    return evidence


def project_research_asset_candidates_v1(*, questions: list[dict[str, Any]], hypotheses: list[dict[str, Any]], evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    related_question_ids = {"Q-001", "Q-002", "Q-003"}
    related_hypotheses = [row for row in hypotheses if row["question_id"] in related_question_ids]
    related_evidence = [row for row in evidence if row["hypothesis_id"] in {item["hypothesis_id"] for item in related_hypotheses}]
    supporting = [row for row in related_evidence if row["effect_direction"] in {"POSITIVE", "NEGATIVE"} and row["sample_count"] >= 2]
    contradictory = [row for row in related_evidence if row["effect_direction"] == "FLAT" or row["sample_count"] < 3]
    if len(related_evidence) < 3 or len({row["hypothesis_id"] for row in related_evidence}) < 2:
        return []
    lineage = sorted(set([item for row in related_evidence for item in row["lineage"]] + [row["question_id"] for row in questions if row["question_id"] in related_question_ids]))
    return [
        {
            "research_asset_candidate_id": "RAC-001",
            "proposed_name": "Real Yield Shock Response",
            "identified_relationship_or_structure": "Precious-metals forward returns change after real-yield and precious-metals shocks, with correlation instability as the discovery source.",
            "supporting_evidence": [row["evidence_id"] for row in supporting[:6]],
            "contradictory_evidence": [row["evidence_id"] for row in contradictory[:6]],
            "hypothesis_family": sorted({row["hypothesis_family"] for row in related_hypotheses}),
            "opportunity_surface_notes": "Research-only candidate for falsifying GLD/SLV response to real-yield and metals shocks. Not trade advice.",
            "lineage": lineage,
            "trading_allowed": False,
            "capital_allocation_allowed": False,
        }
    ]


def _fixture_observations() -> list[Observation]:
    rows: list[Observation] = []
    start = date(2024, 1, 2)
    days = [start + timedelta(days=idx) for idx in range(90) if (start + timedelta(days=idx)).weekday() < 5][:64]
    values = {
        "GLD": 185.0,
        "SLV": 22.0,
        "TLT": 96.0,
        "SPY": 475.0,
        "QQQ": 405.0,
        "UUP": 28.0,
        "VIX": 13.0,
        "REAL_YIELD": 1.72,
        "NOMINAL_YIELD": 4.05,
        "INFLATION_EXPECTATIONS": 2.33,
    }
    for idx, day_obj in enumerate(days):
        real_shock = idx in {22, 23, 41, 48}
        metals_shock = idx in {24, 42, 49}
        risk_shock = idx in {18, 37, 56}
        values["REAL_YIELD"] += (0.09 if real_shock else 0.01 * math.sin(idx / 3.0))
        values["NOMINAL_YIELD"] += (0.07 if real_shock else 0.008 * math.cos(idx / 4.0))
        values["INFLATION_EXPECTATIONS"] = values["NOMINAL_YIELD"] - values["REAL_YIELD"]
        values["VIX"] = max(10.0, values["VIX"] + (4.0 if risk_shock else -0.12 + 0.05 * math.sin(idx)))
        values["GLD"] *= 1.0 + (-0.018 if real_shock else 0.026 if metals_shock else 0.0015 * math.sin(idx / 2.0))
        values["SLV"] *= 1.0 + (-0.024 if real_shock else 0.033 if metals_shock else 0.0020 * math.cos(idx / 2.5))
        values["TLT"] *= 1.0 + (-0.015 if real_shock else 0.001 * math.cos(idx / 3.0))
        values["SPY"] *= 1.0 + (-0.014 if risk_shock else 0.0012 * math.sin(idx / 5.0))
        values["QQQ"] *= 1.0 + (-0.018 if risk_shock else 0.0016 * math.sin(idx / 4.0))
        values["UUP"] *= 1.0 + (0.007 if real_shock else 0.0004 * math.cos(idx / 6.0))
        day = day_obj.isoformat()
        for asset in ASSETS:
            rows.append(
                Observation(
                    observation_id=f"OBS-{asset}-{day}",
                    asset=asset,
                    day=day,
                    value=round(values[asset], 6),
                    source="built_in_deterministic_daily_history_fixture_v1",
                    known_at=f"{day}T21:00:00Z",
                )
            )
    return rows


def _read_observations_csv(path: Path) -> list[Observation]:
    rows: list[Observation] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            asset = str(row["asset"]).upper()
            day = str(row["day"])
            rows.append(
                Observation(
                    observation_id=str(row.get("observation_id") or f"OBS-{asset}-{day}"),
                    asset=asset,
                    day=day,
                    value=float(row["value"]),
                    source=str(row.get("source") or "csv_daily_history"),
                    known_at=str(row.get("known_at") or f"{day}T21:00:00Z"),
                )
            )
    missing = sorted(set(ASSETS) - {row.asset for row in rows})
    if missing:
        raise ValueError(f"observations_csv missing required assets: {missing}")
    return sorted(rows, key=lambda row: (row.day, row.asset))


def _series_by_asset(observations: list[Observation]) -> dict[str, list[tuple[str, float]]]:
    series = {asset: [] for asset in ASSETS}
    for row in sorted(observations, key=lambda item: (item.day, item.asset)):
        if row.asset in series:
            series[row.asset].append((row.day, row.value))
    return series


def _relationship_features(asset: str, by_asset: dict[str, list[tuple[str, float]]], obs_id: dict[tuple[str, str], str]) -> list[dict[str, Any]]:
    related_assets = ("REAL_YIELD", "SLV") if asset == "GLD" else ("REAL_YIELD",)
    out: list[dict[str, Any]] = []
    series = by_asset[asset]
    returns = [_daily_return(series, idx) for idx in range(len(series))]
    for related in related_assets:
        related_series = by_asset[related]
        related_changes = [_daily_return(related_series, idx) if related not in {"REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS", "VIX"} else _diff(related_series, idx) for idx in range(len(related_series))]
        for idx, (day, _value) in enumerate(series):
            x = [row for row in related_changes[max(1, idx - 19) : idx + 1] if row is not None]
            y = [row for row in returns[max(1, idx - 19) : idx + 1] if row is not None]
            n = min(len(x), len(y))
            corr = _corr(x[-n:], y[-n:]) if n >= 5 else None
            beta = _beta(x[-n:], y[-n:]) if n >= 5 else None
            source = [obs_id[(asset, day)], obs_id[(related, day)]]
            out.append(_feature(asset, day, "rolling_correlation_20d", corr, source, related_asset=related))
            out.append(_feature(asset, day, "rolling_beta_20d", beta, source, related_asset=related))
    return out


def _feature(asset: str, day: str, feature_type: str, value: Any, source: list[str], related_asset: str | None = None) -> dict[str, Any]:
    related = f"-{related_asset}" if related_asset else ""
    return {
        "feature_id": f"FEAT-{asset}{related}-{feature_type}-{day}",
        "asset": asset,
        "related_asset": related_asset,
        "day": day,
        "feature_type": feature_type,
        "value": round(value, 8) if isinstance(value, float) else value,
        "source_observations": source,
        "calculation_version": CALCULATION_VERSION,
        "known_at_rule": KNOWN_AT_RULE,
        "lineage": list(source),
    }


def _question(question_id: str, source_type: str, question: str, source_features: list[dict[str, Any]], observations: list[Observation]) -> dict[str, Any]:
    source_feature_ids = [row["feature_id"] for row in source_features]
    source_observations = sorted(set(item for row in source_features for item in row.get("source_observations", [])))
    if not source_observations:
        source_observations = [row.observation_id for row in observations[:3]]
    return {
        "question_id": question_id,
        "source_type": source_type,
        "question": question,
        "source_features": source_feature_ids,
        "source_observations": source_observations,
        "lineage": sorted(set(source_feature_ids + source_observations)),
    }


def _features_by_type(features: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in features:
        out.setdefault(row["feature_type"], []).append(row)
    return out


def _emit(events: list[dict[str, Any]], event_type: str, entity_id: str, lineage: list[str]) -> None:
    event_id = f"EVT-{len(events) + 1:04d}-{event_type}-{entity_id}"
    events.append({"event_id": event_id, "event_type": event_type, "entity_id": entity_id, "append_only": True, "lineage": sorted(set(lineage))})


def _daily_return(series: list[tuple[str, float]], idx: int) -> float | None:
    if idx <= 0 or series[idx - 1][1] == 0:
        return None
    return (series[idx][1] / series[idx - 1][1]) - 1.0


def _diff(series: list[tuple[str, float]], idx: int) -> float | None:
    if idx <= 0:
        return None
    return series[idx][1] - series[idx - 1][1]


def _return_over(series: list[tuple[str, float]], idx: int, horizon: int) -> float | None:
    if idx < horizon or series[idx - horizon][1] == 0:
        return None
    return (series[idx][1] / series[idx - horizon][1]) - 1.0


def _std(values: list[float | None]) -> float | None:
    clean = [row for row in values if row is not None]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    return math.sqrt(sum((row - mean) ** 2 for row in clean) / (len(clean) - 1))


def _z_score(values: list[float], value: float) -> float | None:
    if len(values) < 5:
        return None
    mean = sum(values) / len(values)
    std = _std(values)
    if not std:
        return None
    return (value - mean) / std


def _corr(x_values: list[float], y_values: list[float]) -> float | None:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return None
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(y_values) / len(y_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values, strict=True))
    x_var = sum((x - x_mean) ** 2 for x in x_values)
    y_var = sum((y - y_mean) ** 2 for y in y_values)
    if x_var <= 0 or y_var <= 0:
        return None
    return numerator / math.sqrt(x_var * y_var)


def _beta(x_values: list[float], y_values: list[float]) -> float | None:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return None
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(y_values) / len(y_values)
    x_var = sum((x - x_mean) ** 2 for x in x_values)
    if x_var <= 0:
        return None
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values, strict=True)) / x_var


def _regime_label(by_asset: dict[str, list[tuple[str, float]]], idx: int) -> str:
    vix = by_asset["VIX"][idx][1]
    real_change = _diff(by_asset["REAL_YIELD"], idx) or 0.0
    if vix >= 18:
        return "RISK_OFF"
    if real_change >= 0.05:
        return "REAL_YIELD_SHOCK"
    return "NORMAL"


def _lineage_complete(candidates: list[dict[str, Any]]) -> bool:
    return bool(candidates) and all(any(str(item).startswith("OBS-") for item in candidate.get("lineage", [])) for candidate in candidates)


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
