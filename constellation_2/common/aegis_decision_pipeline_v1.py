from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from constellation_2.common.aegis_structure_selection_v1 import (
    STRUCTURE_REASON_CODES,
    build_structure_input_snapshot_v1,
    build_structure_selection_summary_v1,
    select_structure_for_candidate_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMPATIBILITY_REGISTRY_PATH = (
    REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_AEGIS_SLEEVE_REGIME_COMPATIBILITY_V1.json"
).resolve()

REGIME_TRENDING = "TRENDING"
REGIME_CHOPPY = "CHOPPY"
REGIME_HIGH_VOL_UNSTABLE = "HIGH_VOL_UNSTABLE"
SUPPORTED_REGIMES = frozenset({REGIME_TRENDING, REGIME_CHOPPY, REGIME_HIGH_VOL_UNSTABLE})

STATUS_PROPOSED = "proposed"
STATUS_REGIME_ALLOWED = "regime_allowed"
STATUS_REGIME_REJECTED = "regime_rejected"

RISK_NOT_EVALUATED = "not_evaluated"
EXECUTION_NOT_ATTEMPTED = "not_attempted"

RC_REGIME_COMPATIBLE = "REGIME_COMPATIBLE"
RC_REGIME_INCOMPATIBLE = "REGIME_INCOMPATIBLE"
RC_REGIME_UNKNOWN = "REGIME_UNKNOWN"
RC_SLEEVE_COMPATIBILITY_UNKNOWN = "SLEEVE_COMPATIBILITY_UNKNOWN"
RC_RISK_APPROVED = "RISK_APPROVED"
RC_RISK_REJECTED = "RISK_REJECTED"
RC_KILL_SWITCH_ACTIVE = "KILL_SWITCH_ACTIVE"
RC_CAPITAL_LIMIT = "CAPITAL_LIMIT"
RC_EXECUTION_APPROVED = "EXECUTION_APPROVED"
RC_EXECUTION_FAILED = "EXECUTION_FAILED"

DECISION_REASON_CODES = frozenset(
    {
        RC_REGIME_COMPATIBLE,
        RC_REGIME_INCOMPATIBLE,
        RC_REGIME_UNKNOWN,
        RC_SLEEVE_COMPATIBILITY_UNKNOWN,
        RC_RISK_APPROVED,
        RC_RISK_REJECTED,
        RC_KILL_SWITCH_ACTIVE,
        RC_CAPITAL_LIMIT,
        RC_EXECUTION_APPROVED,
        RC_EXECUTION_FAILED,
    }
)

TRACE_SOURCE_FIELDS = (
    "source_intent_path",
    "source_intent_sha256",
    "producer_result_path",
    "producer_result_sha256",
    "registry_path",
    "registry_sha256",
    "regime_source_path",
    "regime_source_sha256",
)


@dataclass(frozen=True)
class RegimeStateV1:
    regime: str
    confidence: str
    trend_strength: str
    volatility_level: str
    breadth_participation: str | None = None
    reason_codes: tuple[str, ...] = ()


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _hash_payload(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _norm_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def _norm_upper(value: Any, default: str = "") -> str:
    return _norm_text(value, default).upper()


def _read_json_object(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _sha256_file_if_present(path_text: str) -> str:
    text = _norm_text(path_text)
    if not text:
        return ""
    path = Path(text).expanduser().resolve()
    if not path.exists() or not path.is_file():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def classify_market_regime_v1(
    *,
    trend_strength: str,
    volatility_level: str,
    breadth_participation: str | None = None,
) -> RegimeStateV1:
    trend = _norm_upper(trend_strength, "UNKNOWN")
    vol = _norm_upper(volatility_level, "UNKNOWN")
    breadth = None if breadth_participation is None else _norm_upper(breadth_participation, "UNKNOWN")
    reason_codes: list[str] = []

    if vol in {"HIGH", "HIGH_VOL_UNSTABLE"}:
        regime = REGIME_HIGH_VOL_UNSTABLE
        reason_codes.append("HIGH_VOL_UNSTABLE_OVERRIDE")
    elif trend == "UNKNOWN":
        regime = "UNKNOWN"
        reason_codes.append("TREND_STRENGTH_UNKNOWN")
    elif vol == "UNKNOWN":
        regime = "UNKNOWN"
        reason_codes.append("VOLATILITY_LEVEL_UNKNOWN")
    elif trend in {"STRONG", "HIGH", "TRENDING"}:
        regime = REGIME_TRENDING
        reason_codes.append("TREND_STRENGTH_SUPPORTS_TRENDING")
    elif trend in {"LOW", "WEAK", "FLAT", "CHOPPY"}:
        regime = REGIME_CHOPPY
        reason_codes.append("TREND_STRENGTH_SUPPORTS_CHOPPY")
    else:
        regime = "UNKNOWN"
        reason_codes.append(RC_REGIME_UNKNOWN)

    if breadth is None:
        reason_codes.append("BREADTH_PARTICIPATION_NOT_AVAILABLE")

    confidence = "HIGH" if regime in SUPPORTED_REGIMES and trend != "UNKNOWN" and vol != "UNKNOWN" else "LOW"
    return RegimeStateV1(
        regime=regime,
        confidence=confidence,
        trend_strength=trend,
        volatility_level=vol,
        breadth_participation=breadth,
        reason_codes=tuple(reason_codes),
    )


def regime_state_from_payload_v1(payload: Mapping[str, Any]) -> RegimeStateV1:
    regime = _norm_upper(payload.get("regime") or payload.get("regime_label"), "UNKNOWN")
    confidence = _norm_upper(payload.get("confidence") or payload.get("confidence_class"), "UNKNOWN")
    inputs = payload.get("inputs") if isinstance(payload.get("inputs"), Mapping) else {}
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else {}
    trend = _norm_upper(payload.get("trend_strength") or inputs.get("trend_strength") or evidence.get("trend_strength"), "UNKNOWN")
    vol = _norm_upper(payload.get("volatility_level") or inputs.get("volatility_level") or evidence.get("volatility_level"), "UNKNOWN")
    breadth_raw = payload.get("breadth_participation") or inputs.get("breadth_participation") or evidence.get("breadth_participation")
    breadth = _norm_upper(breadth_raw, "UNKNOWN") if breadth_raw is not None else None
    reason_codes = tuple(str(code) for code in payload.get("reason_codes", []) if str(code).strip()) if isinstance(payload.get("reason_codes"), list) else ()
    return RegimeStateV1(
        regime=regime,
        confidence=confidence,
        trend_strength=trend,
        volatility_level=vol,
        breadth_participation=breadth,
        reason_codes=reason_codes,
    )


def load_compatibility_registry_v1(path: Path | None = None) -> dict[str, Any]:
    registry_path = Path(path or DEFAULT_COMPATIBILITY_REGISTRY_PATH).resolve()
    registry = _read_json_object(registry_path)
    if registry.get("schema_id") != "c2_aegis_sleeve_regime_compatibility_registry":
        raise ValueError(f"COMPATIBILITY_REGISTRY_SCHEMA_ID_INVALID:{registry_path}")
    if registry.get("schema_version") != "v1":
        raise ValueError(f"COMPATIBILITY_REGISTRY_SCHEMA_VERSION_INVALID:{registry_path}")
    regimes = registry.get("regimes")
    if not isinstance(regimes, list) or set(str(item) for item in regimes) != set(SUPPORTED_REGIMES):
        raise ValueError(f"COMPATIBILITY_REGISTRY_REGIMES_INVALID:{registry_path}")
    sleeves = registry.get("sleeves")
    if not isinstance(sleeves, list):
        raise ValueError(f"COMPATIBILITY_REGISTRY_SLEEVES_INVALID:{registry_path}")
    seen: set[str] = set()
    for row in sleeves:
        if not isinstance(row, dict):
            raise ValueError("COMPATIBILITY_REGISTRY_SLEEVE_NOT_OBJECT")
        sleeve_id = _norm_text(row.get("sleeve_id"))
        if not sleeve_id:
            raise ValueError("COMPATIBILITY_REGISTRY_SLEEVE_ID_MISSING")
        if sleeve_id in seen:
            raise ValueError(f"COMPATIBILITY_REGISTRY_DUPLICATE_SLEEVE:{sleeve_id}")
        seen.add(sleeve_id)
        compatible = row.get("compatible_regimes")
        if not isinstance(compatible, list) or not compatible:
            raise ValueError(f"COMPATIBILITY_REGISTRY_COMPATIBLE_REGIMES_MISSING:{sleeve_id}")
        unknown = sorted(set(str(item) for item in compatible) - SUPPORTED_REGIMES)
        if unknown:
            raise ValueError(f"COMPATIBILITY_REGISTRY_UNKNOWN_REGIME:{sleeve_id}:{','.join(unknown)}")
    return registry


def sleeve_compatibility_map_v1(registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("sleeve_id")).strip(): dict(row)
        for row in registry.get("sleeves", [])
        if isinstance(row, Mapping) and str(row.get("sleeve_id") or "").strip()
    }


def normalize_candidate_v1(
    *,
    raw_signal_payload: Mapping[str, Any],
    current_regime: str,
    compatible_regimes: Sequence[str] | None = None,
    candidate_id: str | None = None,
    timestamp: str | None = None,
    source_intent_path: str = "",
    source_intent_sha256: str = "",
    producer_result_path: str = "",
    producer_result_sha256: str = "",
    registry_path: str = "",
    registry_sha256: str = "",
    regime_source_path: str = "",
    regime_source_sha256: str = "",
) -> dict[str, Any]:
    raw = dict(raw_signal_payload)
    engine = raw.get("engine") if isinstance(raw.get("engine"), Mapping) else {}
    underlying = raw.get("underlying") if isinstance(raw.get("underlying"), Mapping) else {}
    sleeve_id = _norm_text(engine.get("engine_id") or raw.get("sleeve_id") or raw.get("strategy_id"), "UNKNOWN")
    symbol = _norm_upper(underlying.get("symbol") or raw.get("symbol"), "UNKNOWN")
    exposure_type = _norm_upper(raw.get("exposure_type") or raw.get("direction"), "UNKNOWN")
    direction = _direction_from_exposure(exposure_type)
    base_id = candidate_id or _hash_payload(
        {
            "sleeve_id": sleeve_id,
            "intent_id": raw.get("intent_id"),
            "symbol": symbol,
            "raw_signal_payload": raw,
        }
    )
    thesis = _norm_text(raw.get("thesis") or raw.get("risk_class") or exposure_type, "")
    source_fields = {
        "source_intent_path": _norm_text(source_intent_path or raw.get("source_intent_path"), ""),
        "source_intent_sha256": _norm_text(source_intent_sha256 or raw.get("source_intent_sha256"), ""),
        "producer_result_path": _norm_text(producer_result_path or raw.get("producer_result_path"), ""),
        "producer_result_sha256": _norm_text(producer_result_sha256 or raw.get("producer_result_sha256"), ""),
        "registry_path": _norm_text(registry_path or raw.get("registry_path"), ""),
        "registry_sha256": _norm_text(registry_sha256 or raw.get("registry_sha256"), ""),
        "regime_source_path": _norm_text(regime_source_path or raw.get("regime_source_path"), ""),
        "regime_source_sha256": _norm_text(regime_source_sha256 or raw.get("regime_source_sha256"), ""),
    }
    candidate = {
        "candidate_id": base_id,
        "timestamp": timestamp or _norm_text(raw.get("created_at_utc") or raw.get("produced_utc"), _utc_now_iso()),
        "sleeve_id": sleeve_id,
        "sleeve_name": _norm_text(raw.get("sleeve_name") or sleeve_id, sleeve_id),
        "symbol": symbol,
        "direction": direction,
        "entry_type": _entry_type_from_payload(raw),
        "thesis": thesis,
        "confidence": _norm_upper(raw.get("confidence"), "UNKNOWN"),
        "expected_holding_period": str(raw.get("expected_holding_period") or raw.get("expected_holding_days") or ""),
        "invalidation_reason": _norm_text(raw.get("invalidation_reason"), ""),
        "raw_signal_payload": raw,
        "compatible_regimes": [str(item).strip().upper() for item in (compatible_regimes or []) if str(item).strip()],
        "current_regime": _norm_upper(current_regime, "UNKNOWN"),
        "regime_compatible": False,
        "decision_status": STATUS_PROPOSED,
        "decision_reason_codes": [],
        "risk_status": RISK_NOT_EVALUATED,
        "execution_status": EXECUTION_NOT_ATTEMPTED,
        **source_fields,
    }
    for optional_field in ("signal_strength", "expected_move", "holding_period"):
        optional_value = _norm_text(raw.get(optional_field), "")
        if optional_value:
            candidate[optional_field] = optional_value
    return candidate


def _direction_from_exposure(exposure_type: str) -> str:
    if exposure_type.startswith("LONG"):
        return "LONG"
    if exposure_type.startswith("SHORT"):
        return "SHORT"
    if exposure_type in {"BUY", "SELL"}:
        return exposure_type
    return "UNKNOWN"


def _entry_type_from_payload(raw: Mapping[str, Any]) -> str:
    option = raw.get("option") if isinstance(raw.get("option"), Mapping) else {}
    if option:
        structure = _norm_upper(option.get("structure"), "OPTION")
        direction = _norm_upper(option.get("direction"), "")
        return f"{direction}_{structure}".strip("_")
    return _norm_upper(raw.get("entry_type") or raw.get("exposure_type"), "UNKNOWN")


def evaluate_candidate_regime_compatibility_v1(
    *,
    candidate: Mapping[str, Any],
    regime_state: RegimeStateV1 | Mapping[str, Any],
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    state = regime_state if isinstance(regime_state, RegimeStateV1) else regime_state_from_payload_v1(regime_state)
    sleeve_id = _norm_text(candidate.get("sleeve_id"), "UNKNOWN")
    sleeve_map = sleeve_compatibility_map_v1(registry)
    out = dict(candidate)
    out["current_regime"] = state.regime
    out["decision_reason_codes"] = list(candidate.get("decision_reason_codes") or [])

    row = sleeve_map.get(sleeve_id)
    if row is None:
        out["compatible_regimes"] = []
        out["regime_compatible"] = False
        out["decision_status"] = STATUS_REGIME_REJECTED
        _append_reason(out, RC_SLEEVE_COMPATIBILITY_UNKNOWN)
        return out

    compatible_regimes = [str(item).strip().upper() for item in row.get("compatible_regimes", []) if str(item).strip()]
    out["sleeve_name"] = _norm_text(row.get("sleeve_name"), sleeve_id)
    out["compatible_regimes"] = compatible_regimes

    if state.regime not in SUPPORTED_REGIMES:
        out["regime_compatible"] = False
        out["decision_status"] = STATUS_REGIME_REJECTED
        _append_reason(out, RC_REGIME_UNKNOWN)
        return out

    if state.regime not in compatible_regimes:
        out["regime_compatible"] = False
        out["decision_status"] = STATUS_REGIME_REJECTED
        _append_reason(out, RC_REGIME_INCOMPATIBLE)
        return out

    out["regime_compatible"] = True
    out["decision_status"] = STATUS_REGIME_ALLOWED
    _append_reason(out, RC_REGIME_COMPATIBLE)
    return out


def _append_reason(candidate: dict[str, Any], reason_code: str) -> None:
    codes = candidate.setdefault("decision_reason_codes", [])
    if reason_code not in codes:
        codes.append(reason_code)


def build_decision_trace_v1(
    *,
    day_utc: str,
    raw_signal_payloads: Sequence[Mapping[str, Any]],
    regime_state: RegimeStateV1 | Mapping[str, Any],
    registry: Mapping[str, Any] | None = None,
    registry_path: str | Path | None = None,
    registry_sha256: str = "",
    regime_source_path: str | Path | None = None,
    regime_source_sha256: str = "",
    producer_result_path: str | Path | None = None,
    producer_result_sha256: str = "",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    state = regime_state if isinstance(regime_state, RegimeStateV1) else regime_state_from_payload_v1(regime_state)
    resolved_registry_path = Path(registry_path or DEFAULT_COMPATIBILITY_REGISTRY_PATH).resolve()
    compatibility_registry = dict(registry or load_compatibility_registry_v1(resolved_registry_path))
    registry_sha = _norm_text(registry_sha256) or _sha256_file_if_present(str(resolved_registry_path))
    regime_path_text = _norm_text(str(regime_source_path or ""))
    regime_sha = _norm_text(regime_source_sha256) or _sha256_file_if_present(regime_path_text)
    producer_path_text = _norm_text(str(producer_result_path or ""))
    producer_sha = _norm_text(producer_result_sha256) or _sha256_file_if_present(producer_path_text)
    regime_candidates = [
        evaluate_candidate_regime_compatibility_v1(
            candidate=normalize_candidate_v1(
                raw_signal_payload=raw,
                current_regime=state.regime,
                source_intent_path=str(raw.get("source_intent_path") or ""),
                source_intent_sha256=str(raw.get("source_intent_sha256") or ""),
                producer_result_path=str(raw.get("producer_result_path") or producer_path_text),
                producer_result_sha256=str(raw.get("producer_result_sha256") or producer_sha),
                registry_path=str(raw.get("registry_path") or resolved_registry_path),
                registry_sha256=str(raw.get("registry_sha256") or registry_sha),
                regime_source_path=str(raw.get("regime_source_path") or regime_path_text),
                regime_source_sha256=str(raw.get("regime_source_sha256") or regime_sha),
            ),
            regime_state=state,
            registry=compatibility_registry,
        )
        for raw in raw_signal_payloads
    ]
    candidates: list[dict[str, Any]] = []
    for candidate in regime_candidates:
        candidate_with_structure = dict(candidate)
        candidate_with_structure["structure_input"] = build_structure_input_snapshot_v1(candidate_with_structure)
        candidate_with_structure["structure_decision"] = select_structure_for_candidate_v1(candidate_with_structure)
        candidates.append(candidate_with_structure)
    active_sleeves = sorted({str(item.get("sleeve_id")) for item in candidates if item.get("decision_status") == STATUS_REGIME_ALLOWED})
    disabled_sleeves = sorted({str(item.get("sleeve_id")) for item in candidates if item.get("decision_status") == STATUS_REGIME_REJECTED})
    structure_decisions = [
        dict(item["structure_decision"])
        for item in candidates
        if isinstance(item.get("structure_decision"), Mapping)
    ]
    summary = {
        "regime": state.regime,
        "active_sleeves": active_sleeves,
        "disabled_sleeves": disabled_sleeves,
        "candidates_generated": len(candidates),
        "candidates_rejected_by_regime": sum(
            1 for item in candidates if RC_REGIME_INCOMPATIBLE in item.get("decision_reason_codes", [])
        ),
        "candidates_rejected_by_risk": sum(1 for item in candidates if item.get("risk_status") == "rejected"),
        "candidates_regime_allowed": sum(1 for item in candidates if item.get("decision_status") == STATUS_REGIME_ALLOWED),
        "execution_failures": sum(1 for item in candidates if item.get("execution_status") == "failed"),
        "structure_decisions_evaluated": len(structure_decisions),
    }
    trace: dict[str, Any] = {
        "schema_id": "C2_AEGIS_DECISION_TRACE_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "regime_state": {
            "regime": state.regime,
            "confidence": state.confidence,
            "trend_strength": state.trend_strength,
            "volatility_level": state.volatility_level,
            "breadth_participation": state.breadth_participation,
            "reason_codes": list(state.reason_codes),
        },
        "trace_sources": {
            "registry_path": str(resolved_registry_path),
            "registry_sha256": registry_sha,
            "regime_source_path": regime_path_text,
            "regime_source_sha256": regime_sha,
            "producer_result_path": producer_path_text,
            "producer_result_sha256": producer_sha,
        },
        "decision_reason_codes_supported": sorted(DECISION_REASON_CODES),
        "structure_reason_codes_supported": sorted(STRUCTURE_REASON_CODES),
        "candidates": candidates,
        "summary": summary,
        "canonical_json_hash": "",
    }
    trace["canonical_json_hash"] = _hash_payload(trace)
    return trace


def _count_by_field(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = _norm_text(row.get(field), "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _rejection_reason_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        if candidate.get("decision_status") != STATUS_REGIME_REJECTED:
            continue
        reason_codes = candidate.get("decision_reason_codes")
        if not isinstance(reason_codes, list) or not reason_codes:
            counts["NO_REASON_CODE"] = counts.get("NO_REASON_CODE", 0) + 1
            continue
        for code in reason_codes:
            key = _norm_text(code, "UNKNOWN")
            counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def build_eod_advisory_from_decision_trace_v1(
    *,
    decision_trace: Mapping[str, Any],
    source_trace_path: str | Path = "",
    source_trace_sha256: str = "",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    regime_state = decision_trace.get("regime_state") if isinstance(decision_trace.get("regime_state"), Mapping) else {}
    trace_sources = decision_trace.get("trace_sources") if isinstance(decision_trace.get("trace_sources"), Mapping) else {}
    candidates = [
        dict(candidate)
        for candidate in decision_trace.get("candidates", [])
        if isinstance(candidate, Mapping)
    ]
    source_trace_path_text = _norm_text(str(source_trace_path or ""))
    source_trace_sha = _norm_text(source_trace_sha256) or _sha256_file_if_present(source_trace_path_text)
    regime_allowed = [row for row in candidates if row.get("decision_status") == STATUS_REGIME_ALLOWED]
    regime_rejected = [row for row in candidates if row.get("decision_status") == STATUS_REGIME_REJECTED]
    source_candidate_refs = [
        {
            "candidate_id": _norm_text(candidate.get("candidate_id"), ""),
            "sleeve_id": _norm_text(candidate.get("sleeve_id"), ""),
            "source_intent_path": _norm_text(candidate.get("source_intent_path"), ""),
            "source_intent_sha256": _norm_text(candidate.get("source_intent_sha256"), ""),
            "producer_result_path": _norm_text(candidate.get("producer_result_path"), ""),
            "producer_result_sha256": _norm_text(candidate.get("producer_result_sha256"), ""),
            "registry_path": _norm_text(candidate.get("registry_path"), ""),
            "registry_sha256": _norm_text(candidate.get("registry_sha256"), ""),
            "regime_source_path": _norm_text(candidate.get("regime_source_path"), ""),
            "regime_source_sha256": _norm_text(candidate.get("regime_source_sha256"), ""),
        }
        for candidate in candidates
    ]
    advisory: dict[str, Any] = {
        "schema_id": "C2_AEGIS_EOD_ADVISORY_V1",
        "schema_version": 1,
        "day_utc": _norm_text(decision_trace.get("day_utc"), ""),
        "produced_utc": produced_utc or _utc_now_iso(),
        "current_regime": _norm_text(regime_state.get("regime"), "UNKNOWN"),
        "regime_confidence": _norm_text(regime_state.get("confidence"), "UNKNOWN"),
        "active_sleeves": sorted({_norm_text(candidate.get("sleeve_id"), "UNKNOWN") for candidate in regime_allowed}),
        "disabled_sleeves": sorted({_norm_text(candidate.get("sleeve_id"), "UNKNOWN") for candidate in regime_rejected}),
        "candidates_proposed": len(candidates),
        "candidates_regime_allowed": len(regime_allowed),
        "candidates_regime_rejected": len(regime_rejected),
        "rejection_reasons": _rejection_reason_summary(candidates),
        "risk_status_summary": _count_by_field(candidates, "risk_status"),
        "execution_status_summary": _count_by_field(candidates, "execution_status"),
        "structure_selection_summary": build_structure_selection_summary_v1(
            [
                dict(candidate["structure_decision"])
                for candidate in candidates
                if isinstance(candidate.get("structure_decision"), Mapping)
            ]
        ),
        "source_trace_refs": {
            "decision_trace_path": source_trace_path_text,
            "decision_trace_sha256": source_trace_sha,
            "registry_path": _norm_text(trace_sources.get("registry_path"), ""),
            "registry_sha256": _norm_text(trace_sources.get("registry_sha256"), ""),
            "regime_source_path": _norm_text(trace_sources.get("regime_source_path"), ""),
            "regime_source_sha256": _norm_text(trace_sources.get("regime_source_sha256"), ""),
            "producer_result_path": _norm_text(trace_sources.get("producer_result_path"), ""),
            "producer_result_sha256": _norm_text(trace_sources.get("producer_result_sha256"), ""),
            "candidate_source_refs": source_candidate_refs,
        },
        "advisory_only": True,
        "controls_phasec_materialization": False,
        "controls_broker_execution": False,
        "canonical_json_hash": "",
    }
    advisory["canonical_json_hash"] = _hash_payload(advisory)
    return advisory


def decision_trace_output_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "aegis_decision_trace_v1" / day_utc / "aegis_decision_trace.v1.json"


def write_decision_trace_v1(*, truth_root: Path, day_utc: str, payload: Mapping[str, Any]) -> Path:
    out = decision_trace_output_path_v1(truth_root=truth_root, day_utc=day_utc)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(_canonical_bytes(payload) + b"\n")
    return out


def eod_advisory_output_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "aegis_eod_advisory_v1" / day_utc / "aegis_eod_advisory.v1.json"


def write_eod_advisory_v1(*, truth_root: Path, day_utc: str, payload: Mapping[str, Any]) -> Path:
    out = eod_advisory_output_path_v1(truth_root=truth_root, day_utc=day_utc)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(_canonical_bytes(payload) + b"\n")
    return out
