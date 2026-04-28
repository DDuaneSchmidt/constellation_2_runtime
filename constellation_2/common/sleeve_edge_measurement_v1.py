from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, WriteResultV1, write_file_immutable_v1


REPO_ROOT = Path(__file__).resolve().parents[2]

FACT_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_edge_fact_ledger.v1.schema.json"
SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_edge_snapshot.v1.schema.json"
QUALIFICATION_POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json").resolve()

CALCULATION_VERSION = "sleeve_edge_measurement_v1"
FACT_LEDGER_FAMILY = "sleeve_edge_fact_ledger_v1"
SNAPSHOT_FAMILY = "sleeve_edge_snapshot_v1"

MEASUREMENT_NATIVE = "NATIVE_ENTRY"
MEASUREMENT_ADOPTED = "ADOPTED_POSITION_MANAGEMENT"
MEASUREMENT_UNKNOWN = "UNKNOWN_ATTRIBUTION"

REVISION_INITIAL_PUBLISH = "INITIAL_PUBLISH"
REVISION_DATA_CORRECTION = "DATA_CORRECTION"
REVISION_POLICY_CHANGE = "POLICY_CHANGE"
REVISION_RECLASSIFICATION = "RECLASSIFICATION"
REVISION_BACKFILL = "BACKFILL"

EDGE_NEGATIVE = "NEGATIVE"
EDGE_WEAK = "WEAK_POSITIVE"
EDGE_QUALIFIED = "QUALIFIED_POSITIVE"
EDGE_STRONG = "STRONG_POSITIVE"

EXEC_UNKNOWN = "UNKNOWN"
EXEC_DEGRADED = "DEGRADED"
EXEC_ACCEPTABLE = "ACCEPTABLE"
EXEC_STRONG = "STRONG"

SAMPLE_INSUFFICIENT = "INSUFFICIENT"
SAMPLE_LIMITED = "LIMITED"
SAMPLE_SUFFICIENT = "SUFFICIENT"

DRIFT_DETERIORATING = "DETERIORATING"
DRIFT_STABLE = "STABLE"
DRIFT_IMPROVING = "IMPROVING"
DRIFT_UNKNOWN = "UNKNOWN"

STATE_MEASUREMENT_INVALID = "MEASUREMENT_INVALID"
STATE_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
STATE_QUALIFIED = "QUALIFIED"
STATE_WATCHLIST = "WATCHLIST"
STATE_THROTTLED = "THROTTLED"
STATE_DISABLED = "DISABLED"

RC_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE = "SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE"
RC_NATIVE_ENGINE_ATTRIBUTION_AMBIGUOUS = "SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_AMBIGUOUS"
RC_NATIVE_ENGINE_OUT_OF_SCOPE = "SLEEVE_EDGE_NATIVE_ENGINE_OUT_OF_SCOPE"
RC_ADOPTED_ENGINE_ATTRIBUTION_UNAVAILABLE = "SLEEVE_EDGE_ADOPTED_ENGINE_ATTRIBUTION_UNAVAILABLE"
RC_ADOPTED_ENGINE_OUT_OF_SCOPE = "SLEEVE_EDGE_ADOPTED_ENGINE_OUT_OF_SCOPE"
RC_UNKNOWN_ATTRIBUTION_EXCLUDED = "SLEEVE_EDGE_UNKNOWN_ATTRIBUTION_EXCLUDED"
RC_UNKNOWN_ATTRIBUTION_THRESHOLD_EXCEEDED = "SLEEVE_EDGE_UNKNOWN_ATTRIBUTION_THRESHOLD_EXCEEDED"
RC_MEASUREMENT_CLASS_UNSUPPORTED = "SLEEVE_EDGE_MEASUREMENT_CLASS_UNSUPPORTED"
RC_OPEN_POSITION_EXCLUDED = "SLEEVE_EDGE_OPEN_POSITION_EXCLUDED"
RC_CLOSED_STATE_REQUIRED = "SLEEVE_EDGE_CLOSED_STATE_REQUIRED"
RC_REQUIRED_FACT_REF_MISSING = "SLEEVE_EDGE_REQUIRED_FACT_REF_MISSING"
RC_CORE2_SUMMARY_MISSING = "SLEEVE_EDGE_CORE2_SUMMARY_MISSING"
RC_CORE2_SUMMARY_EMPTY = "SLEEVE_EDGE_CORE2_SUMMARY_EMPTY"
RC_DECISION_PRICE_UNAVAILABLE = "SLEEVE_EDGE_DECISION_PRICE_UNAVAILABLE"
RC_CAPITAL_AT_RISK_UNAVAILABLE = "SLEEVE_EDGE_CAPITAL_AT_RISK_UNAVAILABLE"
RC_BUDGET_TRUTH_UNAVAILABLE = "SLEEVE_EDGE_BUDGET_TRUTH_UNAVAILABLE"
RC_CAPITAL_EFFICIENCY_UNAVAILABLE = "SLEEVE_EDGE_CAPITAL_EFFICIENCY_UNAVAILABLE"
RC_DRIFT_WINDOW_INSUFFICIENT = "SLEEVE_EDGE_DRIFT_WINDOW_INSUFFICIENT"
RC_SAMPLE_INSUFFICIENT = "SLEEVE_EDGE_SAMPLE_INSUFFICIENT"
RC_SAMPLE_LIMITED = "SLEEVE_EDGE_SAMPLE_LIMITED"
RC_NEGATIVE_EXPECTANCY = "SLEEVE_EDGE_NEGATIVE_EXPECTANCY"
RC_WEAK_EXPECTANCY = "SLEEVE_EDGE_WEAK_EXPECTANCY"
RC_EXECUTION_DEGRADED = "SLEEVE_EDGE_EXECUTION_DEGRADED"
RC_DRIFT_DETERIORATING = "SLEEVE_EDGE_DRIFT_DETERIORATING"
RC_DRIFT_UNKNOWN = "SLEEVE_EDGE_DRIFT_UNKNOWN"
RC_MEASUREMENT_INVALID = "SLEEVE_EDGE_MEASUREMENT_INVALID"
RC_POLICY_VERSION_MISMATCH = "SLEEVE_EDGE_POLICY_VERSION_MISMATCH"
RC_CALCULATION_VERSION_MISMATCH = "SLEEVE_EDGE_CALCULATION_VERSION_MISMATCH"
RC_SNAPSHOT_INTEGRITY_FAILURE = "SLEEVE_EDGE_SNAPSHOT_INTEGRITY_FAILURE"
RC_POLICY_REGISTRY_INVALID = "SLEEVE_EDGE_POLICY_REGISTRY_INVALID"
RC_STATE_TRANSITION_PREFIX = "SLEEVE_EDGE_STATE_TRANSITION"
POSITION_FACT_LINEAGE_DIAGNOSTIC = (
    "Position facts have no order/engine lineage; sleeve grading requires order/fill attributed facts."
)


@dataclass(frozen=True)
class SleeveEdgeSnapshotMaterializationV1:
    truth_root: Path
    day_utc: str
    sleeve_id: str
    snapshot_id: str
    fact_ledger_path: Path
    snapshot_path: Path
    fact_ledger: Dict[str, Any]
    snapshot: Dict[str, Any]


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ValueError(f"MISSING_FILE:path={path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _git_sha_failclosed() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        value = out.decode("utf-8").strip()
    except Exception:
        value = "0" * 40
    return value if len(value) >= 7 else ("0" * 40)


def _coerce_utc_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        raise ValueError("UTC_TEXT_MISSING")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_sort_key(text: str) -> Tuple[datetime, str]:
    return (datetime.fromisoformat(_coerce_utc_text(text).replace("Z", "+00:00")), str(text))


def _parse_day(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise ValueError(f"BAD_DAY_UTC:{day!r}")
    return day


def _decimal_from_text(value: Any) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"INVALID_DECIMAL:{value!r}") from exc


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal("1")), "f")
    return format(normalized, "f")


def _unique_strings(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _write_validated_immutable_json(*, repo_root: Path, path: Path, payload: Dict[str, Any], schema_relpath: str) -> WriteResultV1:
    validate_against_repo_schema_v1(payload, repo_root, schema_relpath)
    return write_file_immutable_v1(path=path, data=canonical_json_bytes_v1(payload) + b"\n", create_dirs=True)


def _schema_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    obj = dict(payload)
    obj.pop("artifact_path", None)
    obj.pop("artifact_sha256", None)
    return obj


def _resolve_core2_summary_root(truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "reconciled_trade_state_summary_v1" / _parse_day(day_utc)).resolve()


def _load_latest_core2_summary(*, truth_root: Path, day_utc: str) -> Tuple[Path, Dict[str, Any]]:
    root = _resolve_core2_summary_root(truth_root, day_utc)
    if not root.exists() or not root.is_dir():
        raise ValueError(RC_CORE2_SUMMARY_MISSING)
    candidates: List[Tuple[Tuple[datetime, str], Path, Dict[str, Any]]] = []
    for path in root.glob("*/reconciled_trade_state_summary.v1.json"):
        try:
            payload = _read_json_obj(path)
            key = (_utc_sort_key(str(payload.get("evaluation_utc") or "1970-01-01T00:00:00Z"))[0], str(payload.get("materialization_set_id") or ""))
        except Exception:
            continue
        candidates.append((key, path.resolve(), payload))
    if not candidates:
        raise ValueError(RC_CORE2_SUMMARY_MISSING)
    candidates.sort(key=lambda item: item[0])
    _, path, payload = candidates[-1]
    trade_refs = payload.get("trade_refs")
    if not isinstance(trade_refs, list) or not trade_refs:
        raise ValueError(RC_CORE2_SUMMARY_EMPTY)
    return path, payload


def _load_execution_index(*, truth_root: Path, max_day_utc: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    root = (Path(truth_root).resolve() / "execution_stream_v1").resolve()
    indexes: Dict[str, Dict[str, List[Dict[str, Any]]]] = {"order_id": {}, "perm_id": {}}
    if not root.exists() or not root.is_dir():
        return indexes
    max_day = _parse_day(max_day_utc)
    for day_dir in sorted(path for path in root.iterdir() if path.is_dir() and path.name <= max_day):
        for path in sorted(day_dir.glob("*.execution_event_stream_record.v1.json")):
            try:
                payload = _read_json_obj(path)
            except Exception:
                continue
            meta = {
                "engine_id": str(payload.get("engine_id") or "").strip(),
                "source_intent_id": str(payload.get("source_intent_id") or "").strip(),
                "intent_sha256": str(payload.get("intent_sha256") or "").strip(),
                "artifact_path": str(path.resolve()),
                "artifact_sha256": _sha256_file(path.resolve()),
            }
            if not meta["engine_id"]:
                continue
            broker_ids = payload.get("broker_ids")
            if not isinstance(broker_ids, dict):
                continue
            order_id = broker_ids.get("order_id")
            perm_id = broker_ids.get("perm_id")
            if isinstance(order_id, int):
                indexes["order_id"].setdefault(str(order_id), []).append(meta)
            if isinstance(perm_id, int):
                indexes["perm_id"].setdefault(str(perm_id), []).append(meta)
    return indexes


def _resolve_engine_bridge(
    *,
    trade_identity_payload: Mapping[str, Any],
    state_payload: Mapping[str, Any],
    execution_index: Mapping[str, Mapping[str, List[Dict[str, Any]]]],
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    lineage = trade_identity_payload.get("lineage_attachment_refs")
    reasons: List[str] = []
    matches: List[Dict[str, Any]] = []
    if isinstance(lineage, dict):
        for key_name in ("order_ids", "perm_ids"):
            values = lineage.get(key_name)
            if not isinstance(values, list):
                continue
            index_key = "order_id" if key_name == "order_ids" else "perm_id"
            for raw in values:
                item = str(raw or "").strip()
                if not item:
                    continue
                matches.extend(list((execution_index.get(index_key) or {}).get(item, [])))
    if not matches:
        for fill in state_payload.get("incorporated_fills") or []:
            if not isinstance(fill, dict):
                continue
            order_id = str(fill.get("order_id") or "").strip()
            perm_id = str(fill.get("perm_id") or "").strip()
            if order_id:
                matches.extend(list((execution_index.get("order_id") or {}).get(order_id, [])))
            if perm_id:
                matches.extend(list((execution_index.get("perm_id") or {}).get(perm_id, [])))
    normalized: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for match in matches:
        engine_id = str(match.get("engine_id") or "").strip()
        artifact_path = str(match.get("artifact_path") or "").strip()
        if not engine_id or not artifact_path:
            continue
        normalized[(engine_id, artifact_path)] = {
            "engine_id": engine_id,
            "source_intent_id": str(match.get("source_intent_id") or "").strip(),
            "intent_sha256": str(match.get("intent_sha256") or "").strip(),
            "artifact_path": artifact_path,
            "artifact_sha256": str(match.get("artifact_sha256") or "").strip(),
        }
    if not normalized:
        direct_engine_candidates = _unique_strings(
            [
                str(trade_identity_payload.get("native_engine_id") or "").strip(),
                str(trade_identity_payload.get("engine_id") or "").strip(),
                str(trade_identity_payload.get("strategy_engine_id") or "").strip(),
                str(state_payload.get("native_engine_id") or "").strip(),
                str(state_payload.get("engine_id") or "").strip(),
                str(state_payload.get("strategy_engine_id") or "").strip(),
            ]
        )
        if len(direct_engine_candidates) > 1:
            reasons.append(RC_NATIVE_ENGINE_ATTRIBUTION_AMBIGUOUS)
            return None, reasons
        if len(direct_engine_candidates) == 1:
            source_intent_ids = _unique_strings(
                [
                    str(trade_identity_payload.get("source_intent_id") or "").strip(),
                    str(state_payload.get("source_intent_id") or "").strip(),
                ]
            )
            return {
                "engine_id": direct_engine_candidates[0],
                "source_intent_ids": source_intent_ids,
                "intent_sha256": _unique_strings(
                    [
                        str(trade_identity_payload.get("intent_sha256") or "").strip(),
                        str(state_payload.get("intent_sha256") or "").strip(),
                    ]
                ),
                "evidence_refs": [],
            }, reasons
        return None, reasons
    engines = sorted({item["engine_id"] for item in normalized.values()})
    if len(engines) != 1:
        reasons.append(RC_NATIVE_ENGINE_ATTRIBUTION_AMBIGUOUS)
        return None, reasons
    bridge = {
        "engine_id": engines[0],
        "source_intent_ids": _unique_strings(item["source_intent_id"] for item in normalized.values()),
        "intent_sha256": _unique_strings(item["intent_sha256"] for item in normalized.values()),
        "evidence_refs": [
            {"artifact_path": item["artifact_path"], "artifact_sha256": item["artifact_sha256"]}
            for item in sorted(normalized.values(), key=lambda entry: entry["artifact_path"])
        ],
    }
    return bridge, reasons


def _measurement_class(ownership_classification: str) -> str:
    value = str(ownership_classification or "").strip()
    if value == "CONSTELLATION_OWNED":
        return MEASUREMENT_NATIVE
    if value == "FOREIGN_MANUAL":
        return MEASUREMENT_ADOPTED
    return MEASUREMENT_UNKNOWN


def _normalize_revision_type(
    revision_type: str,
    *,
    previous_snapshot: Optional[Mapping[str, Any]],
    current_policy_version: str,
    current_fact_input_hash: str,
) -> str:
    value = str(revision_type or "").strip()
    allowed = {
        REVISION_INITIAL_PUBLISH,
        REVISION_DATA_CORRECTION,
        REVISION_POLICY_CHANGE,
        REVISION_RECLASSIFICATION,
        REVISION_BACKFILL,
    }
    if value:
        if value not in allowed:
            raise ValueError(f"SLEEVE_EDGE_REVISION_TYPE_INVALID:{value}")
        if previous_snapshot is None and value != REVISION_INITIAL_PUBLISH:
            raise ValueError(f"SLEEVE_EDGE_REVISION_TYPE_REQUIRES_PRIOR_SNAPSHOT:{value}")
        if previous_snapshot is not None and value == REVISION_INITIAL_PUBLISH:
            raise ValueError("SLEEVE_EDGE_REVISION_TYPE_INITIAL_REQUIRES_EMPTY_LINEAGE")
        return value
    if previous_snapshot is None:
        return REVISION_INITIAL_PUBLISH
    previous_policy_version = str(previous_snapshot.get("policy_version") or "").strip()
    previous_fact_input_hash = str(previous_snapshot.get("fact_input_hash") or "").strip()
    if previous_policy_version != current_policy_version:
        return REVISION_POLICY_CHANGE
    if previous_fact_input_hash != current_fact_input_hash:
        return REVISION_DATA_CORRECTION
    return REVISION_BACKFILL


def _normalize_revision_reason(revision_reason: str, *, revision_type: str) -> str:
    value = str(revision_reason or "").strip()
    if value:
        return value
    defaults = {
        REVISION_INITIAL_PUBLISH: "INITIAL_PUBLISH",
        REVISION_DATA_CORRECTION: "FACT_INPUT_HASH_CHANGED",
        REVISION_POLICY_CHANGE: "POLICY_VERSION_CHANGED",
        REVISION_RECLASSIFICATION: "MEASUREMENT_CLASS_RECLASSIFIED",
        REVISION_BACKFILL: "AS_OF_CONTEXT_BACKFILL",
    }
    return defaults[revision_type]


def _unknown_attribution_limit(policy: Mapping[str, Any]) -> int:
    thresholds = policy.get("unknown_attribution_thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("SLEEVE_EDGE_POLICY_UNKNOWN_ATTRIBUTION_THRESHOLDS_INVALID")
    unsupported_keys = sorted(set(str(key) for key in thresholds.keys()) - {"max_unknown_trade_count"})
    if unsupported_keys:
        raise ValueError(
            "SLEEVE_EDGE_POLICY_UNKNOWN_ATTRIBUTION_THRESHOLDS_UNSUPPORTED_KEYS:" + ",".join(unsupported_keys)
        )
    limit = int(thresholds.get("max_unknown_trade_count") or 0)
    if limit < 0:
        raise ValueError("SLEEVE_EDGE_POLICY_UNKNOWN_ATTRIBUTION_THRESHOLDS_NEGATIVE")
    return limit


def _allocator_allowed_calculation_versions(policy: Mapping[str, Any]) -> List[str]:
    compatibility = policy.get("allocator_compatibility")
    if not isinstance(compatibility, dict):
        raise ValueError("SLEEVE_EDGE_POLICY_ALLOCATOR_COMPATIBILITY_INVALID")
    values = compatibility.get("allowed_calculation_versions")
    if not isinstance(values, list) or not values:
        raise ValueError("SLEEVE_EDGE_POLICY_ALLOCATOR_ALLOWED_CALCULATION_VERSIONS_INVALID")
    allowed = _unique_strings(str(item) for item in values)
    if not allowed:
        raise ValueError("SLEEVE_EDGE_POLICY_ALLOCATOR_ALLOWED_CALCULATION_VERSIONS_EMPTY")
    return allowed


def _snapshot_lineage_compatible(
    snapshot: Mapping[str, Any],
    *,
    policy_version: str,
    calculation_version: str,
) -> bool:
    return (
        str(snapshot.get("policy_version") or "").strip() == policy_version
        and str(snapshot.get("calculation_version") or "").strip() == calculation_version
    )


def _lineage_restart_reason(
    previous_snapshot: Mapping[str, Any],
    *,
    policy_version: str,
    calculation_version: str,
) -> str:
    previous_policy_version = str(previous_snapshot.get("policy_version") or "").strip()
    previous_calculation_version = str(previous_snapshot.get("calculation_version") or "").strip()
    if previous_policy_version != policy_version:
        return f"LINEAGE_RESET_POLICY_VERSION_BOUNDARY:{previous_policy_version}->{policy_version}"
    if previous_calculation_version != calculation_version:
        return f"LINEAGE_RESET_CALCULATION_VERSION_BOUNDARY:{previous_calculation_version}->{calculation_version}"
    return "INITIAL_PUBLISH"


def _signed_fill_quantity(fill: Mapping[str, Any]) -> Decimal:
    quantity = _decimal_from_text(fill.get("fill_quantity"))
    side = str(fill.get("side") or "").strip().upper()
    if side in {"BUY", "BOT"}:
        return quantity
    if side in {"SELL", "SLD"}:
        return Decimal("0") - quantity
    raise ValueError(f"UNSUPPORTED_FILL_SIDE:{side}")


def _fill_sort_key(fill: Mapping[str, Any]) -> Tuple[datetime, str, str]:
    return (
        _utc_sort_key(str(fill.get("observed_utc") or "1970-01-01T00:00:00Z"))[0],
        str(fill.get("execution_id") or ""),
        str(fill.get("order_id") or ""),
    )


def _realized_trade_metrics(state_payload: Mapping[str, Any]) -> Dict[str, Any]:
    fills = sorted(
        [fill for fill in state_payload.get("incorporated_fills") or [] if isinstance(fill, dict)],
        key=_fill_sort_key,
    )
    realized_gross = Decimal("0")
    fees = Decimal("0")
    position_qty = Decimal("0")
    cost_basis = Decimal("0")
    first_fill_utc = ""
    last_fill_utc = ""
    for fill in fills:
        observed = _coerce_utc_text(str(fill.get("observed_utc") or "1970-01-01T00:00:00Z"))
        if not first_fill_utc:
            first_fill_utc = observed
        last_fill_utc = observed
        fee = _decimal_from_text(fill.get("commission"))
        fees += fee
        signed_qty = _signed_fill_quantity(fill)
        fill_price = _decimal_from_text(fill.get("fill_price"))
        fill_qty = abs(signed_qty)
        if position_qty == 0 or (position_qty > 0 and signed_qty > 0) or (position_qty < 0 and signed_qty < 0):
            total_qty = abs(position_qty) + fill_qty
            if total_qty > 0:
                weighted_cost = (abs(position_qty) * cost_basis) + (fill_qty * fill_price)
                cost_basis = weighted_cost / total_qty
            position_qty += signed_qty
            continue
        closing_qty = min(abs(position_qty), fill_qty)
        if position_qty > 0 and signed_qty < 0:
            realized_gross += (fill_price - cost_basis) * closing_qty
        elif position_qty < 0 and signed_qty > 0:
            realized_gross += (cost_basis - fill_price) * closing_qty
        position_qty += signed_qty
        if position_qty == 0:
            cost_basis = Decimal("0")
        elif abs(signed_qty) > closing_qty:
            cost_basis = fill_price
    current_quantity = _decimal_from_text(state_payload.get("current_quantity"))
    return {
        "fill_count": len(fills),
        "first_fill_utc": first_fill_utc,
        "last_fill_utc": last_fill_utc,
        "total_fees": fees,
        "realized_gross_pnl": realized_gross,
        "realized_net_pnl": realized_gross - fees,
        "final_position_qty": position_qty,
        "current_quantity": current_quantity,
    }


def _compute_drawdown(trades: Sequence[Mapping[str, Any]]) -> Decimal:
    running = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    ordered = sorted(trades, key=lambda item: _utc_sort_key(str(item.get("close_utc") or "1970-01-01T00:00:00Z")))
    for trade in ordered:
        running += _decimal_from_text(trade.get("net_pnl"))
        if running > peak:
            peak = running
        drawdown = peak - running
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _optional_metric(*, reason_codes: Iterable[str], value: Optional[Decimal] = None) -> Dict[str, Any]:
    codes = _unique_strings(reason_codes)
    if value is None:
        return {"status": "UNAVAILABLE", "value": None, "reason_codes": codes}
    return {"status": "AVAILABLE", "value": _decimal_text(value), "reason_codes": codes}


def _unavailable_metrics(metrics: Mapping[str, Any]) -> List[str]:
    out: List[str] = []
    for name, value in metrics.items():
        if isinstance(value, dict) and str(value.get("status") or "").strip() == "UNAVAILABLE":
            out.append(str(name))
    out.sort()
    return out


def _require_policy_dict(obj: Mapping[str, Any], key: str, *, path: Path) -> Mapping[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:{key.upper()}_INVALID:path={path}")
    return value


def _require_policy_nonempty_string(obj: Mapping[str, Any], key: str, *, path: Path) -> str:
    value = str(obj.get(key) or "").strip()
    if not value:
        raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:{key.upper()}_MISSING:path={path}")
    return value


def _validate_sleeve_edge_policy_v1(obj: Mapping[str, Any], *, path: Path) -> None:
    _require_policy_nonempty_string(obj, "policy_version", path=path)
    metric_window = _metric_window(obj)
    if int(metric_window["recent_trade_count"]) <= 0 or int(metric_window["baseline_trade_count"]) <= 0:
        raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:METRIC_WINDOWS_NONPOSITIVE:path={path}")

    sample_sufficiency = _require_policy_dict(obj, "sample_sufficiency", path=path)
    limited_min = int(sample_sufficiency.get("limited_min_sample_count") or 0)
    sufficient_min = int(sample_sufficiency.get("sufficient_min_sample_count") or 0)
    if limited_min <= 0 or sufficient_min <= 0:
        raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:SAMPLE_SUFFICIENCY_NONPOSITIVE:path={path}")
    if sufficient_min < limited_min:
        raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:SAMPLE_SUFFICIENCY_ORDER_INVALID:path={path}")

    _unknown_attribution_limit(obj)

    edge_thresholds = _require_policy_dict(obj, "edge_band_thresholds", path=path)
    for key in (
        "weak_positive_min_net_expectancy",
        "qualified_positive_min_net_expectancy",
        "strong_positive_min_net_expectancy",
    ):
        _decimal_from_text(edge_thresholds.get(key))

    drift_thresholds = _require_policy_dict(obj, "drift_band_thresholds", path=path)
    for key in ("improving_min_delta_net_expectancy", "deteriorating_max_delta_net_expectancy"):
        _decimal_from_text(drift_thresholds.get(key))

    _allocator_allowed_calculation_versions(obj)

    allocator_actions = _require_policy_dict(obj, "allocator_actions", path=path)
    for state in (
        STATE_MEASUREMENT_INVALID,
        STATE_INSUFFICIENT_DATA,
        STATE_QUALIFIED,
        STATE_WATCHLIST,
        STATE_THROTTLED,
        STATE_DISABLED,
    ):
        action = allocator_actions.get(state)
        if not isinstance(action, dict):
            raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:ALLOCATOR_ACTION_MISSING:state={state}:path={path}")
        raw_multiplier_bp = action.get("capital_multiplier_bp")
        if raw_multiplier_bp is None:
            raise ValueError(f"{RC_POLICY_REGISTRY_INVALID}:ALLOCATOR_ACTION_MULTIPLIER_MISSING:state={state}:path={path}")
        multiplier_bp = int(raw_multiplier_bp)
        if multiplier_bp < 0 or multiplier_bp > 10000:
            raise ValueError(
                f"{RC_POLICY_REGISTRY_INVALID}:ALLOCATOR_ACTION_MULTIPLIER_INVALID:state={state}:path={path}"
            )
        _require_policy_nonempty_string(action, "reason_code", path=path)


def load_sleeve_edge_policy_v1(repo_root: Path = REPO_ROOT) -> Dict[str, Any]:
    path = (Path(repo_root).resolve() / "governance" / "02_REGISTRIES" / "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json").resolve()
    obj = _read_json_obj(path)
    if str(obj.get("schema_id") or "").strip() != "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1":
        raise ValueError(f"SLEEVE_EDGE_POLICY_SCHEMA_ID_INVALID:path={path}")
    if int(obj.get("schema_version") or 0) != 1:
        raise ValueError(f"SLEEVE_EDGE_POLICY_SCHEMA_VERSION_INVALID:path={path}")
    _validate_sleeve_edge_policy_v1(obj, path=path)
    return obj


def allocator_action_from_qualification_v1(policy: Mapping[str, Any], qualification: Mapping[str, Any]) -> Dict[str, Any]:
    state = str(qualification.get("qualification_state") or "").strip()
    allocator_actions = policy.get("allocator_actions")
    if not isinstance(allocator_actions, dict) or state not in allocator_actions:
        raise ValueError(f"SLEEVE_EDGE_ALLOCATOR_ACTION_MISSING:state={state}")
    action = allocator_actions[state]
    if not isinstance(action, dict):
        raise ValueError(f"SLEEVE_EDGE_ALLOCATOR_ACTION_INVALID:state={state}")
    multiplier_bp = int(action.get("capital_multiplier_bp") or 0)
    reason_code = str(action.get("reason_code") or "").strip()
    return {
        "qualification_state": state,
        "capital_multiplier_bp": multiplier_bp,
        "reason_code": reason_code,
    }


def _policy_version(policy: Mapping[str, Any]) -> str:
    return str(policy.get("policy_version") or "v1").strip() or "v1"


def _metric_window(policy: Mapping[str, Any]) -> Dict[str, Any]:
    metric_windows = policy.get("metric_windows")
    if not isinstance(metric_windows, dict):
        raise ValueError("SLEEVE_EDGE_POLICY_METRIC_WINDOWS_INVALID")
    recent = int(metric_windows.get("recent_trade_count") or 0)
    baseline = int(metric_windows.get("baseline_trade_count") or 0)
    if recent <= 0 or baseline <= 0:
        raise ValueError("SLEEVE_EDGE_POLICY_METRIC_WINDOWS_NONPOSITIVE")
    return {
        "sample_basis": "CLOSED_TRADES_ONLY",
        "recent_trade_count": recent,
        "baseline_trade_count": baseline,
    }


def _fact_input_descriptor_from_trade_rows(
    *,
    core2_summary_path: Path,
    core2_summary_sha256: str,
    materialization_set_id: str,
    sleeve_id: str,
    engine_ids: Sequence[str],
    as_of_ts: str,
    trade_rows: Sequence[Mapping[str, Any]],
    lineage_requirement_diagnostic: Optional[str],
) -> Dict[str, Any]:
    descriptor = {
        "core2_summary_path": str(core2_summary_path),
        "core2_summary_sha256": str(core2_summary_sha256),
        "materialization_set_id": materialization_set_id,
        "sleeve_id": sleeve_id,
        "engine_ids": sorted(str(item).strip() for item in engine_ids if str(item).strip()),
        "as_of_ts": as_of_ts,
        "trade_rows": [
            {
                "trade_identity_id": str(row.get("trade_identity_id") or ""),
                "trade_identity_sha256": str(row.get("trade_identity_sha256") or ""),
                "incorporated_state_sha256": str(row.get("incorporated_state_sha256") or ""),
                "reconciliation_provenance_sha256": str(row.get("reconciliation_provenance_sha256") or ""),
                "execution_evidence_refs": list(row.get("execution_evidence_refs") or []),
            }
            for row in sorted(trade_rows, key=lambda item: str(item.get("trade_identity_id") or ""))
        ],
    }
    diagnostic_text = str(lineage_requirement_diagnostic or "").strip()
    if diagnostic_text:
        descriptor["lineage_requirement_diagnostic"] = diagnostic_text
    return descriptor


def _build_fact_ledger(
    *,
    core2_summary_path: Path,
    core2_summary_payload: Mapping[str, Any],
    execution_index: Mapping[str, Mapping[str, List[Dict[str, Any]]]],
    sleeve_id: str,
    strategy_family: str,
    engine_ids: Sequence[str],
    qualification_policy: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    trade_rows: List[Dict[str, Any]] = []
    matched_execution_refs: List[Dict[str, str]] = []
    invalidity_reasons: List[str] = []
    included_trade_ids: List[str] = []
    excluded_trade_ids: List[str] = []
    excluded_trade_details: List[Dict[str, Any]] = []
    unattributed_samples: List[Dict[str, Any]] = []
    fields_present: set[str] = set()
    trade_refs = core2_summary_payload.get("trade_refs") or []
    if not isinstance(trade_refs, list):
        raise ValueError(RC_CORE2_SUMMARY_EMPTY)
    target_engine_ids = {str(item).strip() for item in engine_ids if str(item).strip()}
    if not target_engine_ids:
        raise ValueError("SLEEVE_EDGE_ENGINE_IDS_EMPTY")
    for ref in trade_refs:
        if not isinstance(ref, dict):
            continue
        state_path = Path(str(ref.get("incorporated_state_path") or "")).resolve()
        provenance_path = Path(str(ref.get("provenance_path") or "")).resolve()
        if not state_path.exists() or not provenance_path.exists():
            invalidity_reasons.append(RC_REQUIRED_FACT_REF_MISSING)
            continue
        state_payload = _read_json_obj(state_path)
        provenance_payload = _read_json_obj(provenance_path)
        trade_identity_ref = state_payload.get("trade_identity_ref")
        if not isinstance(trade_identity_ref, dict):
            invalidity_reasons.append(RC_REQUIRED_FACT_REF_MISSING)
            continue
        trade_identity_path = Path(str(trade_identity_ref.get("artifact_path") or "")).resolve()
        if not trade_identity_path.exists():
            invalidity_reasons.append(RC_REQUIRED_FACT_REF_MISSING)
            continue
        trade_identity_payload = _read_json_obj(trade_identity_path)
        trade_identity_id = str(trade_identity_payload.get("trade_identity_id") or "")
        ownership_classification = str(state_payload.get("ownership_classification") or "")
        measurement_class = _measurement_class(ownership_classification)
        bridge, bridge_reasons = _resolve_engine_bridge(
            trade_identity_payload=trade_identity_payload,
            state_payload=state_payload,
            execution_index=execution_index,
        )
        continuity = trade_identity_payload.get("open_close_continuity")
        continuity_status = str(continuity.get("continuity_status") or "") if isinstance(continuity, dict) else ""
        realized = _realized_trade_metrics(state_payload)
        exclusion_reason_codes = list(bridge_reasons)
        included_in_metrics = True
        engine_id = ""
        source_intent_ids: List[str] = []
        evidence_refs: List[Dict[str, str]] = []
        if measurement_class == MEASUREMENT_UNKNOWN:
            included_in_metrics = False
            exclusion_reason_codes.append(RC_UNKNOWN_ATTRIBUTION_EXCLUDED)
        elif bridge is None:
            included_in_metrics = False
            if measurement_class == MEASUREMENT_NATIVE:
                exclusion_reason_codes.append(RC_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE)
                invalidity_reasons.append(RC_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE)
            else:
                exclusion_reason_codes.append(RC_ADOPTED_ENGINE_ATTRIBUTION_UNAVAILABLE)
        else:
            engine_id = str(bridge.get("engine_id") or "").strip()
            source_intent_ids = list(bridge.get("source_intent_ids") or [])
            evidence_refs = list(bridge.get("evidence_refs") or [])
            matched_execution_refs.extend(evidence_refs)
            if engine_id not in target_engine_ids:
                included_in_metrics = False
                if measurement_class == MEASUREMENT_NATIVE:
                    exclusion_reason_codes.append(RC_NATIVE_ENGINE_OUT_OF_SCOPE)
                else:
                    exclusion_reason_codes.append(RC_ADOPTED_ENGINE_OUT_OF_SCOPE)
        if continuity_status != "CLOSED":
            included_in_metrics = False
            exclusion_reason_codes.append(RC_OPEN_POSITION_EXCLUDED)
        if realized["current_quantity"] != 0:
            included_in_metrics = False
            exclusion_reason_codes.append(RC_CLOSED_STATE_REQUIRED)
        trade_row = {
            "trade_identity_id": trade_identity_id,
            "measurement_class": measurement_class,
            "ownership_classification": ownership_classification,
            "continuity_status": continuity_status,
            "engine_id": engine_id,
            "source_intent_ids": source_intent_ids,
            "trade_identity_path": str(trade_identity_path),
            "trade_identity_sha256": _sha256_file(trade_identity_path),
            "incorporated_state_path": str(state_path),
            "incorporated_state_sha256": _sha256_file(state_path),
            "reconciliation_provenance_path": str(provenance_path),
            "reconciliation_provenance_sha256": _sha256_file(provenance_path),
            "lineage_attachment_refs": trade_identity_payload.get("lineage_attachment_refs"),
            "incorporated_fact_refs": provenance_payload.get("incorporated_fact_refs") or [],
            "ignored_fact_refs": provenance_payload.get("ignored_fact_refs") or [],
            "blocked_fact_refs": provenance_payload.get("blocked_fact_refs") or [],
            "current_quantity": _decimal_text(realized["current_quantity"]),
            "average_cost": str(state_payload.get("average_cost") or ""),
            "side": str(state_payload.get("side") or ""),
            "fill_count": int(realized["fill_count"]),
            "first_fill_utc": str(realized["first_fill_utc"]),
            "last_fill_utc": str(realized["last_fill_utc"]),
            "total_fees": _decimal_text(realized["total_fees"]),
            "included_in_metrics": bool(included_in_metrics),
            "exclusion_reason_codes": _unique_strings(exclusion_reason_codes),
            "execution_evidence_refs": evidence_refs,
        }
        if measurement_class == MEASUREMENT_NATIVE and not engine_id:
            lineage = trade_identity_payload.get("lineage_attachment_refs")
            lineage_values = lineage if isinstance(lineage, dict) else {}
            lineage_nonempty = [
                key
                for key in ("order_ids", "perm_ids", "execution_ids", "fact_record_ids")
                if isinstance(lineage_values.get(key), list) and bool(lineage_values.get(key))
            ]
            fill_keys_present: List[str] = []
            fill_ids = state_payload.get("incorporated_fills")
            if isinstance(fill_ids, list):
                for key in ("order_id", "perm_id", "execution_id"):
                    if any(str((fill or {}).get(key) or "").strip() for fill in fill_ids if isinstance(fill, dict)):
                        fill_keys_present.append(key)
            trade_identity_nonempty = [
                key
                for key in ("native_engine_id", "engine_id", "strategy_engine_id", "source_intent_id", "intent_sha256", "sleeve_id")
                if str(trade_identity_payload.get(key) or "").strip()
            ]
            state_nonempty = [
                key
                for key in ("native_engine_id", "engine_id", "strategy_engine_id", "source_intent_id", "intent_sha256")
                if str(state_payload.get(key) or "").strip()
            ]
            for key in trade_identity_nonempty:
                fields_present.add(f"trade_identity.{key}")
            for key in state_nonempty:
                fields_present.add(f"incorporated_state.{key}")
            for key in lineage_nonempty:
                fields_present.add(f"lineage_attachment_refs.{key}")
            for key in fill_keys_present:
                fields_present.add(f"incorporated_fills.{key}")
            if len(unattributed_samples) < 3:
                unattributed_samples.append(
                    {
                        "trade_identity_id": trade_identity_id,
                        "measurement_class": measurement_class,
                        "continuity_status": continuity_status,
                        "current_quantity": _decimal_text(realized["current_quantity"]),
                        "fields_present": {
                            "trade_identity": trade_identity_nonempty,
                            "incorporated_state": state_nonempty,
                            "lineage_attachment_refs": lineage_nonempty,
                            "incorporated_fills": fill_keys_present,
                        },
                    }
                )
        trade_rows.append(trade_row)
        if included_in_metrics:
            included_trade_ids.append(trade_identity_id)
        else:
            excluded_trade_ids.append(trade_identity_id)
            excluded_trade_details.append(
                {
                    "trade_identity_id": trade_identity_id,
                    "reason_codes": trade_row["exclusion_reason_codes"],
                }
            )
    as_of_ts = _coerce_utc_text(str(core2_summary_payload.get("evaluation_utc") or "1970-01-01T00:00:00Z"))
    materialization_set_id = str(core2_summary_payload.get("materialization_set_id") or "")
    attributed_fact_count = sum(1 for row in trade_rows if str(row.get("engine_id") or "").strip())
    unattributed_fact_count = sum(
        1
        for row in trade_rows
        if str(row.get("measurement_class") or "") == MEASUREMENT_NATIVE
        and not str(row.get("engine_id") or "").strip()
    )
    lineage_signal_fields = {
        "lineage_attachment_refs.order_ids",
        "lineage_attachment_refs.perm_ids",
        "lineage_attachment_refs.execution_ids",
        "incorporated_fills.order_id",
        "incorporated_fills.perm_id",
        "incorporated_fills.execution_id",
        "trade_identity.native_engine_id",
        "trade_identity.engine_id",
        "trade_identity.strategy_engine_id",
        "incorporated_state.native_engine_id",
        "incorporated_state.engine_id",
        "incorporated_state.strategy_engine_id",
    }
    lineage_requirement_diagnostic = ""
    if unattributed_fact_count > 0 and not any(field in fields_present for field in lineage_signal_fields):
        lineage_requirement_diagnostic = POSITION_FACT_LINEAGE_DIAGNOSTIC
    fact_input_descriptor = _fact_input_descriptor_from_trade_rows(
        core2_summary_path=core2_summary_path,
        core2_summary_sha256=_sha256_file(core2_summary_path),
        materialization_set_id=materialization_set_id,
        sleeve_id=sleeve_id,
        engine_ids=sorted(target_engine_ids),
        as_of_ts=as_of_ts,
        trade_rows=trade_rows,
        lineage_requirement_diagnostic=(
            lineage_requirement_diagnostic if str(lineage_requirement_diagnostic or "").strip() else None
        ),
    )
    fact_input_hash = _sha256_bytes(canonical_json_bytes_v1(fact_input_descriptor))
    producer = {"repo": REPO_ROOT.name, "git_sha": _git_sha_failclosed(), "module": "constellation_2/common/sleeve_edge_measurement_v1.py"}
    fact_ledger = {
        "schema_id": "C2_SLEEVE_EDGE_FACT_LEDGER_V1",
        "schema_version": "v1",
        "produced_utc": as_of_ts,
        "day_utc": _parse_day(str(core2_summary_payload.get("day_utc") or "")),
        "as_of_ts": as_of_ts,
        "sleeve_id": sleeve_id,
        "strategy_family": strategy_family,
        "source_execution_sleeve_id": str(core2_summary_payload.get("sleeve_id") or ""),
        "source_execution_root_path": str(core2_summary_payload.get("execution_root_path") or ""),
        "calculation_version": CALCULATION_VERSION,
        "core2_materialization_set_id": materialization_set_id,
        "engine_ids": sorted(target_engine_ids),
        "fact_input_hash": fact_input_hash,
        "input_manifest": [
            {
                "type": "reconciled_trade_state_summary",
                "path": str(core2_summary_path),
                "sha256": _sha256_file(core2_summary_path),
                "day_utc": _parse_day(str(core2_summary_payload.get("day_utc") or "")),
                "producer": "reconciled_trade_state_summary_v1",
            }
        ],
        "trade_facts": sorted(trade_rows, key=lambda item: item["trade_identity_id"]),
        "included_trade_ids": sorted(included_trade_ids),
        "excluded_trade_ids": sorted(excluded_trade_ids),
        "exclusion_details": sorted(excluded_trade_details, key=lambda item: item["trade_identity_id"]),
        "invalidity_reasons": _unique_strings(invalidity_reasons),
        "attribution_diagnostics": {
            "source_fact_count": len(trade_rows),
            "attributed_fact_count": attributed_fact_count,
            "unattributed_fact_count": unattributed_fact_count,
            "expected_attribution_field": "engine_id via execution stream join on lineage_attachment_refs.order_ids/perm_ids or incorporated_fills.order_id/perm_id",
            "fields_actually_present": sorted(fields_present),
            "sample_records": unattributed_samples,
            "upstream_artifact_path": str(core2_summary_path),
            "lineage_requirement_diagnostic": lineage_requirement_diagnostic,
        },
        "producer": producer,
    }
    return fact_ledger, {
        "matched_execution_refs": sorted(
            {(ref["artifact_path"], ref["artifact_sha256"]) for ref in matched_execution_refs},
            key=lambda item: item[0],
        ),
        "fact_input_hash": fact_input_hash,
    }


def _fact_input_descriptor_from_fact_ledger(fact_ledger: Mapping[str, Any]) -> Dict[str, Any]:
    input_manifest = fact_ledger.get("input_manifest")
    if not isinstance(input_manifest, list):
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_INPUT_MANIFEST_INVALID")
    summary_manifest = next(
        (
            item
            for item in input_manifest
            if isinstance(item, dict) and str(item.get("type") or "").strip() == "reconciled_trade_state_summary"
        ),
        None,
    )
    if not isinstance(summary_manifest, dict):
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_SUMMARY_MANIFEST_MISSING")
    trade_facts = fact_ledger.get("trade_facts")
    if not isinstance(trade_facts, list):
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_TRADE_FACTS_INVALID")
    return _fact_input_descriptor_from_trade_rows(
        core2_summary_path=Path(str(summary_manifest.get("path") or "")),
        core2_summary_sha256=str(summary_manifest.get("sha256") or ""),
        materialization_set_id=str(fact_ledger.get("core2_materialization_set_id") or ""),
        sleeve_id=str(fact_ledger.get("sleeve_id") or ""),
        engine_ids=list(fact_ledger.get("engine_ids") or []),
        as_of_ts=str(fact_ledger.get("as_of_ts") or ""),
        trade_rows=trade_facts,
        lineage_requirement_diagnostic=(
            str((((fact_ledger.get("attribution_diagnostics") or {}).get("lineage_requirement_diagnostic")) or "")).strip()
            or None
        ),
    )


def _fact_input_hash_from_fact_ledger(fact_ledger: Mapping[str, Any]) -> str:
    return _sha256_bytes(canonical_json_bytes_v1(_fact_input_descriptor_from_fact_ledger(fact_ledger)))


def _build_trade_metric_rows(fact_ledger: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    trades: List[Dict[str, Any]] = []
    reasons: List[str] = []
    for row in fact_ledger.get("trade_facts") or []:
        if not isinstance(row, dict):
            continue
        if row.get("included_in_metrics") is not True:
            continue
        state_path = Path(str(row.get("incorporated_state_path") or "")).resolve()
        if not state_path.exists():
            reasons.append(RC_REQUIRED_FACT_REF_MISSING)
            continue
        state_payload = _read_json_obj(state_path)
        realized = _realized_trade_metrics(state_payload)
        trades.append(
            {
                "trade_identity_id": str(row.get("trade_identity_id") or ""),
                "measurement_class": str(row.get("measurement_class") or ""),
                "engine_id": str(row.get("engine_id") or ""),
                "close_utc": str(realized["last_fill_utc"] or ""),
                "gross_pnl": _decimal_text(realized["realized_gross_pnl"]),
                "net_pnl": _decimal_text(realized["realized_net_pnl"]),
                "fees": _decimal_text(realized["total_fees"]),
            }
        )
    return trades, _unique_strings(reasons)


def _compute_metrics(*, fact_ledger: Mapping[str, Any], qualification_policy: Mapping[str, Any]) -> Dict[str, Any]:
    trades, extra_invalidity = _build_trade_metric_rows(fact_ledger)
    native_trades = [trade for trade in trades if trade["measurement_class"] == MEASUREMENT_NATIVE]
    adopted_trades = [trade for trade in trades if trade["measurement_class"] == MEASUREMENT_ADOPTED]
    invalidity_reasons = _unique_strings(list(fact_ledger.get("invalidity_reasons") or []) + list(extra_invalidity))
    sample_count = len(native_trades)
    native_net_pnl = sum((_decimal_from_text(trade["net_pnl"]) for trade in native_trades), Decimal("0"))
    native_gross_pnl = sum((_decimal_from_text(trade["gross_pnl"]) for trade in native_trades), Decimal("0"))
    adopted_net_pnl = sum((_decimal_from_text(trade["net_pnl"]) for trade in adopted_trades), Decimal("0"))
    fee_drag = sum((_decimal_from_text(trade["fees"]) for trade in trades), Decimal("0"))
    metric_window = _metric_window(qualification_policy)
    recent_n = int(metric_window["recent_trade_count"])
    baseline_n = int(metric_window["baseline_trade_count"])
    ordered_native = sorted(native_trades, key=lambda item: _utc_sort_key(item["close_utc"]))
    recent_trades = ordered_native[-recent_n:] if recent_n > 0 else []
    baseline_trades = ordered_native[-(recent_n + baseline_n):-recent_n] if len(ordered_native) > recent_n else []
    recent_expectancy = (
        sum((_decimal_from_text(trade["net_pnl"]) for trade in recent_trades), Decimal("0")) / Decimal(len(recent_trades))
        if recent_trades
        else None
    )
    baseline_expectancy = (
        sum((_decimal_from_text(trade["net_pnl"]) for trade in baseline_trades), Decimal("0")) / Decimal(len(baseline_trades))
        if baseline_trades
        else None
    )
    drift_value = (recent_expectancy - baseline_expectancy) if recent_expectancy is not None and baseline_expectancy is not None else None
    execution_data_completeness = {
        "fill_price_complete": True,
        "fee_complete": True,
        "decision_price_complete": False,
        "state": "PARTIAL",
    }
    return {
        "native_trade_count": len(native_trades),
        "native_net_pnl": _decimal_text(native_net_pnl),
        "native_gross_pnl": _decimal_text(native_gross_pnl),
        "native_net_expectancy": _decimal_text(native_net_pnl / Decimal(len(native_trades))) if native_trades else "0",
        "native_expectancy_per_unit_risk": _optional_metric(reason_codes=[RC_CAPITAL_AT_RISK_UNAVAILABLE]),
        "native_realized_drawdown": _decimal_text(_compute_drawdown(native_trades)),
        "native_capital_efficiency": _optional_metric(reason_codes=[RC_CAPITAL_EFFICIENCY_UNAVAILABLE]),
        "native_budget_utilization": _optional_metric(reason_codes=[RC_BUDGET_TRUTH_UNAVAILABLE]),
        "native_recent_vs_baseline_drift": _optional_metric(reason_codes=[RC_DRIFT_WINDOW_INSUFFICIENT] if drift_value is None else [], value=drift_value),
        "adopted_trade_count": len(adopted_trades),
        "adopted_net_pnl": _decimal_text(adopted_net_pnl),
        "adopted_management_expectancy": _decimal_text(adopted_net_pnl / Decimal(len(adopted_trades))) if adopted_trades else "0",
        "adopted_drawdown": _decimal_text(_compute_drawdown(adopted_trades)),
        "adopted_capital_efficiency": _optional_metric(reason_codes=[RC_CAPITAL_EFFICIENCY_UNAVAILABLE]),
        "fee_drag": _decimal_text(fee_drag),
        "measured_slippage_drag": _optional_metric(reason_codes=[RC_DECISION_PRICE_UNAVAILABLE]),
        "execution_data_completeness": execution_data_completeness,
        "unknown_attribution_count": sum(
            1
            for row in fact_ledger.get("trade_facts") or []
            if isinstance(row, dict) and str(row.get("measurement_class") or "") == MEASUREMENT_UNKNOWN
        ),
        "sample_count": sample_count,
        "window_coverage": {
            "recent_required_trade_count": recent_n,
            "recent_available_trade_count": len(recent_trades),
            "baseline_required_trade_count": baseline_n,
            "baseline_available_trade_count": len(baseline_trades),
        },
        "required_inputs_complete": not bool(invalidity_reasons),
        "invalidity_reasons": invalidity_reasons,
    }


def _edge_band(metrics: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    thresholds = policy.get("edge_band_thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("SLEEVE_EDGE_POLICY_EDGE_THRESHOLDS_INVALID")
    expectancy = _decimal_from_text(metrics.get("native_net_expectancy"))
    weak_min = _decimal_from_text(thresholds.get("weak_positive_min_net_expectancy"))
    qualified_min = _decimal_from_text(thresholds.get("qualified_positive_min_net_expectancy"))
    strong_min = _decimal_from_text(thresholds.get("strong_positive_min_net_expectancy"))
    if expectancy < weak_min:
        return EDGE_NEGATIVE
    if expectancy < qualified_min:
        return EDGE_WEAK
    if expectancy < strong_min:
        return EDGE_QUALIFIED
    return EDGE_STRONG


def _sample_band(metrics: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    thresholds = policy.get("sample_sufficiency")
    if not isinstance(thresholds, dict):
        raise ValueError("SLEEVE_EDGE_POLICY_SAMPLE_THRESHOLDS_INVALID")
    sample_count = int(metrics.get("sample_count") or 0)
    limited_min = int(thresholds.get("limited_min_sample_count") or 0)
    sufficient_min = int(thresholds.get("sufficient_min_sample_count") or 0)
    if sample_count < limited_min:
        return SAMPLE_INSUFFICIENT
    if sample_count < sufficient_min:
        return SAMPLE_LIMITED
    return SAMPLE_SUFFICIENT


def _execution_band(metrics: Mapping[str, Any]) -> str:
    completeness = metrics.get("execution_data_completeness")
    if not isinstance(completeness, dict):
        return EXEC_UNKNOWN
    if not bool(completeness.get("fill_price_complete")) or not bool(completeness.get("fee_complete")):
        return EXEC_DEGRADED
    if bool(completeness.get("decision_price_complete")):
        return EXEC_STRONG
    if int(metrics.get("native_trade_count") or 0) <= 0 and int(metrics.get("adopted_trade_count") or 0) <= 0:
        return EXEC_UNKNOWN
    return EXEC_ACCEPTABLE


def _drift_band(metrics: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    drift_metric = metrics.get("native_recent_vs_baseline_drift")
    if not isinstance(drift_metric, dict):
        return DRIFT_UNKNOWN
    if str(drift_metric.get("status") or "") != "AVAILABLE":
        return DRIFT_UNKNOWN
    thresholds = policy.get("drift_band_thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("SLEEVE_EDGE_POLICY_DRIFT_THRESHOLDS_INVALID")
    delta = _decimal_from_text(drift_metric.get("value"))
    improving_min = _decimal_from_text(thresholds.get("improving_min_delta_net_expectancy"))
    deteriorating_max = _decimal_from_text(thresholds.get("deteriorating_max_delta_net_expectancy"))
    if delta >= improving_min:
        return DRIFT_IMPROVING
    if delta <= deteriorating_max:
        return DRIFT_DETERIORATING
    return DRIFT_STABLE


def _qualify(metrics: Mapping[str, Any], policy: Mapping[str, Any]) -> Dict[str, Any]:
    reasons: List[str] = []
    edge_band = _edge_band(metrics, policy)
    sample_band = _sample_band(metrics, policy)
    execution_band = _execution_band(metrics)
    drift_band = _drift_band(metrics, policy)
    unknown_attribution_limit = _unknown_attribution_limit(policy)
    unknown_attribution_count = int(metrics.get("unknown_attribution_count") or 0)
    if not bool(metrics.get("required_inputs_complete")) or list(metrics.get("invalidity_reasons") or []):
        reasons.extend(list(metrics.get("invalidity_reasons") or []))
        reasons.append(RC_MEASUREMENT_INVALID)
        state = STATE_MEASUREMENT_INVALID
    elif unknown_attribution_count > unknown_attribution_limit:
        reasons.append(RC_UNKNOWN_ATTRIBUTION_THRESHOLD_EXCEEDED)
        reasons.append(RC_MEASUREMENT_INVALID)
        state = STATE_MEASUREMENT_INVALID
    elif sample_band == SAMPLE_INSUFFICIENT:
        reasons.append(RC_SAMPLE_INSUFFICIENT)
        state = STATE_INSUFFICIENT_DATA
    elif edge_band == EDGE_NEGATIVE:
        reasons.append(RC_NEGATIVE_EXPECTANCY)
        state = STATE_DISABLED
    elif drift_band == DRIFT_DETERIORATING and sample_band == SAMPLE_SUFFICIENT:
        reasons.append(RC_DRIFT_DETERIORATING)
        state = STATE_THROTTLED
    elif execution_band == EXEC_DEGRADED:
        reasons.append(RC_EXECUTION_DEGRADED)
        state = STATE_WATCHLIST
    elif sample_band == SAMPLE_LIMITED:
        reasons.append(RC_SAMPLE_LIMITED)
        state = STATE_WATCHLIST
    elif edge_band == EDGE_WEAK:
        reasons.append(RC_WEAK_EXPECTANCY)
        state = STATE_WATCHLIST
    elif drift_band == DRIFT_UNKNOWN:
        reasons.append(RC_DRIFT_UNKNOWN)
        state = STATE_WATCHLIST
    else:
        state = STATE_QUALIFIED
    return {
        "edge_band": edge_band,
        "execution_health_band": execution_band,
        "sample_sufficiency_band": sample_band,
        "drift_band": drift_band,
        "qualification_state": state,
        "reason_codes": _unique_strings(reasons),
    }


def _resolve_fact_ledger_path(*, truth_root: Path, day_utc: str, sleeve_id: str, snapshot_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / FACT_LEDGER_FAMILY
        / _parse_day(day_utc)
        / sleeve_id
        / snapshot_id
        / "sleeve_edge_fact_ledger.v1.json"
    ).resolve()


def _resolve_snapshot_path(*, truth_root: Path, day_utc: str, sleeve_id: str, snapshot_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / SNAPSHOT_FAMILY
        / _parse_day(day_utc)
        / sleeve_id
        / snapshot_id
        / "sleeve_edge_snapshot.v1.json"
    ).resolve()


def _validate_snapshot_integrity_for_control_use(*, snapshot: Mapping[str, Any], repo_root: Path) -> None:
    try:
        validate_against_repo_schema_v1(_schema_payload(snapshot), repo_root, SNAPSHOT_SCHEMA)
    except Exception as exc:
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:SNAPSHOT_SCHEMA_INVALID:{exc}") from exc
    snapshot_path = Path(str(snapshot.get("artifact_path") or "")).resolve()
    if not snapshot_path.exists() or not snapshot_path.is_file():
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:SNAPSHOT_PATH_MISSING:path={snapshot_path}")
    expected_snapshot_sha = str(snapshot.get("artifact_sha256") or "").strip()
    actual_snapshot_sha = _sha256_file(snapshot_path)
    if expected_snapshot_sha and actual_snapshot_sha != expected_snapshot_sha:
        raise ValueError(
            f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:SNAPSHOT_SHA256_MISMATCH:"
            f"path={snapshot_path}:expected={expected_snapshot_sha}:actual={actual_snapshot_sha}"
        )
    fact_ledger_ref = snapshot.get("fact_ledger_ref")
    if not isinstance(fact_ledger_ref, dict):
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_REF_INVALID:path={snapshot_path}")
    fact_ledger_path = Path(str(fact_ledger_ref.get("artifact_path") or "")).resolve()
    if not fact_ledger_path.exists() or not fact_ledger_path.is_file():
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_PATH_MISSING:path={fact_ledger_path}")
    expected_fact_sha = str(fact_ledger_ref.get("artifact_sha256") or "").strip()
    actual_fact_sha = _sha256_file(fact_ledger_path)
    if expected_fact_sha and actual_fact_sha != expected_fact_sha:
        raise ValueError(
            f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_SHA256_MISMATCH:"
            f"path={fact_ledger_path}:expected={expected_fact_sha}:actual={actual_fact_sha}"
        )
    try:
        fact_ledger_payload = _read_json_obj(fact_ledger_path)
        validate_against_repo_schema_v1(fact_ledger_payload, repo_root, FACT_LEDGER_SCHEMA)
    except Exception as exc:
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_SCHEMA_INVALID:{exc}") from exc
    snapshot_fact_input_hash = str(snapshot.get("fact_input_hash") or "").strip()
    if not snapshot_fact_input_hash:
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_INPUT_HASH_MISSING:path={snapshot_path}")
    fact_ledger_fact_input_hash = str(fact_ledger_payload.get("fact_input_hash") or "").strip()
    if not fact_ledger_fact_input_hash:
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_FACT_INPUT_HASH_MISSING:path={fact_ledger_path}")
    recomputed_fact_input_hash = _fact_input_hash_from_fact_ledger(fact_ledger_payload)
    if fact_ledger_fact_input_hash != recomputed_fact_input_hash:
        raise ValueError(
            f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_LEDGER_FACT_INPUT_HASH_MISMATCH:"
            f"path={fact_ledger_path}:recorded={fact_ledger_fact_input_hash}:recomputed={recomputed_fact_input_hash}"
        )
    if snapshot_fact_input_hash != fact_ledger_fact_input_hash:
        raise ValueError(
            f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:FACT_INPUT_HASH_MISMATCH:"
            f"snapshot={snapshot_fact_input_hash}:fact_ledger={fact_ledger_fact_input_hash}"
        )


def _load_snapshot_candidate_strict(path: Path) -> Dict[str, Any]:
    resolved = path.resolve()
    try:
        payload = _read_json_obj(resolved)
    except Exception as exc:
        raise ValueError(f"{RC_SNAPSHOT_INTEGRITY_FAILURE}:SNAPSHOT_PARSE_INVALID:path={resolved}:detail={exc}") from exc
    payload["artifact_path"] = str(resolved)
    payload["artifact_sha256"] = _sha256_file(resolved)
    _validate_snapshot_integrity_for_control_use(snapshot=payload, repo_root=REPO_ROOT)
    return payload


def _iter_snapshot_paths(*, truth_root: Path, sleeve_id: str, day_utc: Optional[str] = None) -> Iterable[Path]:
    root = (Path(truth_root).resolve() / "reports" / SNAPSHOT_FAMILY).resolve()
    if not root.exists() or not root.is_dir():
        return []
    if day_utc:
        return sorted((root / _parse_day(day_utc) / sleeve_id).glob("*/sleeve_edge_snapshot.v1.json"))
    return sorted(root.glob(f"*/{sleeve_id}/*/sleeve_edge_snapshot.v1.json"))


def _read_latest_snapshot_for_day(*, truth_root: Path, sleeve_id: str, day_utc: str) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[Tuple[datetime, datetime, int, str], Dict[str, Any]]] = []
    for path in _iter_snapshot_paths(truth_root=truth_root, sleeve_id=sleeve_id, day_utc=day_utc):
        payload = _load_snapshot_candidate_strict(path)
        lineage_diagnostic_rank = (
            1
            if str((((payload.get("attribution_diagnostics") or {}).get("lineage_requirement_diagnostic")) or "")).strip()
            else 0
        )
        key = (
            _utc_sort_key(str(payload.get("as_of_ts") or "1970-01-01T00:00:00Z"))[0],
            _utc_sort_key(str(payload.get("produced_utc") or payload.get("as_of_ts") or "1970-01-01T00:00:00Z"))[0],
            lineage_diagnostic_rank,
            str(payload.get("snapshot_id") or ""),
        )
        candidates.append((key, payload))
    if not candidates:
        return None
    referenced_snapshot_ids = {
        str((((payload.get("snapshot_lineage") or {}).get("previous_snapshot_id")) or "")).strip()
        for _, payload in candidates
    }
    lineage_heads = [item for item in candidates if str(item[1].get("snapshot_id") or "").strip() not in referenced_snapshot_ids]
    if len(lineage_heads) == 1:
        return lineage_heads[0][1]
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def read_sleeve_edge_snapshot_for_day_v1(
    *,
    truth_root: Path,
    sleeve_id: str,
    day_utc: str,
    expected_policy_version: str = "",
    allowed_calculation_versions: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    payload = _read_latest_snapshot_for_day(truth_root=truth_root, sleeve_id=sleeve_id, day_utc=day_utc)
    if payload is None:
        raise ValueError(f"SLEEVE_EDGE_SNAPSHOT_MISSING:sleeve_id={sleeve_id}:day_utc={day_utc}")
    _validate_snapshot_integrity_for_control_use(snapshot=payload, repo_root=REPO_ROOT)
    locked_policy_version = str(expected_policy_version or "").strip()
    actual_policy_version = str(payload.get("policy_version") or "").strip()
    if locked_policy_version and actual_policy_version != locked_policy_version:
        raise ValueError(
            f"{RC_POLICY_VERSION_MISMATCH}:sleeve_id={sleeve_id}:day_utc={day_utc}:"
            f"expected={locked_policy_version}:actual={actual_policy_version}"
        )
    allowed_versions = _unique_strings(str(item) for item in (allowed_calculation_versions or []))
    actual_calculation_version = str(payload.get("calculation_version") or "").strip()
    if allowed_versions and actual_calculation_version not in allowed_versions:
        raise ValueError(
            f"{RC_CALCULATION_VERSION_MISMATCH}:sleeve_id={sleeve_id}:day_utc={day_utc}:"
            f"expected={','.join(allowed_versions)}:actual={actual_calculation_version}"
        )
    return payload


def _read_latest_snapshot_for_lineage(*, truth_root: Path, sleeve_id: str) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[Tuple[datetime, datetime, int, str], Dict[str, Any]]] = []
    for path in _iter_snapshot_paths(truth_root=truth_root, sleeve_id=sleeve_id):
        payload = _load_snapshot_candidate_strict(path)
        lineage_diagnostic_rank = (
            1
            if str((((payload.get("attribution_diagnostics") or {}).get("lineage_requirement_diagnostic")) or "")).strip()
            else 0
        )
        key = (
            _utc_sort_key(str(payload.get("as_of_ts") or "1970-01-01T00:00:00Z"))[0],
            _utc_sort_key(str(payload.get("produced_utc") or payload.get("as_of_ts") or "1970-01-01T00:00:00Z"))[0],
            lineage_diagnostic_rank,
            str(payload.get("snapshot_id") or ""),
        )
        candidates.append((key, payload))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _read_prior_snapshot(*, truth_root: Path, sleeve_id: str, before_as_of_ts: str) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[Tuple[datetime, datetime, int, str], Dict[str, Any]]] = []
    cutoff = _utc_sort_key(before_as_of_ts)[0]
    for path in _iter_snapshot_paths(truth_root=truth_root, sleeve_id=sleeve_id):
        payload = _load_snapshot_candidate_strict(path)
        lineage_diagnostic_rank = (
            1
            if str((((payload.get("attribution_diagnostics") or {}).get("lineage_requirement_diagnostic")) or "")).strip()
            else 0
        )
        key = (
            _utc_sort_key(str(payload.get("as_of_ts") or "1970-01-01T00:00:00Z"))[0],
            _utc_sort_key(str(payload.get("produced_utc") or payload.get("as_of_ts") or "1970-01-01T00:00:00Z"))[0],
            lineage_diagnostic_rank,
            str(payload.get("snapshot_id") or ""),
        )
        if key[0] >= cutoff:
            continue
        candidates.append((key, payload))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def materialize_sleeve_edge_snapshot_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    sleeve_id: str,
    strategy_family: str,
    engine_ids: Sequence[str],
    qualification_policy: Optional[Mapping[str, Any]] = None,
    revision_type: str = "",
    revision_reason: str = "",
) -> SleeveEdgeSnapshotMaterializationV1:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = _parse_day(day_utc)
    policy = dict(qualification_policy or load_sleeve_edge_policy_v1(repo_root))
    core2_summary_path, core2_summary_payload = _load_latest_core2_summary(truth_root=truth_root, day_utc=day)
    fact_ledger, ledger_aux = _build_fact_ledger(
        core2_summary_path=core2_summary_path,
        core2_summary_payload=core2_summary_payload,
        execution_index=_load_execution_index(truth_root=truth_root, max_day_utc=day),
        sleeve_id=sleeve_id,
        strategy_family=strategy_family,
        engine_ids=engine_ids,
        qualification_policy=policy,
    )
    metrics = _compute_metrics(fact_ledger=fact_ledger, qualification_policy=policy)
    qualification = _qualify(metrics, policy)
    unavailable_metrics = _unavailable_metrics(metrics)
    as_of_ts = str(fact_ledger.get("as_of_ts") or "")
    existing_snapshot = _read_latest_snapshot_for_day(truth_root=truth_root, sleeve_id=sleeve_id, day_utc=day)
    current_policy_version = _policy_version(policy)
    current_fact_input_hash = str(ledger_aux["fact_input_hash"])
    current_lineage_requirement_diagnostic = str(
        ((fact_ledger.get("attribution_diagnostics") or {}).get("lineage_requirement_diagnostic") or "")
    ).strip()
    existing_lineage_requirement_diagnostic = str(
        (((existing_snapshot or {}).get("attribution_diagnostics") or {}).get("lineage_requirement_diagnostic") or "")
    ).strip()
    if (
        existing_snapshot is not None
        and str(existing_snapshot.get("as_of_ts") or "") == as_of_ts
        and str(existing_snapshot.get("fact_input_hash") or "") == current_fact_input_hash
        and str(existing_snapshot.get("policy_version") or "") == current_policy_version
        and str(existing_snapshot.get("calculation_version") or "") == CALCULATION_VERSION
        and existing_lineage_requirement_diagnostic == current_lineage_requirement_diagnostic
    ):
        fact_ledger_ref = existing_snapshot.get("fact_ledger_ref")
        if not isinstance(fact_ledger_ref, dict):
            raise ValueError("SLEEVE_EDGE_FACT_LEDGER_REF_INVALID")
        fact_ledger_path = Path(str(fact_ledger_ref.get("artifact_path") or "")).resolve()
        if not fact_ledger_path.exists() or not fact_ledger_path.is_file():
            raise ValueError(f"SLEEVE_EDGE_FACT_LEDGER_MISSING:path={fact_ledger_path}")
        return SleeveEdgeSnapshotMaterializationV1(
            truth_root=truth_root,
            day_utc=day,
            sleeve_id=sleeve_id,
            snapshot_id=str(existing_snapshot.get("snapshot_id") or ""),
            fact_ledger_path=fact_ledger_path,
            snapshot_path=Path(str(existing_snapshot.get("artifact_path") or "")).resolve(),
            fact_ledger=_read_json_obj(fact_ledger_path),
            snapshot=existing_snapshot,
        )
    prior_snapshot = _read_latest_snapshot_for_lineage(truth_root=truth_root, sleeve_id=sleeve_id)
    compatible_prior_snapshot = (
        prior_snapshot
        if prior_snapshot is not None and _snapshot_lineage_compatible(
            prior_snapshot,
            policy_version=current_policy_version,
            calculation_version=CALCULATION_VERSION,
        )
        else None
    )
    incompatible_lineage_reason = (
        _lineage_restart_reason(
            prior_snapshot,
            policy_version=current_policy_version,
            calculation_version=CALCULATION_VERSION,
        )
        if prior_snapshot is not None and compatible_prior_snapshot is None
        else ""
    )
    resolved_revision_type = _normalize_revision_type(
        revision_type,
        previous_snapshot=compatible_prior_snapshot,
        current_policy_version=current_policy_version,
        current_fact_input_hash=current_fact_input_hash,
    )
    resolved_revision_reason = (
        incompatible_lineage_reason
        if incompatible_lineage_reason
        else _normalize_revision_reason(revision_reason, revision_type=resolved_revision_type)
    )
    snapshot_seed = {
        "sleeve_id": sleeve_id,
        "day_utc": day,
        "as_of_ts": as_of_ts,
        "fact_input_hash": current_fact_input_hash,
        "calculation_version": CALCULATION_VERSION,
        "policy_version": current_policy_version,
        "previous_snapshot_id": str((compatible_prior_snapshot or {}).get("snapshot_id") or ""),
        "revision_type": resolved_revision_type,
        "revision_reason": resolved_revision_reason,
    }
    snapshot_id = _sha256_bytes(canonical_json_bytes_v1(snapshot_seed))
    fact_ledger_path = _resolve_fact_ledger_path(truth_root=truth_root, day_utc=day, sleeve_id=sleeve_id, snapshot_id=snapshot_id)
    transition_reason_codes: List[str] = []
    if compatible_prior_snapshot is not None:
        prior_state = str(((compatible_prior_snapshot.get("qualification") or {}).get("qualification_state")) or "")
        current_state = str(qualification.get("qualification_state") or "")
        if prior_state and prior_state != current_state:
            transition_reason_codes.append(f"{RC_STATE_TRANSITION_PREFIX}:{prior_state}:{current_state}")
    snapshot_reason_codes = _unique_strings(list(qualification.get("reason_codes") or []) + transition_reason_codes)
    producer = {"repo": REPO_ROOT.name, "git_sha": _git_sha_failclosed(), "module": "constellation_2/common/sleeve_edge_measurement_v1.py"}
    snapshot = {
        "schema_id": "C2_SLEEVE_EDGE_SNAPSHOT_V1",
        "schema_version": "v1",
        "snapshot_id": snapshot_id,
        "produced_utc": as_of_ts,
        "day_utc": day,
        "as_of_ts": as_of_ts,
        "sleeve_id": sleeve_id,
        "strategy_family": strategy_family,
        "metric_window": _metric_window(policy),
        "included_trade_ids": list(fact_ledger.get("included_trade_ids") or []),
        "excluded_trade_ids": list(fact_ledger.get("excluded_trade_ids") or []),
        "exclusion_details": list(fact_ledger.get("exclusion_details") or []),
        "fact_input_hash": ledger_aux["fact_input_hash"],
        "calculation_version": CALCULATION_VERSION,
        "policy_version": current_policy_version,
        "fact_ledger_ref": {
            "artifact_path": str(fact_ledger_path),
            "artifact_sha256": _sha256_bytes(canonical_json_bytes_v1(fact_ledger) + b"\n"),
        },
        "factual_metrics": metrics,
        "unavailable_metrics": unavailable_metrics,
        "qualification": qualification,
        "reason_codes": snapshot_reason_codes,
        "attribution_diagnostics": dict(fact_ledger.get("attribution_diagnostics") or {}),
        "snapshot_lineage": {
            "core2_materialization_set_id": str(fact_ledger.get("core2_materialization_set_id") or ""),
            "source_execution_sleeve_id": str(fact_ledger.get("source_execution_sleeve_id") or ""),
            "previous_snapshot_id": str((compatible_prior_snapshot or {}).get("snapshot_id") or "") or None,
            "revision_type": resolved_revision_type,
            "revision_reason": resolved_revision_reason,
            "prior_snapshot_ref": (
                {
                    "artifact_path": str(compatible_prior_snapshot.get("artifact_path") or ""),
                    "snapshot_id": str(compatible_prior_snapshot.get("snapshot_id") or ""),
                    "qualification_state": str(((compatible_prior_snapshot.get("qualification") or {}).get("qualification_state")) or ""),
                }
                if compatible_prior_snapshot is not None
                else {"artifact_path": "", "snapshot_id": "", "qualification_state": ""}
            ),
            "state_transition_reason_codes": transition_reason_codes,
        },
        "input_manifest": [
            {
                "type": "reconciled_trade_state_summary",
                "path": str(core2_summary_path),
                "sha256": _sha256_file(core2_summary_path),
                "day_utc": day,
                "producer": "reconciled_trade_state_summary_v1",
            },
            {
                "type": "qualification_policy_manifest",
                "path": str(QUALIFICATION_POLICY_PATH),
                "sha256": _sha256_file(QUALIFICATION_POLICY_PATH),
                "day_utc": None,
                "producer": "governance",
            },
        ]
        + [
            {
                "type": "execution_event_stream_record",
                "path": path,
                "sha256": sha,
                "day_utc": None,
                "producer": "execution_stream_v1",
            }
            for path, sha in ledger_aux["matched_execution_refs"]
        ],
        "producer": producer,
    }
    validate_against_repo_schema_v1(fact_ledger, repo_root, FACT_LEDGER_SCHEMA)
    validate_against_repo_schema_v1(snapshot, repo_root, SNAPSHOT_SCHEMA)
    fact_write = _write_validated_immutable_json(repo_root=repo_root, path=fact_ledger_path, payload=fact_ledger, schema_relpath=FACT_LEDGER_SCHEMA)
    snapshot["fact_ledger_ref"]["artifact_sha256"] = fact_write.sha256
    snapshot_path = _resolve_snapshot_path(truth_root=truth_root, day_utc=day, sleeve_id=sleeve_id, snapshot_id=snapshot_id)
    snapshot_write = _write_validated_immutable_json(repo_root=repo_root, path=snapshot_path, payload=snapshot, schema_relpath=SNAPSHOT_SCHEMA)
    snapshot["artifact_path"] = str(snapshot_path)
    snapshot["artifact_sha256"] = snapshot_write.sha256
    return SleeveEdgeSnapshotMaterializationV1(
        truth_root=truth_root,
        day_utc=day,
        sleeve_id=sleeve_id,
        snapshot_id=snapshot_id,
        fact_ledger_path=fact_ledger_path,
        snapshot_path=snapshot_path,
        fact_ledger=fact_ledger,
        snapshot=snapshot,
    )


__all__ = [
    "CALCULATION_VERSION",
    "ImmutableWriteError",
    "SleeveEdgeSnapshotMaterializationV1",
    "allocator_action_from_qualification_v1",
    "load_sleeve_edge_policy_v1",
    "materialize_sleeve_edge_snapshot_v1",
    "read_sleeve_edge_snapshot_for_day_v1",
]
