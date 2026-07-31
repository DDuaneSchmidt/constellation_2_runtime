from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1


EVIDENCE_LEDGER_SCHEMA_ID = "thesis_evidence_ledger"
POLICY_REGISTRY_SCHEMA_ID = "thesis_lifecycle_policy_registry"
THESIS_PROJECTION_SCHEMA_ID = "thesis_state_projection"
THESIS_OUTCOME_FEEDBACK_SCHEMA_ID = "thesis_outcome_feedback"
SCHEMA_VERSION = "v1"
EVIDENCE_FAMILY = "thesis_evidence_ledger_v1"
PROJECTION_FAMILY = "thesis_state_projection_v1"
OUTCOME_FAMILY = "thesis_outcome_feedback_v1"

EVIDENCE_TYPES = {
    "HOLDING_PERIOD_EXCEEDED",
    "RETURN_BELOW_TARGET",
    "MFE_BELOW_EXPECTATION",
    "MAE_ELEVATED",
    "RELATIVE_STRENGTH_WEAKENED",
    "REGIME_CHANGED",
    "EVENT_RESOLVED",
    "VOLATILITY_COMPRESSED",
    "THESIS_SUPPORTING_SIGNAL_PRESENT",
    "THESIS_CONTRADICTING_SIGNAL_PRESENT",
}

THESIS_STATES = {
    "FORMING",
    "STRENGTHENING",
    "STABLE",
    "DECAYING",
    "INVALIDATED",
    "RESOLVED_SUCCESSFULLY",
    "RESOLVED_UNSUCCESSFULLY",
    "INCONCLUSIVE",
    "BLOCKED_MISSING_EVIDENCE",
}

SAFETY_FLAGS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "policy_auto_change_allowed": False,
}

REPO_ROOT = Path(__file__).resolve().parents[3]
POLICY_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/THESIS_LIFECYCLE_POLICY_REGISTRY_V1.json").resolve()


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _policy_registry(policy_registry_path: Path | str | None = None) -> dict[str, Any]:
    path = Path(policy_registry_path).resolve() if policy_registry_path else POLICY_REGISTRY_PATH
    payload = _read_json(path)
    if payload.get("schema_id") != POLICY_REGISTRY_SCHEMA_ID:
        return {"schema_id": POLICY_REGISTRY_SCHEMA_ID, "schema_version": SCHEMA_VERSION, "policies": []}
    return payload


def load_thesis_lifecycle_policies_v1(policy_registry_path: Path | str | None = None) -> list[dict[str, Any]]:
    registry = _policy_registry(policy_registry_path)
    policies = registry.get("policies") if isinstance(registry.get("policies"), list) else []
    return [dict(policy) for policy in policies if isinstance(policy, dict)]


def resolve_policy_for_trade_v1(trade: Mapping[str, Any], policies: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sleeve_id = _text(trade.get("sleeve_id")).upper()
    candidates = []
    for policy in policies:
        pid = _text(policy.get("sleeve_id")).upper()
        if pid and pid == sleeve_id:
            candidates.append(dict(policy))
    if candidates:
        return sorted(candidates, key=lambda row: (_text(row.get("version")), _text(row.get("policy_id"))))[-1]
    for policy in policies:
        if _text(policy.get("sleeve_id")).upper() == "DEFAULT":
            return dict(policy)
    return {
        "policy_id": "thesis_policy_default_missing_v1",
        "sleeve_id": "DEFAULT",
        "setup_type": "DEFAULT",
        "version": "v1",
        "evidence_requirements": [],
        "deterministic_rules": {},
        "required_inputs": [],
        "missing_input_behavior": "INCONCLUSIVE",
        "confidence_mapping": {"default": "LOW"},
    }


def thesis_evidence_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / EVIDENCE_FAMILY / str(day_utc) / "thesis_evidence_ledger.v1.jsonl"


def thesis_state_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / PROJECTION_FAMILY / str(day_utc) / "thesis_state_projection.v1.json"


def thesis_outcome_feedback_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / OUTCOME_FAMILY / str(day_utc) / "thesis_outcome_feedback.v1.json"


def _source_refs_for_trade(trade: Mapping[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for raw in trade.get("source_artifacts") or []:
        path = Path(str(raw)).expanduser().resolve()
        refs.append({"artifact_path": str(path), "artifact_sha256": _sha256(path)})
    if trade.get("mark_source_artifact"):
        path = Path(str(trade.get("mark_source_artifact"))).expanduser().resolve()
        refs.append({"artifact_path": str(path), "artifact_sha256": str(trade.get("mark_content_hash") or _sha256(path))})
    dedup: dict[str, dict[str, str]] = {}
    for ref in refs:
        if ref.get("artifact_path"):
            dedup[ref["artifact_path"]] = ref
    return [dedup[key] for key in sorted(dedup)]


def _evidence_row(
    *,
    trade: Mapping[str, Any],
    evidence_type: str,
    evidence_value: Any,
    threshold_value: Any,
    observed_at: str,
    rule_version: str,
    source_artifacts: list[dict[str, str]],
) -> dict[str, Any]:
    if evidence_type not in EVIDENCE_TYPES:
        raise ValueError(f"unsupported evidence_type: {evidence_type}")
    base = {
        "evidence_id": "",
        "trade_id": _text(trade.get("trade_id")),
        "position_id": _text(trade.get("position_id"), _text(trade.get("trade_id")).replace(":", "_")),
        "symbol": _text(trade.get("symbol")).upper(),
        "sleeve_id": _text(trade.get("sleeve_id")),
        "hypothesis_id": _text(trade.get("hypothesis_id")),
        "evidence_type": evidence_type,
        "evidence_value": evidence_value,
        "threshold_value": threshold_value,
        "source_artifacts": source_artifacts,
        "input_hash": stable_hash_v1({"trade": dict(trade), "evidence_type": evidence_type, "threshold_value": threshold_value}),
        "observed_at": observed_at,
        "rule_version": rule_version,
        "content_hash": "",
    }
    content_hash = stable_hash_v1({**base, "evidence_id": "", "content_hash": ""})
    base["evidence_id"] = f"thesis-evidence:{content_hash[:24]}"
    base["content_hash"] = content_hash
    return base


def _trade_metric(trade: Mapping[str, Any], key: str) -> float | None:
    aliases = {
        "holding_days": ("holding_days", "holding_time_days", "days_held"),
        "return_pct": ("return_pct",),
        "MFE": ("MFE", "max_favorable_excursion", "mfe_r"),
        "MAE": ("MAE", "max_adverse_excursion", "mae_r"),
    }
    for alias in aliases.get(key, (key,)):
        value = _number(trade.get(alias))
        if value is not None:
            return value
    return None


def evidence_rows_for_trade_v1(
    *,
    trade: Mapping[str, Any],
    policy: Mapping[str, Any],
    observed_at: str,
) -> list[dict[str, Any]]:
    rules = policy.get("deterministic_rules") if isinstance(policy.get("deterministic_rules"), Mapping) else {}
    version = _text(policy.get("version"), "v1")
    source_refs = _source_refs_for_trade(trade)
    rows: list[dict[str, Any]] = []

    holding_days = _trade_metric(trade, "holding_days")
    max_holding = _number(rules.get("max_holding_days"))
    if holding_days is not None and max_holding is not None and holding_days > max_holding:
        rows.append(_evidence_row(trade=trade, evidence_type="HOLDING_PERIOD_EXCEEDED", evidence_value=holding_days, threshold_value=max_holding, observed_at=observed_at, rule_version=version, source_artifacts=source_refs))

    return_pct = _trade_metric(trade, "return_pct")
    partial_target = _number(rules.get("partial_target_threshold_pct"))
    if return_pct is not None and partial_target is not None and return_pct < partial_target:
        rows.append(_evidence_row(trade=trade, evidence_type="RETURN_BELOW_TARGET", evidence_value=return_pct, threshold_value=partial_target, observed_at=observed_at, rule_version=version, source_artifacts=source_refs))

    mfe = _trade_metric(trade, "MFE")
    mfe_min = _number(rules.get("mfe_min_r_multiple"))
    if mfe is not None and mfe_min is not None and mfe < mfe_min:
        rows.append(_evidence_row(trade=trade, evidence_type="MFE_BELOW_EXPECTATION", evidence_value=mfe, threshold_value=mfe_min, observed_at=observed_at, rule_version=version, source_artifacts=source_refs))

    mae = _trade_metric(trade, "MAE")
    mae_threshold = _number(rules.get("mae_elevated_r_multiple"))
    if mae is not None and mae_threshold is not None and abs(mae) > mae_threshold:
        rows.append(_evidence_row(trade=trade, evidence_type="MAE_ELEVATED", evidence_value=mae, threshold_value=mae_threshold, observed_at=observed_at, rule_version=version, source_artifacts=source_refs))

    if not rows and return_pct is not None and partial_target is not None and return_pct >= partial_target:
        rows.append(_evidence_row(trade=trade, evidence_type="THESIS_SUPPORTING_SIGNAL_PRESENT", evidence_value=return_pct, threshold_value=partial_target, observed_at=observed_at, rule_version=version, source_artifacts=source_refs))
    return rows


def build_thesis_evidence_ledger_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    trades: Sequence[Mapping[str, Any]] | None = None,
    policy_registry_path: Path | str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    policies = load_thesis_lifecycle_policies_v1(policy_registry_path)
    source_projection_hash = ""
    if trades is None:
        projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
        source_projection_hash = _text(projection.get("content_hash"))
        trade_rows = [row for row in projection.get("all_trades") or [] if isinstance(row, Mapping)]
    else:
        trade_rows = [row for row in trades if isinstance(row, Mapping)]
    observed_at = f"{day_utc}T00:00:00Z"
    rows: list[dict[str, Any]] = []
    for trade in trade_rows:
        policy = resolve_policy_for_trade_v1(trade, policies)
        rows.extend(evidence_rows_for_trade_v1(trade=trade, policy=policy, observed_at=observed_at))
    rows = sorted(rows, key=lambda row: (str(row.get("trade_id") or ""), str(row.get("evidence_type") or ""), str(row.get("evidence_id") or "")))
    payload = {
        "schema_id": EVIDENCE_LEDGER_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "row_count": len(rows),
        "rows": rows,
        "source_projection_hash": source_projection_hash,
        "policy_registry_path": str(Path(policy_registry_path).resolve() if policy_registry_path else POLICY_REGISTRY_PATH),
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def append_thesis_evidence_rows_v1(*, ledger_path: Path | str, rows: Sequence[Mapping[str, Any]]) -> Path:
    path = Path(ledger_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_size = path.stat().st_size if path.exists() else 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    if path.stat().st_size < existing_size:
        raise RuntimeError("THESIS_EVIDENCE_LEDGER_APPEND_ONLY_VIOLATION")
    return path


def write_thesis_evidence_ledger_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = thesis_evidence_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    append_thesis_evidence_rows_v1(ledger_path=path, rows=[row for row in payload.get("rows") or [] if isinstance(row, Mapping)])
    return {"thesis_evidence_ledger": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}


def _rows_by_trade(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        trade_id = _text(row.get("trade_id"))
        if trade_id:
            out.setdefault(trade_id, []).append(dict(row))
    return out


def _missing_inputs(trade: Mapping[str, Any], policy: Mapping[str, Any]) -> list[str]:
    missing = []
    for key in policy.get("required_inputs") or []:
        if _trade_metric(trade, str(key)) is None:
            missing.append(str(key))
    return missing


def project_trade_thesis_state_v1(*, trade: Mapping[str, Any], evidence_rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]) -> dict[str, Any]:
    missing = _missing_inputs(trade, policy)
    policy_version = _text(policy.get("version"), "v1")
    policy_id = _text(policy.get("policy_id"), "UNKNOWN_POLICY")
    types = {str(row.get("evidence_type") or "") for row in evidence_rows}
    rules = policy.get("deterministic_rules") if isinstance(policy.get("deterministic_rules"), Mapping) else {}
    decay_required = {str(item) for item in rules.get("decay_requires_all") or [] if str(item)}
    invalidated_required = {str(item) for item in rules.get("invalidated_requires_all") or [] if str(item)}
    supporting_ids = [str(row.get("evidence_id")) for row in evidence_rows if str(row.get("evidence_type")) == "THESIS_SUPPORTING_SIGNAL_PRESENT"]
    contradicting_ids = [str(row.get("evidence_id")) for row in evidence_rows if str(row.get("evidence_type")) != "THESIS_SUPPORTING_SIGNAL_PRESENT"]

    if missing and _text(policy.get("missing_input_behavior"), "BLOCKED_MISSING_EVIDENCE") == "BLOCKED_MISSING_EVIDENCE":
        state = "BLOCKED_MISSING_EVIDENCE"
        confidence = "LOW"
        exit_bias = "NONE"
        manual_next_action = "Collect missing thesis evidence before using thesis state in review."
        why = f"Missing required thesis evidence inputs: {', '.join(missing)}."
    elif invalidated_required and invalidated_required.issubset(types):
        state = "INVALIDATED"
        confidence = "HIGH"
        exit_bias = "REVIEW"
        manual_next_action = "Review thesis invalidation evidence."
        why = "All invalidation evidence requirements were present."
    elif decay_required and decay_required.issubset(types):
        state = "DECAYING"
        confidence = "MEDIUM"
        exit_bias = "REVIEW"
        manual_next_action = "Review thesis decay evidence."
        why = "Holding period, return, and MFE decay evidence met deterministic policy thresholds."
    elif supporting_ids and not contradicting_ids:
        state = "STABLE"
        confidence = "LOW"
        exit_bias = "NONE"
        manual_next_action = "Continue monitoring thesis evidence."
        why = "Only supporting thesis evidence was present."
    elif contradicting_ids:
        state = "INCONCLUSIVE"
        confidence = "LOW"
        exit_bias = "REVIEW"
        manual_next_action = "Review mixed thesis evidence."
        why = "Contradicting thesis evidence was present but did not meet decay/invalidation thresholds."
    else:
        state = "INCONCLUSIVE"
        confidence = "LOW"
        exit_bias = "NONE"
        manual_next_action = "Collect thesis evidence."
        why = "No deterministic thesis lifecycle evidence was available."

    input_hashes = [str(row.get("input_hash") or "") for row in evidence_rows if row.get("input_hash")]
    projection = {
        "trade_id": _text(trade.get("trade_id")),
        "position_id": _text(trade.get("position_id"), _text(trade.get("trade_id")).replace(":", "_")),
        "symbol": _text(trade.get("symbol")).upper(),
        "sleeve_id": _text(trade.get("sleeve_id")),
        "hypothesis_id": _text(trade.get("hypothesis_id")),
        "current_thesis_state": state,
        "confidence": confidence,
        "evidence_summary": {
            "evidence_count": len(evidence_rows),
            "supporting_count": len(supporting_ids),
            "contradicting_count": len(contradicting_ids),
            "missing_inputs": missing,
            "why": why,
        },
        "supporting_evidence_ids": supporting_ids,
        "contradicting_evidence_ids": contradicting_ids,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "input_hashes": sorted(input_hashes),
        "exit_bias": exit_bias,
        "manual_next_action": manual_next_action,
        "safety": dict(SAFETY_FLAGS),
        "projection_hash": "",
    }
    projection["projection_hash"] = stable_hash_v1({**projection, "projection_hash": ""})
    return projection


def build_thesis_state_projection_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    trades: Sequence[Mapping[str, Any]] | None = None,
    evidence_ledger: Mapping[str, Any] | None = None,
    policy_registry_path: Path | str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    policies = load_thesis_lifecycle_policies_v1(policy_registry_path)
    if trades is None:
        trade_projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
        trade_rows = [row for row in trade_projection.get("all_trades") or [] if isinstance(row, Mapping)]
        source_projection_hash = _text(trade_projection.get("content_hash"))
    else:
        trade_rows = [row for row in trades if isinstance(row, Mapping)]
        source_projection_hash = ""
    ledger = dict(evidence_ledger or build_thesis_evidence_ledger_v1(truth_root=root, day_utc=day_utc, trades=trade_rows, policy_registry_path=policy_registry_path))
    evidence_rows = [row for row in ledger.get("rows") or [] if isinstance(row, Mapping)]
    evidence_by_trade = _rows_by_trade(evidence_rows)
    rows = []
    for trade in trade_rows:
        policy = resolve_policy_for_trade_v1(trade, policies)
        rows.append(project_trade_thesis_state_v1(trade=trade, evidence_rows=evidence_by_trade.get(_text(trade.get("trade_id")), []), policy=policy))
    rows = sorted(rows, key=lambda row: (str(row.get("symbol") or ""), str(row.get("trade_id") or "")))
    payload = {
        "schema_id": THESIS_PROJECTION_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "row_count": len(rows),
        "rows": rows,
        "source_projection_hash": source_projection_hash,
        "evidence_ledger_hash": str(ledger.get("content_hash") or stable_hash_v1(ledger)),
        "policy_registry_path": str(Path(policy_registry_path).resolve() if policy_registry_path else POLICY_REGISTRY_PATH),
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_thesis_state_projection_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = thesis_state_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"thesis_state_projection": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}


def read_thesis_evidence_ledger_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    path = thesis_evidence_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    rows = _read_jsonl(path)
    return {
        "schema_id": EVIDENCE_LEDGER_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "row_count": len(rows),
        "rows": rows,
        "content_hash": stable_hash_v1(rows),
        **SAFETY_FLAGS,
    }


def _projection_by_trade(projection: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in projection.get("rows") or []:
        if isinstance(row, Mapping) and row.get("trade_id"):
            out[str(row.get("trade_id"))] = dict(row)
    return out


def build_thesis_outcome_feedback_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    thesis_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    trade_projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
    projection = dict(thesis_projection or build_thesis_state_projection_v1(truth_root=root, day_utc=day_utc))
    thesis_by_trade = _projection_by_trade(projection)
    rows = []
    for trade in trade_projection.get("closed_trades") or []:
        if not isinstance(trade, Mapping):
            continue
        trade_id = _text(trade.get("trade_id"))
        thesis = thesis_by_trade.get(trade_id, {})
        outcome = _text(trade.get("evaluation_summary"), "INCONCLUSIVE")
        thesis_state = _text(thesis.get("current_thesis_state"), "INCONCLUSIVE")
        decay_predicted = thesis_state in {"DECAYING", "INVALIDATED"}
        unsuccessful = outcome in {"FAILED", "RESOLVED_UNSUCCESSFULLY"} or (_number(trade.get("realized_pnl")) is not None and (_number(trade.get("realized_pnl")) or 0) < 0)
        if thesis_state == "BLOCKED_MISSING_EVIDENCE":
            policy_review = "REVIEW_DECAY_RULE"
        elif decay_predicted and not unsuccessful:
            policy_review = "REVIEW_DECAY_RULE"
        elif decay_predicted and unsuccessful:
            policy_review = "NONE"
        else:
            policy_review = "NONE"
        row = {
            "trade_id": trade_id,
            "position_id": _text(trade.get("position_id"), trade_id.replace(":", "_")),
            "symbol": _text(trade.get("symbol")).upper(),
            "planned_thesis_state_path": [thesis_state] if thesis_state else [],
            "actual_outcome": outcome,
            "thesis_decay_predicted_outcome": bool(decay_predicted and unsuccessful),
            "exit_bias_helped": "UNKNOWN",
            "recommended_policy_review": policy_review,
            "source_thesis_projection_hash": _text(thesis.get("projection_hash")),
            **SAFETY_FLAGS,
        }
        row["content_hash"] = stable_hash_v1(row)
        rows.append(row)
    payload = {
        "schema_id": THESIS_OUTCOME_FEEDBACK_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "feedback_count": len(rows),
        "rows": sorted(rows, key=lambda row: (row.get("symbol", ""), row.get("trade_id", ""))),
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_thesis_outcome_feedback_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> dict[str, str]:
    body = dict(payload or build_thesis_outcome_feedback_v1(truth_root=truth_root, day_utc=day_utc))
    path = thesis_outcome_feedback_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"thesis_outcome_feedback": str(path), "content_hash": str(body.get("content_hash") or stable_hash_v1(body))}
